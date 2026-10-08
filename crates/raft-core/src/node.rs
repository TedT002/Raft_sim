//! Sans-IO Raft düğümü: tek genel API `step(Input) -> Vec<Output>`.
//!
//! Kapsam: lider seçimi (§5.2, seçim kısıtı §5.4.1), log replikasyonu (§5.3), commit kuralı
//! (§5.4.2), commit edilen girdilerin sırayla uygulanması ve çökme sonrası yeniden başlatma.
//! İstemci arayüzünün geri kalanı (Faz 4: `NotLeader` cevabı, tekrarlanan isteklerin ayıklanması,
//! liderliğin başında no-op girdi) henüz yoktur.

use std::collections::{BTreeMap, BTreeSet};

use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};

use crate::config::Config;
use crate::input::Input;
use crate::log::{Log, LogEntry};
use crate::message::{
    AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse,
};
use crate::output::Output;
use crate::persist::{PersistUpdate, PersistentState};
use crate::role::Role;
use crate::types::{Command, LogIndex, NodeId, Term};

/// Bir adımda gönderilecek mesajlar, üretildikleri sırayla. İşleyiciler yalnızca buraya yazar;
/// `Persist` kararını `step` tek bir yerde verir (O2). Böylece hiçbir işleyici `Persist`'i yanlış
/// yere koyamaz ya da unutamaz.
type Outbox = Vec<(NodeId, Message)>;

/// Liderin bir takipçi için tuttuğu ilerleme (Figure 2, "Volatile state on leaders"). Her seçimden
/// sonra yeniden kurulur; lider olmayan bir düğümde boştur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Progress {
    /// O takipçiye gönderilecek bir sonraki girdinin index'i (`nextIndex`).
    next_index: LogIndex,
    /// O takipçinin log'unun liderinkiyle eşleştiği bilinen en yüksek index (`matchIndex`).
    match_index: LogIndex,
}

/// Bir Raft düğümünün durum makinesi.
///
/// `raft-core` içindeki TEK genel giriş noktası `step`'tir: saat, ağ ve disk tamamen dışarıdadır
/// (sans-IO mimarisi). Bu sayede sürücü (simülatör veya gerçek çalıştırıcı) her adımı tam kontrol
/// eder, testler deterministik olur ve `raft-core` hiçbir G/Ç bağımlılığı taşımaz.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftNode {
    // Private: dışarıdan yalnızca `id()` erişimcisiyle okunur, doğrudan alan erişimi yoktur.
    id: NodeId,
    // Private ve BTreeSet: yineleme sırası deterministik olsun diye (bkz. crate kuralı,
    // `HashMap`/`HashSet` yasak). Çoğunluk (majority) `peers.len() + 1` üzerinden hesaplanır; bu
    // yüzden `id`'nin kendisi bu kümede bulunmamalı (N1).
    peers: BTreeSet<NodeId>,
    config: Config,
    // Seçim zaman aşımının TEK rastgelelik kaynağı: kurucuya verilen seed'den kurulan ChaCha8.
    // Akışı platformlar ve sürümler arasında değer-kararlıdır; aynı seed her makinede aynı zaman
    // aşımlarını üretir. Sistem entropisi (`thread_rng` vb.) bu crate'te yasaktır.
    rng: ChaCha8Rng,

    // --- Kalıcı durum (Figure 2, "Persistent state on all servers"). Değiştiği her adımda, o
    // adımın bütün `Send`'lerinden önce tek bir `Output::Persist` ile diske yazdırılır (O1, O2).
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // --- Geçici durum: çökmede kaybolur, `Restart` onu sıfırdan kurar (R1).
    role: Role,
    // Bu term'deki adaylıkta oy veren düğümler (kendisi dahil). Sayaç değil küme: aynı düğümün
    // çoğaltılmış ya da tekrarlanmış cevabı iki kez sayılmasın (E2).
    votes: BTreeSet<NodeId>,
    // Commit edildiği bilinen en yüksek index ve durum makinesine uygulanmış en yüksek index
    // (Figure 2, "Volatile state on all servers"). İkisi de yalnızca artar; yeniden başlatmada
    // 0'dan başlar. Commit bilgisi kaybolmaz: lider onu bir sonraki AppendEntries'le yeniden
    // bildirir.
    commit_index: LogIndex,
    last_applied: LogIndex,
    // Son sıfırlamadan bu yana geçen tick sayısı ve bu tur için çekilen zaman aşımı. Lider bu
    // sayacı işletmez (liderin seçim zaman aşımı yoktur).
    election_elapsed: u64,
    election_timeout: u64,
    // Liderin son heartbeat'ten bu yana geçen tick sayısı.
    heartbeat_elapsed: u64,
    // Lider olunan term'de her takipçinin ilerlemesi. BTreeMap: takipçiler her koşuda aynı sırayla
    // gezilir.
    progress: BTreeMap<NodeId, Progress>,
}

impl RaftNode {
    /// Yeni bir düğüm oluşturur: boş diskle ilk kez açılan bir Follower (term 0, oy yok, boş log).
    ///
    /// N1: `id`, `peers` kümesinde varsa çıkarılır ("kendi kendinin eşi olamaz"). Gerekçe: çoğunluk
    /// sayımı `peers().len() + 1` (kendisi + eşler) biçiminde yapılır; `peers` içinde yanlışlıkla
    /// bir kendi-girdisi kalırsa bu sayım bir fazla sayar ve çoğunluk yanlış hesaplanır.
    ///
    /// `seed`, düğümün seçim zaman aşımlarını çektiği ChaCha8 akışının anahtarıdır: aynı seed her
    /// zaman aynı zaman aşımı dizisini verir. Kümedeki düğümler farklı seed'lerle kurulmalıdır;
    /// aynı seed'i paylaşan düğümler aynı anda zaman aşımına uğrar ve bölünmüş oylar (split vote)
    /// kendini tekrarlayabilir (§5.2). Seed'i türetmek sürücünün işidir (simülatörde `SeedTree`).
    /// Bir RNG nesnesi değil `[u8; 32]` alınır: genel API rand türlerine bağlanmaz.
    #[must_use]
    pub fn new(id: NodeId, mut peers: BTreeSet<NodeId>, config: Config, seed: [u8; 32]) -> Self {
        peers.remove(&id);
        Self::boot(
            id,
            peers,
            config,
            ChaCha8Rng::from_seed(seed),
            PersistentState::default(),
        )
    }

    /// Açılış: verilen kalıcı durumla, bütün geçici durumu sıfırdan kurulmuş bir Follower. Hem ilk
    /// açılış (`new`, boş disk) hem yeniden başlatma (`Restart`) buradan geçer.
    fn boot(
        id: NodeId,
        peers: BTreeSet<NodeId>,
        config: Config,
        mut rng: ChaCha8Rng,
        state: PersistentState,
    ) -> Self {
        let election_timeout = draw_election_timeout(&mut rng, config);
        let PersistentState {
            current_term,
            voted_for,
            log,
        } = state;
        Self {
            id,
            peers,
            config,
            rng,
            current_term,
            voted_for,
            log: Log::new(log),
            role: Role::Follower,
            votes: BTreeSet::new(),
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
            progress: BTreeMap::new(),
        }
    }

    /// Kurucuya verilen düğüm kimliğini döndürür (N2).
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Normalize edilmiş (kendisi çıkarılmış, sıralı) eş kümesini döndürür (N2).
    #[must_use]
    pub fn peers(&self) -> &BTreeSet<NodeId> {
        &self.peers
    }

    /// Kurucuya verilen ayarlar (N2).
    #[must_use]
    pub fn config(&self) -> Config {
        self.config
    }

    /// Düğümün şu anki rolü.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// Düğümün gördüğü en son term (Figure 2: `currentTerm`).
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.current_term
    }

    /// Şu anki term'de oy verilen aday (Figure 2: `votedFor`).
    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Log girdileri; dilimdeki 0. konum 1. index'tir.
    #[must_use]
    pub fn log(&self) -> &[LogEntry] {
        self.log.entries()
    }

    /// Commit edildiği bilinen en yüksek index (Figure 2: `commitIndex`).
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Durum makinesine uygulanmış en yüksek index (Figure 2: `lastApplied`).
    #[must_use]
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Düğümün bellekteki kalıcı durumu: diskte bulunması GEREKEN değer. Sürücü, diskine
    /// uyguladığı farkların toplamının bununla aynı olduğunu denetleyerek "değişti ama persist
    /// edilmedi" hatalarını hemen yakalayabilir. Log'u kopyaladığı için maliyeti log boyutuyla
    /// orantılıdır; sık denetimlerde `current_term`, `voted_for` ve `log` erişimcileri yeterlidir.
    #[must_use]
    pub fn persistent_state(&self) -> PersistentState {
        PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.entries().to_vec(),
        }
    }

    /// Bir girdiyi işler, sürücünün SIRAYLA yürütmesi gereken çıktı listesini döndürür.
    ///
    /// `#[must_use]`: dönen listenin tamamen yok sayılması (hiçbir mesaj gitmez, hiçbir durum
    /// persist edilmez) neredeyse her zaman bir sürücü hatasıdır ve derleyici bunu uyarır. Listeyi
    /// sırasız ya da eksik yürütmek (O1 ihlali) ise derleyicinin göremeyeceği, sürücünün
    /// sorumluluğundaki bir hatadır.
    ///
    /// Çıktıların sırası: varsa tek bir `Persist`, sonra `Send`'ler, en sonda `Apply`'lar.
    ///
    /// N3: `step` tam (total) bir fonksiyondur: her `Input` değeri için panik atmadan döner.
    /// Simülatör her düğümü her tick'te adımlar; tek bir panik bütün koşuyu ve determinizm
    /// testlerini çökertirdi. Bu yüzden imkânsız sayılan durumlar bile (ör. term uzayının sonu,
    /// aynı term'de ikinci bir lider) panikle değil güvenli bir cevapla ele alınır.
    #[must_use = "outputs must be executed in order: Persist before the Sends that depend on it"]
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        // O2: term ve oyun adımdan önceki hâli. Adım sonunda değişmişse çıktıların BAŞINA tek bir
        // `Persist` konur. Bu "önce/sonra" karşılaştırması, durumu değiştiren her noktaya ayrı ayrı
        // persist eklemekten daha güvenlidir: yeni bir kod yolu durumu değiştirip persist'i
        // unutamaz. Log için aynı güvenceyi `Log`'un kendi değişiklik takibi verir.
        let before = (self.current_term, self.voted_for);
        let mut outbox = Outbox::new();
        match input {
            Input::Tick => self.on_tick(&mut outbox),
            Input::Message { from, msg } => self.on_message(from, msg, &mut outbox),
            Input::ClientRequest(command) => self.on_client_request(command, &mut outbox),
            Input::Restart(state) => {
                self.restart(state);
                // R1: yüklenen durum zaten diskteki durumdur; yeniden yazmaya gerek yok. Yeniden
                // başlayan düğüm Follower'dır ve gönderecek bir şeyi yoktur.
                return Vec::new();
            }
        }
        let log_update = self.log.take_update();
        let persist = if (self.current_term, self.voted_for) != before || log_update.is_some() {
            Some(Output::Persist(PersistUpdate {
                current_term: self.current_term,
                voted_for: self.voted_for,
                log: log_update,
            }))
        } else {
            None
        };
        let applies = self.apply_committed();
        persist
            .into_iter()
            .chain(outbox.into_iter().map(|(to, msg)| Output::Send { to, msg }))
            .chain(applies)
            .collect()
    }

    /// Mantıksal zaman bir tick ilerledi.
    fn on_tick(&mut self, outbox: &mut Outbox) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(1);
                if self.heartbeat_elapsed >= self.config.heartbeat_interval() {
                    self.broadcast_append_entries(outbox);
                }
            }
            // Figure 2: Follower, zaman aşımı boyunca mevcut liderden AppendEntries almaz ya da bir
            // adaya oy vermezse aday olur; Candidate zaman aşımında yeni bir seçim başlatır. İkisi
            // aynı kuraldır: süre doldu, yeni seçim.
            Role::Follower | Role::Candidate => {
                self.election_elapsed = self.election_elapsed.saturating_add(1);
                if self.election_elapsed >= self.election_timeout {
                    self.start_election(outbox);
                }
            }
        }
    }

    /// Seçim başlatır (Figure 2, "Candidates"): term'i artır, kendine oy ver, zamanlayıcıyı
    /// sıfırla, diğer bütün düğümlere `RequestVote` gönder.
    fn start_election(&mut self, outbox: &mut Outbox) {
        // Zamanlayıcı her durumda yeniden kurulur: bu seçim sonuçsuz kalırsa bir sonraki deneme
        // yeni bir rastgele süre sonra olur. Süre sabit kalsaydı, aynı anda aday olan iki düğüm
        // bölünmüş oyu her turda tekrarlayabilirdi (§5.2).
        self.reset_election_timer();
        let Some(term) = self.current_term.0.checked_add(1) else {
            // Term uzayı tükendi (2^64 seçim pratikte imkânsız, ama bir eşten keyfi büyük bir term
            // de gelebilir). Term'i artıramayan düğüm seçim başlatsaydı, oyunu belki başka birine
            // vermiş olduğu term'de kendine ikinci bir oy verirdi (E1 ihlali). Güvenlik canlılıktan
            // önce gelir: düğüm seçim başlatmadan bekler.
            return;
        };
        self.current_term = Term(term);
        // §5.2: aday önce kendine oy verir. Bu oy da kalıcıdır: çöküp aynı term'de kalkan aday,
        // başka bir adaya oy vermemelidir.
        self.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.votes = BTreeSet::from([self.id]);
        // Tek düğümlü küme: kendi oyu zaten çoğunluktur.
        if self.has_majority() {
            self.become_leader(outbox);
            return;
        }
        let request = RequestVote {
            term: self.current_term,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for &peer in &self.peers {
            outbox.push((peer, Message::RequestVote(request.clone())));
        }
    }

    /// Seçimi kazanan aday lider olur.
    fn become_leader(&mut self, outbox: &mut Outbox) {
        self.role = Role::Leader;
        self.votes.clear();
        // Figure 2 ("Volatile state on leaders", seçimden sonra yeniden kurulur): nextIndex = son
        // log index'i + 1, matchIndex = 0. Lider takipçilerin log'larını bilmez; iyimser başlar.
        // Tutarsızlık AppendEntries reddiyle ortaya çıkar ve nextIndex geri çekilir (§5.3).
        let next_index = self.log.last_index().next();
        self.progress = self
            .peers
            .iter()
            .map(|&peer| {
                let progress = Progress {
                    next_index,
                    match_index: LogIndex(0),
                };
                (peer, progress)
            })
            .collect();
        // §5.2: lider seçilir seçilmez AppendEntries (heartbeat) gönderir. Böylece aynı term'in
        // diğer adayları liderliği öğrenip Follower'a döner, takipçiler de yeni seçim başlatmaz.
        self.broadcast_append_entries(outbox);
    }

    /// Bütün eşlere AppendEntries gönderir ve heartbeat sayacını sıfırlar. Her mesaj, o takipçinin
    /// `nextIndex`'inden başlayan eksik girdileri taşır; eksik girdisi olmayan takipçiye giden
    /// mesaj, girdisiz bir heartbeat'tir (Figure 2, Leaders). Onaylanmamış girdiler böylece her
    /// heartbeat'te yeniden gönderilir: kaybolan mesajlar için ayrı bir yeniden gönderim
    /// zamanlayıcısı gerekmez.
    fn broadcast_append_entries(&mut self, outbox: &mut Outbox) {
        self.heartbeat_elapsed = 0;
        for &peer in &self.peers {
            outbox.push((peer, self.append_entries_for(peer)));
        }
    }

    /// `peer` için bir AppendEntries: `nextIndex`'ten başlayan en fazla `max_entries` girdi, hemen
    /// öncesindeki girdinin index'i ve term'i (tutarlılık denetimi için) ve liderin
    /// `commitIndex`'i.
    fn append_entries_for(&self, peer: NodeId) -> Message {
        let next_index = self
            .progress
            .get(&peer)
            .map_or(self.log.last_index().next(), |progress| progress.next_index);
        let prev_log_index = next_index.prev();
        Message::AppendEntries(AppendEntries {
            term: self.current_term,
            prev_log_index,
            // `nextIndex` hiçbir zaman son index + 1'i aşmaz, yani önceki girdi her zaman
            // log'dadır. Olmasaydı (bir hata), term 0 gönderilir: takipçi büyük olasılıkla reddeder
            // ve lider geri çekilir; panik atılmaz (N3).
            prev_log_term: self.log.term_at(prev_log_index).unwrap_or(Term(0)),
            entries: self.log.entries_from(next_index, self.config.max_entries()),
            leader_commit: self.commit_index,
        })
    }

    /// İstemciden bir komut geldi.
    fn on_client_request(&mut self, command: Command, outbox: &mut Outbox) {
        // Komutu yalnızca lider kabul eder (Figure 2, Leaders: "If command received from client:
        // append entry to local log"). Lider olmayan düğüm isteği yok sayar; liderin kim olduğunu
        // bildiren `NotLeader` cevabı Faz 4'te gelecek.
        if self.role != Role::Leader {
            return;
        }
        self.log.append(LogEntry {
            term: self.current_term,
            command,
        });
        // Tek düğümlü küme: girdi liderin kendi kopyasıyla zaten çoğunluktadır.
        self.advance_commit_index();
        // Girdiyi bir sonraki heartbeat'i beklemeden gönder; bu gönderim heartbeat yerine de geçer.
        self.broadcast_append_entries(outbox);
    }

    /// Bir eşten gelen RPC'yi ya da cevabı işler.
    fn on_message(&mut self, from: NodeId, msg: Message, outbox: &mut Outbox) {
        // Yapılandırmada olmayan bir göndericiden (kendisi dahil) gelen mesaj yok sayılır. Böyle
        // bir düğümün oyu sayılsaydı, aday gerçek çoğunluk olmadan lider olabilirdi; taşıdığı term
        // benimsenseydi küme dışından biri liderleri devirebilirdi. Üyelik değişikliği (§6) Faz
        // 6'nın kapsamıdır.
        if !self.peers.contains(&from) {
            return;
        }
        // T1, Figure 2 ("All Servers"): RPC isteği ya da cevabı daha yüksek bir term taşıyorsa,
        // currentTerm o term olur ve düğüm Follower'a döner (§5.1).
        let term = msg.term();
        if term > self.current_term {
            self.become_follower(term);
        }
        match msg {
            Message::RequestVote(request) => self.on_request_vote(from, &request, outbox),
            Message::RequestVoteResponse(response) => {
                self.on_vote_response(from, &response, outbox)
            }
            Message::AppendEntries(request) => self.on_append_entries(from, request, outbox),
            Message::AppendEntriesResponse(response) => {
                self.on_append_entries_response(from, &response, outbox)
            }
        }
    }

    /// Daha yüksek bir term görüldü: o term'e geç ve Follower ol.
    fn become_follower(&mut self, term: Term) {
        let was_leader = self.role == Role::Leader;
        self.current_term = term;
        // Yeni term'de henüz kimseye oy verilmedi.
        self.voted_for = None;
        self.role = Role::Follower;
        self.votes.clear();
        // Liderlik bitti: takipçi ilerlemesi yalnızca lider olunan term için anlamlıdır.
        self.progress.clear();
        // Seçim zamanlayıcısını Figure 2'ye göre yalnızca üç şey sıfırlar: mevcut liderden
        // AppendEntries almak, bir adaya oy vermek ve seçim başlatmak. Yüksek term'i görmek tek
        // başına bunlardan biri değildir; bu yüzden Follower ve Candidate'ın zamanlayıcısına
        // dokunulmaz. Lider ise zamanlayıcı işletmez: liderlikten düşen düğüm için yeni bir süre
        // çekilir, yoksa sayaç liderlikten önceki eski değerinden devam ederdi.
        if was_leader {
            self.reset_election_timer();
        }
    }

    /// `RequestVote` alıcısı (Figure 2).
    fn on_request_vote(&mut self, from: NodeId, request: &RequestVote, outbox: &mut Outbox) {
        // Daha yüksek term `on_message`'da zaten benimsendi; burada istek ya bizim term'imizde ya
        // da eskidir. Oy ancak şu üç koşul birlikte sağlanırsa verilir:
        // 1. §5.1: eski term'li isteğe oy yok.
        // 2. E1, §5.2: bu term'de ya hiç oy verilmedi ya da zaten bu adaya verildi (cevabı kaybolan
        //    adayın tekrarlanan isteği yine "evet" almalı).
        // 3. §5.4.1: adayın log'u en az bizimki kadar güncel (seçim kısıtı).
        let vote_granted = request.term == self.current_term
            && self.voted_for.is_none_or(|candidate| candidate == from)
            && candidate_is_up_to_date(
                (request.last_log_term, request.last_log_index),
                (self.log.last_term(), self.log.last_index()),
            );
        if vote_granted {
            self.voted_for = Some(from);
            // Figure 2: oy vermek seçim zamanlayıcısını sıfırlar; yeni bir adaya fırsat tanınır.
            self.reset_election_timer();
        }
        outbox.push((
            from,
            Message::RequestVoteResponse(RequestVoteResponse {
                term: self.current_term,
                vote_granted,
            }),
        ));
    }

    /// `RequestVote` cevabını işler (Figure 2, "Candidates").
    fn on_vote_response(
        &mut self,
        from: NodeId,
        response: &RequestVoteResponse,
        outbox: &mut Outbox,
    ) {
        // E2: yalnızca HÂLÂ aday olduğumuz term'e ait olumlu cevaplar sayılır. Eski bir seçimin
        // geciken (ya da ağda çoğaltılmış) cevabı yeni seçimde sayılsaydı, aday bu term'de gerçek
        // bir çoğunluk olmadan lider olabilirdi.
        if self.role != Role::Candidate
            || response.term != self.current_term
            || !response.vote_granted
        {
            return;
        }
        // Küme: aynı düğümün ikinci cevabı oyu artırmaz.
        self.votes.insert(from);
        if self.has_majority() {
            self.become_leader(outbox);
        }
    }

    /// `AppendEntries` alıcısı (Figure 2): isteği işler ve cevabını gönderir.
    fn on_append_entries(&mut self, from: NodeId, request: AppendEntries, outbox: &mut Outbox) {
        let (success, match_index) = self.accept_append_entries(request);
        outbox.push((
            from,
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term,
                success,
                match_index,
            }),
        ));
    }

    /// AppendEntries'i Figure 2'nin beş kuralıyla uygular; cevabın `success` ve `match_index`
    /// alanlarını döndürür.
    fn accept_append_entries(&mut self, request: AppendEntries) -> (bool, LogIndex) {
        // Madde 1, §5.1: eski term'li bir liderin isteği reddedilir. Cevaptaki güncel term
        // sayesinde eski lider geride kaldığını öğrenip Follower'a döner; ipucu anlamsızdır.
        if request.term < self.current_term {
            return (false, LogIndex(0));
        }
        if self.role == Role::Leader {
            // Buradan sonra istek bizim term'imizdedir (daha yükseği `on_message`'da benimsendi).
            // Aynı term'de ikinci bir lider Election Safety'ye göre imkânsızdır. Olursa (ör. bir
            // hata), lider isteği reddeder ve liderliği bırakmaz: rolü yalnızca daha yüksek bir
            // term değiştirir. İhlali raporlamak denetçinin (checker) işidir; çekirdek panik atmaz
            // (N3).
            return (false, LogIndex(0));
        }
        // §5.2: aday, aynı term'de seçilmiş bir liderden AppendEntries alırsa onun liderliğini
        // tanır ve Follower'a döner. `votedFor` değişmez: bu term'deki oy zaten kullanıldı.
        self.role = Role::Follower;
        self.votes.clear();
        // Figure 2: mevcut liderden AppendEntries almak seçim zamanlayıcısını sıfırlar.
        self.reset_election_timer();

        let AppendEntries {
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            ..
        } = request;
        // Madde 2: log'da `prev_log_index`'te term'i `prev_log_term` olan bir girdi yoksa reddet.
        // Log Matching'in tümevarım adımı budur: kabul edilen her girdinin öncesi liderinkiyle
        // aynıdır. İpucu, eşleşmenin olabileceği en büyük index'tir: log'umuz kısaysa son
        // index'imiz, değilse `prev_log_index`'in bir öncesi.
        if self.log.term_at(prev_log_index) != Some(prev_log_term) {
            let hint = self.log.last_index().min(prev_log_index.prev());
            return (false, hint);
        }
        // Madde 3 ve 4: girdileri tek tek karşılaştır. Aynı index'te farklı term'li bir girdi
        // (çakışma) varsa o girdi ve sonrası silinir ve yenileri eklenir; log'da zaten aynısı olan
        // girdilere dokunulmaz. ÇAKIŞMA YOKSA HİÇBİR ŞEY SİLİNMEZ: gecikmiş ya da çoğaltılmış eski
        // bir istek (örneğin yalnızca ilk girdiyi taşıyan) log'u o isteğin boyuna kısaltsaydı,
        // takipçinin zaten onayladığı (belki commit edilmiş) girdiler kaybolurdu.
        let mut index = prev_log_index;
        for entry in entries {
            index = index.next();
            match self.log.term_at(index) {
                Some(term) if term == entry.term => {}
                Some(_) => {
                    self.log.truncate_from(index);
                    self.log.append(entry);
                }
                None => self.log.append(entry),
            }
        }
        // `index` artık bu isteğin doğruladığı son girdidir (`prev_log_index` + girdi sayısı);
        // log'da ondan sonra gelen girdiler bu istekle doğrulanmamıştır.
        //
        // Madde 5: commitIndex = min(leaderCommit, son yeni girdinin index'i). Doğrulanmamış
        // girdiler bu yüzden commit edilmez. `max` ile yalnızca ileri gidilir: sırası değişmiş eski
        // bir istek daha kısa bir önek taşıyabilir ve commitIndex'i geri çekmemelidir.
        if leader_commit > self.commit_index {
            self.commit_index = self.commit_index.max(leader_commit.min(index));
        }
        (true, index)
    }

    /// `AppendEntries` cevabını işler (Figure 2, Leaders).
    fn on_append_entries_response(
        &mut self,
        from: NodeId,
        response: &AppendEntriesResponse,
        outbox: &mut Outbox,
    ) {
        // Yalnızca bu term'in lideri cevapları işler; başka bir term'in (geciken ya da çoğaltılmış)
        // cevabı başka bir liderliğe aittir. Daha yüksek term `on_message`'da zaten işlendi.
        if self.role != Role::Leader || response.term != self.current_term {
            return;
        }
        let last_index = self.log.last_index();
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        if response.success {
            // Eşleşme noktası yalnızca ileri gider: sırası değişmiş ya da çoğaltılmış eski bir
            // başarı cevabı ilerlemeyi geri almaz. Liderin log'unun ötesini gösteren bir cevap
            // (imkânsız) log'un sonuna kırpılır.
            let matched = response.match_index.min(last_index);
            progress.match_index = progress.match_index.max(matched);
            progress.next_index = progress.next_index.max(matched.next());
            let lagging = progress.next_index <= last_index;
            self.advance_commit_index();
            // Takipçide hâlâ eksik girdi varsa bir sonraki heartbeat'i beklemeden devam et.
            if lagging {
                outbox.push((from, self.append_entries_for(from)));
            }
        } else {
            // §5.3: tutarsızlık → nextIndex'i geri çek ve hemen yeniden dene. İpucu sayesinde birer
            // birer değil bir hamlede geri çekilir. Ama asla matchIndex + 1'in altına inilmez
            // (orası eşleştiği bilinen kısım) ve asla ileri gidilmez: eski bir ret cevabı
            // nextIndex'i büyütmemeli.
            let floor = progress.match_index.next();
            let backed_off = progress.next_index.prev().min(response.match_index.next());
            progress.next_index = floor.max(backed_off);
            outbox.push((from, self.append_entries_for(from)));
        }
    }

    /// Liderin commitIndex'ini ilerletir (Figure 2, Leaders; §5.3, §5.4.2).
    fn advance_commit_index(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        // N > commitIndex, çoğunluğun matchIndex'i ≥ N ve log[N].term == currentTerm ise
        // commitIndex = N. En büyük N'den aşağı doğru aranır; ilk uyan N commit edilir ve ondan
        // önceki bütün girdiler de dolaylı olarak commit olur (Log Matching).
        let mut candidate = self.log.last_index();
        while candidate > self.commit_index {
            match self.log.term_at(candidate) {
                Some(term) if term == self.current_term => {
                    if self.replicated_on_majority(candidate) {
                        self.commit_index = candidate;
                        return;
                    }
                }
                // §5.4.2: önceki term'lerden gelen bir girdi, kopyaları sayılarak commit EDİLEMEZ
                // (Figure 8): çoğunlukta bulunsa bile daha güncel log'lu bir aday onu ezebilir.
                // Log'da term'ler index'le birlikte azalmadığı için buradan aşağısı da önceki
                // term'lerdendir; aramaya devam etmek boşunadır. Bu girdiler, kendi term'imizden
                // bir girdi commit edilince dolaylı olarak commit olur.
                _ => return,
            }
            candidate = candidate.prev();
        }
    }

    /// `index`'e kadar olan girdiler kümenin çoğunluğunda (lider dahil) var mı?
    fn replicated_on_majority(&self, index: LogIndex) -> bool {
        // Lider kendini de sayar. Girdisi, onu taşıyan AppendEntries'ten önce persist edildi (O1)
        // ve sürücü o mesajları yazma kalıcı olana kadar göndermez; yani hiçbir takipçinin onayı
        // liderin kendi kopyası diske ulaşmadan gelemez.
        //
        // Tek düğümlü bir kümede beklenecek onay yoktur: lider girdiyi eklediği adımda, kopyası
        // henüz diske ulaşmadan commitIndex'ini ilerletir. Bu da güvenlidir. Commit'in dışarıya
        // görünen tek etkisi `Apply`'dır; o, aynı adımın `Persist`'inden sonra gelir (O1) ve
        // sürücü onu yazma kalıcı olana kadar tutar. Düğüm arada çökerse girdi hiç commit
        // edilmemiş olur ve dışarıdan hiçbir iz kalmaz.
        let replicas = 1 + self
            .progress
            .values()
            .filter(|progress| progress.match_index >= index)
            .count();
        replicas >= self.quorum()
    }

    /// Commit edilmiş ama henüz uygulanmamış girdileri, index sırasıyla `Apply` çıktılarına çevirir
    /// (Figure 2, All Servers: "If commitIndex > lastApplied: increment lastApplied, apply
    /// log[lastApplied] to state machine").
    fn apply_committed(&mut self) -> Vec<Output> {
        let mut applies = Vec::new();
        while self.last_applied < self.commit_index {
            let index = self.last_applied.next();
            // commitIndex log'un sonunu aşamaz: lider kendi log'undan, takipçi bu istekte
            // doğrulanan son girdiye kadar commit eder. Aşsaydı (bir hata) girdi uydurulmaz,
            // uygulama orada durur ve denetçi eksik uygulamayı görür (N3: panik yok).
            let Some(entry) = self.log.entry(index) else {
                break;
            };
            applies.push(Output::Apply {
                index,
                command: entry.command.clone(),
            });
            self.last_applied = index;
        }
        applies
    }

    /// Çökme sonrası yeniden başlatma (R1).
    fn restart(&mut self, state: PersistentState) {
        // Figure 2: çökme bütün geçici durumu siler; yalnızca diskteki durum kalır. Düğüm alan alan
        // sıfırlanmak yerine `boot` ile baştan kurulur: geçici bir alanın (commitIndex,
        // lastApplied, takipçi ilerlemesi, ...) sıfırlanması böylece unutulamaz.
        //
        // RNG bilerek sürdürülür (yeniden tohumlanmaz): gerçek bir düğüm açılışta taze rastgelelik
        // alır; aynı akışın devamı da taze ama deterministik değerler verir. Baştaki seed'le
        // yeniden kurulsaydı, düğüm ilk açılışındaki zaman aşımı dizisini birebir tekrarlardı.
        let peers = std::mem::take(&mut self.peers);
        *self = Self::boot(self.id, peers, self.config, self.rng.clone(), state);
    }

    /// Seçim zamanlayıcısını sıfırlar ve bu tur için yeni bir zaman aşımı çeker.
    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = draw_election_timeout(&mut self.rng, self.config);
    }

    /// Çoğunluk için gereken oy sayısı: küme boyutunun yarısından fazlası.
    fn quorum(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }

    /// Adaylıkta toplanan oylar çoğunluğa ulaştı mı?
    fn has_majority(&self) -> bool {
        self.votes.len() >= self.quorum()
    }
}

/// Seçim zaman aşımını `[T, 2T)` aralığından çeker (§5.2). TAM OLARAK bir `next_u64` tüketir.
///
/// Neden tek çekiliş ve mod: hazır örnekleme algoritmaları (ör. `rand` crate'inin `gen_range`'i)
/// reddetme (rejection) döngüsü kullanır ve sürümden sürüme değişebilir; o zaman aynı seed farklı
/// zaman aşımları üretirdi. Mod işleminin sapması `T / 2^64` mertebesindedir, ihmal edilebilir.
fn draw_election_timeout(rng: &mut ChaCha8Rng, config: Config) -> u64 {
    let base = config.election_timeout();
    // `Config` T ≥ 2'yi garanti eder (heartbeat ≥ 1 ve heartbeat < T). `checked_rem` yine de sıfıra
    // bölme paniğini yapısal olarak imkânsız kılar (N3).
    let jitter = rng.next_u64().checked_rem(base).unwrap_or(0);
    // T > 2^63 gibi anlamsız bir ayarda toplam taşmaz: doygun toplama sonucu aralığın u64'e sığan
    // kısmında tutar.
    base.saturating_add(jitter)
}

/// §5.4.1 seçim kısıtı: adayın log'u en az bizimki kadar güncel mi?
///
/// "Daha güncel" şöyle tanımlanır: son girdilerin term'leri farklıysa daha yüksek term'li log daha
/// günceldir; term'ler eşitse daha uzun log daha günceldir. Bu tanım tam olarak `(son term, son
/// index)` çiftlerinin sözlük sırasıyla (önce term, sonra index) karşılaştırılmasıdır. Kısıt,
/// commit edilmiş her girdinin gelecekteki liderlerin log'unda bulunmasını sağlar (Leader
/// Completeness): eksik log'lu bir aday çoğunluğun oyunu alamaz.
fn candidate_is_up_to_date(candidate_last: (Term, LogIndex), own_last: (Term, LogIndex)) -> bool {
    candidate_last >= own_last
}

#[cfg(test)]
mod tests {
    use super::{RaftNode, candidate_is_up_to_date};
    use crate::config::Config;
    use crate::input::Input;
    use crate::log::LogEntry;
    use crate::message::{
        AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse,
    };
    use crate::output::Output;
    use crate::persist::{LogUpdate, PersistUpdate, PersistentState};
    use crate::role::Role;
    use crate::types::{Command, LogIndex, NodeId, Term};
    use rand_chacha::ChaCha8Rng;
    use rand_chacha::rand_core::{Rng, SeedableRng};
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;

    /// Varsayılan seçim zaman aşımı tabanı (`Config::default()`).
    const T: u64 = 20;
    /// Varsayılan heartbeat aralığı (`Config::default()`).
    const H: u64 = 4;

    fn ids(list: &[u64]) -> BTreeSet<NodeId> {
        list.iter().copied().map(NodeId).collect()
    }

    /// Bir sayıdan 32 baytlık seed (ilk 8 bayt little-endian, gerisi sıfır).
    fn seed_bytes(seed: u64) -> [u8; 32] {
        let mut key = [0_u8; 32];
        key[..8].copy_from_slice(&seed.to_le_bytes());
        key
    }

    /// `cluster` kimlikli kümenin `id` düğümü; `seed` zaman aşımı akışını seçer.
    fn node(id: u64, cluster: &[u64], seed: u64) -> RaftNode {
        RaftNode::new(
            NodeId(id),
            ids(cluster),
            Config::default(),
            seed_bytes(seed),
        )
    }

    fn message(from: u64, msg: Message) -> Input {
        Input::Message {
            from: NodeId(from),
            msg,
        }
    }

    fn request_vote(term: u64) -> Message {
        Message::RequestVote(RequestVote {
            term: Term(term),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        })
    }

    fn vote(term: u64, vote_granted: bool) -> Message {
        Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(term),
            vote_granted,
        })
    }

    /// Tek baytlık komutlu bir log girdisi.
    fn entry(term: u64, byte: u8) -> LogEntry {
        LogEntry {
            term: Term(term),
            command: Command::new(vec![byte]),
        }
    }

    fn append(term: u64, prev: (u64, u64), entries: Vec<LogEntry>, leader_commit: u64) -> Message {
        Message::AppendEntries(AppendEntries {
            term: Term(term),
            prev_log_index: LogIndex(prev.0),
            prev_log_term: Term(prev.1),
            entries,
            leader_commit: LogIndex(leader_commit),
        })
    }

    /// Boş log'a gönderilen girdisiz AppendEntries (önceki girdi yok, commit yok).
    fn heartbeat(term: u64) -> Message {
        append(term, (0, 0), Vec::new(), 0)
    }

    fn append_reply(term: u64, success: bool, match_index: u64) -> Message {
        Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(term),
            success,
            match_index: LogIndex(match_index),
        })
    }

    /// Boş log'lu bir takipçinin heartbeat cevabı (eşleşme index'i 0).
    fn heartbeat_reply(term: u64, success: bool) -> Message {
        append_reply(term, success, 0)
    }

    /// Yalnızca term/oy değişen bir adımın yazması.
    fn persist(term: u64, voted_for: Option<u64>) -> Output {
        Output::Persist(PersistUpdate {
            current_term: Term(term),
            voted_for: voted_for.map(NodeId),
            log: None,
        })
    }

    /// Log'u da değişen bir adımın yazması: `from` ve sonrası `entries` ile değişir.
    fn persist_log(term: u64, voted_for: Option<u64>, from: u64, entries: Vec<LogEntry>) -> Output {
        Output::Persist(PersistUpdate {
            current_term: Term(term),
            voted_for: voted_for.map(NodeId),
            log: Some(LogUpdate {
                from: LogIndex(from),
                entries,
            }),
        })
    }

    fn apply(index: u64, byte: u8) -> Output {
        Output::Apply {
            index: LogIndex(index),
            command: Command::new(vec![byte]),
        }
    }

    fn client(byte: u8) -> Input {
        Input::ClientRequest(Command::new(vec![byte]))
    }

    /// Diskinde `state` olan bir düğüm (yeni düğüm + yeniden başlatma).
    fn node_with_state(id: u64, cluster: &[u64], seed: u64, state: PersistentState) -> RaftNode {
        let mut node = node(id, cluster, seed);
        assert!(node.step(Input::Restart(state)).is_empty());
        node
    }

    /// Log'u `log` olan (term'i son girdinin term'i) bir düğümün bir sonraki term'de lider olmuş
    /// hâli: zaman aşımında aday olur ve eşlerinden çoğunluğa yetecek kadar oy alır.
    fn leader_with_log(id: u64, cluster: &[u64], seed: u64, log: Vec<LogEntry>) -> RaftNode {
        let term = log.last().map_or(0, |entry| entry.term.0);
        let state = PersistentState {
            current_term: Term(term),
            voted_for: None,
            log,
        };
        let mut node = node_with_state(id, cluster, seed, state);
        let _ = tick_until_output(&mut node);
        for &peer in cluster.iter().filter(|&&peer| peer != id) {
            if node.role() == Role::Leader {
                break;
            }
            let _ = node.step(message(peer, vote(term + 1, true)));
        }
        assert_eq!(node.role(), Role::Leader);
        node
    }

    fn send(to: u64, msg: Message) -> Output {
        Output::Send {
            to: NodeId(to),
            msg,
        }
    }

    /// Düğümü bir çıktı üretene kadar tick'ler; geçen tick sayısını ve çıktıları döndürür.
    fn tick_until_output(node: &mut RaftNode) -> (u64, Vec<Output>) {
        for ticks in 1..=4 * T {
            let outputs = node.step(Input::Tick);
            if !outputs.is_empty() {
                return (ticks, outputs);
            }
        }
        panic!("no output within {} ticks", 4 * T);
    }

    /// Term 1'de seçim başlatmış bir aday.
    fn candidate(id: u64, cluster: &[u64], seed: u64) -> RaftNode {
        let mut node = node(id, cluster, seed);
        let _ = tick_until_output(&mut node);
        assert_eq!(node.role(), Role::Candidate);
        node
    }

    /// Term 1'de seçilmiş bir lider: aday, eşlerinden çoğunluğa yetecek kadar oy alır.
    fn leader(id: u64, cluster: &[u64], seed: u64) -> RaftNode {
        let mut node = candidate(id, cluster, seed);
        for &peer in cluster.iter().filter(|&&peer| peer != id) {
            if node.role() == Role::Leader {
                break;
            }
            let _ = node.step(message(peer, vote(1, true)));
        }
        assert_eq!(node.role(), Role::Leader);
        node
    }

    // N1: kurucu, `id`'yi `peers` kümesinden çıkarır; aksi hâlde çoğunluk sayımı
    // (`peers().len() + 1`) kendini iki kez sayardı.
    #[test]
    fn new_removes_self_from_peers() {
        let node = node(1, &[1, 2, 3], 0);
        assert_eq!(node.peers(), &ids(&[2, 3]));
    }

    // N2: erişimciler kurucuya verilen değerleri (normalize edilmiş hâliyle) birebir yansıtır. Taze
    // düğüm, boş diskle açılmış bir Follower'dır.
    #[test]
    fn accessors_return_constructor_values() {
        let config = Config::new(10, 3).expect("valid config");
        let node = RaftNode::new(NodeId(1), ids(&[2, 3]), config, [0; 32]);
        assert_eq!(node.id(), NodeId(1));
        assert_eq!(node.peers(), &ids(&[2, 3]));
        assert_eq!(node.config(), config);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), Term(0));
        assert_eq!(node.voted_for(), None);
        assert!(node.log().is_empty());
        assert_eq!(node.commit_index(), LogIndex(0));
        assert_eq!(node.last_applied(), LogIndex(0));
        assert_eq!(node.persistent_state(), PersistentState::default());
    }

    // §5.2: seçim zaman aşımı [T, 2T) aralığından rastgele çekilir. 500 seed'de ilk seçim hep bu
    // aralıkta başlar ve aralığın her değeri en az bir kez görülür (dağılım aralığın tamamını
    // kaplar). Aynı seed aynı zaman aşımını verir.
    #[test]
    fn the_election_timeout_is_drawn_from_t_to_2t() {
        let mut seen = BTreeSet::new();
        for seed in 0..500 {
            let mut follower = node(1, &[1, 2, 3], seed);
            let (ticks, _) = tick_until_output(&mut follower);
            assert!(
                (T..2 * T).contains(&ticks),
                "seed {seed}: the election started after {ticks} ticks"
            );
            seen.insert(ticks);
        }
        assert_eq!(seen, (T..2 * T).collect::<BTreeSet<_>>());
        let (first, _) = tick_until_output(&mut node(1, &[1, 2, 3], 7));
        let (second, _) = tick_until_output(&mut node(1, &[1, 2, 3], 7));
        assert_eq!(first, second, "the same seed gives the same timeout");
    }

    // §5.2: zaman aşımı her sıfırlamada yeniden çekilir; bu test seçim başlangıcındaki sıfırlamayı
    // sınar (oy verme, heartbeat ve liderlikten düşme sıfırlamaları ayrı testlerde). Art arda
    // cevapsız kalan seçimlerin her biri [T, 2T) sonra yeniden başlar ve süreler turdan tura
    // değişir. Süre sabit kalsaydı, aynı anda aday olan iki düğüm bölünmüş oyu her turda
    // tekrarlayabilirdi.
    #[test]
    fn each_new_election_redraws_the_timeout() {
        let mut node = node(1, &[1, 2, 3], 3);
        let mut timeouts = BTreeSet::new();
        for round in 1..=30 {
            let (ticks, outputs) = tick_until_output(&mut node);
            assert!((T..2 * T).contains(&ticks));
            timeouts.insert(ticks);
            // Her tur yeni bir seçimdir: term bir artar ve düğüm yine kendine oy verir.
            assert_eq!(node.current_term(), Term(round));
            assert_eq!(outputs[0], persist(round, Some(1)));
        }
        assert!(
            timeouts.len() > 1,
            "timeouts must vary between rounds: {timeouts:?}"
        );
    }

    // Figure 2: bir adaya oy vermek ve mevcut liderden AppendEntries almak seçim zamanlayıcısını
    // sıfırlar. T-1 tick'te bir gelen heartbeat'ler varken düğüm hiç seçim başlatmaz; lider
    // susunca [T, 2T) içinde başlatır.
    #[test]
    fn granted_votes_and_heartbeats_reset_the_election_timer() {
        let mut follower = node(1, &[1, 2, 3], 5);
        for _ in 0..T - 1 {
            assert!(follower.step(Input::Tick).is_empty());
        }
        assert_eq!(
            follower.step(message(2, request_vote(1))),
            vec![persist(1, Some(2)), send(2, vote(1, true))]
        );
        for _ in 0..50 {
            for _ in 0..T - 1 {
                assert!(
                    follower.step(Input::Tick).is_empty(),
                    "no election while the leader is heard from"
                );
            }
            assert_eq!(
                follower.step(message(2, heartbeat(1))),
                vec![send(2, heartbeat_reply(1, true))]
            );
        }
        let (ticks, _) = tick_until_output(&mut follower);
        assert!((T..2 * T).contains(&ticks));
        assert_eq!(follower.role(), Role::Candidate);
    }

    // O1/O2 ve Figure 2 (Candidates): zaman aşımında düğüm term'i artırır ve kendine oy verir; bu
    // ikisi, oy istekleri ağa çıkmadan ÖNCE diske yazdırılır. İstekler adayın son log girdisini
    // taşır (burada log boş: 0/0).
    #[test]
    fn a_timeout_starts_an_election_and_persists_before_sending() {
        let mut node = node(1, &[1, 2, 3], 1);
        let (_, outputs) = tick_until_output(&mut node);
        assert_eq!(
            outputs,
            vec![
                persist(1, Some(1)),
                send(2, request_vote(1)),
                send(3, request_vote(1))
            ]
        );
        assert_eq!(node.role(), Role::Candidate);
    }

    // §5.2: çoğunluğun (3 düğümde 2) oyunu alan aday lider olur ve hemen heartbeat gönderir. Term
    // ve oy değişmediği için bu adımda `Persist` yoktur (O2).
    #[test]
    fn a_majority_of_votes_makes_a_leader_that_sends_heartbeats_at_once() {
        let mut node = candidate(1, &[1, 2, 3], 1);
        assert_eq!(
            node.step(message(2, vote(1, true))),
            vec![send(2, heartbeat(1)), send(3, heartbeat(1))]
        );
        assert_eq!(node.role(), Role::Leader);
    }

    // E2: oylar küme olarak sayılır. 5 düğümde çoğunluk 3'tür: aynı düğümün (ağda çoğaltılmış) oyu
    // iki kez gelse de adayın oyu 2'de kalır; reddeden cevaplar sayılmaz. Üçüncü farklı oy
    // liderlik getirir.
    #[test]
    fn duplicate_and_negative_votes_do_not_count() {
        let mut node = candidate(1, &[1, 2, 3, 4, 5], 2);
        assert!(node.step(message(2, vote(1, true))).is_empty());
        assert!(node.step(message(2, vote(1, true))).is_empty());
        assert!(node.step(message(3, vote(1, false))).is_empty());
        assert_eq!(node.role(), Role::Candidate);
        let _ = node.step(message(4, vote(1, true)));
        assert_eq!(node.role(), Role::Leader);
    }

    // E2: eski bir seçimin geciken olumlu cevapları yeni seçimde sayılmaz. Term 1'deki adaylık
    // cevapsız kalır ve düğüm term 2'de yeniden aday olur; term 1'e ait iki olumlu oy onu lider
    // yapmaz.
    #[test]
    fn votes_from_an_earlier_term_are_ignored() {
        let mut node = candidate(1, &[1, 2, 3, 4, 5], 4);
        let _ = tick_until_output(&mut node);
        assert_eq!(node.current_term(), Term(2));
        assert!(node.step(message(2, vote(1, true))).is_empty());
        assert!(node.step(message(3, vote(1, true))).is_empty());
        assert_eq!(node.role(), Role::Candidate);
    }

    // Yapılandırmada olmayan göndericiler (düğümün kendisi dahil) yok sayılır: ne oyları sayılır ne
    // de taşıdıkları daha yüksek term benimsenir. Düğüm birebir aynı kalır.
    #[test]
    fn messages_from_outside_the_configuration_are_ignored() {
        let mut node = candidate(1, &[1, 2, 3], 6);
        let before = node.clone();
        for from in [1, 9] {
            assert!(node.step(message(from, vote(1, true))).is_empty());
            assert!(node.step(message(from, request_vote(7))).is_empty());
            assert!(node.step(message(from, heartbeat(7))).is_empty());
        }
        assert_eq!(node, before);
    }

    // E1: bir term'de tek oy. 2'ye oy veren düğüm aynı term'de 3'e oy vermez. 2'nin tekrarlanan
    // isteğine yine "evet" der (ilk cevap kaybolmuş olabilir); bu sefer `Persist` yoktur, çünkü oy
    // değişmedi. Yeni bir term yeni bir oy hakkı getirir.
    #[test]
    fn a_node_votes_at_most_once_per_term() {
        let mut node = node(1, &[1, 2, 3], 8);
        assert_eq!(
            node.step(message(2, request_vote(1))),
            vec![persist(1, Some(2)), send(2, vote(1, true))]
        );
        assert_eq!(
            node.step(message(3, request_vote(1))),
            vec![send(3, vote(1, false))]
        );
        assert_eq!(
            node.step(message(2, request_vote(1))),
            vec![send(2, vote(1, true))]
        );
        assert_eq!(
            node.step(message(3, request_vote(2))),
            vec![persist(2, Some(3)), send(3, vote(2, true))]
        );
    }

    // §5.4.1: log'u bizimkinden eski bir adaya oy verilmez: önce son girdinin term'i, eşitse log
    // uzunluğu karşılaştırılır. Kısıt burada saf fonksiyon düzeyinde sınanır; düğüm düzeyindeki iki
    // yönü (güncel log'a evet, eski log'a hayır) ayrı testlerdedir.
    #[test]
    fn the_election_restriction_compares_last_term_then_length() {
        let last = |term, index| (Term(term), LogIndex(index));
        assert!(
            candidate_is_up_to_date(last(0, 0), last(0, 0)),
            "equal logs"
        );
        assert!(
            candidate_is_up_to_date(last(3, 1), last(2, 9)),
            "a higher last term wins even with a shorter log"
        );
        assert!(
            !candidate_is_up_to_date(last(2, 9), last(3, 1)),
            "a lower last term loses even with a longer log"
        );
        assert!(
            candidate_is_up_to_date(last(2, 5), last(2, 4)),
            "same last term: the longer log wins"
        );
        assert!(
            !candidate_is_up_to_date(last(2, 4), last(2, 5)),
            "same last term: the shorter log loses"
        );
    }

    // §5.4.1, düğüm düzeyinde: log'u boş olan takipçi, log'u daha güncel bir adaya (son girdi term
    // 3, index 7) oyunu VERİR. Saf fonksiyonun argümanları çağrı yerinde yer değiştirseydi (aday
    // ile kendi log'u), bu oy reddedilirdi; bu test o kablolama hatasını yakalar. Ters yön (eski
    // log'lu adaya ret) `a_candidate_with_a_stale_log_is_denied_the_vote` testindedir.
    #[test]
    fn a_candidate_with_a_more_up_to_date_log_gets_the_vote() {
        let mut node = node(1, &[1, 2, 3], 19);
        let request = Message::RequestVote(RequestVote {
            term: Term(1),
            last_log_index: LogIndex(7),
            last_log_term: Term(3),
        });
        assert_eq!(
            node.step(message(2, request)),
            vec![persist(1, Some(2)), send(2, vote(1, true))]
        );
    }

    // Liderlikten düşen düğüm yeni bir zaman aşımıyla başlar. Lider seçim sayacını işletmez: aday
    // olarak T-1 tick bekleyip seçilen liderin sayacı T-1'de donmuştur. Daha yüksek bir term görüp
    // Follower'a döndüğünde sayaç sıfırlanmasaydı, kalan süre 1..T tick olurdu ve yeni seçim en geç
    // T tick içinde başlardı; sıfırlandığı için sonraki T-1 tick sessiz geçer. Birkaç seed denenir:
    // çekilen süre aralığın en ucuna (2T-1) denk gelirse sıfırlanmayan sayaç da T-1 tick dayanırdı.
    #[test]
    fn a_leader_that_steps_down_starts_a_fresh_election_timer() {
        for seed in 0..20 {
            let mut node = candidate(1, &[1, 2, 3], seed);
            for _ in 0..T - 1 {
                assert!(node.step(Input::Tick).is_empty());
            }
            let _ = node.step(message(2, vote(1, true)));
            assert_eq!(node.role(), Role::Leader);
            assert_eq!(
                node.step(message(3, vote(5, false))),
                vec![persist(5, None)]
            );
            assert_eq!(node.role(), Role::Follower);
            for _ in 0..T - 1 {
                assert!(
                    node.step(Input::Tick).is_empty(),
                    "seed {seed}: the election timer must restart from zero"
                );
            }
        }
    }

    // T1 ve Figure 2 (All Servers): daha yüksek term taşıyan HER mesaj türü, düğüm hangi rolde
    // olursa olsun onu o term'e taşır ve Follower yapar. Yeni term'de henüz oy yoktur; isteğin
    // kendisi bir oy isteğiyse verilen oy yeni term'in ilk oyudur. Değişiklik, aynı adımdaki
    // cevaptan önce diske yazdırılır.
    #[test]
    fn a_higher_term_turns_any_role_into_a_follower() {
        let higher = [
            vote(5, false),
            heartbeat_reply(5, false),
            heartbeat(5),
            request_vote(5),
        ];
        for msg in higher {
            let starts = [
                node(1, &[1, 2, 3], 9),
                candidate(1, &[1, 2, 3], 9),
                leader(1, &[1, 2, 3], 9),
            ];
            for mut node in starts {
                let outputs = node.step(message(2, msg.clone()));
                assert_eq!(node.current_term(), Term(5));
                assert_eq!(node.role(), Role::Follower);
                let new_vote = matches!(msg, Message::RequestVote(_)).then_some(2);
                assert_eq!(outputs.first(), Some(&persist(5, new_vote)));
            }
        }
    }

    // §5.1: eski term'li istekler, cevaplayanın güncel term'iyle reddedilir; böylece eski aday ya
    // da lider geride kaldığını öğrenir. Eski term'li cevaplar sessizce yok sayılır. Durum
    // değişmez, `Persist` yoktur.
    #[test]
    fn stale_requests_are_rejected_with_the_current_term() {
        let mut node = node(1, &[1, 2, 3], 10);
        let _ = node.step(message(2, request_vote(3)));
        assert_eq!(
            node.step(message(3, request_vote(2))),
            vec![send(3, vote(3, false))]
        );
        assert_eq!(
            node.step(message(3, heartbeat(2))),
            vec![send(3, heartbeat_reply(3, false))]
        );
        assert!(node.step(message(3, vote(2, true))).is_empty());
        assert!(node.step(message(3, heartbeat_reply(2, true))).is_empty());
        assert_eq!(node.current_term(), Term(3));
    }

    // §5.2: aday, aynı term'de seçilmiş bir liderden heartbeat alırsa onun liderliğini tanır ve
    // Follower'a döner. Oyu (kendine) aynı term'de değişmez, bu yüzden `Persist` yoktur.
    #[test]
    fn a_candidate_steps_down_on_a_heartbeat_of_its_own_term() {
        let mut node = candidate(1, &[1, 2, 3], 11);
        assert_eq!(
            node.step(message(2, heartbeat(1))),
            vec![send(2, heartbeat_reply(1, true))]
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.voted_for(), Some(NodeId(1)));
    }

    // Lider her `heartbeat_interval` tick'te bir bütün eşlerine heartbeat gönderir; arada
    // sessizdir.
    #[test]
    fn a_leader_sends_heartbeats_every_interval() {
        let mut node = leader(1, &[1, 2, 3], 12);
        for _ in 0..10 {
            for _ in 0..H - 1 {
                assert!(node.step(Input::Tick).is_empty());
            }
            assert_eq!(
                node.step(Input::Tick),
                vec![send(2, heartbeat(1)), send(3, heartbeat(1))]
            );
        }
    }

    // Aynı term'de ikinci bir liderden heartbeat Election Safety'ye göre imkânsızdır. Olursa lider
    // isteği reddeder ve liderliği bırakmaz: rolü yalnızca daha yüksek bir term değiştirir.
    #[test]
    fn a_leader_rejects_a_heartbeat_of_its_own_term() {
        let mut node = leader(1, &[1, 2, 3], 13);
        assert_eq!(
            node.step(message(2, heartbeat(1))),
            vec![send(2, heartbeat_reply(1, false))]
        );
        assert_eq!(node.role(), Role::Leader);
    }

    // R1: yeniden başlatma diskteki durumu yükler ve bütün geçici durumu siler. Term 1'in lideri
    // çöküp kalkınca Follower'dır; term'ini ve (kendine verdiği) oyunu korur, bu yüzden aynı
    // term'de başka bir adaya oy vermez. Restart hiç çıktı üretmez. Kurtarma bellekten değil
    // verilen değerden yapılır: disk bellekten eski olsa bile düğüm diskteki duruma döner.
    #[test]
    fn restart_restores_the_durable_state_as_a_follower() {
        let mut node = leader(1, &[1, 2, 3], 14);
        let disk = node.persistent_state();
        assert!(node.step(Input::Restart(disk.clone())).is_empty());
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.persistent_state(), disk);
        assert_eq!(
            node.step(message(2, request_vote(1))),
            vec![send(2, vote(1, false))]
        );
        // Zamanlayıcı da sıfırlandı: yeni seçim, yeniden başlatmadan [T, 2T) tick sonra.
        let (ticks, outputs) = tick_until_output(&mut node);
        assert!((T..2 * T).contains(&ticks));
        assert_eq!(outputs[0], persist(2, Some(1)));

        let older = PersistentState::default();
        assert!(node.step(Input::Restart(older.clone())).is_empty());
        assert_eq!(node.persistent_state(), older);
    }

    // Tek düğümlü küme: kendi oyu çoğunluktur, ilk zaman aşımında lider olur. Gönderecek eşi
    // olmadığından tek çıktı yeni term'in kaydıdır ve lider sonrasında sessiz kalır.
    #[test]
    fn a_single_node_cluster_elects_itself() {
        let mut node = node(1, &[1], 16);
        let (_, outputs) = tick_until_output(&mut node);
        assert_eq!(outputs, vec![persist(1, Some(1))]);
        assert_eq!(node.role(), Role::Leader);
        for _ in 0..3 * T {
            assert!(node.step(Input::Tick).is_empty());
        }
    }

    // Term uzayının sonu: term u64::MAX'ta ve o term'de başka bir adaya oy vermişken zaman aşımı
    // dolan düğüm seçim BAŞLATMAZ. Başlatsaydı term'i artıramadığı için aynı term'de kendine ikinci
    // bir oy verirdi (E1). Güvenlik canlılıktan önce gelir.
    #[test]
    fn an_exhausted_term_space_never_reuses_a_term() {
        let mut node = node(1, &[1, 2, 3], 17);
        let exhausted = PersistentState {
            current_term: Term(u64::MAX),
            voted_for: Some(NodeId(2)),
            log: Vec::new(),
        };
        assert!(node.step(Input::Restart(exhausted.clone())).is_empty());
        for _ in 0..10 * T {
            assert!(node.step(Input::Tick).is_empty());
        }
        assert_eq!(node.persistent_state(), exhausted);
        assert_eq!(node.role(), Role::Follower);
    }

    // Lider olmayan düğüm istemci isteğini yok sayar (Faz 4'te `NotLeader` cevabı gelecek): ne
    // takipçi ne aday isteği log'una ekler; düğüm birebir aynı kalır.
    #[test]
    fn non_leaders_ignore_client_requests() {
        for mut node in [node(1, &[1, 2, 3], 18), candidate(1, &[1, 2, 3], 18)] {
            let before = node.clone();
            assert!(node.step(client(7)).is_empty());
            assert_eq!(node, before);
        }
    }

    // Figure 2, AppendEntries madde 3-4 ve 2: takipçi yeni girdileri ekler, bunları cevaptan ÖNCE
    // diske yazdırır (log farkı ve yeni term tek bir Persist'te) ve cevapta eşleşmenin kesin olduğu
    // son index'i bildirir. Devam eden bir istek yalnızca yeni girdiyi yazdırır.
    #[test]
    fn a_follower_appends_new_entries_and_reports_the_match_index() {
        let mut node = node(1, &[1, 2, 3], 20);
        assert_eq!(
            node.step(message(
                2,
                append(1, (0, 0), vec![entry(1, 10), entry(1, 11)], 0)
            )),
            vec![
                persist_log(1, None, 1, vec![entry(1, 10), entry(1, 11)]),
                send(2, append_reply(1, true, 2)),
            ]
        );
        assert_eq!(
            node.step(message(2, append(1, (2, 1), vec![entry(1, 12)], 0))),
            vec![
                persist_log(1, None, 3, vec![entry(1, 12)]),
                send(2, append_reply(1, true, 3)),
            ]
        );
        assert_eq!(node.log(), &[entry(1, 10), entry(1, 11), entry(1, 12)]);
    }

    // Figure 2, AppendEntries madde 2: `prev_log_index`'te `prev_log_term`'lü girdi yoksa istek
    // reddedilir; log'a dokunulmaz ve Persist yoktur. Cevap, eşleşmenin olabileceği en büyük
    // index'i ipucu olarak taşır: log kısaysa son index, değilse `prev_log_index - 1`.
    #[test]
    fn a_mismatched_previous_entry_is_rejected_with_a_hint() {
        let mut node = node(1, &[1, 2, 3], 21);
        let _ = node.step(message(
            2,
            append(1, (0, 0), vec![entry(1, 10), entry(1, 11)], 0),
        ));
        assert_eq!(
            node.step(message(2, append(1, (5, 1), vec![entry(1, 15)], 0))),
            vec![send(2, append_reply(1, false, 2))],
            "the log is too short: the hint is its last index"
        );
        assert_eq!(
            node.step(message(2, append(1, (2, 9), vec![entry(9, 15)], 0))),
            vec![send(2, append_reply(1, false, 1))],
            "the term at index 2 differs: the hint is the index before it"
        );
        assert_eq!(node.log(), &[entry(1, 10), entry(1, 11)]);
    }

    // Figure 2, AppendEntries madde 3: aynı index'te farklı term'li bir girdi (çakışma) varsa o
    // girdi ve sonrası silinir, yerine liderin girdileri yazılır. Persist, değişen kuyruğu çakışma
    // index'inden itibaren taşır.
    #[test]
    fn a_conflicting_suffix_is_replaced() {
        let mut node = node(1, &[1, 2, 3], 22);
        let old = vec![entry(1, 10), entry(1, 11), entry(1, 12)];
        let _ = node.step(message(2, append(1, (0, 0), old, 0)));
        assert_eq!(
            node.step(message(
                3,
                append(2, (1, 1), vec![entry(2, 20), entry(2, 21)], 0)
            )),
            vec![
                persist_log(2, None, 2, vec![entry(2, 20), entry(2, 21)]),
                send(3, append_reply(2, true, 3)),
            ]
        );
        assert_eq!(node.log(), &[entry(1, 10), entry(2, 20), entry(2, 21)]);
    }

    // ÇAKIŞMA YOKSA SİLME YOK (Figure 2, madde 3): gecikmiş ya da çoğaltılmış eski bir istek
    // (burada yalnızca ilk girdiyi ya da girdilerin tamamını yeniden taşıyan) log'u kısaltmaz,
    // hiçbir şey yazdırmaz ve kendi doğruladığı öneki bildirir. Kısaltsaydı, takipçinin zaten
    // onayladığı (belki commit edilmiş) girdiler kaybolurdu.
    #[test]
    fn stale_or_duplicate_requests_never_truncate_the_log() {
        let mut node = node(1, &[1, 2, 3], 23);
        let full = vec![entry(1, 10), entry(1, 11), entry(1, 12)];
        let _ = node.step(message(2, append(1, (0, 0), full.clone(), 0)));
        assert_eq!(
            node.step(message(2, append(1, (0, 0), vec![entry(1, 10)], 0))),
            vec![send(2, append_reply(1, true, 1))]
        );
        assert_eq!(
            node.step(message(2, append(1, (0, 0), full.clone(), 0))),
            vec![send(2, append_reply(1, true, 3))]
        );
        assert_eq!(node.log(), full.as_slice());
    }

    // Figure 2, madde 5: takipçinin commitIndex'i min(leaderCommit, bu istekte doğrulanan son
    // girdi) olur ve yalnızca ileri gider; commit edilen girdiler index sırasıyla, her biri bir kez
    // uygulanır. Kısa bir önek taşıyan eski bir istek, yüksek bir leaderCommit taşısa bile ne
    // commitIndex'i geri çeker ne de doğrulamadığı girdileri commit eder.
    #[test]
    fn the_follower_commit_index_follows_the_leader_within_the_verified_prefix() {
        let mut node = node(1, &[1, 2, 3], 24);
        let full = vec![entry(1, 10), entry(1, 11), entry(1, 12)];
        assert_eq!(
            node.step(message(2, append(1, (0, 0), full.clone(), 2))),
            vec![
                persist_log(1, None, 1, full),
                send(2, append_reply(1, true, 3)),
                apply(1, 10),
                apply(2, 11),
            ]
        );
        assert_eq!(node.commit_index(), LogIndex(2));
        assert_eq!(
            node.step(message(2, append(1, (1, 1), Vec::new(), 3))),
            vec![send(2, append_reply(1, true, 1))],
            "only index 1 was verified: index 3 is not committed, nothing goes backwards"
        );
        assert_eq!(node.commit_index(), LogIndex(2));
        assert_eq!(
            node.step(message(2, append(1, (3, 1), Vec::new(), 3))),
            vec![send(2, append_reply(1, true, 3)), apply(3, 12)]
        );
        assert_eq!(node.last_applied(), LogIndex(3));
    }

    // Figure 2, Leaders: lider istemci komutunu kendi term'iyle log'una ekler, ekleneni cevaptan
    // önce diske yazdırır ve heartbeat'i beklemeden bütün takipçilere gönderir.
    #[test]
    fn a_leader_appends_client_commands_and_replicates_them_at_once() {
        let mut node = leader(1, &[1, 2, 3], 25);
        let replicate = append(1, (0, 0), vec![entry(1, 7)], 0);
        assert_eq!(
            node.step(client(7)),
            vec![
                persist_log(1, Some(1), 1, vec![entry(1, 7)]),
                send(2, replicate.clone()),
                send(3, replicate),
            ]
        );
    }

    // Figure 2, Leaders: başarılı cevap matchIndex/nextIndex'i ilerletir; kendi term'indeki girdi
    // çoğunluğa (lider + 1 takipçi) ulaşınca commit edilir ve uygulanır. Takipçinin eksiği
    // kalmadığından ek bir gönderim yoktur.
    #[test]
    fn a_majority_of_matches_commits_a_current_term_entry() {
        let mut node = leader(1, &[1, 2, 3], 26);
        let _ = node.step(client(7));
        assert_eq!(
            node.step(message(2, append_reply(1, true, 1))),
            vec![apply(1, 7)]
        );
        assert_eq!(node.commit_index(), LogIndex(1));
        assert_eq!(node.progress[&NodeId(2)].match_index, LogIndex(1));
        assert_eq!(node.progress[&NodeId(2)].next_index, LogIndex(2));
    }

    // §5.3: ret gelince nextIndex ipucuyla bir hamlede geri çekilir ve eksik girdiler hemen yeniden
    // gönderilir. Eşleşme bildirilince ilerleme güncellenir. Burada girdiler önceki term'den (1)
    // olduğu için çoğunlukta olsalar bile commit edilmez (§5.4.2).
    #[test]
    fn a_rejection_backs_off_next_index_and_retries_at_once() {
        let log = vec![entry(1, 1), entry(1, 2), entry(1, 3)];
        let mut node = leader_with_log(1, &[1, 2, 3], 27, log);
        assert_eq!(node.current_term(), Term(2));
        assert_eq!(node.progress[&NodeId(2)].next_index, LogIndex(4));
        assert_eq!(
            node.step(message(2, append_reply(2, false, 1))),
            vec![send(
                2,
                append(2, (1, 1), vec![entry(1, 2), entry(1, 3)], 0)
            )]
        );
        assert_eq!(node.progress[&NodeId(2)].next_index, LogIndex(2));
        assert!(node.step(message(2, append_reply(2, true, 3))).is_empty());
        assert_eq!(node.progress[&NodeId(2)].match_index, LogIndex(3));
        assert_eq!(
            node.commit_index(),
            LogIndex(0),
            "earlier-term entries are not counted"
        );
    }

    // Sırası değişmiş ya da çoğaltılmış eski cevaplar ilerlemeyi geri almaz: eski bir başarı
    // matchIndex'i küçültmez, eski bir ret nextIndex'i matchIndex + 1'in altına indirmez.
    #[test]
    fn stale_responses_do_not_move_progress_backwards() {
        let log = vec![entry(1, 1), entry(1, 2), entry(1, 3)];
        let mut node = leader_with_log(1, &[1, 2, 3], 28, log);
        let _ = node.step(message(2, append_reply(2, true, 3)));
        let _ = node.step(message(2, append_reply(2, true, 1)));
        assert_eq!(node.progress[&NodeId(2)].match_index, LogIndex(3));
        assert_eq!(node.progress[&NodeId(2)].next_index, LogIndex(4));
        let _ = node.step(message(2, append_reply(2, false, 0)));
        assert_eq!(node.progress[&NodeId(2)].next_index, LogIndex(4));
    }

    // §5.4.2 (Figure 8): lider, önceki bir term'den (1) gelen girdiyi çoğunlukta olsa bile kopya
    // sayarak commit ETMEZ. Kendi term'inden (2) bir girdi çoğunluğa ulaşınca o commit edilir ve
    // öncesindeki girdi de dolaylı olarak commit olur; ikisi de sırayla uygulanır.
    #[test]
    fn only_current_term_entries_are_committed_by_counting_replicas() {
        let mut node = leader_with_log(1, &[1, 2, 3], 29, vec![entry(1, 1)]);
        assert!(node.step(message(2, append_reply(2, true, 1))).is_empty());
        assert_eq!(node.commit_index(), LogIndex(0));
        let _ = node.step(client(9));
        assert_eq!(
            node.step(message(2, append_reply(2, true, 2))),
            vec![apply(1, 1), apply(2, 9)]
        );
        assert_eq!(node.commit_index(), LogIndex(2));
    }

    // Mesaj başına girdi sınırı: geride kalmış bir takipçiye eksik girdiler en fazla `max_entries`
    // girdilik parçalar hâlinde gider; her başarılı cevap bir sonraki parçayı hemen gönderir.
    #[test]
    fn missing_entries_are_sent_in_batches_of_max_entries() {
        let log = (1..=5).map(|byte| entry(1, byte)).collect();
        let state = PersistentState {
            current_term: Term(1),
            voted_for: None,
            log,
        };
        let config = Config::default().with_max_entries(NonZeroUsize::new(2).expect("not zero"));
        let mut node = RaftNode::new(NodeId(1), ids(&[1, 2, 3]), config, seed_bytes(30));
        let _ = node.step(Input::Restart(state));
        let _ = tick_until_output(&mut node);
        let _ = node.step(message(2, vote(2, true)));
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(
            node.step(message(2, append_reply(2, false, 0))),
            vec![send(
                2,
                append(2, (0, 0), vec![entry(1, 1), entry(1, 2)], 0)
            )]
        );
        assert_eq!(
            node.step(message(2, append_reply(2, true, 2))),
            vec![send(
                2,
                append(2, (2, 1), vec![entry(1, 3), entry(1, 4)], 0)
            )]
        );
    }

    // §5.4.1, düğüm düzeyinde ters yön: log'unun son girdisi term 2'de olan düğüm, son girdisi term
    // 1'de olan bir adaya (log'u daha uzun olsa bile) oy VERMEZ; son term'i eşit ve log'u en az
    // bizimki kadar uzun bir adaya verir. Yeni term her durumda benimsenir ve yazılır.
    #[test]
    fn a_candidate_with_a_stale_log_is_denied_the_vote() {
        let state = PersistentState {
            current_term: Term(2),
            voted_for: None,
            log: vec![entry(2, 1)],
        };
        let mut node = node_with_state(1, &[1, 2, 3], 31, state);
        let stale = Message::RequestVote(RequestVote {
            term: Term(3),
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        });
        assert_eq!(
            node.step(message(2, stale)),
            vec![persist(3, None), send(2, vote(3, false))]
        );
        let current = Message::RequestVote(RequestVote {
            term: Term(3),
            last_log_index: LogIndex(1),
            last_log_term: Term(2),
        });
        assert_eq!(
            node.step(message(3, current)),
            vec![persist(3, Some(3)), send(3, vote(3, true))]
        );
    }

    // R1 ve Figure 2: commitIndex ve lastApplied geçicidir; yeniden başlatmada 0'dan başlar. Log
    // kalır; lider commit bilgisini bir sonraki AppendEntries'le yeniden bildirince girdiler
    // baştan, sırasıyla yeniden uygulanır (durum makinesi de çökmede kaybolduğu için doğru davranış
    // budur).
    #[test]
    fn a_restart_forgets_the_commit_index_and_reapplies_after_learning_it() {
        let mut node = node(1, &[1, 2, 3], 32);
        let entries = vec![entry(1, 10), entry(1, 11)];
        let _ = node.step(message(2, append(1, (0, 0), entries.clone(), 2)));
        assert_eq!(node.last_applied(), LogIndex(2));
        let disk = node.persistent_state();
        assert!(node.step(Input::Restart(disk)).is_empty());
        assert_eq!(node.commit_index(), LogIndex(0));
        assert_eq!(node.last_applied(), LogIndex(0));
        assert_eq!(node.log(), entries.as_slice());
        assert_eq!(
            node.step(message(2, append(1, (2, 1), Vec::new(), 2))),
            vec![
                send(2, append_reply(1, true, 2)),
                apply(1, 10),
                apply(2, 11)
            ]
        );
    }

    // Tek düğümlü küme: liderin kendi kopyası çoğunluktur; istemci komutu eklendiği adımda commit
    // edilir ve uygulanır. Gönderecek eş olmadığından tek çıktılar yazma ve uygulamadır.
    #[test]
    fn a_single_node_cluster_commits_client_commands_at_once() {
        let mut node = node(1, &[1], 33);
        let _ = tick_until_output(&mut node);
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(
            node.step(client(5)),
            vec![persist_log(1, Some(1), 1, vec![entry(1, 5)]), apply(1, 5)]
        );
    }

    // Kapsama bekçisi: `Input`'in her varyantı için ayrık bir isim döndürür. Kasıtlı olarak `_`
    // kolu YOK — `Input`'e yeni bir varyant eklenirse bu fonksiyon derlenmez; derleme hatası seni
    // aşağıdaki rastgele girdi testine getirir. Yeni varyant için hem girdi üreticisini hem de
    // beklenen isim kümesini güncelle (yalnızca buraya kol eklemek küme karşılaştırmasını geçirmeye
    // yetmez).
    fn input_name(input: &Input) -> &'static str {
        match input {
            Input::Tick => "Tick",
            Input::Message { .. } => "Message",
            Input::ClientRequest(_) => "ClientRequest",
            Input::Restart(_) => "Restart",
        }
    }

    // Aynı bekçi mantığı `Message` için: 4 RPC varyantının hepsi üretilmeli.
    fn message_name(msg: &Message) -> &'static str {
        match msg {
            Message::RequestVote(_) => "RequestVote",
            Message::RequestVoteResponse(_) => "RequestVoteResponse",
            Message::AppendEntries(_) => "AppendEntries",
            Message::AppendEntriesResponse(_) => "AppendEntriesResponse",
        }
    }

    /// `[0, n)` aralığından bir sayı (testte mod sapması önemsizdir).
    fn below(rng: &mut ChaCha8Rng, n: u64) -> u64 {
        rng.next_u64() % n
    }

    /// Rastgele bir girdi. Tick ağırlıklıdır (zaman aşımları ve heartbeat'ler gerçekten
    /// tetiklensin). Mesajların göndericisi 1..=5'tir: düğümün kendisi (1) ve yapılandırma dışı 5
    /// dahil. Term'ler düğümün term'i civarındadır (bir eksik, aynı, bir fazla); aynı term daha
    /// sıktır. `Restart`, sürücünün diskindeki durumu verir.
    ///
    /// Aday iken üretilen mesajların yarısı, güncel term'e ait bir oy cevabıdır; lider iken yarısı
    /// güncel term'e ait başarılı bir AppendEntries cevabıdır. Neden: tamamen tekdüze girdilerle
    /// bir adaylık çoğunluğu toplamadan, bir liderlik de commit görmeden (daha yüksek bir term, bir
    /// heartbeat ya da bir yeniden başlatma yüzünden) neredeyse her zaman biter; sözleşmenin lider
    /// ve commit tarafı hiç sınanmazdı. Kapsama denetimi bunu yakalar.
    fn random_input(rng: &mut ChaCha8Rng, node: &RaftNode, disk: &PersistentState) -> Input {
        match below(rng, 20) {
            0..=8 => Input::Tick,
            9..=16 => {
                let from = NodeId(1 + below(rng, 5));
                let current = node.current_term().0;
                if node.role() == Role::Candidate && below(rng, 2) == 0 {
                    let msg = Message::RequestVoteResponse(RequestVoteResponse {
                        term: Term(current),
                        vote_granted: below(rng, 4) != 0,
                    });
                    return Input::Message { from, msg };
                }
                if node.role() == Role::Leader && below(rng, 2) == 0 {
                    let msg = Message::AppendEntriesResponse(AppendEntriesResponse {
                        term: Term(current),
                        success: true,
                        match_index: LogIndex(below(rng, node.log.last_index().0 + 1)),
                    });
                    return Input::Message { from, msg };
                }
                let term = Term(match below(rng, 4) {
                    0 => current.saturating_sub(1),
                    1 => current.saturating_add(1),
                    _ => current,
                });
                let msg = match below(rng, 4) {
                    0 => Message::RequestVote(RequestVote {
                        term,
                        last_log_index: LogIndex(below(rng, 4)),
                        last_log_term: Term(below(rng, current + 2)),
                    }),
                    1 => Message::RequestVoteResponse(RequestVoteResponse {
                        term,
                        vote_granted: below(rng, 4) != 0,
                    }),
                    2 => Message::AppendEntries(random_append(rng, node, term)),
                    _ => Message::AppendEntriesResponse(AppendEntriesResponse {
                        term,
                        success: below(rng, 2) == 0,
                        match_index: LogIndex(below(rng, node.log.last_index().0 + 2)),
                    }),
                };
                Input::Message { from, msg }
            }
            17 => Input::Restart(disk.clone()),
            _ => Input::ClientRequest(Command::new(vec![random_byte(rng)])),
        }
    }

    fn random_byte(rng: &mut ChaCha8Rng) -> u8 {
        u8::try_from(below(rng, 256)).unwrap_or(0)
    }

    /// Rastgele bir AppendEntries. Önceki girdi çoğunlukla düğümün kendi log'uyla tutarlıdır
    /// (tutarlılık denetimi geçsin, log gerçekten büyüsün ve çakışmalar çözülsün); bazen
    /// tutarsızdır (ret yolu). Girdilerin term'leri önceki girdinin term'i ile mesajın term'i
    /// arasında artan sıradadır; leaderCommit doğrulanan önekin biraz ötesine kadar rastgeledir.
    fn random_append(rng: &mut ChaCha8Rng, node: &RaftNode, term: Term) -> AppendEntries {
        let prev_log_index = LogIndex(below(rng, node.log.last_index().0 + 2));
        let prev_log_term = match node.log.term_at(prev_log_index) {
            Some(own) if below(rng, 4) != 0 => own,
            _ => Term(below(rng, term.0 + 1)),
        };
        let low = prev_log_term.0.min(term.0);
        let mut terms: Vec<u64> = (0..below(rng, 4))
            .map(|_| low + below(rng, term.0 - low + 1))
            .collect();
        terms.sort_unstable();
        let entries: Vec<LogEntry> = terms
            .into_iter()
            .map(|entry_term| LogEntry {
                term: Term(entry_term),
                command: Command::new(vec![random_byte(rng)]),
            })
            .collect();
        let verified = prev_log_index.0 + u64::try_from(entries.len()).unwrap_or(0);
        AppendEntries {
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: LogIndex(below(rng, verified + 2)),
        }
    }

    // Sözleşme testi: 300 rastgele girdi dizisinin (her biri 300 adım) her adımından sonra `step`
    // sözleşmesi denetlenir. Test bir sürücü gibi davranır: her `Persist` farkını "diskine" uygular
    // ve `Restart`'ta diski geri verir. Kısa bir zaman aşımı (T = 4, heartbeat = 2) kullanılır:
    // seçimler birkaç tick'te başlasın ve bir dizide rol geçişleri sık görülsün. Denetlenenler:
    // - hiçbir girdi panik attırmaz (N3);
    // - kalıcı durum değiştiyse İLK çıktı TEK bir `Persist`'tir ve farkı diske uygulamak tam olarak
    //   bellekteki durumu verir; değişmediyse hiç `Persist` yoktur (O2);
    // - çıktı sırası: `Persist`, sonra `Send`'ler, en sonda `Apply`'lar;
    // - term asla azalmaz (T1) ve aynı term içinde verilmiş oy değişmez (E1);
    // - aday ve lider kendine oy vermiştir; olumlu oy yalnızca `votedFor`'a gider;
    // - mesajlar yalnızca eşlere gider ve her zaman düğümün güncel term'ini taşır;
    // - oy isteği yalnızca tick'te seçim başlatan adaydan, AppendEntries yalnızca liderden çıkar,
    //   cevaplar yalnızca isteği gönderene gider; liderin AppendEntries'i kendi log'unun bir
    //   dilimini ve kendi commitIndex'ini taşır;
    // - commitIndex yalnızca artar, lastApplied onu aşmaz; `Apply`'lar ardışık index'lerle ve
    //   log'daki komutlarla gelir;
    // - aynı term'de lider kalan düğümün log'u yalnızca uzar (Leader Append-Only);
    // - `Restart` hiç çıktı üretmez ve düğümü diskteki durumla, commitIndex ve lastApplied 0 olan
    //   bir Follower olarak açar (R1).
    //
    // Kapsama: her girdi ve mesaj varyantı üretilmeli, her role ulaşılmalı; seçim kazanma, oy
    // verme, uygulama ve log kesme yeterince sık görülmeli. Aksi hâlde test, sözleşmenin bir
    // kısmını hiç sınamadan geçerdi.
    #[test]
    fn random_inputs_preserve_the_step_contract() {
        let mut inputs_seen = BTreeSet::new();
        let mut messages_seen = BTreeSet::new();
        let mut roles_seen = BTreeSet::new();
        let mut votes_granted = 0_u32;
        let mut elections_won = 0_u32;
        let mut entries_applied = 0_u32;
        let mut truncations = 0_u32;
        for seed in 0..300 {
            let mut rng = ChaCha8Rng::from_seed(seed_bytes(1_000 + seed));
            let config = Config::new(4, 2).expect("valid config");
            let mut node = RaftNode::new(NodeId(1), ids(&[1, 2, 3, 4]), config, seed_bytes(seed));
            let mut disk = PersistentState::default();
            for _ in 0..300 {
                let input = random_input(&mut rng, &node, &disk);
                inputs_seen.insert(input_name(&input));
                if let Input::Message { msg, .. } = &input {
                    messages_seen.insert(message_name(msg));
                }
                let restart = matches!(input, Input::Restart(_));
                let tick = matches!(input, Input::Tick);
                let sender = match &input {
                    Input::Message { from, .. } => Some(*from),
                    _ => None,
                };
                let before = node.persistent_state();
                let was_leader = node.role() == Role::Leader;
                let commit_before = node.commit_index();
                let applied_before = node.last_applied();
                let outputs = node.step(input);
                let after = node.persistent_state();
                roles_seen.insert(node.role());
                if !was_leader && node.role() == Role::Leader {
                    elections_won += 1;
                }

                if restart {
                    assert!(outputs.is_empty(), "R1: a restart produces no outputs");
                    assert_eq!(after, disk, "R1: a restart loads the disk");
                    assert_eq!(node.role(), Role::Follower);
                    assert_eq!(node.commit_index(), LogIndex(0));
                    assert_eq!(node.last_applied(), LogIndex(0));
                    continue;
                }
                let persists = outputs
                    .iter()
                    .filter(|output| matches!(output, Output::Persist(_)))
                    .count();
                if after == before {
                    assert_eq!(persists, 0, "O2: no Persist without a change");
                } else {
                    assert_eq!(persists, 1, "O2: exactly one Persist per changing step");
                    let Output::Persist(update) = &outputs[0] else {
                        panic!("O2: the Persist must come first: {outputs:?}");
                    };
                    if let Some(log) = &update.log
                        && log.from.0 <= u64::try_from(before.log.len()).unwrap_or(u64::MAX)
                    {
                        truncations += 1;
                    }
                    disk.apply(update);
                    assert_eq!(disk, after, "O2: replaying the update gives the new state");
                }
                assert!(
                    node.commit_index() >= commit_before,
                    "the commit index never goes backwards"
                );
                assert!(node.last_applied() <= node.commit_index());
                if was_leader
                    && node.role() == Role::Leader
                    && after.current_term == before.current_term
                {
                    assert!(
                        after.log.starts_with(&before.log),
                        "Leader Append-Only: a leader only appends to its log"
                    );
                }
                assert!(
                    after.current_term >= before.current_term,
                    "T1: the term never decreases"
                );
                if after.current_term == before.current_term && before.voted_for.is_some() {
                    assert_eq!(after.voted_for, before.voted_for, "E1: one vote per term");
                }
                if node.role() != Role::Follower {
                    assert_eq!(node.voted_for(), Some(node.id()));
                }
                let mut next_apply = applied_before.next();
                let mut applying = false;
                for output in &outputs[persists..] {
                    if let Output::Apply { index, command } = output {
                        applying = true;
                        assert_eq!(*index, next_apply, "entries are applied in index order");
                        assert_eq!(
                            Some(command),
                            node.log.entry(*index).map(|entry| &entry.command),
                            "an applied command is the logged one"
                        );
                        next_apply = next_apply.next();
                        entries_applied += 1;
                        continue;
                    }
                    let Output::Send { to, msg } = output else {
                        panic!("only Sends and Applies may follow the Persist: {output:?}");
                    };
                    assert!(!applying, "Sends come before Applies");
                    assert!(node.peers().contains(to), "messages go to peers only");
                    assert_eq!(
                        msg.term(),
                        node.current_term(),
                        "messages carry the current term"
                    );
                    // Mesaj türü, rol ve tetikleyiciyle uyumlu olmalı: oy isteği yalnızca bir
                    // tick'te seçim başlatan adaydan, heartbeat yalnızca liderden çıkar; cevaplar
                    // yalnızca bir isteğe ve yalnızca onu gönderene verilir.
                    match msg {
                        Message::RequestVote(_) => assert!(
                            tick && node.role() == Role::Candidate,
                            "RequestVote only from a candidate starting an election on a tick"
                        ),
                        Message::AppendEntries(request) => {
                            assert_eq!(
                                node.role(),
                                Role::Leader,
                                "AppendEntries only from a leader"
                            );
                            let from = request.prev_log_index.next();
                            let max = node.config().max_entries();
                            assert_eq!(
                                Some(request.prev_log_term),
                                node.log.term_at(request.prev_log_index),
                                "the previous entry is the leader's own"
                            );
                            assert_eq!(
                                request.entries,
                                node.log.entries_from(from, max),
                                "the entries are a slice of the leader's log"
                            );
                            assert_eq!(request.leader_commit, node.commit_index());
                        }
                        Message::RequestVoteResponse(_) | Message::AppendEntriesResponse(_) => {
                            assert_eq!(sender, Some(*to), "a response goes back to the requester");
                        }
                    }
                    if let Message::RequestVoteResponse(RequestVoteResponse {
                        vote_granted: true,
                        ..
                    }) = msg
                    {
                        assert_eq!(
                            node.voted_for(),
                            Some(*to),
                            "a granted vote goes to votedFor"
                        );
                        votes_granted += 1;
                    }
                }
                assert_eq!(
                    next_apply,
                    node.last_applied().next(),
                    "every newly applied index produced exactly one Apply"
                );
            }
        }
        assert_eq!(
            inputs_seen,
            ["Tick", "Message", "ClientRequest", "Restart"]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            messages_seen,
            [
                "RequestVote",
                "RequestVoteResponse",
                "AppendEntries",
                "AppendEntriesResponse"
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            roles_seen,
            [Role::Follower, Role::Candidate, Role::Leader]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "every role must be reached"
        );
        // Eşikler ölçülen değerlerin (511 liderlik, 1240 oy, 2744 uygulama, 812 kesme) çok
        // altındadır: üretici bozulup bir tarafı seyrek görmeye başlarsa test bunu söyler.
        assert!(
            elections_won >= 100,
            "too few elections won ({elections_won}); the leader side is barely exercised"
        );
        assert!(
            votes_granted >= 100,
            "too few votes granted ({votes_granted}); the voting side is barely exercised"
        );
        assert!(
            entries_applied >= 500,
            "too few entries applied ({entries_applied}); the commit side is barely exercised"
        );
        assert!(
            truncations >= 100,
            "too few conflicting suffixes replaced ({truncations}); conflict resolution is barely \
             exercised"
        );
    }
}
