//! Sans-IO Raft düğümü: tek genel API `step(Input) -> Vec<Output>`.
//!
//! Faz 2 kapsamı lider seçimidir (§5.2): roller, term'ler, rastgele seçim zaman aşımı, oy verme
//! (seçim kısıtı dahil, §5.4.1), heartbeat ve çökme sonrası yeniden başlatma. Log replikasyonu
//! (Faz 3) ve istemci arayüzü (Faz 4) henüz yoktur.

use std::collections::BTreeSet;

use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};

use crate::config::Config;
use crate::input::Input;
use crate::message::{
    AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse,
};
use crate::output::Output;
use crate::persist::PersistentState;
use crate::role::Role;
use crate::types::{LogIndex, NodeId, Term};

/// Bir adımda gönderilecek mesajlar, üretildikleri sırayla. İşleyiciler yalnızca buraya yazar;
/// `Persist` kararını `step` tek bir yerde verir (O2). Böylece hiçbir işleyici `Persist`'i yanlış
/// yere koyamaz ya da unutamaz.
type Outbox = Vec<(NodeId, Message)>;

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

    // --- Geçici durum: çökmede kaybolur, `Restart` onu sıfırdan kurar (R1).
    role: Role,
    // Bu term'deki adaylıkta oy veren düğümler (kendisi dahil). Sayaç değil küme: aynı düğümün
    // çoğaltılmış ya da tekrarlanmış cevabı iki kez sayılmasın (E2).
    votes: BTreeSet<NodeId>,
    // Son sıfırlamadan bu yana geçen tick sayısı ve bu tur için çekilen zaman aşımı. Lider bu
    // sayacı işletmez (liderin seçim zaman aşımı yoktur).
    election_elapsed: u64,
    election_timeout: u64,
    // Liderin son heartbeat'ten bu yana geçen tick sayısı.
    heartbeat_elapsed: u64,
}

impl RaftNode {
    /// Yeni bir düğüm oluşturur: boş diskle ilk kez açılan bir Follower (term 0, oy yok).
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
        Self {
            id,
            peers,
            config,
            rng,
            current_term: state.current_term,
            voted_for: state.voted_for,
            role: Role::Follower,
            votes: BTreeSet::new(),
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
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

    /// Kurucuya verilen zamanlama ayarları (N2).
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

    /// Düğümün bellekteki kalıcı durumu: diskte bulunması GEREKEN değer. Her adımın `Persist`
    /// çıktısı tam olarak bunu taşır; sürücü, her adımdan sonra diskin bununla aynı olduğunu
    /// denetleyerek "değişti ama persist edilmedi" hatalarını hemen yakalayabilir.
    #[must_use]
    pub fn persistent_state(&self) -> PersistentState {
        PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
        }
    }

    /// Bir girdiyi işler, sürücünün SIRAYLA yürütmesi gereken çıktı listesini döndürür.
    ///
    /// `#[must_use]`: dönen listenin tamamen yok sayılması (hiçbir mesaj gitmez, hiçbir durum
    /// persist edilmez) neredeyse her zaman bir sürücü hatasıdır ve derleyici bunu uyarır. Listeyi
    /// sırasız ya da eksik yürütmek (O1 ihlali) ise derleyicinin göremeyeceği, sürücünün
    /// sorumluluğundaki bir hatadır.
    ///
    /// N3: `step` tam (total) bir fonksiyondur: her `Input` değeri için panik atmadan döner.
    /// Simülatör her düğümü her tick'te adımlar; tek bir panik bütün koşuyu ve determinizm
    /// testlerini çökertirdi. Bu yüzden imkânsız sayılan durumlar bile (ör. term uzayının sonu,
    /// aynı term'de ikinci bir lider) panikle değil güvenli bir cevapla ele alınır.
    #[must_use = "outputs must be executed in order: Persist before the Sends that depend on it"]
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        // O2: kalıcı durumun adımdan önceki hâli. Adım sonunda değişmişse çıktıların BAŞINA tek bir
        // `Persist` konur. Bu "önce/sonra" karşılaştırması, durumu değiştiren her noktaya ayrı ayrı
        // persist eklemekten daha güvenlidir: yeni bir kod yolu durumu değiştirip persist'i
        // unutamaz.
        let before = (self.current_term, self.voted_for);
        let mut outbox = Outbox::new();
        match input {
            Input::Tick => self.on_tick(&mut outbox),
            Input::Message { from, msg } => self.on_message(from, msg, &mut outbox),
            // İstemci arayüzü Faz 3/4'ün kapsamı: Faz 2'de hiçbir sürücü istek göndermez, gelen
            // istek de yok sayılır (cevap tipi `ClientResponse` henüz bir yer tutucudur).
            Input::ClientRequest(_) => {}
            Input::Restart(state) => {
                self.restart(state);
                // R1: yüklenen durum zaten diskteki durumdur; yeniden yazmaya gerek yok. Yeniden
                // başlayan düğüm Follower'dır ve gönderecek bir şeyi yoktur.
                return Vec::new();
            }
        }
        let persist = ((self.current_term, self.voted_for) != before)
            .then(|| Output::Persist(self.persistent_state()));
        persist
            .into_iter()
            .chain(outbox.into_iter().map(|(to, msg)| Output::Send { to, msg }))
            .collect()
    }

    /// Mantıksal zaman bir tick ilerledi.
    fn on_tick(&mut self, outbox: &mut Outbox) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(1);
                if self.heartbeat_elapsed >= self.config.heartbeat_interval() {
                    self.broadcast_heartbeat(outbox);
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
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        for &peer in &self.peers {
            outbox.push((peer, Message::RequestVote(request.clone())));
        }
    }

    /// Seçimi kazanan aday lider olur.
    fn become_leader(&mut self, outbox: &mut Outbox) {
        self.role = Role::Leader;
        self.votes.clear();
        // §5.2: lider seçilir seçilmez boş AppendEntries (heartbeat) gönderir. Böylece aynı term'in
        // diğer adayları liderliği öğrenip Follower'a döner, takipçiler de yeni seçim başlatmaz.
        self.broadcast_heartbeat(outbox);
    }

    /// Bütün eşlere heartbeat (girdisiz `AppendEntries`) gönderir ve heartbeat sayacını sıfırlar.
    fn broadcast_heartbeat(&mut self, outbox: &mut Outbox) {
        self.heartbeat_elapsed = 0;
        let heartbeat = AppendEntries {
            term: self.current_term,
        };
        for &peer in &self.peers {
            outbox.push((peer, Message::AppendEntries(heartbeat.clone())));
        }
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
            Message::AppendEntries(request) => self.on_append_entries(from, &request, outbox),
            // Faz 2'de lider cevaptan yalnızca term'i öğrenir; o da yukarıda işlendi. Faz 3'te
            // `nextIndex`/`matchIndex` buradan güncellenecek.
            Message::AppendEntriesResponse(_) => {}
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
                (self.last_log_term(), self.last_log_index()),
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

    /// `AppendEntries` alıcısı (Figure 2). Faz 2'de yalnızca heartbeat'tir.
    fn on_append_entries(&mut self, from: NodeId, request: &AppendEntries, outbox: &mut Outbox) {
        // §5.1: eski term'li bir liderin isteği reddedilir. Cevaptaki güncel term sayesinde eski
        // lider geride kaldığını öğrenip Follower'a döner.
        let success = if request.term < self.current_term {
            false
        } else if self.role == Role::Leader {
            // Buradan sonra istek bizim term'imizdedir (daha yükseği `on_message`'da benimsendi).
            // Aynı term'de ikinci bir lider Election Safety'ye göre imkânsızdır. Olursa (ör. bir
            // hata), lider isteği reddeder ve liderliği bırakmaz: rolü yalnızca daha yüksek bir
            // term değiştirir. İhlali raporlamak denetçinin (checker) işidir; çekirdek panik atmaz
            // (N3).
            false
        } else {
            // §5.2: aday, aynı term'de seçilmiş bir liderden AppendEntries alırsa onun liderliğini
            // tanır ve Follower'a döner. `votedFor` değişmez: bu term'deki oy zaten kullanıldı.
            self.role = Role::Follower;
            self.votes.clear();
            // Figure 2: mevcut liderden AppendEntries almak seçim zamanlayıcısını sıfırlar.
            self.reset_election_timer();
            // Faz 2'de log yok: log tutarlılık kontrolü (Figure 2, AppendEntries madde 2-5) Faz
            // 3'te gelecek.
            true
        };
        outbox.push((
            from,
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term,
                success,
            }),
        ));
    }

    /// Çökme sonrası yeniden başlatma (R1).
    fn restart(&mut self, state: PersistentState) {
        // Figure 2: çökme bütün geçici durumu siler; yalnızca diskteki durum kalır. Düğüm alan alan
        // sıfırlanmak yerine `boot` ile baştan kurulur: ileride eklenecek bir geçici alanın (Faz 3:
        // commitIndex, nextIndex, ...) sıfırlanması böylece unutulamaz.
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

    /// Son log girdisinin index'i. Faz 2'de log henüz yok (Faz 3): boş log için 0 (Figure 2).
    fn last_log_index(&self) -> LogIndex {
        LogIndex(0)
    }

    /// Son log girdisinin term'i. Faz 2'de log henüz yok (Faz 3): boş log için 0.
    fn last_log_term(&self) -> Term {
        Term(0)
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
    use crate::message::{
        AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse,
    };
    use crate::output::Output;
    use crate::persist::PersistentState;
    use crate::role::Role;
    use crate::types::{Command, LogIndex, NodeId, Term};
    use rand_chacha::ChaCha8Rng;
    use rand_chacha::rand_core::{Rng, SeedableRng};
    use std::collections::BTreeSet;

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

    fn heartbeat(term: u64) -> Message {
        Message::AppendEntries(AppendEntries { term: Term(term) })
    }

    fn heartbeat_reply(term: u64, success: bool) -> Message {
        Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(term),
            success,
        })
    }

    fn persist(term: u64, voted_for: Option<u64>) -> Output {
        Output::Persist(PersistentState {
            current_term: Term(term),
            voted_for: voted_for.map(NodeId),
        })
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
    // taşır (Faz 2'de log boş: 0/0).
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
    // uzunluğu karşılaştırılır. Faz 2'de log boş olduğu için kısıt saf fonksiyon düzeyinde sınanır.
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
    // log'lu adaya ret) log'un geldiği Faz 3'te düğüm düzeyinde sınanacak.
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
        };
        assert!(node.step(Input::Restart(exhausted.clone())).is_empty());
        for _ in 0..10 * T {
            assert!(node.step(Input::Tick).is_empty());
        }
        assert_eq!(node.persistent_state(), exhausted);
        assert_eq!(node.role(), Role::Follower);
    }

    // Faz 2'de istemci arayüzü yok: istek yok sayılır ve düğüm birebir aynı kalır.
    #[test]
    fn client_requests_are_ignored_for_now() {
        let mut node = leader(1, &[1, 2, 3], 18);
        let before = node.clone();
        assert!(
            node.step(Input::ClientRequest(Command::new(vec![1, 2, 3])))
                .is_empty()
        );
        assert_eq!(node, before);
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
    /// Aday iken üretilen mesajların yarısı, güncel term'e ait bir oy cevabıdır. Neden: tamamen
    /// tekdüze girdilerle bir adaylık, çoğunluk oyunu toplamadan (daha yüksek bir term, bir
    /// heartbeat ya da bir yeniden başlatma yüzünden) neredeyse her zaman biter ve sözleşmenin
    /// lider tarafı hiç sınanmazdı. Kapsama denetimi bunu yakalar.
    fn random_input(rng: &mut ChaCha8Rng, node: &RaftNode, disk: &PersistentState) -> Input {
        match below(rng, 20) {
            0..=9 => Input::Tick,
            10..=17 => {
                let from = NodeId(1 + below(rng, 5));
                let current = node.current_term().0;
                if node.role() == Role::Candidate && below(rng, 2) == 0 {
                    let msg = Message::RequestVoteResponse(RequestVoteResponse {
                        term: Term(current),
                        vote_granted: below(rng, 4) != 0,
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
                        last_log_index: LogIndex(below(rng, 2)),
                        last_log_term: Term(below(rng, 2)),
                    }),
                    1 => Message::RequestVoteResponse(RequestVoteResponse {
                        term,
                        vote_granted: below(rng, 4) != 0,
                    }),
                    2 => Message::AppendEntries(AppendEntries { term }),
                    _ => Message::AppendEntriesResponse(AppendEntriesResponse {
                        term,
                        success: below(rng, 2) == 0,
                    }),
                };
                Input::Message { from, msg }
            }
            18 => Input::Restart(disk.clone()),
            _ => Input::ClientRequest(Command::new(Vec::new())),
        }
    }

    // Sözleşme testi: 300 rastgele girdi dizisinin (her biri 300 adım) her adımından sonra `step`
    // sözleşmesi denetlenir. Test bir sürücü gibi davranır: her `Persist`'i "diske" yazar ve
    // `Restart`'ta diski geri verir. Kısa bir zaman aşımı (T = 4, heartbeat = 2) kullanılır:
    // seçimler birkaç tick'te başlasın ve bir dizide rol geçişleri sık görülsün. Denetlenenler:
    // - hiçbir girdi panik attırmaz (N3);
    // - kalıcı durum değiştiyse İLK çıktı, yeni durumu taşıyan TEK `Persist`'tir; değişmediyse hiç
    //   `Persist` yoktur (O2);
    // - term asla azalmaz (T1) ve aynı term içinde verilmiş oy değişmez (E1);
    // - aday ve lider kendine oy vermiştir; olumlu oy yalnızca `votedFor`'a gider;
    // - mesajlar yalnızca eşlere gider ve her zaman düğümün güncel term'ini taşır;
    // - oy isteği yalnızca tick'te seçim başlatan adaydan, heartbeat yalnızca liderden çıkar,
    //   cevaplar yalnızca isteği gönderene gider;
    // - `Restart` hiç çıktı üretmez ve düğümü diskteki durumla Follower olarak açar (R1).
    //
    // Kapsama: her girdi ve mesaj varyantı üretilmeli, her role ulaşılmalı; seçim kazanma ve oy
    // verme yeterince sık görülmeli. Aksi hâlde test, sözleşmenin bir kısmını hiç sınamadan
    // geçerdi.
    #[test]
    fn random_inputs_preserve_the_step_contract() {
        let mut inputs_seen = BTreeSet::new();
        let mut messages_seen = BTreeSet::new();
        let mut roles_seen = BTreeSet::new();
        let mut votes_granted = 0_u32;
        let mut elections_won = 0_u32;
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
                    assert_eq!(
                        outputs[0],
                        Output::Persist(after.clone()),
                        "O2: the Persist comes first and carries the new state"
                    );
                    disk = after.clone();
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
                for output in &outputs[persists..] {
                    let Output::Send { to, msg } = output else {
                        panic!("only Sends may follow the Persist: {output:?}");
                    };
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
                        Message::AppendEntries(_) => assert_eq!(
                            node.role(),
                            Role::Leader,
                            "AppendEntries only from a leader"
                        ),
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
        // Eşikler ölçülen değerlerin (531 liderlik, 2096 oy) çok altındadır: üretici bozulup lider
        // tarafını seyrek görmeye başlarsa test bunu söyler.
        assert!(
            elections_won >= 100,
            "too few elections won ({elections_won}); the leader side is barely exercised"
        );
        assert!(
            votes_granted >= 100,
            "too few votes granted ({votes_granted}); the voting side is barely exercised"
        );
    }
}
