//! Raft adaptörü: `raft_core::RaftNode`'u simülatöre bağlar ve her olaydan sonra invariant'ları
//! denetleyen [`RaftCluster`]'ı sunar.
//!
//! Adaptörün iki görevi var:
//!
//! 1. `RaftNode` için [`SimNode`]'u uygular: `NodeInput`/`NodeOutput` ile raft-core'un
//!    `Input`/`Output`'u arasında birebir çeviri. Mesajlar, kalıcı durum ve farklar için kanonik
//!    trace kodlamaları da buradadır.
//! 2. [`RaftCluster`]: simülasyonu olay olay sürer ve HER olaydan sonra (bir düğüm adımı, bir fsync
//!    ya da bir teslim ve çıktılarının uygulanması) şunları denetler:
//!    - Figure 3'ün beş güvenlik özelliği, bağımsız kâhinlerle (`checker` crate'i): **Election
//!      Safety**, **Leader Append-Only**, **Log Matching**, **Leader Completeness** ve **State
//!      Machine Safety**.
//!    - **Dayanıklılık:** ayaktaki her düğümün bellekteki `currentTerm`, `votedFor` ve log'u, diske
//!      yazdırdığı en son durumla aynı olmalı. Bir adım durumu değiştirip `Persist` etmeyi
//!      unutursa, hatayı ortaya çıkaracak bir çökmeyi beklemeden hemen yakalanır.
//!    - **Çıktı sırası:** bir adımda `Persist`, aynı adımın `Send` ve `Apply`'larından önce gelmeli
//!      (Figure 2: "cevap vermeden önce kalıcı depoya yaz"; O1). Simülatör bir yazmadan SONRAKİ
//!      çıktıları yazma kalıcı olana kadar tutar, ama yazmadan ÖNCE verilmiş bir çıktıyı geri
//!      alamaz: o mesaj durum kalıcı olmadan ağa çıkmıştır. Bu yüzden ters sıra, bir çökmenin onu
//!      görünür kılmasını beklemeden, üretildiği adımda bildirilir.
//!    - **Commit edilmiş girdilerin korunması:** bir düğüm, commit ettiği gözlenen (kâhinlere
//!      görünen; bkz. aşağıda) bir index'ten itibaren log'unu yeniden yazamaz (§5.3: silme yalnızca
//!      çakışmada olur ve commit edilmiş bir girdi çakışamaz).
//!
//! Log kâhinleri (Log Matching ve Leader Completeness) düğümlerin belleğini değil, KALICI (fsync
//! edilmiş) log'unu gözler. Bellek, diske henüz ulaşmamış değişiklikler taşıyabilir ve düğüm bu
//! pencerede çökerse onlar hiç olmamış sayılır. Bu güvenlidir, çünkü O1 gereği dışarıya hiçbir
//! etkileri çıkmamıştır: mesajlar ve uygulamalar yazmaların arkasında tutulur. Bellek gözlemi ise
//! hiç gerçekleşmemiş bir geçmişi kayda geçirirdi. Örnek: tek düğümlü bir küme, term'i diske
//! ulaşmadan kendi oyuyla AYNI adımda lider olur ve bir komutu eklediği adımda commitIndex'ini
//! ilerletir. Bir çökme ikisini de silerse düğüm aynı term'i bu kez başka bir log'la yeniden
//! kazanır. Bu bir Raft ihlali değildir, ama bellek gözlemi onu Log Matching ve Leader Completeness
//! ihlali sanırdı. Bu yüzden bu iki kâhin diskteki log'a bakar ve commitIndex, diskteki log'un
//! bellekteki log'la aynı olduğu önekle sınırlanarak gözlenir (bkz. `visible_commit`). Çok düğümlü
//! bir kümede bu hiçbir şeyi kaçırmaz: bir düğümün durumu ancak mesajlarla dışarı çıkar ve mesajlar
//! yazmaların arkasında beklediği için, dışarıyı etkileyen her durum önce kalıcı olur.
//!
//! Rol ve liderin kendi log'una yaptıkları ise bellekten gözlenir: Election Safety liderliği,
//! Leader Append-Only liderin log'unu yazmanın VERİLDİĞİ adımda görür. Diskten gözlemek burada bir
//! ihlali gizleyebilirdi: lider log'unu bozan bir yazma verip yazma kalıcı olmadan liderliği
//! bırakırsa, yazmanın arkasında tutulan mesajlar yine çıkar ama yazma kalıcı olduğunda düğüm artık
//! lider değildir. Term'i henüz kalıcı olmayan bir lider yalnızca tek düğümlü bir kümede olur
//! (başka kümelerde oylar, adayın term yazması kalıcı olduktan sonra gelir) ve orada aynı term'i
//! yeniden kazanan aynı düğümdür; Election Safety çiğnenmez. Çökme, Leader Append-Only'nin düğümün
//! liderliklerinden sakladığı log görüntülerini unutturur (bkz.
//! `LeaderAppendOnly::observe_restart`): aynı term'i yeniden kazanmak yeni bir liderliktir.
//! Election Safety'nin term başına lider kaydı ise unutulmaz. Dayanıklılık, çıktı sırası ve commit
//! edilmiş girdilerin korunması da belleğe ve yazmanın VERİLDİĞİ âna bakar: unutulan bir
//! `Persist`'i hemen yakalamanın yolu budur.
//!
//! Neden her olaydan sonra: bir ihlal geçici olabilir. Örneğin bir düğüm yanlışlıkla lider olup
//! bir sonraki olayda daha yüksek bir term görerek düşebilir. Yalnızca koşunun sonunda bakmak bu
//! ara durumu kaçırırdı. Maliyet sınırlı tutulur: log denetimleri yalnızca log'u değişen düğüm için
//! yapılır ve Log Matching yalnızca değişen kısmı karşılaştırır. Denetçiye verilen görünümü kurmak
//! ve liderin log'unu saklamak ise hâlâ log boyuyla orantılıdır.

use std::collections::{BTreeMap, BTreeSet};

use checker::{
    ElectionSafety, ElectionSafetyViolation, EntryView, LeaderAppendOnly,
    LeaderAppendOnlyViolation, LeaderCompleteness, LeaderCompletenessViolation, LogMatching,
    LogMatchingViolation, StateMachineSafety, StateMachineSafetyViolation,
};
use raft_core::{
    AppendEntries, AppendEntriesResponse, Command, Config, Input, LogEntry, LogIndex, LogUpdate,
    Message, NodeId, Output, PersistUpdate, PersistentState, RaftNode, RequestVote,
    RequestVoteResponse, Role, Term,
};

use crate::disk::{DiskConfig, SimDisk};
use crate::error::{ConfigError, LifecycleError, PartitionError};
use crate::kv::{KvCommand, KvStore};
use crate::network::{NetworkConfig, SimNetwork};
use crate::node::{DurableState, InputOf, NodeInput, NodeOutput, OutputOf, SimNode};
use crate::rng::{Component, SeedTree};
use crate::simulation::{SimConfig, SimOptions, Simulation, TickOrder};
use crate::trace::TraceEncode;

/// Bir düğümün durum makinesine uyguladığı commit edilmiş girdi (`Output::Apply`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedEntry {
    /// Girdinin log index'i.
    pub index: LogIndex,
    /// Uygulanan komut.
    pub command: Command,
}

impl SimNode for RaftNode {
    type Msg = Message;
    type Durable = PersistentState;
    type Request = Command;
    type Applied = AppliedEntry;

    fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>> {
        let input = match input {
            NodeInput::Tick => Input::Tick,
            NodeInput::Message { from, msg } => Input::Message { from, msg },
            NodeInput::Restart(state) => Input::Restart(state),
            NodeInput::Client(command) => Input::ClientRequest(command),
        };
        // `RaftNode::step` yazımı raft-core'un kendi `step`'ini çağırır (yerleşik metot, trait
        // metodundan önce gelir). Çıktılar sırası korunarak çevrilir (O1).
        RaftNode::step(self, input)
            .into_iter()
            .filter_map(|output| match output {
                Output::Send { to, msg } => Some(NodeOutput::Send { to, msg }),
                Output::Persist(update) => Some(NodeOutput::Persist(update)),
                Output::Apply { index, command } => {
                    Some(NodeOutput::Apply(AppliedEntry { index, command }))
                }
                // Faz 3'te çekirdek istemci cevabı üretmez; Faz 4'te istemci geçmişine
                // bağlanacak. `_` kolu bilerek yok: `Output`'a yeni bir varyant eklenince burası
                // derlenmez ve adaptör bilinçli olarak güncellenir.
                Output::ClientResponse(_) => None,
            })
            .collect()
    }
}

impl DurableState for PersistentState {
    type Update = PersistUpdate;

    // Farkın uygulanma kuralı raft-core'da tanımlıdır; disk onu olduğu gibi kullanır.
    fn apply(&mut self, update: &PersistUpdate) {
        PersistentState::apply(self, update);
    }
}

/// Sabit genişlikli, little-endian bir `u64` yazar.
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Uzunluk önekli bir bayt dizisi yazar: önek, ardışık iki alanın sınırını belirsizlikten kurtarır.
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    out.extend_from_slice(bytes);
}

/// Oy: 0 = yok, 1 + düğüm kimliği = var. Etiket baytı sayesinde "oy yok" ile "düğüm 0'a oy"
/// birbirine karışmaz.
fn put_vote(out: &mut Vec<u8>, vote: Option<NodeId>) {
    match vote {
        None => out.push(0),
        Some(node) => {
            out.push(1);
            put_u64(out, node.0);
        }
    }
}

/// Uzunluk önekli bir girdi listesi.
fn put_entries(out: &mut Vec<u8>, entries: &[LogEntry]) {
    put_u64(out, u64::try_from(entries.len()).unwrap_or(u64::MAX));
    for LogEntry { term, command } in entries {
        put_u64(out, term.0);
        put_bytes(out, command.as_bytes());
    }
}

impl TraceEncode for Message {
    // Kanonik kodlama: varyant etiketi ve ardından alanlar sabit sırayla (u64'ler little-endian,
    // bool tek bayt, listeler ve baytlar uzunluk önekli). Yapılar desenle açılır (`..` YOK): bir
    // mesaja alan eklendiğinde bu kod derlenmez ve yeni alan kodlamaya bilinçli olarak eklenir.
    // Aksi hâlde yalnızca o alanda farklılaşan iki mesaj aynı özeti verir ve trace bir sapmayı
    // gizleyebilirdi.
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Message::RequestVote(RequestVote {
                term,
                last_log_index,
                last_log_term,
            }) => {
                out.push(1);
                put_u64(out, term.0);
                put_u64(out, last_log_index.0);
                put_u64(out, last_log_term.0);
            }
            Message::RequestVoteResponse(RequestVoteResponse { term, vote_granted }) => {
                out.push(2);
                put_u64(out, term.0);
                out.push(u8::from(*vote_granted));
            }
            Message::AppendEntries(AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            }) => {
                out.push(3);
                put_u64(out, term.0);
                put_u64(out, prev_log_index.0);
                put_u64(out, prev_log_term.0);
                put_u64(out, leader_commit.0);
                put_entries(out, entries);
            }
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term,
                success,
                match_index,
            }) => {
                out.push(4);
                put_u64(out, term.0);
                out.push(u8::from(*success));
                put_u64(out, match_index.0);
            }
        }
    }
}

impl TraceEncode for PersistentState {
    // Term, oy ve log. Desen yine `..` olmadan açılır.
    fn encode(&self, out: &mut Vec<u8>) {
        let PersistentState {
            current_term,
            voted_for,
            log,
        } = self;
        put_u64(out, current_term.0);
        put_vote(out, *voted_for);
        put_entries(out, log);
    }
}

impl TraceEncode for PersistUpdate {
    // Term, oy ve varsa log farkı (0 = yok, 1 + `from` + girdiler = var).
    fn encode(&self, out: &mut Vec<u8>) {
        let PersistUpdate {
            current_term,
            voted_for,
            log,
        } = self;
        put_u64(out, current_term.0);
        put_vote(out, *voted_for);
        match log {
            None => out.push(0),
            Some(LogUpdate { from, entries }) => {
                out.push(1);
                put_u64(out, from.0);
                put_entries(out, entries);
            }
        }
    }
}

impl TraceEncode for Command {
    fn encode(&self, out: &mut Vec<u8>) {
        put_bytes(out, self.as_bytes());
    }
}

impl TraceEncode for AppliedEntry {
    fn encode(&self, out: &mut Vec<u8>) {
        let AppliedEntry { index, command } = self;
        put_u64(out, index.0);
        put_bytes(out, command.as_bytes());
    }
}

/// Bir log'un denetçiye görünen hâli (bkz. `checker::EntryView`): kopyasız bir pencere.
fn entry_views(log: &[LogEntry]) -> Vec<EntryView<'_>> {
    log.iter()
        .map(|entry| EntryView {
            term: entry.term.0,
            command: entry.command.as_bytes(),
        })
        .collect()
}

/// Bir term'de gözlenen seçim: o term'de aday olan düğümler ve (varsa) kazanan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Election {
    /// Bu term'de aday olarak gözlenen düğümler.
    pub candidates: BTreeSet<NodeId>,
    /// Bu term'in lideri (seçilemediyse `None`).
    pub leader: Option<NodeId>,
}

/// Bir düğümün belleği ile diske yazdırdığı en son durum arasındaki ilk fark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DurabilityMismatch {
    /// `currentTerm` farklı.
    #[error("current term {memory:?} in memory, {disk:?} written")]
    Term {
        /// Bellekteki değer.
        memory: Term,
        /// Yazdırılmış değer.
        disk: Term,
    },
    /// `votedFor` farklı.
    #[error("vote {memory:?} in memory, {disk:?} written")]
    Vote {
        /// Bellekteki değer.
        memory: Option<NodeId>,
        /// Yazdırılmış değer.
        disk: Option<NodeId>,
    },
    /// Log uzunluğu farklı.
    #[error("{memory} log entries in memory, {disk} written")]
    LogLength {
        /// Bellekteki uzunluk.
        memory: u64,
        /// Yazdırılmış uzunluk.
        disk: u64,
    },
    /// Log'un bir girdisi farklı.
    #[error("log entry {index:?} differs between memory and the written state")]
    LogEntry {
        /// Farklı ilk girdinin index'i.
        index: LogIndex,
    },
}

/// Bir invariant ihlali.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Violation {
    /// Aynı term'de iki lider (Figure 3, Election Safety).
    #[error(transparent)]
    ElectionSafety(#[from] ElectionSafetyViolation),
    /// Bir lider kendi log'unu değiştirdi (Figure 3, Leader Append-Only).
    #[error(transparent)]
    LeaderAppendOnly(#[from] LeaderAppendOnlyViolation),
    /// Aynı (index, term) iki log'da farklı (Figure 3, Log Matching).
    #[error(transparent)]
    LogMatching(#[from] LogMatchingViolation),
    /// Commit edilmiş bir girdi kayboldu ya da değişti (Figure 3, Leader Completeness).
    #[error(transparent)]
    LeaderCompleteness(#[from] LeaderCompletenessViolation),
    /// Aynı index'te farklı komut uygulandı ya da sıra bozuldu (Figure 3, State Machine Safety).
    #[error(transparent)]
    StateMachineSafety(#[from] StateMachineSafetyViolation),
    /// Ayaktaki bir düğümün belleği, diske yazdırdığı en son durumdan farklı: bir değişiklik
    /// persist edilmeden kalmış.
    #[error("durability violated: node {node:?}: {mismatch}")]
    Durability {
        /// Düğüm.
        node: NodeId,
        /// İlk fark.
        mismatch: DurabilityMismatch,
    },
    /// Bir adım, aynı adımın bir `Send` ya da `Apply`'ından SONRA `Persist` üretti: o çıktılar
    /// durum kalıcı olmadan dışarı çıktı (Figure 2: "cevap vermeden önce kalıcı depoya yaz"; O1).
    #[error("output order violated: node {node:?} persisted after sending or applying in one step")]
    PersistAfterOutput {
        /// Düğüm.
        node: NodeId,
    },
    /// Bir düğüm, commit ettiği gözlenen bir index'ten itibaren log'unu yeniden yazdı (§5.3: silme
    /// yalnızca çakışmada olur ve commit edilmiş bir girdi çakışamaz).
    #[error(
        "committed entry rewritten: node {node:?} rewrote its log from {from:?} after committing \
         up to {commit_index:?}"
    )]
    CommittedEntryRewritten {
        /// Düğüm.
        node: NodeId,
        /// Yazmanın değiştirdiği ilk index.
        from: LogIndex,
        /// Düğümün yazmadan önce commit ettiği gözlenen son index.
        commit_index: LogIndex,
    },
}

/// `RaftCluster` üzerindeki bir işlemin hatası.
///
/// `Violation` uygulamanın bir invariant'ı çiğnediği anlamına gelir (bulunmak istenen hata);
/// diğer varyantlar kümenin yanlış kullanıldığını söyler (ör. var olmayan bir düğümü çökertmek).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterError {
    /// Bir invariant çiğnendi.
    #[error("invariant violated at t={time}: {violation}")]
    Violation {
        /// İhlalin görüldüğü mantıksal zaman.
        time: u64,
        /// İhlalin kendisi.
        violation: Violation,
    },
    /// Geçersiz bir çökme, yeniden başlatma ya da istemci isteği.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// Geçersiz bir bölünme tanımı.
    #[error(transparent)]
    Partition(#[from] PartitionError),
}

/// Bir Raft kümesinin ayarları.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClusterConfig {
    /// Düğüm sayısı: kimlikler `1..=size`.
    pub size: u64,
    /// Ağ.
    pub network: NetworkConfig,
    /// Her düğümün Raft ayarları.
    pub raft: Config,
    /// Simüle disk.
    pub disk: DiskConfig,
}

impl ClusterConfig {
    /// `size` düğümlü, verilen ağla, varsayılan Raft ve disk ayarlarıyla bir küme.
    #[must_use]
    pub fn new(size: u64, network: NetworkConfig) -> Self {
        Self {
            size,
            network,
            raft: Config::default(),
            disk: DiskConfig::default(),
        }
    }
}

/// Bir düğümün son gözlenen hâli: yeni liderlikleri ve commitIndex ilerlemelerini fark etmek için.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Observed {
    // Lider olarak gözlendiği son term (lider değilse `None`).
    leader_term: Option<Term>,
    // Kâhinlere görünen son commitIndex (bkz. `visible_commit`).
    commit_index: LogIndex,
}

/// Raft düğümlerinden oluşan ve her olaydan sonra invariant'ları denetleyen simülasyon.
///
/// Bütün rastgelelik tek bir ana seed'den türetilir: ağ `Component::Network`, disk
/// `Component::Disk`, aynı anlı tick'lerin sırası `Component::Schedule`, her düğüm
/// `Component::Node(id)` akışını alır. Aynı seed, aynı ayarlar ve aynı çağrılar (çökme, bölünme,
/// istemci istekleri, ...) birebir aynı koşuyu verir.
///
/// ```
/// use sim::{ClusterConfig, KvCommand, NetworkConfig, RaftCluster};
///
/// let mut cluster = RaftCluster::new(42, ClusterConfig::new(3, NetworkConfig::reliable(2)))?;
/// // Her olaydan sonra beş güvenlik invariant'ı ve kümenin diğer denetimleri (dayanıklılık, çıktı
/// // sırası, commit edilmiş girdilerin korunması) koşar; ihlal bir hata olarak döner.
/// cluster.run_until(200)?;
/// let (leader, _) = cluster.leaders()[0];
/// cluster.submit(leader, KvCommand::Put { key: b"k".to_vec(), value: b"v".to_vec() })?;
/// cluster.run_until(300)?;
/// for id in cluster.node_ids() {
///     assert_eq!(cluster.kv(id).and_then(|kv| kv.get(b"k")), Some(&b"v"[..]));
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct RaftCluster {
    sim: Simulation<RaftNode, SimNetwork>,
    election_safety: ElectionSafety,
    append_only: LeaderAppendOnly,
    log_matching: LogMatching,
    completeness: LeaderCompleteness,
    state_machine: StateMachineSafety,
    // BTreeMap: term ve düğüm sırasıyla gezilir, her koşuda aynı sırayla.
    elections: BTreeMap<Term, Election>,
    stores: BTreeMap<NodeId, KvStore>,
    observed: BTreeMap<NodeId, Observed>,
}

impl RaftCluster {
    /// `1..=size` kimlikli düğümlerden oluşan bir küme kurar. Her düğüm boş bir diskle açılan bir
    /// Follower'dır ve ilk tick'ini 1 anında alır; aynı anlı tick'lerin sırası seed'e bağlıdır.
    ///
    /// # Errors
    ///
    /// Ağ ya da disk ayarları geçersizse [`ConfigError`].
    pub fn new(master_seed: u64, config: ClusterConfig) -> Result<Self, ConfigError> {
        let seeds = SeedTree::new(master_seed);
        let ids: BTreeSet<NodeId> = (1..=config.size).map(NodeId).collect();
        let network = SimNetwork::new(config.network, seeds.rng_for(Component::Network))?;
        let options = SimOptions {
            disk: SimDisk::new(config.disk, seeds.rng_for(Component::Disk))?,
            tick_order: TickOrder::Shuffled(Box::new(seeds.rng_for(Component::Schedule))),
        };
        let nodes = ids.iter().map(|&id| {
            let seed = seeds.seed_bytes_for(Component::Node(id));
            (id, RaftNode::new(id, ids.clone(), config.raft, seed))
        });
        let sim = Simulation::with_options(SimConfig::default(), network, nodes, options)?;
        Ok(Self {
            sim,
            election_safety: ElectionSafety::new(),
            append_only: LeaderAppendOnly::new(),
            log_matching: LogMatching::new(),
            completeness: LeaderCompleteness::new(),
            state_machine: StateMachineSafety::new(),
            elections: BTreeMap::new(),
            stores: ids.iter().map(|&id| (id, KvStore::default())).collect(),
            observed: BTreeMap::new(),
        })
    }

    /// Alttaki simülasyon (salt okunur): trace, zaman, düğümler, diskler.
    #[must_use]
    pub fn sim(&self) -> &Simulation<RaftNode, SimNetwork> {
        &self.sim
    }

    /// Şu anki mantıksal zaman.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.sim.now()
    }

    /// Kümedeki düğümlerin kimlikleri, sırasıyla.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.sim.hosts().map(|host| host.id)
    }

    /// Bir düğüm. Çökmüş bir düğümün bellek durumu yeniden başlatmaya kadar donmuştur.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&RaftNode> {
        self.sim.node(id)
    }

    /// Düğüm ayakta mı?
    #[must_use]
    pub fn is_up(&self, id: NodeId) -> bool {
        self.sim.is_up(id)
    }

    /// Bir düğümün KV durum makinesi: o düğümün uyguladığı komutların sonucu. Çökmede sıfırlanır ve
    /// yeniden başlatmadan sonra yeniden uygulanan girdilerle kurulur.
    #[must_use]
    pub fn kv(&self, id: NodeId) -> Option<&KvStore> {
        self.stores.get(&id)
    }

    /// Şu an lider rolündeki ayaktaki düğümler ve term'leri, `NodeId` sırasıyla. Bölünme sırasında
    /// birden fazla olabilir: azınlıkta kalan eski bir lider, daha yüksek bir term görene kadar
    /// kendini lider sanır (farklı term'lerde oldukları için Election Safety ihlali değildir).
    #[must_use]
    pub fn leaders(&self) -> Vec<(NodeId, Term)> {
        self.sim
            .hosts()
            .filter(|host| host.up && host.node.role() == Role::Leader)
            .map(|host| (host.id, host.node.current_term()))
            .collect()
    }

    /// Koşu boyunca gözlenen seçimler, term sırasıyla.
    #[must_use]
    pub fn elections(&self) -> &BTreeMap<Term, Election> {
        &self.elections
    }

    /// Kuyruktaki en erken olayı işler ve invariant'ları denetler. Kuyruk boşsa `Ok(false)`.
    ///
    /// # Errors
    ///
    /// Olaydan sonra bir invariant çiğnenmişse [`ClusterError::Violation`].
    pub fn step(&mut self) -> Result<bool, ClusterError> {
        let progressed = self.sim.step();
        if progressed {
            self.check()?;
        }
        Ok(progressed)
    }

    /// `time` anına kadar bütün olayları işler; HER olaydan sonra invariant'ları denetler.
    ///
    /// # Errors
    ///
    /// Bir olaydan sonra bir invariant çiğnenmişse [`ClusterError::Violation`]; koşu o olayda
    /// durur.
    pub fn run_until(&mut self, time: u64) -> Result<(), ClusterError> {
        while self.sim.step_until(time) {
            self.check()?;
        }
        Ok(())
    }

    /// Bir düğümü çökertir (bkz. [`Simulation::crash`]). Düğümün durum makinesi de çökmeyle
    /// kaybolur: KV tablosu boşaltılır ve yeniden başlatmadan sonra girdiler 1'den yeniden
    /// uygulanır. Kâhinler düğümün geçici durumunu (commitIndex, uygulama sırası) ve Leader
    /// Append-Only'nin onun liderliklerinden sakladığı log görüntülerini unutur; Election
    /// Safety'nin term başına lider kaydı ise korunur. Ardından invariant'lar HEMEN denetlenir:
    /// hiçbir düğüm adımlanmaz, ama çökmede bekleyen yazmaların bir öneki diske ulaşmış sayılabilir
    /// ve diskteki log değişir.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da zaten çökmüşse [`ClusterError::Lifecycle`]; diske ulaşan önek bir
    /// invariant'ı çiğnerse [`ClusterError::Violation`].
    pub fn crash(&mut self, id: NodeId) -> Result<(), ClusterError> {
        self.sim.crash(id)?;
        self.stores.insert(id, KvStore::default());
        self.state_machine.observe_restart(id.0);
        self.completeness.observe_restart(id.0);
        self.append_only.observe_restart(id.0);
        self.observed.insert(id, Observed::default());
        self.check()
    }

    /// Çökmüş bir düğümü diskindeki durumla yeniden başlatır (bkz. [`Simulation::restart`]) ve
    /// invariant'ları HEMEN denetler: yeniden başlatma da bir düğüm adımıdır. Örneğin kalıcı durumu
    /// yanlış yükleyen bir düğüm, diskten farklı bir bellek durumuyla açılır ve bu anında
    /// yakalanır.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da zaten ayaktaysa [`ClusterError::Lifecycle`]; yeniden başlatmadan sonra bir
    /// invariant çiğnenmişse [`ClusterError::Violation`].
    pub fn restart(&mut self, id: NodeId) -> Result<(), ClusterError> {
        self.sim.restart(id)?;
        self.check()
    }

    /// Ayaktaki bir düğüme bir KV komutu verir ve invariant'ları denetler. Komutu yalnızca lider
    /// kabul eder; lider olmayan düğüm onu yok sayar (Faz 4'te `NotLeader` cevabı gelecek).
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da çökmüşse [`ClusterError::Lifecycle`]; ardından bir invariant çiğnenmişse
    /// [`ClusterError::Violation`].
    pub fn submit(&mut self, id: NodeId, command: KvCommand) -> Result<(), ClusterError> {
        self.sim.submit(id, command.encode())?;
        self.check()
    }

    /// Ağı gruplara böler (bkz. [`Simulation::partition`]).
    ///
    /// # Errors
    ///
    /// Bölünme tanımı geçersizse [`ClusterError::Partition`].
    pub fn partition(&mut self, groups: &[&[NodeId]]) -> Result<(), ClusterError> {
        Ok(self.sim.partition(groups)?)
    }

    /// Bölünmeyi kaldırır.
    pub fn heal(&mut self) {
        self.sim.heal();
    }

    /// Son olaydan sonra bütün invariant'ları denetler ve gözlem kayıtlarını günceller.
    fn check(&mut self) -> Result<(), ClusterError> {
        let time = self.sim.now();
        let violation = |violation: Violation| ClusterError::Violation { time, violation };

        if let Some(&node) = self.sim.take_persists_after_output().first() {
            return Err(violation(Violation::PersistAfterOutput { node }));
        }
        // Bu olayda VERİLEN yazmalar: yazma veren her düğüm (log'u değişmese bile) tam dayanıklılık
        // denetiminden geçer; log'u değiştiren yazmalar Leader Append-Only'ye ve commit edilmiş
        // girdilerin korunmasına girer. Bu olayda KALICI olan yazmalar: Log Matching yalnızca
        // diskteki log'u değişen düğümlere ve değişen kısma bakar.
        let written = log_changes(self.sim.take_writes());
        let synced = log_changes(self.sim.take_synced());

        // Bırakılan uygulamalar: State Machine Safety ve KV. Çözülemeyen bir komut KV'yi
        // değiştirmez (bkz. `KvStore::apply`); her düğüm onu aynı biçimde yok sayar.
        for (id, applied) in self.sim.take_applied() {
            self.state_machine
                .observe_apply(id.0, applied.index.0, applied.command.as_bytes())
                .map_err(|v| violation(v.into()))?;
            let _ = self.stores.entry(id).or_default().apply(&applied.command);
        }

        let mut newly_committed: Vec<u64> = Vec::new();
        let mut new_leaders: Vec<NodeId> = Vec::new();
        for host in self.sim.hosts() {
            let id = host.id;
            let disk_changed_from = synced.get(&id).copied().flatten();
            // Çökmüş bir düğümün diski de denetlenir: çökmede diske ulaşmış sayılan önek de
            // kalıcıdır ve düğüm onunla açılacaktır.
            if let Some(from) = disk_changed_from {
                self.log_matching
                    .observe(id.0, &entry_views(&host.disk.log), from.0)
                    .map_err(|v| violation(v.into()))?;
            }
            // Çökmüş bir düğümün bellek durumu yoktur (yeniden başlatmada diskten kurulur); eski
            // rolü, term'i ve commitIndex'i denetlenmez. Düğüm açılınca yeniden denetime girer.
            if !host.up {
                continue;
            }
            let node = host.node;
            let wrote = written.get(&id).copied();
            if let Some(mismatch) = durability_mismatch(node, host.latest, wrote.is_some()) {
                return Err(violation(Violation::Durability { node: id, mismatch }));
            }
            let log_written_from = wrote.flatten();
            let observed = self.observed.entry(id).or_default();
            // `observed.commit_index` bu adımdan önceki görünen değerdir ve düğümün o anki gerçek
            // commitIndex'inden büyük olamaz: aşağıdaki karşılaştırma yalnızca gerçekten commit
            // edilmiş bir girdiyi yeniden yazan adımı yakalar. İçerik aynı kalsa bile bu bir
            // hatadır: çakışma olmadan silinmiştir.
            if let Some(from) = log_written_from
                && from <= observed.commit_index
            {
                return Err(violation(Violation::CommittedEntryRewritten {
                    node: id,
                    from,
                    commit_index: observed.commit_index,
                }));
            }

            let term = node.current_term();
            match node.role() {
                Role::Leader => {
                    self.election_safety
                        .observe_leader(term.0, id.0)
                        .map_err(|v| violation(v.into()))?;
                    self.elections.entry(term).or_default().leader = Some(id);
                    let new_leadership = observed.leader_term != Some(term);
                    // Bellekteki log, yazmanın verildiği adımda (bkz. modül belgesi).
                    if new_leadership || log_written_from.is_some() {
                        self.append_only
                            .observe(term.0, id.0, &entry_views(node.log()))
                            .map_err(|v| violation(v.into()))?;
                    }
                    if new_leadership {
                        new_leaders.push(id);
                    }
                    observed.leader_term = Some(term);
                }
                // Aday kaydı yalnızca tanılama içindir (ör. bölünmüş oyları saymak) ve hiçbir
                // güvenlik denetimine girmez; bellekteki rol olduğu gibi kaydedilir.
                Role::Candidate => {
                    self.elections
                        .entry(term)
                        .or_default()
                        .candidates
                        .insert(id);
                    observed.leader_term = None;
                }
                Role::Follower => observed.leader_term = None,
            }

            let visible = visible_commit(node, host.disk, observed.commit_index);
            if visible != observed.commit_index {
                // Görünen kısımda bellekteki ve diskteki log aynıdır. Kâhine bellekteki log
                // verilir: log'un ötesine geçen bir commitIndex (bir hata) ona göre bildirilsin.
                let mut newly = self
                    .completeness
                    .observe_commit(id.0, term.0, visible.0, &entry_views(node.log()))
                    .map_err(|v| violation(v.into()))?;
                newly_committed.append(&mut newly);
                observed.commit_index = visible;
            }
        }

        // Leader Completeness: yeni bir lider o ana kadar commit edilmiş bütün girdileri, var olan
        // liderler de bu olayda yeni commit edilenleri diskteki log'larında taşımalı.
        if new_leaders.is_empty() && newly_committed.is_empty() {
            return Ok(());
        }
        for host in self.sim.hosts() {
            let term = host.node.current_term();
            if !host.up || host.node.role() != Role::Leader {
                continue;
            }
            let views = entry_views(&host.disk.log);
            let result = if new_leaders.contains(&host.id) {
                self.completeness.check_leader(host.id.0, term.0, &views)
            } else {
                self.completeness.check_entries(
                    host.id.0,
                    term.0,
                    &views,
                    newly_committed.iter().copied(),
                )
            };
            result.map_err(|v| violation(v.into()))?;
        }
        Ok(())
    }
}

/// Yazmaların log'u değiştirdiği ilk index, düğüm başına: `Some(i)`, log `i`'den itibaren değişti;
/// `None`, düğüm yazdı ama log'u değişmedi (yalnızca term ya da oy).
fn log_changes(updates: Vec<(NodeId, PersistUpdate)>) -> BTreeMap<NodeId, Option<LogIndex>> {
    let mut changes: BTreeMap<NodeId, Option<LogIndex>> = BTreeMap::new();
    for (id, update) in updates {
        let from = changes.entry(id).or_insert(None);
        if let Some(log) = &update.log {
            *from = Some(from.map_or(log.from, |earlier| earlier.min(log.from)));
        }
    }
    changes
}

/// Bir düğümün commitIndex'inin kâhinlere görünen kısmı (bkz. modül belgesi).
///
/// Bellekteki commitIndex, diske ulaşmamış girdileri kapsayabilir. Tek düğümlü bir küme girdiyi
/// eklediği adımda commit eder. Bir takipçi ise diskinde hâlâ eski (silinmeyi bekleyen) bir girdi
/// duran bir index'i commit etmiş olabilir. Görünen kısım, commitIndex'in diskteki log'un
/// bellekteki log'la aynı olduğu önekle sınırlanmış hâlidir. Diskteki log'un uzunluğuyla sınırlamak
/// yetmezdi: takipçi örneğinde diskteki eski girdi commit edilmiş sanılırdı.
///
/// `observed` (önceki görünen değer) ve öncesi önceki gözlemlerde karşılaştırıldı; yalnızca sonrası
/// karşılaştırılır, yani maliyet yeni commit edilen girdi sayısıyla sınırlıdır. Karşılaştırılmış
/// önekin sonra da aynı kaldığı varsayılır. Tek istisna zararsızdır: bekleyen iki yazma bir girdiyi
/// önce başka bir girdiyle değiştirip sonra geri koyarsa, disk bir an görünen commit'in içindeki
/// bir index'te başka bir girdi taşır. Ama o girdi kümede gerçekten commit edilmiştir ve düğümün
/// commitIndex'i zaten geçicidir (çökmede sıfırlanır). İki durumda değer sınırlanmadan kâhine
/// gider, çünkü ikisi de bir hatadır ve kâhin onları bildirir: commitIndex geri gittiyse (yeniden
/// başlatma dışında) ya da bellekteki log'un ötesine geçtiyse.
fn visible_commit(node: &RaftNode, disk: &PersistentState, observed: LogIndex) -> LogIndex {
    let commit = node.commit_index();
    let memory = node.log();
    let beyond_log = u64::try_from(memory.len()).is_ok_and(|length| commit.0 > length);
    if commit <= observed || beyond_log {
        return commit;
    }
    let start = usize::try_from(observed.0).unwrap_or(usize::MAX);
    let end = usize::try_from(commit.0).unwrap_or(usize::MAX);
    let fresh = memory.get(start..end).unwrap_or_default();
    let on_disk = disk.log.get(start..).unwrap_or_default();
    let agreeing = fresh
        .iter()
        .zip(on_disk)
        .take_while(|(in_memory, durable)| in_memory == durable)
        .count();
    LogIndex(
        observed
            .0
            .saturating_add(u64::try_from(agreeing).unwrap_or(u64::MAX)),
    )
}

/// Düğümün belleği ile diske yazdırdığı en son durum arasındaki ilk fark.
///
/// Term, oy ve log uzunluğu her olayda karşılaştırılır. Log'un içeriği, düğüm bu olayda bir yazma
/// verdiyse baştan sona; vermediyse yalnızca son girdisi karşılaştırılır. Neden: yazma vermeyen bir
/// adım log'u değiştirmemelidir ve log'u değiştiren hatalar (ekleme, kesme, kuyruk değiştirme)
/// uzunluğu ya da son girdiyi değiştirir; her olayda bütün log'u karşılaştırmak ise log boyu × olay
/// sayısı kadar iş olurdu. Ortadaki bir girdiyi sessizce değiştiren bir hata, düğümün bir sonraki
/// yazmasında tam karşılaştırmaya takılır.
fn durability_mismatch(
    node: &RaftNode,
    latest: &PersistentState,
    wrote: bool,
) -> Option<DurabilityMismatch> {
    let length = |log: &[LogEntry]| u64::try_from(log.len()).unwrap_or(u64::MAX);
    if node.current_term() != latest.current_term {
        return Some(DurabilityMismatch::Term {
            memory: node.current_term(),
            disk: latest.current_term,
        });
    }
    if node.voted_for() != latest.voted_for {
        return Some(DurabilityMismatch::Vote {
            memory: node.voted_for(),
            disk: latest.voted_for,
        });
    }
    let memory = node.log();
    if memory.len() != latest.log.len() {
        return Some(DurabilityMismatch::LogLength {
            memory: length(memory),
            disk: length(&latest.log),
        });
    }
    let first_difference = if wrote {
        memory
            .iter()
            .zip(&latest.log)
            .position(|(in_memory, on_disk)| in_memory != on_disk)
    } else {
        (memory.last() != latest.log.last()).then(|| memory.len().saturating_sub(1))
    };
    first_difference.map(|position| DurabilityMismatch::LogEntry {
        index: LogIndex(length(&memory[..position]).saturating_add(1)),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        AppliedEntry, ClusterConfig, ClusterError, DurabilityMismatch, RaftCluster, Violation,
    };
    use crate::disk::DiskConfig;
    use crate::kv::KvCommand;
    use crate::network::NetworkConfig;
    use crate::trace::{TraceKind, digest};
    use checker::EntryView;
    use raft_core::{
        AppendEntries, AppendEntriesResponse, Command, LogEntry, LogIndex, LogUpdate, Message,
        NodeId, PersistUpdate, PersistentState, RequestVote, RequestVoteResponse, Term,
    };

    fn entry(term: u64, byte: u8) -> LogEntry {
        LogEntry {
            term: Term(term),
            command: Command::new(vec![byte]),
        }
    }

    fn put(key: &[u8]) -> KvCommand {
        KvCommand::Put {
            key: key.to_vec(),
            value: b"v".to_vec(),
        }
    }

    /// Verilen diskli, güvenilir ağlı 3 düğümlü bir küme.
    fn cluster_with_disk(seed: u64, disk: DiskConfig) -> RaftCluster {
        let config = ClusterConfig {
            disk,
            ..ClusterConfig::new(3, NetworkConfig::reliable(1))
        };
        RaftCluster::new(seed, config).expect("valid config")
    }

    /// Gecikmesiz diskli, güvenilir ağlı 3 düğümlü bir küme.
    fn quiet_cluster(seed: u64) -> RaftCluster {
        cluster_with_disk(seed, DiskConfig::instant())
    }

    /// fsync'i tam 3 tick süren, çökmede bekleyen yazmaların bir öneğini verilen olasılıkla
    /// diske ulaştıran disk.
    fn slow_disk(partial_write_prob: f64) -> DiskConfig {
        DiskConfig {
            min_fsync_delay: 3,
            max_fsync_delay: 3,
            partial_write_prob,
        }
    }

    /// Lider seçilene kadar koşturur ve lideri döndürür.
    fn elect(cluster: &mut RaftCluster) -> (NodeId, Term) {
        cluster.run_until(200).expect("no violation");
        cluster.leaders()[0]
    }

    fn violation_of(error: ClusterError) -> Violation {
        match error {
            ClusterError::Violation { violation, .. } => violation,
            other => panic!("expected an invariant violation, got {other:?}"),
        }
    }

    // Kanonik kodlama her alanı kapsar: yalnızca tek bir alanda (ya da yalnızca varyantta) farklı
    // olan mesajların, disk kayıtlarının, farkların ve uygulamaların özetleri farklıdır.
    #[test]
    fn encodings_distinguish_every_field() {
        fn assert_distinct(digests: Vec<u64>) {
            let count = digests.len();
            let mut unique = digests;
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(unique.len(), count);
        }
        let request = |term, index, last_term| {
            Message::RequestVote(RequestVote {
                term: Term(term),
                last_log_index: LogIndex(index),
                last_log_term: Term(last_term),
            })
        };
        let append = |term, prev: (u64, u64), entries: Vec<LogEntry>, commit| {
            Message::AppendEntries(AppendEntries {
                term: Term(term),
                prev_log_index: LogIndex(prev.0),
                prev_log_term: Term(prev.1),
                entries,
                leader_commit: LogIndex(commit),
            })
        };
        let reply = |success, match_index| {
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(1),
                success,
                match_index: LogIndex(match_index),
            })
        };
        let messages = [
            request(1, 0, 0),
            request(2, 0, 0),
            request(1, 1, 0),
            request(1, 0, 1),
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(1),
                vote_granted: false,
            }),
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(1),
                vote_granted: true,
            }),
            append(1, (0, 0), Vec::new(), 0),
            append(2, (0, 0), Vec::new(), 0),
            append(1, (1, 0), Vec::new(), 0),
            append(1, (0, 1), Vec::new(), 0),
            append(1, (0, 0), Vec::new(), 1),
            append(1, (0, 0), vec![entry(1, 1)], 0),
            append(1, (0, 0), vec![entry(2, 1)], 0),
            append(1, (0, 0), vec![entry(1, 2)], 0),
            append(1, (0, 0), vec![entry(1, 1), entry(1, 1)], 0),
            reply(false, 0),
            reply(true, 0),
            reply(true, 1),
        ];
        assert_distinct(messages.iter().map(digest).collect());

        let state = |term, vote: Option<u64>, log: Vec<LogEntry>| PersistentState {
            current_term: Term(term),
            voted_for: vote.map(NodeId),
            log,
        };
        let states = [
            state(0, None, Vec::new()),
            state(0, Some(0), Vec::new()),
            state(1, None, Vec::new()),
            state(0, Some(1), Vec::new()),
            state(0, None, vec![entry(1, 1)]),
            state(0, None, vec![entry(1, 2)]),
        ];
        assert_distinct(states.iter().map(digest).collect());

        let update = |vote: Option<u64>, log: Option<(u64, Vec<LogEntry>)>| PersistUpdate {
            current_term: Term(1),
            voted_for: vote.map(NodeId),
            log: log.map(|(from, entries)| LogUpdate {
                from: LogIndex(from),
                entries,
            }),
        };
        let updates = [
            update(None, None),
            update(Some(1), None),
            update(None, Some((1, Vec::new()))),
            update(None, Some((2, Vec::new()))),
            update(None, Some((1, vec![entry(1, 1)]))),
        ];
        assert_distinct(updates.iter().map(digest).collect());

        let applied = |index, byte| AppliedEntry {
            index: LogIndex(index),
            command: Command::new(vec![byte]),
        };
        assert_distinct(vec![
            digest(&applied(1, 1)),
            digest(&applied(2, 1)),
            digest(&applied(1, 2)),
            digest(&Command::new(vec![1])),
            digest(&Command::new(vec![1, 1])),
        ]);
    }

    // Dayanıklılık denetimi gerçekten çalışır: bir düğümün yazdırdığı durum bellekten ayrılırsa
    // (sanki bir adım `votedFor`'u değiştirip persist etmeyi unutmuş gibi), bir sonraki olayda
    // ihlal bildirilir; bu olay o düğüme ait olmasa bile.
    #[test]
    fn a_disk_that_diverges_from_memory_is_reported() {
        let mut cluster = quiet_cluster(1);
        cluster.run_until(5).expect("no violation");
        cluster
            .sim
            .latest_mut(NodeId(2))
            .expect("node 2 exists")
            .voted_for = Some(NodeId(3));
        let error = cluster.step().expect_err("the divergence must be reported");
        assert_eq!(
            violation_of(error),
            Violation::Durability {
                node: NodeId(2),
                mismatch: DurabilityMismatch::Vote {
                    memory: None,
                    disk: Some(NodeId(3)),
                },
            }
        );
    }

    // Çıktı sırası denetimi her olaya bağlı: simülatörün ters sıra kaydı (sanki bir düğüm önce
    // gönderip sonra persist etmiş gibi) bir sonraki olayda ihlal olarak bildirilir.
    #[test]
    fn a_persist_after_an_output_is_reported() {
        let mut cluster = quiet_cluster(6);
        cluster.run_until(5).expect("no violation");
        cluster.sim.record_persist_after_output(NodeId(2));
        let error = cluster
            .step()
            .expect_err("the order violation must be reported");
        assert_eq!(
            violation_of(error),
            Violation::PersistAfterOutput { node: NodeId(2) }
        );
    }

    // Commit edilmiş girdilerin korunması her yazmada denetlenir: liderin 1. index'i commit ettiği
    // gözlenmiş sayılır (sahte gözlem). Liderin ilk girdisini ekleyen yazması 1. index'ten
    // başladığı için commit edilmiş bir girdiyi yeniden yazmış sayılır.
    #[test]
    fn rewriting_a_committed_entry_is_reported() {
        let mut cluster = quiet_cluster(7);
        let (leader, _) = elect(&mut cluster);
        cluster.observed.entry(leader).or_default().commit_index = LogIndex(1);
        let error = cluster
            .submit(leader, put(b"k"))
            .expect_err("the leader's first write starts at index 1");
        assert_eq!(
            violation_of(error),
            Violation::CommittedEntryRewritten {
                node: leader,
                from: LogIndex(1),
                commit_index: LogIndex(1),
            }
        );
    }

    // Election Safety gerçekten her adıma bağlı: kâhine sahte bir geçmiş yüklenir (term 1..=20'nin
    // lideri, var olmayan düğüm 99). Kümenin seçtiği ilk gerçek lider bu geçmişle çakışır ve ihlal
    // olarak bildirilir. `check()` içindeki gözlem silinir ya da hatası yutulursa bu test kırılır;
    // kâhinin kendi birim testleri bu bağı göremez.
    #[test]
    fn election_safety_is_checked_after_every_event() {
        let mut cluster = quiet_cluster(1);
        for term in 1..=20 {
            cluster
                .election_safety
                .observe_leader(term, 99)
                .expect("a fresh term");
        }
        let error = cluster
            .run_until(400)
            .expect_err("the first real leader collides with node 99");
        let Violation::ElectionSafety(violation) = violation_of(error) else {
            panic!("expected an election safety violation");
        };
        assert_eq!(violation.first, 99);
        assert!((1..=20).contains(&violation.term));
    }

    // Log Matching, diskteki log'u değiştiren her yazmada denetlenir (gecikmesiz diskte yazma
    // verildiği anda kalıcıdır): kâhin, her term'in 1. index'ine sahte bir komut görmüş sayılır;
    // liderin ilk girdisi bu kayıtla çakışır.
    #[test]
    fn log_matching_is_checked_on_every_durable_write() {
        let mut cluster = quiet_cluster(2);
        let fake = [0xee_u8];
        for term in 1..=20 {
            let view = [EntryView {
                term,
                command: &fake,
            }];
            cluster
                .log_matching
                .observe(99, &view, 1)
                .expect("a fresh entry");
        }
        let (leader, _) = elect(&mut cluster);
        let error = cluster
            .submit(leader, put(b"k"))
            .expect_err("the leader's first entry collides with the fake one");
        assert!(matches!(violation_of(error), Violation::LogMatching(_)));
    }

    // Leader Completeness her yeni lider için denetlenir: kâhin, 1. index'te term 0'da commit
    // edilmiş sahte bir girdi görmüş sayılır; ilk lider onu taşımadığı için seçildiği anda ihlal
    // bildirilir.
    #[test]
    fn leader_completeness_is_checked_for_every_new_leader() {
        let mut cluster = quiet_cluster(3);
        let fake = [0xee_u8];
        let view = [EntryView {
            term: 0,
            command: &fake,
        }];
        cluster
            .completeness
            .observe_commit(99, 0, 1, &view)
            .expect("a fresh commit");
        let error = cluster
            .run_until(400)
            .expect_err("the first leader lacks the committed entry");
        assert!(matches!(
            violation_of(error),
            Violation::LeaderCompleteness(_)
        ));
    }

    // State Machine Safety her uygulamada denetlenir: kâhin, 1. index'te sahte bir komutun
    // uygulandığını görmüş sayılır; kümenin ilk gerçek uygulaması bununla çakışır.
    #[test]
    fn state_machine_safety_is_checked_on_every_apply() {
        let mut cluster = quiet_cluster(4);
        cluster
            .state_machine
            .observe_apply(99, 1, &[0xee])
            .expect("a fresh index");
        let (leader, _) = elect(&mut cluster);
        cluster.submit(leader, put(b"k")).expect("no violation yet");
        let error = cluster
            .run_until(400)
            .expect_err("the first real apply collides with the fake one");
        assert!(matches!(
            violation_of(error),
            Violation::StateMachineSafety(_)
        ));
    }

    // Leader Append-Only, liderin log'unu değiştiren her yazmada ve yazmanın VERİLDİĞİ adımda
    // denetlenir: fsync'i 3 tick süren bir diskte, liderin bu term için en son gözlenen log'u sahte
    // bir girdi taşıyormuş gibi gösterilir. Liderin bir sonraki yazması onu içermediği için ihlal,
    // yazma kalıcı olmadan, isteğin verildiği adımda bildirilir. (Kalıcı olmayı beklemek, lider bu
    // arada liderliği bırakırsa ihlali gizleyebilirdi; bkz. modül belgesi.)
    #[test]
    fn leader_append_only_is_checked_when_a_leader_write_is_issued() {
        let mut cluster = cluster_with_disk(5, slow_disk(0.0));
        let (leader, term) = elect(&mut cluster);
        let fake = [0xee_u8];
        let view = [EntryView {
            term: term.0,
            command: &fake,
        }];
        cluster
            .append_only
            .observe(term.0, leader.0, &view)
            .expect("a fresh snapshot");
        let error = cluster
            .submit(leader, put(b"k"))
            .expect_err("the leader's log does not extend the fake snapshot");
        assert!(matches!(
            violation_of(error),
            Violation::LeaderAppendOnly(_)
        ));
    }

    // Çökmede diske ulaşan önek de, çökmenin kendisinde denetlenir: kâhin 1. index'te sahte bir
    // komut görmüş sayılır ve liderin ilk girdisi fsync penceresindeyken lider çöker. Kısmi yazma
    // girdiyi diske ulaştırırsa çakışma çökmede bildirilir; yazma kaybolursa ihlal yoktur. Seed
    // taraması iki durumu da görür.
    #[test]
    fn a_prefix_kept_by_a_crash_is_checked_at_the_crash() {
        let mut kept_counts = BTreeSet::new();
        for seed in 0..20 {
            let mut cluster = cluster_with_disk(seed, slow_disk(1.0));
            let (leader, term) = elect(&mut cluster);
            let fake = [0xee_u8];
            let view = [EntryView {
                term: term.0,
                command: &fake,
            }];
            cluster
                .log_matching
                .observe(99, &view, 1)
                .expect("a fresh entry");
            cluster
                .submit(leader, put(b"k"))
                .expect("the entry is not durable yet");
            let result = cluster.crash(leader);
            let loss =
                cluster
                    .sim
                    .trace()
                    .events()
                    .iter()
                    .rev()
                    .find_map(|event| match event.kind {
                        TraceKind::CrashLoss {
                            kept_writes,
                            lost_writes,
                            ..
                        } => Some((kept_writes, lost_writes)),
                        _ => None,
                    });
            // Öncül: çökme anında liderin bekleyen tek yazması girdinin yazmasıdır.
            assert!(
                loss.is_some_and(|(kept, lost)| kept + lost == 1),
                "seed {seed}: {loss:?}"
            );
            let kept = loss.map(|(kept, _)| kept);
            match result {
                Ok(()) => assert_eq!(kept, Some(0), "seed {seed}"),
                Err(error) => {
                    assert!(kept.is_some_and(|kept| kept > 0), "seed {seed}");
                    assert!(
                        matches!(violation_of(error), Violation::LogMatching(_)),
                        "seed {seed}"
                    );
                }
            }
            kept_counts.insert(kept);
        }
        assert!(kept_counts.contains(&Some(0)), "{kept_counts:?}");
        assert!(kept_counts.len() >= 2, "{kept_counts:?}");
    }
}
