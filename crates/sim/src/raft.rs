//! Raft adaptörü: `raft_core::RaftNode`'u simülatöre bağlar ve her olaydan sonra invariant'ları
//! denetleyen [`RaftCluster`]'ı sunar.
//!
//! Adaptörün iki görevi var:
//!
//! 1. `RaftNode` için [`SimNode`]'u uygular: `NodeInput`/`NodeOutput` ile raft-core'un
//!    `Input`/`Output`'u arasında birebir çeviri. Mesajlar, kalıcı durum ve farklar için kanonik
//!    trace kodlamaları da buradadır. İstemci isteği ya log'a yazılacak bir komut ya da log'a
//!    yazılmadan cevaplanacak bir okumadır ([`RaftRequest`]); `Ready` olan okumayı küme, düğümün
//!    KV durum makinesinden cevaplar (ReadIndex, tezin §6.4'ü).
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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use checker::{
    ElectionSafety, ElectionSafetyViolation, EntryView, LeaderAppendOnly,
    LeaderAppendOnlyViolation, LeaderCompleteness, LeaderCompletenessViolation, LogMatching,
    LogMatchingViolation, LogView, StateMachineSafety, StateMachineSafetyViolation,
};
use raft_core::{
    AppendEntries, AppendEntriesResponse, ClientResponse, Command, Config, Input, InstallSnapshot,
    LogEntry, LogIndex, LogUpdate, Message, NodeId, Output, PersistUpdate, PersistentState, Probe,
    ProbeResponse, RaftNode, ReadId, ReadOutcome, RequestVote, RequestVoteResponse, Role, Snapshot,
    Term,
};

use crate::disk::{DiskConfig, SimDisk};
use crate::error::{ConfigError, LifecycleError, PartitionError};
use crate::kv::{KvApplied, KvCommand, KvRequest, KvResult, KvStore};
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

/// Bir Raft düğümünün durum makinesine sırayla bıraktığı yerel etki (`NodeOutput::Apply`):
/// commit edilmiş bir girdinin uygulanması ya da liderden kurulan bir snapshot'ın durum
/// makinesinin yerine geçmesi (§7). İkisi aynı sıralı yoldan geçer: bir snapshot'tan sonraki
/// uygulamalar onun ardından gelir ve ikisi de kendilerinden önce verilmiş yazmalar kalıcı olana
/// kadar tutulur (O1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftApplied {
    /// Commit edilmiş bir girdi (`Output::Apply`).
    Entry(AppliedEntry),
    /// Durum makinesini değiştiren snapshot (`Output::Restore`).
    Restore(Snapshot),
}

/// Raft düğümüne verilen bir istemci isteği (`NodeInput::Client`): log'a yazılacak bir komut ya da
/// log'a yazılmadan cevaplanacak bir okuma (ReadIndex, tezin §6.4'ü).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftRequest {
    /// Log üzerinden geçen bir komut (`Input::ClientRequest`).
    Command(Command),
    /// Log'a girmeyen bir okuma (`Input::Read`).
    Read(ReadId),
    /// İstemci isteği değil, sürücünün kendi işi: durum makinesinin `index`'teki snapshot'ı
    /// (`Input::Compact`, §7). Aynı kanaldan verilir, çünkü simülatör bir düğüme dışarıdan
    /// yalnızca tick, mesaj, yeniden başlatma ve "istek" verebilir; bkz.
    /// [`RaftCluster::new`] (`snapshot_every`).
    Compact {
        /// Snapshot'ın kapsadığı son index.
        index: LogIndex,
        /// Durum makinesinin o index'teki hâli.
        data: Vec<u8>,
    },
}

/// Raft düğümünün istemciye cevabı (`NodeOutput::Reply`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftResponse {
    /// Bir komutun reddi (§8).
    NotLeader(NotLeaderReply),
    /// Bir okumanın sonucu (`Output::Read`).
    Read {
        /// Okumanın kimliği.
        id: ReadId,
        /// Sonuç.
        outcome: ReadOutcome,
    },
}

/// Lider olmayan bir düğümün bir istemci isteğine cevabı (`ClientResponse::NotLeader`, §8):
/// reddedilen istek ve düğümün bildiği lider.
///
/// İsteği adaptör ekler. Çekirdek istemcileri tanımaz (komutlar opaktır, C1); ama cevabı her zaman
/// o isteği işleyen adımda üretir. Adaptör o adımın girdisini bildiği için cevabı isteğe bağlar ve
/// sürücü, cevabın hangi `(client, seq)`'e ait olduğunu isteğin baytlarından okur. Cevap
/// yazmaların arkasında tutulup daha sonra bırakılsa bile bu bağ kopmaz.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLeaderReply {
    /// Reddedilen isteğin komutu.
    pub request: Command,
    /// Düğümün bildiği lider.
    pub hint: Option<NodeId>,
}

impl SimNode for RaftNode {
    type Msg = Message;
    type Durable = PersistentState;
    type Request = RaftRequest;
    type Applied = RaftApplied;
    type Response = RaftResponse;

    fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>> {
        // Cevabı isteğe bağlamak için komutun bir kopyası (bkz. `NotLeaderReply`). Okumanın cevabı
        // kimliğini zaten taşır.
        let request = match &input {
            NodeInput::Client(RaftRequest::Command(command)) => Some(command.clone()),
            NodeInput::Client(RaftRequest::Read(_) | RaftRequest::Compact { .. })
            | NodeInput::Tick
            | NodeInput::Message { .. }
            | NodeInput::Restart(_) => None,
        };
        let input = match input {
            NodeInput::Tick => Input::Tick,
            NodeInput::Message { from, msg } => Input::Message { from, msg },
            NodeInput::Restart(state) => Input::Restart(state),
            NodeInput::Client(RaftRequest::Command(command)) => Input::ClientRequest(command),
            NodeInput::Client(RaftRequest::Read(id)) => Input::Read(id),
            NodeInput::Client(RaftRequest::Compact { index, data }) => {
                Input::Compact { index, data }
            }
        };
        // `RaftNode::step` yazımı raft-core'un kendi `step`'ini çağırır (yerleşik metot, trait
        // metodundan önce gelir). Çıktılar sırası korunarak çevrilir (O1).
        RaftNode::step(self, input)
            .into_iter()
            .filter_map(|output| match output {
                Output::Send { to, msg } => Some(NodeOutput::Send { to, msg }),
                Output::Persist(update) => Some(NodeOutput::Persist(update)),
                Output::Apply { index, command } => {
                    Some(NodeOutput::Apply(RaftApplied::Entry(AppliedEntry {
                        index,
                        command,
                    })))
                }
                Output::Restore(snapshot) => {
                    Some(NodeOutput::Apply(RaftApplied::Restore(snapshot)))
                }
                // Çekirdek cevabı yalnızca bir istemci isteği adımında üretir (S1); başka bir
                // adımda üretseydi bağlanacak bir istek olmazdı ve cevap düşerdi. `_` kolu bilerek
                // yok: `Output`'a ya da cevaba yeni bir varyant eklenince burası derlenmez ve
                // adaptör bilinçli olarak güncellenir.
                Output::ClientResponse(ClientResponse::NotLeader { hint }) => {
                    request.clone().map(|request| {
                        NodeOutput::Reply(RaftResponse::NotLeader(NotLeaderReply { request, hint }))
                    })
                }
                Output::Read { id, outcome } => {
                    Some(NodeOutput::Reply(RaftResponse::Read { id, outcome }))
                }
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
/// Uzunluk `u64`'e her desteklenen platformda sığar; sığmasaydı yazılacak `u64::MAX`, okumaların
/// işaretiyle (`READ_MARK`) aynı olurdu. O yol pratikte erişilemez (2^64 baytlık bir komut yoktur).
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    out.extend_from_slice(bytes);
}

/// Seçimlik bir düğüm (oy, lider ipucu): 0 = yok, 1 + düğüm kimliği = var. Etiket baytı sayesinde
/// "yok" ile "düğüm 0" birbirine karışmaz.
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
            Message::Probe(Probe { term, round }) => {
                out.push(5);
                put_u64(out, term.0);
                put_u64(out, *round);
            }
            Message::ProbeResponse(ProbeResponse { term, round }) => {
                out.push(6);
                put_u64(out, term.0);
                put_u64(out, *round);
            }
            Message::InstallSnapshot(InstallSnapshot { term, snapshot }) => {
                out.push(7);
                put_u64(out, term.0);
                put_snapshot(out, snapshot);
            }
        }
    }
}

/// Bir snapshot: son index, son term ve uzunluk önekli veri.
fn put_snapshot(out: &mut Vec<u8>, snapshot: &Snapshot) {
    let Snapshot {
        last_index,
        last_term,
        data,
    } = snapshot;
    put_u64(out, last_index.0);
    put_u64(out, last_term.0);
    put_bytes(out, data);
}

impl TraceEncode for PersistentState {
    // Term, oy, log ve varsa snapshot. Desen yine `..` olmadan açılır. Snapshot yalnızca varken
    // kodlanır (sona, bir etiketle): snapshot'sız durumların kodlaması (ve onları kullanan
    // koşuların trace özetleri) snapshot'lar eklenmeden önceki hâliyle aynıdır; snapshot'lı bir
    // durumun kodlaması daha uzundur ve onlarla karışmaz.
    fn encode(&self, out: &mut Vec<u8>) {
        let PersistentState {
            current_term,
            voted_for,
            snapshot,
            log,
        } = self;
        put_u64(out, current_term.0);
        put_vote(out, *voted_for);
        put_entries(out, log);
        if let Some(snapshot) = snapshot {
            out.push(1);
            put_snapshot(out, snapshot);
        }
    }
}

impl TraceEncode for PersistUpdate {
    // Term, oy, varsa log farkı (0 = yok, 1 + `from` + girdiler = var) ve varsa snapshot (yalnızca
    // varken, sona; bkz. `PersistentState`'in kodlaması).
    fn encode(&self, out: &mut Vec<u8>) {
        let PersistUpdate {
            current_term,
            voted_for,
            snapshot,
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
        if let Some(snapshot) = snapshot {
            out.push(1);
            put_snapshot(out, snapshot);
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

impl TraceEncode for RaftApplied {
    // Girdi, `AppliedEntry`'nin kodlamasıyla (snapshot'sız koşuların özetleri değişmez); snapshot,
    // index yerinde kendi işaretiyle (`RESTORE_MARK`: o index'li bir girdi olamaz).
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            RaftApplied::Entry(entry) => entry.encode(out),
            RaftApplied::Restore(snapshot) => {
                put_u64(out, RESTORE_MARK);
                put_snapshot(out, snapshot);
            }
        }
    }
}

impl TraceEncode for NotLeaderReply {
    fn encode(&self, out: &mut Vec<u8>) {
        let NotLeaderReply { request, hint } = self;
        put_bytes(out, request.as_bytes());
        put_vote(out, *hint);
    }
}

/// Kodlamaların başındaki işaretler: bir uzunluk öneki ya da bir index'in yerinde, onların asla
/// alamayacağı değerler (2^64 - 1, - 2, - 3 baytlık bir komut ya da o index'li bir girdi olamaz).
/// Komutlar ve girdiler etiketsiz kalır, böylece okumasız ve snapshot'sız koşuların trace
/// özetleri bu türler eklenmeden önceki hâliyle birebir aynıdır. Her türün kendi işareti vardır:
/// bir kodlama başka bir türün kodlamasının öneki olamaz.
const READ_MARK: u64 = u64::MAX;
/// Sıkıştırma isteğinin işareti (bkz. `READ_MARK`).
const COMPACT_MARK: u64 = u64::MAX - 1;
/// Kurulan snapshot'ın işareti (bkz. `READ_MARK`).
const RESTORE_MARK: u64 = u64::MAX - 2;

impl TraceEncode for RaftRequest {
    // Komut: uzunluk önekli baytlar (önek hiçbir zaman bir işaret değildir). Okuma: kendi işareti
    // ve kimlik. Sıkıştırma: kendi işareti, index ve uzunluk önekli veri.
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            RaftRequest::Command(command) => command.encode(out),
            RaftRequest::Read(id) => {
                put_u64(out, READ_MARK);
                put_u64(out, id.0);
            }
            RaftRequest::Compact { index, data } => {
                put_u64(out, COMPACT_MARK);
                put_u64(out, index.0);
                put_bytes(out, data);
            }
        }
    }
}

impl TraceEncode for RaftResponse {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            RaftResponse::NotLeader(reply) => reply.encode(out),
            RaftResponse::Read { id, outcome } => {
                put_u64(out, READ_MARK);
                put_u64(out, id.0);
                match outcome {
                    ReadOutcome::Ready => out.push(0),
                    ReadOutcome::NotLeader { hint } => {
                        out.push(1);
                        put_vote(out, *hint);
                    }
                }
            }
        }
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

/// Bir istemci isteğinin bir düğümden aldığı cevap (bkz. [`RaftCluster::submit_request`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientReply {
    /// İstemci.
    pub client: u64,
    /// İsteğin oturumdaki sıra numarası.
    pub seq: u64,
    /// Cevabı veren düğüm (isteğin verildiği düğüm).
    pub node: NodeId,
    /// Cevap.
    pub outcome: ReplyOutcome,
}

/// Bir istemci cevabının içeriği.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyOutcome {
    /// Düğüm lider değildi; istek log'a eklenmedi (§8).
    NotLeader {
        /// Düğümün bildiği lider.
        hint: Option<NodeId>,
    },
    /// İstek commit edildi ve düğüm onu uyguladı.
    Done {
        /// Sonuç.
        result: KvResult,
        /// Sonuç oturumdan geldi: aynı istek daha önce uygulanmıştı (yeniden denenmiş bir istek,
        /// §8).
        duplicate: bool,
    },
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
    /// Snapshot farklı (§7): son index'leri ve term'leri (snapshot yoksa `None`). İkisi aynıysa
    /// veriler farklıdır.
    #[error(
        "snapshot (index, term) {memory:?} in memory, {disk:?} written (equal pairs: the data \
         differs)"
    )]
    Snapshot {
        /// Bellekteki snapshot'ın son index'i ve term'i.
        memory: Option<(LogIndex, Term)>,
        /// Yazdırılmış snapshot'ın son index'i ve term'i.
        disk: Option<(LogIndex, Term)>,
    },
    /// Log uzunluğu (snapshot'tan sonraki girdi sayısı) farklı.
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
    /// Bir düğümün durum makinesi bir index'te, o index'i uygulamış başka bir kopyanınkinden
    /// farklı (§7: bir snapshot, durum makinesinin o index'teki hâli olmalıdır; snapshot'tan
    /// kurulan bir kopya, girdileri tek tek uygulamış olanlarla aynı duruma gelmelidir).
    #[error(
        "snapshot safety violated: node {node:?} has a different state machine at index {index:?} \
         than node {first:?} had there"
    )]
    StateDiverged {
        /// Düğüm.
        node: NodeId,
        /// Index.
        index: LogIndex,
        /// O index'teki durumu ilk kaydeden kopya.
        first: NodeId,
    },
    /// Bir düğüme kurulan (ya da diskinden yüklenen) snapshot'ın verisi çözülemiyor.
    #[error(
        "snapshot safety violated: node {node:?} got a snapshot through {index:?} that cannot be \
         decoded"
    )]
    UndecodableSnapshot {
        /// Düğüm.
        node: NodeId,
        /// Snapshot'ın son index'i.
        index: LogIndex,
    },
}

impl Violation {
    /// İhlalin türünün kısa adı: bir hatayı türüyle tanımak için (ör. küçültmede "aynı hata"
    /// ölçütü, mutasyon tablosu).
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Violation::ElectionSafety(_) => "election safety",
            Violation::LeaderAppendOnly(_) => "leader append-only",
            Violation::LogMatching(_) => "log matching",
            Violation::LeaderCompleteness(_) => "leader completeness",
            Violation::StateMachineSafety(_) => "state machine safety",
            Violation::Durability { .. } => "durability",
            Violation::PersistAfterOutput { .. } => "output order",
            Violation::CommittedEntryRewritten { .. } => "committed entry rewritten",
            Violation::StateDiverged { .. } | Violation::UndecodableSnapshot { .. } => {
                "snapshot safety"
            }
        }
    }
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
    /// Snapshot sıklığı (§7): bir düğümün durum makinesi son snapshot'ından bu yana bu kadar girdi
    /// uyguladığında küme onu snapshot'a alır ve log'u sıkıştırır (`Input::Compact`). `None`:
    /// hiç sıkıştırılmaz (log sınırsız büyür).
    pub snapshot_every: Option<NonZeroU64>,
}

impl ClusterConfig {
    /// `size` düğümlü, verilen ağla, varsayılan Raft ve disk ayarlarıyla, sıkıştırmasız bir küme.
    #[must_use]
    pub fn new(size: u64, network: NetworkConfig) -> Self {
        Self {
            size,
            network,
            raft: Config::default(),
            disk: DiskConfig::default(),
            snapshot_every: None,
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

/// Bir düğümün bir andaki hâli: ayakta mı, rolü ve term'i (zaman çizelgesi için; bkz.
/// [`RaftCluster::timeline`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeStatus {
    /// Düğüm ayakta mı?
    pub up: bool,
    /// Rol. Çökmüş bir düğümün rolü, belleğinin çöktüğü andaki (donmuş) hâlidir.
    pub role: Role,
    /// Term.
    pub term: Term,
}

/// Zaman çizelgesinin bir kaydı: `at` anında `node` düğümünün hâli `status` oldu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusChange {
    /// An (tick).
    pub at: u64,
    /// Düğüm.
    pub node: NodeId,
    /// Yeni hâl.
    pub status: NodeStatus,
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
    // İstemci istekleri: düğüm başına, cevabı henüz verilmemiş denemeler (`(client, seq)` başına
    // sayı). Gerçek bir sistemde bunlar istemcinin o düğüme açık bağlantılarıdır; düğüm çökünce
    // kopar. Sayı tutulur (küme değil): aynı isteğin iki denemesi aynı düğüme gidebilir ve
    // birinin reddi, öbürünün (kabul edilmiş olabilecek) cevabını yutmamalıdır.
    pending: BTreeMap<NodeId, BTreeMap<(u64, u64), u32>>,
    // Üretilen ama henüz alınmamış istemci cevapları, üretilme sırasıyla.
    client_replies: Vec<ClientReply>,
    // `submit`'in iç oturumunun (istemci 0) bir sonraki sıra numarası.
    driver_seq: u64,
    // Cevap bekleyen okumalar (ReadIndex): düğüm ve okuma kimliği başına hangi istemcinin hangi
    // anahtarı okuduğu. Düğüm çökünce o düğümün okumaları düşer (bağlantı kopar).
    reads: BTreeMap<(NodeId, ReadId), ClientRead>,
    // Bir sonraki okuma kimliği: her deneme yeni bir kimlik alır.
    next_read: u64,
    // Snapshot sıklığı (bkz. `ClusterConfig::snapshot_every`).
    snapshot_every: Option<NonZeroU64>,
    // Her düğümün KV durum makinesinin uyguladığı son index (snapshot'tan kurulduysa snapshot'ın
    // sonu). Sıkıştırma bu index'te yapılır: snapshot, durum makinesinin o anki hâlidir.
    applied: BTreeMap<NodeId, LogIndex>,
    // Snapshot güvenliği (§7): index → o index uygulandıktan sonraki durum makinesinin parmak izi
    // (`KvStore::fingerprint`) ve onu ilk kaydeden kopya. Sonraki her kopya ve kurulan ya da
    // diskten yüklenen her snapshot aynı izi vermelidir. Yalnızca sıkıştırma açıkken tutulur:
    // snapshot'sız bir koşuda kopyaların durumlarının eşitliğini State Machine Safety (aynı
    // komutlar, aynı sıra) zaten verir.
    digests: BTreeMap<LogIndex, (u64, NodeId)>,
    // Sayaçlar: sıkıştırmalar, liderden kurulan snapshot'lar ve geldiği adımda cevaplanan
    // (kiralamalı) okumalar.
    compactions: u64,
    installs: u64,
    lease_reads: u64,
    // Zaman çizelgesi (bkz. `timeline`) ve her düğümün en son kaydedilen hâli. Yalnızca gözlemdir:
    // koşuyu etkilemez, trace'e girmez.
    timeline: Vec<StatusChange>,
    statuses: BTreeMap<NodeId, NodeStatus>,
}

/// Cevap bekleyen bir okuma: kimin, hangi anahtarı.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientRead {
    client: u64,
    seq: u64,
    key: Vec<u8>,
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
        let mut cluster = Self {
            sim,
            election_safety: ElectionSafety::new(),
            append_only: LeaderAppendOnly::new(),
            log_matching: LogMatching::new(),
            completeness: LeaderCompleteness::new(),
            state_machine: StateMachineSafety::new(),
            elections: BTreeMap::new(),
            stores: ids.iter().map(|&id| (id, KvStore::default())).collect(),
            observed: BTreeMap::new(),
            pending: BTreeMap::new(),
            client_replies: Vec::new(),
            driver_seq: 0,
            reads: BTreeMap::new(),
            next_read: 0,
            snapshot_every: config.snapshot_every,
            applied: BTreeMap::new(),
            digests: BTreeMap::new(),
            compactions: 0,
            installs: 0,
            lease_reads: 0,
            timeline: Vec::new(),
            statuses: BTreeMap::new(),
        };
        cluster.record_statuses();
        Ok(cluster)
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

    /// Bir düğümün KV durum makinesi: o düğümün uyguladığı komutların (ve kurduğu snapshot'ların)
    /// sonucu. Çökmede sıfırlanır; yeniden başlatmadan sonra diskteki snapshot'tan (varsa) ve
    /// yeniden uygulanan girdilerle kurulur.
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
            self.compact_due()?;
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
            self.compact_due()?;
        }
        Ok(())
    }

    /// Koşunun zaman çizelgesi: her düğümün hâlinin (ayakta mı, rol, term) her değişimi, zaman
    /// sırasıyla. İlk kayıtlar 0 anındaki başlangıç hâlleridir. Hâller her olayın sonunda
    /// okunur: bir olayın içindeki ara hâller (ör. aynı adımda aday olup hemen lider olmak) değil,
    /// olaydan sonraki hâl kaydedilir. `raftsim replay --svg` çizelgeyi bundan çizer.
    #[must_use]
    pub fn timeline(&self) -> &[StatusChange] {
        &self.timeline
    }

    /// Hâli değişen düğümleri zaman çizelgesine ekler.
    fn record_statuses(&mut self) {
        let at = self.sim.now();
        for host in self.sim.hosts() {
            let status = NodeStatus {
                up: host.up,
                role: host.node.role(),
                term: host.node.current_term(),
            };
            if self.statuses.insert(host.id, status) != Some(status) {
                self.timeline.push(StatusChange {
                    at,
                    node: host.id,
                    status,
                });
            }
        }
    }

    /// Sıkıştırmalar: alınan durum makinesi snapshot'ları (bkz. `ClusterConfig::snapshot_every`).
    #[must_use]
    pub fn compactions(&self) -> u64 {
        self.compactions
    }

    /// Liderden kurulan snapshot'lar (Figure 13).
    #[must_use]
    pub fn installs(&self) -> u64 {
        self.installs
    }

    /// Geldiği adımda `Ready` ile cevaplanan okumalar: birden çok düğümlü bir kümede bunlar lider
    /// kiralamasıyla (tezin §6.4.1) cevaplanmıştır, çünkü ReadIndex'in doğrulama turu en az bir
    /// gidiş-dönüş sürer. Bekledikten sonra kiralamayla cevaplanan okumaları saymaz: bir alt
    /// sınırdır.
    #[must_use]
    pub fn lease_reads(&self) -> u64 {
        self.lease_reads
    }

    /// Bir düğümün saatini ileri sıçratır (bkz. [`Simulation::jump_clock`]) ve invariant'ları
    /// denetler. Sıçrama tek bir olaydır: ek tick'lerin hepsi verildikten sonra denetlenir.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da çökmüşse [`ClusterError::Lifecycle`]; ardından bir invariant çiğnenmişse
    /// [`ClusterError::Violation`].
    pub fn jump_clock(&mut self, id: NodeId, ticks: u64) -> Result<(), ClusterError> {
        self.sim.jump_clock(id, ticks)?;
        self.check()
    }

    /// Durum makinesi son snapshot'ından bu yana `snapshot_every` girdi uygulamış ayaktaki her
    /// düğümün durum makinesini snapshot'a alır (§7) ve invariant'ları denetler. Neden sürücüde:
    /// çekirdek durum makinesini görmez; ne zaman sıkıştırılacağına sürücü karar verir (bkz.
    /// `Input::Compact`). Snapshot, durum makinesinin uyguladığı son index'teki hâlidir; çekirdek
    /// o index'e kadar uygulamıştır (uygulamalar ondan gelir).
    ///
    /// Sıra önemlidir: bu fonksiyon her olayın `check`'inden SONRA çağrılır. `check`, bir girdinin
    /// uygulandığı olayda düğümün görünen commitIndex'ini Leader Completeness'a kaydeder;
    /// sıkıştırma ondan önce yapılsaydı, kaydedilmemiş commit edilmiş girdiler log'dan atılmış olur
    /// ve kâhin onları hiç göremezdi (sıkıştırılmış önek "snapshot kapsamında" sayılır).
    fn compact_due(&mut self) -> Result<(), ClusterError> {
        let Some(every) = self.snapshot_every.map(NonZeroU64::get) else {
            return Ok(());
        };
        let due: Vec<(NodeId, LogIndex)> = self
            .sim
            .hosts()
            .filter(|host| host.up)
            .filter_map(|host| {
                let applied = self.applied.get(&host.id).copied()?;
                let base = host
                    .node
                    .snapshot()
                    .map_or(0, |snapshot| snapshot.last_index.0);
                (applied.0 >= base.saturating_add(every)).then_some((host.id, applied))
            })
            .collect();
        for (id, index) in due {
            let data = self
                .stores
                .get(&id)
                .map(KvStore::snapshot)
                .unwrap_or_default();
            self.sim.submit(id, RaftRequest::Compact { index, data })?;
            self.compactions += 1;
            self.check()?;
        }
        Ok(())
    }

    /// Bir düğümün durum makinesini snapshot'la değiştirir (liderden kurulan ya da yeniden
    /// başlatmada diskten yüklenen snapshot) ve snapshot'ı kopyaların o index'teki durumuyla
    /// karşılaştırır (snapshot güvenliği). State Machine Safety'ye düğümün uygulanmış öneki
    /// bildirilir: snapshot onu geri alamaz.
    fn restore(&mut self, id: NodeId, snapshot: &Snapshot) -> Result<(), ClusterError> {
        let time = self.sim.now();
        let violation = |violation: Violation| ClusterError::Violation { time, violation };
        let index = snapshot.last_index;
        self.state_machine
            .observe_snapshot(id.0, index.0)
            .map_err(|v| violation(v.into()))?;
        // Önce çözülür, sonra çözülen durum makinesinin parmak izi karşılaştırılır: kâhin
        // snapshot'ın kodlamasına değil, onun kurduğu duruma bakar. Kodlayıcı durumun bir
        // parçasını düşürseydi (ör. oturumları), kurulan durum o index'teki durumdan ayrışır.
        let Ok(store) = KvStore::restore(&snapshot.data) else {
            return Err(violation(Violation::UndecodableSnapshot {
                node: id,
                index,
            }));
        };
        record_or_compare(&mut self.digests, index, store.fingerprint(), id).map_err(violation)?;
        self.stores.insert(id, store);
        self.applied.insert(id, index);
        Ok(())
    }

    /// Bir düğümü çökertir (bkz. [`Simulation::crash`]). Düğümün durum makinesi de çökmeyle
    /// kaybolur: KV tablosu boşaltılır ve yeniden başlatmadan sonra diskteki snapshot'tan (varsa)
    /// kurulur, girdiler onun ardından (snapshot yoksa 1'den) yeniden uygulanır. Kâhinler düğümün
    /// geçici durumunu (commitIndex, uygulama sırası) ve Leader Append-Only'nin onun
    /// liderliklerinden sakladığı log görüntülerini unutur; Election Safety'nin term başına lider
    /// kaydı ise korunur. Ardından invariant'lar HEMEN denetlenir: hiçbir düğüm adımlanmaz, ama
    /// çökmede bekleyen yazmaların bir öneki diske ulaşmış sayılabilir ve diskteki log değişir.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da zaten çökmüşse [`ClusterError::Lifecycle`]; diske ulaşan önek bir
    /// invariant'ı çiğnerse [`ClusterError::Violation`].
    pub fn crash(&mut self, id: NodeId) -> Result<(), ClusterError> {
        self.sim.crash(id)?;
        // İstemcilerin bu düğüme açık istekleri bağlantıyla birlikte kopar: cevapları hiç gelmez.
        self.pending.remove(&id);
        self.reads.retain(|&(node, _), _| node != id);
        self.applied.remove(&id);
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
        // Düğüm artık ayakta: aşağıdaki snapshot yüklemesi bir ihlalle bitse bile zaman çizelgesi
        // onu ayakta gösterir (`check` o durumda çağrılmaz).
        self.record_statuses();
        // Diskte bir snapshot varsa durum makinesi ondan kurulur: düğüm de `lastApplied` ile oradan
        // açılır (bkz. `Input::Restart`) ve girdiler snapshot'ın ardından uygulanır.
        let snapshot = self
            .sim
            .hosts()
            .find(|host| host.id == id)
            .and_then(|host| host.disk.snapshot.clone());
        if let Some(snapshot) = snapshot {
            self.restore(id, &snapshot)?;
        }
        self.check()
    }

    /// Ayaktaki bir düğüme bir KV komutu verir ve invariant'ları denetler: "gönder ve unut". Komut
    /// kümenin kendi iç oturumuyla (istemci 0, her çağrıda yeni bir sıra numarası) gönderilir ve
    /// cevabı beklenmez. Komutu yalnızca lider kabul eder; lider olmayan düğümün `NotLeader`
    /// cevabı yok sayılır. Cevapları izleyen istemciler için bkz. [`RaftCluster::submit_request`].
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da çökmüşse [`ClusterError::Lifecycle`]; ardından bir invariant çiğnenmişse
    /// [`ClusterError::Violation`].
    pub fn submit(&mut self, id: NodeId, command: KvCommand) -> Result<(), ClusterError> {
        self.driver_seq = self.driver_seq.saturating_add(1);
        let request = KvRequest {
            client: 0,
            seq: self.driver_seq,
            command,
        };
        self.sim
            .submit(id, RaftRequest::Command(request.encode()))?;
        self.check()
    }

    /// Ayaktaki bir düğüme bir istemci isteği verir ve invariant'ları denetler. İsteğin cevabı
    /// sırası gelince [`RaftCluster::take_client_replies`]'tan alınır:
    ///
    /// - Düğüm lider değilse `NotLeader { hint }` (istek log'a eklenmedi).
    /// - Lider isteği kabul ettiyse, düğüm isteği uyguladığında `Done` (ilk uygulamanın sonucu;
    ///   aynı istek daha önce uygulanmışsa oturumdaki sonuç, §8). Düğüm bu arada liderliği
    ///   bırakmış olsa bile: commit edilmiş bir isteğin sonucu doğrudur.
    /// - Düğüm cevap veremeden çökerse hiç cevap gelmez; istemci zaman aşımında yeniden dener.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da çökmüşse [`ClusterError::Lifecycle`] (istek verilmez); ardından bir
    /// invariant çiğnenmişse [`ClusterError::Violation`].
    pub fn submit_request(&mut self, id: NodeId, request: KvRequest) -> Result<(), ClusterError> {
        self.sim
            .submit(id, RaftRequest::Command(request.encode()))?;
        // Cevap (reddetme) aynı adımda bırakılmış olabilir: istek, `check` cevapları işlemeden
        // önce kaydedilir.
        *self
            .pending
            .entry(id)
            .or_default()
            .entry((request.client, request.seq))
            .or_default() += 1;
        self.check()
    }

    /// Ayaktaki bir düğüme log'a yazılmayacak bir okuma verir (ReadIndex, tezin §6.4'ü) ve
    /// invariant'ları denetler. Okumanın cevabı sırası gelince
    /// [`RaftCluster::take_client_replies`]'tan alınır:
    ///
    /// - Düğüm lider değilse ya da okuma tamamlanmadan liderliği bırakırsa `NotLeader { hint }`.
    /// - Lider okumayı doğruladığında `Done { result: Value(..) }`: değer, düğümün KV durum
    ///   makinesinin o anki hâlinden okunur. Okuma oturumlara girmez (hiçbir etkisi yoktur).
    /// - Düğüm cevap veremeden çökerse hiç cevap gelmez; istemci zaman aşımında yeniden dener.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da çökmüşse [`ClusterError::Lifecycle`] (okuma verilmez); ardından bir
    /// invariant çiğnenmişse [`ClusterError::Violation`].
    pub fn submit_read(
        &mut self,
        id: NodeId,
        client: u64,
        seq: u64,
        key: Vec<u8>,
    ) -> Result<(), ClusterError> {
        self.next_read = self.next_read.saturating_add(1);
        let read = ReadId(self.next_read);
        self.sim.submit(id, RaftRequest::Read(read))?;
        // Cevap aynı adımda bırakılmış olabilir (ör. lider olmayan düğümün reddi): okuma, `check`
        // cevapları işlemeden önce kaydedilir.
        self.reads
            .insert((id, read), ClientRead { client, seq, key });
        let replies_before = self.client_replies.len();
        self.check()?;
        let answered_at_once = self.client_replies[replies_before..].iter().any(|reply| {
            reply.node == id
                && (reply.client, reply.seq) == (client, seq)
                && matches!(reply.outcome, ReplyOutcome::Done { .. })
        });
        if answered_at_once && self.sim.hosts().count() > 1 {
            self.lease_reads += 1;
        }
        Ok(())
    }

    /// Son çağrıdan bu yana üretilen istemci cevapları, üretilme sırasıyla.
    pub fn take_client_replies(&mut self) -> Vec<ClientReply> {
        std::mem::take(&mut self.client_replies)
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

    /// Ağın ayarlarını değiştirir (bkz. [`Simulation::set_network_config`]); ör. kayıp oranı.
    ///
    /// # Errors
    ///
    /// Ayarlar geçersizse [`ConfigError`]; o durumda ağ değişmez.
    pub fn set_network(&mut self, config: NetworkConfig) -> Result<(), ConfigError> {
        self.sim.set_network_config(config)
    }

    /// Son olaydan sonra bütün invariant'ları denetler ve gözlem kayıtlarını günceller.
    fn check(&mut self) -> Result<(), ClusterError> {
        // Önce zaman çizelgesi: bu olay bir ihlalle bitse bile, ihlale yol açan hâller kayıtlıdır.
        self.record_statuses();
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
        // değiştirmez (bkz. `KvStore::apply`); her düğüm onu aynı biçimde yok sayar. Uygulanan
        // istek bu düğümde bir istemci tarafından bekleniyorsa sonucu ona döner.
        for (id, applied) in self.sim.take_applied() {
            let applied = match applied {
                RaftApplied::Entry(entry) => entry,
                // Liderden kurulan bir snapshot: durum makinesi onunla değiştirilir (§7).
                RaftApplied::Restore(snapshot) => {
                    self.installs += 1;
                    self.restore(id, &snapshot)?;
                    continue;
                }
            };
            self.state_machine
                .observe_apply(id.0, applied.index.0, applied.command.as_bytes())
                .map_err(|v| violation(v.into()))?;
            let store = self.stores.entry(id).or_default();
            let outcome = store.apply(&applied.command);
            self.applied.insert(id, applied.index);
            // Snapshot güvenliği: aynı index'teki durum makineleri aynı olmalıdır. Komutlar aynıysa
            // (State Machine Safety) aynıdırlar; farklılaşmanın tek yolu bir snapshot'tır (ör.
            // oturumları taşımayan bir snapshot'tan kurulan kopya, yeniden denenen bir isteği
            // ikinci kez uygular).
            if self.snapshot_every.is_some() {
                record_or_compare(&mut self.digests, applied.index, store.fingerprint(), id)
                    .map_err(violation)?;
            }
            let (client, seq, done) = match outcome {
                KvApplied::Executed {
                    client,
                    seq,
                    result,
                } => (client, seq, Some((result, false))),
                KvApplied::Duplicate {
                    client,
                    seq,
                    result,
                } => (client, seq, Some((result, true))),
                // Geride kalmış istek: istemci onu beklemiyor, cevap yok; ama bir deneme kapanır.
                KvApplied::Stale { client, seq } => (client, seq, None),
                KvApplied::Noop | KvApplied::Invalid(_) => continue,
            };
            if close_attempt(&mut self.pending, id, (client, seq))
                && let Some((result, duplicate)) = done
            {
                self.client_replies.push(ClientReply {
                    client,
                    seq,
                    node: id,
                    outcome: ReplyOutcome::Done { result, duplicate },
                });
            }
        }
        // Reddedilen istekler bekleyen istemciye `NotLeader` döner. Sonuçlanan okumalar düğümün
        // durum makinesinden cevaplanır: bu olayın uygulamaları yukarıda işlendi, yani okuma,
        // düğümün `Ready`'den önce verdiği her `Apply`'ı görür (Q1). Aynı olayda okumadan SONRA
        // bırakılan bir uygulama da görünebilir; o da commit edilmiş bir yazmadır ve okumanın
        // aralığı içinde gerçekleşmiştir: sonuç yine doğrusaldır.
        for (id, reply) in self.sim.take_replies() {
            match reply {
                RaftResponse::NotLeader(reply) => {
                    let Ok(request) = KvRequest::decode(reply.request.as_bytes()) else {
                        continue;
                    };
                    if close_attempt(&mut self.pending, id, (request.client, request.seq)) {
                        self.client_replies.push(ClientReply {
                            client: request.client,
                            seq: request.seq,
                            node: id,
                            outcome: ReplyOutcome::NotLeader { hint: reply.hint },
                        });
                    }
                }
                RaftResponse::Read { id: read, outcome } => {
                    let Some(ClientRead { client, seq, key }) = self.reads.remove(&(id, read))
                    else {
                        continue;
                    };
                    let outcome = match outcome {
                        ReadOutcome::Ready => {
                            let value = self
                                .stores
                                .get(&id)
                                .and_then(|store| store.get(&key))
                                .map(<[u8]>::to_vec);
                            ReplyOutcome::Done {
                                result: KvResult::Value(value),
                                duplicate: false,
                            }
                        }
                        ReadOutcome::NotLeader { hint } => ReplyOutcome::NotLeader { hint },
                    };
                    self.client_replies.push(ClientReply {
                        client,
                        seq,
                        node: id,
                        outcome,
                    });
                }
            }
        }

        let mut newly_committed: Vec<u64> = Vec::new();
        let mut new_leaders: Vec<NodeId> = Vec::new();
        for host in self.sim.hosts() {
            let id = host.id;
            let disk_changed_from = synced.get(&id).copied().flatten();
            // Çökmüş bir düğümün diski de denetlenir: çökmede diske ulaşmış sayılan önek de
            // kalıcıdır ve düğüm onunla açılacaktır.
            if let Some(from) = disk_changed_from {
                let views = entry_views(&host.disk.log);
                self.log_matching
                    .observe(id.0, log_view(host.disk.snapshot.as_ref(), &views), from.0)
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
                        let views = entry_views(node.log());
                        self.append_only
                            .observe(term.0, id.0, log_view(node.snapshot(), &views))
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
                let views = entry_views(node.log());
                let mut newly = self
                    .completeness
                    .observe_commit(id.0, term.0, visible.0, log_view(node.snapshot(), &views))
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
            let log = log_view(host.disk.snapshot.as_ref(), &views);
            let result = if new_leaders.contains(&host.id) {
                self.completeness.check_leader(host.id.0, term.0, log)
            } else {
                self.completeness.check_entries(
                    host.id.0,
                    term.0,
                    log,
                    newly_committed.iter().copied(),
                )
            };
            result.map_err(|v| violation(v.into()))?;
        }
        Ok(())
    }
}

/// `node`'daki bekleyen bir denemeyi kapatır: varsa sayısını bir düşürür (sıfırda kaydı siler) ve
/// `true` döner; bekleyen deneme yoksa `false`.
fn close_attempt(
    pending: &mut BTreeMap<NodeId, BTreeMap<(u64, u64), u32>>,
    node: NodeId,
    request: (u64, u64),
) -> bool {
    let Some(waiting) = pending.get_mut(&node) else {
        return false;
    };
    let Some(count) = waiting.get_mut(&request) else {
        return false;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        waiting.remove(&request);
    }
    true
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
    let memory_base = node.snapshot().map_or(0, |snapshot| snapshot.last_index.0);
    let memory_last =
        memory_base.saturating_add(u64::try_from(node.log().len()).unwrap_or(u64::MAX));
    if commit <= observed || commit.0 > memory_last {
        return commit;
    }
    // Index'ler mutlaktır: bellekteki ve diskteki log'lar farklı snapshot'lardan sonra başlayabilir
    // (sıkıştırmanın yazması henüz kalıcı değilse). Diskteki snapshot'ın kapsadığı bir index
    // kalıcıdır ve commit edilmiştir (snapshot yalnızca commit edilmiş girdiler içerir). Bellekte
    // sıkıştırılmış ama diskte hâlâ log'da olan bir index karşılaştırılamaz: görünürlük, sıkıştırma
    // kalıcı olana kadar orada durur.
    let disk_base = disk.snapshot_index().0;
    let mut visible = observed.0;
    for index in observed.0.saturating_add(1)..=commit.0 {
        if index > disk_base {
            let in_memory = node.entry(LogIndex(index));
            let durable = disk.entry(LogIndex(index));
            if in_memory.is_none() || in_memory != durable {
                break;
            }
        }
        visible = index;
    }
    LogIndex(visible)
}

/// Bir index'teki durum makinesi parmak izini kaydeder ya da kayıtlı olanla karşılaştırır
/// (snapshot güvenliği): ilk kopya kaydeder, sonrakiler aynı izi vermelidir.
fn record_or_compare(
    digests: &mut BTreeMap<LogIndex, (u64, NodeId)>,
    index: LogIndex,
    fingerprint: u64,
    node: NodeId,
) -> Result<(), Violation> {
    match digests.entry(index) {
        Entry::Occupied(recorded) if recorded.get().0 != fingerprint => {
            Err(Violation::StateDiverged {
                node,
                index,
                first: recorded.get().1,
            })
        }
        Entry::Occupied(_) => Ok(()),
        Entry::Vacant(slot) => {
            slot.insert((fingerprint, node));
            Ok(())
        }
    }
}

/// Bir log'un denetçiye görünen hâli: snapshot'tan sonraki girdiler ve snapshot'ın tabanı (§7).
fn log_view<'a>(snapshot: Option<&Snapshot>, views: &'a [EntryView<'a>]) -> LogView<'a> {
    snapshot.map_or_else(
        || LogView::full(views),
        |snapshot| LogView::compacted(snapshot.last_index.0, snapshot.last_term.0, views),
    )
}

/// Düğümün belleği ile diske yazdırdığı en son durum arasındaki ilk fark.
///
/// Term, oy, snapshot'ın son index'i ve term'i ve log uzunluğu her olayda karşılaştırılır. Log'un
/// içeriği ve snapshot'ın verisi, düğüm bu olayda bir yazma verdiyse tamamen; vermediyse log'un
/// yalnızca son girdisi karşılaştırılır. Neden: yazma vermeyen bir adım log'u ve snapshot'ı
/// değiştirmemelidir ve log'u değiştiren hatalar (ekleme, kesme, kuyruk değiştirme) uzunluğu ya da
/// son girdiyi, snapshot'ı değiştiren hatalar onun son index'ini ya da term'ini değiştirir; her
/// olayda bütün log'u ve snapshot verisini karşılaştırmak ise log boyu × olay sayısı kadar iş
/// olurdu. Ortadaki bir girdiyi ya da snapshot verisini sessizce değiştiren bir hata, düğümün bir
/// sonraki yazmasında tam karşılaştırmaya takılır.
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
    // Snapshot (§7) da kalıcı durumun parçasıdır; ikisi aynıysa log'lar aynı tabandan başlar.
    let point = |snapshot: Option<&Snapshot>| {
        snapshot.map(|snapshot| (snapshot.last_index, snapshot.last_term))
    };
    let (in_memory, written) = (node.snapshot(), latest.snapshot.as_ref());
    let differs = if wrote {
        in_memory != written
    } else {
        point(in_memory) != point(written)
    };
    if differs {
        return Some(DurabilityMismatch::Snapshot {
            memory: point(in_memory),
            disk: point(written),
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
    let base = latest.snapshot_index().0;
    first_difference.map(|position| DurabilityMismatch::LogEntry {
        index: LogIndex(
            base.saturating_add(length(&memory[..position]))
                .saturating_add(1),
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU64;

    use super::{
        AppliedEntry, ClusterConfig, ClusterError, DurabilityMismatch, NodeStatus, RaftApplied,
        RaftCluster, RaftRequest, Violation,
    };
    use crate::disk::DiskConfig;
    use crate::kv::{KvCommand, KvStore};
    use crate::network::NetworkConfig;
    use crate::trace::{TraceKind, digest};
    use checker::{EntryView, StateMachineSafetyViolation};
    use raft_core::{
        AppendEntries, AppendEntriesResponse, Command, InstallSnapshot, LogEntry, LogIndex,
        LogUpdate, Message, NodeId, PersistUpdate, PersistentState, ReadId, RequestVote,
        RequestVoteResponse, Role, Snapshot, Term,
    };

    /// Tek baytlık verili bir snapshot.
    fn snap(last_index: u64, last_term: u64, byte: u8) -> Snapshot {
        Snapshot {
            last_index: LogIndex(last_index),
            last_term: Term(last_term),
            data: vec![byte],
        }
    }

    fn install(term: u64, snapshot: Snapshot) -> Message {
        Message::InstallSnapshot(InstallSnapshot {
            term: Term(term),
            snapshot,
        })
    }

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

    /// Gecikmesiz diskli, güvenilir ağlı, her 4 girdide snapshot alan 3 düğümlü bir küme: durum
    /// makinesi parmak izleri (snapshot güvenliği) kaydedilir.
    fn compacting_cluster(seed: u64) -> RaftCluster {
        let config = ClusterConfig {
            disk: DiskConfig::instant(),
            snapshot_every: NonZeroU64::new(4),
            ..ClusterConfig::new(3, NetworkConfig::reliable(1))
        };
        RaftCluster::new(seed, config).expect("valid config")
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
            install(1, snap(1, 1, 1)),
            install(2, snap(1, 1, 1)),
            install(1, snap(2, 1, 1)),
            install(1, snap(1, 2, 1)),
            install(1, snap(1, 1, 2)),
        ];
        assert_distinct(messages.iter().map(digest).collect());

        let state = |term, vote: Option<u64>, log: Vec<LogEntry>| PersistentState {
            current_term: Term(term),
            voted_for: vote.map(NodeId),
            snapshot: None,
            log,
        };
        let with_snapshot = |snapshot| PersistentState {
            snapshot: Some(snapshot),
            ..state(0, None, Vec::new())
        };
        let states = [
            state(0, None, Vec::new()),
            state(0, Some(0), Vec::new()),
            state(1, None, Vec::new()),
            state(0, Some(1), Vec::new()),
            state(0, None, vec![entry(1, 1)]),
            state(0, None, vec![entry(1, 2)]),
            with_snapshot(snap(1, 1, 1)),
            with_snapshot(snap(2, 1, 1)),
            with_snapshot(snap(1, 1, 2)),
        ];
        assert_distinct(states.iter().map(digest).collect());

        let update = |vote: Option<u64>, log: Option<(u64, Vec<LogEntry>)>| PersistUpdate {
            current_term: Term(1),
            voted_for: vote.map(NodeId),
            snapshot: None,
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
            PersistUpdate {
                snapshot: Some(snap(1, 1, 1)),
                ..update(None, None)
            },
            PersistUpdate {
                snapshot: Some(snap(1, 1, 2)),
                ..update(None, None)
            },
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
        // Uygulamalar ve istekler: snapshot'lar ve sıkıştırmalar girdilerden, komutlardan ve
        // okumalardan ayrışır; bir girdinin kodlaması `AppliedEntry`'ninkiyle aynıdır (snapshot'sız
        // koşuların özetleri değişmez).
        assert_eq!(
            digest(&RaftApplied::Entry(applied(1, 1))),
            digest(&applied(1, 1))
        );
        assert_distinct(vec![
            digest(&RaftApplied::Entry(applied(1, 1))),
            digest(&RaftApplied::Restore(snap(1, 1, 1))),
            digest(&RaftApplied::Restore(snap(1, 1, 2))),
            digest(&RaftRequest::Command(Command::new(vec![1]))),
            digest(&RaftRequest::Read(ReadId(1))),
            digest(&RaftRequest::Compact {
                index: LogIndex(1),
                data: Vec::new(),
            }),
            digest(&RaftRequest::Compact {
                index: LogIndex(1),
                data: vec![1],
            }),
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

    // Commit edilmiş girdilerin korunması her yazmada denetlenir: liderin 2. index'i commit ettiği
    // gözlenmiş sayılır (sahte gözlem; gerçekte yalnızca 1. index'teki no-op commit edildi).
    // Liderin ilk istemci girdisini ekleyen yazması 2. index'ten başladığı için commit edilmiş bir
    // girdiyi yeniden yazmış sayılır.
    #[test]
    fn rewriting_a_committed_entry_is_reported() {
        let mut cluster = quiet_cluster(7);
        let (leader, _) = elect(&mut cluster);
        cluster.observed.entry(leader).or_default().commit_index = LogIndex(2);
        let error = cluster
            .submit(leader, put(b"k"))
            .expect_err("the leader's first client write starts at index 2");
        assert_eq!(
            violation_of(error),
            Violation::CommittedEntryRewritten {
                node: leader,
                from: LogIndex(2),
                commit_index: LogIndex(2),
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
    // verildiği anda kalıcıdır): kâhin, her term'in 2. index'ine (term başındaki no-op'tan sonraki
    // ilk girdi) sahte bir komut görmüş sayılır; liderin ilk istemci girdisi bu kayıtla çakışır.
    #[test]
    fn log_matching_is_checked_on_every_durable_write() {
        let mut cluster = quiet_cluster(2);
        let fake = [0xee_u8];
        for term in 1..=20 {
            let view = [
                EntryView { term, command: &[] },
                EntryView {
                    term,
                    command: &fake,
                },
            ];
            cluster
                .log_matching
                .observe(99, &view, 2)
                .expect("a fresh entry");
        }
        let (leader, _) = elect(&mut cluster);
        let error = cluster
            .submit(leader, put(b"k"))
            .expect_err("the leader's first client entry collides with the fake one");
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

    // State Machine Safety her uygulamada denetlenir: kâhin, 1. index'te no-op'un ve 2. index'te
    // sahte bir komutun uygulandığını görmüş sayılır; kümenin ilk istemci komutunun uygulaması
    // bununla çakışır.
    #[test]
    fn state_machine_safety_is_checked_on_every_apply() {
        let mut cluster = quiet_cluster(4);
        cluster
            .state_machine
            .observe_apply(99, 1, &[])
            .expect("a fresh index");
        cluster
            .state_machine
            .observe_apply(99, 2, &[0xee])
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
        // Gözlenen log'un (term başındaki no-op) devamı gibi görünen sahte bir görüntü.
        let view = [
            EntryView {
                term: term.0,
                command: &[],
            },
            EntryView {
                term: term.0,
                command: &fake,
            },
        ];
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

    // Çökmede diske ulaşan önek de, çökmenin kendisinde denetlenir: kâhin 2. index'te (no-op'tan
    // sonra) sahte bir komut görmüş sayılır ve liderin ilk istemci girdisi fsync penceresindeyken
    // lider çöker. Kısmi yazma girdiyi diske ulaştırırsa çakışma çökmede bildirilir; yazma
    // kaybolursa ihlal yoktur. Seed taraması iki durumu da görür.
    #[test]
    fn a_prefix_kept_by_a_crash_is_checked_at_the_crash() {
        let mut kept_counts = BTreeSet::new();
        for seed in 0..20 {
            let mut cluster = cluster_with_disk(seed, slow_disk(1.0));
            let (leader, term) = elect(&mut cluster);
            let fake = [0xee_u8];
            let view = [
                EntryView {
                    term: term.0,
                    command: &[],
                },
                EntryView {
                    term: term.0,
                    command: &fake,
                },
            ];
            cluster
                .log_matching
                .observe(99, &view, 2)
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

    /// Sıkıştıran bir kümede beş yazma uygulatır: liderin no-op'u ile 6 girdi, her düğümde.
    /// Bir takipçiyi ve onun uyguladığı son index'i döndürür.
    fn applied_follower(cluster: &mut RaftCluster) -> (NodeId, LogIndex) {
        let (leader, _) = elect(cluster);
        for key in [b"a", b"b", b"c", b"d", b"e"] {
            cluster.submit(leader, put(key)).expect("the leader is up");
        }
        let target = cluster.now() + 100;
        cluster.run_until(target).expect("no violation");
        let follower = cluster
            .node_ids()
            .find(|&id| id != leader)
            .expect("a follower");
        let applied = cluster.applied[&follower];
        assert_eq!(applied, LogIndex(6), "every write is applied");
        (follower, applied)
    }

    // Snapshot güvenliği, kurulan snapshot'ın kurduğu duruma bakar: kopyaların o index'teki
    // durumuyla aynı veriyi taşıyan bir snapshot kabul edilir; aynı index'te farklı bir durum
    // (burada boş tablo) kuran snapshot, durumu ilk kaydeden kopyayla birlikte bildirilir.
    #[test]
    fn a_restored_snapshot_must_rebuild_the_recorded_state() {
        let mut cluster = compacting_cluster(31);
        let (follower, applied) = applied_follower(&mut cluster);
        let snapshot = |data: Vec<u8>| Snapshot {
            last_index: applied,
            last_term: Term(1),
            data,
        };

        let same = cluster.kv(follower).expect("a store").snapshot();
        cluster
            .restore(follower, &snapshot(same))
            .expect("the same state at the same index");

        let error = cluster
            .restore(follower, &snapshot(KvStore::default().snapshot()))
            .expect_err("an empty table is not the state at that index");
        let first = cluster.digests[&applied].1;
        assert_eq!(
            violation_of(error),
            Violation::StateDiverged {
                node: follower,
                index: applied,
                first,
            }
        );
    }

    // Bir snapshot düğümün zaten uyguladığı girdileri geri alamaz (State Machine Safety): son
    // uygulanan index'in gerisindeki bir snapshot, veri doğru olsa bile reddedilir.
    #[test]
    fn a_snapshot_cannot_roll_back_applied_entries() {
        let mut cluster = compacting_cluster(32);
        let (follower, applied) = applied_follower(&mut cluster);
        let behind = Snapshot {
            last_index: LogIndex(applied.0 - 1),
            last_term: Term(1),
            data: cluster.kv(follower).expect("a store").snapshot(),
        };
        let error = cluster
            .restore(follower, &behind)
            .expect_err("the snapshot is behind the applied entries");
        assert!(matches!(
            violation_of(error),
            Violation::StateMachineSafety(StateMachineSafetyViolation::RolledBack { .. })
        ));
    }

    // Çözülemeyen bir snapshot boş bir tabloya dönüştürülmez (bu, hatayı bir sonraki
    // karşılaştırmaya erteler ve nedenini gizlerdi): kurulduğu anda bildirilir.
    #[test]
    fn an_undecodable_snapshot_is_reported_when_it_is_restored() {
        let mut cluster = compacting_cluster(33);
        let (follower, applied) = applied_follower(&mut cluster);
        let garbage = Snapshot {
            last_index: applied,
            last_term: Term(1),
            data: vec![0xff],
        };
        let error = cluster
            .restore(follower, &garbage)
            .expect_err("the data does not decode");
        assert_eq!(
            violation_of(error),
            Violation::UndecodableSnapshot {
                node: follower,
                index: applied,
            }
        );
    }

    // Zaman çizelgesi her düğümün hâlinin (ayakta mı, rol, term) her değişimini zaman sırasıyla
    // kaydeder: 0 anındaki başlangıç hâlleri, seçilen lider, çökme (donmuş rolüyle, ayakta değil)
    // ve yeniden başlatma (ayakta, takipçi). Bir düğümün ardışık iki kaydı hiç aynı değildir.
    // (Çizelge yalnızca gözlemdir: koşuyu değiştirmediğini, altın Raft trace özetinin çizelge
    // eklendiğinde aynı kalması gösterir; bkz. `tests/determinism.rs`.)
    #[test]
    fn the_timeline_records_every_status_change() {
        let mut cluster = quiet_cluster(41);
        let (leader, term) = elect(&mut cluster);
        cluster.crash(leader).expect("the leader is up");
        let crashed_at = cluster.now();
        let later = crashed_at + 50;
        cluster.run_until(later).expect("no violation");
        cluster.restart(leader).expect("the leader is down");

        let timeline = cluster.timeline();
        let initial = NodeStatus {
            up: true,
            role: Role::Follower,
            term: Term(0),
        };
        assert_eq!(
            timeline[..3]
                .iter()
                .map(|change| (change.at, change.node, change.status))
                .collect::<Vec<_>>(),
            [1, 2, 3].map(|id| (0, NodeId(id), initial))
        );
        assert!(timeline.windows(2).all(|pair| pair[0].at <= pair[1].at));
        let leading = NodeStatus {
            up: true,
            role: Role::Leader,
            term,
        };
        assert!(
            timeline
                .iter()
                .any(|change| change.node == leader && change.status == leading)
        );
        assert!(timeline.iter().any(|change| change.node == leader
            && change.at == crashed_at
            && change.status
                == NodeStatus {
                    up: false,
                    ..leading
                }));
        let last = timeline
            .iter()
            .rev()
            .find(|change| change.node == leader)
            .expect("the leader has records");
        assert_eq!(
            (last.at, last.status.up, last.status.role),
            (later, true, Role::Follower)
        );
        let mut current = BTreeMap::new();
        for change in timeline {
            assert_ne!(
                current.insert(change.node, change.status),
                Some(change.status),
                "{change:?} repeats the previous status"
            );
        }
    }
}
