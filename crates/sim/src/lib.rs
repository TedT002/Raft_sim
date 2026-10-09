//! # sim: deterministik simülasyon çatısı
//!
//! Bu crate, sans-IO düğümleri gerçek zamanlı bir sistemmiş gibi koşturan, ama tamamen
//! deterministik (aynı `seed` aynı sonucu üretir) bir test ortamıdır. Simülatör protokolden
//! bağımsızdır: [`SimNode`] trait'ini uygulayan her düğümü sürebilir. Raft ayrı bir adaptörle
//! bağlanır: [`RaftCluster`], Raft düğümlerini sürer ve her olaydan sonra invariant'ları denetler.
//!
//! Bileşenler:
//!
//! - **Sanal saat:** `u64` tick; gerçek saate (`Instant`/`SystemTime`) asla dokunulmaz.
//! - **Olay kuyruğu** ([`EventQueue`]): `(time, seq)` sırası. Eşit zamanlı olaylar eklenme
//!   sırasıyla (FIFO) işlenir, çünkü `BinaryHeap` tek başına bunu garanti etmez.
//! - **Seed ağacı** ([`SeedTree`]): tek bir ana seed'den bileşen başına bağımsız `ChaCha8Rng`
//!   akışları. Bir bileşene eklenen yeni bir rastgele çağrı diğerlerinin akışını kaydırmaz.
//! - **Ağ** ([`Network`], [`SimNetwork`]): kayıp, gecikme (dolayısıyla sıra değişimi), çoğaltma ve
//!   "kablo kesildi" modeliyle bölünme: bir mesaj ancak uçuşu boyunca uçları hiç ayrı gruplara
//!   düşmediyse ulaşır.
//! - **Trace** ([`Trace`]): işlenen her olayın kanonik kaydı ve sürümler arası kararlı FNV-1a
//!   özeti.
//! - **Makineler:** her düğümün bir diski vardır (`Persist` ile yazılır). Düğüm çökebilir ve
//!   diskindeki durumla yeniden başlatılabilir (`Simulation::crash`/`Simulation::restart`); çökmüş
//!   düğüm tick almaz, ona gelen mesajlar düşer.
//! - **Disk** ([`SimDisk`]): yazmalar `fsync` tamamlanana kadar bekler; bir yazmadan sonraki
//!   çıktılar (mesajlar, uygulamalar) o yazma kalıcı olana kadar tutulur. Çökme bekleyen yazmaları
//!   kaybettirir, istenirse bir öneklerini diske ulaştırır ("kısmen yazılır").
//! - **Raft adaptörü** ([`RaftCluster`]): istemci istekleri ([`KvRequest`]: oturum ve
//!   [`KvCommand`]), log'a yazılmadan cevaplanan okumalar (ReadIndex, tezin §6.4'ü;
//!   [`RaftCluster::submit_read`]), düğüm başına oturumlu KV durum makinesi ([`KvStore`], §8: aynı
//!   istek bir kez uygulanır) ve her olaydan sonra Figure 3'ün beş güvenlik özelliğiyle kümenin
//!   diğer denetimleri (dayanıklılık, çıktı sırası, commit edilmiş girdilerin korunması).
//! - **İstemciler** ([`ClientDriver`]): sırayla çalışan, zaman aşımında aynı `(client, seq)` ile
//!   yeniden deneyen, `NotLeader` ipucunu izleyen istemciler; cevapların bir kısmı seed'li olarak
//!   kaybolur. Geçmiş, `checker`'ın linearizability kontrolcüsünün tipleriyle kaydedilir.
//! - **Kaos senaryoları** ([`Scenario`], [`run`]): bir seed'den koşudan ÖNCE üretilen açık bir hata
//!   programı (çökme, lideri çökertme, yeniden başlatma, bölünme, iyileşme, kayıp oranı) ve onu
//!   koşturan sürücü: her olaydan sonra denetimler, hatalardan sonra canlılık, sonda
//!   linearizability. `raftsim fuzz`/`replay` ve kaos testleri aynı programı koşar.
//! - **Küçültme** ([`shrink`]): başarısız bir senaryodan, aynı hatayı veren daha küçük bir senaryo
//!   (hata alt kümesi ve daha kısa hata süresi) bulur.
//! - **Mutasyonlar:** `mutation-*` Cargo özellikleri (varsayılan derlemede yok) çekirdeğe ya da KV
//!   durum makinesine bilerek hata ekler; derlemede en fazla biri açık olabilir
//!   ([`ENABLED_MUTATION`]). Her birinin yakalandığı `docs/mutation-table.md`'de tablolanır.
//!
//! Bu crate `raft-core`'a ve `checker`'a bağımlıdır (bağımlılık yönü: `sim -> raft-core`,
//! `sim -> checker`); tersi asla olmaz. Sans-IO çekirdek hiçbir workspace crate'ini bilmemelidir.
//!
//! Aşağıdaki örnek, her tick'te eşine selam yollayan en küçük protokolü koşturur:
//!
//! ```
//! use raft_core::NodeId;
//! use sim::{
//!     Component, InputOf, NetworkConfig, NodeInput, NodeOutput, OutputOf, SeedTree, SimConfig,
//!     SimNetwork, SimNode, Simulation, TraceEncode,
//! };
//!
//! #[derive(Debug, Clone)]
//! struct Hello;
//!
//! impl TraceEncode for Hello {
//!     fn encode(&self, out: &mut Vec<u8>) {
//!         out.push(1);
//!     }
//! }
//!
//! struct Greeter {
//!     peer: NodeId,
//! }
//!
//! impl SimNode for Greeter {
//!     type Msg = Hello;
//!     // Kalıcı durumu yok: çöküp kalktığında hatırlayacağı bir şey de yok. İstemcisi ve
//!     // durum makinesi de yok.
//!     type Durable = ();
//!     type Request = ();
//!     type Applied = ();
//!     type Response = ();
//!
//!     fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>> {
//!         match input {
//!             NodeInput::Tick => vec![NodeOutput::Send { to: self.peer, msg: Hello }],
//!             NodeInput::Message { .. } | NodeInput::Restart(()) | NodeInput::Client(()) => {
//!                 Vec::new()
//!             }
//!         }
//!     }
//! }
//!
//! let run = |seed| -> Result<u64, sim::ConfigError> {
//!     let seeds = SeedTree::new(seed);
//!     let network_rng = seeds.rng_for(Component::Network);
//!     let network = SimNetwork::new(NetworkConfig::reliable(2), network_rng)?;
//!     let nodes = [
//!         (NodeId(1), Greeter { peer: NodeId(2) }),
//!         (NodeId(2), Greeter { peer: NodeId(1) }),
//!     ];
//!     let mut sim = Simulation::new(SimConfig::default(), network, nodes)?;
//!     sim.run_until(10);
//!     Ok(sim.trace_hash())
//! };
//!
//! // Aynı seed, aynı koşu: trace özeti koşunun kimliğidir.
//! assert_eq!(run(42)?, run(42)?);
//! # Ok::<(), sim::ConfigError>(())
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Doctest'ler de uyarısız olmalı (clippy doctest'leri görmez).
#![doc(test(attr(deny(warnings))))]

// Mutasyon testi (Faz 5): aynı anda en fazla bir `mutation-*` özelliği açılabilir (çekirdeğin
// mutasyonları raft-core'da da denetlenir; `mutation-no-dedup` yalnızca burada vardır). Çekirdeğin
// mutasyonu bu crate'in özellikleriyle değil `raft_core::ENABLED_MUTATION` ile sayılır: çekirdeğin
// özelliği doğrudan (`raft-core/mutation-…`) açılsa bile sayılmış olur ve `mutation-no-dedup` ile
// birlikte açılması derlenmez.
const ENABLED_MUTATIONS: usize = raft_core::ENABLED_MUTATION.is_some() as usize
    + cfg!(feature = "mutation-no-dedup") as usize
    + cfg!(feature = "mutation-snapshot-without-sessions") as usize;
// `<= 1` yerine `matches!`: varsayılan derlemede sabit 0'dır ve clippy, türün en küçük değeriyle
// yapılan her zaman doğru bir karşılaştırmayı (`absurd_extreme_comparisons`) hata sayar.
const _: () = assert!(
    matches!(ENABLED_MUTATIONS, 0 | 1),
    "enable at most one mutation-* feature at a time"
);

/// Bu derlemede açık olan mutasyon özelliğinin adı (varsayılan derlemede `None`). `raftsim`,
/// başarısız bir seed'i yeniden üretme komutuna bunu ekler: mutant bir derlemenin bulduğu seed,
/// ancak aynı özellikle derlenince aynı hatayı verir. Çekirdeğin mutasyonları
/// `raft_core::ENABLED_MUTATION`'dan gelir; özellik adları `cli`, `sim` ve `raft-core`'da aynıdır.
pub const ENABLED_MUTATION: Option<&str> = if cfg!(feature = "mutation-no-dedup") {
    Some("mutation-no-dedup")
} else if cfg!(feature = "mutation-snapshot-without-sessions") {
    Some("mutation-snapshot-without-sessions")
} else {
    raft_core::ENABLED_MUTATION
};

mod client;
mod disk;
mod error;
mod fnv;
mod kv;
mod network;
mod node;
mod queue;
mod raft;
mod rng;
mod scenario;
mod shrink;
mod simulation;
mod trace;

// Genel API düz (flat) olarak kökten dışa aktarılır; modüller ileride yeniden düzenlenebilir.
// `NodeId` burada da dışa aktarılır: sim'i kullanan crate'ler (ör. Faz 5'te `cli`) düğüm
// kimliklerine raft-core'a doğrudan bağımlı olmadan ulaşabilsin.
// İstemci geçmişi `checker`'ın linearizability tipleriyle kaydedilir. Onlar da buradan dışa
// aktarılır: sim'i kullanan kod (testler, Faz 5'te `cli`) geçmişi aynı API'den denetleyebilsin.
pub use checker::{
    KvInput, KvOperation, KvOutput, LinearizabilityError, MalformedReason, check_kv,
};
pub use client::{ClientConfig, ClientDriver, ClientStats, OpMix};
pub use disk::{DiskConfig, SimDisk};
pub use error::{ConfigError, LifecycleError, PartitionError};
pub use fnv::{Fnv1a64, fnv1a64};
pub use kv::{KvApplied, KvCommand, KvDecodeError, KvRequest, KvResult, KvStore};
pub use network::{Fate, Network, NetworkConfig, SimNetwork};
pub use node::{DurableState, InputOf, NodeInput, NodeOutput, OutputOf, SimNode, UpdateOf};
pub use queue::{EventQueue, Scheduled};
pub use raft::{
    AppliedEntry, ClientReply, ClusterConfig, ClusterError, DurabilityMismatch, Election,
    NodeStatus, NotLeaderReply, RaftApplied, RaftCluster, RaftRequest, RaftResponse, ReplyOutcome,
    StatusChange, Violation,
};
pub use raft_core::NodeId;
// `RaftCluster`'ın genel API'sinde görünen raft-core tipleri de aynı gerekçeyle buradan dışa
// aktarılır. Bağımlılık yönü gereği `cli` raft-core'u göremez (cli -> sim); Faz 5'te kümeyi
// kurabilmeli (`RaftConfig`) ve sonuçlarını adlandırabilmelidir (`Term`, `Role`, ...). `Config` ve
// `ConfigError` takma adla verilir: sim'in kendi `ConfigError`'ıyla karışmasınlar.
pub use raft_core::{
    Command, Config as RaftConfig, ConfigError as RaftConfigError, LogEntry, LogIndex, LogUpdate,
    PersistUpdate, PersistentState, RaftNode, ReadId, ReadOutcome, Role, Snapshot, Term,
};
pub use rng::{ChaCha8Rng, Component, SeedTree, chance, uniform_inclusive};
pub use scenario::{
    Fault, FaultMix, Run, RunError, RunStats, Scenario, ScenarioConfig, ScheduledFault, run,
};
pub use shrink::{Shrunk, shrink};
pub use simulation::{HostView, SimConfig, SimOptions, Simulation, TickOrder};
pub use trace::{DropReason, Trace, TraceEncode, TraceEvent, TraceKind, digest};
