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
//! - **Raft adaptörü** ([`RaftCluster`]): istemci komutları ([`KvCommand`]), düğüm başına KV
//!   durum makinesi ([`KvStore`]) ve her olaydan sonra Figure 3'ün beş güvenlik özelliğiyle
//!   dayanıklılık (bellek = diske yazdırılan durum) denetimi.
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

mod disk;
mod error;
mod fnv;
mod kv;
mod network;
mod node;
mod queue;
mod raft;
mod rng;
mod simulation;
mod trace;

// Genel API düz (flat) olarak kökten dışa aktarılır; modüller ileride yeniden düzenlenebilir.
// `NodeId` burada da dışa aktarılır: sim'i kullanan crate'ler (ör. Faz 5'te `cli`) düğüm
// kimliklerine raft-core'a doğrudan bağımlı olmadan ulaşabilsin.
pub use disk::{DiskConfig, SimDisk};
pub use error::{ConfigError, LifecycleError, PartitionError};
pub use fnv::{Fnv1a64, fnv1a64};
pub use kv::{KvCommand, KvDecodeError, KvStore};
pub use network::{Fate, Network, NetworkConfig, SimNetwork};
pub use node::{DurableState, InputOf, NodeInput, NodeOutput, OutputOf, SimNode, UpdateOf};
pub use queue::{EventQueue, Scheduled};
pub use raft::{
    AppliedEntry, ClusterConfig, ClusterError, DurabilityMismatch, Election, RaftCluster, Violation,
};
pub use raft_core::NodeId;
// `RaftCluster`'ın genel API'sinde görünen raft-core tipleri de aynı gerekçeyle buradan dışa
// aktarılır. Bağımlılık yönü gereği `cli` raft-core'u göremez (cli -> sim); Faz 5'te kümeyi
// kurabilmeli (`RaftConfig`) ve sonuçlarını adlandırabilmelidir (`Term`, `Role`, ...). `Config` ve
// `ConfigError` takma adla verilir: sim'in kendi `ConfigError`'ıyla karışmasınlar.
pub use raft_core::{
    Command, Config as RaftConfig, ConfigError as RaftConfigError, LogEntry, LogIndex, LogUpdate,
    PersistUpdate, PersistentState, RaftNode, Role, Term,
};
pub use rng::{ChaCha8Rng, Component, SeedTree, chance, uniform_inclusive};
pub use simulation::{HostView, SimConfig, SimOptions, Simulation, TickOrder};
pub use trace::{DropReason, Trace, TraceEncode, TraceEvent, TraceKind, digest};
