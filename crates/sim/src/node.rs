//! Simülatörün sürdüğü düğüm arayüzü: protokolden bağımsız, sans-IO.
//!
//! Simülatör Raft'ı bilmez; `SimNode` trait'ini uygulayan her düğümü sürebilir. Faz 1'de bu,
//! yalnızca testlerde yaşayan Ping/Pong protokolüdür. Raft, Faz 2'de ayrı bir adaptörle bağlanacak.
//! Arayüz raft-core'un `step(Input) -> Vec<Output>` biçimini aynalar: düğüm saate, ağa ve diske
//! dokunmaz; olayı alır, yapılacakları liste olarak döndürür.

use raft_core::NodeId;

use crate::trace::TraceEncode;

/// Simülatörün bir düğüme verdiği girdi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeInput<M> {
    /// Mantıksal zaman bir tick ilerledi.
    Tick,
    /// Başka bir düğümden bir mesaj geldi.
    Message {
        /// Gönderen düğüm.
        from: NodeId,
        /// Mesajın kendisi.
        msg: M,
    },
}

/// Bir düğümün simülatörden yapmasını istediği eylem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeOutput<M> {
    /// `to` düğümüne mesaj gönder. Mesajın kaderine (kayıp, gecikme, çoğaltma, bölünme) ağ karar
    /// verir.
    Send {
        /// Alıcı düğüm.
        to: NodeId,
        /// Mesajın kendisi.
        msg: M,
    },
}

/// Simülatörün sürebildiği bir düğüm.
pub trait SimNode {
    /// Düğümler arası mesaj tipi. `Clone`: çoğaltılan mesajın ikinci kopyası için; `TraceEncode`:
    /// trace özetine girecek kanonik baytlar için.
    type Msg: Clone + std::fmt::Debug + TraceEncode;

    /// Bir girdiyi işler ve istenen eylemleri döndürür. Çıktılar verildikleri sırayla uygulanır.
    ///
    /// Düğüm kendi rastgeleliğini kendi RNG'sinden almalıdır (ör. `SeedTree::rng_for` ile kurulan
    /// bir `ChaCha8Rng`); simülatör düğüme rastgele sayı vermez.
    #[must_use]
    fn step(&mut self, input: NodeInput<Self::Msg>) -> Vec<NodeOutput<Self::Msg>>;
}
