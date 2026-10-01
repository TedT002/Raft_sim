//! Simülatörün sürdüğü düğüm arayüzü: protokolden bağımsız, sans-IO.
//!
//! Simülatör Raft'ı bilmez; `SimNode` trait'ini uygulayan her düğümü sürebilir. Testlerdeki
//! Ping/Pong protokolü de, `raft` modülündeki adaptör üzerinden `raft_core::RaftNode` da böyle
//! sürülür. Arayüz raft-core'un `step(Input) -> Vec<Output>` biçimini aynalar: düğüm saate, ağa ve
//! diske dokunmaz; olayı alır, yapılacakları liste olarak döndürür.
//!
//! Disk de bu sözleşmenin parçasıdır: düğüm kalıcı durumunu kendisi yazamaz, `Persist` ister; çöküp
//! yeniden başladığında diskteki durumu `Restart` ile geri alır. Böylece "diske yazılmadan cevap
//! verildi" türünden hatalar simülasyonda görünür hâle gelir.

use raft_core::NodeId;

use crate::trace::TraceEncode;

/// Simülatörün bir düğüme verdiği girdi. `M` mesaj tipi, `D` diskteki kalıcı durumun tipidir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeInput<M, D> {
    /// Mantıksal zaman bir tick ilerledi.
    Tick,
    /// Başka bir düğümden bir mesaj geldi.
    Message {
        /// Gönderen düğüm.
        from: NodeId,
        /// Mesajın kendisi.
        msg: M,
    },
    /// Düğüm çöktü ve yeniden başlatıldı. Taşınan değer diskteki kalıcı durumdur: düğümün
    /// bellekteki her şeyi (diske yazılmamış değişiklikler dahil) kaybolmuştur ve kurtarma yalnızca
    /// bu değerden başlamalıdır.
    Restart(D),
}

/// Bir düğümün simülatörden yapmasını istediği eylem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeOutput<M, D> {
    /// `to` düğümüne mesaj gönder. Mesajın kaderine (kayıp, gecikme, çoğaltma, bölünme) ağ karar
    /// verir.
    Send {
        /// Alıcı düğüm.
        to: NodeId,
        /// Mesajın kendisi.
        msg: M,
    },
    /// Kalıcı durumu diske yaz. Çıktılar sırayla uygulandığından, aynı adımda kendisinden sonra
    /// gelen `Send`'ler ağa çıkmadan önce disk güncellenmiş olur. Faz 2'de disk anında ve atomik
    /// olarak kalıcıdır; `fsync`'e kadar bekleyen ve çökmede kaybolabilen yazmalar Faz 3'te
    /// gelecek.
    Persist(D),
}

/// Simülatörün sürebildiği bir düğüm.
pub trait SimNode {
    /// Düğümler arası mesaj tipi. `Clone`: çoğaltılan mesajın ikinci kopyası için; `TraceEncode`:
    /// trace özetine girecek kanonik baytlar için.
    type Msg: Clone + std::fmt::Debug + TraceEncode;

    /// Diskteki kalıcı durum: çökmeden sağ çıkan TEK şey. Kalıcı durumu olmayan düğümler `()`
    /// kullanır.
    ///
    /// `Default` taze bir düğümün boş diskidir: simülasyon her düğümü boş bir diskle başlatır, bu
    /// yüzden düğümler de boş diskle açılmış gibi kurulmalıdır. `TraceEncode`: her `Persist`'in
    /// özeti trace'e girer; böylece iç durumdaki bir sapma, ilk farklı mesajı beklemeden trace
    /// özetinde görünür.
    type Durable: Clone + std::fmt::Debug + Default + TraceEncode;

    /// Bir girdiyi işler ve istenen eylemleri döndürür. Çıktılar verildikleri sırayla uygulanır.
    ///
    /// Düğüm kendi rastgeleliğini kendi RNG'sinden almalıdır (ör. `SeedTree::rng_for` ile kurulan
    /// bir `ChaCha8Rng`); simülatör düğüme rastgele sayı vermez.
    #[must_use]
    fn step(
        &mut self,
        input: NodeInput<Self::Msg, Self::Durable>,
    ) -> Vec<NodeOutput<Self::Msg, Self::Durable>>;
}
