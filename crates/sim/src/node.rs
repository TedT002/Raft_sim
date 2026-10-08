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

/// Simülatörün bir düğüme verdiği girdi. `M` mesaj tipi, `D` diskteki kalıcı durumun tipi, `R`
/// istemci isteğinin tipidir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeInput<M, D, R> {
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
    /// Bir istemci düğüme bir istek verdi.
    Client(R),
}

/// Bir düğümün simülatörden yapmasını istediği eylem. `M` mesaj tipi, `U` diske yazılan farkın
/// tipi, `A` sırayla bırakılan yerel etkinin, `R` istemciye verilen cevabın tipidir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeOutput<M, U, A, R> {
    /// `to` düğümüne mesaj gönder. Mesajın kaderine (kayıp, gecikme, çoğaltma, bölünme) ağ karar
    /// verir.
    Send {
        /// Alıcı düğüm.
        to: NodeId,
        /// Mesajın kendisi.
        msg: M,
    },
    /// Kalıcı durumdaki bir değişikliği diske yaz. Çıktılar sırayla işlenir: bir `Persist`'ten
    /// sonra gelen `Send` ve `Apply`'lar, bu yazmanın `fsync`'i tamamlanana kadar simülatörde
    /// tutulur (bkz. `SimDisk`). Önce gelenler tutulamaz; simülatör bu ters sırayı kaydeder (bkz.
    /// `Simulation::take_persists_after_output`).
    Persist(U),
    /// Sırası geldiğinde bırakılacak yerel bir etki: ör. commit edilmiş bir girdinin durum
    /// makinesine uygulanması. Ağa gitmez; simülatör onu kaydeder ve sürücüye verir.
    Apply(A),
    /// İstemciye bir cevap: ör. Raft'ta lider olmayan düğümün "lider değilim" cevabı. Ağa gitmez
    /// (istemciler simülasyonda düğümlere doğrudan bağlıdır); simülatör onu kaydeder ve sürücüye
    /// verir. Dışarıya dönük bir çıktıdır: `Send` ve `Apply` gibi kendisinden önce verilmiş
    /// yazmalar kalıcı olana kadar tutulur (O1); cevap, kalıcı olmayan bir durumu dışarıya
    /// sızdırmamalıdır.
    Reply(R),
}

/// Diskte tutulan kalıcı durum ve ona uygulanan fark.
///
/// Düğüm her yazmada bütün durumu değil yalnızca farkı (`Update`) verir; disk farkları
/// [`DurableState::apply`] ile biriktirir. `Default` taze bir düğümün boş diskidir: simülasyon her
/// düğümü boş bir diskle başlatır, bu yüzden düğümler de boş diskle açılmış gibi kurulmalıdır.
/// `TraceEncode`: her yazmanın özeti trace'e girer; böylece iç durumdaki bir sapma, ilk farklı
/// mesajı beklemeden trace özetinde görünür.
pub trait DurableState: Clone + std::fmt::Debug + Default + TraceEncode {
    /// Diske yazılan fark (`NodeOutput::Persist` yükü).
    type Update: Clone + std::fmt::Debug + TraceEncode;

    /// Farkı bu duruma uygular.
    fn apply(&mut self, update: &Self::Update);
}

/// Kalıcı durumu olmayan düğümler için: fark da durum da boştur.
impl DurableState for () {
    type Update = ();

    fn apply(&mut self, _update: &()) {}
}

/// Simülatörün sürebildiği bir düğüm.
pub trait SimNode {
    /// Düğümler arası mesaj tipi. `Clone`: çoğaltılan mesajın ikinci kopyası için; `TraceEncode`:
    /// trace özetine girecek kanonik baytlar için.
    type Msg: Clone + std::fmt::Debug + TraceEncode;

    /// Diskteki kalıcı durum: çökmeden sağ çıkan TEK şey. Kalıcı durumu olmayan düğümler `()`
    /// kullanır.
    type Durable: DurableState;

    /// İstemci isteği (`NodeInput::Client`). İstemcisi olmayan düğümler `()` kullanır.
    type Request: Clone + std::fmt::Debug + TraceEncode;

    /// Sırayla bırakılan yerel etki (`NodeOutput::Apply`). Böyle bir etkisi olmayan düğümler `()`
    /// kullanır.
    type Applied: Clone + std::fmt::Debug + TraceEncode;

    /// İstemciye verilen cevap (`NodeOutput::Reply`). İstemcisi olmayan düğümler `()` kullanır.
    type Response: Clone + std::fmt::Debug + TraceEncode;

    /// Bir girdiyi işler ve istenen eylemleri döndürür. Çıktılar verildikleri sırayla uygulanır.
    ///
    /// Düğüm kendi rastgeleliğini kendi RNG'sinden almalıdır (ör. `SeedTree::rng_for` ile kurulan
    /// bir `ChaCha8Rng`); simülatör düğüme rastgele sayı vermez.
    #[must_use]
    fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>>;
}

/// `N` düğümünün girdi tipi.
pub type InputOf<N> =
    NodeInput<<N as SimNode>::Msg, <N as SimNode>::Durable, <N as SimNode>::Request>;

/// `N` düğümünün diske yazdığı farkın tipi.
pub type UpdateOf<N> = <<N as SimNode>::Durable as DurableState>::Update;

/// `N` düğümünün çıktı tipi.
pub type OutputOf<N> =
    NodeOutput<<N as SimNode>::Msg, UpdateOf<N>, <N as SimNode>::Applied, <N as SimNode>::Response>;
