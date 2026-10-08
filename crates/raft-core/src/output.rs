//! `RaftNode::step`'in ürettiği, sürücünün sırayla yürütmesi gereken tek çıktı tipi.

use crate::message::Message;
use crate::persist::PersistUpdate;
use crate::types::{Command, LogIndex, NodeId};

/// Bir `step` çağrısının sonucunda yapılması istenen eylemlerden biri.
///
/// **Sıralama sözleşmesi (O1):** Bir `step` çağrısının döndürdüğü `Vec<Output>`, sürücü tarafından
/// SIRAYLA işlenmek zorundadır. Raft, `currentTerm`/`votedFor`/`log[]`'un RPC'lere cevap vermeden
/// önce kalıcı depoya yazılmasını şart koşar (Figure 2, "Persistent state": *updated on stable
/// storage before responding to RPCs*); bu yüzden aynı adımda üretilen bir `Persist`, ona bağlı
/// `Send`'lerden her zaman ÖNCE gelir. Sürücü (simülatör veya gerçek çalıştırıcı) bu sırayı asla
/// bozmamalıdır — aksi hâlde diske yazılmadan cevap verilmiş bir RPC, çökme sonrası tutarsızlık
/// yaratabilir (ör. aynı term'de iki farklı adaya oy verilmesi).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Belirtilen düğüme bir mesaj gönderilmesi istenir.
    Send {
        /// Mesajın gönderileceği düğüm.
        to: NodeId,
        /// Gönderilecek RPC/cevap.
        msg: Message,
    },
    /// Verilen farkın diske kalıcı biçimde (fsync ile) yazılması istenir; bu, aynı adımdaki
    /// sonraki `Output`'lardan (özellikle `Send`'lerden) ÖNCE tamamlanmış olmalıdır (O1). Fark
    /// diskteki duruma `PersistentState::apply` ile uygulanır.
    Persist(PersistUpdate),
    /// Verilen komutun durum makinesine uygulanması istenir: yalnızca commit edilmiş girdiler için,
    /// index sırasıyla ve her index bir kez (`commitIndex`/`lastApplied`, Figure 2). Yeniden
    /// başlatmadan sonra `lastApplied` 0'dan başladığı için girdiler baştan yeniden uygulanır;
    /// durum makinesi de çökmeyle kaybolan geçici bir yapı olarak baştan kurulmalıdır.
    ///
    /// `index`, girdinin log'daki yeridir. Neden komutla birlikte taşınıyor: State Machine Safety
    /// ("hiçbir iki düğüm aynı index'te farklı komut uygulamaz") doğrudan bu index üzerinden
    /// denetlenir, ve yeniden başlatma sonrası aynı girdinin tekrar uygulanması index'e bakılarak
    /// ayıklanabilir. Index olmasaydı, denetçi index'i uygulama sırasından tahmin etmek zorunda
    /// kalırdı ve bir kayma, ihlali gizleyebilirdi.
    Apply {
        /// Girdinin log index'i.
        index: LogIndex,
        /// Uygulanacak komut.
        command: Command,
    },
    /// İstemciye bir cevap gönderilmesi istenir.
    ClientResponse(ClientResponse),
}

/// İstemciye dönülecek cevabın gövdesi.
///
/// Faz 0'da yer tutucudur (boş `struct`); Faz 4'te `NotLeader { hint }` gibi varyantlar taşıyan bir
/// `enum`'a dönüşecek (§8: liderin kim olduğuna dair ipucu ile birlikte).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientResponse {}
