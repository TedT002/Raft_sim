//! `RaftNode::step`'in ürettiği, sürücünün sırayla yürütmesi gereken tek çıktı tipi.

use crate::message::Message;
use crate::persist::{PersistUpdate, Snapshot};
use crate::types::{Command, LogIndex, NodeId, ReadId};

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
    /// index sırasıyla ve her index bir kez (`commitIndex`/`lastApplied`, Figure 2). Durum makinesi
    /// çökmeyle kaybolan geçici bir yapıdır: yeniden başlatmada sürücü onu diskteki snapshot'tan
    /// (§7; snapshot yoksa boş) kurar ve girdiler snapshot'ın ardından (yoksa 1'den) yeniden
    /// uygulanır. Bir snapshot'ın kapsadığı girdiler hiç `Apply` edilmez (bkz. `Restore`).
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
    /// Durum makinesini bu snapshot'la DEĞİŞTİR (§7, Figure 13'ün 8. adımı): düğüm liderden bir
    /// snapshot kurdu; `snapshot.last_index`'e kadar bütün girdiler uygulanmış sayılır
    /// (`lastApplied`). Sonraki `Apply`'lar `last_index + 1`'den devam eder. Bir `Apply` gibi
    /// sırayla yürütülür ve aynı adımın `Persist`'inden sonra gelir (O1): snapshot önce kalıcı
    /// olur.
    Restore(Snapshot),
    /// Bir okuma isteğinin (`Input::Read`) sonucu. Aynı adımın `Apply`'larından SONRA gelir:
    /// `Ready` geldiğinde sürücünün durum makinesi, çekirdeğin o ana kadar ürettiği bütün
    /// `Apply`'ları uygulamış olur (çıktılar sırayla yürütülür, O1) ve okuma o durumdan
    /// cevaplanır.
    Read {
        /// İsteğin kimliği.
        id: ReadId,
        /// Sonuç.
        outcome: ReadOutcome,
    },
    /// Bu adımı başlatan istemci isteğine bir cevap gönderilmesi istenir. Yalnızca bir
    /// `Input::ClientRequest` adımında üretilir; cevabın hangi isteğe ait olduğu böylece bellidir
    /// (çekirdek istemci kimliklerini bilmez, komutlar opaktır; C1).
    ClientResponse(ClientResponse),
}

/// Bir okumanın sonucu (ReadIndex, tezin §6.4'ü).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    /// Okuma şimdi yerel durum makinesinden cevaplanabilir. Lider, okuma geldikten SONRA
    /// başlattığı bir doğrulama turunu çoğunluğa onaylattı (o turda daha yüksek term'li bir lider
    /// yoktu) ve durum makinesi okumanın `readIndex`'ine kadar uygulandı. Okuma geldiğinde
    /// tamamlanmış her yazma bu durumdadır.
    Ready,
    /// Bu düğüm lider değil ya da okuma tamamlanmadan liderliği bıraktı; okuma cevaplanmadı ve
    /// başka bir düğüme yeniden gönderilebilir (okumanın hiçbir etkisi yoktur). `hint`, düğümün
    /// bildiği lider.
    NotLeader {
        /// Bilinen lider, varsa.
        hint: Option<NodeId>,
    },
}

/// Çekirdeğin bir istemci isteğine verdiği cevap.
///
/// Başarılı bir isteğin sonucu buradan gelmez: sonucu, commit edilen komutu uygulayan durum
/// makinesi üretir (çekirdeğin dışında; komutların anlamı oradadır, C1). Çekirdek yalnızca isteği
/// hiç kabul etmediğini, yani log'a eklemediğini bildirir. Bu ayrım istemcinin yeniden deneme
/// kararı için önemlidir: reddedilen istek hiçbir zaman uygulanmaz ve hemen başka bir düğüme
/// gönderilebilir. Kabul edilmiş ama cevabı gelmeyen bir istek ise uygulanmış da olabilir; onu
/// durum makinesinin tekilleştirmesi (§8) korur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResponse {
    /// Bu düğüm lider değil; istek log'a eklenmedi (§8: lider olmayan düğüm isteği reddeder ve
    /// bildiği lideri söyler). `hint`, düğümün bu term'de AppendEntries aldığı lider; bilmiyorsa
    /// (ör. adaysa ya da term yeni başladıysa) `None`.
    NotLeader {
        /// Bilinen lider, varsa.
        hint: Option<NodeId>,
    },
}
