//! `RaftNode::step`'e dışarıdan verilebilecek tek giriş tipi.

use crate::message::Message;
use crate::persist::PersistentState;
use crate::types::{Command, NodeId};

/// Sans-IO çekirdeğe dışarıdan gelebilecek tüm olaylar.
///
/// Çekirdek saati, ağı ve diski bilmez; bunların yerini bu enum'un varyantları tutar. Sürücü
/// (simülatör veya gerçek çalıştırıcı) zamanı/ağı/diski simüle eder veya gerçekten sürer, ama
/// çekirdeğe her zaman bu kapalı sözlükten bir değer verir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// Mantıksal zaman bir birim ilerledi (gerçek saatle ilgisi yok). Seçim zaman aşımı ve
    /// heartbeat sayaçları yalnızca bu olayla ilerler.
    Tick,
    /// Başka bir düğümden bir Raft RPC'si (veya cevabı) geldi.
    Message {
        /// Mesajı gönderen düğümün kimliği.
        from: NodeId,
        /// İletilen RPC/cevap.
        msg: Message,
    },
    /// Bir istemci, durum makinesine uygulanmak üzere bir komut gönderdi. İstemci arayüzü Faz
    /// 3/4'ün kapsamıdır; Faz 2'de bu girdi yok sayılır (hiç çıktı üretmez).
    ClientRequest(Command),
    /// Düğüm çöktü ve yeniden başlatıldı; sürücü, simüle diskte `fsync` olmuş (dolayısıyla çökmeden
    /// sağ çıkan) kalıcı durumu geri veriyor. Çekirdek diski kendisi okuyamadığı için (sans-IO)
    /// kurtarma yalnızca bu değere dayanır; bellekte kalmış ama henüz persist edilmemiş
    /// `currentTerm`/`votedFor` burada YOKTUR — aksi hâlde "votedFor diske yazılmadan cevap
    /// verildi" gibi hatalar simülasyonda asla yakalanamazdı. R1: `Restart` bütün geçici durumu
    /// siler, kalıcı durumu bu değerden yükler ve düğümü Follower olarak başlatır (Figure 2: geçici
    /// durum çökmede kaybolur). Hiç çıktı üretmez: yüklenen durum zaten diskteki durumdur.
    Restart(PersistentState),
}
