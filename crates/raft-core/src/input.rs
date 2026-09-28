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
    /// heartbeat sayaçları Faz 2'de bu olayla ilerletilecek.
    Tick,
    /// Başka bir düğümden bir Raft RPC'si (veya cevabı) geldi.
    Message {
        /// Mesajı gönderen düğümün kimliği.
        from: NodeId,
        /// İletilen RPC/cevap.
        msg: Message,
    },
    /// Bir istemci, durum makinesine uygulanmak üzere bir komut gönderdi.
    ClientRequest(Command),
    /// Düğüm çöktü ve yeniden başlatıldı; sürücü, simüle diskte `fsync` olmuş (dolayısıyla çökmeden
    /// sağ çıkan) kalıcı durumu geri veriyor. Çekirdek diski kendisi okuyamadığı için (sans-IO)
    /// kurtarma yalnızca bu değere dayanır; bellekte kalmış ama henüz persist edilmemiş
    /// `currentTerm`/`votedFor` burada YOKTUR — aksi hâlde "votedFor diske yazılmadan cevap
    /// verildi" gibi hatalar simülasyonda asla yakalanamazdı. R1: Faz 2'de `Restart`, tüm geçici
    /// durumu bu değerden yeniden kurar ve düğümü Follower olarak başlatır (Figure 2: geçici durum
    /// çökmede kaybolur).
    Restart(PersistentState),
}
