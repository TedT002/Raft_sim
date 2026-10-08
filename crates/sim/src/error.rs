//! Simülatörün yapılandırma, bölünme ve yaşam döngüsü (çökme/yeniden başlatma) hataları.

use raft_core::NodeId;

/// Geçersiz bir simülasyon ya da ağ yapılandırması.
///
/// Hatalı bir yapılandırma panikle değil bu tiple bildirilir: Faz 5'teki fuzz koşucusu binlerce
/// rastgele senaryo üretecek; tek bir geçersiz değer bütün süreci çökertmemeli, reddedilmelidir.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// Bir olasılık sonlu değil ya da `[0, 1]` aralığının dışında.
    #[error("{name} must be a probability in [0, 1], got {value}")]
    InvalidProbability {
        /// Alanın adı (ör. `drop_prob`).
        name: &'static str,
        /// Verilen değer.
        value: f64,
    },
    /// `min_delay` sıfır. Bir mesaj gönderildiği anda varamaz: aksi hâlde aynı zaman damgasında
    /// sonsuz bir mesaj zinciri oluşabilir ve `run_until` hiç ilerleyemezdi.
    #[error("min_delay must be at least 1 tick")]
    ZeroMinDelay,
    /// `min_delay`, `max_delay`'den büyük.
    #[error("min_delay ({min}) must not exceed max_delay ({max})")]
    DelayRange {
        /// En küçük gecikme (tick).
        min: u64,
        /// En büyük gecikme (tick).
        max: u64,
    },
    /// `tick_every` sıfır: saat hiç ilerlemezdi.
    #[error("tick_every must be at least 1")]
    ZeroTickInterval,
    /// `min_fsync_delay`, `max_fsync_delay`'den büyük.
    #[error("min_fsync_delay ({min}) must not exceed max_fsync_delay ({max})")]
    FsyncDelayRange {
        /// En kısa fsync gecikmesi (tick).
        min: u64,
        /// En uzun fsync gecikmesi (tick).
        max: u64,
    },
    /// Aynı düğüm kimliği birden fazla kez verildi.
    #[error("node {0:?} was added more than once")]
    DuplicateNode(NodeId),
}

/// Geçersiz bir bölünme tanımı. Hata durumunda ne ağ ne trace değişir.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PartitionError {
    /// Bir düğüm tanımda birden fazla kez geçiyor (aynı grupta ya da farklı gruplarda): hangi gruba
    /// ait olduğu belirsiz olurdu, sessizce "son yazılan kazanır" demek ise bir yazım hatasını
    /// gizlerdi.
    #[error("node {0:?} is listed more than once in the partition")]
    DuplicateNode(NodeId),
    /// Tanımdaki bir düğüm simülasyonda yok: büyük olasılıkla bir yazım hatası. Kabul edilseydi
    /// bölünme, farkına varılmadan amaçlanandan farklı bir ağ kurardı.
    #[error("node {0:?} is not part of the simulation")]
    UnknownNode(NodeId),
}

/// Geçersiz bir çökme, yeniden başlatma ya da istemci isteği. Hata durumunda simülasyon değişmez
/// (ne düğüm ne trace).
///
/// Sessizce yok saymak yerine hata dönülür: zaten çökmüş bir düğümü "çökertmek" ya da ayaktaki bir
/// düğümü "yeniden başlatmak", hata senaryosunu üreten kodda (Faz 5 fuzz koşucusu) bir mantık
/// hatasının işaretidir; yok sayılsaydı senaryo, yazıldığından farklı bir koşu üretirdi.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleError {
    /// Düğüm simülasyonda yok.
    #[error("node {0:?} is not part of the simulation")]
    UnknownNode(NodeId),
    /// Düğüm zaten çökmüş durumda.
    #[error("node {0:?} is already down")]
    AlreadyDown(NodeId),
    /// Düğüm zaten ayakta.
    #[error("node {0:?} is already up")]
    AlreadyUp(NodeId),
    /// Düğüm çökmüş durumda: istek alamaz.
    #[error("node {0:?} is down")]
    Down(NodeId),
}
