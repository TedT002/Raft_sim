//! Düğümün ayarları: seçim zaman aşımı ve heartbeat aralığı (tick cinsinden), bir AppendEntries
//! mesajının taşıyabileceği en fazla girdi sayısı ve isteğe bağlı lider kiralaması (tezin §6.4.1).

use std::num::NonZeroUsize;

/// Bir AppendEntries'in varsayılan en fazla girdi sayısı. `expect` derleme zamanında (const)
/// değerlendirilir: değer sıfır olsaydı bu bir derleme hatası olurdu, çalışma zamanında panik
/// imkânsızdır.
const DEFAULT_MAX_ENTRIES: NonZeroUsize = NonZeroUsize::new(64).expect("64 is not zero");

/// Raft'ın zamanlama ayarları. Bütün değerler mantıksal tick cinsindendir; gerçek saatle ilgileri
/// yoktur (sans-IO: zaman yalnızca `Input::Tick` ile ilerler).
///
/// Geçersiz bir ayar kurulamaz: alanlar private'tır ve tek kurucu doğrulama yapan
/// [`Config::new`]'dir. Böylece `RaftNode::new` hata döndürmek zorunda kalmaz ve düğümün içinde
/// "ya ayar bozuksa?" kontrollerine gerek kalmaz.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    election_timeout: u64,
    heartbeat_interval: u64,
    max_entries: NonZeroUsize,
    lease: Option<u64>,
}

impl Config {
    /// Ayarları doğrulayıp kurar.
    ///
    /// - `election_timeout` (T): seçim zaman aşımının tabanı. Zaman aşımı her sıfırlamada `[T, 2T)`
    ///   aralığından rastgele çekilir (§5.2).
    /// - `heartbeat_interval`: liderin boş `AppendEntries` (heartbeat) gönderme aralığı.
    ///
    /// # Errors
    ///
    /// `heartbeat_interval` sıfırsa ya da `election_timeout`'tan küçük değilse [`ConfigError`].
    pub fn new(election_timeout: u64, heartbeat_interval: u64) -> Result<Self, ConfigError> {
        if heartbeat_interval == 0 {
            return Err(ConfigError::ZeroHeartbeatInterval);
        }
        // Kesin alt sınır: heartbeat aralığı zaman aşımının altında olmalı. Aksi hâlde kayıpsız bir
        // ağda bile takipçiler bir sonraki heartbeat'ten önce zaman aşımına uğrar ve küme durmadan
        // seçim yapar (canlılık kaybı). Pratikte aralık bundan çok daha küçük seçilir (§5.6:
        // broadcastTime ≪ electionTimeout), çünkü ağ gecikmesi ve kayıplar heartbeat'leri
        // seyreltir.
        if heartbeat_interval >= election_timeout {
            return Err(ConfigError::HeartbeatNotBelowElectionTimeout {
                heartbeat_interval,
                election_timeout,
            });
        }
        Ok(Self {
            election_timeout,
            heartbeat_interval,
            max_entries: DEFAULT_MAX_ENTRIES,
            lease: None,
        })
    }

    /// Lider kiralamasını açar (tezin §6.4.1): çoğunluğun onayladığı bir doğrulama turunun
    /// GÖNDERİLDİĞİ andan sonraki `lease` tick boyunca lider, okumaları tur beklemeden cevaplar
    /// (bkz. `Input::Read`, Q2). Kiralama açıkken takipçiler bir liderden haber aldıktan (ya da
    /// açıldıktan) sonraki T tick boyunca bütün oy isteklerini yok sayar (§4.2.3): kiralamanın
    /// güvenliği buna dayanır.
    ///
    /// Neden `lease < T`: onay veren her takipçi, liderden haber aldığı andan (≥ turun gönderimi)
    /// sonraki T tick boyunca kimseye oy vermez; lider ise ancak liderliği bırakarak oy verebilir
    /// ve bırakınca kiralaması biter. Yani lider kaldığı ve kiralaması sürdüğü sürece başka bir
    /// lider yoktur. Aradaki `T - lease` tick, saatlerin hızları arasındaki sapmaya (drift) ayrılan
    /// paydır: kiralama ancak saatler bir kiralama süresinde bu paydan fazla ayrışmıyorsa
    /// güvenlidir.
    ///
    /// # Errors
    ///
    /// `lease` sıfırsa ya da seçim zaman aşımı tabanından (T) küçük değilse [`ConfigError`].
    pub const fn with_lease(mut self, lease: u64) -> Result<Self, ConfigError> {
        if lease == 0 {
            return Err(ConfigError::ZeroLease);
        }
        if lease >= self.election_timeout {
            return Err(ConfigError::LeaseNotBelowElectionTimeout {
                lease,
                election_timeout: self.election_timeout,
            });
        }
        self.lease = Some(lease);
        Ok(self)
    }

    /// Bir AppendEntries'in taşıyabileceği en fazla girdi sayısını değiştirir (varsayılan 64).
    ///
    /// Sınır neden var: geride kalmış bir takipçiye bütün eksik log'u tek mesajda göndermek,
    /// kayıplı bir ağda her kayıpta hepsini yeniden göndermek demektir. Sıfır kabul edilmez (tip
    /// garanti eder): girdi taşıyamayan bir lider eksik girdileri hiç gönderemezdi.
    #[must_use]
    pub const fn with_max_entries(mut self, max_entries: NonZeroUsize) -> Self {
        self.max_entries = max_entries;
        self
    }

    /// Seçim zaman aşımı tabanı T (tick). Zaman aşımı `[T, 2T)` aralığından çekilir.
    #[must_use]
    pub const fn election_timeout(&self) -> u64 {
        self.election_timeout
    }

    /// Heartbeat aralığı (tick).
    #[must_use]
    pub const fn heartbeat_interval(&self) -> u64 {
        self.heartbeat_interval
    }

    /// Bir AppendEntries'in taşıyabileceği en fazla girdi sayısı (en az 1).
    #[must_use]
    pub const fn max_entries(&self) -> usize {
        self.max_entries.get()
    }

    /// Lider kiralamasının süresi (tick); kiralama kapalıysa `None` (varsayılan).
    #[must_use]
    pub const fn lease(&self) -> Option<u64> {
        self.lease
    }
}

impl Default for Config {
    /// T = 20, heartbeat = 4 tick (oran 1/5). Gecikmesi 1..5 tick olan bir ağda bir takipçi, arka
    /// arkaya birkaç heartbeat kaybolmadıkça zaman aşımına uğramaz.
    fn default() -> Self {
        Self {
            election_timeout: 20,
            heartbeat_interval: 4,
            max_entries: DEFAULT_MAX_ENTRIES,
            lease: None,
        }
    }
}

/// Geçersiz bir [`Config`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// Heartbeat aralığı sıfır. "Sıfır tick'te bir" anlamsızdır; en sık aralık her tick'tir (1).
    #[error("heartbeat_interval must be at least 1 tick")]
    ZeroHeartbeatInterval,
    /// Heartbeat aralığı seçim zaman aşımı tabanından küçük değil: takipçiler heartbeat'ler
    /// arasında zaman aşımına uğrar.
    #[error(
        "heartbeat_interval ({}) must be below election_timeout ({})",
        .heartbeat_interval,
        .election_timeout
    )]
    HeartbeatNotBelowElectionTimeout {
        /// Verilen heartbeat aralığı.
        heartbeat_interval: u64,
        /// Verilen seçim zaman aşımı tabanı.
        election_timeout: u64,
    },
    /// Kiralama süresi sıfır: hiçbir okumayı kapsamayan bir kiralama anlamsızdır.
    #[error("lease must be at least 1 tick")]
    ZeroLease,
    /// Kiralama, seçim zaman aşımı tabanından kısa değil: yeni bir lider, eskinin kiralaması
    /// bitmeden seçilebilir (bkz. `Config::with_lease`).
    #[error("lease ({}) must be below election_timeout ({})", .lease, .election_timeout)]
    LeaseNotBelowElectionTimeout {
        /// Verilen kiralama süresi.
        lease: u64,
        /// Seçim zaman aşımı tabanı.
        election_timeout: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::{Config, ConfigError};

    // Geçerli ve geçersiz ayarlar: sıfır heartbeat ve zaman aşımından küçük olmayan heartbeat
    // reddedilir; varsayılanlar geçerli bir ayardır.
    #[test]
    fn config_is_validated() {
        assert_eq!(Config::new(20, 0), Err(ConfigError::ZeroHeartbeatInterval));
        assert_eq!(
            Config::new(10, 10),
            Err(ConfigError::HeartbeatNotBelowElectionTimeout {
                heartbeat_interval: 10,
                election_timeout: 10
            })
        );
        let config = Config::new(10, 9).expect("valid config");
        assert_eq!(
            (config.election_timeout(), config.heartbeat_interval()),
            (10, 9)
        );
        assert_eq!(
            ConfigError::HeartbeatNotBelowElectionTimeout {
                heartbeat_interval: 10,
                election_timeout: 10
            }
            .to_string(),
            "heartbeat_interval (10) must be below election_timeout (10)"
        );
        let default = Config::default();
        assert_eq!(
            Config::new(default.election_timeout(), default.heartbeat_interval()),
            Ok(default)
        );
        assert_eq!(default.lease(), None, "leases are off by default");
    }

    // Kiralama, seçim zaman aşımı tabanının (T) altında ve sıfırdan büyük olmalıdır.
    #[test]
    fn leases_are_validated() {
        let config = Config::default();
        assert_eq!(config.with_lease(0), Err(ConfigError::ZeroLease));
        assert_eq!(
            config.with_lease(20),
            Err(ConfigError::LeaseNotBelowElectionTimeout {
                lease: 20,
                election_timeout: 20
            })
        );
        let leased = config.with_lease(19).expect("below T");
        assert_eq!(leased.lease(), Some(19));
        assert_eq!(
            (leased.election_timeout(), leased.heartbeat_interval()),
            (20, 4)
        );
        assert_eq!(
            ConfigError::LeaseNotBelowElectionTimeout {
                lease: 20,
                election_timeout: 20
            }
            .to_string(),
            "lease (20) must be below election_timeout (20)"
        );
    }
}
