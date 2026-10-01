//! Düğümün zamanlama ayarları: seçim zaman aşımı ve heartbeat aralığı (tick cinsinden).

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
        })
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
}

impl Default for Config {
    /// T = 20, heartbeat = 4 tick (oran 1/5). Gecikmesi 1..5 tick olan bir ağda bir takipçi, arka
    /// arkaya birkaç heartbeat kaybolmadıkça zaman aşımına uğramaz.
    fn default() -> Self {
        Self {
            election_timeout: 20,
            heartbeat_interval: 4,
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
    }
}
