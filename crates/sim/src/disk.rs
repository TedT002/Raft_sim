//! Simüle disk: yazmalar `fsync` tamamlanana kadar bekler; çökme bekleyen yazmaları kaybettirir.
//!
//! Model (bkz. `Simulation`, diski bu kurallarla işletir):
//!
//! - Düğümün her `Persist` çıktısı bir **yazma**dır. Yazma hemen verilir ama ancak `fsync`
//!   tamamlanınca kalıcı olur. fsync gecikmesi `[min_fsync_delay, max_fsync_delay]` aralığından
//!   Disk alt-seed'iyle çekilir; bir düğümün fsync'leri yazma sırasıyla tamamlanır (FIFO).
//! - O1'i (`Persist`, sonraki çıktılardan önce kalıcı olmalı) sürücü zorlar: bir yazmadan SONRA
//!   üretilen `Send` ve `Apply`'lar, o yazma ve öncekilerin hepsi kalıcı olana kadar tutulur. Bir
//!   çıktı yalnızca kendisinden ÖNCE verilmiş yazmaları bekler: bir çekirdek cevabı `Persist`'ten
//!   önce koyarsa cevap o yazmayı beklemez ve durum kalıcı olmadan ağa çıkar. Simülatör bu ters
//!   sırayı üretildiği adımda kaydeder (bkz. `Simulation::take_persists_after_output`); fsync
//!   penceresinde gelen bir çökme ise sonucunu görünür kılar.
//! - Çökmede tutulan çıktılar ve bekleyen yazmalar kaybolur. `partial_write_prob` olasılıkla
//!   bekleyenlerin rastgele bir öneki yine de diske ulaşmış sayılır ("kısmen yazılır": disk yazmayı
//!   bitirmiş ama süreç bunu öğrenemeden çökmüştür). Her yazma kendi içinde atomiktir ve yazmalar
//!   sıralıdır: daha sonraki bir yazma, öncekisi olmadan diske ulaşmaz.
//! - Gecikmesi 0 olan disk ([`SimDisk::instant`]) yazmaları hemen kalıcı yapar ve hiç rastgele
//!   sayı çekmez; protokolü diskle ilgilenmeyen testler bunu kullanır.

use crate::error::ConfigError;
use crate::rng::{ChaCha8Rng, chance, uniform_inclusive};

/// Simüle diskin ayarları.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskConfig {
    /// En kısa fsync gecikmesi (tick). 0, yazmanın (önünde bekleyen yazma yoksa) aynı anda kalıcı
    /// olması demektir.
    pub min_fsync_delay: u64,
    /// En uzun fsync gecikmesi (tick, ≥ `min_fsync_delay`).
    pub max_fsync_delay: u64,
    /// Çökme anında bekleyen yazmaların rastgele bir öneğinin yine de diske ulaşmış sayılma
    /// olasılığı, `[0, 1]`.
    pub partial_write_prob: f64,
}

impl DiskConfig {
    /// Gecikmesiz disk: her yazma hemen kalıcıdır, çökme hiçbir şey kaybettirmez.
    #[must_use]
    pub const fn instant() -> Self {
        Self {
            min_fsync_delay: 0,
            max_fsync_delay: 0,
            partial_write_prob: 0.0,
        }
    }

    /// Ayarları doğrular: olasılık sonlu ve `[0, 1]` içinde, `min_fsync_delay ≤ max_fsync_delay`.
    ///
    /// # Errors
    ///
    /// Kurallardan biri çiğnenirse ilgili [`ConfigError`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        let p = self.partial_write_prob;
        if !(p.is_finite() && (0.0..=1.0).contains(&p)) {
            return Err(ConfigError::InvalidProbability {
                name: "partial_write_prob",
                value: p,
            });
        }
        if self.min_fsync_delay > self.max_fsync_delay {
            return Err(ConfigError::FsyncDelayRange {
                min: self.min_fsync_delay,
                max: self.max_fsync_delay,
            });
        }
        Ok(())
    }
}

impl Default for DiskConfig {
    /// fsync 1..3 tick sürer; bir çökmede bekleyen yazmaların dörtte bir olasılıkla bir öneki
    /// diske ulaşır. Her yazmada bir pencere açılır ve çökmeler o pencereye denk gelebilir.
    fn default() -> Self {
        Self {
            min_fsync_delay: 1,
            max_fsync_delay: 3,
            partial_write_prob: 0.25,
        }
    }
}

/// Simüle disk: ayarlar ve (gecikmeli diskte) Disk alt-seed'inden gelen RNG.
#[derive(Debug, Clone)]
pub struct SimDisk {
    config: DiskConfig,
    // Gecikmesiz diskte RNG yoktur: hiçbir karar rastgele değildir.
    rng: Option<ChaCha8Rng>,
}

impl SimDisk {
    /// Gecikmesiz disk (bkz. [`DiskConfig::instant`]); RNG gerekmez ve hiç çekiliş yapılmaz.
    #[must_use]
    pub fn instant() -> Self {
        Self {
            config: DiskConfig::instant(),
            rng: None,
        }
    }

    /// Verilen ayarlarla ve verilen RNG ile bir disk. RNG'yi dışarıdan almak, diskin akışının
    /// `SeedTree`'deki kendi bileşeninden (`Component::Disk`) gelmesini sağlar.
    ///
    /// # Errors
    ///
    /// Ayarlar geçersizse [`ConfigError`].
    pub fn new(config: DiskConfig, rng: ChaCha8Rng) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            rng: Some(rng),
        })
    }

    /// Diskin ayarları.
    #[must_use]
    pub fn config(&self) -> &DiskConfig {
        &self.config
    }

    /// Bir yazmanın fsync gecikmesi. Gecikmeli diskte TAM OLARAK bir çekiliş yapar.
    pub(crate) fn fsync_delay(&mut self) -> u64 {
        match &mut self.rng {
            None => 0,
            Some(rng) => uniform_inclusive(
                rng,
                self.config.min_fsync_delay,
                self.config.max_fsync_delay,
            ),
        }
    }

    /// Çökmede bekleyen `pending` yazmanın kaçının (baştan itibaren) yine de diske ulaştığı.
    /// Gecikmeli diskte, bekleyen yazma olsun olmasın, TAM OLARAK iki çekiliş yapar: çekiliş sayısı
    /// çökme anındaki duruma bağlı olsaydı, diskin sonraki bütün kararları ondan etkilenirdi.
    pub(crate) fn kept_on_crash(&mut self, pending: usize) -> usize {
        let Some(rng) = &mut self.rng else {
            return 0;
        };
        let partial = chance(rng, self.config.partial_write_prob);
        let upper = u64::try_from(pending).unwrap_or(u64::MAX);
        let kept = uniform_inclusive(rng, 0, upper);
        if partial {
            usize::try_from(kept).unwrap_or(pending).min(pending)
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DiskConfig, SimDisk};
    use crate::error::ConfigError;
    use crate::rng::{Component, SeedTree};

    // Geçersiz ayarlar hata döner; varsayılan ve gecikmesiz ayarlar geçerlidir.
    #[test]
    fn disk_configs_are_validated() {
        let bad_range = DiskConfig {
            min_fsync_delay: 3,
            max_fsync_delay: 1,
            ..DiskConfig::default()
        };
        assert_eq!(
            bad_range.validate(),
            Err(ConfigError::FsyncDelayRange { min: 3, max: 1 })
        );
        let bad_prob = DiskConfig {
            partial_write_prob: f64::NAN,
            ..DiskConfig::default()
        };
        assert!(matches!(
            bad_prob.validate(),
            Err(ConfigError::InvalidProbability {
                name: "partial_write_prob",
                ..
            })
        ));
        assert_eq!(DiskConfig::default().validate(), Ok(()));
        assert_eq!(DiskConfig::instant().validate(), Ok(()));
    }

    // Gecikmesiz disk hiçbir şey kaybettirmez; gecikmeli disk gecikmeyi aralıktan seçer ve kısmi
    // yazmada bekleyenlerin en fazla tamamını korur. Olasılık 0 iken hiçbir şey korunmaz.
    #[test]
    fn delays_and_crash_losses_stay_in_range() {
        let mut instant = SimDisk::instant();
        assert_eq!(instant.fsync_delay(), 0);
        assert_eq!(instant.kept_on_crash(5), 0);

        let rng = SeedTree::new(3).rng_for(Component::Disk);
        let config = DiskConfig {
            min_fsync_delay: 2,
            max_fsync_delay: 4,
            partial_write_prob: 1.0,
        };
        let mut disk = SimDisk::new(config, rng).expect("valid config");
        let mut kept_some = false;
        for _ in 0..200 {
            assert!((2..=4).contains(&disk.fsync_delay()));
            let kept = disk.kept_on_crash(3);
            assert!(kept <= 3);
            kept_some |= kept > 0 && kept < 3;
        }
        assert!(kept_some, "a strict prefix must be reachable");

        let rng = SeedTree::new(3).rng_for(Component::Disk);
        let never = DiskConfig {
            partial_write_prob: 0.0,
            ..config
        };
        let mut disk = SimDisk::new(never, rng).expect("valid config");
        assert!((0..200).all(|_| disk.kept_on_crash(3) == 0));
    }
}
