//! Simüle ağ: her mesajın kaderine (kayıp, gecikmeli tek teslim, çoğaltma) ve bölünmelere karar
//! verir.
//!
//! Sıra değişimi ayrı bir ayar değildir, gecikmenin doğal sonucudur: her mesaj `[min_delay,
//! max_delay]` aralığından bağımsız bir gecikme alır. A'dan B'ye önce gönderilen bir mesaj, sonra
//! gönderilenden daha uzun gecikme alırsa ondan sonra varır. Gerçek ağlardaki yeniden sıralama da
//! Raft'ın (ve her dağıtık protokolün) dayanması gereken bir durumdur.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use raft_core::NodeId;

use crate::error::{ConfigError, PartitionError};
use crate::rng::{ChaCha8Rng, chance, uniform_inclusive};
use crate::trace::DropReason;

/// Ağın rastgele davranışını belirleyen ayarlar.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NetworkConfig {
    /// Bir mesajın rastgele kaybolma olasılığı, `[0, 1]`.
    pub drop_prob: f64,
    /// Kaybolmayan bir mesajın bir kez daha (ikinci bir kopya olarak) teslim edilme olasılığı,
    /// `[0, 1]`.
    pub duplicate_prob: f64,
    /// En küçük gecikme (tick, ≥ 1).
    pub min_delay: u64,
    /// En büyük gecikme (tick, ≥ `min_delay`).
    pub max_delay: u64,
}

impl NetworkConfig {
    /// Kayıpsız, çoğaltmasız ve sabit gecikmeli bir ağ: zamanlamayı kesin bilmek isteyen testler
    /// için.
    #[must_use]
    pub const fn reliable(delay: u64) -> Self {
        Self {
            drop_prob: 0.0,
            duplicate_prob: 0.0,
            min_delay: delay,
            max_delay: delay,
        }
    }

    /// Ayarları doğrular: olasılıklar sonlu ve `[0, 1]` içinde, `1 ≤ min_delay ≤ max_delay`.
    ///
    /// # Errors
    ///
    /// Kurallardan biri çiğnenirse ilgili [`ConfigError`] döner.
    pub fn validate(&self) -> Result<(), ConfigError> {
        check_probability("drop_prob", self.drop_prob)?;
        check_probability("duplicate_prob", self.duplicate_prob)?;
        if self.min_delay == 0 {
            return Err(ConfigError::ZeroMinDelay);
        }
        if self.min_delay > self.max_delay {
            return Err(ConfigError::DelayRange {
                min: self.min_delay,
                max: self.max_delay,
            });
        }
        Ok(())
    }
}

/// Olasılık değeri sonlu ve `[0, 1]` içinde mi? (NaN da reddedilir.)
fn check_probability(name: &'static str, value: f64) -> Result<(), ConfigError> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        Err(ConfigError::InvalidProbability { name, value })
    }
}

/// Ağın bir mesaj için verdiği karar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fate {
    /// Mesaj hiç teslim edilmeyecek.
    Dropped(DropReason),
    /// Mesaj `delay` tick sonra teslim edilecek; `duplicate` varsa bir kopyası da o kadar tick
    /// sonra.
    ///
    /// Gecikmeler GÖRELİ ve sıfırdan büyüktür (`NonZeroU64`): "yeni olay hep şimdiden sonraya
    /// konur" kuralını tip garanti eder. Hiçbir `Network` uygulaması bir mesajı aynı âna (sonsuz
    /// döngü riski) ya da geçmişe (saatin geri gitmesi) koyamaz; en fazla bir kopya kuralı da tipin
    /// içindedir.
    Deliver {
        /// Asıl mesajın gecikmesi (tick).
        delay: NonZeroU64,
        /// Çoğaltılmışsa kopyanın (bağımsız) gecikmesi.
        duplicate: Option<NonZeroU64>,
    },
}

/// Bir ağ modeli: mesajların kaderine ve düğümler arası bağlantıya karar verir.
///
/// Mesajın içeriğini bilmez; yalnızca kimin kime gönderdiğine bakar. Bağlantı düzeni her
/// `partition`/`heal` çağrısında yeni bir **epoch** başlatır; yoldaki bir mesajın teslim edilip
/// edilmeyeceğine, gönderildiği epoch'tan bu yana yaşanan düzenlere bakılarak karar verilir.
pub trait Network {
    /// `from`'dan `to`'ya gönderilen bir mesajın kaderi.
    fn route(&mut self, from: NodeId, to: NodeId) -> Fate;

    /// İki düğüm şu anda haberleşebiliyor mu?
    fn connected(&self, a: NodeId, b: NodeId) -> bool;

    /// Şu anki bağlantı düzeninin numarası.
    fn epoch(&self) -> u64;

    /// `a` ile `b`, `since` epoch'undan bu yana (o dahil) HER bağlantı düzeninde bağlı mıydı?
    /// Yoldaki bir mesaj ancak uçuşu boyunca uçları hiç ayrılmadıysa teslim edilir.
    fn connected_since(&self, a: NodeId, b: NodeId, since: u64) -> bool;

    /// Ağı gruplara böler: yalnızca aynı gruptaki düğümler haberleşebilir. Hiçbir grupta geçmeyen
    /// düğümler tamamen yalıtılır (birbirlerinden de).
    ///
    /// # Errors
    ///
    /// Bir düğüm tanımda birden fazla kez geçiyorsa [`PartitionError`] döner ve ağ değişmez.
    fn partition(&mut self, groups: &[&[NodeId]]) -> Result<(), PartitionError>;

    /// Bölünmeyi kaldırır: herkes yeniden herkesle haberleşebilir.
    fn heal(&mut self);
}

/// Bir bağlantı düzeni: `None` = bölünme yok; `Some(harita)` = her düğümün grup numarası.
///
/// `BTreeMap`: burada yalnızca arama yapılıyor, yineleme yok. Yine de `HashMap` yerine `BTreeMap`
/// seçildi. Bu kod gelecekte düzen üzerinde yinelerse (ör. trace'e yazarken) sıra kendiliğinden
/// deterministik kalır; projenin "şüphede BTreeMap" kuralı da budur.
type Layout = Option<BTreeMap<NodeId, usize>>;

/// Düzen `a` ile `b`'yi aynı grupta tutuyor mu?
fn layout_connects(layout: &Layout, a: NodeId, b: NodeId) -> bool {
    if a == b {
        return true;
    }
    match layout {
        None => true,
        // Hiçbir grupta geçmeyen düğüm (None) kimseyle aynı grupta değildir: yalıtılmıştır.
        Some(groups) => match (groups.get(&a), groups.get(&b)) {
            (Some(ga), Some(gb)) => ga == gb,
            _ => false,
        },
    }
}

/// Seed'li, deterministik simüle ağ.
///
/// Bölünme "kablo kesildi" modeliyle uygulanır. Bir mesaj ancak gönderildiği andan teslim anına
/// kadar uçları HİÇ ayrı gruplara düşmediyse teslim edilir. Bu kuralın üç sonucu vardır:
/// - Bölünme sırasında gönderilen mesaj gönderim anında düşer.
/// - Yoldaki bir mesaj, uçuşu sırasında bir bölünme uçlarını ayırırsa kaybolur. Bölünme teslim
///   anından önce `heal()` ile bitmiş olsa bile bu değişmez.
/// - `heal()` sonrası yalnızca yeni gönderilen mesajlar ulaşır.
///
/// Gerçek bir bağlantı kopmasına en yakın ve protokolü en çok zorlayan model budur.
#[derive(Debug, Clone)]
pub struct SimNetwork {
    config: NetworkConfig,
    rng: ChaCha8Rng,
    /// Bağlantı düzenlerinin geçmişi; indeks = epoch. İlk düzen (epoch 0) bölünmesizdir. Geçmiş,
    /// yoldaki bir mesajın uçuşu boyunca yaşanmış her bölünmeyi görebilmek için tutulur.
    layouts: Vec<Layout>,
}

impl SimNetwork {
    /// Verilen ayarlarla ve verilen RNG ile bir ağ kurar. RNG'yi dışarıdan almak, ağın akışının
    /// `SeedTree`'deki kendi bileşeninden (`Component::Network`) gelmesini sağlar.
    ///
    /// # Errors
    ///
    /// Ayarlar geçersizse [`ConfigError`] döner.
    pub fn new(config: NetworkConfig, rng: ChaCha8Rng) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            rng,
            layouts: vec![None],
        })
    }

    /// Ağın ayarları.
    #[must_use]
    pub fn config(&self) -> &NetworkConfig {
        &self.config
    }

    /// `[min_delay, max_delay]` aralığından bir gecikme (tam olarak bir çekiliş).
    fn delay(&mut self) -> NonZeroU64 {
        let ticks = uniform_inclusive(&mut self.rng, self.config.min_delay, self.config.max_delay);
        // Doğrulama `min_delay ≥ 1`'i garanti eder ve ayarlar kurulumdan sonra değiştirilemez; bu
        // yüzden 0 gelmesi imkânsızdır. Yine de panik yerine en küçük geçerli gecikmeye (1)
        // düşülür.
        NonZeroU64::new(ticks).unwrap_or(NonZeroU64::MIN)
    }

    fn current(&self) -> &Layout {
        // `layouts` hiçbir zaman boş değildir (kurucuda epoch 0 eklenir); boşsa bölünmesiz sayılır.
        self.layouts.last().unwrap_or(&None)
    }
}

impl Network for SimNetwork {
    fn route(&mut self, from: NodeId, to: NodeId) -> Fate {
        // Her çağrıda, sonuçtan bağımsız olarak, TAM OLARAK 4 çekiliş yapılır ve sıra sabittir:
        // kayıp, çoğaltma, gecikme-1, gecikme-2. Çekiliş SAYISI kaderden bağımsızdır: aynı `route`
        // çağrı dizisi (bölünme olsun olmasın, kayıp olsun olmasın) aynı sayıları alır. Uyarı:
        // protokol cevapları teslime bağlı olduğundan (düşen bir Ping'in Pong'u hiç gönderilmez),
        // tam bir simülasyonda bir ayar değişikliği çağrı DİZİSİNİ ve dolayısıyla sonraki kararları
        // yine değiştirebilir.
        let dropped = chance(&mut self.rng, self.config.drop_prob);
        let duplicated = chance(&mut self.rng, self.config.duplicate_prob);
        let first_delay = self.delay();
        let second_delay = self.delay();

        if !self.connected(from, to) {
            return Fate::Dropped(DropReason::PartitionAtSend);
        }
        if dropped {
            return Fate::Dropped(DropReason::Random);
        }
        // En fazla bir fazladan kopya; kopya kendi bağımsız gecikmesini alır, bu yüzden orijinalden
        // önce de varabilir.
        Fate::Deliver {
            delay: first_delay,
            duplicate: duplicated.then_some(second_delay),
        }
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        layout_connects(self.current(), a, b)
    }

    fn epoch(&self) -> u64 {
        // Epoch = düzen geçmişindeki son indeks. `usize`'dan `u64`'e dönüşüm kayıpsızdır.
        self.layouts.len().saturating_sub(1) as u64
    }

    fn connected_since(&self, a: NodeId, b: NodeId, since: u64) -> bool {
        // Bilinmeyen (gelecekteki) bir epoch denetlenecek düzen bırakmaz; epoch'lar yalnızca
        // `epoch()`'tan geldiği için bu dal pratikte erişilemez.
        let start = usize::try_from(since).unwrap_or(usize::MAX);
        self.layouts
            .iter()
            .skip(start)
            .all(|layout| layout_connects(layout, a, b))
    }

    fn partition(&mut self, groups: &[&[NodeId]]) -> Result<(), PartitionError> {
        let mut membership = BTreeMap::new();
        for (index, group) in groups.iter().enumerate() {
            for &node in *group {
                if membership.insert(node, index).is_some() {
                    return Err(PartitionError::DuplicateNode(node));
                }
            }
        }
        self.layouts.push(Some(membership));
        Ok(())
    }

    fn heal(&mut self) {
        self.layouts.push(None);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::{Fate, Network, NetworkConfig, SimNetwork};
    use crate::error::{ConfigError, PartitionError};
    use crate::rng::{Component, SeedTree};
    use crate::trace::DropReason;
    use raft_core::NodeId;

    fn network(config: NetworkConfig) -> SimNetwork {
        SimNetwork::new(config, SeedTree::new(11).rng_for(Component::Network))
            .expect("valid config")
    }

    const N1: NodeId = NodeId(1);
    const N2: NodeId = NodeId(2);
    const N3: NodeId = NodeId(3);
    const N4: NodeId = NodeId(4);

    // Geçersiz ayarlar panik değil hata üretir.
    #[test]
    fn invalid_configs_are_rejected() {
        let valid = NetworkConfig::reliable(1);
        let cases = [
            (
                NetworkConfig {
                    drop_prob: 1.5,
                    ..valid
                },
                ConfigError::InvalidProbability {
                    name: "drop_prob",
                    value: 1.5,
                },
            ),
            (
                NetworkConfig {
                    duplicate_prob: -0.1,
                    ..valid
                },
                ConfigError::InvalidProbability {
                    name: "duplicate_prob",
                    value: -0.1,
                },
            ),
            (
                NetworkConfig {
                    min_delay: 0,
                    ..valid
                },
                ConfigError::ZeroMinDelay,
            ),
            (
                NetworkConfig {
                    min_delay: 5,
                    max_delay: 2,
                    ..valid
                },
                ConfigError::DelayRange { min: 5, max: 2 },
            ),
        ];
        for (config, expected) in cases {
            assert_eq!(config.validate(), Err(expected));
        }
        let nan = NetworkConfig {
            drop_prob: f64::NAN,
            ..valid
        };
        assert!(matches!(
            nan.validate(),
            Err(ConfigError::InvalidProbability { .. })
        ));
        assert_eq!(valid.validate(), Ok(()));
    }

    // Bağlantı kuralları: HER grup kendi içinde haberleşir (yalnızca ilki değil), farklı gruplar
    // haberleşemez, listede olmayan düğümler yalıtılır (birbirlerinden de), düğüm kendisiyle her
    // zaman bağlıdır, heal herkesi yeniden bağlar.
    #[test]
    fn partition_connectivity_rules() {
        let mut net = network(NetworkConfig::reliable(1));
        assert!(net.connected(N1, N3));
        net.partition(&[&[N1], &[N2, N3]]).expect("valid partition");
        assert!(
            net.connected(N2, N3),
            "every group is connected internally, not just the first"
        );
        assert!(!net.connected(N1, N2));
        net.partition(&[&[N1, N2]]).expect("valid partition");
        assert!(net.connected(N1, N2));
        assert!(
            !net.connected(N1, N3),
            "N3 is in no group, so it is isolated"
        );
        assert!(
            !net.connected(N3, N4),
            "two unlisted nodes are isolated from each other too"
        );
        assert!(
            net.connected(N3, N3),
            "a node is always connected to itself"
        );
        net.heal();
        assert!(net.connected(N1, N3));
    }

    // Bir düğüm tanımda iki kez geçemez (farklı gruplarda da, aynı grupta da); hata durumunda ağ
    // eski hâlinde kalır.
    #[test]
    fn duplicate_node_in_partition_is_an_error() {
        let mut net = network(NetworkConfig::reliable(1));
        assert_eq!(
            net.partition(&[&[N1, N2], &[N2, N3]]),
            Err(PartitionError::DuplicateNode(N2))
        );
        assert_eq!(
            net.partition(&[&[N1, N1]]),
            Err(PartitionError::DuplicateNode(N1))
        );
        assert!(
            net.connected(N1, N3),
            "a failed partition must not change the network"
        );
        assert_eq!(net.epoch(), 0, "a failed partition must not start an epoch");
    }

    // Uçuş boyunca bağlantı: her partition/heal yeni bir epoch başlatır. Arada bir kez bile ayrılan
    // uçlar, şu an yeniden bağlı olsalar da "o epoch'tan beri bağlı" sayılmaz.
    #[test]
    fn connected_since_sees_every_layout_since_the_epoch() {
        let mut net = network(NetworkConfig::reliable(1));
        assert_eq!(net.epoch(), 0);
        net.partition(&[&[N1], &[N2]]).expect("valid partition");
        assert_eq!(net.epoch(), 1);
        net.heal();
        assert_eq!(net.epoch(), 2);
        assert!(net.connected(N1, N2), "currently connected again");
        assert!(
            !net.connected_since(N1, N2, 0),
            "but separated at some point since epoch 0"
        );
        assert!(net.connected_since(N1, N2, 2));
        assert!(net.connected_since(N1, N1, 0), "a node never loses itself");
    }

    // Gönderim anında bölünmüş uçlar arasındaki mesaj düşer; güvenilir ağda diğerleri tam
    // gecikmeyle ve kopyasız ulaşır.
    #[test]
    fn route_respects_partitions_and_fixed_delay() {
        let mut net = network(NetworkConfig::reliable(4));
        let four = NonZeroU64::new(4).expect("non-zero");
        assert_eq!(
            net.route(N1, N2),
            Fate::Deliver {
                delay: four,
                duplicate: None
            }
        );
        net.partition(&[&[N1], &[N2]]).expect("valid partition");
        assert_eq!(
            net.route(N1, N2),
            Fate::Dropped(DropReason::PartitionAtSend)
        );
    }

    // Bölünme, sonraki `route` çağrılarının rastgele kararlarını KAYDIRMAZ: bölünmüş ağın ilk
    // mesajı düşse bile her iki ağ da aynı sayıda çekiliş yapar ve sonraki çağrılarda aynı
    // kararları verir.
    #[test]
    fn partitions_do_not_shift_the_random_stream() {
        let lossy = NetworkConfig {
            drop_prob: 0.3,
            duplicate_prob: 0.3,
            min_delay: 1,
            max_delay: 50,
        };
        let mut plain = network(lossy);
        let mut cut = network(lossy);
        cut.partition(&[&[N1], &[N2]]).expect("valid partition");
        let _ = plain.route(N1, N2);
        assert_eq!(
            cut.route(N1, N2),
            Fate::Dropped(DropReason::PartitionAtSend)
        );
        cut.heal();
        for _ in 1..50 {
            assert_eq!(plain.route(N1, N2), cut.route(N1, N2));
        }
    }
}
