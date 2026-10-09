//! Kaos senaryoları: bir seed'den üretilen AÇIK bir hata programı ve onu koşturan sürücü.
//!
//! Üret-sonra-yürüt (generate-then-execute): senaryo koşudan önce bütünüyle üretilir. Program
//! (`Component::Scenario` akışından) hangi anda hangi hatanın enjekte edileceğini sabit bir listeye
//! döker; koşu bu listeyi yürütür. Neden: hata programı koşunun durumuna bakarak üretilseydi, bir
//! hatayı çıkarmak sonraki bütün kararları kaydırırdı ve başarısız bir senaryoyu küçültmek
//! (shrinking) imkânsızlaşırdı. Burada her hata, hedefini yürütüldüğü andaki duruma göre seçen bir
//! niyettir ("ayaktaki düğümlerden `pick`'inciyi çökert"). Bir hatayı listeden çıkarmak öbürlerini
//! yeniden tohumlamaz; koşu, hata listesinin deterministik bir fonksiyonu olarak kalır.
//!
//! Aynı program hem kaos testlerinde hem `raftsim fuzz`/`replay`'de koşar: basılan bir seed, testte
//! görülen koşuyu birebir yeniden üretir.
//!
//! Bir koşunun başarısı üç şeye bağlıdır: HER olaydan sonra kümenin denetimleri (bkz.
//! `RaftCluster`), hata programı bitip ağ iyileşince istemcilerin yeniden ilerlemesi (canlılık) ve
//! sonda istemci geçmişinin linearizability'si. Koşu sırasında atılan bir panik de başarısızlıktır
//! (bkz. [`RunError::Panic`]).

use std::any::Any;
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::{self, AssertUnwindSafe};

use checker::LinearizabilityError;
use raft_core::{Config as RaftConfig, NodeId};

use crate::client::{ClientConfig, ClientDriver, ClientStats, OpMix};
use crate::disk::DiskConfig;
use crate::error::ConfigError;
use crate::network::NetworkConfig;
use crate::raft::{ClusterConfig, ClusterError, RaftCluster, Violation};
use crate::rng::{Component, SeedTree, uniform_inclusive};
use crate::trace::TraceKind;

/// Bir senaryonun ayarları.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScenarioConfig {
    /// Düğüm sayısı: kimlikler `1..=nodes`.
    pub nodes: u64,
    /// Ağ (koşunun başındaki ve sakinleşmedeki ayarlar; `Loss` hataları kayıp oranını değiştirir).
    pub network: NetworkConfig,
    /// Simüle disk.
    pub disk: DiskConfig,
    /// İstemci iş yükü.
    pub clients: ClientConfig,
    /// Her düğümün Raft ayarları.
    pub raft: RaftConfig,
    /// Ardışık iki hata arasındaki en kısa ve en uzun süre (tick).
    pub fault_gap: (u64, u64),
    /// Hata türlerinin ağırlıkları.
    pub fault_mix: FaultMix,
    /// Hataların üretildiği süre (tick): hatalar `(0, horizon]` içindedir, sonra sakinleşme gelir.
    pub horizon: u64,
    /// Sakinleşmede her bekleme için tanınan en uzun süre (tick).
    pub settle: u64,
    /// Snapshot sıklığı (bkz. `ClusterConfig::snapshot_every`); `None`: log hiç sıkıştırılmaz.
    pub snapshot_every: Option<NonZeroU64>,
}

impl ScenarioConfig {
    /// Senaryoya özgü ayarı doğrular: hatalar arası süre en az 1 tick olmalı ve aralık boş
    /// olmamalı. Ağ, disk, istemci ve Raft ayarlarını koşu kurulurken kendi kurucuları doğrular.
    ///
    /// # Errors
    ///
    /// `fault_gap` geçersizse [`ConfigError::FaultGap`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        let (min, max) = self.fault_gap;
        if min == 0 || min > max {
            return Err(ConfigError::FaultGap { min, max });
        }
        Ok(())
    }

    /// Kaos taramasının ve `raftsim fuzz`'ın ayarları: 5 düğüm; %5 kayıp ve çoğaltmalı, 1..5 tick
    /// gecikmeli ağ; varsayılan disk (fsync 1..3 tick, kısmi yazma); 4 istemci 4 anahtar
    /// üzerinde, cevapların %10'u kaybolur; varsayılan Raft ayarları; hatalar arasında 5..40 tick;
    /// 1200 tick hata, 800 tick sakinleşme payı.
    #[must_use]
    pub fn chaos() -> Self {
        Self {
            nodes: 5,
            network: NetworkConfig {
                drop_prob: 0.05,
                duplicate_prob: 0.05,
                min_delay: 1,
                max_delay: 5,
                tail_prob: 0.0,
                tail_delay: 0,
            },
            disk: DiskConfig::default(),
            clients: ClientConfig {
                clients: 4,
                keys: 4,
                min_think: 2,
                max_think: 10,
                timeout: 40,
                max_attempts: 6,
                reply_loss_prob: 0.1,
                ops: OpMix::DEFAULT,
            },
            raft: RaftConfig::default(),
            fault_gap: (5, 40),
            fault_mix: FaultMix::CHAOS,
            horizon: 1_200,
            settle: 800,
            snapshot_every: None,
        }
    }

    /// Figure 8 türü hatalara (§5.4.2) odaklanan ayarlar. Yeni lider önceki term'lerin girdilerini
    /// takipçilere mesaj başına TEK girdiyle (`max_entries = 1`) gönderir: eski girdiler, liderin
    /// kendi term'indeki no-op'tan önce çoğunluğa ulaşır. Liderin bu pencerede devrilmesi için
    /// hatalar sıktır (3..12 tick arayla), çökmelerin çoğu lideri hedefler ve ağ kayıpsızdır;
    /// istemciler sık yazar ki çoğaltılacak girdi bulunsun. Varsayılan taramada no-op bu pencereyi
    /// hızla kapatır; bu profil, önceki term girdilerini kopya sayarak commit eden bir hatayı
    /// yakalanabilir kılar.
    #[must_use]
    pub fn figure8() -> Self {
        Self {
            nodes: 5,
            network: NetworkConfig {
                drop_prob: 0.0,
                duplicate_prob: 0.0,
                min_delay: 1,
                max_delay: 3,
                tail_prob: 0.0,
                tail_delay: 0,
            },
            disk: DiskConfig::default(),
            clients: ClientConfig {
                clients: 3,
                keys: 2,
                min_think: 1,
                max_think: 4,
                timeout: 40,
                max_attempts: 6,
                reply_loss_prob: 0.0,
                ops: OpMix::DEFAULT,
            },
            raft: RaftConfig::default().with_max_entries(NonZeroUsize::MIN),
            fault_gap: (3, 12),
            fault_mix: FaultMix::FIGURE8,
            horizon: 1_200,
            settle: 1_200,
            snapshot_every: None,
        }
    }

    /// Snapshot'lara ve log sıkıştırmaya (§7) odaklanan ayarlar: kaos profili, ama her düğüm
    /// durum makinesi 16 girdi ilerledikçe snapshot alır ve log'unu sıkıştırır. Çöken, bölünmede
    /// geride kalan ya da yeniden başlayan bir düğümün ihtiyaç duyduğu girdiler çoğu zaman liderin
    /// log'undan atılmıştır: düğüm liderden snapshot kurar (Figure 13) ya da kendi diskindeki
    /// snapshot'tan açılır.
    #[must_use]
    pub fn snapshots() -> Self {
        Self {
            snapshot_every: NonZeroU64::new(16),
            ..Self::chaos()
        }
    }

    /// Log'a yazılmayan okumalara (ReadIndex, tezin §6.4'ü) odaklanan ayarlar: kaos profilinin
    /// diski ve istemcileri; okumaların çoğu log'a yazılmadan cevaplanır, lider sık devrilir ya da
    /// azınlıkta yalıtılır (`FaultMix::READS`) ve ağın mesajlarının %1'i 120 tick'e kadar, çoğu
    /// seçim zaman aşımlarını aşan bir gecikme alır. Okuma yolunun tuzakları şunlardır: azınlıkta
    /// kalıp kendini hâlâ lider sanan eski bir lider, commitIndex'i geride kalmış yeni bir lider ve
    /// bir seçimi atlatıp yeni bir liderliğe varan bayat bir mesaj. Bu profil üçünü de üretebilir;
    /// sonuncusunun tetiklediği bazı sıralamalar yine de nadir kalır (bkz. docs/mutation-table.md).
    #[must_use]
    pub fn reads() -> Self {
        let chaos = Self::chaos();
        Self {
            network: NetworkConfig {
                tail_prob: 0.01,
                tail_delay: 120,
                ..chaos.network
            },
            clients: ClientConfig {
                ops: OpMix {
                    get: 2,
                    put: 5,
                    append: 5,
                    delete: 2,
                    read_index: 8,
                },
                ..chaos.clients
            },
            fault_mix: FaultMix::READS,
            ..chaos
        }
    }

    /// Lider kiralamasına (tezin §6.4.1) odaklanan ayarlar: `reads` profili, ama lider okumaları
    /// kiralaması sürdükçe doğrulama turu beklemeden cevaplar (T = 20, kiralama 16 tick; takipçiler
    /// liderden haber aldıktan sonraki 20 tick boyunca oy vermez). Saatler aynı hızla akar:
    /// kiralamanın varsayımı (saatler bir kiralama süresinde 4 tick'ten fazla ayrışmaz) burada hep
    /// sağlanır. Varsayım bozulunca (saat sıçraması, `RaftCluster::jump_clock`) kiralamanın
    /// gerçekten bayat okuma verdiğini ayrı bir test gösterir.
    #[must_use]
    pub fn leases() -> Self {
        let reads = Self::reads();
        Self {
            // `expect` imkânsız bir durumu belgeler: 16 < T = 20 sabittir (`Config::with_lease`).
            raft: reads
                .raft
                .with_lease(16)
                .expect("a 16-tick lease is below the 20-tick election timeout"),
            ..reads
        }
    }
}

/// Hata karışımı: her adımda bir hata türünün seçilme ağırlığı.
///
/// Tür, ağırlıkların toplamı üzerinden TEK bir çekilişle seçilir; aralıklar sabit sırayla dizilir
/// (çökme, lideri çökertme, yeniden başlatma, bölünme, iyileşme, kayıp oranı, sessizlik, lideri
/// yalıtma). Ağırlığı
/// 0 olan bir tür karışımda hiç yokmuş gibi davranır: toplam ve aralıklar, o tür listede olmasaydı
/// ne olacaksa odur. Bu yüzden yeni bir tür, ağırlığı 0 olan karışımların ürettiği programları
/// değiştirmeden eklenebilir (`CHAOS`'ta lideri çökertme böyledir). Sıfır OLMAYAN bir ağırlığı
/// değiştirmek ise toplamı ve sonraki aralıkları kaydırır: o karışımın programları değişir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultMix {
    /// Rastgele bir ayaktaki düğümü çökertme.
    pub crash: u64,
    /// Lideri çökertme (Jepsen'deki "kill primary" gibi).
    pub crash_leader: u64,
    /// Çökmüş bir düğümü yeniden başlatma.
    pub restart: u64,
    /// Ağı ikiye bölme.
    pub partition: u64,
    /// Bölünmeyi kaldırma.
    pub heal: u64,
    /// Kayıp oranını değiştirme.
    pub loss: u64,
    /// Hata yok (yalnızca zaman geçer).
    pub quiet: u64,
    /// Lideri tek başına azınlığa ayırma. Kendini hâlâ lider sanan ama çoğunluğa ulaşamayan bir
    /// "eski lider" üretir: ReadIndex'in (tezin §6.4'ü) korumak istediği durum budur.
    pub isolate_leader: u64,
}

impl FaultMix {
    /// Kaos taramasının karışımı: %25 çökme, %25 yeniden başlatma, %17 bölünme, %8 iyileşme, %8
    /// kayıp oranı değişimi, %17 sessizlik.
    pub const CHAOS: Self = Self {
        crash: 3,
        crash_leader: 0,
        restart: 3,
        partition: 2,
        heal: 1,
        loss: 1,
        quiet: 2,
        isolate_leader: 0,
    };

    /// ReadIndex profilinin karışımı: kaos karışımı, artı lideri çökertme ve lideri yalıtma.
    pub const READS: Self = Self {
        crash: 3,
        crash_leader: 2,
        restart: 3,
        partition: 2,
        heal: 1,
        loss: 1,
        quiet: 2,
        isolate_leader: 2,
    };

    /// Figure 8 profilinin karışımı: liderler sık devrilir ve düğümler hızla geri döner.
    pub const FIGURE8: Self = Self {
        crash: 1,
        crash_leader: 3,
        restart: 4,
        partition: 2,
        heal: 1,
        loss: 0,
        quiet: 1,
        isolate_leader: 0,
    };

    /// Ağırlıklar, sabit sırayla.
    fn weights(&self) -> [u64; 8] {
        [
            self.crash,
            self.crash_leader,
            self.restart,
            self.partition,
            self.heal,
            self.loss,
            self.quiet,
            self.isolate_leader,
        ]
    }
}

/// "Sessizlik"in `FaultMix::weights` içindeki yeri: ağırlıkların hepsi sıfırsa seçilen tür.
const QUIET: usize = 6;

/// Bir hata niyeti. Hedef, hatanın yürütüldüğü andaki duruma göre seçilir (bkz. modül belgesi).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Ayaktaki düğümlerden `pick mod sayı`'ncısını (kimlik sırasıyla) çökert; ayakta düğüm yoksa
    /// etkisizdir.
    Crash {
        /// Seçim sayısı.
        pick: u64,
    },
    /// Ayaktaki liderlerden term'i en yüksek olanı çökert (eşitlikte küçük kimlik); lider yoksa
    /// etkisizdir.
    CrashLeader,
    /// Çökmüş düğümlerden `pick mod sayı`'ncısını yeniden başlat; çökmüş düğüm yoksa etkisizdir.
    Restart {
        /// Seçim sayısı.
        pick: u64,
    },
    /// Ağı ikiye böl: `k`. düğüm (1'den), `mask`'in `k - 1`. biti 1 ise ilk gruptadır. Maske bütün
    /// düğümleri tek gruba koyabilir; o zaman bölünme etkisizdir.
    ///
    /// İstemciler bölünme sırasında azınlıkta kalmış eski bir lidere de istek verebilir: onun
    /// girdileri commit edilemez, iyileşince ezilir (§5.3) ve istemci zaman aşımında yeniden dener.
    Partition {
        /// Grup maskesi.
        mask: u64,
    },
    /// Bölünmeyi kaldır.
    Heal,
    /// Ağın kayıp olasılığını binde `permille` yap (çoğaltma ve gecikme aynı kalır).
    Loss {
        /// Binde kayıp olasılığı.
        permille: u16,
    },
    /// Ayaktaki liderlerden term'i en yüksek olanı (eşitlikte küçük kimlik) tek başına bir gruba,
    /// diğer bütün düğümleri öbür gruba ayır; lider yoksa etkisizdir.
    IsolateLeader,
}

/// Zamanı belli bir hata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledFault {
    /// Hatanın enjekte edildiği an (tick).
    pub at: u64,
    /// Hata.
    pub fault: Fault,
}

/// Bir kaos senaryosu: ana seed (ağ, disk, düğümler ve istemciler kendi alt-seed akışlarını ondan
/// türetir), ayarlar ve açık hata listesi.
#[derive(Debug, Clone, PartialEq)]
pub struct Scenario {
    /// Ana seed.
    pub seed: u64,
    /// Ayarlar.
    pub config: ScenarioConfig,
    /// Hatalar, zaman sırasıyla.
    pub faults: Vec<ScheduledFault>,
}

impl Scenario {
    /// `seed`'in senaryosu: hata programı `Component::Scenario` akışından üretilir. Hatalar
    /// arasında `fault_gap` kadar tick vardır; her adımın türü `fault_mix` ağırlıklarıyla seçilir
    /// (kayıp oranı binde 0, 50 ya da 200 olur).
    #[must_use]
    pub fn generate(seed: u64, config: ScenarioConfig) -> Self {
        let mut rng = SeedTree::new(seed).rng_for(Component::Scenario);
        let mut faults = Vec::new();
        let mut at: u64 = 0;
        loop {
            // Aralık en az 1 tick: `fault_gap` (0, 0) olsaydı `at` hiç ilerlemez ve döngü bellek
            // bitene kadar hata eklerdi. Geçersiz bir aralık `run`'da `validate` ile reddedilir;
            // burada yalnızca üretimin her durumda sonlanması güvenceye alınır. Zaman ekseninin
            // sonunda (u64::MAX) da durulur: orada `at` artık ilerleyemez.
            let gap = uniform_inclusive(&mut rng, config.fault_gap.0, config.fault_gap.1).max(1);
            at = at.saturating_add(gap);
            if at > config.horizon || at == u64::MAX {
                break;
            }
            let weights = config.fault_mix.weights();
            let total = weights
                .iter()
                .fold(0_u64, |sum, &weight| sum.saturating_add(weight));
            let mut draw = uniform_inclusive(&mut rng, 0, total.saturating_sub(1));
            // Çekilişin düştüğü aralık; ağırlıkların hepsi sıfırsa "sessizlik".
            let mut kind = QUIET;
            for (index, &weight) in weights.iter().enumerate() {
                if draw < weight {
                    kind = index;
                    break;
                }
                draw -= weight;
            }
            let fault = match kind {
                0 => Fault::Crash {
                    pick: uniform_inclusive(&mut rng, 0, u64::MAX),
                },
                1 => Fault::CrashLeader,
                2 => Fault::Restart {
                    pick: uniform_inclusive(&mut rng, 0, u64::MAX),
                },
                3 => Fault::Partition {
                    mask: uniform_inclusive(&mut rng, 0, u64::MAX),
                },
                4 => Fault::Heal,
                5 => Fault::Loss {
                    permille: match uniform_inclusive(&mut rng, 0, 2) {
                        0 => 0,
                        1 => 50,
                        _ => 200,
                    },
                },
                7 => Fault::IsolateLeader,
                // `QUIET`: hata yok, yalnızca zaman geçer.
                _ => continue,
            };
            faults.push(ScheduledFault { at, fault });
        }
        Self {
            seed,
            config,
            faults,
        }
    }

    /// Yalnızca verilen sıralardaki hataları tutan senaryo (küçültme ve `replay --faults`).
    /// Geçersiz sıralar yok sayılır; sıra korunur.
    #[must_use]
    pub fn keep_faults(&self, indices: &[usize]) -> Self {
        let mut keep: Vec<usize> = indices.to_vec();
        keep.sort_unstable();
        keep.dedup();
        Self {
            faults: keep
                .into_iter()
                .filter_map(|index| self.faults.get(index).copied())
                .collect(),
            ..self.clone()
        }
    }

    /// Hata süresi `horizon` olan senaryo: daha sonraki hatalar düşer.
    #[must_use]
    pub fn with_horizon(&self, horizon: u64) -> Self {
        let mut shorter = self.clone();
        shorter.config.horizon = horizon;
        shorter.faults.retain(|fault| fault.at <= horizon);
        shorter
    }
}

/// Bir koşunun sayaçları: bir taramanın gerçekten bir şeyleri sınadığını göstermek için.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunStats {
    /// Gerçekten çökertilen düğümler: çökertilecek düğüm (ya da lider) yokken etkisiz kalan
    /// `Crash`/`CrashLeader` hataları sayılmaz.
    pub crashes: u64,
    /// Gerçekten yeniden başlatılan düğümler: çökmüş düğüm yokken etkisiz kalan `Restart` hataları
    /// sayılmaz.
    pub restarts: u64,
    /// Uygulanan bölünmeler (lideri yalıtma dahil); maskesi bütün düğümleri tek gruba koyan
    /// (etkisiz) bölünmeler de sayılır.
    pub partitions: u64,
    /// Kayıp oranı değişimleri; oranı zaten olduğu değere "değiştiren" hatalar da sayılır.
    pub loss_changes: u64,
    /// İstemci iş yükünün sayaçları.
    pub clients: ClientStats,
    /// Kümenin sonunda commit ettiği girdi sayısı (1. düğümün commitIndex'i).
    pub committed: u64,
    /// Çökmede kaybolan (fsync'i tamamlanmamış) yazmalar.
    pub lost_writes: u64,
    /// Çökmede kısmi yazmayla yine de diske ulaşan yazmalar.
    pub kept_writes: u64,
    /// Bir lider seçilen term'ler.
    pub terms_with_a_leader: u64,
    /// Durum makinesi snapshot'ları (log sıkıştırmaları, §7).
    pub compactions: u64,
    /// Liderden kurulan snapshot'lar (Figure 13).
    pub installs: u64,
    /// Geldiği adımda kiralamayla cevaplanan okumalar (bkz. `RaftCluster::lease_reads`).
    pub lease_reads: u64,
    /// Koşunun kimliği: trace özeti.
    pub trace_hash: u64,
}

/// Bir koşunun başarısızlığı.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RunError {
    /// Senaryonun ayarları geçersiz.
    #[error("invalid scenario: {0}")]
    Setup(#[from] ConfigError),
    /// Bir olaydan sonra kümenin bir denetimi çiğnendi.
    #[error("invariant violated at t={time}: {violation}")]
    Violation {
        /// İhlalin görüldüğü an.
        time: u64,
        /// İhlal.
        violation: Violation,
    },
    /// Hata programı kümeyi yanlış kullandı (program hatası, bulunmak istenen hata değil).
    #[error("scenario error: {0}")]
    Cluster(ClusterError),
    /// İstemci geçmişi linearizable değil.
    #[error(transparent)]
    Linearizability(#[from] LinearizabilityError),
    /// Küme sakinleşmede ilerleyemedi.
    #[error("liveness: {0}")]
    Liveness(String),
    /// Koşu sırasında bir panik atıldı (mesajı). Çekirdek hiçbir girdide panik atmamayı vaat eder
    /// (N3); simülatördeki ya da denetçilerdeki bir panik de bir hatadır. Fuzzer'ın bulması
    /// gereken türden olduğu için başarısızlık olarak raporlanır ve küçültülür.
    #[error("panic: {0}")]
    Panic(String),
}

impl From<ClusterError> for RunError {
    fn from(error: ClusterError) -> Self {
        match error {
            ClusterError::Violation { time, violation } => RunError::Violation { time, violation },
            other => RunError::Cluster(other),
        }
    }
}

impl RunError {
    /// Küçültmede "aynı hata" ölçütü: hatanın türü ve, bir invariant çiğnendiyse, onun adı.
    /// Zaman ve index'ler ölçüte girmez: küçültülen senaryo aynı hatayı başka bir anda verebilir.
    #[must_use]
    pub fn signature(&self) -> String {
        match self {
            RunError::Setup(_) => "setup".to_owned(),
            RunError::Violation { violation, .. } => format!("violation: {}", violation.name()),
            RunError::Cluster(_) => "scenario".to_owned(),
            RunError::Linearizability(LinearizabilityError::Malformed { .. }) => {
                "malformed history".to_owned()
            }
            RunError::Linearizability(LinearizabilityError::NotLinearizable { .. }) => {
                "linearizability".to_owned()
            }
            RunError::Liveness(_) => "liveness".to_owned(),
            RunError::Panic(_) => "panic".to_owned(),
        }
    }
}

/// Bir koşunun sonucu ve kümenin son hâli (trace'i yazdırmak ya da incelemek için).
pub struct Run {
    /// Başarı (sayaçlar) ya da başarısızlık.
    pub outcome: Result<RunStats, RunError>,
    /// Koşunun sonundaki (ya da hatanın görüldüğü andaki) küme; ayarlar geçersizse ya da küme
    /// kurulurken bir panik atıldıysa `None`. Koşu bir panikle bittiyse küme, paniğin yarıda
    /// bıraktığı adımın hâlindedir: yalnızca incelemek (trace'ini yazdırmak) içindir, koşuya devam
    /// etmek için değil.
    pub cluster: Option<RaftCluster>,
}

// Statik `Send` denetimi: `raftsim fuzz` her seed'in koşusunu ayrı bir iş parçacığında yürütür.
// Koşunun parçalarından biri (ör. bir `Rc` alanı yüzünden) iş parçacıkları arasında taşınamaz hâle
// gelirse bu satır derlenmez; hata koşu anında değil derleme anında görülür. Kapanış hiç
// çağrılmaz, yalnızca tür denetiminden geçer.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<Scenario>();
    assert_send::<Run>();
    assert_send::<RaftCluster>();
    assert_send::<ClientDriver>();
    assert_send::<RunError>();
};

/// Senaryoyu koşturur (bkz. modül belgesi).
#[must_use]
pub fn run(scenario: &Scenario) -> Run {
    let config = scenario.config;
    if let Err(error) = config.validate() {
        return Run {
            outcome: Err(error.into()),
            cluster: None,
        };
    }
    let cluster_config = ClusterConfig {
        disk: config.disk,
        raft: config.raft,
        snapshot_every: config.snapshot_every,
        ..ClusterConfig::new(config.nodes, config.network)
    };
    // Kurulum da korunur: düğümlerin kurucusundaki bir panik de seed'li bir başarısızlıktır.
    let mut cluster = None;
    let outcome = guarded(|| {
        let built = RaftCluster::new(scenario.seed, cluster_config)?;
        drive(scenario, cluster.insert(built))
    });
    Run { outcome, cluster }
}

/// Koşuyu bir panikten korur: panik yakalanır ve [`RunError::Panic`] olur. Yakalanmasaydı
/// `raftsim fuzz` bütün süreciyle çöker, başarısız seed ve yeniden üretme komutu hiç basılmazdı;
/// küçültme de paniği "aynı hata" olarak tanıyamazdı.
///
/// `AssertUnwindSafe` neden yeterli: paniğin yarıda bıraktığı küme koşuya devam etmek için değil,
/// yalnızca incelemek (trace'ini yazdırmak) için geri verilir. Yarım kalmış bir durumun "geçerli"
/// sanılıp kullanılması riski yoktur. Panik mesajı yine de varsayılan panik kancasıyla stderr'e
/// basılır; kanca süreç genelidir ve paralel koşan diğer seed'leri de etkileyeceği için
/// değiştirilmez.
fn guarded(run: impl FnOnce() -> Result<RunStats, RunError>) -> Result<RunStats, RunError> {
    match panic::catch_unwind(AssertUnwindSafe(run)) {
        Ok(outcome) => outcome,
        Err(payload) => Err(RunError::Panic(panic_message(payload.as_ref()))),
    }
}

/// Panik yükünün metni: biçimli bir `panic!` `String`, sabit mesajlı bir `panic!` `&str` taşır.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "a panic with a non-string payload".to_owned()
    }
}

fn drive(scenario: &Scenario, cluster: &mut RaftCluster) -> Result<RunStats, RunError> {
    let config = scenario.config;
    let mut clients = ClientDriver::new(scenario.seed, config.clients)?;
    let mut stats = RunStats::default();
    for scheduled in &scenario.faults {
        clients.run_until(cluster, scheduled.at)?;
        inject(cluster, scheduled.fault, config, &mut stats)?;
    }
    clients.run_until(cluster, config.horizon)?;

    // Sakinleşme: ağ iyileşir ve ilk ayarlarına döner, herkes ayağa kalkar. Her istemci en az bir
    // işlemi daha tamamlayabilmeli: küme yeniden ilerliyor olmalı (canlılık).
    cluster.heal();
    cluster.set_network(config.network)?;
    let down: Vec<NodeId> = cluster
        .node_ids()
        .filter(|&id| !cluster.is_up(id))
        .collect();
    for id in down {
        cluster.restart(id)?;
    }
    let before = clients.completed_per_client();
    let deadline = cluster.now().saturating_add(config.settle);
    while !clients
        .completed_per_client()
        .iter()
        .zip(&before)
        .all(|(after, before)| after > before)
    {
        if cluster.now() >= deadline {
            return Err(RunError::Liveness(format!(
                "some client completed no operation within {} ticks after healing ({before:?} -> \
                 {:?})",
                config.settle,
                clients.completed_per_client()
            )));
        }
        let next = cluster.now().saturating_add(1);
        clients.run_until(cluster, next)?;
    }
    // Yeni işlem başlatılmaz; bekleyenler biter (cevap ya da vazgeçme) ve küme yakınsar.
    clients.set_issuing(false);
    let deadline = cluster.now().saturating_add(config.settle);
    while clients.busy() || !converged(cluster) {
        if cluster.now() >= deadline {
            return Err(RunError::Liveness(format!(
                "the cluster did not converge within {} ticks (clients busy: {})",
                config.settle,
                clients.busy()
            )));
        }
        let next = cluster.now().saturating_add(1);
        clients.run_until(cluster, next)?;
    }
    checker::check_kv(&clients.history())?;

    stats.clients = clients.stats();
    stats.committed = cluster
        .node(NodeId(1))
        .map_or(0, |node| node.commit_index().0);
    for event in cluster.sim().trace().events() {
        if let TraceKind::CrashLoss {
            kept_writes,
            lost_writes,
            ..
        } = event.kind
        {
            stats.kept_writes += kept_writes;
            stats.lost_writes += lost_writes;
        }
    }
    stats.terms_with_a_leader = u64::try_from(
        cluster
            .elections()
            .values()
            .filter(|election| election.leader.is_some())
            .count(),
    )
    .unwrap_or(u64::MAX);
    stats.compactions = cluster.compactions();
    stats.installs = cluster.installs();
    stats.lease_reads = cluster.lease_reads();
    stats.trace_hash = cluster.sim().trace_hash();
    Ok(stats)
}

/// Bir hatayı yürütür; etkisiz kalanlar sayılmaz.
fn inject(
    cluster: &mut RaftCluster,
    fault: Fault,
    config: ScenarioConfig,
    stats: &mut RunStats,
) -> Result<(), RunError> {
    let nodes: Vec<NodeId> = cluster.node_ids().collect();
    match fault {
        Fault::Crash { pick } => {
            let up: Vec<NodeId> = nodes
                .iter()
                .copied()
                .filter(|&id| cluster.is_up(id))
                .collect();
            if let Some(id) = choose(&up, pick) {
                cluster.crash(id)?;
                stats.crashes += 1;
            }
        }
        Fault::CrashLeader => {
            if let Some(id) = current_leader(cluster) {
                cluster.crash(id)?;
                stats.crashes += 1;
            }
        }
        Fault::IsolateLeader => {
            if let Some(leader) = current_leader(cluster) {
                let rest: Vec<NodeId> = nodes.iter().copied().filter(|&id| id != leader).collect();
                cluster.partition(&[&[leader], &rest])?;
                stats.partitions += 1;
            }
        }
        Fault::Restart { pick } => {
            let down: Vec<NodeId> = nodes
                .iter()
                .copied()
                .filter(|&id| !cluster.is_up(id))
                .collect();
            if let Some(id) = choose(&down, pick) {
                cluster.restart(id)?;
                stats.restarts += 1;
            }
        }
        Fault::Partition { mask } => {
            let (left, right): (Vec<NodeId>, Vec<NodeId>) = nodes
                .iter()
                .partition(|id| id.0 >= 1 && id.0 <= 64 && mask & (1 << (id.0 - 1)) != 0);
            cluster.partition(&[&left, &right])?;
            stats.partitions += 1;
        }
        Fault::Heal => cluster.heal(),
        Fault::Loss { permille } => {
            let network = NetworkConfig {
                drop_prob: f64::from(permille) / 1000.0,
                ..config.network
            };
            cluster.set_network(network)?;
            stats.loss_changes += 1;
        }
    }
    Ok(())
}

/// Ayaktaki liderlerden term'i en yüksek olanı; eşitlikte küçük kimlik. Bölünme sırasında
/// azınlıkta kalmış eski bir lider de kendini lider sanabilir; hedef, en yeni liderliktir.
fn current_leader(cluster: &RaftCluster) -> Option<NodeId> {
    // `leaders` kimlik sırasıyla döner; en yüksek term'i seçmek eşitlikte küçük kimliği korur
    // (`max_by_key` eşitlikte SONUNCUYU seçtiği için ters sırada gezilir).
    cluster
        .leaders()
        .into_iter()
        .rev()
        .max_by_key(|&(_, term)| term)
        .map(|(id, _)| id)
}

/// `items` içinden `pick mod uzunluk`'uncusu; liste boşsa `None`.
fn choose(items: &[NodeId], pick: u64) -> Option<NodeId> {
    let len = u64::try_from(items.len()).ok()?;
    let index = usize::try_from(pick.checked_rem(len)?).ok()?;
    items.get(index).copied()
}

/// Ayaktaki bütün düğümler aynı commitIndex'te, hepsi uygulanmış ve KV tabloları aynı mı?
fn converged(cluster: &RaftCluster) -> bool {
    let live: Vec<NodeId> = cluster.node_ids().filter(|&id| cluster.is_up(id)).collect();
    let Some(&first) = live.first() else {
        return false;
    };
    let commit = |id| cluster.node(id).map(|node| node.commit_index());
    live.iter().all(|&id| {
        commit(id) == commit(first)
            && cluster.node(id).map(|node| node.last_applied()) == commit(first)
            && cluster.kv(id) == cluster.kv(first)
    })
}

#[cfg(test)]
mod tests {
    use super::{Fault, RunError, RunStats, Scenario, ScenarioConfig, choose, guarded, run};
    use crate::error::ConfigError;
    use raft_core::NodeId;

    // Üretim deterministiktir ve seed'e bağlıdır; hatalar zaman sırasıyla `(0, horizon]` içindedir
    // ve aralarında 5..40 tick vardır. Bir seed taramasında her hata türü görülür.
    #[test]
    fn generation_is_deterministic_and_well_formed() {
        let config = ScenarioConfig::chaos();
        assert_eq!(Scenario::generate(3, config), Scenario::generate(3, config));
        assert_ne!(
            Scenario::generate(3, config).faults,
            Scenario::generate(4, config).faults
        );
        let mut kinds = [false; 7];
        for seed in 0..20 {
            let scenario = Scenario::generate(seed, config);
            let mut previous = 0;
            for scheduled in &scenario.faults {
                assert!(scheduled.at > previous && scheduled.at <= config.horizon);
                assert!(scheduled.at - previous >= 5);
                previous = scheduled.at;
                let kind = match scheduled.fault {
                    Fault::Crash { .. } => 0,
                    Fault::Restart { .. } => 1,
                    Fault::Partition { .. } => 2,
                    Fault::Heal => 3,
                    Fault::Loss { permille } => {
                        assert!([0, 50, 200].contains(&permille));
                        4
                    }
                    Fault::CrashLeader => 5,
                    Fault::IsolateLeader => 6,
                };
                kinds[kind] = true;
            }
        }
        // Kaos karışımında lideri çökertme ve lideri yalıtma kapalıdır (ağırlıkları 0).
        assert_eq!(kinds, [true, true, true, true, true, false, false]);
        let reads = Scenario::generate(1, ScenarioConfig::reads());
        assert!(
            reads
                .faults
                .iter()
                .any(|scheduled| scheduled.fault == Fault::IsolateLeader)
        );
        let figure8 = Scenario::generate(1, ScenarioConfig::figure8());
        assert!(
            figure8
                .faults
                .iter()
                .any(|scheduled| scheduled.fault == Fault::CrashLeader)
        );
    }

    // Küçültmenin yapı taşları: hata alt kümesi sırayı korur ve geçersiz sıraları yok sayar; hata
    // süresini kısaltmak sonraki hataları düşürür.
    #[test]
    fn faults_can_be_kept_by_index_and_cut_by_horizon() {
        let scenario = Scenario::generate(5, ScenarioConfig::chaos());
        let kept = scenario.keep_faults(&[3, 1, 3, 10_000]);
        assert_eq!(kept.faults, vec![scenario.faults[1], scenario.faults[3]]);
        let cut_at = scenario.faults[2].at;
        let shorter = scenario.with_horizon(cut_at);
        assert_eq!(shorter.config.horizon, cut_at);
        assert_eq!(shorter.faults, scenario.faults[..3].to_vec());
    }

    // Niyetin hedefi, yürütme anındaki listeden mod alınarak seçilir; boş listede hedef yoktur.
    #[test]
    fn a_pick_chooses_modulo_the_current_candidates() {
        let nodes = [NodeId(2), NodeId(4), NodeId(5)];
        assert_eq!(choose(&nodes, 0), Some(NodeId(2)));
        assert_eq!(choose(&nodes, 4), Some(NodeId(4)));
        assert_eq!(choose(&nodes, u64::MAX), Some(NodeId(2)));
        assert_eq!(choose(&[], 7), None);
    }

    // Geçersiz bir hata aralığı koşudan önce reddedilir ve üretim yine de sonlanır: (0, 0)
    // aralığıyla `at` hiç ilerlemeseydi üretim bellek bitene kadar sürerdi.
    #[test]
    fn invalid_fault_gaps_are_rejected_and_generation_terminates() {
        assert_eq!(ScenarioConfig::chaos().validate(), Ok(()));
        assert_eq!(ScenarioConfig::figure8().validate(), Ok(()));
        for (gap, horizon) in [((0, 0), 50), ((7, 3), 50), ((0, 0), u64::MAX - 1)] {
            let mut config = ScenarioConfig::chaos();
            config.fault_gap = gap;
            config.horizon = horizon;
            assert_eq!(
                config.validate(),
                Err(ConfigError::FaultGap {
                    min: gap.0,
                    max: gap.1
                })
            );
            if horizon < 1_000 {
                let scenario = Scenario::generate(1, config);
                assert!(scenario.faults.len() <= 50, "{}", scenario.faults.len());
                let error = run(&scenario).outcome.expect_err("an invalid fault gap");
                assert!(matches!(
                    error,
                    RunError::Setup(ConfigError::FaultGap { .. })
                ));
            }
        }
    }

    // Koşu sırasındaki bir panik, seed'li bir başarısızlığa dönüşür: süreç çökmez, mesaj korunur ve
    // imzası "panic"tir. Metin olmayan bir panik yükü de raporlanır.
    #[test]
    fn a_panic_becomes_a_failure() {
        assert_eq!(guarded(|| Ok(RunStats::default())), Ok(RunStats::default()));
        let error = guarded(|| panic!("boom {}", 7)).expect_err("the panic is caught");
        assert_eq!(error, RunError::Panic("boom 7".to_owned()));
        assert_eq!(error.signature(), "panic");
        assert_eq!(error.to_string(), "panic: boom 7");
        let error = guarded(|| std::panic::panic_any(42_u32)).expect_err("the panic is caught");
        assert_eq!(
            error,
            RunError::Panic("a panic with a non-string payload".to_owned())
        );
    }

    // Hatasız bir senaryo geçer ve iş yükü ilerler; geçersiz ayarlar koşudan önce reddedilir;
    // sakinleşmeye hiç süre tanınmazsa koşu canlılık hatasıyla biter. Hataların "imzası" türünü
    // söyler.
    #[test]
    fn runs_report_success_setup_and_liveness_failures() {
        let quiet = Scenario::generate(1, ScenarioConfig::chaos()).keep_faults(&[]);
        let stats = run(&quiet).outcome.expect("a fault-free run passes");
        assert!(stats.clients.completed > 0);
        assert_eq!(stats.crashes + stats.restarts + stats.partitions, 0);

        let mut invalid = quiet.clone();
        invalid.config.nodes = 3;
        invalid.config.network.min_delay = 0;
        let error = run(&invalid).outcome.expect_err("an invalid network");
        assert!(matches!(error, RunError::Setup(_)));
        assert_eq!(error.signature(), "setup");

        let mut impatient = quiet;
        impatient.config.settle = 0;
        let error = run(&impatient).outcome.expect_err("no time to settle");
        assert!(matches!(error, RunError::Liveness(_)));
        assert_eq!(error.signature(), "liveness");
    }
}
