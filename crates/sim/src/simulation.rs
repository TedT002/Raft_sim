//! `Simulation`: düğümleri, ağı, diski, sanal saati ve olay kuyruğunu birleştiren deterministik
//! sürücü.
//!
//! Saat simülatörün kendisindedir (`now`); ayrı bir `Clock` trait'i yoktur. Gerçek zamanla çalışan
//! bir sürücü ancak Faz 6'da gerekir. Zaman yalnızca kuyruktaki bir sonraki olayın zamanına
//! atlayarak ilerler (discrete-event simulation): arada "boşta geçen" gerçek süre yoktur, bu yüzden
//! saatlerce süren bir senaryo milisaniyeler içinde ve her seferinde aynı sırayla koşar.
//!
//! Her düğüm simüle bir makinede yaşar: süreç (düğümün kendisi), diski ve yaşam durumu. Makine
//! çökebilir ve diskindeki durumla yeniden başlatılabilir (`crash`/`restart`). Disk yazmaları
//! `fsync` tamamlanana kadar bekler (bkz. `disk` modülü).

use std::collections::{BTreeMap, VecDeque};

use raft_core::NodeId;

use crate::disk::SimDisk;
use crate::error::{ConfigError, LifecycleError, PartitionError};
use crate::network::{Fate, Network};
use crate::node::{DurableState, NodeInput, NodeOutput, OutputOf, SimNode, UpdateOf};
use crate::queue::EventQueue;
use crate::rng::{ChaCha8Rng, uniform_inclusive};
use crate::trace::{DropReason, Trace, TraceEvent, TraceKind, digest};

/// Simülasyonun zamanlama ayarları.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimConfig {
    /// Her düğüm kaç tick'te bir `Tick` alır (≥ 1). İlk tick `tick_every` anındadır.
    pub tick_every: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self { tick_every: 1 }
    }
}

/// Simülasyonun isteğe bağlı parçaları: disk modeli ve aynı anlı tick'lerin sırası.
#[derive(Debug, Clone)]
pub struct SimOptions {
    /// Simüle disk. Varsayılan gecikmesiz disktir: yazmalar hemen kalıcıdır.
    pub disk: SimDisk,
    /// Aynı anda tick alan düğümlerin sırası. Varsayılan `NodeId` sırasıdır.
    pub tick_order: TickOrder,
}

impl Default for SimOptions {
    fn default() -> Self {
        Self {
            disk: SimDisk::instant(),
            tick_order: TickOrder::ById,
        }
    }
}

/// Aynı anda tick alan düğümlerin işlenme sırası.
///
/// Sıra koşunun başında bir kez belirlenir ve sonra kendiliğinden korunur: her tick, bir sonraki
/// tick'ini kendi işlenişi sırasında kuyruğa koyar, yani bir sonraki anda da aynı göreli sırayla
/// (`seq`) işlenir. Yeniden başlatılan bir düğümün zinciri yeniden kurulduğu için o düğüm sıranın
/// sonuna geçer.
#[derive(Debug, Clone)]
pub enum TickOrder {
    /// `NodeId` sırası. Aynı anda zaman aşımına uğrayan iki düğümden küçük kimlikli olan hep önce
    /// davranır; sabit gecikmeli bir ağda bu sistematik bir yanlılıktır.
    ById,
    /// Verilen RNG ile bir kez karıştırılmış sıra (Fisher-Yates, konum başına tek çekiliş). Hangi
    /// düğümün önce davranacağı seed'e bağlıdır; böylece farklı seed'ler farklı sıralamaları dener.
    /// RNG kutudadır: büyük (yüzlerce bayt) bir durum taşır ve bu seçenek yalnızca kurulumda bir
    /// kez kullanılır.
    Shuffled(Box<ChaCha8Rng>),
}

/// Kuyruktaki olaylar.
#[derive(Debug, Clone)]
enum Event<M> {
    /// Bir düğümün periyodik tick'i. `incarnation`, tick zincirinin düğümün hangi açılışına ait
    /// olduğunu söyler (bkz. `Host::incarnation`).
    Tick { node: NodeId, incarnation: u64 },
    /// Yoldaki bir mesajın (ya da kopyasının) teslim anı.
    Deliver(Delivery<M>),
    /// Bir düğümün en eski bekleyen yazmasının `fsync`'inin tamamlanma anı. Önceki bir açılışa ait
    /// olanlar yok sayılır: o yazmalar çökmede zaten karara bağlandı.
    Sync { node: NodeId, incarnation: u64 },
}

/// Yoldaki bir mesaj (ya da kopyası). Özet gönderimde bir kez hesaplanır; `sent_epoch`, teslimde
/// "uçuş boyunca bağlı mıydı?" sorusunun başlangıç noktasıdır.
#[derive(Debug, Clone)]
struct Delivery<M> {
    msg_id: u64,
    from: NodeId,
    to: NodeId,
    msg: M,
    digest: u64,
    sent_at: u64,
    sent_epoch: u64,
}

/// Bir yazmanın kalıcı olmasını beklerken tutulan çıktılar.
enum Release<N: SimNode> {
    Send { to: NodeId, msg: N::Msg },
    Apply(N::Applied),
}

/// Tutulan bir çıktı ve bırakılabilmesi için kalıcı olması gereken yazma sayısı.
struct Held<N: SimNode> {
    after_writes: u64,
    output: Release<N>,
}

/// Simüle bir makine: düğüm süreci, diski ve yaşam durumu.
struct Host<N: SimNode> {
    node: N,
    /// Diskteki KALICI durum: yalnızca fsync'i tamamlanmış yazmaları içerir ve çökmeden sağ çıkan
    /// tek şeydir.
    durable: N::Durable,
    /// Düğümün diske yazdırdığı en son durum: kalıcı durum ve bütün bekleyen yazmalar. Dayanıklılık
    /// denetimi düğümün belleğini bununla karşılaştırır ("her değişiklik için bir yazma var mı?").
    latest: N::Durable,
    /// Verilmiş ama fsync'i tamamlanmamış yazmalar, verilme sırasıyla.
    pending: VecDeque<UpdateOf<N>>,
    /// Bir yazmanın kalıcı olmasını bekleyen çıktılar, üretilme sırasıyla.
    held: VecDeque<Held<N>>,
    /// Bu açılışta verilen yazma sayısı ve bunlardan fsync'i tamamlananların sayısı.
    writes_issued: u64,
    writes_synced: u64,
    /// Son fsync'in tamamlanacağı an: fsync'ler yazma sırasıyla tamamlanır.
    last_sync_at: u64,
    up: bool,
    /// Kaçıncı açılış (0 = ilk). Her yeniden başlatmada artar ve yeni tick zinciri bu numarayla
    /// kurulur. Kuyrukta önceki bir açılıştan kalmış tick bulunursa numarası tutmadığı için yok
    /// sayılır. Aksi hâlde çöküp hemen kalkan bir düğümde eski ve yeni zincir üst üste biner ("çift
    /// tick") ve düğümün saati iki kat hızlı akardı.
    incarnation: u64,
}

/// Bir düğümün simülatördeki anlık görünümü: süreci, diski ve ayakta olup olmadığı.
#[derive(Debug)]
pub struct HostView<'a, N: SimNode> {
    /// Düğümün kimliği.
    pub id: NodeId,
    /// Düğüm süreci. Çökmüş bir düğümün bellek durumu anlamsızdır (yeniden başlatmada diskten
    /// yeniden kurulur); invariant denetimleri yalnızca ayaktaki düğümlere bakmalıdır.
    pub node: &'a N,
    /// Diskteki kalıcı (fsync edilmiş) durum.
    pub disk: &'a N::Durable,
    /// Düğümün diske yazdırdığı en son durum (bekleyen yazmalar dahil).
    pub latest: &'a N::Durable,
    /// Düğüm ayakta mı?
    pub up: bool,
}

/// Deterministik simülasyon: aynı düğümler, aynı ağ ve disk (aynı seed'li RNG'ler) ve aynı çağrılar
/// her seferinde birebir aynı trace'i üretir.
///
/// Ayakta düğüm olduğu sürece kuyruk asla boşalmaz: her tick bir sonrakini kurar. Simülasyonu her
/// zaman sonlu bir ufukla (`run_until(t)`) sürün; "hepsini boşalt" niyetiyle `while sim.step() {}`
/// ya da `run_until(u64::MAX)` pratikte bitmez.
pub struct Simulation<N: SimNode, Net: Network> {
    config: SimConfig,
    now: u64,
    queue: EventQueue<Event<N::Msg>>,
    // BTreeMap: düğümler üzerinde gezinme sırası (ilk tick'lerin sıralanması, invariant
    // denetimleri) her koşuda aynı olsun.
    hosts: BTreeMap<NodeId, Host<N>>,
    network: Net,
    disk: SimDisk,
    trace: Trace,
    next_msg_id: u64,
    // Verilen yazmalar, kalıcı olan yazmalar, bırakılan yerel etkiler ve sırası bozuk adımlar:
    // sürücü (ör. `RaftCluster`) her olaydan sonra bunları alıp denetler. Alınmayanlar birikir.
    writes: Vec<(NodeId, UpdateOf<N>)>,
    synced: Vec<(NodeId, UpdateOf<N>)>,
    applied: Vec<(NodeId, N::Applied)>,
    persists_after_output: Vec<NodeId>,
}

impl<N: SimNode, Net: Network> Simulation<N, Net> {
    /// Simülasyonu gecikmesiz bir diskle ve `NodeId` tick sırasıyla kurar (bkz.
    /// [`Simulation::with_options`]).
    ///
    /// # Errors
    ///
    /// `tick_every` sıfırsa ya da bir düğüm kimliği birden fazla kez verilmişse [`ConfigError`].
    pub fn new(
        config: SimConfig,
        network: Net,
        nodes: impl IntoIterator<Item = (NodeId, N)>,
    ) -> Result<Self, ConfigError> {
        Self::with_options(config, network, nodes, SimOptions::default())
    }

    /// Simülasyonu kurar ve her düğümün ilk tick'ini `tick_every` anına, seçilen sırayla koyar. Her
    /// düğüm boş bir diskle (`N::Durable::default()`) ve ayakta başlar.
    ///
    /// # Errors
    ///
    /// `tick_every` sıfırsa ya da bir düğüm kimliği birden fazla kez verilmişse [`ConfigError`].
    pub fn with_options(
        config: SimConfig,
        network: Net,
        nodes: impl IntoIterator<Item = (NodeId, N)>,
        options: SimOptions,
    ) -> Result<Self, ConfigError> {
        if config.tick_every == 0 {
            return Err(ConfigError::ZeroTickInterval);
        }
        let mut hosts = BTreeMap::new();
        for (id, node) in nodes {
            let host = Host {
                node,
                durable: N::Durable::default(),
                latest: N::Durable::default(),
                pending: VecDeque::new(),
                held: VecDeque::new(),
                writes_issued: 0,
                writes_synced: 0,
                last_sync_at: 0,
                up: true,
                incarnation: 0,
            };
            if hosts.insert(id, host).is_some() {
                return Err(ConfigError::DuplicateNode(id));
            }
        }
        let mut order: Vec<NodeId> = hosts.keys().copied().collect();
        if let TickOrder::Shuffled(rng) = options.tick_order {
            let mut rng = *rng;
            // Fisher-Yates: sondan başa, her konum için TAM OLARAK bir çekiliş.
            for last in (1..order.len()).rev() {
                let upper = u64::try_from(last).unwrap_or(u64::MAX);
                let pick = usize::try_from(uniform_inclusive(&mut rng, 0, upper)).unwrap_or(last);
                order.swap(last, pick);
            }
        }
        let mut queue = EventQueue::new();
        for id in order {
            queue.push(
                config.tick_every,
                Event::Tick {
                    node: id,
                    incarnation: 0,
                },
            );
        }
        Ok(Self {
            config,
            now: 0,
            queue,
            hosts,
            network,
            disk: options.disk,
            trace: Trace::new(),
            next_msg_id: 0,
            writes: Vec::new(),
            synced: Vec::new(),
            applied: Vec::new(),
            persists_after_output: Vec::new(),
        })
    }

    /// Şu anki mantıksal zaman.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.now
    }

    /// Şu ana kadarki trace.
    #[must_use]
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Şu ana kadarki trace özeti: koşunun kimliği.
    #[must_use]
    pub fn trace_hash(&self) -> u64 {
        self.trace.hash()
    }

    /// Bir düğüme (salt okunur) erişim: testlerin düğüm iç durumunu incelemesi için. Çökmüş bir
    /// düğüm de döner; onun bellek durumu yeniden başlatmaya kadar donmuştur ve anlamsızdır.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&N> {
        self.hosts.get(&id).map(|host| &host.node)
    }

    /// Bir düğümün diskindeki kalıcı (fsync edilmiş) durum: çökmeden sağ çıkacak olan.
    #[must_use]
    pub fn disk(&self, id: NodeId) -> Option<&N::Durable> {
        self.hosts.get(&id).map(|host| &host.durable)
    }

    /// Bir düğümün diske yazdırdığı en son durum: kalıcı durum ve bekleyen bütün yazmalar.
    #[must_use]
    pub fn latest(&self, id: NodeId) -> Option<&N::Durable> {
        self.hosts.get(&id).map(|host| &host.latest)
    }

    /// Bir düğümün "yazdırılmış en son durumuna" yazma erişimi. YALNIZCA crate içi testler içindir:
    /// bir denetimin (ör. `RaftCluster`'ın dayanıklılık denetimi) disk ile bellek ayrıştığında
    /// gerçekten ihlal bildirdiğini göstermek için onu elle bozar. Genel API'de yoktur;
    /// simülasyonda bu durum yalnızca `Persist` çıktılarıyla değişir.
    #[cfg(test)]
    pub(crate) fn latest_mut(&mut self, id: NodeId) -> Option<&mut N::Durable> {
        self.hosts.get_mut(&id).map(|host| &mut host.latest)
    }

    /// Çıktı sırası kaydına elle bir düğüm ekler. YALNIZCA crate içi testler içindir: sürücünün
    /// (`RaftCluster`) kaydı gerçekten ihlal olarak bildirdiğini, ters sıra üreten bir çekirdek
    /// olmadan göstermek için.
    #[cfg(test)]
    pub(crate) fn record_persist_after_output(&mut self, id: NodeId) {
        self.persists_after_output.push(id);
    }

    /// Düğüm ayakta mı? Simülasyonda olmayan bir düğüm için `false`.
    #[must_use]
    pub fn is_up(&self, id: NodeId) -> bool {
        self.hosts.get(&id).is_some_and(|host| host.up)
    }

    /// Bütün düğümler (çökmüşler dahil), `NodeId` sırasıyla: her adımdan sonra invariant
    /// denetleyen sürücüler için.
    pub fn hosts(&self) -> impl Iterator<Item = HostView<'_, N>> {
        self.hosts.iter().map(|(&id, host)| HostView {
            id,
            node: &host.node,
            disk: &host.durable,
            latest: &host.latest,
            up: host.up,
        })
    }

    /// Ağ modeline (salt okunur) erişim.
    #[must_use]
    pub fn network(&self) -> &Net {
        &self.network
    }

    /// Son çağrıdan bu yana verilen yazmalar (düğüm ve fark), verilme sırasıyla.
    pub fn take_writes(&mut self) -> Vec<(NodeId, UpdateOf<N>)> {
        std::mem::take(&mut self.writes)
    }

    /// Son çağrıdan bu yana KALICI olan yazmalar (düğüm ve fark), kalıcı olma sırasıyla: fsync'i
    /// tamamlananlar, gecikmesiz diskin yazmaları ve bir çökmede diske ulaşmış sayılan önek. Bir
    /// düğümün kalıcı durumu ([`Simulation::disk`]) yalnızca bunlarla değişir.
    pub fn take_synced(&mut self) -> Vec<(NodeId, UpdateOf<N>)> {
        std::mem::take(&mut self.synced)
    }

    /// Son çağrıdan bu yana bırakılan yerel etkiler (düğüm ve etki), bırakılma sırasıyla.
    pub fn take_applied(&mut self) -> Vec<(NodeId, N::Applied)> {
        std::mem::take(&mut self.applied)
    }

    /// Son çağrıdan bu yana, aynı adımın bir `Send` ya da `Apply` çıktısından SONRA gelen her
    /// `Persist` için onu üreten düğüm, üretilme sırasıyla (böyle iki `Persist` üreten bir adım iki
    /// kayıt bırakır).
    ///
    /// Simülatör çıktıları verildikleri sırayla uygular ve böyle bir `Persist`, kendisinden önceki
    /// çıktıları tutamaz: onlar durum kalıcı olmadan dışarı çıkar. Genel bir düğüm için bu bilinçli
    /// bir seçim olabilir; simülatör yargılamaz, kaydeder. Raft için ise Figure 2'nin "cevap
    /// vermeden önce kalıcı depoya yaz" kuralının ihlalidir ve `RaftCluster` onu öyle bildirir.
    pub fn take_persists_after_output(&mut self) -> Vec<NodeId> {
        std::mem::take(&mut self.persists_after_output)
    }

    /// Kuyruktaki en erken olayı işler. Kuyruk boşsa `false` döner; ayakta düğüm olduğu sürece
    /// kuyruk boşalmadığından bunu bir "bitti" sinyali olarak kullanmayın (bkz. tip dokümanı).
    pub fn step(&mut self) -> bool {
        let Some(scheduled) = self.queue.pop() else {
            return false;
        };
        // Kuyruk en erken olayı verir ve yeni olaylar hiçbir zaman "şimdi"den önceye konmaz (tick
        // aralığı ≥ 1, ağ gecikmesi `NonZeroU64`, fsync gecikmesi ≥ 0); bu yüzden zaman asla geri
        // gitmez.
        self.now = scheduled.time;
        match scheduled.event {
            Event::Tick { node, incarnation } => self.handle_tick(node, incarnation),
            Event::Deliver(delivery) => self.handle_delivery(delivery),
            Event::Sync { node, incarnation } => self.handle_sync(node, incarnation),
        }
        true
    }

    /// `time` anına kadar (o dahil) bekleyen bir olay varsa onu işler ve `true` döner. Yoksa saati
    /// `time`'a getirir (saat zaten ilerideyse olduğu yerde kalır; zaman geri gitmez) ve `false`
    /// döner. `run_until` bunun döngüsüdür; olaylar arasında invariant denetlemek isteyen
    /// sürücüler (ör. `RaftCluster`) doğrudan bunu kullanır.
    pub fn step_until(&mut self, time: u64) -> bool {
        if self.queue.peek_time().is_some_and(|next| next <= time) {
            self.step()
        } else {
            self.now = self.now.max(time);
            false
        }
    }

    /// Zamanı `time` anına kadar ilerletir: zamanı `time` veya daha erken olan bütün olayları
    /// işler, sonra saati `time`'a getirir (sonraki olay daha geç olsa bile; saat zaten ilerideyse
    /// olduğu yerde kalır).
    ///
    /// Aynı zaman damgasında sonsuz döngü olamaz: tick ve teslim yalnızca kendisinden SONRAKİ bir
    /// zamana yeni olay koyar; aynı ana konabilen tek olay türü fsync'tir ve o da yalnızca bir
    /// yazmanın sonucudur. Zaman ekseninin sonunda (`u64::MAX`) tick'ler durur ve sonu aşan
    /// teslimler düşer.
    pub fn run_until(&mut self, time: u64) {
        while self.step_until(time) {}
    }

    /// Ağı gruplara böler ve bunu trace'e kaydeder. Anlamı için bkz. [`Network::partition`].
    ///
    /// # Errors
    ///
    /// Bir düğüm simülasyonda yoksa ya da tanımda birden fazla kez geçiyorsa [`PartitionError`];
    /// o durumda ne ağ ne trace değişir.
    pub fn partition(&mut self, groups: &[&[NodeId]]) -> Result<(), PartitionError> {
        if let Some(&unknown) = groups
            .iter()
            .flat_map(|group| group.iter())
            .find(|id| !self.hosts.contains_key(id))
        {
            return Err(PartitionError::UnknownNode(unknown));
        }
        self.network.partition(groups)?;
        // Kanonik kayıt: her grup sıralı, gruplar sıralı. Aynı anlama gelen iki bölünme (grupların
        // ya da düğümlerin yazılış sırası farklı) aynı trace özetini versin.
        let mut canonical: Vec<Vec<NodeId>> = groups
            .iter()
            .map(|group| {
                let mut sorted = group.to_vec();
                sorted.sort_unstable();
                sorted
            })
            .collect();
        canonical.sort_unstable();
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Partition { groups: canonical },
        });
        Ok(())
    }

    /// Bölünmeyi kaldırır ve bunu trace'e kaydeder.
    pub fn heal(&mut self) {
        self.network.heal();
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Heal,
        });
    }

    /// Bir düğümü çökertir. Düğüm artık tick almaz ve ona gelen mesajlar teslim anında düşer
    /// ([`DropReason::NodeDown`]). Kalıcı diski korunur: `restart` düğümü diskteki durumla geri
    /// getirir.
    ///
    /// Çökme, fsync bekleyen yazmaları ve onları bekleyen çıktıları kaybettirir; disk ayarına göre
    /// bekleyen yazmaların bir öneki yine de diske ulaşmış sayılabilir (bkz. `disk` modülü).
    /// Düğümün çökmeden önce gönderdiği ve hâlâ yolda olan mesajlar ağdadır, yine teslim edilir:
    /// kablodaki paket, gönderen makine kapansa da yoluna devam eder. Düğüm nesnesi bellekte kalır
    /// ama hiçbir girdi almaz; yeniden başlatmada `Restart` sözleşmesi gereği bütün bellek durumunu
    /// diskten yeniden kurmalıdır.
    ///
    /// # Errors
    ///
    /// Düğüm simülasyonda yoksa ya da zaten çökmüşse [`LifecycleError`]; o durumda simülasyon
    /// değişmez.
    pub fn crash(&mut self, id: NodeId) -> Result<(), LifecycleError> {
        let host = self
            .hosts
            .get_mut(&id)
            .ok_or(LifecycleError::UnknownNode(id))?;
        if !host.up {
            return Err(LifecycleError::AlreadyDown(id));
        }
        host.up = false;
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Crash { node: id },
        });
        // Bekleyen yazmalardan diske ulaşmış sayılan önek kalıcı duruma eklenir, gerisi kaybolur.
        // Disk her çökmede aynı sayıda çekiliş yapar (bekleyen yazma olmasa bile).
        let pending = host.pending.len();
        let kept = self.disk.kept_on_crash(pending);
        for update in host.pending.drain(..).take(kept) {
            host.durable.apply(&update);
            self.synced.push((id, update));
        }
        let dropped = host.held.len();
        host.held.clear();
        host.latest = host.durable.clone();
        host.writes_issued = 0;
        host.writes_synced = 0;
        host.last_sync_at = self.now;
        if pending > 0 || dropped > 0 {
            let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
            self.trace.record(TraceEvent {
                time: self.now,
                kind: TraceKind::CrashLoss {
                    node: id,
                    kept_writes: count(kept),
                    lost_writes: count(pending - kept),
                    dropped_outputs: count(dropped),
                },
            });
        }
        Ok(())
    }

    /// Çökmüş bir düğümü diskindeki kalıcı durumla yeniden başlatır. Düğüm hemen
    /// `NodeInput::Restart(disk)` ile adımlanır ve çıktıları her olayınki gibi sırayla uygulanır.
    /// Yeni tick zinciri bir aralık sonra başlar (ilk açılıştaki gibi).
    ///
    /// # Errors
    ///
    /// Düğüm simülasyonda yoksa ya da zaten ayaktaysa [`LifecycleError`]; o durumda simülasyon
    /// değişmez.
    pub fn restart(&mut self, id: NodeId) -> Result<(), LifecycleError> {
        let host = self
            .hosts
            .get_mut(&id)
            .ok_or(LifecycleError::UnknownNode(id))?;
        if host.up {
            return Err(LifecycleError::AlreadyUp(id));
        }
        host.up = true;
        // Yeni açılış, yeni tick zinciri: kuyrukta önceki açılıştan kalmış bir tick artık yok
        // sayılır (bkz. `Host::incarnation`). 2^64 açılış imkânsızdır; yine de taşma paniği yerine
        // doygun toplama.
        host.incarnation = host.incarnation.saturating_add(1);
        let incarnation = host.incarnation;
        let disk = host.durable.clone();
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Restart { node: id },
        });
        if let Some(first_tick) = self.now.checked_add(self.config.tick_every) {
            self.queue.push(
                first_tick,
                Event::Tick {
                    node: id,
                    incarnation,
                },
            );
        }
        let outputs = host.node.step(NodeInput::Restart(disk));
        self.apply_outputs(id, outputs);
        Ok(())
    }

    /// Ayaktaki bir düğüme bir istemci isteği verir: düğüm hemen adımlanır ve çıktıları her
    /// olayınki gibi sırayla uygulanır.
    ///
    /// # Errors
    ///
    /// Düğüm simülasyonda yoksa ya da çökmüşse [`LifecycleError`]; o durumda simülasyon değişmez.
    pub fn submit(&mut self, id: NodeId, request: N::Request) -> Result<(), LifecycleError> {
        let host = self
            .hosts
            .get_mut(&id)
            .ok_or(LifecycleError::UnknownNode(id))?;
        if !host.up {
            return Err(LifecycleError::Down(id));
        }
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Client {
                node: id,
                digest: digest(&request),
            },
        });
        let outputs = host.node.step(NodeInput::Client(request));
        self.apply_outputs(id, outputs);
        Ok(())
    }

    fn handle_tick(&mut self, id: NodeId, incarnation: u64) {
        let Some(host) = self.hosts.get_mut(&id) else {
            return;
        };
        // Çökmüş düğüm tick almaz; önceki bir açılışa ait tick de (numarası tutmaz) yok sayılır.
        // İkisinde de zincir burada biter ve yeniden kurulmaz: yeniden başlatma kendi zincirini
        // kurar (bkz. `restart`). Yok sayılan tick trace'e de girmez, çünkü hiçbir düğüm onu
        // gözlemlemedi.
        if !host.up || host.incarnation != incarnation {
            return;
        }
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Tick { node: id },
        });
        // Sonraki tick, bu tick'in çıktıları yönlendirilmeden ÖNCE kuyruğa konur: tick'ler sabit
        // bir ritimle ilerleyen "saat"tir. Böylece aynı anda düşen bir tick ile bir mesaj teslimi
        // arasında tick, daha küçük `seq` ile önce işlenir. Kural keyfidir ama sabittir; önemli
        // olan her koşuda aynı olmasıdır.
        //
        // `checked_add`: doygun toplama, zaman ekseninin sonunda yeni tick'i AYNI âna koyar ve
        // sonsuz döngü yaratırdı; taşma durumunda tick zinciri biter.
        if let Some(next) = self.now.checked_add(self.config.tick_every) {
            self.queue.push(
                next,
                Event::Tick {
                    node: id,
                    incarnation,
                },
            );
        }
        let outputs = host.node.step(NodeInput::Tick);
        self.apply_outputs(id, outputs);
    }

    fn handle_delivery(&mut self, delivery: Delivery<N::Msg>) {
        let Delivery {
            msg_id,
            from,
            to,
            msg,
            digest,
            sent_at,
            sent_epoch,
        } = delivery;
        let drop_reason = match self.hosts.get(&to) {
            None => Some(DropReason::UnknownDestination),
            // "Kablo kesildi": uçuş boyunca uçlar bir an bile ayrıldıysa mesaj kaybolur; bölünme
            // şimdi iyileşmiş olsa bile. Ağ kararı makineden önce gelir: kesik kablodaki paket
            // makineye hiç ulaşmaz.
            Some(_) if !self.network.connected_since(from, to, sent_epoch) => {
                Some(DropReason::PartitionInFlight)
            }
            // Kablo sağlam ama karşıdaki makine kapalı: paket kaybolur.
            Some(host) if !host.up => Some(DropReason::NodeDown),
            Some(_) => None,
        };
        if let Some(reason) = drop_reason {
            self.trace.record(TraceEvent {
                time: self.now,
                kind: TraceKind::Drop {
                    msg_id,
                    from,
                    to,
                    digest,
                    sent_at,
                    reason,
                },
            });
            return;
        }
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Deliver {
                msg_id,
                from,
                to,
                digest,
                sent_at,
            },
        });
        if let Some(host) = self.hosts.get_mut(&to) {
            let outputs = host.node.step(NodeInput::Message { from, msg });
            self.apply_outputs(to, outputs);
        }
    }

    /// Bir düğümün en eski bekleyen yazması kalıcı oldu: diske işlenir ve artık beklemesi
    /// gerekmeyen çıktılar sırayla bırakılır.
    fn handle_sync(&mut self, id: NodeId, incarnation: u64) {
        let Some(host) = self.hosts.get_mut(&id) else {
            return;
        };
        // Çökmüş düğümün ya da önceki bir açılışın fsync'i: o yazmalar çökmede zaten karara
        // bağlandı (kayboldu ya da diske ulaştı).
        if !host.up || host.incarnation != incarnation {
            return;
        }
        let Some(update) = host.pending.pop_front() else {
            return;
        };
        host.durable.apply(&update);
        host.writes_synced = host.writes_synced.saturating_add(1);
        self.synced.push((id, update));
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Sync { node: id },
        });
        let mut ready = Vec::new();
        while host
            .held
            .front()
            .is_some_and(|held| held.after_writes <= host.writes_synced)
        {
            if let Some(held) = host.held.pop_front() {
                ready.push(held.output);
            }
        }
        for output in ready {
            self.release(id, output);
        }
    }

    /// Bir düğümün çıktılarını VERİLDİKLERİ SIRAYLA uygular (sans-IO sözleşmesi). Sıra anlamlıdır:
    /// bir `Persist`'ten sonraki `Send` ve `Apply`'lar, yazma kalıcı olana kadar bekler (O1). Ters
    /// sıra (önce dışarıya dönük bir çıktı, sonra `Persist`) kaydedilir; bkz.
    /// [`Simulation::take_persists_after_output`].
    fn apply_outputs(&mut self, from: NodeId, outputs: Vec<OutputOf<N>>) {
        let mut outward = false;
        for output in outputs {
            match output {
                NodeOutput::Persist(update) => {
                    // Yazma yine verilir; yalnızca kendisinden önceki çıktıları artık tutamaz.
                    if outward {
                        self.persists_after_output.push(from);
                    }
                    self.write(from, update);
                }
                NodeOutput::Send { to, msg } => {
                    outward = true;
                    self.release_or_hold(from, Release::Send { to, msg });
                }
                NodeOutput::Apply(applied) => {
                    outward = true;
                    self.release_or_hold(from, Release::Apply(applied));
                }
            }
        }
    }

    /// Bir yazma verir. Yazma, düğümün "en son yazdırdığı durum"una hemen işlenir; kalıcı duruma
    /// ise fsync tamamlanınca işlenir. Gecikme 0 ise ve önünde bekleyen yazma yoksa yazma aynı anda
    /// kalıcı olur (gecikmesiz disk).
    fn write(&mut self, id: NodeId, update: UpdateOf<N>) {
        let Some(host) = self.hosts.get_mut(&id) else {
            return;
        };
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Persist {
                node: id,
                digest: digest(&update),
            },
        });
        host.latest.apply(&update);
        host.writes_issued = host.writes_issued.saturating_add(1);
        self.writes.push((id, update.clone()));
        let delay = self.disk.fsync_delay();
        if delay == 0 && host.pending.is_empty() {
            host.durable.apply(&update);
            host.writes_synced = host.writes_synced.saturating_add(1);
            self.synced.push((id, update));
            self.trace.record(TraceEvent {
                time: self.now,
                kind: TraceKind::Sync { node: id },
            });
            return;
        }
        host.pending.push_back(update);
        // fsync'ler yazma sırasıyla tamamlanır: daha sonra verilen bir yazma, öncekinden önce
        // kalıcı olamaz. Zaman ekseninin sonunda an doygun kalır (u64::MAX).
        let at = self.now.saturating_add(delay).max(host.last_sync_at);
        host.last_sync_at = at;
        self.queue.push(
            at,
            Event::Sync {
                node: id,
                incarnation: host.incarnation,
            },
        );
    }

    /// Bir çıktıyı, önünde kalıcı olmayı bekleyen bir yazma yoksa hemen bırakır; varsa o yazmalar
    /// kalıcı olana kadar tutar (O1, sürücü tarafı).
    fn release_or_hold(&mut self, id: NodeId, output: Release<N>) {
        let Some(host) = self.hosts.get_mut(&id) else {
            return;
        };
        if host.writes_synced < host.writes_issued {
            host.held.push_back(Held {
                after_writes: host.writes_issued,
                output,
            });
        } else {
            self.release(id, output);
        }
    }

    /// Bir çıktıyı bırakır: mesaj ağa çıkar, yerel etki kaydedilir.
    fn release(&mut self, id: NodeId, output: Release<N>) {
        match output {
            Release::Send { to, msg } => self.send(id, to, msg),
            Release::Apply(applied) => {
                self.trace.record(TraceEvent {
                    time: self.now,
                    kind: TraceKind::Apply {
                        node: id,
                        digest: digest(&applied),
                    },
                });
                self.applied.push((id, applied));
            }
        }
    }

    fn send(&mut self, from: NodeId, to: NodeId, msg: N::Msg) {
        let msg_id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.saturating_add(1);
        let digest = digest(&msg);
        let sent_at = self.now;
        let sent_epoch = self.network.epoch();
        self.trace.record(TraceEvent {
            time: sent_at,
            kind: TraceKind::Send {
                msg_id,
                from,
                to,
                digest,
            },
        });
        let delays = match self.network.route(from, to) {
            Fate::Dropped(reason) => {
                self.drop_at_send(msg_id, from, to, digest, reason);
                return;
            }
            Fate::Deliver { delay, duplicate } => std::iter::once(delay).chain(duplicate),
        };
        for delay in delays {
            match sent_at.checked_add(delay.get()) {
                Some(at) => {
                    self.queue.push(
                        at,
                        Event::Deliver(Delivery {
                            msg_id,
                            from,
                            to,
                            msg: msg.clone(),
                            digest,
                            sent_at,
                            sent_epoch,
                        }),
                    );
                }
                // Zaman ekseninin sonunu aşan teslim sessizce yok edilmez, açıkça düşer.
                None => self.drop_at_send(msg_id, from, to, digest, DropReason::TimeOverflow),
            }
        }
    }

    fn drop_at_send(
        &mut self,
        msg_id: u64,
        from: NodeId,
        to: NodeId,
        digest: u64,
        reason: DropReason,
    ) {
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Drop {
                msg_id,
                from,
                to,
                digest,
                sent_at: self.now,
                reason,
            },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{SimConfig, Simulation};
    use crate::error::{ConfigError, PartitionError};
    use crate::network::{NetworkConfig, SimNetwork};
    use crate::node::{InputOf, NodeInput, NodeOutput, OutputOf, SimNode};
    use crate::rng::{Component, SeedTree};
    use crate::trace::{DropReason, TraceEncode, TraceKind};
    use raft_core::NodeId;

    #[derive(Debug, Clone)]
    struct Blip;

    impl TraceEncode for Blip {
        fn encode(&self, out: &mut Vec<u8>) {
            out.push(0xb1);
        }
    }

    /// Her tick'te sabit bir hedefe `Blip` yollayan en basit test düğümü.
    struct Sender {
        to: NodeId,
    }

    impl SimNode for Sender {
        type Msg = Blip;
        type Durable = ();
        type Request = ();
        type Applied = ();

        fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>> {
            match input {
                NodeInput::Tick => vec![NodeOutput::Send {
                    to: self.to,
                    msg: Blip,
                }],
                NodeInput::Message { .. } | NodeInput::Restart(()) | NodeInput::Client(()) => {
                    Vec::new()
                }
            }
        }
    }

    /// Her tick'te verilen çıktı türlerini verilen sırayla üreten düğüm: çıktı sırası kaydını
    /// sınamak için.
    struct Scripted(Vec<Kind>);

    #[derive(Debug, Clone, Copy)]
    enum Kind {
        Persist,
        Send,
        Apply,
    }

    impl SimNode for Scripted {
        type Msg = Blip;
        type Durable = ();
        type Request = ();
        type Applied = ();

        fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>> {
            match input {
                NodeInput::Tick => self
                    .0
                    .iter()
                    .map(|kind| match kind {
                        Kind::Persist => NodeOutput::Persist(()),
                        Kind::Send => NodeOutput::Send {
                            to: NodeId(1),
                            msg: Blip,
                        },
                        Kind::Apply => NodeOutput::Apply(()),
                    })
                    .collect(),
                NodeInput::Message { .. } | NodeInput::Restart(()) | NodeInput::Client(()) => {
                    Vec::new()
                }
            }
        }
    }

    fn network() -> Result<SimNetwork, ConfigError> {
        SimNetwork::new(
            NetworkConfig::reliable(1),
            SeedTree::new(1).rng_for(Component::Network),
        )
    }

    fn sim_with(
        config: SimConfig,
        nodes: Vec<(NodeId, Sender)>,
    ) -> Result<Simulation<Sender, SimNetwork>, ConfigError> {
        Simulation::new(config, network()?, nodes)
    }

    fn pair() -> Simulation<Sender, SimNetwork> {
        sim_with(
            SimConfig::default(),
            vec![
                (NodeId(1), Sender { to: NodeId(2) }),
                (NodeId(2), Sender { to: NodeId(1) }),
            ],
        )
        .expect("valid simulation")
    }

    // run_until(t): zamanı t veya daha erken olan olaylar işlenir, sonrakiler beklemede kalır.
    #[test]
    fn run_until_processes_events_up_to_and_including_t() {
        let mut sim = pair();
        sim.run_until(3);
        assert_eq!(sim.now(), 3);
        let ticks = sim
            .trace()
            .events()
            .iter()
            .filter(|e| matches!(e.kind, TraceKind::Tick { .. }))
            .count();
        assert_eq!(ticks, 6, "two nodes tick at t = 1, 2, 3");
        assert!(sim.trace().events().iter().all(|e| e.time <= 3));
    }

    // Aynı andaki olayların sırası sabittir. t=2'de: önce 1'in tick'i (t=1'de, mesajından ÖNCE
    // kuyruğa kondu) ve o tick'te gönderdiği yeni mesajın Send kaydı; sonra 1'in t=1'de gönderdiği
    // mesajın teslimi; ancak ondan sonra 2'nin tick'i.
    #[test]
    fn equal_time_events_have_a_fixed_documented_order() {
        let mut sim = pair();
        sim.run_until(2);
        let at_two: Vec<&TraceKind> = sim
            .trace()
            .events()
            .iter()
            .filter(|e| e.time == 2)
            .map(|e| &e.kind)
            .take(4)
            .collect();
        assert!(matches!(at_two[0], TraceKind::Tick { node: NodeId(1) }));
        assert!(matches!(
            at_two[1],
            TraceKind::Send {
                from: NodeId(1),
                ..
            }
        ));
        assert!(matches!(
            at_two[2],
            TraceKind::Deliver {
                from: NodeId(1),
                ..
            }
        ));
        assert!(matches!(at_two[3], TraceKind::Tick { node: NodeId(2) }));
    }

    // Var olmayan bir düğüme giden mesaj panik değil, "bilinmeyen hedef" düşüşüdür.
    #[test]
    fn unknown_destination_is_dropped_at_delivery() {
        let mut sim = sim_with(
            SimConfig::default(),
            vec![(NodeId(1), Sender { to: NodeId(99) })],
        )
        .expect("valid simulation");
        sim.run_until(3);
        assert!(sim.trace().events().iter().any(|e| matches!(
            e.kind,
            TraceKind::Drop {
                reason: DropReason::UnknownDestination,
                ..
            }
        )));
    }

    // Zaman ekseninin sonu: tick_every = u64::MAX iken ilk tick u64::MAX'tadır ve bir sonraki tick
    // taşacağı için kurulmaz; o tick'teki mesajın teslim zamanı da taşar ve açıkça düşer.
    // run_until(u64::MAX) sonsuza dek dönmek yerine biter.
    #[test]
    fn the_end_of_time_neither_loops_nor_loses_messages_silently() {
        let mut sim = sim_with(
            SimConfig {
                tick_every: u64::MAX,
            },
            vec![
                (NodeId(1), Sender { to: NodeId(2) }),
                (NodeId(2), Sender { to: NodeId(1) }),
            ],
        )
        .expect("valid simulation");
        sim.run_until(u64::MAX);
        let events = sim.trace().events();
        let ticks = events
            .iter()
            .filter(|e| matches!(e.kind, TraceKind::Tick { .. }))
            .count();
        let overflows = events
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    TraceKind::Drop {
                        reason: DropReason::TimeOverflow,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(ticks, 2, "one tick per node, then the clock runs out");
        assert_eq!(overflows, 2, "each send is dropped explicitly, not lost");
    }

    // Çıktı sırası kaydı: aynı adımda bir Send ya da Apply'dan SONRA gelen her Persist ayrı
    // kaydedilir. Persist önce gelirse (O1'in istediği sıra; birden fazla yazma da olabilir) kayıt
    // yoktur. Kayıt bir kez alınır. Senaryo iki tick koşar.
    #[test]
    fn a_persist_after_an_outward_output_is_recorded() {
        use Kind::{Apply, Persist, Send};
        for (script, per_tick) in [
            (vec![Persist, Send, Apply], 0),
            (vec![Persist, Persist, Send], 0),
            (vec![Send, Persist], 1),
            (vec![Apply, Persist, Send], 1),
            (vec![Send, Persist, Persist], 2),
        ] {
            let nodes = [(NodeId(1), Scripted(script.clone()))];
            let mut sim = Simulation::new(SimConfig::default(), network().expect("valid"), nodes)
                .expect("valid simulation");
            sim.run_until(2);
            let expected = vec![NodeId(1); 2 * per_tick];
            assert_eq!(sim.take_persists_after_output(), expected, "{script:?}");
            assert!(sim.take_persists_after_output().is_empty(), "{script:?}");
        }
    }

    // Geçersiz yapılandırmalar hata döner.
    #[test]
    fn invalid_simulation_configs_are_rejected() {
        let duplicate = sim_with(
            SimConfig::default(),
            vec![
                (NodeId(1), Sender { to: NodeId(2) }),
                (NodeId(1), Sender { to: NodeId(2) }),
            ],
        );
        assert!(matches!(
            duplicate,
            Err(ConfigError::DuplicateNode(NodeId(1)))
        ));
        let zero_tick = sim_with(SimConfig { tick_every: 0 }, Vec::new());
        assert!(matches!(zero_tick, Err(ConfigError::ZeroTickInterval)));
    }

    // Bölünme girdisi doğrulanır: simülasyonda olmayan ya da iki kez yazılan düğüm hata verir ve
    // ne ağ ne trace değişir. Geçerli bir bölünme kanonik biçimde kaydedilir.
    #[test]
    fn partition_input_is_validated_and_recorded_canonically() {
        let mut sim = pair();
        assert_eq!(
            sim.partition(&[&[NodeId(1)], &[NodeId(7)]]),
            Err(PartitionError::UnknownNode(NodeId(7)))
        );
        assert_eq!(
            sim.partition(&[&[NodeId(1), NodeId(1)]]),
            Err(PartitionError::DuplicateNode(NodeId(1)))
        );
        assert!(sim.trace().is_empty(), "failed partitions leave no trace");

        sim.partition(&[&[NodeId(2)], &[NodeId(1)]])
            .expect("valid partition");
        assert_eq!(
            sim.trace().events()[0].kind,
            TraceKind::Partition {
                groups: vec![vec![NodeId(1)], vec![NodeId(2)]]
            }
        );
    }
}
