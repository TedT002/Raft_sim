//! `Simulation`: düğümleri, ağı, sanal saati ve olay kuyruğunu birleştiren deterministik sürücü.
//!
//! Saat simülatörün kendisindedir (`now`); ayrı bir `Clock` trait'i yoktur. Gerçek zamanla çalışan
//! bir sürücü ancak Faz 6'da gerekir. Zaman yalnızca kuyruktaki bir sonraki olayın zamanına
//! atlayarak ilerler (discrete-event simulation): arada "boşta geçen" gerçek süre yoktur, bu yüzden
//! saatlerce süren bir senaryo milisaniyeler içinde ve her seferinde aynı sırayla koşar.
//!
//! Her düğüm simüle bir makinede yaşar: süreç (düğümün kendisi), diski ve yaşam durumu. Makine
//! çökebilir ve diskindeki durumla yeniden başlatılabilir (`crash`/`restart`).

use std::collections::BTreeMap;

use raft_core::NodeId;

use crate::error::{ConfigError, LifecycleError, PartitionError};
use crate::network::{Fate, Network};
use crate::node::{NodeInput, NodeOutput, SimNode};
use crate::queue::EventQueue;
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

/// Kuyruktaki olaylar.
#[derive(Debug, Clone)]
enum Event<M> {
    /// Bir düğümün periyodik tick'i. `incarnation`, tick zincirinin düğümün hangi açılışına ait
    /// olduğunu söyler (bkz. `Host::incarnation`).
    Tick { node: NodeId, incarnation: u64 },
    /// Yoldaki bir mesajın (ya da kopyasının) teslim anı.
    Deliver(Delivery<M>),
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

/// Simüle bir makine: düğüm süreci, diski ve yaşam durumu.
struct Host<N: SimNode> {
    node: N,
    /// Diskteki kalıcı durum. Faz 2'de her `Persist` anında ve bütünüyle kalıcıdır.
    disk: N::Durable,
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
    /// Diskteki kalıcı durum.
    pub disk: &'a N::Durable,
    /// Düğüm ayakta mı?
    pub up: bool,
}

/// Deterministik simülasyon: aynı düğümler, aynı ağ (aynı seed'li RNG) ve aynı çağrılar her
/// seferinde birebir aynı trace'i üretir.
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
    trace: Trace,
    next_msg_id: u64,
}

impl<N: SimNode, Net: Network> Simulation<N, Net> {
    /// Simülasyonu kurar ve her düğümün ilk tick'ini `tick_every` anına, `NodeId` sırasıyla koyar.
    /// Her düğüm boş bir diskle (`N::Durable::default()`) ve ayakta başlar.
    ///
    /// # Errors
    ///
    /// `tick_every` sıfırsa ya da bir düğüm kimliği birden fazla kez verilmişse [`ConfigError`].
    pub fn new(
        config: SimConfig,
        network: Net,
        nodes: impl IntoIterator<Item = (NodeId, N)>,
    ) -> Result<Self, ConfigError> {
        if config.tick_every == 0 {
            return Err(ConfigError::ZeroTickInterval);
        }
        let mut hosts = BTreeMap::new();
        for (id, node) in nodes {
            let host = Host {
                node,
                disk: N::Durable::default(),
                up: true,
                incarnation: 0,
            };
            if hosts.insert(id, host).is_some() {
                return Err(ConfigError::DuplicateNode(id));
            }
        }
        let mut queue = EventQueue::new();
        for &id in hosts.keys() {
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
            trace: Trace::new(),
            next_msg_id: 0,
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

    /// Bir düğümün diskindeki kalıcı durum.
    #[must_use]
    pub fn disk(&self, id: NodeId) -> Option<&N::Durable> {
        self.hosts.get(&id).map(|host| &host.disk)
    }

    /// Bir düğümün diskine yazma erişimi. YALNIZCA crate içi testler içindir: bir denetimin (ör.
    /// `RaftCluster`'ın dayanıklılık denetimi) disk ile bellek ayrıştığında gerçekten ihlal
    /// bildirdiğini göstermek için diski elle bozar. Genel API'de yoktur; simülasyonda disk
    /// yalnızca `Persist` çıktılarıyla değişir.
    #[cfg(test)]
    pub(crate) fn disk_mut(&mut self, id: NodeId) -> Option<&mut N::Durable> {
        self.hosts.get_mut(&id).map(|host| &mut host.disk)
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
            disk: &host.disk,
            up: host.up,
        })
    }

    /// Ağ modeline (salt okunur) erişim.
    #[must_use]
    pub fn network(&self) -> &Net {
        &self.network
    }

    /// Kuyruktaki en erken olayı işler. Kuyruk boşsa `false` döner; ayakta düğüm olduğu sürece
    /// kuyruk boşalmadığından bunu bir "bitti" sinyali olarak kullanmayın (bkz. tip dokümanı).
    pub fn step(&mut self) -> bool {
        let Some(scheduled) = self.queue.pop() else {
            return false;
        };
        // Kuyruk en erken olayı verir ve yeni olaylar hep "şimdi"den sonraya konur (tick aralığı
        // ≥ 1, ağ gecikmesi `NonZeroU64`); bu yüzden zaman asla geri gitmez.
        self.now = scheduled.time;
        match scheduled.event {
            Event::Tick { node, incarnation } => self.handle_tick(node, incarnation),
            Event::Deliver(delivery) => self.handle_delivery(delivery),
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
    /// Aynı zaman damgasında sonsuz döngü olamaz: her olay yalnızca kendisinden SONRAKİ bir zamana
    /// yeni olay koyar. Zaman ekseninin sonunda (`u64::MAX`) tick'ler durur ve sonu aşan teslimler
    /// düşer.
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
    /// ([`DropReason::NodeDown`]). Diski korunur: `restart` düğümü diskteki durumla geri getirir.
    ///
    /// Düğümün çökmeden önce gönderdiği ve hâlâ yolda olan mesajlar ağdadır, yine teslim edilir:
    /// kablodaki paket, gönderen makine kapansa da yoluna devam eder. Düğüm nesnesi bellekte kalır
    /// ama hiçbir girdi almaz; yeniden başlatmada `Restart` sözleşmesi gereği bütün bellek
    /// durumunu diskten yeniden kurmalıdır.
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
        Ok(())
    }

    /// Çökmüş bir düğümü diskindeki durumla yeniden başlatır. Düğüm hemen
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
        let disk = host.disk.clone();
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

    /// Bir düğümün çıktılarını VERİLDİKLERİ SIRAYLA uygular (sans-IO sözleşmesi). Sıra anlamlıdır:
    /// bir `Persist`, kendisinden sonraki `Send`'ler ağa çıkmadan önce diske yazılır.
    fn apply_outputs(&mut self, from: NodeId, outputs: Vec<NodeOutput<N::Msg, N::Durable>>) {
        for output in outputs {
            match output {
                NodeOutput::Send { to, msg } => self.send(from, to, msg),
                NodeOutput::Persist(state) => self.persist(from, state),
            }
        }
    }

    /// Kalıcı durumu diske yazar ve özetini trace'e kaydeder. Faz 2'de disk anında ve bütünüyle
    /// kalıcıdır; `fsync`'e kadar bekleyen yazmalar Faz 3'te gelecek.
    fn persist(&mut self, id: NodeId, state: N::Durable) {
        let digest = digest(&state);
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Persist { node: id, digest },
        });
        if let Some(host) = self.hosts.get_mut(&id) {
            host.disk = state;
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
    use crate::node::{NodeInput, NodeOutput, SimNode};
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

        fn step(&mut self, input: NodeInput<Blip, ()>) -> Vec<NodeOutput<Blip, ()>> {
            match input {
                NodeInput::Tick => vec![NodeOutput::Send {
                    to: self.to,
                    msg: Blip,
                }],
                NodeInput::Message { .. } | NodeInput::Restart(()) => Vec::new(),
            }
        }
    }

    fn sim_with(
        config: SimConfig,
        nodes: Vec<(NodeId, Sender)>,
    ) -> Result<Simulation<Sender, SimNetwork>, ConfigError> {
        let network = SimNetwork::new(
            NetworkConfig::reliable(1),
            SeedTree::new(1).rng_for(Component::Network),
        )?;
        Simulation::new(config, network, nodes)
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
