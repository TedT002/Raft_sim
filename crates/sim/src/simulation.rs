//! `Simulation`: düğümleri, ağı, sanal saati ve olay kuyruğunu birleştiren deterministik sürücü.
//!
//! Saat simülatörün kendisindedir (`now`); ayrı bir `Clock` trait'i yoktur. Gerçek zamanla çalışan
//! bir sürücü ancak Faz 6'da gerekir. Zaman yalnızca kuyruktaki bir sonraki olayın zamanına
//! atlayarak ilerler (discrete-event simulation): arada "boşta geçen" gerçek süre yoktur, bu yüzden
//! saatlerce süren bir senaryo milisaniyeler içinde ve her seferinde aynı sırayla koşar.

use std::collections::BTreeMap;

use raft_core::NodeId;

use crate::error::{ConfigError, PartitionError};
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
    /// Bir düğümün periyodik tick'i.
    Tick { node: NodeId },
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

/// Deterministik simülasyon: aynı düğümler, aynı ağ (aynı seed'li RNG) ve aynı çağrılar her
/// seferinde birebir aynı trace'i üretir.
///
/// Düğüm olduğu sürece kuyruk asla boşalmaz: her tick bir sonrakini kurar. Simülasyonu her zaman
/// sonlu bir ufukla (`run_until(t)`) sürün; "hepsini boşalt" niyetiyle `while sim.step() {}` ya da
/// `run_until(u64::MAX)` pratikte bitmez.
pub struct Simulation<N: SimNode, Net: Network> {
    config: SimConfig,
    now: u64,
    queue: EventQueue<Event<N::Msg>>,
    // BTreeMap: düğümler üzerinde gezinme sırası (ilk tick'lerin sıralanması) her koşuda aynı
    // olsun.
    nodes: BTreeMap<NodeId, N>,
    network: Net,
    trace: Trace,
    next_msg_id: u64,
}

impl<N: SimNode, Net: Network> Simulation<N, Net> {
    /// Simülasyonu kurar ve her düğümün ilk tick'ini `tick_every` anına, `NodeId` sırasıyla koyar.
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
        let mut by_id = BTreeMap::new();
        for (id, node) in nodes {
            if by_id.insert(id, node).is_some() {
                return Err(ConfigError::DuplicateNode(id));
            }
        }
        let mut queue = EventQueue::new();
        for &id in by_id.keys() {
            queue.push(config.tick_every, Event::Tick { node: id });
        }
        Ok(Self {
            config,
            now: 0,
            queue,
            nodes: by_id,
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

    /// Bir düğüme (salt okunur) erişim: testlerin düğüm iç durumunu incelemesi için.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&N> {
        self.nodes.get(&id)
    }

    /// Ağ modeline (salt okunur) erişim.
    #[must_use]
    pub fn network(&self) -> &Net {
        &self.network
    }

    /// Kuyruktaki en erken olayı işler. Kuyruk boşsa `false` döner; düğüm olduğu sürece kuyruk
    /// boşalmadığından bunu bir "bitti" sinyali olarak kullanmayın (bkz. tip dokümanı).
    pub fn step(&mut self) -> bool {
        let Some(scheduled) = self.queue.pop() else {
            return false;
        };
        // Kuyruk en erken olayı verir ve yeni olaylar hep "şimdi"den sonraya konur (tick aralığı
        // ≥ 1, ağ gecikmesi `NonZeroU64`); bu yüzden zaman asla geri gitmez.
        self.now = scheduled.time;
        match scheduled.event {
            Event::Tick { node } => self.handle_tick(node),
            Event::Deliver(delivery) => self.handle_delivery(delivery),
        }
        true
    }

    /// Zamanı `time` anına kadar ilerletir: zamanı `time` veya daha erken olan bütün olayları
    /// işler, sonra saati `time`'a getirir (sonraki olay daha geç olsa bile).
    ///
    /// Aynı zaman damgasında sonsuz döngü olamaz: her olay yalnızca kendisinden SONRAKİ bir zamana
    /// yeni olay koyar. Zaman ekseninin sonunda (`u64::MAX`) tick'ler durur ve sonu aşan teslimler
    /// düşer.
    pub fn run_until(&mut self, time: u64) {
        while self.queue.peek_time().is_some_and(|next| next <= time) {
            self.step();
        }
        self.now = self.now.max(time);
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
            .find(|id| !self.nodes.contains_key(id))
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

    fn handle_tick(&mut self, node: NodeId) {
        self.trace.record(TraceEvent {
            time: self.now,
            kind: TraceKind::Tick { node },
        });
        // Sonraki tick, bu tick'in çıktıları yönlendirilmeden ÖNCE kuyruğa konur: tick'ler sabit
        // bir ritimle ilerleyen "saat"tir. Böylece aynı anda düşen bir tick ile bir mesaj teslimi
        // arasında tick, daha küçük `seq` ile önce işlenir. Kural keyfidir ama sabittir; önemli
        // olan her koşuda aynı olmasıdır.
        //
        // `checked_add`: doygun toplama, zaman ekseninin sonunda yeni tick'i AYNI âna koyar ve
        // sonsuz döngü yaratırdı; taşma durumunda tick zinciri biter.
        if let Some(next) = self.now.checked_add(self.config.tick_every) {
            self.queue.push(next, Event::Tick { node });
        }
        if let Some(state) = self.nodes.get_mut(&node) {
            let outputs = state.step(NodeInput::Tick);
            self.route_outputs(node, outputs);
        }
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
        let drop_reason = if !self.nodes.contains_key(&to) {
            Some(DropReason::UnknownDestination)
        } else if !self.network.connected_since(from, to, sent_epoch) {
            // "Kablo kesildi": uçuş boyunca uçlar bir an bile ayrıldıysa mesaj kaybolur; bölünme
            // şimdi iyileşmiş olsa bile.
            Some(DropReason::PartitionInFlight)
        } else {
            None
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
        if let Some(state) = self.nodes.get_mut(&to) {
            let outputs = state.step(NodeInput::Message { from, msg });
            self.route_outputs(to, outputs);
        }
    }

    /// Bir düğümün çıktılarını VERİLDİKLERİ SIRAYLA uygular (sans-IO sözleşmesi).
    fn route_outputs(&mut self, from: NodeId, outputs: Vec<NodeOutput<N::Msg>>) {
        for output in outputs {
            match output {
                NodeOutput::Send { to, msg } => self.send(from, to, msg),
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

        fn step(&mut self, input: NodeInput<Blip>) -> Vec<NodeOutput<Blip>> {
            match input {
                NodeInput::Tick => vec![NodeOutput::Send {
                    to: self.to,
                    msg: Blip,
                }],
                NodeInput::Message { .. } => Vec::new(),
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
