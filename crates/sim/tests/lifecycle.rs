//! Düğüm yaşam döngüsü testleri: çökme, yeniden başlatma, disk ve tick zincirleri.

mod support;

use sim::{
    Component, DropReason, DurableState, InputOf, LifecycleError, NetworkConfig, NodeId, NodeInput,
    NodeOutput, OutputOf, SeedTree, SimConfig, SimNetwork, SimNode, Simulation, TraceEncode,
    TraceKind, digest,
};
use support::{PingPongSim, cluster, deliveries, drops};

/// Bir düğümün tick aldığı anlar, sırasıyla.
fn tick_times(sim: &PingPongSim, node: NodeId) -> Vec<u64> {
    sim.trace()
        .events()
        .iter()
        .filter(|event| event.kind == TraceKind::Tick { node })
        .map(|event| event.time)
        .collect()
}

// Çökmüş düğüm tick almaz ve ona gelen mesajlar teslim anında "düğüm kapalı" nedeniyle düşer.
// Yeniden başlatılınca hem tick'ler hem teslimler kaldığı yerden sürer. Çökme ve yeniden başlatma
// trace'e kaydedilir.
#[test]
fn a_crashed_node_gets_no_ticks_and_misses_its_messages() {
    let mut sim = cluster(1, 3, NetworkConfig::reliable(2));
    sim.run_until(20);
    sim.crash(NodeId(2)).expect("node 2 is up");
    sim.run_until(40);
    sim.restart(NodeId(2)).expect("node 2 is down");
    sim.run_until(60);

    let node2 = NodeId(2);
    let ticks = tick_times(&sim, node2);
    assert!(ticks.iter().all(|&t| t <= 20 || t > 40));
    assert_eq!(ticks.iter().filter(|&&t| t > 40).count(), 20);

    let down_drops: Vec<_> = drops(&sim)
        .into_iter()
        .filter(|drop| drop.reason == DropReason::NodeDown)
        .collect();
    assert!(!down_drops.is_empty());
    assert!(
        down_drops
            .iter()
            .all(|drop| drop.to == node2 && drop.at > 20 && drop.at <= 40)
    );
    assert!(
        deliveries(&sim)
            .iter()
            .filter(|delivery| delivery.to == node2)
            .all(|delivery| delivery.at <= 20 || delivery.at > 40)
    );
    assert!(
        deliveries(&sim)
            .iter()
            .any(|delivery| delivery.to == node2 && delivery.at > 40),
        "deliveries resume after the restart"
    );

    let lifecycle: Vec<(u64, &TraceKind)> = sim
        .trace()
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::Crash { .. } | TraceKind::Restart { .. }
            )
        })
        .map(|event| (event.time, &event.kind))
        .collect();
    assert_eq!(
        lifecycle,
        vec![
            (20, &TraceKind::Crash { node: node2 }),
            (40, &TraceKind::Restart { node: node2 })
        ]
    );
}

// Çift tick yok: aynı anda çöküp yeniden başlayan düğümün kuyrukta eski açılışından kalmış bir
// tick'i vardır (t=11). Yeni açılış da ilk tick'ini t=11'e kurar. Eski tick açılış numarası
// tutmadığı için yok sayılır; düğüm her anda tam olarak bir tick alır. Bu kural olmasaydı t=11'den
// sonra iki tick zinciri üst üste biner ve düğümün saati iki kat hızlı akardı.
#[test]
fn a_restarted_node_ticks_exactly_once_per_interval() {
    let mut sim = cluster(2, 2, NetworkConfig::reliable(1));
    sim.run_until(10);
    sim.crash(NodeId(1)).expect("node 1 is up");
    sim.restart(NodeId(1)).expect("node 1 is down");
    sim.run_until(30);
    assert_eq!(tick_times(&sim, NodeId(1)), (1..=30).collect::<Vec<_>>());

    // Kapalıyken geçen süre de tick üretmez: t=12'de yeniden başlayan düğümün ilk tick'i t=13'tür.
    let mut sim = cluster(2, 2, NetworkConfig::reliable(1));
    sim.run_until(10);
    sim.crash(NodeId(1)).expect("node 1 is up");
    sim.run_until(12);
    sim.restart(NodeId(1)).expect("node 1 is down");
    sim.run_until(30);
    let expected: Vec<u64> = (1..=10).chain(13..=30).collect();
    assert_eq!(tick_times(&sim, NodeId(1)), expected);
}

// Geçersiz yaşam döngüsü istekleri hata döner ve simülasyonu (trace dahil) değiştirmez.
#[test]
fn lifecycle_errors_leave_the_simulation_unchanged() {
    let mut sim = cluster(3, 2, NetworkConfig::reliable(1));
    sim.run_until(5);
    let events = sim.trace().len();
    assert_eq!(
        sim.crash(NodeId(9)),
        Err(LifecycleError::UnknownNode(NodeId(9)))
    );
    assert_eq!(
        sim.restart(NodeId(9)),
        Err(LifecycleError::UnknownNode(NodeId(9)))
    );
    assert_eq!(
        sim.restart(NodeId(1)),
        Err(LifecycleError::AlreadyUp(NodeId(1)))
    );
    assert_eq!(sim.trace().len(), events);
    assert!(sim.is_up(NodeId(1)));

    sim.crash(NodeId(1)).expect("node 1 is up");
    assert_eq!(
        sim.crash(NodeId(1)),
        Err(LifecycleError::AlreadyDown(NodeId(1)))
    );
    assert_eq!(
        sim.trace().len(),
        events + 1,
        "only the valid crash is traced"
    );
    assert!(!sim.is_up(NodeId(1)));
    assert!(!sim.is_up(NodeId(9)), "an unknown node is not up");
}

/// Diske yazılan sayaç (yalnızca bu test dosyasının düğümü için).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Count(u64);

impl TraceEncode for Count {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0.to_le_bytes());
    }
}

/// Her tick'te sayacını artıran ama yalnızca çift değerleri diske yazan düğüm. Yeniden
/// başlatmada sayacı diskteki değere döner: diske yazılmamış her şey kaybolur.
#[derive(Debug, Default)]
struct Journal {
    count: u64,
    restarted_with: Vec<u64>,
}

/// Sayacın diskteki hâli: her yazma yeni değeri bütünüyle taşır.
impl DurableState for Count {
    type Update = Count;

    fn apply(&mut self, update: &Count) {
        *self = update.clone();
    }
}

impl SimNode for Journal {
    type Msg = Count;
    type Durable = Count;
    type Request = ();
    type Applied = ();
    type Response = ();

    fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>> {
        match input {
            NodeInput::Tick => {
                self.count += 1;
                if self.count.is_multiple_of(2) {
                    vec![NodeOutput::Persist(Count(self.count))]
                } else {
                    Vec::new()
                }
            }
            NodeInput::Message { .. } | NodeInput::Client(()) => Vec::new(),
            NodeInput::Restart(Count(count)) => {
                self.count = count;
                self.restarted_with.push(count);
                Vec::new()
            }
        }
    }
}

// Disk sözleşmesi: yeniden başlatılan düğüm, bellekteki son değeri değil diske EN SON yazılan
// değeri alır. t=5'te sayaç 5'tir ama diskte 4 vardır; çökme sonrası düğüm 4'ten devam eder. Her
// `Persist` trace'e yazılan durumun özetiyle kaydedilir.
#[test]
fn a_restart_recovers_exactly_what_was_persisted() {
    let network = SimNetwork::new(
        NetworkConfig::reliable(1),
        SeedTree::new(4).rng_for(Component::Network),
    )
    .expect("valid network config");
    let mut sim = Simulation::new(
        SimConfig::default(),
        network,
        [(NodeId(1), Journal::default())],
    )
    .expect("valid simulation config");

    assert_eq!(
        sim.disk(NodeId(1)),
        Some(&Count(0)),
        "every disk starts empty"
    );
    sim.run_until(5);
    assert_eq!(sim.node(NodeId(1)).map(|node| node.count), Some(5));
    assert_eq!(sim.disk(NodeId(1)), Some(&Count(4)));

    sim.crash(NodeId(1)).expect("node 1 is up");
    sim.restart(NodeId(1)).expect("node 1 is down");
    let node = sim.node(NodeId(1)).expect("node 1 exists");
    assert_eq!(node.restarted_with, vec![4]);
    assert_eq!(node.count, 4, "the unpersisted fifth tick is lost");

    sim.run_until(7);
    assert_eq!(sim.disk(NodeId(1)), Some(&Count(6)));
    let persisted: Vec<(u64, u64)> = sim
        .trace()
        .events()
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Persist {
                node: NodeId(1),
                digest,
            } => Some((event.time, digest)),
            _ => None,
        })
        .collect();
    assert_eq!(
        persisted,
        vec![
            (2, digest(&Count(2))),
            (4, digest(&Count(4))),
            (7, digest(&Count(6)))
        ]
    );
}
