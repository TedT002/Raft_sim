//! Simüle disk testleri: yazmaların fsync'i beklemesi, tutulan çıktılar, çökmede kayıp ve kısmi
//! yazma, fsync sırası, istemci istekleri ve uygulamaların sürücüye ulaşması.

use sim::{
    Component, DiskConfig, DurableState, InputOf, LifecycleError, NetworkConfig, NodeId, NodeInput,
    NodeOutput, OutputOf, SeedTree, SimConfig, SimDisk, SimNetwork, SimNode, SimOptions,
    Simulation, TickOrder, TraceEncode, TraceKind, digest, uniform_inclusive,
};

/// Sayaç: hem mesaj, hem diskteki durum, hem istemci isteği, hem de uygulanan etki.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Count(u64);

impl TraceEncode for Count {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0.to_le_bytes());
    }
}

/// Sayacın diskteki hâli: her yazma yeni değeri bütünüyle taşır.
impl DurableState for Count {
    type Update = Count;

    fn apply(&mut self, update: &Count) {
        *self = *update;
    }
}

/// Her tick'te sayacını artırıp diske yazdıran, ardından yeni değeri eşine gönderen ve "uygulayan"
/// düğüm. Doğru bir sans-IO düğümün yaptığı gibi yazma, ona bağlı çıktılardan önce gelir (O1).
/// `send_first` bu sırayı bilerek bozar: mesaj yazmadan ÖNCE verilir. İstemci isteği sayacı
/// verilen değere getirir ve istemciye bir cevap (`Reply`) verir.
#[derive(Debug, Default)]
struct Ledger {
    count: u64,
    peer: Option<NodeId>,
    send_first: bool,
    restarted_with: Vec<u64>,
}

impl SimNode for Ledger {
    type Msg = Count;
    type Durable = Count;
    type Request = Count;
    type Applied = Count;
    type Response = ();

    fn step(&mut self, input: InputOf<Self>) -> Vec<OutputOf<Self>> {
        match input {
            NodeInput::Tick => {
                self.count += 1;
                let persist = NodeOutput::Persist(Count(self.count));
                let send = self.peer.map(|to| NodeOutput::Send {
                    to,
                    msg: Count(self.count),
                });
                let mut outputs = Vec::new();
                if self.send_first {
                    outputs.extend(send);
                    outputs.push(persist);
                } else {
                    outputs.push(persist);
                    outputs.extend(send);
                }
                outputs.push(NodeOutput::Apply(Count(self.count)));
                outputs
            }
            NodeInput::Message { .. } => Vec::new(),
            NodeInput::Restart(Count(count)) => {
                self.count = count;
                self.restarted_with.push(count);
                Vec::new()
            }
            NodeInput::Client(Count(count)) => {
                self.count = count;
                vec![
                    NodeOutput::Persist(Count(count)),
                    NodeOutput::Apply(Count(count)),
                    NodeOutput::Reply(()),
                ]
            }
        }
    }
}

type LedgerSim = Simulation<Ledger, SimNetwork>;

/// İki düğümlü (1 → 2) bir Ledger simülasyonu: ağ güvenilir (gecikme 1), disk verilen ayarda.
fn ledger(seed: u64, disk: DiskConfig) -> LedgerSim {
    ledger_with(seed, disk, false)
}

/// [`ledger`], ama düğüm 1 isteğe bağlı olarak mesajını yazmadan önce verir.
fn ledger_with(seed: u64, disk: DiskConfig, send_first: bool) -> LedgerSim {
    let seeds = SeedTree::new(seed);
    let network = SimNetwork::new(
        NetworkConfig::reliable(1),
        seeds.rng_for(Component::Network),
    )
    .expect("valid network config");
    let options = SimOptions {
        disk: SimDisk::new(disk, seeds.rng_for(Component::Disk)).expect("valid disk config"),
        tick_order: TickOrder::ById,
    };
    let nodes = [
        (
            NodeId(1),
            Ledger {
                peer: Some(NodeId(2)),
                send_first,
                ..Ledger::default()
            },
        ),
        (NodeId(2), Ledger::default()),
    ];
    Simulation::with_options(SimConfig::default(), network, nodes, options)
        .expect("valid simulation config")
}

/// Sabit gecikmeli, kısmi yazmasız disk.
fn fixed(delay: u64) -> DiskConfig {
    DiskConfig {
        min_fsync_delay: delay,
        max_fsync_delay: delay,
        partial_write_prob: 0.0,
    }
}

/// Düğüm 1'in gönderdiği mesajların (zaman, sayaç) çiftleri, gönderim sırasıyla.
fn sends_of_node_1(sim: &LedgerSim, upto: u64) -> Vec<(u64, u64)> {
    let digests: Vec<(u64, u64)> = (1..=upto).map(|k| (digest(&Count(k)), k)).collect();
    sim.trace()
        .events()
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Send {
                from: NodeId(1),
                digest: sent,
                ..
            } => digests
                .iter()
                .find(|(d, _)| *d == sent)
                .map(|&(_, k)| (event.time, k)),
            _ => None,
        })
        .collect()
}

/// Bir türdeki kayıtların zamanları (yalnızca düğüm 1).
fn times_of(sim: &LedgerSim, wanted: fn(&TraceKind) -> bool) -> Vec<u64> {
    sim.trace()
        .events()
        .iter()
        .filter(|event| wanted(&event.kind))
        .map(|event| event.time)
        .collect()
}

// O1, sürücü tarafı: bir yazmadan sonraki mesaj ve uygulama, o yazmanın fsync'i bitene kadar
// tutulur. fsync 3 tick sürdüğünde k. tick'te üretilen mesaj ve uygulama k + 3'te bırakılır; yazma
// ise üretildiği anda verilir.
#[test]
fn outputs_after_a_write_wait_for_its_fsync() {
    let mut sim = ledger(1, fixed(3));
    sim.run_until(10);
    assert_eq!(
        times_of(&sim, |kind| matches!(
            kind,
            TraceKind::Persist {
                node: NodeId(1),
                ..
            }
        )),
        (1..=10).collect::<Vec<_>>()
    );
    assert_eq!(
        times_of(&sim, |kind| matches!(
            kind,
            TraceKind::Sync { node: NodeId(1) }
        )),
        (4..=10).collect::<Vec<_>>()
    );
    let expected: Vec<(u64, u64)> = (1..=7).map(|k| (k + 3, k)).collect();
    assert_eq!(sends_of_node_1(&sim, 10), expected);
    let applied: Vec<u64> = sim
        .take_applied()
        .into_iter()
        .filter(|&(node, _)| node == NodeId(1))
        .map(|(_, Count(k))| k)
        .collect();
    assert_eq!(applied, (1..=7).collect::<Vec<_>>());
    assert_eq!(sim.disk(NodeId(1)), Some(&Count(7)));
    assert_eq!(sim.latest(NodeId(1)), Some(&Count(10)));
    assert!(
        sim.take_persists_after_output().is_empty(),
        "every write precedes its outputs"
    );
}

// Ters sıra (önce Send, sonra Persist): simülatör, yazmadan önce verilmiş bir mesajı tutamaz; mesaj
// durum kalıcı olmadan ağa çıkar. fsync penceresinde gelen bir çökme bunu görünür kılar: eş, diske
// hiç ulaşmamış bir değeri almıştır. Simülatör ters sırayı üretildiği adımda kaydeder (bkz.
// `Simulation::take_persists_after_output`); `RaftCluster` onu Raft için bir ihlal olarak bildirir.
#[test]
fn a_send_before_its_persist_escapes_the_fsync_window() {
    let mut sim = ledger_with(5, fixed(3), true);
    sim.run_until(1);
    assert_eq!(sim.take_persists_after_output(), vec![NodeId(1)]);
    sim.crash(NodeId(1)).expect("node 1 is up");
    sim.run_until(2);
    assert_eq!(
        sends_of_node_1(&sim, 1),
        vec![(1, 1)],
        "the message left at once"
    );
    let delivered = sim.trace().events().iter().any(|event| {
        event.time == 2
            && matches!(
                event.kind,
                TraceKind::Deliver {
                    from: NodeId(1),
                    to: NodeId(2),
                    digest: sent,
                    ..
                } if sent == digest(&Count(1))
            )
    });
    assert!(delivered, "the peer received the value");
    assert_eq!(
        sim.disk(NodeId(1)),
        Some(&Count(0)),
        "but the value never reached the sender's disk"
    );
}

// Çökme, fsync'i tamamlanmamış yazmaları ve onları bekleyen çıktıları kaybettirir. t=5'te 3
// yazma beklemededir (3, 4, 5) ve 6 çıktı (3 mesaj, 3 uygulama) onları beklemektedir: hepsi
// kaybolur, düğüm diskteki son kalıcı değerle (2) yeniden başlar ve kaybolan mesajlar asla
// gönderilmez.
#[test]
fn a_crash_loses_pending_writes_and_held_outputs() {
    let mut sim = ledger(2, fixed(3));
    sim.run_until(5);
    sim.crash(NodeId(1)).expect("node 1 is up");
    let loss: Vec<&TraceKind> = sim
        .trace()
        .events()
        .iter()
        .map(|event| &event.kind)
        .filter(|kind| matches!(kind, TraceKind::CrashLoss { .. }))
        .collect();
    assert_eq!(
        loss,
        vec![&TraceKind::CrashLoss {
            node: NodeId(1),
            kept_writes: 0,
            lost_writes: 3,
            dropped_outputs: 6,
        }]
    );
    assert_eq!(sim.disk(NodeId(1)), Some(&Count(2)));
    assert_eq!(sim.latest(NodeId(1)), Some(&Count(2)));
    sim.run_until(20);
    sim.restart(NodeId(1)).expect("node 1 is down");
    assert_eq!(
        sim.node(NodeId(1)).map(|node| node.restarted_with.clone()),
        Some(vec![2])
    );
    let sent: Vec<u64> = sends_of_node_1(&sim, 5)
        .into_iter()
        .map(|(_, k)| k)
        .collect();
    assert_eq!(
        sent,
        vec![1, 2],
        "the held messages 3, 4 and 5 are never sent"
    );
}

// "Kısmen yazılır": kısmi yazma olasılığı 1 iken çökme, bekleyen yazmaların rastgele bir öneğini
// yine de kalıcı yapar (sonrakiler olmadan öncekiler; asla tersi). Kalan disk durumu, korunan
// önekle tutarlıdır ve tutulan bütün çıktılar yine kaybolur. Seed taramasında hem hiçbir şeyin hem
// de bir kısmın korunduğu durumlar görülür.
#[test]
fn a_partial_write_keeps_a_prefix_of_the_pending_writes() {
    let disk = DiskConfig {
        partial_write_prob: 1.0,
        ..fixed(3)
    };
    let mut kept_counts = Vec::new();
    for seed in 0..30 {
        let mut sim = ledger(seed, disk);
        sim.run_until(5);
        sim.crash(NodeId(1)).expect("node 1 is up");
        let Some(&TraceKind::CrashLoss {
            kept_writes,
            lost_writes,
            dropped_outputs,
            ..
        }) = sim
            .trace()
            .events()
            .iter()
            .map(|event| &event.kind)
            .find(|kind| matches!(kind, TraceKind::CrashLoss { .. }))
        else {
            panic!("seed {seed}: the crash must report its losses");
        };
        assert_eq!(kept_writes + lost_writes, 3, "seed {seed}");
        assert_eq!(dropped_outputs, 6, "seed {seed}");
        assert_eq!(
            sim.disk(NodeId(1)),
            Some(&Count(2 + kept_writes)),
            "seed {seed}"
        );
        kept_counts.push(kept_writes);
    }
    assert!(kept_counts.contains(&0), "{kept_counts:?}");
    assert!(
        kept_counts.iter().any(|&kept| kept > 0 && kept < 3),
        "{kept_counts:?}"
    );
}

// Önceki açılışın fsync'leri yeni açılışın yazmalarını kalıcı yapmaz. t=5'te 3, 4 ve 5. yazmalar
// beklemedeyken (fsync'leri t=6, 7, 8'de) düğüm çöker ve hemen yeniden başlar. Kuyrukta kalan o üç
// fsync olayı yok sayılmalıdır: yoksa yeni açılışın t=6'daki yazması, fsync'i t=9'da bitecekken
// t=7'deki eski olayla kalıcı sayılır ve ona bağlı mesaj iki tick erken çıkardı.
#[test]
fn a_restart_ignores_the_fsyncs_of_the_previous_incarnation() {
    let mut sim = ledger(2, fixed(3));
    sim.run_until(5);
    sim.crash(NodeId(1)).expect("node 1 is up");
    sim.restart(NodeId(1)).expect("node 1 is down");
    sim.run_until(10);
    assert_eq!(
        times_of(&sim, |kind| matches!(
            kind,
            TraceKind::Sync { node: NodeId(1) }
        )),
        vec![4, 5, 9, 10]
    );
    assert_eq!(
        sends_of_node_1(&sim, 4),
        vec![(4, 1), (5, 2), (9, 3), (10, 4)]
    );
}

// fsync zamanlaması bir referans modelle birebir aynıdır. Ledger kümesinde her tick'te önce düğüm
// 1, sonra düğüm 2 bir yazma verir; disk her yazma için Disk alt-seed'inden sırayla tam bir gecikme
// çeker. Bir düğümün k. yazmasının kalıcı olacağı an `s_k = max(k + d_k, s_{k-1})`'dir: gecikme
// kadar sonra, ama önceki yazmasından önce değil (FIFO). Model, trace'teki fsync kayıtlarının
// zamanlarıyla karşılaştırılır.
#[test]
fn fsync_times_match_a_reference_model() {
    let (min, max) = (1, 5);
    let disk = DiskConfig {
        min_fsync_delay: min,
        max_fsync_delay: max,
        partial_write_prob: 0.0,
    };
    let horizon = 40;
    for seed in 0..10 {
        let mut sim = ledger(seed, disk);
        sim.run_until(horizon);
        let mut rng = SeedTree::new(seed).rng_for(Component::Disk);
        let mut expected = [Vec::new(), Vec::new()];
        let mut last = [0, 0];
        for tick in 1..=horizon {
            for node in 0..2 {
                let at = (tick + uniform_inclusive(&mut rng, min, max)).max(last[node]);
                last[node] = at;
                if at <= horizon {
                    expected[node].push(at);
                }
            }
        }
        for (node, expected) in [(NodeId(1), &expected[0]), (NodeId(2), &expected[1])] {
            let recorded: Vec<u64> = sim
                .trace()
                .events()
                .iter()
                .filter(|event| event.kind == TraceKind::Sync { node })
                .map(|event| event.time)
                .collect();
            assert_eq!(&recorded, expected, "seed {seed}, {node:?}");
        }
    }
}

// fsync'ler yazma sırasıyla tamamlanır: gecikmeler rastgele (1..5) olsa bile daha sonraki bir
// yazma öncekinden önce kalıcı olmaz, dolayısıyla mesajlar da sırayla bırakılır.
#[test]
fn fsyncs_complete_in_write_order_despite_random_delays() {
    let disk = DiskConfig {
        min_fsync_delay: 1,
        max_fsync_delay: 5,
        partial_write_prob: 0.0,
    };
    for seed in 0..10 {
        let mut sim = ledger(seed, disk);
        sim.run_until(40);
        let sent: Vec<u64> = sends_of_node_1(&sim, 40)
            .into_iter()
            .map(|(_, k)| k)
            .collect();
        assert!(sent.len() >= 30, "seed {seed}: {sent:?}");
        assert_eq!(
            sent,
            (1..=sent.len() as u64).collect::<Vec<_>>(),
            "seed {seed}"
        );
    }
}

// Gecikmesiz disk: yazma, fsync ve ona bağlı mesaj aynı anda olur; çökme hiçbir şey kaybettirmez
// (kayıp kaydı yoktur).
#[test]
fn an_instant_disk_syncs_and_releases_at_once() {
    let mut sim = ledger(3, DiskConfig::instant());
    sim.run_until(4);
    assert_eq!(
        sends_of_node_1(&sim, 4),
        vec![(1, 1), (2, 2), (3, 3), (4, 4)]
    );
    sim.crash(NodeId(1)).expect("node 1 is up");
    assert_eq!(sim.disk(NodeId(1)), Some(&Count(4)));
    assert!(
        !sim.trace()
            .events()
            .iter()
            .any(|event| matches!(event.kind, TraceKind::CrashLoss { .. }))
    );
}

// İstemci cevabı (`Reply`) da dışarıya dönük bir çıktıdır: kendisinden önce verilmiş yazmalar
// kalıcı olana kadar tutulur (O1). fsync 3 tick sürerken düğüm 2'nin t=1, 2, 3'teki yazmaları t=4,
// 5, 6'da kalıcı olur. t=3'te verilen isteğin yazması da onların arkasında, t=6'da kalıcı olur;
// cevap ancak o an bırakılır ve trace'e girer.
#[test]
fn a_reply_waits_for_the_writes_before_it() {
    let mut sim = ledger(4, fixed(3));
    sim.run_until(3);
    sim.submit(NodeId(2), Count(42)).expect("node 2 is up");
    sim.run_until(5);
    assert!(sim.take_replies().is_empty(), "the reply is held");
    sim.run_until(6);
    assert_eq!(sim.take_replies(), vec![(NodeId(2), ())]);
    assert_eq!(
        times_of(&sim, |kind| matches!(
            kind,
            TraceKind::Reply {
                node: NodeId(2),
                ..
            }
        )),
        vec![6]
    );
}

// İstemci isteği düğüme ulaşır ve trace'e kaydedilir; ürettiği uygulama da sırası gelince sürücüye
// verilir. Çökmüş ya da var olmayan bir düğüme istek verilemez ve simülasyon değişmez.
#[test]
fn client_requests_reach_the_node_and_applies_reach_the_driver() {
    let mut sim = ledger(4, fixed(2));
    sim.run_until(3);
    let _ = sim.take_applied();
    sim.submit(NodeId(2), Count(42)).expect("node 2 is up");
    assert!(sim.trace().events().iter().any(|event| {
        event.kind
            == TraceKind::Client {
                node: NodeId(2),
                digest: digest(&Count(42)),
            }
    }));
    sim.run_until(10);
    let applied: Vec<(NodeId, Count)> = sim.take_applied();
    assert!(applied.contains(&(NodeId(2), Count(42))), "{applied:?}");
    assert!(sim.take_applied().is_empty(), "applies are taken once");

    sim.crash(NodeId(2)).expect("node 2 is up");
    let events = sim.trace().len();
    assert_eq!(
        sim.submit(NodeId(2), Count(7)),
        Err(LifecycleError::Down(NodeId(2)))
    );
    assert_eq!(
        sim.submit(NodeId(9), Count(7)),
        Err(LifecycleError::UnknownNode(NodeId(9)))
    );
    assert_eq!(sim.trace().len(), events, "failed requests leave no trace");
}
