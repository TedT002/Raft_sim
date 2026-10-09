//! Snapshot'lar ve log sıkıştırma (§7): her düğüm durum makinesi belli sayıda girdi ilerledikçe
//! snapshot alır ve log'unun o önekini atar. Liderin log'undan atılmış girdilere ihtiyacı olan
//! takipçi liderden snapshot kurar (Figure 13); yeniden başlayan bir düğüm kendi diskindeki
//! snapshot'tan açılır. Her iki durumda da durum makinesi, girdileri tek tek uygulamış kopyalarla
//! aynı olmalıdır (snapshot güvenliği; bkz. `RaftCluster`).
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır.

use std::num::NonZeroU64;
use std::ops::Range;

use sim::{
    ClusterConfig, DiskConfig, KvCommand, NetworkConfig, NodeId, RaftCluster, RunStats, Scenario,
    ScenarioConfig, run,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`).
const T: u64 = 20;

/// `snapshots` profilinde taranan seed'ler.
const SNAPSHOT_SEEDS: Range<u64> = 0..100;

/// Gecikmesiz diskli, güvenilir ağlı (gecikme 1), her 4 girdide snapshot alan 3 düğümlü bir
/// küme.
fn compacting_cluster(seed: u64) -> RaftCluster {
    let config = ClusterConfig {
        disk: DiskConfig::instant(),
        snapshot_every: NonZeroU64::new(4),
        ..ClusterConfig::new(3, NetworkConfig::reliable(1))
    };
    RaftCluster::new(seed, config).expect("valid config")
}

/// Bir tick ilerler; bir invariant çiğnenirse bildirir.
fn tick(cluster: &mut RaftCluster) {
    let next = cluster.now() + 1;
    if let Err(error) = cluster.run_until(next) {
        panic!("{error}");
    }
}

/// Koşul sağlanana kadar birer tick ilerler; `max_ticks` içinde sağlanmazsa testi düşürür.
fn wait_for(
    cluster: &mut RaftCluster,
    max_ticks: u64,
    what: &str,
    mut condition: impl FnMut(&RaftCluster) -> bool,
) {
    let deadline = cluster.now() + max_ticks;
    while !condition(cluster) {
        assert!(
            cluster.now() < deadline,
            "{what} did not happen within {max_ticks} ticks"
        );
        tick(cluster);
    }
}

/// Ayaktaki tek lider.
fn sole_leader(cluster: &mut RaftCluster) -> NodeId {
    wait_for(cluster, 20 * T, "a single leader", |c| {
        c.leaders().len() == 1
    });
    cluster.leaders()[0].0
}

/// `k` anahtarına `count` ekleme yazdırır ve hepsi liderde uygulanana kadar bekler.
fn append_many(cluster: &mut RaftCluster, leader: NodeId, count: u8) {
    let applied = |c: &RaftCluster| c.node(leader).map_or(0, |node| node.last_applied().0);
    let target = applied(cluster) + u64::from(count);
    for byte in 0..count {
        cluster
            .submit(
                leader,
                KvCommand::Append {
                    key: b"k".to_vec(),
                    value: vec![b'a' + byte % 26],
                },
            )
            .expect("the leader is up");
    }
    wait_for(cluster, 10 * T, "the writes to apply", |c| {
        applied(c) >= target
    });
}

// Bölünmede geride kalan bir takipçi: lider bu arada pek çok girdi uygular ve log'unu sıkıştırır.
// İyileşmeden sonra takipçinin ihtiyaç duyduğu girdiler liderin log'unda yoktur; takipçi
// liderden snapshot kurar ve durum makinesi liderinkiyle aynı olur. Kurulan snapshot sayılır.
#[test]
fn a_lagging_follower_catches_up_through_a_snapshot() {
    let mut cluster = compacting_cluster(21);
    let leader = sole_leader(&mut cluster);
    let all: Vec<NodeId> = cluster.node_ids().collect();
    let lagging = all
        .iter()
        .copied()
        .find(|&id| id != leader)
        .expect("a follower");
    let rest: Vec<NodeId> = all.iter().copied().filter(|&id| id != lagging).collect();
    cluster
        .partition(&[&[lagging], &rest])
        .expect("a valid partition");
    append_many(&mut cluster, leader, 20);
    let compacted = cluster
        .node(leader)
        .and_then(|node| node.snapshot())
        .map(|snapshot| snapshot.last_index);
    assert!(
        compacted.is_some_and(|index| index.0 >= 16),
        "the leader compacted its log: {compacted:?}"
    );

    cluster.heal();
    wait_for(&mut cluster, 20 * T, "the follower to catch up", |c| {
        c.node(lagging).map(|node| node.last_applied())
            == c.node(leader).map(|node| node.last_applied())
            && c.kv(lagging) == c.kv(leader)
    });
    assert!(cluster.installs() >= 1, "the follower installed a snapshot");
    assert!(
        cluster
            .node(lagging)
            .is_some_and(|node| node.snapshot().is_some()),
        "the follower holds a snapshot now"
    );
    assert_eq!(
        cluster
            .kv(lagging)
            .and_then(|kv| kv.get(b"k"))
            .map(<[u8]>::len),
        Some(20)
    );
}

// Kendi log'unu sıkıştırmış bir takipçi çöküp kalkar: durum makinesi diskindeki snapshot'tan
// kurulur (girdiler 1'den yeniden uygulanmaz) ve küme yeni yazmalarla birlikte yakınsar.
#[test]
fn a_node_restarts_from_its_own_snapshot() {
    let mut cluster = compacting_cluster(22);
    let leader = sole_leader(&mut cluster);
    append_many(&mut cluster, leader, 12);
    let follower = cluster
        .node_ids()
        .find(|&id| id != leader)
        .expect("a follower");
    wait_for(&mut cluster, 10 * T, "the follower to compact", |c| {
        c.node(follower)
            .is_some_and(|node| node.snapshot().is_some())
    });
    let snapshot_index = cluster
        .node(follower)
        .and_then(|node| node.snapshot())
        .map(|snapshot| snapshot.last_index)
        .expect("a snapshot");

    cluster.crash(follower).expect("the follower is up");
    cluster.restart(follower).expect("the follower is down");
    let node = cluster.node(follower).expect("the follower exists");
    assert!(
        node.last_applied() >= snapshot_index,
        "the node starts from its snapshot"
    );
    assert!(
        cluster
            .kv(follower)
            .and_then(|kv| kv.get(b"k"))
            .is_some_and(|value| !value.is_empty()),
        "the state machine is restored from the snapshot, not rebuilt from an empty log"
    );

    append_many(&mut cluster, leader, 4);
    wait_for(&mut cluster, 20 * T, "the replicas to converge", |c| {
        c.kv(follower) == c.kv(leader)
    });
}

// `snapshots` profilinin taraması (`raftsim fuzz --profile snapshots` ile aynı program): her seed
// güvenlik invariant'larını, snapshot güvenliğini ve istemci geçmişinin linearizability'sini
// korur. Snapshot yolu gerçekten sınanır: düğümler log'larını sık sık sıkıştırır ve geride kalan
// düğümler liderden snapshot kurar.
#[test]
fn snapshots_stay_safe_across_seeds() {
    let mut failures = Vec::new();
    let mut totals = RunStats::default();
    for seed in SNAPSHOT_SEEDS {
        match run(&Scenario::generate(seed, ScenarioConfig::snapshots())).outcome {
            Ok(stats) => {
                totals.compactions += stats.compactions;
                totals.installs += stats.installs;
                totals.clients += stats.clients;
            }
            Err(error) => failures.push(format!(
                "seed {seed}: {error}\n  reproduce: cargo run -p cli -- replay --seed {seed} \
                 --profile snapshots"
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} seeds failed:\n{}",
        failures.len(),
        SNAPSHOT_SEEDS.count(),
        failures.join("\n")
    );
    // Eşikler ölçülen değerlerin (2136 sıkıştırma, liderden kurulan 620 snapshot, tamamlanan 8052
    // işlem) çok altındadır.
    let seeds = SNAPSHOT_SEEDS.count() as u64;
    assert!(totals.compactions >= 5 * seeds, "{totals:?}");
    assert!(totals.installs >= seeds, "{totals:?}");
    assert!(totals.clients.completed >= 20 * seeds, "{totals:?}");
}
