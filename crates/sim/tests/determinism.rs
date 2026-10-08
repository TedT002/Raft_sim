//! Determinizm testleri: aynı seed aynı koşu, farklı seed farklı koşu, eşit zamanlı olayların
//! sırası, alt-seed bağımsızlığı ve "altın" trace özeti.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use sim::{
    ClusterConfig, Component, DropReason, KvCommand, KvRequest, NodeId, RaftCluster, Role,
    SeedTree, TraceEvent, TraceKind, uniform_inclusive,
};
use support::{LOSSY, cluster, cluster_with_ghost_peers, cluster_with_network_rng, count_drops};

// Determinizm: aynı seed ve aynı ayarlarla iki bağımsız koşu, olay olay aynı trace'i ve aynı özeti
// üretmeli. Simülatörün bütün değeri buna dayanır: başarısız bir seed yeniden koşulduğunda aynı
// hatayı aynı adımda göstermelidir.
#[test]
fn same_seed_same_trace() {
    let mut first = cluster(7, 5, LOSSY);
    let mut second = cluster(7, 5, LOSSY);
    first.run_until(500);
    second.run_until(500);
    assert!(!first.trace().is_empty());
    assert_eq!(first.trace().events(), second.trace().events());
    assert_eq!(first.trace_hash(), second.trace_hash());
}

// Seed duyarlılığı: ilk 20 seed'in özetlerinin çoğu farklı olmalı. Hepsi aynı çıksaydı, seed
// rastgeleliğe hiç ulaşmıyor demektir (ör. RNG'ler sabit bir seed'le kuruluyor).
#[test]
fn different_seeds_give_different_traces() {
    let hashes: BTreeSet<u64> = (0..20)
        .map(|seed| {
            let mut sim = cluster(seed, 3, LOSSY);
            sim.run_until(200);
            sim.trace_hash()
        })
        .collect();
    assert!(
        hashes.len() >= 18,
        "only {} distinct hashes out of 20 seeds",
        hashes.len()
    );
}

// Sıralama, simülasyon düzeyinde: aynı anda tick alan düğümler her zaman kuyruğa eklendikleri
// sırayla (burada NodeId sırasıyla) işlenmeli. Olay kuyruğundaki `seq` kaldırılırsa bu sıra heap'in
// iç düzenine kalır ve test kırılır.
#[test]
fn equal_time_ticks_are_processed_in_insertion_order() {
    let mut sim = cluster(3, 6, LOSSY);
    sim.run_until(100);
    let mut ticks_by_time: BTreeMap<u64, Vec<NodeId>> = BTreeMap::new();
    for event in sim.trace().events() {
        if let TraceKind::Tick { node } = event.kind {
            ticks_by_time.entry(event.time).or_default().push(node);
        }
    }
    let expected: Vec<NodeId> = (1..=6).map(NodeId).collect();
    assert_eq!(ticks_by_time.len(), 100);
    for (time, nodes) in ticks_by_time {
        assert_eq!(nodes, expected, "tick order at t={time}");
    }
}

// Alt-seed bağımsızlığı: ağ bileşeninin akışına TEK bir fazladan çekiliş eklemek ağın kararlarını
// değiştirir (trace farklıdır), ama düğümlerin kendi kararlarını (her tick'te hangi eşe Ping
// yolladıklarını) DEĞİŞTİRMEZ; çünkü her düğüm kendi alt-seed'inden türetilen ayrı bir akış
// kullanır.
#[test]
fn an_extra_network_draw_does_not_change_node_decisions() {
    let seeds = SeedTree::new(99);
    let mut baseline = cluster(99, 4, LOSSY);

    let mut shifted_network = seeds.rng_for(Component::Network);
    let _ = uniform_inclusive(&mut shifted_network, 0, 0); // tam olarak bir fazladan çekiliş
    let mut shifted = cluster_with_network_rng(99, 4, LOSSY, shifted_network);

    baseline.run_until(300);
    shifted.run_until(300);

    assert_ne!(
        baseline.trace_hash(),
        shifted.trace_hash(),
        "the extra draw must change the network's decisions"
    );
    for id in (1..=4).map(NodeId) {
        let expected = &baseline.node(id).expect("node exists").pings_sent;
        let actual = &shifted.node(id).expect("node exists").pings_sent;
        assert!(!expected.is_empty());
        assert_eq!(expected, actual, "node {id:?} must pick the same peers");
    }
}

// "Altın" özet: sabit bir senaryonun trace özeti dondurulmuştur. Seed türetimi, ağın çekiliş
// sırası, trace kodlaması ya da bir bağımlılık (ör. rand_chacha) istemeden değişirse bu test
// kırılır. Değişiklik bilinçliyse, yayımlanmış seed'lerin artık başka koşular ürettiği kabul edilir
// ve değer güncellenir.
//
// Senaryo Faz 1'in trace kodlamasının her parçasına dokunur: o fazın her olay türü (Tick, Send,
// Deliver, Drop, Partition, Heal) ve çökme olmadan ulaşılabilen her düşme nedeni. Faz 2'nin yaşam
// döngüsü olayları (Persist, Crash, Restart, NodeDown) Raft altın senaryosunda sabitlenir; bu
// senaryo bilerek değiştirilmedi, Faz 1'in değeri olduğu gibi kalsın diye. Ping/Pong test protokolü
// de sabitlenen senaryonun parçasıdır: `support` modülündeki bir değişiklik bu değeri değiştirirse,
// bu da bilinçli bir güncelleme gerektirir ("değeri güncelle" refleksiyle geçiştirilmemeli).
#[test]
fn golden_trace_hash_is_stable() {
    let mut sim = cluster_with_ghost_peers(1, 3, &[NodeId(99)], LOSSY);
    sim.run_until(60);
    sim.partition(&[&[NodeId(1)], &[NodeId(2), NodeId(3)]])
        .expect("valid partition");
    sim.run_until(90);
    sim.heal();
    sim.run_until(200);

    // Senaryo gerçekten her nedeni içermeli; yoksa "sabitlenen" şey eksik kalırdı.
    for reason in [
        DropReason::Random,
        DropReason::PartitionAtSend,
        DropReason::PartitionInFlight,
        DropReason::UnknownDestination,
    ] {
        assert!(
            count_drops(&sim, reason) > 0,
            "the pinned scenario must exercise {reason:?}"
        );
    }
    assert_eq!(sim.trace().len(), 2528);
    assert_eq!(
        sim.trace_hash(),
        0xad97_5436_c53d_becd,
        "trace hash of the pinned scenario changed"
    );
}

/// Ayaktaki lidere (birden fazla varsa en yüksek term'liye) KV komutları verir ve lideri döndürür;
/// lider yoksa hiçbir şey yapmaz.
fn submit_to_leader(cluster: &mut RaftCluster, keys: &[&str]) -> Option<NodeId> {
    let (leader, _) = cluster
        .leaders()
        .into_iter()
        .max_by_key(|&(_, term)| term)?;
    for key in keys {
        let command = KvCommand::Put {
            key: key.as_bytes().to_vec(),
            value: b"v".to_vec(),
        };
        cluster.submit(leader, command).expect("no violation");
    }
    Some(leader)
}

/// Sabit bir Raft senaryosu: 5 düğüm kayıplı ağda ve varsayılan diskle (fsync 1..3 tick). 60.
/// tick'te lidere üç komut verilir. 80'de lidere bir komut daha verilir ve lider, o komutun yazması
/// fsync'i beklerken çöker (bekleyen yazma ve tutulan mesajlar kaybolur). 120'de ağ {1,3} | {4,5}
/// diye bölünür, 200'de iyileşir, 220'de çökmüş düğümler yeniden başlar, 300'de iki komut daha
/// verilir ve bir istemci, lider olmayan bir düğüme istek verir (`NotLeader` cevabı); koşu 400'e
/// kadar sürer. Her olaydan sonra invariant'lar denetlenir.
fn raft_scenario(seed: u64) -> RaftCluster {
    let mut cluster = RaftCluster::new(seed, ClusterConfig::new(5, LOSSY)).expect("valid config");
    cluster.run_until(60).expect("no violation");
    let _ = submit_to_leader(&mut cluster, &["a", "b", "c"]);
    cluster.run_until(80).expect("no violation");
    if let Some(leader) = submit_to_leader(&mut cluster, &["d"]) {
        cluster.crash(leader).expect("the leader is up");
    }
    cluster.run_until(120).expect("no violation");
    cluster
        .partition(&[&[NodeId(1), NodeId(3)], &[NodeId(4), NodeId(5)]])
        .expect("valid partition");
    cluster.run_until(200).expect("no violation");
    cluster.heal();
    cluster.run_until(220).expect("no violation");
    for id in (1..=5).map(NodeId) {
        if !cluster.is_up(id) {
            cluster.restart(id).expect("the node is down");
        }
    }
    cluster.run_until(300).expect("no violation");
    let _ = submit_to_leader(&mut cluster, &["e", "f"]);
    let follower = (1..=5).map(NodeId).find(|&id| {
        cluster
            .node(id)
            .is_some_and(|node| node.role() != Role::Leader)
    });
    if let Some(follower) = follower {
        let request = KvRequest {
            client: 1,
            seq: 1,
            command: KvCommand::Get { key: b"e".to_vec() },
        };
        cluster
            .submit_request(follower, request)
            .expect("the node is up");
    }
    cluster.run_until(400).expect("no violation");
    cluster
}

// Raft koşuları da deterministiktir: aynı seed ve aynı hata programı olay olay aynı trace'i ve
// aynı özeti verir. Farklı seed'ler farklı koşular üretir.
#[test]
fn raft_runs_are_deterministic() {
    let first = raft_scenario(11);
    let second = raft_scenario(11);
    assert_eq!(first.sim().trace().events(), second.sim().trace().events());
    assert_eq!(first.sim().trace_hash(), second.sim().trace_hash());
    let hashes: BTreeSet<u64> = (0..10)
        .map(|seed| raft_scenario(seed).sim().trace_hash())
        .collect();
    assert_eq!(hashes.len(), 10);
}

// Raft için "altın" özet: sabit senaryonun trace özeti dondurulmuştur. Raft mantığı, mesaj ya da
// disk kodlaması, seed türetimi ya da simülatörün olay sırası istemeden değişirse bu test kırılır.
// Değişiklik bilinçliyse değer bilinçli olarak güncellenir (yayımlanmış seed'ler artık başka
// koşular üretir).
//
// Senaryo Faz 2-4'ün trace olaylarının hepsine dokunur: yazma (Persist) ve fsync (Sync), çökme ve
// çökmenin kaybettirdikleri (CrashLoss), yeniden başlatma, kapalı düğüme giden mesajların düşüşü
// (NodeDown), istemci komutları (Client), uygulamalar (Apply) ve istemci cevapları (Reply).
// Komutlar commit edilip uygulanmış olmalı: aksi hâlde senaryo log replikasyonunu sabitlemezdi.
#[test]
fn golden_raft_trace_hash_is_stable() {
    let cluster = raft_scenario(1);
    let events = cluster.sim().trace().events();
    let has = |wanted: fn(&TraceEvent) -> bool| events.iter().any(wanted);
    assert!(has(|e| matches!(e.kind, TraceKind::Persist { .. })));
    assert!(has(|e| matches!(e.kind, TraceKind::Sync { .. })));
    assert!(has(|e| matches!(e.kind, TraceKind::Crash { .. })));
    assert!(has(|e| matches!(e.kind, TraceKind::CrashLoss { .. })));
    assert!(has(|e| matches!(e.kind, TraceKind::Restart { .. })));
    assert!(has(|e| matches!(e.kind, TraceKind::Client { .. })));
    assert!(has(|e| matches!(e.kind, TraceKind::Apply { .. })));
    assert!(has(|e| matches!(e.kind, TraceKind::Reply { .. })));
    assert!(has(|e| matches!(
        e.kind,
        TraceKind::Drop {
            reason: DropReason::NodeDown,
            ..
        }
    )));
    assert!(!cluster.elections().is_empty());
    assert_eq!(cluster.sim().trace().len(), 3486);
    assert_eq!(
        cluster.sim().trace_hash(),
        0xf865_89f8_cba6_1210,
        "trace hash of the pinned Raft scenario changed"
    );
}
