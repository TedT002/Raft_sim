//! Determinizm testleri: aynı seed aynı koşu, farklı seed farklı koşu, eşit zamanlı olayların
//! sırası, alt-seed bağımsızlığı ve "altın" trace özeti.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use sim::{Component, DropReason, NodeId, SeedTree, TraceKind, uniform_inclusive};
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
// Senaryo trace kodlamasının her parçasına dokunur: her olay türü (Tick, Send, Deliver, Drop,
// Partition, Heal) ve simülasyonda ulaşılabilen her düşme nedeni. Ping/Pong test protokolü de
// sabitlenen senaryonun parçasıdır: `support` modülündeki bir değişiklik bu değeri değiştirirse, bu
// da bilinçli bir güncelleme gerektirir ("değeri güncelle" refleksiyle geçiştirilmemeli).
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
