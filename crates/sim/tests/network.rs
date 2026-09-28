//! Ağ testleri: kayıp, çoğaltma, gecikme sınırları ve "kablo kesildi" bölünmesi.

mod support;

use std::collections::BTreeMap;

use sim::{DropReason, NetworkConfig, NodeId};
use support::{DeliveryRecord, PingPongSim, cluster, count_drops, deliveries, drops, sends};

/// Testlerin koşturulduğu süre (tick).
const HORIZON: u64 = 300;

/// Her gönderimin (msg_id) teslimleri.
fn deliveries_by_msg(sim: &PingPongSim) -> BTreeMap<u64, Vec<DeliveryRecord>> {
    let mut by_msg: BTreeMap<u64, Vec<DeliveryRecord>> = BTreeMap::new();
    for delivery in deliveries(sim) {
        by_msg.entry(delivery.msg_id).or_default().push(delivery);
    }
    by_msg
}

/// Teslim edilmek için yeterince zamanı olmuş gönderimlerin numaraları (gönderim + en büyük
/// gecikme ≤ ufuk).
fn settled_msg_ids(sim: &PingPongSim, max_delay: u64) -> Vec<u64> {
    sends(sim)
        .into_iter()
        .filter(|send| send.at + max_delay <= HORIZON)
        .map(|send| send.msg_id)
        .collect()
}

// Kayıp: drop_prob = 1.0 iken hiçbir mesaj teslim edilmez ve her gönderim "rastgele kayıp" olarak
// düşer.
#[test]
fn drop_prob_one_delivers_nothing() {
    let config = NetworkConfig {
        drop_prob: 1.0,
        ..NetworkConfig::reliable(1)
    };
    let mut sim = cluster(5, 3, config);
    sim.run_until(HORIZON);
    let sent = sends(&sim);
    assert!(!sent.is_empty());
    assert!(deliveries(&sim).is_empty());
    let dropped = drops(&sim);
    assert_eq!(dropped.len(), sent.len());
    assert!(dropped.iter().all(|drop| drop.reason == DropReason::Random));
}

// Kayıp: drop_prob = 0.0 (ve çoğaltma yok) iken her mesaj tam olarak bir kez teslim edilir.
#[test]
fn drop_prob_zero_delivers_every_message_exactly_once() {
    let config = NetworkConfig {
        drop_prob: 0.0,
        duplicate_prob: 0.0,
        min_delay: 1,
        max_delay: 5,
    };
    let mut sim = cluster(5, 3, config);
    sim.run_until(HORIZON);
    let by_msg = deliveries_by_msg(&sim);
    let settled = settled_msg_ids(&sim, config.max_delay);
    assert!(!settled.is_empty());
    for msg_id in &settled {
        assert_eq!(by_msg.get(msg_id).map(Vec::len), Some(1), "msg {msg_id}");
    }
    assert!(drops(&sim).is_empty());
}

// Çoğaltma: duplicate_prob = 1.0 iken her mesaj en az iki kez (bu modelde tam iki kez) teslim
// edilir ve kopya BAĞIMSIZ bir gecikme alır: en az bir mesajın iki kopyası farklı anlarda varır.
#[test]
fn duplicate_prob_one_delivers_every_message_twice_with_independent_delays() {
    let config = NetworkConfig {
        drop_prob: 0.0,
        duplicate_prob: 1.0,
        min_delay: 1,
        max_delay: 5,
    };
    let mut sim = cluster(6, 3, config);
    sim.run_until(HORIZON);
    let by_msg = deliveries_by_msg(&sim);
    let settled = settled_msg_ids(&sim, config.max_delay);
    assert!(!settled.is_empty());
    let mut copies_at_different_times = 0;
    for msg_id in &settled {
        let copies = by_msg.get(msg_id).map(Vec::as_slice).unwrap_or_default();
        assert_eq!(
            copies.len(),
            2,
            "msg {msg_id} delivered {} time(s)",
            copies.len()
        );
        if copies[0].at != copies[1].at {
            copies_at_different_times += 1;
        }
    }
    assert!(
        copies_at_different_times > 0,
        "the copy must get its own delay, independent of the original"
    );
}

// Gecikme sınırları: her teslim [gönderim + min_delay, gönderim + max_delay] aralığındadır ve iki
// uç da gerçekten görülür. Değişken gecikme, sıra değişiminin kaynağıdır.
#[test]
fn delivery_times_stay_within_delay_bounds() {
    let config = NetworkConfig {
        drop_prob: 0.2,
        duplicate_prob: 0.3,
        min_delay: 3,
        max_delay: 9,
    };
    let mut sim = cluster(8, 4, config);
    sim.run_until(HORIZON);
    let delivered = deliveries(&sim);
    assert!(!delivered.is_empty());
    let (mut seen_min, mut seen_max) = (false, false);
    for delivery in delivered {
        let delay = delivery.at - delivery.sent_at;
        assert!((3..=9).contains(&delay), "delay {delay} out of bounds");
        seen_min |= delay == 3;
        seen_max |= delay == 9;
    }
    assert!(seen_min && seen_max, "both delay bounds should be reached");
}

// Gecikme sınırları: min_delay = max_delay iken her teslim tam o kadar gecikir.
#[test]
fn fixed_delay_is_exact() {
    let mut sim = cluster(8, 3, NetworkConfig::reliable(4));
    sim.run_until(100);
    let delivered = deliveries(&sim);
    assert!(!delivered.is_empty());
    assert!(delivered.iter().all(|d| d.at - d.sent_at == 4));
}

/// {1, 2} | {3, 4} bölünmesinde düğümün tarafı.
fn side(node: NodeId) -> u8 {
    if node.0 <= 2 { 0 } else { 1 }
}

// Bölünme: bölünme sürerken gruplar arası HİÇBİR mesaj teslim edilmez, HER grubun kendi içindeki
// trafik sürer; heal sonrası gruplar arası trafik yeniden akar.
#[test]
fn partition_blocks_cross_group_traffic_until_heal() {
    let mut sim = cluster(21, 4, NetworkConfig::reliable(2));
    sim.run_until(10);
    sim.partition(&[&[NodeId(1), NodeId(2)], &[NodeId(3), NodeId(4)]])
        .expect("valid partition");
    sim.run_until(30);
    sim.heal();
    sim.run_until(50);

    let during = |at: u64| at > 10 && at <= 30;
    let delivered = deliveries(&sim);
    assert!(
        delivered
            .iter()
            .all(|d| !(during(d.at) && side(d.from) != side(d.to))),
        "no cross-group delivery while partitioned"
    );
    for group in [0, 1] {
        assert!(
            delivered
                .iter()
                .any(|d| during(d.at) && side(d.from) == group && side(d.to) == group),
            "traffic inside group {group} continues"
        );
    }
    assert!(
        delivered
            .iter()
            .any(|d| d.sent_at > 30 && side(d.from) != side(d.to)),
        "cross-group traffic resumes after heal"
    );
    assert!(count_drops(&sim, DropReason::PartitionAtSend) > 0);
}

// Yoldaki mesajlar: bölünme başladığı anda kesilen bağlantıda yolda olan her mesaj kaybolur.
// 2 düğüm ve 10 tick'lik sabit gecikme: t = 1..=5'te gönderilen 10 Ping t = 11..=15'te varacaktı ve
// hepsi yoldayken bölünme başlar.
#[test]
fn in_flight_messages_are_lost_when_a_partition_starts() {
    let mut sim = cluster(4, 2, NetworkConfig::reliable(10));
    sim.run_until(5);
    sim.partition(&[&[NodeId(1)], &[NodeId(2)]])
        .expect("valid partition");
    sim.run_until(20);

    let lost: Vec<_> = drops(&sim)
        .into_iter()
        .filter(|drop| drop.reason == DropReason::PartitionInFlight)
        .collect();
    assert_eq!(
        lost.len(),
        10,
        "both nodes' pings sent at t = 1..=5 were in flight"
    );
    assert!(lost.iter().all(|drop| drop.sent_at <= 5 && drop.at > 5));
    assert!(deliveries(&sim).is_empty(), "nothing crosses the cut");
}

// "Kablo kesildi": yoldaki mesaj, bölünme teslim anından ÖNCE iyileşse bile kaybolur. Kesik bir
// bağlantıdaki paket, bağlantı sonradan onarılınca geri gelmez. t = 1..=5'teki 10 Ping t =
// 11..=15'te varacaktı; bölünme t = 5'te başlayıp t = 7'de biter. t = 6..=7'deki 4 Ping gönderimde
// düşer; t ≥ 8'de gönderilenler ulaşır.
#[test]
fn in_flight_messages_are_lost_even_if_the_partition_heals_before_they_arrive() {
    let mut sim = cluster(4, 2, NetworkConfig::reliable(10));
    sim.run_until(5);
    sim.partition(&[&[NodeId(1)], &[NodeId(2)]])
        .expect("valid partition");
    sim.run_until(7);
    sim.heal();
    sim.run_until(20);

    assert_eq!(count_drops(&sim, DropReason::PartitionInFlight), 10);
    assert_eq!(count_drops(&sim, DropReason::PartitionAtSend), 4);
    let delivered = deliveries(&sim);
    assert!(!delivered.is_empty(), "pings sent after heal arrive");
    assert!(delivered.iter().all(|d| d.sent_at >= 8));
}

// Bölünme sırasında gönderilen mesaj, teslim zamanı heal'den SONRAYA düşse bile hiç ulaşmaz
// (gönderim anında düşer). heal'den sonra gönderilenler ise ulaşır.
#[test]
fn messages_sent_during_a_partition_never_arrive_even_after_heal() {
    let mut sim = cluster(4, 2, NetworkConfig::reliable(10));
    sim.partition(&[&[NodeId(1)], &[NodeId(2)]])
        .expect("valid partition");
    sim.run_until(5);
    sim.heal();
    sim.run_until(40);

    let delivered = deliveries(&sim);
    assert!(
        delivered.iter().all(|d| d.sent_at > 5),
        "messages sent during the partition never arrive"
    );
    assert!(!delivered.is_empty(), "messages sent after heal do arrive");
    assert_eq!(
        count_drops(&sim, DropReason::PartitionAtSend),
        10,
        "both nodes' pings at t = 1..=5 were blocked at send"
    );
}
