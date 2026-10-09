//! Log'a yazılmayan okumalar (ReadIndex, tezin §6.4'ü): lider bir okumayı, okuma geldikten sonra
//! başlattığı bir doğrulama turunu çoğunluğa onaylatınca ve durum makinesi okumanın `readIndex`'ine
//! kadar uygulandığında, kendi durum makinesinden cevaplar. Okuma log'a girmez; yine de doğrusal
//! (linearizable) olmalıdır: okuma gelmeden önce tamamlanmış her yazmayı görmelidir.
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır.

use std::ops::Range;

use sim::{
    ClientReply, ClusterConfig, DiskConfig, KvCommand, KvRequest, KvResult, NetworkConfig, NodeId,
    RaftCluster, ReplyOutcome, Role, RunStats, Scenario, ScenarioConfig, run,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`).
const T: u64 = 20;

/// `reads` profilinde taranan seed'ler.
const READ_SEEDS: Range<u64> = 0..100;

/// Gecikmesiz diskli, güvenilir ağlı (gecikme 1) `size` düğümlü bir küme.
fn quiet_cluster(seed: u64, size: u64) -> RaftCluster {
    let config = ClusterConfig {
        disk: DiskConfig::instant(),
        ..ClusterConfig::new(size, NetworkConfig::reliable(1))
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
    mut condition: impl FnMut(&mut RaftCluster) -> bool,
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

/// `among` içindeki ayaktaki düğümlerden tek bir lider çıkana ve no-op'u onlarda commit edilene
/// kadar bekler.
fn settled_leader(cluster: &mut RaftCluster, among: &[NodeId]) -> NodeId {
    wait_for(cluster, 20 * T, "a settled leader", |c| {
        let leaders: Vec<NodeId> = c
            .leaders()
            .into_iter()
            .map(|(id, _)| id)
            .filter(|id| among.contains(id))
            .collect();
        leaders.len() == 1
            && leaders.iter().all(|&leader| {
                c.node(leader).is_some_and(|node| {
                    node.log().last().map(|entry| entry.term) == Some(node.current_term())
                        && node.commit_index().0 == node.log().len() as u64
                })
            })
    });
    cluster
        .leaders()
        .into_iter()
        .map(|(id, _)| id)
        .find(|id| among.contains(id))
        .expect("a leader among the given nodes")
}

/// `(client, seq)` için bir cevap gelene kadar bekler ve onu döndürür.
fn await_reply(cluster: &mut RaftCluster, client: u64, seq: u64) -> ClientReply {
    let mut found = None;
    wait_for(cluster, 10 * T, "a reply", |c| {
        if found.is_none() {
            found = c
                .take_client_replies()
                .into_iter()
                .find(|reply| reply.client == client && reply.seq == seq);
        }
        found.is_some()
    });
    found.expect("the reply arrived")
}

/// Bir yazmayı verir ve tamamlanmasını bekler.
fn write(cluster: &mut RaftCluster, node: NodeId, client: u64, seq: u64, command: KvCommand) {
    let request = KvRequest {
        client,
        seq,
        command,
    };
    cluster
        .submit_request(node, request)
        .expect("the node is up");
    let reply = await_reply(cluster, client, seq);
    assert!(
        matches!(reply.outcome, ReplyOutcome::Done { .. }),
        "{reply:?}"
    );
}

fn value(reply: &ClientReply) -> Option<Option<Vec<u8>>> {
    match &reply.outcome {
        ReplyOutcome::Done {
            result: KvResult::Value(value),
            ..
        } => Some(value.clone()),
        _ => None,
    }
}

// Lider okumayı log'a yazmadan cevaplar ve okuma, okumadan önce tamamlanmış her yazmayı görür
// (`Put`, sonra `Append`). Okumalar log'u uzatmaz. Lider olmayan bir düğüm okumayı lider ipucuyla
// reddeder.
#[test]
fn index_reads_see_every_completed_write() {
    let mut cluster = quiet_cluster(11, 3);
    let all: Vec<NodeId> = cluster.node_ids().collect();
    let leader = settled_leader(&mut cluster, &all);
    let key = b"k".to_vec();
    write(
        &mut cluster,
        leader,
        1,
        1,
        KvCommand::Put {
            key: key.clone(),
            value: b"v1".to_vec(),
        },
    );
    let log_length = cluster.node(leader).map(|node| node.log().len());
    cluster
        .submit_read(leader, 1, 2, key.clone())
        .expect("the leader is up");
    let reply = await_reply(&mut cluster, 1, 2);
    assert_eq!(value(&reply), Some(Some(b"v1".to_vec())), "{reply:?}");
    assert_eq!(
        cluster.node(leader).map(|node| node.log().len()),
        log_length,
        "a read does not touch the log"
    );

    write(
        &mut cluster,
        leader,
        1,
        3,
        KvCommand::Append {
            key: key.clone(),
            value: b"+x".to_vec(),
        },
    );
    cluster
        .submit_read(leader, 1, 4, key.clone())
        .expect("the leader is up");
    let reply = await_reply(&mut cluster, 1, 4);
    assert_eq!(value(&reply), Some(Some(b"v1+x".to_vec())), "{reply:?}");

    let follower = all
        .iter()
        .copied()
        .find(|&id| id != leader)
        .expect("a follower");
    cluster
        .submit_read(follower, 2, 1, key)
        .expect("the follower is up");
    let reply = await_reply(&mut cluster, 2, 1);
    assert_eq!(
        reply.outcome,
        ReplyOutcome::NotLeader { hint: Some(leader) }
    );
}

// Tezin §6.4'ünün korumak istediği durum: bölünmede azınlıkta kalan eski lider kendini hâlâ lider
// sanır, çoğunluk ise yeni bir lider seçip yeni bir değer yazar. Eski liderin doğrulama turu
// çoğunluğa ulaşamadığı için okuma bölünme boyunca HİÇ cevaplanmaz (eski değeri döndürseydi okuma
// doğrusal olmazdı: yazma okumadan önce tamamlandı). Bölünme kalkınca eski lider daha yüksek
// term'i görür, liderliği bırakır ve bekleyen okumayı `NotLeader` ile reddeder.
#[test]
fn a_deposed_leader_never_answers_a_read() {
    let mut cluster = quiet_cluster(12, 5);
    let all: Vec<NodeId> = cluster.node_ids().collect();
    let old = settled_leader(&mut cluster, &all);
    let key = b"k".to_vec();
    write(
        &mut cluster,
        old,
        1,
        1,
        KvCommand::Put {
            key: key.clone(),
            value: b"old".to_vec(),
        },
    );

    let majority: Vec<NodeId> = all.iter().copied().filter(|&id| id != old).collect();
    cluster
        .partition(&[&[old], &majority])
        .expect("a valid partition");
    let new = settled_leader(&mut cluster, &majority);
    write(
        &mut cluster,
        new,
        2,
        1,
        KvCommand::Put {
            key: key.clone(),
            value: b"new".to_vec(),
        },
    );
    assert_eq!(
        cluster.node(old).map(|node| node.role()),
        Some(Role::Leader),
        "the old leader does not know it was deposed"
    );

    cluster
        .submit_read(old, 3, 1, key.clone())
        .expect("the old leader is up");
    for _ in 0..10 * T {
        tick(&mut cluster);
        let replies = cluster.take_client_replies();
        assert!(
            replies.iter().all(|reply| reply.client != 3),
            "the deposed leader answered a read during the partition: {replies:?}"
        );
    }
    cluster.heal();
    let reply = await_reply(&mut cluster, 3, 1);
    assert!(
        matches!(reply.outcome, ReplyOutcome::NotLeader { .. }),
        "{reply:?}"
    );
    assert_ne!(
        cluster.node(old).map(|node| node.role()),
        Some(Role::Leader)
    );
    // İyileşmeden sonra okuma güncel lidere gider ve yeni değeri görür.
    let leader = settled_leader(&mut cluster, &all);
    cluster
        .submit_read(leader, 3, 2, key)
        .expect("the leader is up");
    let reply = await_reply(&mut cluster, 3, 2);
    assert_eq!(value(&reply), Some(Some(b"new".to_vec())), "{reply:?}");
}

// `reads` profilinin taraması (`raftsim fuzz --profile reads` ile aynı program): okumaların çoğu
// log'a yazılmadan cevaplanır, liderler sık devrilir ve ağ bölünür. Her seed güvenlik
// invariant'larını ve istemci geçmişinin linearizability'sini korur. Başarısız her seed, yeniden
// üretme komutuyla raporlanır.
#[test]
fn reads_stay_linearizable_across_seeds() {
    let mut failures = Vec::new();
    let mut index_reads = 0;
    let mut completed = 0;
    for seed in READ_SEEDS {
        match run(&Scenario::generate(seed, ScenarioConfig::reads())).outcome {
            Ok(RunStats { clients, .. }) => {
                index_reads += clients.index_reads;
                completed += clients.completed;
            }
            Err(error) => failures.push(format!(
                "seed {seed}: {error}\n  reproduce: cargo run -p cli -- replay --seed {seed} \
                 --profile reads"
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} seeds failed:\n{}",
        failures.len(),
        READ_SEEDS.count(),
        failures.join("\n")
    );
    // Okuma yolu gerçekten sınandı: okumaların önemli bir kısmı log'a yazılmadan cevaplandı.
    // Eşikler ölçülen değerlerin (tamamlanan 4674 işlemin 1691'i ReadIndex okuması) çok
    // altındadır.
    let seeds = READ_SEEDS.count() as u64;
    assert!(index_reads >= 5 * seeds, "{index_reads} index reads");
    assert!(completed >= 20 * seeds, "{completed} completed operations");
}
