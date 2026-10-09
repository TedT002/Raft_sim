//! Lider kiralaması (tezin §6.4.1): lider, çoğunluğun onayladığı son doğrulama turunun
//! gönderiminden sonraki bir süre boyunca okumaları tur beklemeden cevaplar. Güvenliği iki şeye
//! dayanır: takipçiler liderden haber aldıktan sonraki T tick boyunca başka bir adaya oy vermez
//! (§4.2.3) ve saatler bir kiralama süresinde `T - lease` tick'ten fazla ayrışmaz. İkincisi bir
//! varsayımdır: bozulduğunda kiralama gerçekten bayat okuma verir, ReadIndex vermez.
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır.

use std::ops::Range;

use sim::{
    ClusterConfig, DiskConfig, KvCommand, KvResult, NetworkConfig, NodeId, RaftCluster, RaftConfig,
    ReplyOutcome, Role, RunStats, Scenario, ScenarioConfig, run,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`).
const T: u64 = 20;

/// Kiralama süresi (tick): `T - 4` tick'lik pay saat sapmasına ayrılır.
const LEASE: u64 = 16;

/// `leases` profilinde taranan seed'ler.
const LEASE_SEEDS: Range<u64> = 0..100;

/// Gecikmesiz diskli, güvenilir ağlı (gecikme 1) 3 düğümlü bir küme; istenirse kiralamalı.
fn cluster(seed: u64, lease: bool) -> RaftCluster {
    let raft = if lease {
        RaftConfig::default().with_lease(LEASE).expect("below T")
    } else {
        RaftConfig::default()
    };
    let config = ClusterConfig {
        disk: DiskConfig::instant(),
        raft,
        ..ClusterConfig::new(3, NetworkConfig::reliable(1))
    };
    RaftCluster::new(seed, config).expect("valid config")
}

/// Gecikmesiz diskli, güvenilir ağlı (gecikme 1), kiralamalı 5 düğümlü bir küme.
fn five_node_cluster(seed: u64) -> RaftCluster {
    let config = ClusterConfig {
        disk: DiskConfig::instant(),
        raft: RaftConfig::default().with_lease(LEASE).expect("below T"),
        ..ClusterConfig::new(5, NetworkConfig::reliable(1))
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

fn put(value: &[u8]) -> KvCommand {
    KvCommand::Put {
        key: b"k".to_vec(),
        value: value.to_vec(),
    }
}

/// `node`'un durum makinesinde `k`'nin değeri.
fn value_at(cluster: &RaftCluster, node: NodeId) -> Option<Vec<u8>> {
    cluster
        .kv(node)
        .and_then(|kv| kv.get(b"k"))
        .map(<[u8]>::to_vec)
}

/// Ayaktaki tek lider; seçilene kadar bekler.
fn sole_leader(cluster: &mut RaftCluster) -> NodeId {
    wait_for(cluster, 20 * T, "a single leader", |c| {
        c.leaders().len() == 1
    });
    cluster.leaders()[0].0
}

/// İstemci 9'un `seq` sıralı okumasının cevabı (varsa): okumanın değeri ya da `None` (henüz cevap
/// yok ya da `NotLeader`).
fn read_value(replies: &[sim::ClientReply], seq: u64) -> Option<Option<Vec<u8>>> {
    replies.iter().find_map(|reply| match &reply.outcome {
        ReplyOutcome::Done {
            result: KvResult::Value(value),
            ..
        } if (reply.client, reply.seq) == (9, seq) => Some(value.clone()),
        _ => None,
    })
}

// Q2: kiralaması geçerli olan lider okumayı geldiği adımda cevaplar; doğrulama turunu beklemez.
// Kiralamasız aynı küme aynı okumayı ancak bir gidiş-dönüş sonra cevaplar.
#[test]
fn a_leased_leader_answers_a_read_in_the_step_it_arrives() {
    for lease in [true, false] {
        let mut cluster = cluster(5, lease);
        let leader = sole_leader(&mut cluster);
        cluster.submit(leader, put(b"v")).expect("the leader is up");
        wait_for(&mut cluster, 2 * T, "the write to apply", |c| {
            c.node_ids()
                .all(|id| value_at(c, id).as_deref() == Some(&b"v"[..]))
        });
        let _ = cluster.take_client_replies();
        cluster
            .submit_read(leader, 9, 1, b"k".to_vec())
            .expect("the leader is up");
        let at_once = read_value(&cluster.take_client_replies(), 1);
        if lease {
            assert_eq!(
                at_once,
                Some(Some(b"v".to_vec())),
                "answered in the same step"
            );
            assert_eq!(cluster.lease_reads(), 1);
        } else {
            assert_eq!(at_once, None, "ReadIndex needs a round trip");
            assert_eq!(cluster.lease_reads(), 0);
        }
    }
}

// `leases` profilinin taraması (`raftsim fuzz --profile leases` ile aynı program): her seed
// güvenlik invariant'larını ve istemci geçmişinin linearizability'sini korur. Kiralama yolu
// gerçekten sınanır: okumaların önemli bir kısmı geldiği adımda kiralamayla cevaplanır.
#[test]
fn leases_stay_safe_across_seeds() {
    let mut failures = Vec::new();
    let mut totals = RunStats::default();
    for seed in LEASE_SEEDS {
        match run(&Scenario::generate(seed, ScenarioConfig::leases())).outcome {
            Ok(stats) => {
                totals.lease_reads += stats.lease_reads;
                totals.clients.completed += stats.clients.completed;
            }
            Err(error) => failures.push(format!(
                "seed {seed}: {error}\n  reproduce: cargo run -p cli -- replay --seed {seed} \
                 --profile leases"
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} seeds failed:\n{}",
        failures.len(),
        LEASE_SEEDS.count(),
        failures.join("\n")
    );
    // Eşikler ölçülen değerlerin (kiralamayla geldiği adımda cevaplanan 1527 okuma, tamamlanan 5006
    // işlem) çok altındadır.
    let seeds = LEASE_SEEDS.count() as u64;
    assert!(totals.lease_reads >= 5 * seeds, "{totals:?}");
    assert!(totals.clients.completed >= 20 * seeds, "{totals:?}");
}

/// Kiralamanın varsayımını bozan senaryo. Lider L, takipçileri A ve B; `k = old` her yerde
/// uygulanmış. L bir bölünmeyle yalıtılır; kiralaması hâlâ sürmektedir. `jump` ise A ve B'nin
/// saatleri ileri sıçrar (B T tick, A 2T tick): ikisinin de liderden haber alalı T tick geçmiş
/// sayılır, A hemen seçim başlatır ve B oyunu verir. Yeni lider `k = new` yazar ve uygular. Sonra
/// L'ye bir okuma gelir. Okumanın L'deki sonucu döner: cevaplandıysa değeri, cevaplanmadıysa
/// `None`.
fn read_at_an_isolated_leader(seed: u64, lease: bool, jump: bool) -> Option<Option<Vec<u8>>> {
    let mut cluster = cluster(seed, lease);
    let old_leader = sole_leader(&mut cluster);
    let old_term = cluster.leaders()[0].1;
    cluster
        .submit(old_leader, put(b"old"))
        .expect("the leader is up");
    wait_for(&mut cluster, 2 * T, "the first write to apply", |c| {
        c.node_ids()
            .all(|id| value_at(c, id).as_deref() == Some(&b"old"[..]))
    });
    let followers: Vec<NodeId> = cluster.node_ids().filter(|&id| id != old_leader).collect();
    let (a, b) = (followers[0], followers[1]);
    cluster
        .partition(&[&[old_leader], &[a, b]])
        .expect("a valid partition");
    if jump {
        cluster.jump_clock(b, T).expect("B is up");
        cluster.jump_clock(a, 2 * T).expect("A is up");
    }
    // Yeni lider, eski liderin kiralaması dolmadan (en geç 16 tick) seçilip yazmayı uygulamalıdır;
    // saatler sıçramadıysa takipçiler T tick boyunca oy vermez ve bu olmaz.
    let new_leader = |c: &RaftCluster| {
        c.leaders()
            .into_iter()
            .find(|&(id, term)| id != old_leader && term > old_term)
            .map(|(id, _)| id)
    };
    for _ in 0..4 {
        if new_leader(&cluster).is_some() {
            break;
        }
        tick(&mut cluster);
    }
    let leader = new_leader(&cluster)?;
    cluster
        .submit(leader, put(b"new"))
        .expect("the new leader is up");
    for _ in 0..6 {
        if value_at(&cluster, leader).as_deref() == Some(&b"new"[..]) {
            break;
        }
        tick(&mut cluster);
    }
    assert_eq!(
        value_at(&cluster, leader).as_deref(),
        Some(&b"new"[..]),
        "the write completed at the new leader"
    );
    assert_eq!(
        cluster.node(old_leader).map(|node| node.role()),
        Some(Role::Leader),
        "the isolated leader still believes it leads"
    );
    // Yazma tamamlandıktan SONRA gelen okuma: doğrusal bir sistem `new` döndürmeli ya da hiç
    // cevaplamamalıdır.
    let _ = cluster.take_client_replies();
    cluster
        .submit_read(old_leader, 9, 1, b"k".to_vec())
        .expect("the old leader is up");
    let mut answer = read_value(&cluster.take_client_replies(), 1);
    for _ in 0..T {
        if answer.is_some() {
            break;
        }
        tick(&mut cluster);
        answer = read_value(&cluster.take_client_replies(), 1);
    }
    answer
}

// Kiralama saatlere dayanır, ReadIndex dayanmaz. Takipçilerin saatleri paydan (T - lease = 4
// tick) fazla ileri sıçrayınca, yalıtılmış eski lider kiralamasıyla bayat bir değer döndürür:
// okuma, tamamlanmış bir yazmayı görmez. Kiralamasız (ReadIndex) aynı senaryoda eski lider
// liderliğini doğrulayamaz ve okumayı hiç cevaplamaz. Saatler sıçramazsa yeni bir lider kiralama
// sürerken seçilemez.
#[test]
fn clocks_that_jump_past_the_margin_break_leases_but_not_read_index() {
    let seed = 7;
    assert_eq!(
        read_at_an_isolated_leader(seed, true, true),
        Some(Some(b"old".to_vec())),
        "with jumping clocks the lease serves a stale value"
    );
    assert_eq!(
        read_at_an_isolated_leader(seed, false, true),
        None,
        "ReadIndex cannot confirm leadership from a minority"
    );
    let mut cluster = cluster(seed, true);
    let old_leader = sole_leader(&mut cluster);
    let rest: Vec<NodeId> = cluster.node_ids().filter(|&id| id != old_leader).collect();
    cluster
        .partition(&[&[old_leader], &rest])
        .expect("a valid partition");
    for _ in 0..LEASE {
        tick(&mut cluster);
        assert!(
            cluster.leaders().iter().all(|&(id, _)| id == old_leader),
            "no new leader while the lease may last"
        );
    }
}

// §4.2.3 simülatör düzeyinde, kademeli iki bölünmeyle. Önce y ve C azınlıkta kalır: liderden haber
// alamazlar, zamanlayıcıları işler ve term'leri büyür; yazma olmadığı için log'ları güncel kalır.
// Sonra lider tek başına ayrılır: x ve z liderden az önce haber almıştır (kiralamayı onaylayanlar),
// C'nin zamanlayıcısı ise çoktan dolmuştur. C hemen seçim başlatır (saati ileri alınır: bu yalnızca
// rastgele zamanlayıcısının tam o anda dolmasıdır; x ve z'nin saatleri doğru akar). x ve z korumalı
// oldukları için oy vermez ve kiralama sürerken yeni bir lider seçilmez. Koruma olmasaydı x ve z,
// C'yi bir gidiş-dönüşte seçerdi.
#[test]
fn followers_inside_a_lease_ignore_a_candidate_from_an_earlier_partition() {
    let mut cluster = five_node_cluster(9);
    let leader = sole_leader(&mut cluster);
    for _ in 0..2 * T {
        tick(&mut cluster);
    }
    let others: Vec<NodeId> = cluster.node_ids().filter(|&id| id != leader).collect();
    let (x, z, y, c) = (others[0], others[1], others[2], others[3]);
    cluster
        .partition(&[&[leader, x, z], &[y, c]])
        .expect("a valid partition");
    for _ in 0..3 * T {
        tick(&mut cluster);
    }
    assert!(
        cluster.leaders().iter().all(|&(id, _)| id == leader),
        "the majority side keeps its leader"
    );
    cluster
        .partition(&[&[leader], &[x, z, y, c]])
        .expect("a valid partition");
    cluster.jump_clock(c, 2 * T).expect("C is up");
    assert_eq!(
        cluster.node(c).map(|node| node.role()),
        Some(Role::Candidate),
        "C campaigns at once"
    );
    for _ in 0..LEASE {
        tick(&mut cluster);
        assert!(
            cluster.leaders().iter().all(|&(id, _)| id == leader),
            "no new leader while the lease may last"
        );
    }
}
