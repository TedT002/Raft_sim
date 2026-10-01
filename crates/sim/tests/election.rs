//! Lider seçimi testleri (Faz 2): hatasız ağda tek ve kararlı lider, liderin çökmesi, azınlık
//! bölünmesi, bölünmüş oylar ve yüzlerce seed'de Election Safety.
//!
//! Bütün senaryolar `RaftCluster` üzerinden koşar: HER olaydan sonra Election Safety ve
//! dayanıklılık (disk = bellekteki kalıcı durum) denetlenir. Senaryodaki bir ihlal, testin kendi
//! iddialarından önce `ClusterError::Violation` olarak yüzeye çıkar.
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır (raft-core'u doğrudan içe aktarmaz): Faz 5'te
//! `cli` de bağımlılık yönü gereği yalnızca `sim`'i görecek.

use std::collections::BTreeSet;
use std::ops::Range;

use sim::{
    ChaCha8Rng, ClusterError, Component, NetworkConfig, NodeId, RaftCluster, RaftConfig, Role,
    SeedTree, Term, chance, uniform_inclusive,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`): zaman aşımları `[T, 2T)` tick.
const T: u64 = 20;

/// Kayıpsız, çoğaltmasız ama değişken gecikmeli ağ: mesajlar sıra değiştirebilir, hiçbiri
/// kaybolmaz.
const JITTERY: NetworkConfig = NetworkConfig {
    drop_prob: 0.0,
    duplicate_prob: 0.0,
    min_delay: 1,
    max_delay: 6,
};

fn cluster(seed: u64, size: u64, network: NetworkConfig) -> RaftCluster {
    RaftCluster::new(seed, size, network, RaftConfig::default()).expect("valid config")
}

/// Başarısız bir seed'in bağlamı: seed (ve varsa ayrıntı) ile tek satırlık yeniden üretme komutu.
/// Koşular deterministik olduğu için komut aynı hatayı aynı olayda yeniden üretir.
fn context(test: &str, seed: u64, detail: &str) -> String {
    format!("seed {seed}{detail} [reproduce: cargo test -p sim --test election -- {test} --exact]")
}

/// Koşuyu `time` anına kadar ilerletir; bir invariant çiğnenirse bağlamla birlikte bildirir.
fn run(cluster: &mut RaftCluster, context: &str, time: u64) {
    if let Err(error) = cluster.run_until(time) {
        panic!("{context}: {error}");
    }
}

/// Ayaktaki düğümler arasında TAM OLARAK bir lider olmalı; onu ve term'ini döndürür.
fn sole_leader(cluster: &RaftCluster, context: &str) -> (NodeId, Term) {
    let leaders = cluster.leaders();
    assert_eq!(
        leaders.len(),
        1,
        "{context}: expected exactly one leader, got {leaders:?}"
    );
    leaders[0]
}

/// Verilen her düğüm ayakta olmalı ve term'i `term` olmalı. Ayakta olma şartı bilerek iddia edilir:
/// çökmüş bir düğümün bellek durumu donmuştur, onun term'ini doğrulamak yanıltıcı olurdu.
fn assert_all_in_term(cluster: &RaftCluster, ids: &[NodeId], term: Term, context: &str) {
    for &id in ids {
        assert!(cluster.is_up(id), "{context}: node {id:?} must be up");
        let node = cluster.node(id).expect("node exists");
        assert_eq!(node.current_term(), term, "{context}: node {id:?}");
    }
}

fn ids(range: Range<u64>) -> Vec<NodeId> {
    range.map(NodeId).collect()
}

// Hatasız ağda 3 ve 5 düğümlü küme 10·T tick içinde tam olarak bir lider seçer ve herkes onun
// term'indedir. Lider sonrasında heartbeat'lerle liderliğini korur: 100·T tick daha boyunca hiçbir
// gereksiz seçim olmaz (lider ve term değişmez, yeni term'de aday görülmez).
#[test]
fn three_and_five_node_clusters_elect_one_stable_leader() {
    const TEST: &str = "three_and_five_node_clusters_elect_one_stable_leader";
    for size in [3, 5] {
        for seed in 0..20 {
            let context = context(TEST, seed, &format!(", size {size}"));
            let mut cluster = cluster(seed, size, NetworkConfig::reliable(2));
            run(&mut cluster, &context, 10 * T);
            let (leader, term) = sole_leader(&cluster, &context);
            assert_all_in_term(&cluster, &ids(1..size + 1), term, &context);

            run(&mut cluster, &context, 110 * T);
            assert_eq!(
                cluster.leaders(),
                vec![(leader, term)],
                "{context}: the leader must stay in place"
            );
            assert_eq!(
                cluster.elections().keys().next_back(),
                Some(&term),
                "{context}: no election may start after the leader is elected"
            );
        }
    }
}

// Lider çökünce kalanlar yeni bir lideri daha yüksek bir term'de seçer. Eski lider yeniden
// başlayınca Follower'dır ve diskteki term'ini ve oyunu (kendine) korur; ilk heartbeat'le yeni
// term'i benimser ve lider değişmez.
#[test]
fn a_new_leader_is_elected_after_the_leader_crashes() {
    const TEST: &str = "a_new_leader_is_elected_after_the_leader_crashes";
    for size in [3, 5] {
        for seed in 0..10 {
            let context = context(TEST, seed, &format!(", size {size}"));
            let mut cluster = cluster(seed, size, NetworkConfig::reliable(2));
            run(&mut cluster, &context, 10 * T);
            let (old_leader, old_term) = sole_leader(&cluster, &context);

            cluster.crash(old_leader).expect("the leader is up");
            let crashed_at = cluster.now();
            run(&mut cluster, &context, crashed_at + 10 * T);
            let (new_leader, new_term) = sole_leader(&cluster, &context);
            assert_ne!(new_leader, old_leader, "{context}");
            assert!(
                new_term > old_term,
                "{context}: {new_term:?} > {old_term:?}"
            );

            cluster.restart(old_leader).expect("the old leader is down");
            let node = cluster.node(old_leader).expect("node exists");
            assert_eq!(node.role(), Role::Follower, "{context}");
            assert_eq!(node.current_term(), old_term, "{context}");
            assert_eq!(node.voted_for(), Some(old_leader), "{context}");

            let restarted_at = cluster.now();
            run(&mut cluster, &context, restarted_at + 5 * T);
            assert_eq!(cluster.leaders(), vec![(new_leader, new_term)], "{context}");
            assert_all_in_term(&cluster, &ids(1..size + 1), new_term, &context);
        }
    }
}

// Azınlık lider seçemez. 5 düğümlü kümede iki takipçi ayrılır: onlar defalarca aday olur (term'leri
// yükselir) ama 3 oya hiçbir zaman ulaşamaz. Çoğunluk tarafı eski lideriyle sürer. Bölünme
// iyileşince azınlığın yüksek term'i herkese yayılır ve küme tek bir liderle, bölünme sırasında
// görülen en yüksek term'e ya da daha yükseğine yakınsar.
#[test]
fn a_minority_partition_cannot_elect_a_leader() {
    const TEST: &str = "a_minority_partition_cannot_elect_a_leader";
    for seed in 0..10 {
        let context = context(TEST, seed, "");
        let mut cluster = cluster(seed, 5, NetworkConfig::reliable(2));
        run(&mut cluster, &context, 10 * T);
        let (leader, term) = sole_leader(&cluster, &context);
        let followers: Vec<NodeId> = ids(1..6).into_iter().filter(|&id| id != leader).collect();
        let minority = [followers[0], followers[1]];
        let majority = [leader, followers[2], followers[3]];
        cluster
            .partition(&[&minority, &majority])
            .expect("valid partition");
        let partitioned_at = cluster.now();
        run(&mut cluster, &context, partitioned_at + 20 * T);

        for (election_term, election) in cluster.elections().range(term..) {
            if let Some(winner) = election.leader {
                assert!(
                    majority.contains(&winner),
                    "{context}: {winner:?} won term {election_term:?} in the minority"
                );
            }
        }
        let minority_term = minority
            .iter()
            .map(|&id| cluster.node(id).expect("node exists").current_term())
            .max()
            .expect("the minority is not empty");
        assert!(
            minority_term.0 >= term.0 + 2,
            "{context}: the minority must keep trying ({minority_term:?})"
        );
        assert_eq!(
            cluster.leaders(),
            vec![(leader, term)],
            "{context}: the majority keeps its leader"
        );

        cluster.heal();
        let healed_at = cluster.now();
        run(&mut cluster, &context, healed_at + 20 * T);
        let (_, final_term) = sole_leader(&cluster, &context);
        assert!(final_term >= minority_term, "{context}");
        assert_all_in_term(&cluster, &ids(1..6), final_term, &context);
    }
}

// Lider azınlıkta kalırsa çoğunluk onu daha yüksek bir term'de yeni bir liderle değiştirir. Azınlık
// yeni bir lider seçemez. Eski lider kendini lider sanmaya devam eder (Figure 2'de çoğunluğu
// kaybeden liderin kendiliğinden çekilmesi yoktur), ama eski term'de kalır; bu iki lider farklı
// term'lerde olduğu için Election Safety ihlali değildir. İyileşince eski lider yüksek term'i görüp
// çekilir ve küme tek lidere yakınsar.
#[test]
fn a_partitioned_leader_is_replaced_by_the_majority() {
    const TEST: &str = "a_partitioned_leader_is_replaced_by_the_majority";
    for seed in 0..10 {
        let context = context(TEST, seed, "");
        let mut cluster = cluster(seed, 5, NetworkConfig::reliable(2));
        run(&mut cluster, &context, 10 * T);
        let (old_leader, old_term) = sole_leader(&cluster, &context);
        let followers: Vec<NodeId> = ids(1..6)
            .into_iter()
            .filter(|&id| id != old_leader)
            .collect();
        let minority = [old_leader, followers[0]];
        let majority = [followers[1], followers[2], followers[3]];
        cluster
            .partition(&[&minority, &majority])
            .expect("valid partition");
        let partitioned_at = cluster.now();
        run(&mut cluster, &context, partitioned_at + 20 * T);

        for (election_term, election) in cluster.elections().range(Term(old_term.0 + 1)..) {
            if let Some(winner) = election.leader {
                assert!(
                    majority.contains(&winner),
                    "{context}: {winner:?} won term {election_term:?} in the minority"
                );
            }
        }
        let leaders = cluster.leaders();
        let new = leaders
            .iter()
            .find(|(id, _)| majority.contains(id))
            .copied();
        let Some((_, new_term)) = new else {
            panic!("{context}: the majority must elect a leader, got {leaders:?}");
        };
        assert!(new_term > old_term, "{context}");
        for (id, term) in &leaders {
            if minority.contains(id) {
                assert_eq!(
                    (*id, *term),
                    (old_leader, old_term),
                    "{context}: only the stale leader may lead in the minority"
                );
            }
        }

        cluster.heal();
        let healed_at = cluster.now();
        run(&mut cluster, &context, healed_at + 20 * T);
        let (_, final_term) = sole_leader(&cluster, &context);
        assert!(final_term >= new_term, "{context}");
        assert_all_in_term(&cluster, &ids(1..6), final_term, &context);
    }
}

// Bölünmüş oylar rastgele zaman aşımlarıyla çözülür. 4 düğümlü kümede (çoğunluk 3) değişken
// gecikmeli bir ağda iki aday oyları ikişer ikişer bölüşebilir. Bu, en az iki adayın görüldüğü ama
// kimsenin kazanamadığı bir term'dir. Taramada bölünmüş oylar yeterince sık görülmeli (senaryo
// gerçekten sınanıyor; ölçülen: 100 seed'in 20'si) ve her seed'de küme sonunda tek bir lidere
// ulaşmalı. Adayların zaman aşımları her turda yeniden çekildiği için bölünme kendini sonsuza dek
// tekrarlamaz.
#[test]
fn split_votes_are_resolved_by_randomized_timeouts() {
    const TEST: &str = "split_votes_are_resolved_by_randomized_timeouts";
    let mut seeds_with_a_split = Vec::new();
    for seed in 0..100 {
        let context = context(TEST, seed, "");
        let mut cluster = cluster(seed, 4, JITTERY);
        run(&mut cluster, &context, 30 * T);
        let split = cluster
            .elections()
            .values()
            .any(|election| election.leader.is_none() && election.candidates.len() >= 2);
        if split {
            seeds_with_a_split.push(seed);
        }
        let (_, term) = sole_leader(&cluster, &context);
        assert_all_in_term(&cluster, &ids(1..5), term, &context);
    }
    assert!(
        seeds_with_a_split.len() >= 5,
        "the sweep must exercise split votes, saw them only for seeds {seeds_with_a_split:?}"
    );
}

/// Kaos taramasının ağı: arada bir kayıp ve çoğaltma, 1..5 tick gecikme.
const CHAOS_NETWORK: NetworkConfig = NetworkConfig {
    drop_prob: 0.05,
    duplicate_prob: 0.05,
    min_delay: 1,
    max_delay: 5,
};
/// Taranan seed'ler.
const CHAOS_SEEDS: Range<u64> = 0..200;
/// Hataların enjekte edildiği süre (tick).
const CHAOS_UNTIL: u64 = 1_200;
/// Hatalar bittikten sonra kümenin toparlanmasına verilen süre (tick).
const SETTLE: u64 = 25 * T;

/// Bir kaos koşusunun özeti: taramanın gerçekten bir şeyleri sınadığını gösteren sayaçlar ve
/// koşunun kimliği olan trace özeti.
#[derive(Debug, Default, PartialEq, Eq)]
struct ChaosStats {
    crashes: u64,
    restarts: u64,
    partitions: u64,
    terms_with_a_leader: usize,
    trace_hash: u64,
}

/// `items` içinden rastgele biri. Her çağrıda TAM OLARAK bir çekiliş yapılır (liste boş olsa
/// bile): hata programının çekiliş sayısı küme durumuna göre değişmesin.
fn pick(rng: &mut ChaCha8Rng, items: &[NodeId]) -> Option<NodeId> {
    let draw = uniform_inclusive(rng, 0, u64::MAX);
    let len = u64::try_from(items.len()).ok()?;
    let index = usize::try_from(draw.checked_rem(len)?).ok()?;
    items.get(index).copied()
}

/// Bir seed'in kaos koşusu: rastgele çökme, yeniden başlatma, bölünme ve iyileşme. Her olaydan
/// sonra invariant'lar denetlenir. Sonda herkes ayağa kalkar, ağ iyileşir ve küme yeniden tek bir
/// lidere yakınsamalıdır (canlılık).
///
/// Hata programı kendi alt-seed akışından gelir (`Component::Scenario`): ağın ve düğümlerin
/// akışlarından bağımsızdır, aynı seed her zaman aynı programı üretir.
fn chaos_run(seed: u64) -> Result<ChaosStats, String> {
    let mut cluster = RaftCluster::new(seed, 5, CHAOS_NETWORK, RaftConfig::default())
        .map_err(|error| error.to_string())?;
    let mut faults = SeedTree::new(seed).rng_for(Component::Scenario);
    let all = ids(1..6);
    let mut stats = ChaosStats::default();
    let violation = |error: ClusterError| error.to_string();

    let mut now = 0;
    while now < CHAOS_UNTIL {
        now += uniform_inclusive(&mut faults, 5, 40);
        cluster.run_until(now).map_err(violation)?;
        let up: Vec<NodeId> = all
            .iter()
            .copied()
            .filter(|&id| cluster.is_up(id))
            .collect();
        let down: Vec<NodeId> = all
            .iter()
            .copied()
            .filter(|&id| !cluster.is_up(id))
            .collect();
        match uniform_inclusive(&mut faults, 0, 9) {
            0..=2 => {
                if let Some(id) = pick(&mut faults, &up) {
                    cluster.crash(id).map_err(violation)?;
                    stats.crashes += 1;
                }
            }
            3..=5 => {
                if let Some(id) = pick(&mut faults, &down) {
                    cluster.restart(id).map_err(violation)?;
                    stats.restarts += 1;
                }
            }
            6 | 7 => {
                let (left, right): (Vec<NodeId>, Vec<NodeId>) =
                    all.iter().partition(|_| chance(&mut faults, 0.5));
                cluster.partition(&[&left, &right]).map_err(violation)?;
                stats.partitions += 1;
            }
            8 => cluster.heal(),
            _ => {}
        }
    }

    cluster.heal();
    let down: Vec<NodeId> = all
        .iter()
        .copied()
        .filter(|&id| !cluster.is_up(id))
        .collect();
    for id in down {
        cluster.restart(id).map_err(violation)?;
    }
    // Canlılık: son 5T içinde en az bir anda küme yakınsamış olmalı (tam olarak bir lider var ve
    // herkes onun term'inde). Tek bir an yeterli sayılır, çünkü kayıplı ağda arka arkaya kaybolan
    // heartbeat'ler tam sonda kısa bir seçime denk gelebilir; bu bir hata değildir.
    let settle_end = cluster.now() + SETTLE;
    let mut converged = false;
    for time in settle_end - 5 * T..=settle_end {
        cluster.run_until(time).map_err(violation)?;
        if let [(_, term)] = cluster.leaders()[..] {
            converged |= all.iter().all(|&id| {
                cluster
                    .node(id)
                    .is_some_and(|node| node.current_term() == term)
            });
        }
    }
    if !converged {
        return Err(format!(
            "liveness: the cluster did not converge on a single leader during the last {} ticks \
             after healing and restarting every node",
            5 * T
        ));
    }
    stats.terms_with_a_leader = cluster
        .elections()
        .values()
        .filter(|election| election.leader.is_some())
        .count();
    stats.trace_hash = cluster.sim().trace_hash();
    Ok(stats)
}

// Yüzlerce seed'de Election Safety: her seed kayıplı ve çoğaltmalı bir ağda rastgele çökme, yeniden
// başlatma, bölünme ve iyileşme yaşar; HER olaydan sonra Election Safety ve dayanıklılık
// denetlenir. Sonda küme yeniden tek bir lidere yakınsamalıdır. Başarısız her seed, seed numarası
// ve tek satırlık bir yeniden üretme komutuyla raporlanır (koşu deterministiktir: aynı komut aynı
// hatayı aynı olayda verir; bkz. `the_chaos_schedule_is_reproducible`).
#[test]
fn election_safety_holds_across_hundreds_of_seeds() {
    let mut failures = Vec::new();
    let mut totals = ChaosStats::default();
    for seed in CHAOS_SEEDS {
        match chaos_run(seed) {
            Ok(stats) => {
                totals.crashes += stats.crashes;
                totals.restarts += stats.restarts;
                totals.partitions += stats.partitions;
                totals.terms_with_a_leader += stats.terms_with_a_leader;
            }
            Err(error) => failures.push((seed, error)),
        }
    }
    if !failures.is_empty() {
        let report: Vec<String> = failures
            .iter()
            .take(5)
            .map(|(seed, error)| {
                format!(
                    "seed {seed}: {error}\n  reproduce: cargo test -p sim --test election -- \
                     election_safety_holds_across_hundreds_of_seeds --exact\n  replay (from \
                     phase 5): cargo run -p cli -- replay --seed {seed}"
                )
            })
            .collect();
        panic!(
            "{} of {} seeds failed:\n{}",
            failures.len(),
            CHAOS_SEEDS.count(),
            report.join("\n")
        );
    }
    // Tarama boş geçmemeli: hatalar gerçekten enjekte edildi ve seçimler gerçekten yapıldı. Eşikler
    // ölçülen değerlerin (2881 çökme, 2401 yeniden başlatma, 2168 bölünme, liderli 1221 term) çok
    // altındadır.
    let seeds = CHAOS_SEEDS.count();
    assert!(totals.crashes >= seeds as u64, "{totals:?}");
    assert!(totals.restarts >= seeds as u64, "{totals:?}");
    assert!(totals.partitions >= seeds as u64, "{totals:?}");
    assert!(totals.terms_with_a_leader >= 2 * seeds, "{totals:?}");
}

// Taramanın hata programı seed'e bağlıdır ve tekrarlanabilir: aynı seed iki kez koşulunca aynı
// sayaçlar ve aynı trace özeti çıkar (olay olay aynı koşu). Farklı seed'ler farklı koşular üretir.
#[test]
fn the_chaos_schedule_is_reproducible() {
    let first = chaos_run(7).expect("seed 7 passes");
    let second = chaos_run(7).expect("seed 7 passes");
    assert_eq!(first, second);
    let hashes: BTreeSet<u64> = (0..5)
        .map(|seed| chaos_run(seed).expect("seed passes").trace_hash)
        .collect();
    assert_eq!(hashes.len(), 5, "{hashes:?}");
}
