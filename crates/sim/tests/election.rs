//! Lider seçimi testleri: hatasız ağda tek ve kararlı lider, liderin çökmesi, azınlık bölünmesi,
//! bölünmüş oylar. Yüzlerce seed'lik kaos taraması `chaos.rs`'tedir.
//!
//! Bütün senaryolar `RaftCluster` üzerinden koşar: HER olaydan sonra beş güvenlik invariant'ı ve
//! dayanıklılık (bellek = diske yazdırılan durum) denetlenir. Senaryodaki bir ihlal, testin kendi
//! iddialarından önce `ClusterError::Violation` olarak yüzeye çıkar. Disk varsayılan ayarlarındadır
//! (fsync 1..3 tick): oy ve term yazmaları, onlara bağlı mesajları bu kadar geciktirir.
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır (raft-core'u doğrudan içe aktarmaz): Faz 5'te
//! `cli` de bağımlılık yönü gereği yalnızca `sim`'i görecek.

use std::ops::Range;

use sim::{ClusterConfig, NetworkConfig, NodeId, RaftCluster, Role, Term};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`): zaman aşımları `[T, 2T)` tick.
const T: u64 = 20;

/// Kayıpsız, çoğaltmasız ama değişken gecikmeli ağ: mesajlar sıra değiştirebilir, hiçbiri
/// kaybolmaz.
const JITTERY: NetworkConfig = NetworkConfig {
    drop_prob: 0.0,
    duplicate_prob: 0.0,
    min_delay: 1,
    max_delay: 6,
    tail_prob: 0.0,
    tail_delay: 0,
};

fn cluster(seed: u64, size: u64, network: NetworkConfig) -> RaftCluster {
    RaftCluster::new(seed, ClusterConfig::new(size, network)).expect("valid config")
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
// gerçekten sınanıyor; ölçülen: 100 seed'in 23'ü) ve her seed'de küme sonunda tek bir lidere
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
