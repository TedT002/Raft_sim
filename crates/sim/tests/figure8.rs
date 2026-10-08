//! Figure 8 senaryosu (§5.4.2): önceki bir term'in girdisi, çoğunlukta bulunsa bile kopyaları
//! sayılarak commit edilemez.
//!
//! Makaledeki adımlar, bölünme ve çökmelerle kuruluyor. Düğümler sabit kimlikleriyle değil
//! senaryodaki rolleriyle seçilir, böylece hangi seed'de kim lider olursa olsun aynı hikâye
//! kurulur. Her lider term'inin başında log'una bir no-op girdi ekler (§8); hikâye bunu hesaba
//! katar:
//!
//! - (a) `old` lider, eski term'de bir girdiyi (`c1`, 3. index) yalnızca `ally`'ye çoğaltır.
//! - (b) `old` çöker; çoğunluk tarafı (`rival` ve iki `voter`) yeni bir lider (`rival`) seçer.
//!   `rival` aynı index'e kendi term'inden farklı bir girdi koyar: term başındaki no-op'u.
//!   Ardından `c2`'yi alır. Seçildiği tick'te yalıtılır ve çöker: girdileri kimseye ulaşmaz.
//! - (c) `old` yeniden başlar; `c1`'i taşıyan `old` ya da `ally` daha yüksek bir term'de lider
//!   olur ve `c1`'i voter'lara da çoğaltır. `c1` artık çoğunluktadır ama önceki bir term'den
//!   olduğu için kopyaları sayılarak commit EDİLMEZ. Liderin kendi term'inden no-op'u ise henüz
//!   voter'lara ulaşmamıştır: mesaj başına tek girdi gider (`max_entries = 1`) ve no-op, c1'in
//!   onayından sonra yola çıkar.
//! - (d) Bu lider no-op'u voter'lara ulaşmadan çöker; `rival` döner ve voter'larla birlikte
//!   seçilir (son girdisinin term'i `c1`'inkinden yüksektir, yani log'u daha günceldir). `c1`'in
//!   yerine `rival`'ın girdisi yazılır. `c1` hiç commit edilmediği için bu bir ihlal değildir.
//! - (e) Ama (c)'deki lider kendi term'inden bir girdiyi (no-op'u) çoğunluğa çoğaltırsa, o girdi
//!   commit edilir ve `c1` de dolaylı olarak commit olur. Artık `rival` seçilemez: voter'ların
//!   log'u daha günceldir. No-op'un varlık nedeni budur: istemci komutu beklemeden önceki term'in
//!   girdilerinin kaderini netleştirir.
//!
//! Her adımda `RaftCluster` beş güvenlik invariant'ını denetler. §5.4.2'yi çiğneyen bir lider
//! (c)'de voter'ların onayıyla `c1`'i commit eder; (d)'de seçilen `rival` onu taşımadığı için
//! Leader Completeness ihlali raporlanır.

use std::num::NonZeroUsize;

use sim::{
    ClusterConfig, DiskConfig, KvCommand, KvRequest, LogIndex, NetworkConfig, NodeId, RaftCluster,
    RaftConfig, Role, Term,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`).
const T: u64 = 20;

fn put(key: &str) -> KvCommand {
    KvCommand::Put {
        key: key.as_bytes().to_vec(),
        value: b"v".to_vec(),
    }
}

/// Başarısız bir seed'in bağlamı: seed ile tek satırlık yeniden üretme komutu.
fn context(test: &str, seed: u64) -> String {
    format!("seed {seed} [reproduce: cargo test -p sim --test figure8 -- {test} --exact]")
}

/// Koşul sağlanana kadar birer tick ilerler. Bir invariant çiğnenirse ya da koşul `max_ticks`
/// içinde sağlanmazsa testi bağlamla birlikte düşürür.
fn wait_for(
    cluster: &mut RaftCluster,
    context: &str,
    max_ticks: u64,
    what: &str,
    condition: impl Fn(&RaftCluster) -> bool,
) {
    let deadline = cluster.now() + max_ticks;
    while !condition(cluster) {
        assert!(
            cluster.now() < deadline,
            "{context}: {what} did not happen within {max_ticks} ticks"
        );
        tick(cluster, context);
    }
}

/// Bir tick ilerler.
fn tick(cluster: &mut RaftCluster, context: &str) {
    let next = cluster.now() + 1;
    ok(cluster.run_until(next), context);
}

/// Bir küme işleminin sonucu: hata, bağlamla birlikte testi düşürür.
fn ok<E: std::fmt::Display>(result: Result<(), E>, context: &str) {
    if let Err(error) = result {
        panic!("{context}: {error}");
    }
}

/// `id`'nin log'unda `index`'te `key` komutu var mı?
fn holds(cluster: &RaftCluster, id: NodeId, index: u64, key: &str) -> bool {
    let wanted = put(key);
    cluster.node(id).is_some_and(|node| {
        node.log()
            .get(usize::try_from(index - 1).unwrap_or(usize::MAX))
            .and_then(|entry| KvRequest::decode(entry.command.as_bytes()).ok())
            .is_some_and(|request| request.command == wanted)
    })
}

/// `id`'nin log uzunluğu.
fn log_len(cluster: &RaftCluster, id: NodeId) -> usize {
    cluster.node(id).map_or(0, |node| node.log().len())
}

/// `group` içinden ayaktaki bir lider (varsa).
fn leader_among(cluster: &RaftCluster, group: &[NodeId]) -> Option<NodeId> {
    cluster
        .leaders()
        .into_iter()
        .map(|(id, _)| id)
        .find(|id| group.contains(id))
}

/// Senaryonun rolleri.
struct Roles {
    old: NodeId,
    ally: NodeId,
    rival: NodeId,
    /// `rival`'ın (b)'de lider olduğu term.
    rival_term: Term,
    voters: [NodeId; 2],
    /// (c)'de seçilen lider: `old` ya da `ally`.
    leader_c: NodeId,
}

/// (a), (b) ve (c) adımlarını kurar: (c)'nin lideri seçilmiş ve `c1` voter'lara ulaşmıştır (voter
/// onayları henüz yoldadır).
fn stages_a_to_c(seed: u64, context: &str) -> (RaftCluster, Roles) {
    // Gecikmesiz disk: senaryo, girdilerin tam olarak nerede kalıcı olduğunu kontrol eder. Mesaj
    // başına tek girdi: (c)'de c1 ile liderin no-op'u voter'lara ayrı ayrı, sırayla gider.
    let config = ClusterConfig {
        disk: DiskConfig::instant(),
        raft: RaftConfig::default().with_max_entries(NonZeroUsize::MIN),
        ..ClusterConfig::new(5, NetworkConfig::reliable(1))
    };
    let mut cluster = RaftCluster::new(seed, config).expect("valid config");
    let all: Vec<NodeId> = (1..=5).map(NodeId).collect();

    // Başlangıç: herkes ilk liderin no-op'unu (1. index) ve c0'ı (2. index) commit etmiş.
    wait_for(&mut cluster, context, 20 * T, "a first leader", |c| {
        c.leaders().len() == 1
    });
    let (old, _) = cluster.leaders()[0];
    ok(cluster.submit(old, put("c0")), context);
    wait_for(
        &mut cluster,
        context,
        10 * T,
        "c0 committed everywhere",
        |c| {
            all.iter().all(|&id| {
                holds(c, id, 2, "c0")
                    && c.node(id)
                        .is_some_and(|node| node.commit_index() == LogIndex(2))
            })
        },
    );
    let followers: Vec<NodeId> = all.iter().copied().filter(|&id| id != old).collect();
    let ally = followers[0];
    let majority = [followers[1], followers[2], followers[3]];

    // (a) old, c1'i (3. index) yalnızca ally'ye çoğaltır.
    ok(cluster.partition(&[&[old, ally], &majority]), context);
    ok(cluster.submit(old, put("c1")), context);
    wait_for(&mut cluster, context, 5 * T, "c1 reaching the ally", |c| {
        holds(c, ally, 3, "c1")
    });
    // Ön koşul: c1 çoğunluğa ulaşmadı (yoksa hikâye Figure 8'in hikâyesi olmazdı).
    assert!(
        majority.iter().all(|&id| log_len(&cluster, id) == 2),
        "{context}: c1 reached the majority in (a)"
    );
    let c1_term = cluster
        .node(ally)
        .and_then(|node| node.log().get(2))
        .map(|entry| entry.term)
        .expect("the ally holds c1");

    // (b) old çöker; çoğunluk tarafı bir lider (rival) seçer. Rival seçildiği tick'te yalıtılır:
    // no-op'unu taşıyan ilk AppendEntries yoldayken bölünme onu düşürür. Sonra c2'yi alır ve
    // çöker.
    ok(cluster.crash(old), context);
    wait_for(&mut cluster, context, 20 * T, "a majority leader", |c| {
        leader_among(c, &majority).is_some()
    });
    let rival = leader_among(&cluster, &majority).expect("a majority leader");
    let rival_term = cluster.node(rival).expect("node exists").current_term();
    let voters: Vec<NodeId> = majority.iter().copied().filter(|&id| id != rival).collect();
    let voters = [voters[0], voters[1]];
    ok(cluster.partition(&[&[rival], &voters, &[ally]]), context);
    // Ön koşul: c2'nin ve rival'ın no-op'unun term'i c1'inkinden yüksek. (d)'de rival'ın log'unu
    // voter'larınkinden daha güncel yapan budur (§5.4.1).
    assert!(
        rival_term > c1_term,
        "{context}: {rival_term:?} vs {c1_term:?}"
    );
    ok(cluster.submit(rival, put("c2")), context);
    assert!(holds(&cluster, rival, 4, "c2"), "{context}");
    // Ön koşul: rival'ın girdileri (no-op ve c2) voter'lara ulaşmadı.
    tick(&mut cluster, context);
    assert!(
        voters.iter().all(|&id| log_len(&cluster, id) == 2),
        "{context}: the rival's entries reached a voter in (b)"
    );
    ok(cluster.crash(rival), context);

    // (c) old döner; c1'i taşıyan old ya da ally lider olur ve c1'i voter'lara çoğaltır. Voter'lar
    // c1'i aldığı tick'te durulur: onayları yoldadır, liderin no-op'u henüz gönderilmedi.
    ok(cluster.restart(old), context);
    ok(
        cluster.partition(&[&[old, ally, voters[0], voters[1]]]),
        context,
    );
    wait_for(&mut cluster, context, 30 * T, "c1 on a majority", |c| {
        leader_among(c, &[old, ally]).is_some() && voters.iter().all(|&id| holds(c, id, 3, "c1"))
    });
    let leader_c = leader_among(&cluster, &[old, ally]).expect("an (c) leader");
    let roles = Roles {
        old,
        ally,
        rival,
        rival_term,
        voters,
        leader_c,
    };
    (cluster, roles)
}

// Figure 8 (c)-(d): önceki term'in girdisi c1 çoğunlukta olsa bile commit edilmez (§5.4.2); sonra
// daha güncel log'lu rival seçilir ve c1'in yerine kendi girdisini yazar. c1 hiçbir düğümde
// uygulanmamıştır ve bu bir ihlal değildir: commit edilmemiş bir girdi kaybolabilir.
#[test]
fn figure_8_an_earlier_term_entry_on_a_majority_is_not_committed() {
    const TEST: &str = "figure_8_an_earlier_term_entry_on_a_majority_is_not_committed";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let (mut cluster, roles) = stages_a_to_c(seed, &context);
        let Roles {
            old,
            ally,
            rival,
            rival_term: _,
            voters,
            leader_c,
        } = roles;

        // (c) devam: voter'ların c1 onayları (c)'nin liderine ulaşır. c1 artık çoğunluktadır
        // (lider, diğer eski düğüm ve voter'lar), ama §5.4.2'ye uyan bir lider onu commit etmez.
        // Liderin no-op'u voter'lara yeni yola çıktı. §5.4.2'yi çiğneyen bir uygulama burada c1'i
        // commit eder; senaryo (d)'ye ilerler ve ihlali oradaki invariant denetimi raporlar.
        tick(&mut cluster, &context);
        assert!(
            voters.iter().all(|&id| log_len(&cluster, id) == 3),
            "{context}: the (c) leader's no-op reached a voter too early"
        );

        // (d) (c)'nin lideri çöker; rival döner ve voter'larla birlikte seçilir; c1'in yerine
        // rival'ın girdileri yazılır ve yeni liderin no-op'u onları commit eder.
        let other = if leader_c == old { ally } else { old };
        ok(cluster.crash(leader_c), &context);
        let committed_early = [old, ally, voters[0], voters[1]].iter().any(|&id| {
            cluster
                .node(id)
                .is_some_and(|node| node.commit_index() >= LogIndex(3))
        });
        ok(cluster.restart(rival), &context);
        ok(
            cluster.partition(&[&[rival, voters[0], voters[1]], &[other]]),
            &context,
        );
        wait_for(&mut cluster, &context, 30 * T, "rival elected", |c| {
            leader_among(c, &[rival]).is_some()
        });
        wait_for(&mut cluster, &context, 10 * T, "c2 committed", |c| {
            [rival, voters[0], voters[1]].iter().all(|&id| {
                holds(c, id, 4, "c2")
                    && c.node(id)
                        .is_some_and(|node| node.commit_index() >= LogIndex(4))
            })
        });

        // Herkes döner ve yakınsar: 4. index'te c2 vardır; c1 hiçbir yerde uygulanmamıştır.
        cluster.heal();
        ok(cluster.restart(leader_c), &context);
        let all: Vec<NodeId> = (1..=5).map(NodeId).collect();
        wait_for(&mut cluster, &context, 30 * T, "convergence", |c| {
            all.iter().all(|&id| {
                holds(c, id, 4, "c2")
                    && c.node(id)
                        .is_some_and(|node| node.last_applied() >= LogIndex(4))
            })
        });
        for &id in &all {
            let kv = cluster.kv(id).expect("node exists");
            assert!(kv.get(b"c1").is_none(), "{context}: {id:?} applied c1");
            assert!(kv.get(b"c2").is_some(), "{context}: {id:?} lacks c2");
        }
        assert!(
            !committed_early,
            "{context}: §5.4.2: an earlier-term entry was committed by counting replicas"
        );
    }
}

// Figure 8 (e): (c)'nin lideri kendi term'inden bir girdiyi, term başındaki no-op'u, çoğunluğa
// çoğaltırsa no-op commit edilir ve c1 de dolaylı olarak commit olur. Bundan sonra rival seçilemez
// (voter'ların son girdisi daha yüksek bir term'dedir) ve c1 her düğümde uygulanır; c2 hiçbir
// yerde uygulanmaz.
#[test]
fn figure_8_a_current_term_entry_commits_the_earlier_one() {
    const TEST: &str = "figure_8_a_current_term_entry_commits_the_earlier_one";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let (mut cluster, roles) = stages_a_to_c(seed, &context);
        let Roles {
            old,
            ally,
            rival,
            rival_term,
            voters,
            leader_c,
        } = roles;

        wait_for(&mut cluster, &context, 10 * T, "the no-op committed", |c| {
            voters.iter().all(|&id| {
                c.node(id)
                    .is_some_and(|node| node.commit_index() >= LogIndex(4))
            })
        });
        assert!(
            voters
                .iter()
                .all(|&id| cluster.kv(id).is_some_and(|kv| kv.get(b"c1").is_some())),
            "{context}: c1 is committed indirectly and applied"
        );

        // (c)'nin lideri çöker; rival döner. Voter'ların log'u daha güncel olduğu için rival
        // seçilemez; voter'lardan biri seçilir.
        let other = if leader_c == old { ally } else { old };
        ok(cluster.crash(leader_c), &context);
        ok(cluster.restart(rival), &context);
        ok(
            cluster.partition(&[&[rival, voters[0], voters[1]], &[other]]),
            &context,
        );
        wait_for(&mut cluster, &context, 30 * T, "a voter elected", |c| {
            leader_among(c, &voters).is_some()
        });
        let settle = cluster.now() + 10 * T;
        ok(cluster.run_until(settle), &context);
        assert_eq!(
            cluster.node(rival).map(|node| node.role()),
            Some(Role::Follower),
            "{context}"
        );
        let led_again = cluster
            .elections()
            .range(Term(rival_term.0 + 1)..)
            .any(|(_, election)| election.leader == Some(rival));
        assert!(
            !led_again,
            "{context}: the rival must not win against a newer log"
        );

        cluster.heal();
        ok(cluster.restart(leader_c), &context);
        let all: Vec<NodeId> = (1..=5).map(NodeId).collect();
        wait_for(&mut cluster, &context, 30 * T, "convergence", |c| {
            all.iter().all(|&id| {
                holds(c, id, 3, "c1")
                    && c.node(id)
                        .is_some_and(|node| node.last_applied() >= LogIndex(4))
            })
        });
        for &id in &all {
            let kv = cluster.kv(id).expect("node exists");
            assert!(kv.get(b"c1").is_some(), "{context}: {id:?} lacks c1");
            assert!(kv.get(b"c2").is_none(), "{context}: {id:?} applied c2");
        }
    }
}
