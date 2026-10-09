//! Log replikasyonu testleri (Faz 3): komutların her düğümde aynı sırayla uygulanması, lider
//! çöktüğünde commit edilmiş girdilerin korunması, kayıp/çoğaltma/sıra değişimi altında yakınsama,
//! geride kalmış bir takipçinin yakalanması ve azınlıktaki eski liderin commit edilmemiş
//! girdilerinin ezilmesi.
//!
//! Bütün senaryolar `RaftCluster` üzerinden koşar: HER olaydan sonra beş güvenlik invariant'ı ve
//! kümenin diğer denetimleri (dayanıklılık, çıktı sırası, commit edilmiş girdilerin korunması)
//! koşar. Aksi belirtilmedikçe disk varsayılan ayarlarındadır (fsync 1..3 tick, çökmede bekleyen
//! yazmaların bir öneki kalabilir).

use std::num::NonZeroUsize;

use sim::{
    ClusterConfig, DiskConfig, KvApplied, KvCommand, KvRequest, KvStore, LogIndex, NetworkConfig,
    NodeId, RaftCluster, RaftConfig, Role, Term, TraceKind,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`).
const T: u64 = 20;

/// Kayıplı, çoğaltmalı, değişken gecikmeli ağ.
const LOSSY: NetworkConfig = NetworkConfig {
    drop_prob: 0.1,
    duplicate_prob: 0.1,
    min_delay: 1,
    max_delay: 5,
    tail_prob: 0.0,
    tail_delay: 0,
};

fn cluster(seed: u64, size: u64, network: NetworkConfig) -> RaftCluster {
    RaftCluster::new(seed, ClusterConfig::new(size, network)).expect("valid config")
}

/// Başarısız bir seed'in bağlamı: seed (ve varsa ayrıntı) ile tek satırlık yeniden üretme komutu.
fn context(test: &str, seed: u64) -> String {
    format!("seed {seed} [reproduce: cargo test -p sim --test replication -- {test} --exact]")
}

fn put(key: &str, value: u64) -> KvCommand {
    KvCommand::Put {
        key: key.as_bytes().to_vec(),
        value: value.to_le_bytes().to_vec(),
    }
}

fn ids(size: u64) -> Vec<NodeId> {
    (1..=size).map(NodeId).collect()
}

/// Koşuyu `time` anına kadar ilerletir; bir invariant çiğnenirse bağlamla birlikte bildirir.
fn run(cluster: &mut RaftCluster, context: &str, time: u64) {
    if let Err(error) = cluster.run_until(time) {
        panic!("{context}: {error}");
    }
}

/// Koşul sağlanana kadar birer tick ilerler; `max_ticks` içinde sağlanmazsa testi düşürür.
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
        let next = cluster.now() + 1;
        run(cluster, context, next);
    }
}

/// Tek bir ayaktaki lider olana kadar bekler ve onu döndürür.
fn sole_leader(cluster: &mut RaftCluster, context: &str) -> (NodeId, Term) {
    wait_for(cluster, context, 20 * T, "electing a single leader", |c| {
        c.leaders().len() == 1
    });
    cluster.leaders()[0]
}

/// `id`'nin commit ettiği istemci komutlarının sayısı. No-op girdiler sayılmaz: her lider term
/// başında bir tane ekler ve sayıları seçim sayısına bağlıdır (§8).
fn committed_commands(cluster: &RaftCluster, id: NodeId) -> Option<usize> {
    let node = cluster.node(id)?;
    let commit = usize::try_from(node.commit_index().0).unwrap_or(usize::MAX);
    Some(
        node.log()
            .iter()
            .take(commit)
            .filter(|entry| !entry.command.is_noop())
            .count(),
    )
}

/// Ayaktaki bütün düğümler aynı commitIndex'te, hepsi uygulanmış, aynı KV tablosunda ve en az
/// `at_least` istemci komutu commit edilmiş mi?
fn converged(cluster: &RaftCluster, at_least: usize) -> bool {
    let live: Vec<NodeId> = cluster.node_ids().filter(|&id| cluster.is_up(id)).collect();
    let Some(&first) = live.first() else {
        return false;
    };
    let commit = |id| cluster.node(id).map(|node| node.commit_index().0);
    let applied = |id| cluster.node(id).map(|node| node.last_applied().0);
    live.iter().all(|&id| {
        commit(id) == commit(first)
            && applied(id) == commit(first)
            && cluster.kv(id) == cluster.kv(first)
    }) && committed_commands(cluster, first).is_some_and(|count| count >= at_least)
}

/// Bir komut dizisini sırayla uygulayan referans tablo. Komutlar, `RaftCluster::submit`'in iç
/// oturumuyla (istemci 0, sıra numaraları 1'den) verilmiş gibi uygulanır: kümenin tablosuyla
/// oturumlar dahil karşılaştırılabilsin.
fn reference(commands: &[KvCommand]) -> KvStore {
    let mut store = KvStore::default();
    for (seq, command) in (1_u64..).zip(commands) {
        let request = KvRequest {
            client: 0,
            seq,
            command: command.clone(),
        };
        let applied = store.apply(&request.encode());
        assert!(matches!(applied, KvApplied::Executed { .. }), "{applied:?}");
    }
    store
}

/// Bir düğümün log'undaki istemci komutları, sırasıyla (no-op girdiler atlanır).
fn logged_commands(cluster: &RaftCluster, id: NodeId) -> Vec<KvCommand> {
    cluster
        .node(id)
        .expect("node exists")
        .log()
        .iter()
        .filter(|entry| !entry.command.is_noop())
        .map(|entry| {
            KvRequest::decode(entry.command.as_bytes())
                .expect("a valid request")
                .command
        })
        .collect()
}

/// Ayaktaki bütün düğümler aynı commitIndex ve aynı KV tablosundalar ve `key` her tabloda var mı?
fn applied_everywhere(cluster: &RaftCluster, key: &[u8]) -> bool {
    converged(cluster, 1)
        && cluster
            .node_ids()
            .all(|id| cluster.kv(id).is_some_and(|kv| kv.get(key).is_some()))
}

/// Ayaktaki iki düğümün log'u aynı mı?
fn same_log(cluster: &RaftCluster, a: NodeId, b: NodeId) -> bool {
    cluster.node(a).map(|node| node.log()) == cluster.node(b).map(|node| node.log())
}

// Hatasız ağda (3 ve 5 düğüm) lidere verilen komutlar her düğümde aynı sırayla uygulanır: bütün
// düğümlerin KV tablosu, komutları verildikleri sırayla uygulayan bir referans tabloyla aynıdır.
// Aynı anahtarların üzerine yazılması ve silinmesi sırayı görünür kılar: sıra farklı olsaydı son
// değerler farklı olurdu. Daha güçlüsü: liderin log'u, verilen komutların kodlamalarını
// verildikleri sırayla taşır (etkisi sonradan ezilen komutlar dahil) ve her düğümün log'u onunla
// aynıdır.
#[test]
fn commands_are_applied_in_the_same_order_on_every_node() {
    const TEST: &str = "commands_are_applied_in_the_same_order_on_every_node";
    for size in [3, 5] {
        for seed in 0..5 {
            let context = context(TEST, seed);
            let mut cluster = cluster(seed, size, NetworkConfig::reliable(2));
            let (leader, _) = sole_leader(&mut cluster, &context);
            let mut commands = Vec::new();
            for i in 0..30_u64 {
                let command = if i % 7 == 6 {
                    KvCommand::Delete {
                        key: format!("k{}", i % 4).into_bytes(),
                    }
                } else {
                    put(&format!("k{}", i % 4), i)
                };
                commands.push(command.clone());
                if let Err(error) = cluster.submit(leader, command) {
                    panic!("{context}: {error}");
                }
                let next = cluster.now() + 2;
                run(&mut cluster, &context, next);
            }
            wait_for(&mut cluster, &context, 10 * T, "convergence", |c| {
                converged(c, 30)
            });
            assert_eq!(cluster.kv(leader), Some(&reference(&commands)), "{context}");
            assert_eq!(logged_commands(&cluster, leader), commands, "{context}");
            for id in ids(size) {
                assert!(same_log(&cluster, leader, id), "{context}: node {id:?}");
            }
        }
    }
}

// Lider çökünce commit edilmiş girdiler kaybolmaz: yeni lider hepsini taşır, yeni komutlar
// onların arkasına eklenir ve eski lider yeniden başlayınca (girdileri diskinden) kümeye yetişir.
// Sonunda her düğüm, 10 + 5 komutun hepsini sırayla uygulamıştır.
#[test]
fn committed_entries_survive_a_leader_crash() {
    const TEST: &str = "committed_entries_survive_a_leader_crash";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let mut cluster = cluster(seed, 5, NetworkConfig::reliable(2));
        let (old_leader, _) = sole_leader(&mut cluster, &context);
        let mut commands = Vec::new();
        for i in 0..10 {
            commands.push(put(&format!("a{i}"), i));
            if let Err(error) = cluster.submit(old_leader, put(&format!("a{i}"), i)) {
                panic!("{context}: {error}");
            }
        }
        wait_for(
            &mut cluster,
            &context,
            10 * T,
            "committing 10 entries",
            |c| converged(c, 10),
        );
        let committed: Vec<_> = cluster
            .node(old_leader)
            .expect("node exists")
            .log()
            .to_vec();

        if let Err(error) = cluster.crash(old_leader) {
            panic!("{context}: {error}");
        }
        let (new_leader, _) = sole_leader(&mut cluster, &context);
        assert_ne!(new_leader, old_leader, "{context}");
        let new_log = cluster.node(new_leader).expect("node exists").log();
        assert!(new_log.starts_with(&committed), "{context}");

        for i in 0..5 {
            commands.push(put(&format!("b{i}"), i));
            if let Err(error) = cluster.submit(new_leader, put(&format!("b{i}"), i)) {
                panic!("{context}: {error}");
            }
        }
        wait_for(
            &mut cluster,
            &context,
            10 * T,
            "committing 15 entries",
            |c| converged(c, 15),
        );
        if let Err(error) = cluster.restart(old_leader) {
            panic!("{context}: {error}");
        }
        wait_for(
            &mut cluster,
            &context,
            10 * T,
            "the old leader catching up",
            |c| converged(c, 15),
        );
        for id in ids(5) {
            assert_eq!(
                cluster.kv(id),
                Some(&reference(&commands)),
                "{context}: {id:?}"
            );
        }
    }
}

// Kayıp, çoğaltma ve sıra değişimi altında yakınsama. Komutlar, o an lider olan düğüme verilir
// (lider yoksa o komut atlanır). Liderliğini kaybeden bir liderin commit edemediği komutlar
// kaybolabilir; bu Raft'a uygundur (istemci Faz 4'te yeniden dener). Sonda verilen bir son komut,
// yeni liderin kendi term'inden bir girdi olarak önceki girdileri de commit eder (§5.4.2). Sonuçta
// her düğüm aynı commitIndex'e ve aynı KV tablosuna ulaşır ve son komut her yerde uygulanmıştır.
#[test]
fn the_cluster_converges_under_loss_duplication_and_reordering() {
    const TEST: &str = "the_cluster_converges_under_loss_duplication_and_reordering";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let mut cluster = cluster(seed, 5, LOSSY);
        run(&mut cluster, &context, 4 * T);
        for i in 0..40_u64 {
            if let Some(&(leader, _)) = cluster.leaders().iter().max_by_key(|(_, term)| *term)
                && let Err(error) = cluster.submit(leader, put(&format!("k{}", i % 8), i))
            {
                panic!("{context}: {error}");
            }
            let next = cluster.now() + 3;
            run(&mut cluster, &context, next);
        }
        // Son komut, o anki lidere verilir ve her düğümde uygulandığı görülene kadar (gerekirse
        // yeni lidere) yeniden verilir: liderliğini kaybeden bir lider onu commit edemeyebilir.
        // `Put` aynı değerle tekrarlandığında sonucu değiştirmez.
        let mut leader = sole_leader(&mut cluster, &context).0;
        let mut attempts = 0;
        loop {
            if let Err(error) = cluster.submit(leader, put("last", 1)) {
                panic!("{context}: {error}");
            }
            let deadline = cluster.now() + 10 * T;
            while cluster.now() < deadline && !applied_everywhere(&cluster, b"last") {
                let next = cluster.now() + 1;
                run(&mut cluster, &context, next);
            }
            if applied_everywhere(&cluster, b"last") {
                break;
            }
            attempts += 1;
            assert!(
                attempts < 10,
                "{context}: the last command was never applied everywhere"
            );
            leader = sole_leader(&mut cluster, &context).0;
        }
        assert!(
            cluster.node(leader).expect("node exists").commit_index().0 >= 20,
            "{context}: most commands must be committed"
        );
    }
}

// Geride kalmış bir takipçi yakalanır. Takipçi ayrıyken 40 komut commit edilir. İyileşince (ayrı
// kalırken yükselen term'i yüzünden yeni bir seçim olsa bile) lider, takipçinin nextIndex'ini ret
// ipucuyla bir hamlede geri çeker ve eksik girdileri mesaj başına en fazla 8 girdiyle gönderir
// (parçalamanın kendisi raft-core birim testinde doğrudan sınanır). Takipçinin log'u ve KV tablosu
// diğerlerine eşitlenir.
#[test]
fn a_lagging_follower_catches_up_in_batches() {
    const TEST: &str = "a_lagging_follower_catches_up_in_batches";
    for seed in 0..5 {
        let context = context(TEST, seed);
        let config = ClusterConfig {
            raft: RaftConfig::default().with_max_entries(NonZeroUsize::new(8).expect("not zero")),
            ..ClusterConfig::new(5, NetworkConfig::reliable(2))
        };
        let mut cluster = RaftCluster::new(seed, config).expect("valid config");
        let (leader, _) = sole_leader(&mut cluster, &context);
        let lagging = ids(5)
            .into_iter()
            .find(|&id| id != leader)
            .expect("a follower");
        let others: Vec<NodeId> = ids(5).into_iter().filter(|&id| id != lagging).collect();
        if let Err(error) = cluster.partition(&[&[lagging], &others]) {
            panic!("{context}: {error}");
        }
        for i in 0..40 {
            if let Err(error) = cluster.submit(leader, put(&format!("k{i}"), i)) {
                panic!("{context}: {error}");
            }
        }
        wait_for(
            &mut cluster,
            &context,
            10 * T,
            "committing without the follower",
            |c| committed_commands(c, leader) == Some(40),
        );
        assert!(
            logged_commands(&cluster, lagging).is_empty(),
            "{context}: the partitioned follower must not have the commands"
        );
        cluster.heal();
        wait_for(
            &mut cluster,
            &context,
            20 * T,
            "the follower catching up",
            |c| same_log(c, leader, lagging) && converged(c, 40),
        );
        assert_eq!(
            cluster.node(lagging).map(|node| node.role()),
            Some(Role::Follower),
            "{context}"
        );
    }
}

// Azınlıkta kalan eski liderin commit edilemeyen girdileri ezilir (§5.3): eski lider azınlıkta
// komut kabul eder ama çoğunluğa ulaşamaz; çoğunluk yeni bir lider seçip kendi komutlarını commit
// eder. İyileşince eski lider daha yüksek term'i görüp çekilir ve çakışan kuyruğu yeni liderin
// girdileriyle değiştirilir. Eski liderin komutları hiçbir düğümde uygulanmamıştır.
#[test]
fn a_stale_leaders_uncommitted_entries_are_overwritten() {
    const TEST: &str = "a_stale_leaders_uncommitted_entries_are_overwritten";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let mut cluster = cluster(seed, 5, NetworkConfig::reliable(2));
        let (stale, _) = sole_leader(&mut cluster, &context);
        let followers: Vec<NodeId> = ids(5).into_iter().filter(|&id| id != stale).collect();
        let minority = [stale, followers[0]];
        let majority = [followers[1], followers[2], followers[3]];
        if let Err(error) = cluster.partition(&[&minority, &majority]) {
            panic!("{context}: {error}");
        }
        for i in 0..5 {
            if let Err(error) = cluster.submit(stale, put(&format!("stale{i}"), i)) {
                panic!("{context}: {error}");
            }
        }
        wait_for(
            &mut cluster,
            &context,
            20 * T,
            "a new majority leader",
            |c| c.leaders().iter().any(|(id, _)| majority.contains(id)),
        );
        let (fresh, _) = cluster
            .leaders()
            .into_iter()
            .find(|(id, _)| majority.contains(id))
            .expect("a majority leader");
        for i in 0..5 {
            if let Err(error) = cluster.submit(fresh, put(&format!("fresh{i}"), i)) {
                panic!("{context}: {error}");
            }
        }
        wait_for(
            &mut cluster,
            &context,
            10 * T,
            "the majority committing",
            |c| c.node(fresh).is_some_and(|node| node.commit_index().0 >= 5),
        );
        assert!(
            cluster.node(stale).expect("node exists").log().len() >= 5,
            "{context}: the stale leader holds its uncommitted entries"
        );

        cluster.heal();
        wait_for(
            &mut cluster,
            &context,
            20 * T,
            "convergence after healing",
            |c| same_log(c, stale, fresh) && converged(c, 5),
        );
        for id in ids(5) {
            let kv = cluster.kv(id).expect("node exists");
            assert!(kv.get(b"stale0").is_none(), "{context}: {id:?}");
            assert!(kv.get(b"fresh4").is_some(), "{context}: {id:?}");
        }
    }
}

// Tek düğümlü küme: düğüm, seçim zaman aşımında AYNI adımda lider olur ve bir komutu eklediği
// adımda commitIndex'ini ilerletir; ikisi de diske henüz ulaşmamışken. Dışarıya görünen etkiler
// (uygulama) O1 gereği yazmaların arkasında tutulduğu için bu güvenlidir, ama denetim hiç
// gerçekleşmemiş bir geçmişi kayda geçirmemelidir. İki durum sınanır:
//
// - Düğüm, term'i ve komutu henüz kalıcı değilken çöker: ikisi de kaybolur, düğüm AYNI term'i
//   yeniden kazanır. Çökme, ilk liderliğin kaydını unutturur (Leader Append-Only); kalıcı olmamış
//   girdi Log Matching'e hiç girmez.
// - Düğüm, term'i kalıcıyken ama komutu henüz kalıcı değilken çöker: komut hiç commit edilmemiştir
//   ve sonraki term'in lideri olarak düğüm onu taşımak zorunda değildir (Leader Completeness).
//
// Her iki durumda da sonraki komut her zamanki gibi commit edilip uygulanır; kaybolan komut hiçbir
// zaman uygulanmamıştır. Senaryonun öncülleri ayrıca doğrulanır: lider commitIndex'ini hemen
// ilerletmiştir, çökme kalıcı olmayan yazmaları kaybettirmiştir ve term'i kalıcı olmayan düğüm aynı
// term'i yeniden kazanır.
#[test]
fn a_single_node_cluster_never_counts_an_unsynced_commit() {
    const TEST: &str = "a_single_node_cluster_never_counts_an_unsynced_commit";
    let slow_disk = DiskConfig {
        min_fsync_delay: 3,
        max_fsync_delay: 3,
        partial_write_prob: 0.0,
    };
    for seed in 0..10 {
        for term_synced in [false, true] {
            let context = format!("{}, term synced: {term_synced}", context(TEST, seed));
            let config = ClusterConfig {
                disk: slow_disk,
                ..ClusterConfig::new(1, NetworkConfig::reliable(1))
            };
            let mut cluster = RaftCluster::new(seed, config).expect("valid config");
            let (leader, first_term) = sole_leader(&mut cluster, &context);
            if term_synced {
                let synced = cluster.now() + 5;
                run(&mut cluster, &context, synced);
            }
            if let Err(error) = cluster.submit(leader, put("lost", 1)) {
                panic!("{context}: {error}");
            }
            // No-op 1. index'tedir (seçim adımında commit edildi), komut 2. index'te.
            assert_eq!(
                cluster.node(leader).map(|node| node.commit_index()),
                Some(LogIndex(2)),
                "{context}: a single node advances its commit index at once"
            );
            if let Err(error) = cluster.crash(leader) {
                panic!("{context}: {error}");
            }
            // Senaryo gerçekten sınanıyor mu: çökme, kalıcı olmamış yazmaları (komut ve gerekirse
            // term) kaybettirmiş olmalı.
            let lost = cluster
                .sim()
                .trace()
                .events()
                .iter()
                .rev()
                .find_map(|event| match event.kind {
                    TraceKind::CrashLoss { lost_writes, .. } => Some(lost_writes),
                    _ => None,
                });
            let expected = if term_synced { 1 } else { 2 };
            assert_eq!(lost, Some(expected), "{context}: unexpected crash loss");
            if let Err(error) = cluster.restart(leader) {
                panic!("{context}: {error}");
            }
            let (leader, second_term) = sole_leader(&mut cluster, &context);
            if term_synced {
                assert!(second_term > first_term, "{context}: {second_term:?}");
            } else {
                assert_eq!(
                    second_term, first_term,
                    "{context}: the lost term is won again"
                );
            }
            if let Err(error) = cluster.submit(leader, put("kept", 2)) {
                panic!("{context}: {error}");
            }
            wait_for(
                &mut cluster,
                &context,
                10 * T,
                "the next command applied",
                |c| c.kv(leader).is_some_and(|kv| kv.get(b"kept").is_some()),
            );
            let kv = cluster.kv(leader).expect("node exists");
            assert!(
                kv.get(b"lost").is_none(),
                "{context}: the lost write was applied"
            );
        }
    }
}
