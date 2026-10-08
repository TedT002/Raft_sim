//! Kaos taraması: yüzlerce seed'de rastgele çökme, yeniden başlatma, bölünme, iyileşme ve istemci
//! komutları; kayıplı ve çoğaltmalı bir ağ; fsync penceresi ve kısmi yazmalarıyla bir disk. HER
//! olaydan sonra Figure 3'ün beş güvenlik invariant'ı ile dayanıklılık, çıktı sırası ve commit
//! edilmiş girdilerin korunması denetlenir (bkz. `RaftCluster`); sonda küme yeniden bir komutu
//! bütün düğümlerde uygulayabilmelidir (canlılık).
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır (raft-core'u doğrudan içe aktarmaz): Faz 5'te
//! `cli` de bağımlılık yönü gereği yalnızca `sim`'i görecek.

use std::collections::BTreeSet;
use std::ops::Range;

use sim::{
    ChaCha8Rng, ClusterConfig, ClusterError, Component, KvCommand, NetworkConfig, NodeId,
    RaftCluster, SeedTree, TraceKind, chance, uniform_inclusive,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`): zaman aşımları `[T, 2T)` tick.
const T: u64 = 20;
/// Kaos taramasının ağı: arada bir kayıp ve çoğaltma, 1..5 tick gecikme.
const CHAOS_NETWORK: NetworkConfig = NetworkConfig {
    drop_prob: 0.05,
    duplicate_prob: 0.05,
    min_delay: 1,
    max_delay: 5,
};
/// Taranan seed'ler.
const CHAOS_SEEDS: Range<u64> = 0..200;
/// Hataların ve komutların enjekte edildiği süre (tick).
const CHAOS_UNTIL: u64 = 1_200;
/// Sondaki işaret komutunun her denemede uygulanması için beklenen en uzun süre (tick).
const SETTLE_ATTEMPT: u64 = 10 * T;
/// İşaret komutunun en fazla kaç kez yeniden verileceği.
const SETTLE_ATTEMPTS: u64 = 10;

/// Bir kaos koşusunun özeti: taramanın gerçekten bir şeyleri sınadığını gösteren sayaçlar ve
/// koşunun kimliği olan trace özeti.
#[derive(Debug, Default, PartialEq, Eq)]
struct ChaosStats {
    crashes: u64,
    restarts: u64,
    partitions: u64,
    commands: u64,
    /// Kümenin sonunda commit ettiği girdi sayısı.
    committed: u64,
    /// Çökmede kaybolan (fsync'i tamamlanmamış) yazmalar ve kısmi yazmayla yine de diske ulaşanlar.
    lost_writes: u64,
    kept_writes: u64,
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

fn ids(range: Range<u64>) -> Vec<NodeId> {
    range.map(NodeId).collect()
}

/// Ayaktaki bütün düğümler aynı commitIndex'te, hepsi uygulanmış, KV tabloları aynı ve `key` her
/// tabloda var mı?
fn applied_everywhere(cluster: &RaftCluster, key: &[u8]) -> bool {
    let live: Vec<NodeId> = cluster.node_ids().filter(|&id| cluster.is_up(id)).collect();
    let Some(&first) = live.first() else {
        return false;
    };
    let commit = |id| cluster.node(id).map(|node| node.commit_index());
    live.iter().all(|&id| {
        commit(id) == commit(first)
            && cluster.node(id).map(|node| node.last_applied()) == commit(first)
            && cluster.kv(id) == cluster.kv(first)
            && cluster.kv(id).is_some_and(|kv| kv.get(key).is_some())
    })
}

/// Bir seed'in kaos koşusu. Hata ve komut programı kendi alt-seed akışından gelir
/// (`Component::Scenario`): ağın, diskin ve düğümlerin akışlarından bağımsızdır; aynı seed her
/// zaman aynı programı üretir.
///
/// Komutlar, o an ayakta olan liderlerden rastgele birine verilir: bölünme sırasında bu, azınlıkta
/// kalmış eski bir lider de olabilir. Onun girdileri commit edilemez ve iyileşince ezilir; bu da
/// çakışma çözümünü (§5.3) sınar.
fn chaos_run(seed: u64) -> Result<ChaosStats, String> {
    let mut cluster = RaftCluster::new(seed, ClusterConfig::new(5, CHAOS_NETWORK))
        .map_err(|error| error.to_string())?;
    let mut program = SeedTree::new(seed).rng_for(Component::Scenario);
    let all = ids(1..6);
    let mut stats = ChaosStats::default();
    let violation = |error: ClusterError| error.to_string();

    let mut now = 0;
    while now < CHAOS_UNTIL {
        now += uniform_inclusive(&mut program, 5, 40);
        cluster.run_until(now).map_err(violation)?;
        let leaders: Vec<NodeId> = cluster.leaders().into_iter().map(|(id, _)| id).collect();
        let target = pick(&mut program, &leaders);
        // Anahtarlar, komutları alacak bir lider olsun olmasın çekilir: programın çekiliş sayısı
        // küme durumuna bağlı olmasın (bkz. `pick`).
        let keys: Vec<u64> = (0..uniform_inclusive(&mut program, 0, 3))
            .map(|_| uniform_inclusive(&mut program, 0, 7))
            .collect();
        if let Some(leader) = target {
            for key in keys {
                let command = KvCommand::Put {
                    key: format!("k{key}").into_bytes(),
                    value: stats.commands.to_le_bytes().to_vec(),
                };
                cluster.submit(leader, command).map_err(violation)?;
                stats.commands += 1;
            }
        }
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
        match uniform_inclusive(&mut program, 0, 9) {
            0..=2 => {
                if let Some(id) = pick(&mut program, &up) {
                    cluster.crash(id).map_err(violation)?;
                    stats.crashes += 1;
                }
            }
            3..=5 => {
                if let Some(id) = pick(&mut program, &down) {
                    cluster.restart(id).map_err(violation)?;
                    stats.restarts += 1;
                }
            }
            6 | 7 => {
                let (left, right): (Vec<NodeId>, Vec<NodeId>) =
                    all.iter().partition(|_| chance(&mut program, 0.5));
                cluster.partition(&[&left, &right]).map_err(violation)?;
                stats.partitions += 1;
            }
            8 => cluster.heal(),
            _ => {}
        }
    }

    // Sakinleşme: ağ iyileşir, herkes ayağa kalkar. Bir işaret komutu o anki lidere verilir ve
    // bütün düğümlerde uygulanana kadar (gerekirse yeni lidere) yeniden verilir: liderliğini
    // kaybeden bir lider onu commit edemeyebilir. İşaret, yeni liderin kendi term'inden bir girdi
    // olarak önceki girdileri de commit eder (§5.4.2).
    cluster.heal();
    let down: Vec<NodeId> = all
        .iter()
        .copied()
        .filter(|&id| !cluster.is_up(id))
        .collect();
    for id in down {
        cluster.restart(id).map_err(violation)?;
    }
    let mut attempts = 0;
    loop {
        if let Some(&(leader, _)) = cluster.leaders().iter().max_by_key(|(_, term)| *term) {
            let marker = KvCommand::Put {
                key: b"marker".to_vec(),
                value: Vec::new(),
            };
            cluster.submit(leader, marker).map_err(violation)?;
        }
        let deadline = cluster.now() + SETTLE_ATTEMPT;
        while cluster.now() < deadline && !applied_everywhere(&cluster, b"marker") {
            let next = cluster.now() + 1;
            cluster.run_until(next).map_err(violation)?;
        }
        if applied_everywhere(&cluster, b"marker") {
            break;
        }
        attempts += 1;
        if attempts >= SETTLE_ATTEMPTS {
            return Err(format!(
                "liveness: the marker command was not applied on every node after {attempts} \
                 attempts of {SETTLE_ATTEMPT} ticks"
            ));
        }
    }

    stats.committed = cluster
        .node(NodeId(1))
        .map_or(0, |node| node.commit_index().0);
    for event in cluster.sim().trace().events() {
        if let TraceKind::CrashLoss {
            kept_writes,
            lost_writes,
            ..
        } = event.kind
        {
            stats.kept_writes += kept_writes;
            stats.lost_writes += lost_writes;
        }
    }
    stats.terms_with_a_leader = cluster
        .elections()
        .values()
        .filter(|election| election.leader.is_some())
        .count();
    stats.trace_hash = cluster.sim().trace_hash();
    Ok(stats)
}

// Yüzlerce seed'de güvenlik: her seed kayıplı ve çoğaltmalı bir ağda rastgele çökme, yeniden
// başlatma, bölünme, iyileşme ve istemci komutları yaşar; disk her yazmada bir fsync penceresi açar
// ve çökmeler bekleyen yazmaları kaybettirir ya da bir öneklerini diske ulaştırır. HER olaydan
// sonra beş güvenlik invariant'ı ve kümenin diğer denetimleri koşar. Sonda küme bir komutu bütün
// düğümlerde uygulayabilmelidir. Başarısız her seed, seed numarası ve tek satırlık bir yeniden
// üretme komutuyla raporlanır (koşu deterministiktir: aynı komut aynı hatayı aynı olayda verir;
// bkz. `the_chaos_schedule_is_reproducible`).
#[test]
fn raft_safety_holds_across_hundreds_of_seeds() {
    let mut failures = Vec::new();
    let mut totals = ChaosStats::default();
    for seed in CHAOS_SEEDS {
        match chaos_run(seed) {
            Ok(stats) => {
                totals.crashes += stats.crashes;
                totals.restarts += stats.restarts;
                totals.partitions += stats.partitions;
                totals.commands += stats.commands;
                totals.committed += stats.committed;
                totals.lost_writes += stats.lost_writes;
                totals.kept_writes += stats.kept_writes;
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
                    "seed {seed}: {error}\n  reproduce: cargo test -p sim --test chaos -- \
                     raft_safety_holds_across_hundreds_of_seeds --exact\n  replay (from phase 5): \
                     cargo run -p cli -- replay --seed {seed}"
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
    // Tarama boş geçmemeli: hatalar gerçekten enjekte edildi, komutlar gerçekten commit edildi ve
    // disk gerçekten yazma kaybettirdi (hem tamamen hem kısmen). Eşikler ölçülen değerlerin (2904
    // çökme, 2354 yeniden başlatma, 2114 bölünme, 6808 komut, 5936 commit, 667 kayıp ve 77 kısmen
    // yazılmış yazma, liderli 975 term) çok altındadır.
    let seeds = CHAOS_SEEDS.count() as u64;
    assert!(totals.crashes >= seeds, "{totals:?}");
    assert!(totals.restarts >= seeds, "{totals:?}");
    assert!(totals.partitions >= seeds, "{totals:?}");
    assert!(totals.commands >= 10 * seeds, "{totals:?}");
    assert!(totals.committed >= 10 * seeds, "{totals:?}");
    assert!(totals.lost_writes >= seeds, "{totals:?}");
    assert!(totals.kept_writes >= 20, "{totals:?}");
    assert!(totals.terms_with_a_leader as u64 >= 2 * seeds, "{totals:?}");
}

// Taramanın programı seed'e bağlıdır ve tekrarlanabilir: aynı seed iki kez koşulunca aynı sayaçlar
// ve aynı trace özeti çıkar (olay olay aynı koşu). Farklı seed'ler farklı koşular üretir.
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
