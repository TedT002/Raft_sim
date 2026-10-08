//! Kaos taraması: yüzlerce seed'de rastgele çökme, yeniden başlatma, bölünme ve iyileşme; kayıplı
//! ve çoğaltmalı bir ağ; fsync penceresi ve kısmi yazmalarıyla bir disk; cevapları kaybolabilen,
//! zaman aşımında yeniden deneyen istemciler. HER olaydan sonra Figure 3'ün beş güvenlik
//! invariant'ı ile dayanıklılık, çıktı sırası ve commit edilmiş girdilerin korunması denetlenir
//! (bkz. `RaftCluster`). Sonda bütün istemciler yeniden ilerleyebilmeli (canlılık), küme
//! yakınsamalı ve istemci geçmişi linearizable olmalıdır.
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır (raft-core'u doğrudan içe aktarmaz): Faz 5'te
//! `cli` de bağımlılık yönü gereği yalnızca `sim`'i görecek.

use std::collections::BTreeSet;
use std::ops::Range;

use sim::{
    ChaCha8Rng, ClientConfig, ClientDriver, ClientStats, ClusterConfig, ClusterError, Component,
    NetworkConfig, NodeId, OpMix, RaftCluster, SeedTree, TraceKind, chance, check_kv,
    uniform_inclusive,
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
/// Kaos taramasının istemcileri: 4 istemci 4 anahtar üzerinde; cevapların %10'u kaybolur.
const CHAOS_CLIENTS: ClientConfig = ClientConfig {
    clients: 4,
    keys: 4,
    min_think: 2,
    max_think: 10,
    timeout: 2 * T,
    max_attempts: 6,
    reply_loss_prob: 0.1,
    ops: OpMix::DEFAULT,
};
/// Taranan seed'ler.
const CHAOS_SEEDS: Range<u64> = 0..200;
/// Hataların enjekte edildiği süre (tick).
const CHAOS_UNTIL: u64 = 1_200;
/// Sakinleşmeden sonra her istemcinin yeniden ilerlemesi için beklenen en uzun süre (tick).
const SETTLE: u64 = 40 * T;

/// Bir kaos koşusunun özeti: taramanın gerçekten bir şeyleri sınadığını gösteren sayaçlar ve
/// koşunun kimliği olan trace özeti.
#[derive(Debug, Default, PartialEq, Eq)]
struct ChaosStats {
    crashes: u64,
    restarts: u64,
    partitions: u64,
    /// İstemci iş yükünün sayaçları.
    clients: ClientStats,
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

/// Ayaktaki bütün düğümler aynı commitIndex'te, hepsi uygulanmış ve KV tabloları aynı mı?
fn converged(cluster: &RaftCluster) -> bool {
    let live: Vec<NodeId> = cluster.node_ids().filter(|&id| cluster.is_up(id)).collect();
    let Some(&first) = live.first() else {
        return false;
    };
    let commit = |id| cluster.node(id).map(|node| node.commit_index());
    live.iter().all(|&id| {
        commit(id) == commit(first)
            && cluster.node(id).map(|node| node.last_applied()) == commit(first)
            && cluster.kv(id) == cluster.kv(first)
    })
}

/// Bir seed'in kaos koşusu. Hata programı kendi alt-seed akışından gelir (`Component::Scenario`),
/// istemcilerin kararları da kendi akışından (`Component::Workload`): ağın, diskin ve düğümlerin
/// akışlarından bağımsızdırlar; aynı seed her zaman aynı koşuyu üretir.
///
/// İstemciler bölünme sırasında azınlıkta kalmış eski bir lidere de istek verebilir: onun girdileri
/// commit edilemez, iyileşince ezilir (§5.3) ve istemci zaman aşımında yeniden dener.
fn chaos_run(seed: u64) -> Result<ChaosStats, String> {
    let mut cluster = RaftCluster::new(seed, ClusterConfig::new(5, CHAOS_NETWORK))
        .map_err(|error| error.to_string())?;
    let mut clients = ClientDriver::new(seed, CHAOS_CLIENTS).map_err(|error| error.to_string())?;
    let mut program = SeedTree::new(seed).rng_for(Component::Scenario);
    let all = ids(1..6);
    let mut stats = ChaosStats::default();
    let violation = |error: ClusterError| error.to_string();

    let mut now = 0;
    while now < CHAOS_UNTIL {
        now += uniform_inclusive(&mut program, 5, 40);
        clients.run_until(&mut cluster, now).map_err(violation)?;
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

    // Sakinleşme: ağ iyileşir, herkes ayağa kalkar. Her istemci en az bir işlemi daha
    // tamamlayabilmeli: küme yeniden ilerliyor olmalı (canlılık).
    cluster.heal();
    let down: Vec<NodeId> = all
        .iter()
        .copied()
        .filter(|&id| !cluster.is_up(id))
        .collect();
    for id in down {
        cluster.restart(id).map_err(violation)?;
    }
    let before = clients.completed_per_client();
    let deadline = cluster.now() + SETTLE;
    loop {
        let progressed = clients
            .completed_per_client()
            .iter()
            .zip(&before)
            .all(|(after, before)| after > before);
        if progressed {
            break;
        }
        if cluster.now() >= deadline {
            return Err(format!(
                "liveness: some client completed no operation within {SETTLE} ticks after \
                 healing ({:?} -> {:?})",
                before,
                clients.completed_per_client()
            ));
        }
        let next = cluster.now() + 1;
        clients.run_until(&mut cluster, next).map_err(violation)?;
    }
    // Yeni işlem başlatılmaz; bekleyenler biter (cevap ya da vazgeçme) ve küme yakınsar.
    clients.set_issuing(false);
    let deadline = cluster.now() + SETTLE;
    while clients.busy() || !converged(&cluster) {
        if cluster.now() >= deadline {
            return Err(format!(
                "liveness: the cluster did not converge within {SETTLE} ticks (clients busy: {})",
                clients.busy()
            ));
        }
        let next = cluster.now() + 1;
        clients.run_until(&mut cluster, next).map_err(violation)?;
    }
    check_kv(&clients.history()).map_err(|error| error.to_string())?;

    stats.clients = clients.stats();
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

// Yüzlerce seed'de güvenlik ve linearizability: her seed kayıplı ve çoğaltmalı bir ağda rastgele
// çökme, yeniden başlatma, bölünme ve iyileşme yaşar; disk her yazmada bir fsync penceresi açar ve
// çökmeler bekleyen yazmaları kaybettirir ya da bir öneklerini diske ulaştırır; istemcilerin
// cevapları kaybolur ve istekler yeniden denenir. HER olaydan sonra beş güvenlik invariant'ı ve
// kümenin diğer denetimleri koşar; sonda istemci geçmişi denetlenir. Başarısız her seed, seed
// numarası ve tek satırlık bir yeniden üretme komutuyla raporlanır (koşu deterministiktir: aynı
// komut aynı hatayı aynı olayda verir; bkz. `the_chaos_schedule_is_reproducible`).
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
                totals.clients += stats.clients;
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
    // Tarama boş geçmemeli: hatalar gerçekten enjekte edildi, işlemler gerçekten tamamlandı ve
    // commit edildi, cevaplar kayboldu, istekler yeniden denendi ve tekilleştirildi, belirsiz ve
    // kesin başarısız işlemler oluştu, disk gerçekten yazma kaybettirdi (hem tamamen hem kısmen).
    // Eşikler ölçülen değerlerin (2881 çökme, 2401 yeniden başlatma, 2168 bölünme; 20244 işlemden
    // 14982'si tamamlandı, 3299'u belirsiz, 1963'ü kesin başarısız; 1055 tekilleştirilmiş cevap,
    // 5483 kayıp cevap, 33856 NotLeader; 19750 commit, 504 kayıp ve 66 kısmen yazılmış yazma,
    // liderli 1042 term) çok altındadır.
    let seeds = CHAOS_SEEDS.count() as u64;
    assert!(totals.crashes >= seeds, "{totals:?}");
    assert!(totals.restarts >= seeds, "{totals:?}");
    assert!(totals.partitions >= seeds, "{totals:?}");
    assert!(totals.clients.completed >= 20 * seeds, "{totals:?}");
    assert!(totals.clients.abandoned >= seeds, "{totals:?}");
    assert!(totals.clients.failed >= seeds, "{totals:?}");
    assert!(totals.clients.deduplicated >= 2 * seeds, "{totals:?}");
    assert!(totals.clients.lost_replies >= 5 * seeds, "{totals:?}");
    assert!(totals.clients.not_leader >= 10 * seeds, "{totals:?}");
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
