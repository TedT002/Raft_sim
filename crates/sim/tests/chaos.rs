//! Kaos taraması: yüzlerce seed'de rastgele çökme, yeniden başlatma, bölünme, iyileşme ve kayıp
//! oranı değişimi; kayıplı ve çoğaltmalı bir ağ; fsync penceresi ve kısmi yazmalarıyla bir disk;
//! cevapları kaybolabilen, zaman aşımında yeniden deneyen istemciler. HER olaydan sonra Figure 3'ün
//! beş güvenlik invariant'ı ile dayanıklılık, çıktı sırası ve commit edilmiş girdilerin korunması
//! denetlenir (bkz. `RaftCluster`). Sonda bütün istemciler yeniden ilerleyebilmeli (canlılık),
//! küme yakınsamalı ve istemci geçmişi linearizable olmalıdır.
//!
//! Tarama, `raftsim fuzz` ile AYNI programı koşar (`sim::Scenario`, `ScenarioConfig::chaos()`):
//! testte başarısız olan bir seed, basılan `replay` komutuyla birebir yeniden üretilir.
//!
//! Testler yalnızca `sim`'in genel API'sini kullanır (raft-core'u doğrudan içe aktarmaz): `cli` de
//! bağımlılık yönü gereği yalnızca `sim`'i görür.

use std::collections::BTreeSet;
use std::ops::Range;

use sim::{RunStats, Scenario, ScenarioConfig, run};

/// Taranan seed'ler.
const CHAOS_SEEDS: Range<u64> = 0..200;

/// Bir seed'in kaos koşusu (bkz. `sim::scenario`).
fn chaos_run(seed: u64) -> Result<RunStats, String> {
    run(&Scenario::generate(seed, ScenarioConfig::chaos()))
        .outcome
        .map_err(|error| error.to_string())
}

/// Bir taramanın toplam sayaçları.
fn add(totals: &mut RunStats, stats: &RunStats) {
    totals.crashes += stats.crashes;
    totals.restarts += stats.restarts;
    totals.partitions += stats.partitions;
    totals.loss_changes += stats.loss_changes;
    totals.clients += stats.clients;
    totals.committed += stats.committed;
    totals.lost_writes += stats.lost_writes;
    totals.kept_writes += stats.kept_writes;
    totals.terms_with_a_leader += stats.terms_with_a_leader;
}

// Yüzlerce seed'de güvenlik ve linearizability: her seed kayıplı ve çoğaltmalı bir ağda rastgele
// çökme, yeniden başlatma, bölünme, iyileşme ve kayıp oranı değişimi yaşar; disk her yazmada bir
// fsync penceresi açar ve çökmeler bekleyen yazmaları kaybettirir ya da bir öneklerini diske
// ulaştırır; istemcilerin cevapları kaybolur ve istekler yeniden denenir. HER olaydan sonra beş
// güvenlik invariant'ı ve kümenin diğer denetimleri koşar; sonda istemci geçmişi denetlenir.
// Başarısız her seed, seed numarası ve tek satırlık bir yeniden üretme komutuyla raporlanır (koşu
// deterministiktir: aynı komut aynı hatayı aynı olayda verir; bkz.
// `the_chaos_schedule_is_reproducible`).
#[test]
fn raft_safety_holds_across_hundreds_of_seeds() {
    let mut failures = Vec::new();
    let mut totals = RunStats::default();
    for seed in CHAOS_SEEDS {
        match chaos_run(seed) {
            Ok(stats) => add(&mut totals, &stats),
            Err(error) => failures.push((seed, error)),
        }
    }
    if !failures.is_empty() {
        let report: Vec<String> = failures
            .iter()
            .take(5)
            .map(|(seed, error)| {
                // Mutant bir derlemede (ör. `cargo test --features mutation-forget-vote`) bulunan
                // seed, ancak aynı özellikle yeniden üretilir.
                let features = sim::ENABLED_MUTATION
                    .map(|feature| format!(" --features {feature}"))
                    .unwrap_or_default();
                format!(
                    "seed {seed}: {error}\n  reproduce: cargo run -p cli{features} -- replay \
                     --seed {seed}"
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
    // Eşikler ölçülen değerlerin (2441 çökme, 1940 yeniden başlatma, 1709 bölünme, 900 kayıp oranı
    // değişimi; 21689 işlemden 16809'u tamamlandı, 3086'sı belirsiz, 1794'ü kesin başarısız; 1321
    // tekilleştirilmiş cevap, 5539 kayıp cevap, 31413 NotLeader; 21779 commit, 433 kayıp ve 68
    // kısmen yazılmış yazma, liderli 928 term) çok altındadır.
    let seeds = CHAOS_SEEDS.count() as u64;
    assert!(totals.crashes >= seeds, "{totals:?}");
    assert!(totals.restarts >= seeds, "{totals:?}");
    assert!(totals.partitions >= seeds, "{totals:?}");
    assert!(totals.loss_changes >= seeds / 2, "{totals:?}");
    assert!(totals.clients.completed >= 20 * seeds, "{totals:?}");
    assert!(totals.clients.abandoned >= seeds, "{totals:?}");
    assert!(totals.clients.failed >= seeds, "{totals:?}");
    assert!(totals.clients.deduplicated >= 2 * seeds, "{totals:?}");
    assert!(totals.clients.lost_replies >= 5 * seeds, "{totals:?}");
    assert!(totals.clients.not_leader >= 10 * seeds, "{totals:?}");
    assert!(totals.committed >= 10 * seeds, "{totals:?}");
    assert!(totals.lost_writes >= seeds, "{totals:?}");
    assert!(totals.kept_writes >= 20, "{totals:?}");
    assert!(totals.terms_with_a_leader >= 2 * seeds, "{totals:?}");
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
