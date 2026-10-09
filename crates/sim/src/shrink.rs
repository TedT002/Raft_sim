//! Başarısız bir senaryoyu küçültme (shrinking): aynı hatayı veren daha küçük bir senaryo bulur.
//!
//! Senaryo açık bir hata listesi olduğu için (bkz. `scenario` modülü) küçültme, listeden hata
//! çıkarıp koşuyu yeniden denemekten ibarettir. Yöntem delta debugging'in (ddmin) sade bir
//! hâlidir: önce büyük parçalar (listenin yarısı, çeyreği, ...), en sonda tek tek hatalar
//! çıkarılır.
//! Bir aday ancak AYNI hatayı (aynı tür ve aynı invariant; bkz. `RunError::signature`) verirse
//! kabul edilir: aksi hâlde küçültme bir hatayı başka bir hataya çevirip "küçük" bir senaryoyla
//! yanıltabilirdi.
//!
//! Hata bir invariant ihlaliyse ilk adım ucuzdur: hata süresi ihlalin görüldüğü âna kesilir. Koşu o
//! âna kadar birebir aynı kaldığı için ihlal korunur ve sonraki bütün hatalar tek bir koşuyla
//! düşer. En sonda hata süresi, kalan son hatanın anına kısaltılmaya çalışılır.
//!
//! Koşular deterministik olduğu için sonuç da deterministiktir: aynı başarısız senaryo her zaman
//! aynı küçük senaryoya iner.
//!
//! Sonuç yeniden üretilebilir: küçük senaryo, özgün senaryonun `keep_faults(kept)` ve
//! `with_horizon(horizon)` ile elde edilen hâlidir (`raftsim replay --faults ... --horizon ...`).

use crate::scenario::{RunError, Scenario, run};

/// Küçültmenin sonucu.
#[derive(Debug, Clone, PartialEq)]
pub struct Shrunk {
    /// Küçük senaryo.
    pub scenario: Scenario,
    /// Küçük senaryonun hatalarının ÖZGÜN senaryodaki sıraları (`replay --faults` için).
    pub kept: Vec<usize>,
    /// Küçük senaryonun verdiği hata.
    pub error: RunError,
    /// Denenen koşu sayısı.
    pub runs: usize,
    /// Küçültme sonuna kadar gitti mi: bütçe tükenmeden kalan her hata tek tek çıkarılmayı ve hata
    /// süresi kısaltılmayı denedi. `false` ise bütçe tükendi; sonuç daha da küçültülebilir.
    pub complete: bool,
}

/// `original` başarısız bir senaryodur ve `error` onun hatasıdır. En fazla `max_runs` koşu
/// deneyerek aynı hatayı veren daha küçük bir senaryo bulur. Hiçbir küçültme tutmazsa özgün
/// senaryo döner.
#[must_use]
pub fn shrink(original: &Scenario, error: &RunError, max_runs: usize) -> Shrunk {
    shrink_with(original, error, max_runs, |candidate| {
        run(candidate).outcome.err()
    })
}

/// `shrink`'in kendisi; bir adayı koşturma işi `attempt`'e bırakılır (adayın hatası, geçtiyse
/// `None`). Neden ayrı: algoritma (ihlal anında kesme, ddmin, hata süresini kısaltma) gerçek
/// koşudan bağımsızdır ve testlerde, hatası bilinen bir sahte koşuyla birebir sınanabilir. Doğru
/// bir Raft'ta invariant ihlali üretmek ise ancak bilerek eklenmiş bir hatayla mümkündür.
fn shrink_with(
    original: &Scenario,
    error: &RunError,
    max_runs: usize,
    mut attempt: impl FnMut(&Scenario) -> Option<RunError>,
) -> Shrunk {
    let signature = error.signature();
    let mut kept: Vec<usize> = (0..original.faults.len()).collect();
    let mut best = error.clone();
    let mut runs = 0;
    let mut base = original.clone();
    // Aday aynı hatayı veriyor mu? Veriyorsa hatasını döndürür.
    let mut fails_alike = |candidate: &Scenario, runs: &mut usize| -> Option<RunError> {
        *runs += 1;
        attempt(candidate).filter(|found| found.signature() == signature)
    };

    // İhlalin anından sonraki hatalar ihlale katkıda bulunamaz: koşu o âna kadar aynıdır.
    if let RunError::Violation { time, .. } = error
        && *time < original.config.horizon
        && max_runs > 0
    {
        let cut = original.with_horizon(*time);
        if let Some(found) = fails_alike(&cut, &mut runs) {
            kept = (0..cut.faults.len()).collect();
            best = found;
            base = cut;
        }
    }

    // Kalan her hata tek tek çıkarılmayı denedi mi (sonuç "1-minimal" mi)? Hiç hata kalmadıysa
    // denenecek bir şey de yoktur.
    let mut minimal = kept.is_empty();
    let mut chunk = kept.len().div_ceil(2).max(1);
    while !kept.is_empty() && runs < max_runs {
        let mut removed = false;
        let mut start = 0;
        while start < kept.len() && runs < max_runs {
            let end = (start + chunk).min(kept.len());
            let candidate_indices: Vec<usize> =
                kept[..start].iter().chain(&kept[end..]).copied().collect();
            let candidate = base.keep_faults(&candidate_indices);
            if let Some(found) = fails_alike(&candidate, &mut runs) {
                kept = candidate_indices;
                best = found;
                removed = true;
            } else {
                start = end;
            }
        }
        // Tek hatalık bir tur, bütçe yarıda kesmeden sonuna kadar gitti ve hiçbir şey çıkmadı.
        if chunk == 1 && !removed && start >= kept.len() {
            minimal = true;
            break;
        }
        if !removed {
            chunk = chunk.div_ceil(2);
        }
    }

    if kept.is_empty() {
        minimal = true;
    }

    let mut scenario = base.keep_faults(&kept);
    let mut complete = minimal;
    // Hata süresini kalan son hatanın anına kısalt (hiç hata kalmadıysa koşunun başına).
    let horizon = scenario.faults.last().map_or(0, |fault| fault.at);
    if horizon < scenario.config.horizon {
        if runs < max_runs {
            let shorter = scenario.with_horizon(horizon);
            if let Some(found) = fails_alike(&shorter, &mut runs) {
                scenario = shorter;
                best = found;
            }
        } else {
            complete = false;
        }
    }
    Shrunk {
        scenario,
        kept,
        error: best,
        runs,
        complete,
    }
}

#[cfg(test)]
mod tests {
    use super::{Shrunk, shrink, shrink_with};
    use crate::raft::Violation;
    use crate::scenario::{RunError, Scenario, ScenarioConfig, run};
    use raft_core::NodeId;

    /// Küçük senaryo, özgün senaryodan `replay --faults ... --horizon ...` ile birebir elde edilir.
    fn assert_reproducible(original: &Scenario, shrunk: &Shrunk) {
        assert_eq!(
            original
                .keep_faults(&shrunk.kept)
                .with_horizon(shrunk.scenario.config.horizon),
            shrunk.scenario
        );
    }

    // Hatayla ilgisiz hatalar elenir: sakinleşmeye hiç süre tanınmayan bir senaryo, hata
    // programından bağımsız olarak canlılık hatası verir. Küçültme bütün hataları çıkarır, hata
    // süresini sıfıra indirir ve aynı türde hatayı korur; sonuç yeniden koşturulunca aynı hatayı
    // verir.
    #[test]
    fn faults_unrelated_to_the_failure_are_removed() {
        let mut config = ScenarioConfig::chaos();
        config.settle = 0;
        config.horizon = 300;
        let scenario = Scenario::generate(2, config);
        assert!(scenario.faults.len() >= 5, "{}", scenario.faults.len());
        let error = run(&scenario).outcome.expect_err("no time to settle");
        let shrunk = shrink(&scenario, &error, 100);
        assert!(shrunk.kept.is_empty(), "{:?}", shrunk.kept);
        assert!(shrunk.scenario.faults.is_empty());
        assert_eq!(shrunk.scenario.config.horizon, 0);
        assert_eq!(shrunk.error.signature(), "liveness");
        assert!(shrunk.runs <= 100);
        assert!(shrunk.complete);
        assert_reproducible(&scenario, &shrunk);
        let again = run(&shrunk.scenario).outcome.expect_err("still fails");
        assert!(matches!(again, RunError::Liveness(_)));
    }

    // Koşu bütçesi aşılmaz: tek koşuluk bir bütçe yalnızca bir aday dener.
    #[test]
    fn the_run_budget_is_respected() {
        let mut config = ScenarioConfig::chaos();
        config.settle = 0;
        config.horizon = 300;
        let scenario = Scenario::generate(2, config);
        let error = run(&scenario).outcome.expect_err("no time to settle");
        let shrunk = shrink(&scenario, &error, 1);
        assert_eq!(shrunk.runs, 1);
        assert!(
            !shrunk.complete,
            "the budget ran out before the result was minimal"
        );
        assert_reproducible(&scenario, &shrunk);
    }

    // İhlal yolu, hatası bilinen sahte bir koşuyla: koşu yalnızca iki belli hata BİRLİKTE varsa,
    // ikincisinden hemen sonra bir invariant ihlaliyle başarısız olur. Küçültme önce hata süresini
    // ihlal anına keser (sonraki bütün hatalar tek koşuyla düşer), sonra ddmin ile tam o iki hatayı
    // bırakır, en sonda hata süresini kalan son hatanın anına indirir. Sonuç yeniden üretilebilir.
    #[test]
    fn a_violation_shrinks_to_the_faults_it_needs() {
        let mut config = ScenarioConfig::chaos();
        config.horizon = 600;
        let scenario = Scenario::generate(4, config);
        assert!(scenario.faults.len() >= 12, "{}", scenario.faults.len());
        let (first, second) = (2, 7);
        let (at_first, at_second) = (scenario.faults[first].at, scenario.faults[second].at);
        let mut horizons = Vec::new();
        let mut fake = |candidate: &Scenario| {
            horizons.push(candidate.config.horizon);
            let has = |at| candidate.faults.iter().any(|fault| fault.at == at);
            (has(at_first) && has(at_second)).then_some(RunError::Violation {
                time: at_second + 1,
                violation: Violation::PersistAfterOutput { node: NodeId(1) },
            })
        };
        let error = fake(&scenario).expect("the original scenario fails");
        let shrunk = shrink_with(&scenario, &error, 200, &mut fake);
        assert_eq!(shrunk.kept, vec![first, second]);
        assert_eq!(shrunk.scenario.config.horizon, at_second);
        assert_eq!(shrunk.error.signature(), "violation: output order");
        assert!(shrunk.complete);
        assert_reproducible(&scenario, &shrunk);
        // İlk deneme (özgün koşudan sonra) ihlal anında kesilmiş senaryodur ve sonraki bütün
        // adaylar o kesimin içinde kalır.
        assert_eq!(horizons[1], at_second + 1);
        assert!(
            horizons[1..]
                .iter()
                .all(|&horizon| horizon <= at_second + 1)
        );
        assert_eq!(horizons.len() - 1, shrunk.runs);
    }

    // "Aynı hata" türüyle ölçülür: başka bir invariant'ın ihlali ya da başka türden bir hata aday
    // olarak kabul edilmez. Burada her aday başka bir türle başarısız olur; hiçbir hata
    // çıkarılamaz.
    #[test]
    fn a_different_failure_is_not_the_same_failure() {
        let mut config = ScenarioConfig::chaos();
        config.horizon = 200;
        let scenario = Scenario::generate(4, config);
        let error = RunError::Liveness("stuck".to_owned());
        let shrunk = shrink_with(&scenario, &error, 50, |_| {
            Some(RunError::Panic("another bug".to_owned()))
        });
        assert_eq!(shrunk.kept, (0..scenario.faults.len()).collect::<Vec<_>>());
        assert_eq!(shrunk.scenario, scenario);
        assert_eq!(shrunk.error, error);
    }
}
