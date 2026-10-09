//! Log Matching (Figure 3): iki log'da aynı index ve term'li bir girdi varsa, o girdiler aynı
//! komutu taşır ve log'lar o noktaya kadar özdeştir.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::view::LogView;

/// Log Matching denetçisi.
///
/// Her log'u bütün diğer log'larla tek tek karşılaştırmak (düğüm sayısının karesi × log boyu) her
/// adımda yapılamayacak kadar pahalıdır. Bunun yerine görülen her `(index, term)` için o girdinin
/// komutu ve BİR ÖNCEKİ girdinin term'i hatırlanır; her log, bu tek kayıtla karşılaştırılır.
///
/// Neden yeterli (tümevarım): bütün log'lar bu kayıtla uyumluysa ve iki log'da aynı `(i, t)`
/// girdisi varsa, ikisinde de i. girdinin komutu kayıttakidir ve (i-1). girdinin term'i kayıttaki
/// `p`'dir. Yani ikisinde de `(i-1, p)` girdisi vardır ve aynı gerekçe bir önceki index için
/// tekrarlanır; index 1'e kadar inilince iki log'un o noktaya kadar özdeş olduğu çıkar.
///
/// `BTreeMap`: kayıtlar her koşuda aynı sırayla tutulur.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogMatching {
    seen: BTreeMap<(u64, u64), Seen>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    previous_term: u64,
    command: Vec<u8>,
    // Girdiyi ilk gösteren düğüm (ihlal raporu için).
    node: u64,
}

impl LogMatching {
    /// Henüz hiç girdi görmemiş bir denetçi.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `node`'un log'unu `from` index'inden (dahil) itibaren denetler ve kaydeder.
    ///
    /// Önceki girdilerin önceki gözlemlerde denetlendiği ve o zamandan beri değişmediği varsayılır:
    /// sürücü, log'un yalnızca `from`'dan itibaren değiştiğini bildiğinde (ör. diske yazılan
    /// farktan) maliyeti değişen kısımla sınırlar. `from = 1` (ya da 0) bütün log'u denetler.
    ///
    /// Sıkıştırılmış bir önek (§7) denetlenmez: snapshot commit edilmiş girdileri kapsar. Önekten
    /// sonraki ilk girdinin "önceki term"i, önekin son girdisinin bilinen term'idir.
    ///
    /// # Errors
    ///
    /// Bir girdi, daha önce görülen aynı `(index, term)` girdisinden farklı bir komut ya da farklı
    /// bir önceki term taşıyorsa [`LogMatchingViolation`].
    pub fn observe<'a>(
        &mut self,
        node: u64,
        log: impl Into<LogView<'a>>,
        from: u64,
    ) -> Result<(), LogMatchingViolation> {
        let log = log.into();
        for (position, entry) in log.entries.iter().enumerate() {
            let index = log.index_of(position);
            if index < from {
                continue;
            }
            let previous_term = position
                .checked_sub(1)
                .and_then(|before| log.entries.get(before))
                .map_or(log.compacted_term, |before| before.term);
            match self.seen.entry((index, entry.term)) {
                Entry::Vacant(slot) => {
                    slot.insert(Seen {
                        previous_term,
                        command: entry.command.to_vec(),
                        node,
                    });
                }
                Entry::Occupied(slot) => {
                    let seen = slot.get();
                    if seen.command != entry.command {
                        return Err(LogMatchingViolation::DifferentCommand {
                            index,
                            term: entry.term,
                            first: seen.node,
                            second: node,
                        });
                    }
                    if seen.previous_term != previous_term {
                        return Err(LogMatchingViolation::DifferentPrefix {
                            index,
                            term: entry.term,
                            first: seen.node,
                            second: node,
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

/// Log Matching ihlali.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LogMatchingViolation {
    /// Aynı `(index, term)` girdisi iki log'da farklı komut taşıyor.
    #[error(
        "log matching violated: nodes {first} and {second} store different commands at index \
         {index}, term {term}"
    )]
    DifferentCommand {
        /// Girdinin index'i.
        index: u64,
        /// Girdinin term'i.
        term: u64,
        /// Girdiyi ilk gösteren düğüm.
        first: u64,
        /// Farklı girdiyi gösteren düğüm.
        second: u64,
    },
    /// Aynı `(index, term)` girdisi iki log'da bulunuyor ama bir önceki girdiler farklı: log'lar o
    /// noktaya kadar özdeş değil.
    #[error(
        "log matching violated: nodes {first} and {second} agree on index {index}, term {term} but \
         not on the entry before it"
    )]
    DifferentPrefix {
        /// Girdinin index'i.
        index: u64,
        /// Girdinin term'i.
        term: u64,
        /// Girdiyi ilk gösteren düğüm.
        first: u64,
        /// Farklı öneki gösteren düğüm.
        second: u64,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{LogMatching, LogMatchingViolation};
    use crate::view::{EntryView, LogView};
    use proptest::prelude::*;
    use proptest::test_runner::{Config as ProptestConfig, RngSeed};

    /// `(term, komut)` çiftlerinden bir log görünümü.
    fn view(log: &[(u64, Vec<u8>)]) -> Vec<EntryView<'_>> {
        log.iter()
            .map(|(term, command)| EntryView {
                term: *term,
                command,
            })
            .collect()
    }

    fn log(entries: &[(u64, u8)]) -> Vec<(u64, Vec<u8>)> {
        entries
            .iter()
            .map(|&(term, byte)| (term, vec![byte]))
            .collect()
    }

    // Geçmesi gereken geçmiş: ortak bir önekten ayrılan iki log (ayrılan kısımlar farklı
    // term'lerde) ve birinin önekleri; aynı log'un tekrar tekrar gözlenmesi de serbesttir.
    #[test]
    fn diverging_logs_with_distinct_terms_are_consistent() {
        let a = log(&[(1, 10), (1, 11), (2, 12)]);
        let b = log(&[(1, 10), (3, 20), (3, 21)]);
        let mut checker = LogMatching::new();
        for (node, entries) in [(1, &a), (2, &b), (3, &a), (1, &a)] {
            assert_eq!(checker.observe(node, &view(entries), 1), Ok(()));
        }
        assert_eq!(checker.observe(4, &view(&a[..2]), 1), Ok(()));
    }

    // Bilerek bozulmuş geçmişler: aynı `(index, term)`'de farklı komut, ve aynı `(index, term)`
    // girdisinin önünde farklı bir girdi.
    #[test]
    fn a_different_command_or_prefix_is_a_violation() {
        let mut checker = LogMatching::new();
        let _ = checker.observe(1, &view(&log(&[(1, 10), (1, 11)])), 1);
        assert_eq!(
            checker.observe(2, &view(&log(&[(1, 10), (1, 99)])), 1),
            Err(LogMatchingViolation::DifferentCommand {
                index: 2,
                term: 1,
                first: 1,
                second: 2,
            })
        );
        let mut checker = LogMatching::new();
        let _ = checker.observe(1, &view(&log(&[(1, 10), (2, 11)])), 1);
        assert_eq!(
            checker.observe(3, &view(&log(&[(2, 10), (2, 11)])), 1),
            Err(LogMatchingViolation::DifferentPrefix {
                index: 2,
                term: 2,
                first: 1,
                second: 3,
            })
        );
    }

    // `from` ile yalnızca değişen kuyruk denetlenir; değişen kuyruğun ilk girdisinin öncesi yine de
    // dikkate alınır (önceki term karşılaştırması).
    #[test]
    fn observing_from_an_index_checks_the_changed_suffix() {
        let mut checker = LogMatching::new();
        let _ = checker.observe(1, &view(&log(&[(1, 10), (1, 11), (2, 12)])), 1);
        let changed = log(&[(1, 10), (3, 11), (2, 12)]);
        assert_eq!(
            checker.observe(2, &view(&changed), 3),
            Err(LogMatchingViolation::DifferentPrefix {
                index: 3,
                term: 2,
                first: 1,
                second: 2,
            })
        );
    }

    /// Geçerli bir "log ağacı": bir kök log ve ondan ayrılan dallar. Her dal, var olan bir log'un
    /// bir önekine o ana kadar kullanılan bütün term'lerden büyük term'li girdiler ekler; böylece
    /// ayrılan dallar hiçbir `(index, term)` çiftini paylaşmaz. Bu, Raft'ın gerçek log'larının
    /// biçimidir: her yeni liderin girdileri kendi (daha yüksek) term'indedir.
    fn log_tree() -> impl Strategy<Value = Vec<Vec<(u64, Vec<u8>)>>> {
        let root = prop::collection::vec((1_u64..3, any::<u8>()), 0..8);
        let branches = prop::collection::vec(
            (
                any::<prop::sample::Index>(),
                any::<prop::sample::Index>(),
                prop::collection::vec(any::<u8>(), 1..5),
            ),
            0..6,
        );
        (root, branches).prop_map(|(root, branches)| {
            let mut terms: Vec<u64> = root.iter().map(|(term, _)| *term).collect();
            terms.sort_unstable();
            let root: Vec<(u64, Vec<u8>)> = terms
                .into_iter()
                .zip(root)
                .map(|(term, (_, byte))| (term, vec![byte]))
                .collect();
            let mut logs = vec![root];
            // Her dal, kökün term'lerinden (1, 2) ve önceki dallarınkinden büyük, kendine ait bir
            // term alır.
            for (term, (parent, fork, bytes)) in (3_u64..).zip(branches) {
                let parent = logs[parent.index(logs.len())].clone();
                let fork = fork.index(parent.len() + 1);
                let mut child: Vec<(u64, Vec<u8>)> = parent[..fork].to_vec();
                child.extend(bytes.into_iter().map(|byte| (term, vec![byte])));
                logs.push(child);
            }
            logs
        })
    }

    /// Log Matching'in tanımı, kaba kuvvetle: herhangi iki log'da aynı index ve term'li bir girdi
    /// varsa, log'lar o index'e kadar (o dahil) özdeştir. Denetçinin tümevarımlı kaydına hiç
    /// güvenmez; fark testlerinin ölçütüdür.
    fn matching(logs: &[Vec<(u64, Vec<u8>)>]) -> bool {
        logs.iter().all(|a| {
            logs.iter().all(|b| {
                a.iter()
                    .zip(b)
                    .enumerate()
                    .all(|(i, (x, y))| x.0 != y.0 || a[..=i] == b[..=i])
            })
        })
    }

    /// Küçük bir alfabeden rastgele girdiler: 3 term ve 3 komut, böylece aynı `(index, term)`
    /// çiftleri ve çakışmalar sık görülür.
    fn entries(max: usize) -> impl Strategy<Value = Vec<(u64, u8)>> {
        prop::collection::vec((1_u64..4, 0_u8..3), 0..max)
    }

    fn config() -> ProptestConfig {
        ProptestConfig {
            cases: 256,
            failure_persistence: None,
            rng_seed: RngSeed::Fixed(0x5eed_1065),
            ..ProptestConfig::default()
        }
    }

    proptest! {
        #![proptest_config(config())]

        // Geçerli her log ağacının bütün dalları (ve önekleri), hangi sırayla gözlenirse gözlensin
        // Log Matching'i sağlar.
        #[test]
        fn valid_log_trees_never_violate(logs in log_tree(), cut in any::<prop::sample::Index>()) {
            let mut checker = LogMatching::new();
            for (node, log) in logs.iter().enumerate().rev() {
                let node = u64::try_from(node).unwrap_or(u64::MAX);
                prop_assert_eq!(checker.observe(node, &view(log), 1), Ok(()));
                let prefix = &log[..cut.index(log.len() + 1)];
                prop_assert_eq!(checker.observe(node, &view(prefix), 1), Ok(()));
            }
        }

        // Geçerli bir dalın kopyasında tek bir girdinin komutu değiştirilirse, orijinali ve kopyayı
        // gözleyen denetçi ihlali bulur.
        #[test]
        fn a_changed_command_is_always_found(
            logs in log_tree(),
            pick in any::<prop::sample::Index>(),
            at in any::<prop::sample::Index>(),
        ) {
            let original = &logs[pick.index(logs.len())];
            prop_assume!(!original.is_empty());
            let at = at.index(original.len());
            let mut corrupted = original.clone();
            corrupted[at].1.push(0xff);
            let mut checker = LogMatching::new();
            prop_assert_eq!(checker.observe(1, &view(original), 1), Ok(()));
            let found = checker.observe(2, &view(&corrupted), 1).is_err();
            prop_assert!(found);
        }

        // Fark testi (differential): rastgele log'lar, çoğu Log Matching'i çiğneyecek biçimde
        // çekilir. Denetçinin ilk ihlal bildirdiği log, kaba kuvvet tanımının ilk çiğnendiği
        // öneğin son log'udur; hiç ihlal yoksa ikisi de yok der.
        #[test]
        fn the_oracle_agrees_with_the_brute_force_definition(
            raw in prop::collection::vec(entries(6), 1..6),
        ) {
            let logs: Vec<Vec<(u64, Vec<u8>)>> = raw.iter().map(|entries| log(entries)).collect();
            let mut checker = LogMatching::new();
            let first_error = logs.iter().zip(0_u64..).position(|(entries, node)| {
                checker.observe(node, &view(entries), 1).is_err()
            });
            let first_violation = (0..logs.len()).find(|&k| !matching(&logs[..=k]));
            prop_assert_eq!(first_error, first_violation);
        }

        // Aynı fark testi, sürücünün kullandığı biçimde: düğümlerin log'ları adım adım değişir (bir
        // önekten kesilir, sonuna girdi eklenir) ve denetçi yalnızca değişen kısmı (`from`) görür.
        // Kaba kuvvet, o ana kadar görülen BÜTÜN log sürümlerine bakar: aynı `(index, term)` hiçbir
        // zaman iki farklı girdiyi göstermemelidir.
        #[test]
        fn incremental_observation_agrees_with_the_brute_force_definition(
            steps in prop::collection::vec(
                (0_u64..3, any::<prop::sample::Index>(), entries(4)),
                1..12,
            ),
        ) {
            let mut current: BTreeMap<u64, Vec<(u64, Vec<u8>)>> = BTreeMap::new();
            let mut versions = Vec::new();
            let mut checker = LogMatching::new();
            let mut first_error = None;
            for (step, (node, cut, appended)) in steps.iter().enumerate() {
                let entries = current.entry(*node).or_default();
                let keep = cut.index(entries.len() + 1);
                entries.truncate(keep);
                entries.extend(log(appended));
                versions.push(entries.clone());
                let from = u64::try_from(keep).unwrap_or(u64::MAX).saturating_add(1);
                if first_error.is_none() && checker.observe(*node, &view(entries), from).is_err() {
                    first_error = Some(step);
                }
            }
            let first_violation = (0..versions.len()).find(|&k| !matching(&versions[..=k]));
            prop_assert_eq!(first_error, first_violation);
        }

        // Geçerli bir dalın kopyasında bir girdinin term'i değiştirilip sonraki girdi aynen
        // bırakılırsa, sonraki girdinin `(index, term)`'i aynı ama öneki farklıdır: ihlal bulunur.
        #[test]
        fn a_changed_predecessor_is_always_found(
            logs in log_tree(),
            pick in any::<prop::sample::Index>(),
            at in any::<prop::sample::Index>(),
        ) {
            let original = &logs[pick.index(logs.len())];
            prop_assume!(original.len() >= 2);
            let at = at.index(original.len() - 1);
            let mut corrupted = original.clone();
            corrupted[at].0 += 100;
            let mut checker = LogMatching::new();
            prop_assert_eq!(checker.observe(1, &view(original), 1), Ok(()));
            let found = checker.observe(2, &view(&corrupted), 1).is_err();
            prop_assert!(found);
        }
    }

    // Sıkıştırılmış bir log (§7): önekten sonraki ilk girdinin "önceki term"i snapshot'ın son
    // term'idir. Doğru sınır term'iyle tam log'la uyumludur; yanlış sınır term'i, o girdinin
    // önündeki farklı bir önektir.
    #[test]
    fn a_compacted_log_is_checked_from_its_snapshot_boundary() {
        let full = log(&[(1, 10), (1, 11), (2, 12)]);
        let mut checker = LogMatching::new();
        assert_eq!(checker.observe(1, &view(&full), 1), Ok(()));
        let suffix = log(&[(2, 12)]);
        let suffix = view(&suffix);
        assert_eq!(
            checker.observe(2, LogView::compacted(2, 1, &suffix), 1),
            Ok(())
        );
        assert_eq!(
            checker.observe(3, LogView::compacted(2, 2, &suffix), 1),
            Err(LogMatchingViolation::DifferentPrefix {
                index: 3,
                term: 2,
                first: 1,
                second: 3,
            })
        );
    }
}
