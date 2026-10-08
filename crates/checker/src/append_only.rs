//! Leader Append-Only (Figure 3): bir lider kendi log'undaki girdileri asla silmez ya da
//! değiştirmez; yalnızca sona ekler.

use std::collections::BTreeMap;

use crate::view::EntryView;

/// Leader Append-Only denetçisi: her term'in liderinin en son gözlenen log'unu saklar ve bir
/// sonraki gözlemin onun devamı olduğunu doğrular.
///
/// Neden kopya saklanıyor: liderin log'u yalnızca istemci komutu geldiğinde değişir, yani gözlemler
/// seyrektir; kopya, denetçiyi uygulamanın "şu kısım değişti" beyanına güvenmekten kurtarır. Kâhin,
/// denetlediği uygulamanın iddialarına değil kendi gözlemlerine dayanmalıdır.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaderAppendOnly {
    // term → o term'in liderinin en son gözlemi
    observed: BTreeMap<u64, LeaderSnapshot>,
}

/// Bir liderin en son gözlenen log'u: `(term, komut)` girdileri.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaderSnapshot {
    node: u64,
    log: Vec<(u64, Vec<u8>)>,
}

impl LeaderAppendOnly {
    /// Henüz hiç lider görmemiş bir denetçi.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `term`'ün lideri `node`'un log'unu kaydeder; aynı term'de aynı liderin daha önce gözlenmiş
    /// log'u varsa yeni log'un onu önek olarak içerdiğini doğrular.
    ///
    /// Aynı term'de farklı bir lider gözlenirse (Election Safety ihlali, ayrıca denetlenir) yeni
    /// lider için yeniden başlanır: burada iki ihlal birbirine karışmasın.
    ///
    /// # Errors
    ///
    /// Önceki gözlemdeki bir girdi silinmiş ya da değiştirilmişse [`LeaderAppendOnlyViolation`].
    pub fn observe(
        &mut self,
        term: u64,
        node: u64,
        log: &[EntryView<'_>],
    ) -> Result<(), LeaderAppendOnlyViolation> {
        if let Some(previous) = self.observed.get(&term)
            && previous.node == node
        {
            for (position, (old_term, old_command)) in previous.log.iter().enumerate() {
                let kept = log
                    .get(position)
                    .is_some_and(|entry| entry.term == *old_term && entry.command == old_command);
                if !kept {
                    return Err(LeaderAppendOnlyViolation {
                        term,
                        node,
                        index: u64::try_from(position)
                            .unwrap_or(u64::MAX)
                            .saturating_add(1),
                    });
                }
            }
        }
        let snapshot = LeaderSnapshot {
            node,
            log: log
                .iter()
                .map(|entry| (entry.term, entry.command.to_vec()))
                .collect(),
        };
        self.observed.insert(term, snapshot);
        Ok(())
    }

    /// `node` çöktü: onun lider olarak gözlendiği bütün term'lerin kayıtları unutulur.
    ///
    /// Neden güvenli: Raft'ta bir düğüm, term'i diske ulaşmadan ancak tek başına bir küme
    /// oluşturuyorsa lider olabilir; başka her kümede oylar adayın term'i kalıcı olduktan sonra
    /// gelir. Term'i kalıcı olan bir lider çökerse aynı term'de bir daha lider olamaz, yani
    /// unutulan kayıt bir daha gerekmez. Term'i kalıcı olmayan (tek düğümlü kümedeki) bir lider
    /// çökerse term diskten silinmiş olabilir ve düğüm aynı term'i başka bir log'la yeniden
    /// kazanabilir. Bu yeni bir liderliktir ve önceki kayıtla karşılaştırılmamalıdır.
    pub fn observe_restart(&mut self, node: u64) {
        self.observed.retain(|_, snapshot| snapshot.node != node);
    }
}

/// Leader Append-Only ihlali: bir lider, lider olduğu term içinde kendi log'undaki bir girdiyi
/// sildi ya da değiştirdi.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "leader append-only violated: the leader of term {term}, node {node}, removed or changed its \
     entry at index {index}"
)]
pub struct LeaderAppendOnlyViolation {
    /// Liderin term'i.
    pub term: u64,
    /// Lider.
    pub node: u64,
    /// Silinen ya da değişen ilk girdinin index'i.
    pub index: u64,
}

#[cfg(test)]
mod tests {
    use super::{LeaderAppendOnly, LeaderAppendOnlyViolation};
    use crate::view::EntryView;

    fn view(entries: &[(u64, Vec<u8>)]) -> Vec<EntryView<'_>> {
        entries
            .iter()
            .map(|(term, command)| EntryView {
                term: *term,
                command,
            })
            .collect()
    }

    // Geçmesi gereken geçmiş: lider log'unu uzatır; aynı log'un yeniden gözlenmesi serbesttir;
    // başka bir term'in lideri (aynı düğüm bile olsa) baştan başlar.
    #[test]
    fn a_growing_leader_log_is_accepted() {
        let mut checker = LeaderAppendOnly::new();
        let short = vec![(1, vec![1])];
        let long = vec![(1, vec![1]), (2, vec![2]), (2, vec![3])];
        assert_eq!(checker.observe(2, 1, &view(&short)), Ok(()));
        assert_eq!(checker.observe(2, 1, &view(&long)), Ok(()));
        assert_eq!(checker.observe(2, 1, &view(&long)), Ok(()));
        assert_eq!(checker.observe(3, 1, &view(&short)), Ok(()));
    }

    // Bilerek bozulmuş geçmişler: lider bir girdiyi değiştirir ya da log'unu kısaltır.
    #[test]
    fn a_leader_that_changes_or_removes_an_entry_is_caught() {
        let mut checker = LeaderAppendOnly::new();
        let log = vec![(1, vec![1]), (2, vec![2])];
        let _ = checker.observe(2, 1, &view(&log));
        let changed = vec![(1, vec![1]), (2, vec![9])];
        assert_eq!(
            checker.observe(2, 1, &view(&changed)),
            Err(LeaderAppendOnlyViolation {
                term: 2,
                node: 1,
                index: 2,
            })
        );
        let mut checker = LeaderAppendOnly::new();
        let _ = checker.observe(2, 1, &view(&log));
        assert_eq!(
            checker.observe(2, 1, &view(&log[..1])),
            Err(LeaderAppendOnlyViolation {
                term: 2,
                node: 1,
                index: 2,
            })
        );
    }

    // Çökme, YALNIZCA çöken düğümün kayıtlarını unutturur: aynı term'i yeniden kazanan düğüm baştan
    // başlar, başka bir düğümün kaydı ise korunur ve denetlenmeye devam eder.
    #[test]
    fn a_restart_forgets_only_the_restarted_nodes_leaderships() {
        let mut checker = LeaderAppendOnly::new();
        let log = vec![(1, vec![1]), (1, vec![2])];
        let _ = checker.observe(1, 1, &view(&log));
        let _ = checker.observe(2, 2, &view(&log));
        checker.observe_restart(1);
        assert_eq!(checker.observe(1, 1, &view(&log[..1])), Ok(()));
        assert_eq!(
            checker.observe(2, 2, &view(&log[..1])),
            Err(LeaderAppendOnlyViolation {
                term: 2,
                node: 2,
                index: 2,
            })
        );
    }
}
