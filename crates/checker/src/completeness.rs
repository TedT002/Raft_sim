//! Leader Completeness (Figure 3): bir girdi bir term'de commit edildiyse, daha yüksek term'lerin
//! bütün liderlerinin log'unda bulunur.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::view::LogView;

/// Leader Completeness denetçisi.
///
/// "Commit edildi" bilgisi düğümlerin gözlenen `commitIndex`'lerinden gelir: bir düğümün
/// commitIndex'i `n`'ye ilerlediğinde, o düğümün log'undaki 1..=n girdileri commit edilmiş sayılır
/// ve kaydedilir. Kayıt iki şeyi sağlar:
///
/// - Commit edilmiş bir index'in girdisi bir daha asla değişemez: başka bir düğüm aynı index için
///   farklı bir girdiyi commit ederse bu, Leader Completeness'in çiğnendiğinin doğrudan kanıtıdır
///   (her yeni lider commit edilmiş girdiyi taşısaydı, takipçiler de ondan aynısını alırdı).
/// - Her girdinin commit edildiği term (onu ilk commit ettiği gözlenen düğümün term'i; ilk gözlem
///   commit eden liderin kendisidir) saklanır ve daha yüksek term'lerin liderleri bu girdiyi
///   içermek zorundadır.
///
/// Denetçi uygulamanın commit kararlarına güvenmez, sonuçlarını doğrular: §5.4.2'yi çiğneyen bir
/// lider önceki term'den bir girdiyi "commit" ederse, o girdiyi taşımayan sonraki bir lider bu
/// denetçiye takılır.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaderCompleteness {
    // index → commit edilmiş girdi
    committed: BTreeMap<u64, Committed>,
    // düğüm → o düğümden kaydedilmiş en yüksek commitIndex
    commit_seen: BTreeMap<u64, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Committed {
    term: u64,
    command: Vec<u8>,
    // Girdinin commit edildiği (gözlenen en küçük) term.
    commit_term: u64,
    // Girdiyi ilk commit ettiği gözlenen düğüm (ihlal raporu için).
    node: u64,
}

impl LeaderCompleteness {
    /// Henüz hiçbir commit görmemiş bir denetçi.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `term`'deki `node`'un commitIndex'i `commit_index` oldu; log'u `log`. Yeni commit edilen
    /// girdiler kaydedilir ve index'leri döndürülür (sürücü, var olan liderleri bunlara karşı
    /// [`LeaderCompleteness::check_entries`] ile denetler).
    ///
    /// # Errors
    ///
    /// Commit edilmiş bir index'te farklı bir girdi, log'un ötesine taşan bir commitIndex ya da
    /// (yeniden başlatma olmadan) geri giden bir commitIndex gözlenirse
    /// [`LeaderCompletenessViolation`].
    pub fn observe_commit<'a>(
        &mut self,
        node: u64,
        term: u64,
        commit_index: u64,
        log: impl Into<LogView<'a>>,
    ) -> Result<Vec<u64>, LeaderCompletenessViolation> {
        let log = log.into();
        let seen = self.commit_seen.get(&node).copied().unwrap_or(0);
        if commit_index < seen {
            return Err(LeaderCompletenessViolation::CommitIndexDecreased {
                node,
                from: seen,
                to: commit_index,
            });
        }
        if commit_index > log.last_index() {
            return Err(LeaderCompletenessViolation::CommitBeyondLog {
                node,
                commit_index,
                last_index: log.last_index(),
            });
        }
        let mut newly_committed = Vec::new();
        for index in seen.saturating_add(1)..=commit_index {
            let Some(entry) = log.entry(index) else {
                // Sıkıştırılmış önekteki bir index (§7): girdi görünmez. Snapshot yalnızca commit
                // edilmiş girdileri kapsar; kaydı varsa önekin son index'inin term'i
                // karşılaştırılır.
                if index == log.compacted
                    && let Some(committed) = self.committed.get(&index)
                    && committed.term != log.compacted_term
                {
                    return Err(LeaderCompletenessViolation::CommittedEntryChanged {
                        index,
                        first: committed.node,
                        second: node,
                    });
                }
                continue;
            };
            match self.committed.entry(index) {
                Entry::Vacant(slot) => {
                    slot.insert(Committed {
                        term: entry.term,
                        command: entry.command.to_vec(),
                        commit_term: term,
                        node,
                    });
                    newly_committed.push(index);
                }
                Entry::Occupied(mut slot) => {
                    let committed = slot.get_mut();
                    if committed.term != entry.term || committed.command != entry.command {
                        return Err(LeaderCompletenessViolation::CommittedEntryChanged {
                            index,
                            first: committed.node,
                            second: node,
                        });
                    }
                    committed.commit_term = committed.commit_term.min(term);
                }
            }
        }
        self.commit_seen.insert(node, commit_index);
        Ok(newly_committed)
    }

    /// `node` yeniden başladı: commitIndex geçicidir ve 0'dan başlar.
    pub fn observe_restart(&mut self, node: u64) {
        self.commit_seen.remove(&node);
    }

    /// `term`'ün lideri `node`'un log'u, daha düşük bir term'de commit edilmiş BÜTÜN girdileri
    /// içeriyor mu? Bir lider ilk kez gözlendiğinde çağrılır.
    ///
    /// # Errors
    ///
    /// Böyle bir girdi eksik ya da farklıysa
    /// [`LeaderCompletenessViolation::MissingCommittedEntry`].
    pub fn check_leader<'a>(
        &self,
        node: u64,
        term: u64,
        log: impl Into<LogView<'a>>,
    ) -> Result<(), LeaderCompletenessViolation> {
        self.check_entries(node, term, log, self.committed.keys().copied())
    }

    /// [`LeaderCompleteness::check_leader`]'ın yalnızca verilen index'lerle sınırlı hâli: yeni
    /// commit edilen girdileri var olan liderlere karşı denetlemek için.
    ///
    /// # Errors
    ///
    /// Bkz. [`LeaderCompleteness::check_leader`].
    pub fn check_entries<'a>(
        &self,
        node: u64,
        term: u64,
        log: impl Into<LogView<'a>>,
        indices: impl IntoIterator<Item = u64>,
    ) -> Result<(), LeaderCompletenessViolation> {
        let log = log.into();
        for index in indices {
            let Some(committed) = self.committed.get(&index) else {
                continue;
            };
            if committed.commit_term >= term {
                continue;
            }
            // Sıkıştırılmış önekteki bir girdi snapshot'ta sayılır (snapshot commit edilmiş
            // girdileri kapsar; doğruluğu sürücüde ayrıca denetlenir); önekin son girdisinin term'i
            // bilinir ve karşılaştırılır.
            let present = if index <= log.compacted {
                index < log.compacted || log.compacted_term == committed.term
            } else {
                log.entry(index).is_some_and(|entry| {
                    entry.term == committed.term && entry.command == committed.command
                })
            };
            if !present {
                return Err(LeaderCompletenessViolation::MissingCommittedEntry {
                    leader: node,
                    term,
                    index,
                    commit_term: committed.commit_term,
                });
            }
        }
        Ok(())
    }
}

/// Leader Completeness ihlali.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LeaderCompletenessViolation {
    /// Daha yüksek bir term'in lideri, commit edilmiş bir girdiyi taşımıyor.
    #[error(
        "leader completeness violated: the leader of term {term}, node {leader}, lacks the entry \
         at index {index} committed in term {commit_term}"
    )]
    MissingCommittedEntry {
        /// Lider.
        leader: u64,
        /// Liderin term'i.
        term: u64,
        /// Eksik girdinin index'i.
        index: u64,
        /// Girdinin commit edildiği term.
        commit_term: u64,
    },
    /// Commit edilmiş bir index için iki farklı girdi commit edildi.
    #[error(
        "leader completeness violated: nodes {first} and {second} committed different entries at \
         index {index}"
    )]
    CommittedEntryChanged {
        /// Index.
        index: u64,
        /// İlk commit eden düğüm.
        first: u64,
        /// Farklı girdiyi commit eden düğüm.
        second: u64,
    },
    /// Bir düğümün commitIndex'i yeniden başlatma olmadan geri gitti.
    #[error("commit index of node {node} went backwards from {from} to {to}")]
    CommitIndexDecreased {
        /// Düğüm.
        node: u64,
        /// Önceki commitIndex.
        from: u64,
        /// Yeni commitIndex.
        to: u64,
    },
    /// Bir düğümün commitIndex'i kendi log'unun sonunu aşıyor.
    #[error("commit index {commit_index} of node {node} is beyond its last log index {last_index}")]
    CommitBeyondLog {
        /// Düğüm.
        node: u64,
        /// commitIndex.
        commit_index: u64,
        /// Log'un son index'i.
        last_index: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::{LeaderCompleteness, LeaderCompletenessViolation};
    use crate::view::{EntryView, LogView};

    fn view(entries: &[(u64, Vec<u8>)]) -> Vec<EntryView<'_>> {
        entries
            .iter()
            .map(|(term, command)| EntryView {
                term: *term,
                command,
            })
            .collect()
    }

    // Geçmesi gereken geçmiş: term 2'nin lideri iki girdiyi commit eder, takipçi aynısını commit
    // eder; term 3'ün lideri ikisini de taşır. Aynı commitIndex'in tekrar gözlenmesi yeni girdi
    // üretmez; eski bir lider (term 2) daha sonraki commit'leri taşımak zorunda değildir.
    #[test]
    fn leaders_that_hold_every_committed_entry_pass() {
        let log = vec![(1, vec![1]), (2, vec![2])];
        let mut checker = LeaderCompleteness::new();
        assert_eq!(checker.observe_commit(1, 2, 2, &view(&log)), Ok(vec![1, 2]));
        assert_eq!(checker.observe_commit(2, 2, 2, &view(&log)), Ok(Vec::new()));
        let longer = vec![(1, vec![1]), (2, vec![2]), (3, vec![3])];
        assert_eq!(checker.check_leader(3, 3, &view(&longer)), Ok(()));
        assert_eq!(checker.check_leader(1, 2, &view(&log[..1])), Ok(()));
    }

    // Bilerek bozulmuş geçmişler: (a) commit edilmiş girdiyi taşımayan daha yüksek term'li bir
    // lider; (b) aynı index için iki farklı girdinin commit edilmesi; (c) log'un ötesine ya da
    // geriye giden commitIndex. Yeniden başlatma commitIndex'in 0'dan başlamasına izin verir.
    #[test]
    fn missing_or_changed_committed_entries_are_caught() {
        let log = vec![(1, vec![1]), (2, vec![2])];
        let mut checker = LeaderCompleteness::new();
        let _ = checker.observe_commit(1, 2, 2, &view(&log));
        let other = vec![(1, vec![1]), (3, vec![9])];
        assert_eq!(
            checker.check_leader(4, 3, &view(&other)),
            Err(LeaderCompletenessViolation::MissingCommittedEntry {
                leader: 4,
                term: 3,
                index: 2,
                commit_term: 2,
            })
        );
        assert_eq!(
            checker.observe_commit(5, 3, 2, &view(&other)),
            Err(LeaderCompletenessViolation::CommittedEntryChanged {
                index: 2,
                first: 1,
                second: 5,
            })
        );
        assert_eq!(
            checker.observe_commit(1, 2, 3, &view(&log)),
            Err(LeaderCompletenessViolation::CommitBeyondLog {
                node: 1,
                commit_index: 3,
                last_index: 2,
            })
        );
        assert_eq!(
            checker.observe_commit(1, 2, 1, &view(&log)),
            Err(LeaderCompletenessViolation::CommitIndexDecreased {
                node: 1,
                from: 2,
                to: 1,
            })
        );
        checker.observe_restart(1);
        assert_eq!(checker.observe_commit(1, 2, 1, &view(&log)), Ok(Vec::new()));
    }

    // Sıkıştırılmış log'lar (§7): snapshot'ın kapsadığı commit edilmiş girdiler snapshot'ta
    // sayılır; sınırdaki (snapshot'ın son) girdinin term'i karşılaştırılır. Bilerek bozulmuş
    // sınırlar: yanlış term'li bir liderin snapshot'ı commit edilmiş girdiyi taşımaz; yanlış
    // term'li bir düğüm snapshot'ı, commit edilmiş girdinin değiştiği anlamına gelir.
    #[test]
    fn compacted_prefixes_hold_committed_entries_up_to_their_boundary() {
        let log = vec![(1, vec![1]), (1, vec![2]), (2, vec![3])];
        let mut checker = LeaderCompleteness::new();
        assert_eq!(
            checker.observe_commit(1, 2, 3, &view(&log)),
            Ok(vec![1, 2, 3])
        );
        let rest = view(&log[2..]);
        assert_eq!(
            checker.check_leader(2, 3, LogView::compacted(2, 1, &rest)),
            Ok(())
        );
        assert_eq!(
            checker.check_leader(2, 3, LogView::compacted(2, 5, &rest)),
            Err(LeaderCompletenessViolation::MissingCommittedEntry {
                leader: 2,
                term: 3,
                index: 2,
                commit_term: 2,
            })
        );
        let nothing: Vec<EntryView<'_>> = Vec::new();
        assert_eq!(
            checker.observe_commit(3, 3, 3, LogView::compacted(3, 9, &nothing)),
            Err(LeaderCompletenessViolation::CommittedEntryChanged {
                index: 3,
                first: 1,
                second: 3,
            })
        );
        assert_eq!(
            checker.observe_commit(4, 3, 3, LogView::compacted(3, 2, &nothing)),
            Ok(Vec::new())
        );
    }
}
