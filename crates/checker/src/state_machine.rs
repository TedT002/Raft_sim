//! State Machine Safety (Figure 3): bir düğüm belirli bir index'teki girdiyi durum makinesine
//! uyguladıysa, hiçbir düğüm aynı index'te farklı bir girdi uygulamaz.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

/// State Machine Safety denetçisi: her index'te uygulanan ilk komutu hatırlar ve her düğümün
/// girdileri 1'den başlayarak, boşluksuz ve tekrarsız sırayla uyguladığını doğrular.
///
/// Sıra denetimi neden burada: durum makinesi (ör. KV) aynı komut dizisini aynı sırayla uygularsa
/// her düğümde aynı duruma ulaşır. Bir girdiyi atlamak ya da iki kez uygulamak, komutlar aynı olsa
/// bile durumları ayırır.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateMachineSafety {
    // index → (ilk uygulayan düğüm, komut)
    applied: BTreeMap<u64, (u64, Vec<u8>)>,
    // düğüm → uygulanması beklenen bir sonraki index
    next_index: BTreeMap<u64, u64>,
}

impl StateMachineSafety {
    /// Henüz hiçbir uygulama görmemiş bir denetçi.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `node`, `index`'teki `command`'ı durum makinesine uyguladı.
    ///
    /// # Errors
    ///
    /// Index beklenen sırada değilse ya da o index'te daha önce farklı bir komut uygulandıysa
    /// [`StateMachineSafetyViolation`].
    pub fn observe_apply(
        &mut self,
        node: u64,
        index: u64,
        command: &[u8],
    ) -> Result<(), StateMachineSafetyViolation> {
        let expected = self.next_index.get(&node).copied().unwrap_or(1);
        if index != expected {
            return Err(StateMachineSafetyViolation::OutOfOrder {
                node,
                expected,
                actual: index,
            });
        }
        match self.applied.entry(index) {
            Entry::Vacant(slot) => {
                slot.insert((node, command.to_vec()));
            }
            Entry::Occupied(slot) => {
                let (first, first_command) = slot.get();
                if first_command != command {
                    return Err(StateMachineSafetyViolation::DifferentCommand {
                        index,
                        first: *first,
                        second: node,
                    });
                }
            }
        }
        self.next_index.insert(node, index.saturating_add(1));
        Ok(())
    }

    /// `node` yeniden başladı: durum makinesi çökmeyle kayboldu ve girdiler 1'den yeniden
    /// uygulanacak.
    pub fn observe_restart(&mut self, node: u64) {
        self.next_index.remove(&node);
    }
}

/// State Machine Safety ihlali.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StateMachineSafetyViolation {
    /// İki düğüm aynı index'te farklı komut uyguladı.
    #[error(
        "state machine safety violated: nodes {first} and {second} applied different commands at \
         index {index}"
    )]
    DifferentCommand {
        /// Index.
        index: u64,
        /// İlk uygulayan düğüm.
        first: u64,
        /// Farklı komutu uygulayan düğüm.
        second: u64,
    },
    /// Bir düğüm bir girdiyi atladı ya da tekrar uyguladı.
    #[error("node {node} applied index {actual} but index {expected} was due")]
    OutOfOrder {
        /// Düğüm.
        node: u64,
        /// Beklenen index.
        expected: u64,
        /// Uygulanan index.
        actual: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::{StateMachineSafety, StateMachineSafetyViolation};

    // Geçmesi gereken geçmiş: iki düğüm aynı komutları aynı sırayla uygular; yeniden başlayan düğüm
    // 1'den yeniden uygular.
    #[test]
    fn identical_applications_in_order_pass() {
        let mut checker = StateMachineSafety::new();
        for node in [1, 2] {
            assert_eq!(checker.observe_apply(node, 1, b"a"), Ok(()));
            assert_eq!(checker.observe_apply(node, 2, b"b"), Ok(()));
        }
        checker.observe_restart(1);
        assert_eq!(checker.observe_apply(1, 1, b"a"), Ok(()));
    }

    // Bilerek bozulmuş geçmişler: aynı index'te farklı komut, atlanan ve tekrarlanan index.
    #[test]
    fn different_commands_and_gaps_are_caught() {
        let mut checker = StateMachineSafety::new();
        let _ = checker.observe_apply(1, 1, b"a");
        assert_eq!(
            checker.observe_apply(2, 1, b"x"),
            Err(StateMachineSafetyViolation::DifferentCommand {
                index: 1,
                first: 1,
                second: 2,
            })
        );
        assert_eq!(
            checker.observe_apply(1, 3, b"c"),
            Err(StateMachineSafetyViolation::OutOfOrder {
                node: 1,
                expected: 2,
                actual: 3,
            })
        );
        assert_eq!(
            checker.observe_apply(1, 1, b"a"),
            Err(StateMachineSafetyViolation::OutOfOrder {
                node: 1,
                expected: 2,
                actual: 1,
            })
        );
    }
}
