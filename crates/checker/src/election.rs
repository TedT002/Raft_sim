//! Election Safety (Figure 3): bir term'de en fazla bir lider seçilebilir.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

/// Election Safety denetçisi: koşu boyunca gözlenen her `(term, lider)` çiftini hatırlar.
///
/// Aynı term için ikinci, FARKLI bir lider gözlendiğinde ihlal bildirir. Yalnızca "şu anki"
/// liderlere bakmak yetmezdi: term 5'in lideri çöktükten sonra aynı term'de başka bir düğüm lider
/// olursa, iki lider hiçbir anda birlikte görünmez ama invariant yine de çiğnenmiştir. Bu yüzden
/// denetçi koşunun bütün geçmişini tutar.
///
/// Term ve düğüm kimlikleri düz `u64`'tür: denetçi raft-core'un tiplerini bilmez (bkz. crate
/// belgesi). `BTreeMap`: kayıtlar term sırasıyla tutulur; yineleme sırası her koşuda aynıdır.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ElectionSafety {
    leaders: BTreeMap<u64, u64>,
}

impl ElectionSafety {
    /// Henüz hiç lider gözlenmemiş bir denetçi.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `node`'un `term`'de lider olarak gözlendiğini kaydeder.
    ///
    /// Aynı liderin aynı term'de tekrar tekrar gözlenmesi olağandır (denetçi her adımdan sonra
    /// çağrılır) ve kabul edilir.
    ///
    /// # Errors
    ///
    /// Bu term'de daha önce BAŞKA bir lider gözlendiyse [`ElectionSafetyViolation`]. Kayıt
    /// değişmez: term'in ilk gözlenen lideri, ihlalden sonra da onun lideri olarak kalır.
    pub fn observe_leader(&mut self, term: u64, node: u64) -> Result<(), ElectionSafetyViolation> {
        match self.leaders.entry(term) {
            Entry::Vacant(slot) => {
                slot.insert(node);
                Ok(())
            }
            Entry::Occupied(slot) if *slot.get() == node => Ok(()),
            Entry::Occupied(slot) => Err(ElectionSafetyViolation {
                term,
                first: *slot.get(),
                second: node,
            }),
        }
    }

    /// `term`'de gözlenen lider (hiç gözlenmediyse `None`).
    #[must_use]
    pub fn leader_of(&self, term: u64) -> Option<u64> {
        self.leaders.get(&term).copied()
    }
}

/// Election Safety ihlali: aynı term'de iki farklı lider gözlendi.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("election safety violated: term {term} has two leaders, node {first} and node {second}")]
pub struct ElectionSafetyViolation {
    /// İki liderin gözlendiği term.
    pub term: u64,
    /// Bu term'de ilk gözlenen lider.
    pub first: u64,
    /// Aynı term'de sonradan gözlenen ikinci lider.
    pub second: u64,
}

#[cfg(test)]
mod tests {
    use super::{ElectionSafety, ElectionSafetyViolation};

    // Geçmesi gereken geçmiş: her term'de tek lider. Aynı lider aynı term'de tekrar tekrar
    // gözlenebilir; farklı term'lerin liderleri aynı ya da farklı düğümler olabilir.
    #[test]
    fn one_leader_per_term_is_accepted() {
        let mut checker = ElectionSafety::new();
        for (term, node) in [(1, 1), (1, 1), (2, 3), (3, 3), (3, 3), (7, 2)] {
            assert_eq!(checker.observe_leader(term, node), Ok(()));
        }
        assert_eq!(checker.leader_of(2), Some(3));
        assert_eq!(checker.leader_of(4), None);
    }

    // Bilerek bozulmuş geçmiş: term 4'te iki farklı lider. İkinci lider ilk liderle birlikte
    // gözlenmese de (ör. ilki çöktükten sonra seçilmiş olsa da) ihlal yakalanır, çünkü denetçi
    // geçmişi hatırlar. Kayıt ihlalden sonra değişmez.
    #[test]
    fn a_second_leader_in_the_same_term_is_a_violation() {
        let mut checker = ElectionSafety::new();
        assert_eq!(checker.observe_leader(4, 1), Ok(()));
        assert_eq!(checker.observe_leader(5, 2), Ok(()));
        let violation = ElectionSafetyViolation {
            term: 4,
            first: 1,
            second: 3,
        };
        assert_eq!(checker.observe_leader(4, 3), Err(violation));
        assert_eq!(checker.leader_of(4), Some(1));
        assert_eq!(
            violation.to_string(),
            "election safety violated: term 4 has two leaders, node 1 and node 3"
        );
    }
}
