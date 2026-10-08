//! Raft log'u: 1'den numaralanan girdiler ve bir adımda değişen kısmın takibi.
//!
//! Index aritmetiği (girdilerin 1'den başlaması, `LogIndex(0)`'ın "log'un öncesi" anlamı, `u64`
//! index ile `usize` konum arasındaki dönüşüm) tek yerde, burada yaşar. Bir-fazla/bir-eksik
//! hataları (off-by-one) Raft uygulamalarının en sık hata kaynağıdır; dağınık `- 1`'ler yerine
//! tek bir dönüşüm noktası bu riski küçültür.

use crate::persist::LogUpdate;
use crate::types::{Command, LogIndex, Term};

/// Log'daki bir girdi (Figure 2: girdi, durum makinesine uygulanacak komutu ve liderin girdiyi
/// aldığı term'i taşır).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// Girdinin lider tarafından alındığı term.
    pub term: Term,
    /// Durum makinesine uygulanacak opak komut (C1).
    pub command: Command,
}

/// Bir düğümün log'u ve bu adımda değişen ilk index.
///
/// Değişiklik takibi O2 içindir: adım sonunda log değiştiyse `Persist`, yalnızca değişen kısmı
/// (`LogUpdate`) taşır. Log'u her adımda kopyalayıp öncekiyle karşılaştırmak log boyutu kadar iş
/// olurdu; bunun yerine log'u değiştiren HER işlem (`append`, `truncate_from`) değişen index'i
/// kendisi işaretler. Alanlar private'tır: log ancak bu işlemlerle değiştirilebilir, yani işaret
/// unutulamaz.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Log {
    entries: Vec<LogEntry>,
    // Bu adımda değişen en küçük index. Adım sonunda `take_update` ile alınıp sıfırlanır.
    changed_from: Option<LogIndex>,
}

impl Log {
    /// Diskten okunmuş (ya da boş) girdilerle bir log. Yüklenen log "değişmiş" sayılmaz: zaten
    /// diskteki hâlidir.
    pub(crate) fn new(entries: Vec<LogEntry>) -> Self {
        Self {
            entries,
            changed_from: None,
        }
    }

    /// Bütün girdiler; dilimdeki 0. konum 1. index'tir.
    pub(crate) fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    /// `index`'in vektördeki konumu: index 1 → 0. Index 0 (log'un öncesi) ya da `usize`'a sığmayan
    /// bir index için `None`.
    fn position(index: LogIndex) -> Option<usize> {
        let offset = index.0.checked_sub(1)?;
        usize::try_from(offset).ok()
    }

    /// Son girdinin index'i; boş log için 0 (Figure 2).
    pub(crate) fn last_index(&self) -> LogIndex {
        // `usize` → `u64` dönüşümü desteklenen bütün platformlarda kayıpsızdır; yine de panik
        // yerine doygunluk (N3).
        LogIndex(u64::try_from(self.entries.len()).unwrap_or(u64::MAX))
    }

    /// Son girdinin term'i; boş log için 0.
    pub(crate) fn last_term(&self) -> Term {
        self.entries.last().map_or(Term(0), |entry| entry.term)
    }

    /// `index`'teki girdi (log'da yoksa `None`).
    pub(crate) fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        self.entries.get(Self::position(index)?)
    }

    /// `index`'teki girdinin term'i. Index 0 log'un öncesidir ve term'i 0 sayılır: boş bir log'a
    /// gönderilen ilk AppendEntries `prevLogIndex = 0, prevLogTerm = 0` taşır ve her log bununla
    /// eşleşir. Log'da olmayan bir index için `None`.
    pub(crate) fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == LogIndex(0) {
            return Some(Term(0));
        }
        self.entry(index).map(|entry| entry.term)
    }

    /// `from`'dan başlayan en fazla `max` girdinin kopyası (bir AppendEntries'in yükü).
    pub(crate) fn entries_from(&self, from: LogIndex, max: usize) -> Vec<LogEntry> {
        let Some(start) = Self::position(from) else {
            return Vec::new();
        };
        self.entries.iter().skip(start).take(max).cloned().collect()
    }

    /// Sona bir girdi ekler.
    pub(crate) fn append(&mut self, entry: LogEntry) {
        self.mark_changed(self.last_index().next());
        self.entries.push(entry);
    }

    /// `index` ve sonrasındaki bütün girdileri siler. `index` log'da yoksa hiçbir şey olmaz
    /// (silinecek bir şey yoktur ve log "değişmiş" sayılmaz).
    pub(crate) fn truncate_from(&mut self, index: LogIndex) {
        let Some(position) = Self::position(index) else {
            return;
        };
        if position < self.entries.len() {
            self.mark_changed(index);
            self.entries.truncate(position);
        }
    }

    fn mark_changed(&mut self, index: LogIndex) {
        self.changed_from = Some(self.changed_from.map_or(index, |from| from.min(index)));
    }

    /// Bu adımda log değiştiyse, değişen kısmı diske yazılacak bir fark olarak verir ve takibi
    /// sıfırlar: `from` index'inden (dahil) sonrası, log'un şimdiki hâliyle değiştirilecektir.
    ///
    /// `from` hiçbir zaman log'un sonundan ilerde değildir: ekleme son index'in bir fazlasını,
    /// kesme var olan bir index'i işaretler; en küçüğü tutulduğu için sonradan yapılan bir kesme
    /// işareti geriye çeker.
    pub(crate) fn take_update(&mut self) -> Option<LogUpdate> {
        let from = self.changed_from.take()?;
        let start = Self::position(from).unwrap_or(0).min(self.entries.len());
        Some(LogUpdate {
            from,
            entries: self.entries[start..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Log, LogEntry};
    use crate::persist::LogUpdate;
    use crate::types::{Command, LogIndex, Term};

    fn entry(term: u64, byte: u8) -> LogEntry {
        LogEntry {
            term: Term(term),
            command: Command::new(vec![byte]),
        }
    }

    // Index aritmetiği: 1'den başlar, 0 log'un öncesidir (term 0), log dışı index'ler `None`.
    #[test]
    fn indices_start_at_one_and_zero_is_before_the_log() {
        let empty = Log::default();
        assert_eq!(empty.last_index(), LogIndex(0));
        assert_eq!(empty.last_term(), Term(0));
        assert_eq!(empty.term_at(LogIndex(0)), Some(Term(0)));
        assert_eq!(empty.term_at(LogIndex(1)), None);

        let log = Log::new(vec![entry(1, 10), entry(1, 11), entry(3, 12)]);
        assert_eq!(log.last_index(), LogIndex(3));
        assert_eq!(log.last_term(), Term(3));
        assert_eq!(log.term_at(LogIndex(2)), Some(Term(1)));
        assert_eq!(log.entry(LogIndex(3)), Some(&entry(3, 12)));
        assert_eq!(log.entry(LogIndex(0)), None);
        assert_eq!(log.entry(LogIndex(4)), None);
        assert_eq!(log.entries_from(LogIndex(2), 1), vec![entry(1, 11)]);
        assert_eq!(
            log.entries_from(LogIndex(2), 9),
            vec![entry(1, 11), entry(3, 12)]
        );
        assert!(log.entries_from(LogIndex(4), 9).is_empty());
        assert!(log.entries_from(LogIndex(0), 9).is_empty());
    }

    // Değişiklik takibi: yüklenen log değişmiş sayılmaz; ekleme ve kesme en küçük değişen index'i
    // işaretler; fark, o index'ten sonrasının şimdiki hâlidir ve alındıktan sonra takip sıfırlanır.
    // Var olmayan bir index'ten kesmek hiçbir şeyi değiştirmez.
    #[test]
    fn changes_are_tracked_from_the_smallest_changed_index() {
        let mut log = Log::new(vec![entry(1, 10), entry(1, 11), entry(1, 12)]);
        assert_eq!(log.take_update(), None);

        log.append(entry(2, 13));
        log.truncate_from(LogIndex(2));
        log.append(entry(2, 14));
        assert_eq!(
            log.take_update(),
            Some(LogUpdate {
                from: LogIndex(2),
                entries: vec![entry(2, 14)],
            })
        );
        assert_eq!(log.take_update(), None, "the update is taken once");

        log.truncate_from(LogIndex(9));
        assert_eq!(
            log.take_update(),
            None,
            "nothing to truncate, nothing changed"
        );
        log.truncate_from(LogIndex(1));
        assert_eq!(
            log.take_update(),
            Some(LogUpdate {
                from: LogIndex(1),
                entries: Vec::new(),
            })
        );
        assert_eq!(log.last_index(), LogIndex(0));
    }
}
