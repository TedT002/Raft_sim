//! Raft log'u: 1'den numaralanan girdiler, varsa onların bir önekinin yerini tutan snapshot ve bir
//! adımda değişen kısmın takibi.
//!
//! Index aritmetiği (girdilerin 1'den başlaması, `LogIndex(0)`'ın "log'un öncesi" anlamı, snapshot
//! sonrasında girdilerin kaydırılmış konumları, `u64` index ile `usize` konum arasındaki dönüşüm)
//! tek yerde, burada yaşar. Bir-fazla/bir-eksik hataları (off-by-one) Raft uygulamalarının en sık
//! hata kaynağıdır; dağınık `- 1`'ler yerine tek bir dönüşüm noktası bu riski küçültür. (Diskteki
//! kalıcı durumun aynı hesabı `PersistentState::entry`'dedir.)

use crate::persist::{LogUpdate, Snapshot};
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

/// Bir düğümün log'u, varsa snapshot'ı ve bu adımda değişenler.
///
/// Değişiklik takibi O2 içindir: adım sonunda log ya da snapshot değiştiyse `Persist`, yalnızca
/// değişeni taşır (`take_update`). Log'u her adımda kopyalayıp öncekiyle karşılaştırmak log boyutu
/// kadar iş olurdu; bunun yerine log'u ya da snapshot'ı değiştiren HER işlem (`append`,
/// `truncate_from`, `compact`, `install`) değişikliği kendisi işaretler. Alanlar private'tır: log
/// ancak bu işlemlerle değiştirilebilir, yani işaret unutulamaz.
///
/// Snapshot (§7): `1..=snapshot.last_index` girdileri log'dan atılmış, yerlerini snapshot tutar.
/// Atılan girdilerin yalnızca sonuncusunun term'i (`snapshot.last_term`) bilinir: snapshot'tan
/// hemen sonraki girdinin tutarlılık denetimi (`prevLogTerm`) ona dayanır. Snapshot yokken taban
/// 0'dır ve term'i 0 sayılır: boş bir log'un "öncesi" ile aynı anlam. Snapshot ile log'un tabanı
/// tek bir alandan gelir: ikisi birbirinden ayrı tutulsaydı, biri değişip öteki unutulabilirdi.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Log {
    snapshot: Option<Snapshot>,
    // Snapshot'tan sonraki girdiler: `entries[0]`, snapshot'ın son index'inin bir sonrasıdır.
    entries: Vec<LogEntry>,
    // Bu adımda değişen en küçük index. Adım sonunda `take_update` ile alınıp sıfırlanır.
    changed_from: Option<LogIndex>,
    // Bu adımda snapshot değişti mi (sıkıştırma ya da kurulum)?
    snapshot_changed: bool,
}

impl Log {
    /// Diskten okunmuş (ya da boş) bir log: varsa snapshot ve ardından gelen girdiler. Yüklenen
    /// log "değişmiş" sayılmaz: zaten diskteki hâlidir.
    pub(crate) fn new(snapshot: Option<Snapshot>, entries: Vec<LogEntry>) -> Self {
        Self {
            snapshot,
            entries,
            changed_from: None,
            snapshot_changed: false,
        }
    }

    /// Snapshot'tan sonraki girdiler; dilimdeki 0. konum `snapshot_index + 1`. index'tir.
    pub(crate) fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    /// Varsa snapshot.
    pub(crate) fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// Snapshot'ın kapsadığı son index (snapshot yoksa 0).
    pub(crate) fn snapshot_index(&self) -> LogIndex {
        self.snapshot
            .as_ref()
            .map_or(LogIndex(0), |snapshot| snapshot.last_index)
    }

    /// Snapshot'ın son girdisinin term'i (snapshot yoksa 0).
    fn snapshot_term(&self) -> Term {
        self.snapshot
            .as_ref()
            .map_or(Term(0), |snapshot| snapshot.last_term)
    }

    /// `index`'in vektördeki konumu: snapshot'tan hemen sonraki index → 0. Snapshot'ın kapsadığı
    /// (ya da 0) bir index, ya da `usize`'a sığmayan bir index için `None`.
    fn position(&self, index: LogIndex) -> Option<usize> {
        let offset = index
            .0
            .checked_sub(self.snapshot_index().0)?
            .checked_sub(1)?;
        usize::try_from(offset).ok()
    }

    /// Son girdinin index'i; boş log için snapshot'ın index'i (snapshot da yoksa 0, Figure 2).
    pub(crate) fn last_index(&self) -> LogIndex {
        // `usize` → `u64` dönüşümü desteklenen bütün platformlarda kayıpsızdır; yine de panik
        // yerine doygunluk (N3).
        let len = u64::try_from(self.entries.len()).unwrap_or(u64::MAX);
        LogIndex(self.snapshot_index().0.saturating_add(len))
    }

    /// Son girdinin term'i; boş log için snapshot'ın term'i (snapshot da yoksa 0).
    pub(crate) fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.snapshot_term(), |entry| entry.term)
    }

    /// `index`'teki girdi (log'da yoksa ya da snapshot'a alındıysa `None`).
    pub(crate) fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        self.entries.get(self.position(index)?)
    }

    /// `index`'teki girdinin term'i. Snapshot'ın son index'inin term'i bilinir (boş bir log'da bu
    /// index 0'dır ve term'i 0: boş bir log'a gönderilen ilk AppendEntries `prevLogIndex = 0,
    /// prevLogTerm = 0` taşır ve her log bununla eşleşir). Snapshot'ın kapsadığı daha önceki bir
    /// index'in ya da log'da olmayan bir index'in term'i bilinmez: `None`.
    pub(crate) fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot_index() {
            return Some(self.snapshot_term());
        }
        self.entry(index).map(|entry| entry.term)
    }

    /// `from`'dan başlayan en fazla `max` girdinin kopyası (bir AppendEntries'in yükü). `from`
    /// snapshot'ın içindeyse boştur: o girdiler artık yalnızca snapshot olarak gönderilebilir.
    pub(crate) fn entries_from(&self, from: LogIndex, max: usize) -> Vec<LogEntry> {
        let Some(start) = self.position(from) else {
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
    /// (silinecek bir şey yoktur ve log "değişmiş" sayılmaz). Snapshot'ın kapsadığı bir index'ten
    /// kesmek de etkisizdir: o girdiler commit edilmiştir ve bir çakışma onları silemez (§5.3).
    pub(crate) fn truncate_from(&mut self, index: LogIndex) {
        let Some(position) = self.position(index) else {
            return;
        };
        if position < self.entries.len() {
            self.mark_changed(index);
            self.entries.truncate(position);
        }
    }

    /// Log'u `through` index'ine kadar (dahil) sıkıştırır (§7): o girdiler atılır, yerlerini
    /// durum makinesinin o index'teki hâli (`data`) tutar. Log'un kalanı değişmez, bu yüzden
    /// "değişmiş" işaretlenmez: diskteki fark yalnızca snapshot'tır. `through` snapshot'ın
    /// gerisindeyse, onun aynısıysa ya da log'un ötesindeyse etkisizdir ve `false` döner.
    pub(crate) fn compact(&mut self, through: LogIndex, data: Vec<u8>) -> bool {
        if through <= self.snapshot_index() {
            return false;
        }
        let Some(term) = self.term_at(through) else {
            return false;
        };
        let dropped = self.position(through).map_or(0, |position| position + 1);
        self.entries.drain(..dropped.min(self.entries.len()));
        self.snapshot = Some(Snapshot {
            last_index: through,
            last_term: term,
            data,
        });
        self.snapshot_changed = true;
        true
    }

    /// Bir liderden gelen snapshot'ı kurar (Figure 13, alıcının 6. ve 7. adımları): log'da aynı
    /// index ve term'li bir girdi varsa ondan SONRAKİ girdiler korunur (snapshot, takipçinin zaten
    /// sahip olduğu bir önekin özetidir); yoksa log'un tamamı atılır (takipçinin log'u o noktada
    /// liderinkinden ayrılmıştır ya da eksiktir). Snapshot'tan sonrası "değişmiş" işaretlenir:
    /// diskteki fark, yeni tabanın ardındaki log'u baştan yazar.
    ///
    /// Ön koşul (doğru bir çekirdekte hep sağlanır): snapshot mevcut snapshot'ın ilerisindedir.
    /// Aynı index ve term'li bir snapshot'ta da log'un tamamı korunur; daha gerideki bir
    /// snapshot'ın son girdisi bilinmez (sıkıştırılmıştır) ve log atılır.
    pub(crate) fn install(&mut self, snapshot: Snapshot) {
        let matching = self.term_at(snapshot.last_index) == Some(snapshot.last_term);
        let keep: Vec<LogEntry> = if !matching {
            Vec::new()
        } else if snapshot.last_index == self.snapshot_index() {
            std::mem::take(&mut self.entries)
        } else {
            self.position(snapshot.last_index)
                .map_or_else(Vec::new, |position| {
                    self.entries.iter().skip(position + 1).cloned().collect()
                })
        };
        let base = snapshot.last_index;
        self.entries = keep;
        self.snapshot = Some(snapshot);
        self.snapshot_changed = true;
        // Değişiklik işareti yeni tabana göre yeniden kurulur: eski işaret (varsa) yeni tabanın
        // gerisinde kalmış olabilir.
        self.changed_from = Some(base.next());
    }

    fn mark_changed(&mut self, index: LogIndex) {
        self.changed_from = Some(self.changed_from.map_or(index, |from| from.min(index)));
    }

    /// Bu adımda değişenleri diske yazılacak bir fark olarak verir ve takibi sıfırlar: snapshot
    /// değiştiyse yenisi, log değiştiyse `from` index'inden (dahil) sonrası log'un şimdiki hâliyle
    /// değiştirilecektir. İkisi birlikte döner: snapshot'ı log'un tabanını kaydıran bir değişiklik
    /// olmadan yazmak (ya da tersi) mümkün değildir.
    ///
    /// `from` hiçbir zaman log'un sonundan ilerde değildir: ekleme son index'in bir fazlasını,
    /// kesme var olan bir index'i, kurulum snapshot'ın bir sonrasını işaretler; en küçüğü tutulduğu
    /// için sonradan yapılan bir kesme işareti geriye çeker. Snapshot'ın içine de düşmez: oraya
    /// yalnızca bir sıkıştırma ya da kurulum işaretten SONRA kaydırabilir ve o zaman snapshot da bu
    /// farkla birlikte yazılır; fark tabanın ardından başlar.
    pub(crate) fn take_update(&mut self) -> (Option<Snapshot>, Option<LogUpdate>) {
        let snapshot = std::mem::take(&mut self.snapshot_changed)
            .then(|| self.snapshot.clone())
            .flatten();
        let log = self.changed_from.take().map(|from| {
            let from = from.max(self.snapshot_index().next());
            let start = self.position(from).unwrap_or(0).min(self.entries.len());
            LogUpdate {
                from,
                entries: self.entries[start..].to_vec(),
            }
        });
        (snapshot, log)
    }
}

#[cfg(test)]
mod tests {
    use super::{Log, LogEntry};
    use crate::persist::{LogUpdate, Snapshot};
    use crate::types::{Command, LogIndex, Term};

    fn entry(term: u64, byte: u8) -> LogEntry {
        LogEntry {
            term: Term(term),
            command: Command::new(vec![byte]),
        }
    }

    fn snap(last_index: u64, last_term: u64) -> Snapshot {
        Snapshot {
            last_index: LogIndex(last_index),
            last_term: Term(last_term),
            data: vec![9],
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

        let log = Log::new(None, vec![entry(1, 10), entry(1, 11), entry(3, 12)]);
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
        let mut log = Log::new(None, vec![entry(1, 10), entry(1, 11), entry(1, 12)]);
        assert_eq!(log.take_update(), (None, None));

        log.append(entry(2, 13));
        log.truncate_from(LogIndex(2));
        log.append(entry(2, 14));
        assert_eq!(
            log.take_update(),
            (
                None,
                Some(LogUpdate {
                    from: LogIndex(2),
                    entries: vec![entry(2, 14)],
                })
            )
        );
        assert_eq!(log.take_update(), (None, None), "the update is taken once");

        log.truncate_from(LogIndex(9));
        assert_eq!(
            log.take_update(),
            (None, None),
            "nothing to truncate, nothing changed"
        );
        log.truncate_from(LogIndex(1));
        assert_eq!(
            log.take_update(),
            (
                None,
                Some(LogUpdate {
                    from: LogIndex(1),
                    entries: Vec::new(),
                })
            )
        );
        assert_eq!(log.last_index(), LogIndex(0));
    }

    // Sıkıştırma (§7): `through`'a kadar olan girdiler atılır; index'ler mutlak kalır, snapshot'ın
    // son index'inin term'i bilinir, öncesininki bilinmez. Snapshot'ın gerisine, aynısına ya da
    // log'un ötesine sıkıştırma etkisizdir. Sıkıştırma log'un kalanını "değişmiş" işaretlemez,
    // yalnızca snapshot'ı; snapshot'ın içinden kesmek de etkisizdir (commit edilmiş girdiler).
    #[test]
    fn compaction_keeps_absolute_indices() {
        let mut log = Log::new(
            None,
            vec![entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)],
        );
        assert!(log.compact(LogIndex(2), vec![9]));
        assert_eq!(log.snapshot_index(), LogIndex(2));
        assert_eq!(log.last_index(), LogIndex(4));
        assert_eq!(log.term_at(LogIndex(2)), Some(Term(1)));
        assert_eq!(log.term_at(LogIndex(1)), None);
        assert_eq!(log.entry(LogIndex(2)), None);
        assert_eq!(log.entry(LogIndex(3)), Some(&entry(2, 3)));
        assert_eq!(log.entries_from(LogIndex(2), 9), Vec::new());
        assert_eq!(
            log.entries_from(LogIndex(3), 9),
            vec![entry(2, 3), entry(2, 4)]
        );
        assert_eq!(log.take_update(), (Some(snap(2, 1)), None));

        assert!(
            !log.compact(LogIndex(2), vec![9]),
            "not beyond the snapshot"
        );
        assert!(
            !log.compact(LogIndex(1), vec![9]),
            "not behind the snapshot"
        );
        assert!(!log.compact(LogIndex(5), vec![9]), "not beyond the log");
        log.truncate_from(LogIndex(2));
        assert_eq!(
            log.take_update(),
            (None, None),
            "the snapshot cannot be truncated"
        );

        assert!(log.compact(LogIndex(4), vec![9]));
        assert!(log.entries().is_empty());
        assert_eq!(log.last_index(), LogIndex(4));
        assert_eq!(log.last_term(), Term(2));
        log.append(entry(3, 5));
        assert_eq!(
            log.take_update(),
            (
                Some(snap(4, 2)),
                Some(LogUpdate {
                    from: LogIndex(5),
                    entries: vec![entry(3, 5)],
                })
            )
        );
    }

    // Kurulum (Figure 13): snapshot'ın son girdisiyle eşleşen bir girdi varsa sonrası korunur;
    // yoksa log'un tamamı atılır. Fark, yeni tabanın ardını baştan yazar. Mevcut snapshot'ın
    // aynısı log'u korur.
    #[test]
    fn installing_a_snapshot_keeps_a_matching_suffix_only() {
        let mut matching = Log::new(None, vec![entry(1, 1), entry(1, 2), entry(2, 3)]);
        matching.install(snap(2, 1));
        assert_eq!(matching.entries(), &[entry(2, 3)]);
        assert_eq!(matching.last_index(), LogIndex(3));
        assert_eq!(
            matching.take_update(),
            (
                Some(snap(2, 1)),
                Some(LogUpdate {
                    from: LogIndex(3),
                    entries: vec![entry(2, 3)],
                })
            )
        );

        let mut conflicting = Log::new(None, vec![entry(1, 1), entry(1, 2), entry(2, 3)]);
        conflicting.install(snap(2, 5));
        assert!(conflicting.entries().is_empty());
        assert_eq!(conflicting.last_index(), LogIndex(2));
        assert_eq!(conflicting.last_term(), Term(5));

        let mut short = Log::new(None, vec![entry(1, 1)]);
        short.install(snap(7, 3));
        assert!(short.entries().is_empty());
        assert_eq!(short.last_index(), LogIndex(7));
        assert_eq!(
            short.take_update(),
            (
                Some(snap(7, 3)),
                Some(LogUpdate {
                    from: LogIndex(8),
                    entries: Vec::new(),
                })
            )
        );

        let mut same = Log::new(Some(snap(2, 1)), vec![entry(2, 3)]);
        same.install(snap(2, 1));
        assert_eq!(
            same.entries(),
            &[entry(2, 3)],
            "the same snapshot keeps the log"
        );
    }
}
