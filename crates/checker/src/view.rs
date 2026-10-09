//! Denetçinin log girdilerini gördüğü nötr biçim.

/// Bir log girdisinin denetçiye görünen hâli: term ve komutun baytları.
///
/// Denetçi raft-core'un tiplerini bilmez (bkz. crate belgesi): sürücü, denetlediği uygulamanın log
/// girdilerini bu görünüme çevirir. Komut ödünç alınır (kopyalanmaz): görünüm, log'un üzerinde ucuz
/// bir pencere olarak kalır. Bir log, bu görünümlerin bir dilimidir (bkz. [`LogView`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryView<'a> {
    /// Girdinin term'i.
    pub term: u64,
    /// Girdinin komutunun baytları.
    pub command: &'a [u8],
}

/// Bir log'un denetçiye görünen hâli: varsa snapshot'a alınmış (sıkıştırılmış, §7) bir önek ve
/// ardından gelen girdiler.
///
/// Sıkıştırılmış önekin girdileri görünmez; yalnızca son index'i ve o girdinin term'i bilinir.
/// Kâhinler o öneki denetleyemez, snapshot'ı kapsadığı commit edilmiş girdilerin yerine geçmiş
/// sayarlar. Snapshot'ın kendisinin doğruluğu (yalnızca commit edilmiş girdileri kapsaması ve durum
/// makinesinin o index'teki hâli olması) sürücü tarafında, ayrıca denetlenir.
///
/// Sıkıştırılmamış bir log için `&[EntryView]`'dan dönüşüm vardır: 0. konum 1. index'tir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogView<'a> {
    /// Sıkıştırılmış önekin son index'i (snapshot yoksa 0).
    pub compacted: u64,
    /// O girdinin term'i (snapshot yoksa 0).
    pub compacted_term: u64,
    /// Önekten sonraki girdiler: 0. konum `compacted + 1`. index'tir.
    pub entries: &'a [EntryView<'a>],
}

impl<'a> LogView<'a> {
    /// Sıkıştırılmamış bir log: 0. konum 1. index'tir.
    #[must_use]
    pub fn full(entries: &'a [EntryView<'a>]) -> Self {
        Self::compacted(0, 0, entries)
    }

    /// `compacted` index'ine kadar (dahil) sıkıştırılmış bir log; o girdinin term'i
    /// `compacted_term`'dür, `entries` ondan sonrasıdır.
    #[must_use]
    pub fn compacted(compacted: u64, compacted_term: u64, entries: &'a [EntryView<'a>]) -> Self {
        Self {
            compacted,
            compacted_term,
            entries,
        }
    }

    /// Son girdinin index'i (boş bir log için sıkıştırılmış önekin son index'i).
    pub(crate) fn last_index(&self) -> u64 {
        let len = u64::try_from(self.entries.len()).unwrap_or(u64::MAX);
        self.compacted.saturating_add(len)
    }

    /// `index`'teki girdi; sıkıştırılmış önekte ya da log'un ötesindeyse `None`.
    pub(crate) fn entry(&self, index: u64) -> Option<EntryView<'a>> {
        let position = index.checked_sub(self.compacted)?.checked_sub(1)?;
        self.entries.get(usize::try_from(position).ok()?).copied()
    }

    /// `position`. girdinin (0'dan) index'i.
    pub(crate) fn index_of(&self, position: usize) -> u64 {
        let position = u64::try_from(position).unwrap_or(u64::MAX);
        self.compacted.saturating_add(position).saturating_add(1)
    }
}

impl<'a> From<&'a [EntryView<'a>]> for LogView<'a> {
    fn from(entries: &'a [EntryView<'a>]) -> Self {
        Self::full(entries)
    }
}

impl<'a> From<&'a Vec<EntryView<'a>>> for LogView<'a> {
    fn from(entries: &'a Vec<EntryView<'a>>) -> Self {
        Self::full(entries)
    }
}

impl<'a, const N: usize> From<&'a [EntryView<'a>; N]> for LogView<'a> {
    fn from(entries: &'a [EntryView<'a>; N]) -> Self {
        Self::full(entries)
    }
}
