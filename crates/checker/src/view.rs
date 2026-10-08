//! Denetçinin log girdilerini gördüğü nötr biçim.

/// Bir log girdisinin denetçiye görünen hâli: term ve komutun baytları.
///
/// Denetçi raft-core'un tiplerini bilmez (bkz. crate belgesi): sürücü, denetlediği uygulamanın log
/// girdilerini bu görünüme çevirir. Komut ödünç alınır (kopyalanmaz): görünüm, log'un üzerinde ucuz
/// bir pencere olarak kalır. Bir log, bu görünümlerin bir dilimidir; dilimdeki 0. konum 1.
/// index'tir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryView<'a> {
    /// Girdinin term'i.
    pub term: u64,
    /// Girdinin komutunun baytları.
    pub command: &'a [u8],
}

/// Log'daki `index`'e (1'den başlar) karşılık gelen girdi; log'da yoksa `None`.
pub(crate) fn entry_at<'a>(log: &[EntryView<'a>], index: u64) -> Option<EntryView<'a>> {
    let position = usize::try_from(index.checked_sub(1)?).ok()?;
    log.get(position).copied()
}

/// Bir dilim uzunluğunun index karşılığı (son girdinin index'i).
pub(crate) fn last_index(log: &[EntryView<'_>]) -> u64 {
    u64::try_from(log.len()).unwrap_or(u64::MAX)
}
