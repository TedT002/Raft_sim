//! Diske yazılması ZORUNLU olan kalıcı durum (Figure 2'nin "Persistent state" kutusu) ve her
//! adımda diske yazılan fark.

use crate::log::LogEntry;
use crate::types::{LogIndex, NodeId, Term};

/// Bir snapshot (§7): log'un `1..=last_index` önekinin yerini tutan durum makinesi durumu.
///
/// Log sonsuza kadar büyüyemez: bir düğüm, durum makinesinin uygulanmış (dolayısıyla commit
/// edilmiş) bir önekini snapshot'a alır ve o önekin girdilerini log'dan atar. `last_index` ve
/// `last_term`, atılan son girdinin yerini tutar: AppendEntries tutarlılık denetimi
/// (`prevLogIndex`/`prevLogTerm`, §5.3) snapshot'ın hemen ardındaki girdi için bu çifte dayanır.
/// `data`, durum makinesinin o index'teki hâlidir; çekirdek için opaktır (C1): onu yalnızca
/// saklar, gönderir ve geri verir.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Snapshot'ın kapsadığı son girdinin index'i (Figure 13: `lastIncludedIndex`).
    pub last_index: LogIndex,
    /// O girdinin term'i (Figure 13: `lastIncludedTerm`).
    pub last_term: Term,
    /// Durum makinesinin `last_index`'teki hâli.
    pub data: Vec<u8>,
}

/// Bir düğümün diske kalıcı biçimde yazması gereken durumu: `currentTerm`, `votedFor`, varsa
/// snapshot ve log'un snapshot'tan sonraki girdileri. `Default` değeri, Figure 2'deki ilk açılış
/// durumuna karşılık gelir (term 0, oy yok, snapshot yok, boş log); alanlar eklendiğinde bu anlam
/// korunmalıdır.
///
/// Bu tip diskin TAM içeriğidir: çökme sonrası `Input::Restart(state)` ile aynen geri verilir.
/// Çekirdek her adımda bütün durumu değil yalnızca değişen kısmı (`PersistUpdate`) yazdırır; sürücü
/// bu farkları [`PersistentState::apply`] ile biriktirir. Sans-IO çekirdek diski kendisi okuyup
/// yazamadığı için (G/Ç yasak) sürücü (simülatör veya gerçek çalıştırıcı) bu tipi taşıyıcı olarak
/// kullanır. "Diskte ne varsa o" ilkesi, fsync edilmemiş bir değişikliğin çökmeden sağ çıkmasını,
/// dolayısıyla "votedFor persist edilmedi" gibi dayanıklılık hatalarının simülasyonda gizlenmesini
/// engeller: `Restart` bütün durumu yalnızca bu değerden yeniden kurar (bkz. `Input::Restart`, R1).
///
/// Alanlar public'tir: bu tip davranışı olmayan bir veri taşıyıcısıdır (disk kaydı). Sürücü onu
/// olduğu gibi saklar ve geri verir; testler de belirli bir disk içeriğini doğrudan kurabilir.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistentState {
    /// Düğümün gördüğü en son term (Figure 2: `currentTerm`).
    pub current_term: Term,
    /// `current_term` içinde oy verilen aday; henüz oy verilmediyse `None` (Figure 2: `votedFor`).
    ///
    /// Kalıcı olması zorunludur: oy diske yazılmadan cevap verilirse, çöküp kalkan düğüm aynı
    /// term'de ikinci bir adaya oy verebilir ve aynı term'de iki lider seçilebilir (§5.2, Election
    /// Safety).
    pub voted_for: Option<NodeId>,
    /// Varsa snapshot (§7): log'un ilk girdisi `snapshot.last_index + 1`. index'tir.
    ///
    /// Kalıcı olması zorunludur: düğüm snapshot'a aldığı girdileri log'dan attı; snapshot
    /// kaybolsaydı o girdiler de kaybolurdu (commit edilmiş girdiler!).
    pub snapshot: Option<Snapshot>,
    /// Log'un snapshot'tan sonraki girdileri; snapshot yoksa dilimdeki 0. konum 1. index'tir
    /// (Figure 2: `log[]`).
    ///
    /// Kalıcı olması zorunludur: bir takipçi, girdiyi diske yazmadan "aldım" derse ve sonra
    /// çökerse, lider o girdiyi çoğunlukta sanıp commit edebilir, ama girdi aslında yok olmuştur.
    pub log: Vec<LogEntry>,
}

impl PersistentState {
    /// Log'un snapshot'ın kapsadığı son index'i (snapshot yoksa 0): `log[0]` bunun bir
    /// sonrasıdır.
    #[must_use]
    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot
            .as_ref()
            .map_or(LogIndex(0), |snapshot| snapshot.last_index)
    }

    /// Son girdinin index'i; boş log için snapshot'ın index'i (snapshot da yoksa 0).
    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        let len = u64::try_from(self.log.len()).unwrap_or(u64::MAX);
        LogIndex(self.snapshot_index().0.saturating_add(len))
    }

    /// `index`'teki girdi; snapshot'ın kapsadığı (ya da 0) ya da log'un ötesindeki bir index için
    /// `None`. Mutlak index'ten konuma dönüşüm burada, tek yerdedir (çekirdeğin `Log`'undaki
    /// hesabın diskteki karşılığı).
    #[must_use]
    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        let position = index
            .0
            .checked_sub(self.snapshot_index().0)?
            .checked_sub(1)?;
        self.log.get(usize::try_from(position).ok()?)
    }

    /// Diske yazılmış bir farkı bu duruma uygular: term ve oy aynen yazılır; varsa yeni snapshot
    /// yazılır ve kapsadığı girdiler log'dan atılır; varsa log farkı uygulanır (`from`'dan önceki
    /// girdiler korunur, `from` ve sonrası farkın girdileriyle değiştirilir).
    ///
    /// Farkın biçimi çekirdekte tanımlıdır ve uygulanma kuralı da burada, tek yerde durur: sürücü
    /// biçimi kendisi yorumlasaydı, çekirdekle sürücünün farkı farklı anlaması diskteki durumu
    /// sessizce bozabilirdi.
    ///
    /// `from` log'un sonundan ilerde olamaz (çekirdek böyle bir fark üretmez). Olsaydı arada boşluk
    /// bırakılmaz, girdiler log'un sonuna eklenirdi; sürücünün disk ile bellek karşılaştırması
    /// (simülatördeki dayanıklılık denetimi) bu farkı yakalar. Panik atılmaz (N3).
    pub fn apply(&mut self, update: &PersistUpdate) {
        self.current_term = update.current_term;
        self.voted_for = update.voted_for;
        if let Some(snapshot) = &update.snapshot {
            // Snapshot'ın kapsadığı girdiler artık log'da tutulmaz. Atılacak girdi sayısı eski ve
            // yeni snapshot index'lerinin farkıdır; snapshot geri gidiyorsa (doğru bir çekirdekte
            // olmaz) hiçbir girdi atılmaz ve aşağıdaki log farkı log'u yeni tabana göre baştan
            // yazar.
            let dropped = snapshot
                .last_index
                .0
                .saturating_sub(self.snapshot_index().0);
            let dropped = usize::try_from(dropped)
                .unwrap_or(usize::MAX)
                .min(self.log.len());
            self.log.drain(..dropped);
            self.snapshot = Some(snapshot.clone());
        }
        if let Some(log) = &update.log {
            // `from` mutlak bir index'tir; diskteki log snapshot'tan sonra başlar.
            let base = self.snapshot_index().0;
            let keep = usize::try_from(log.from.0.saturating_sub(1).saturating_sub(base))
                .unwrap_or(usize::MAX)
                .min(self.log.len());
            self.log.truncate(keep);
            self.log.extend(log.entries.iter().cloned());
        }
    }
}

/// Bir adımda kalıcı duruma yapılan değişiklik: `Output::Persist` ile diske yazılacak fark.
///
/// Neden tam durum değil fark: tam durum her yazmada bütün log'u taşırdı; yazma maliyeti log
/// büyüdükçe artar. Fark ise gerçek bir log dosyasına yapılan yazma gibidir: çoğu zaman sona birkaç
/// girdi eklenir, çakışmada bir kuyruk değiştirilir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistUpdate {
    /// Adım sonundaki `currentTerm`. Term ve oy her yazmada tam yazılır: küçük ve sabit boyutludur.
    pub current_term: Term,
    /// Adım sonundaki `votedFor`.
    pub voted_for: Option<NodeId>,
    /// Bu adımda alınan ya da kurulan snapshot; değişmediyse `None`. Önce o uygulanır (kapsadığı
    /// girdiler log'dan atılır), sonra log farkı.
    pub snapshot: Option<Snapshot>,
    /// Log'daki değişiklik; bu adımda log değişmediyse `None`.
    pub log: Option<LogUpdate>,
}

/// Log'un bir kuyruğunun değiştirilmesi: `from` index'i (dahil) ve sonrası silinir, yerine
/// `entries` eklenir. Saf ekleme `from = eski son index + 1` demektir; yalnızca kesme `entries`'in
/// boş olmasıdır; çakışma çözümü (§5.3) ikisinin birleşimidir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogUpdate {
    /// Değişen ilk index (en az 1; snapshot varsa ondan sonra).
    pub from: LogIndex,
    /// `from`'dan itibaren log'un yeni hâli.
    pub entries: Vec<LogEntry>,
}

#[cfg(test)]
mod tests {
    use super::{LogUpdate, PersistUpdate, PersistentState, Snapshot};
    use crate::log::LogEntry;
    use crate::types::{Command, LogIndex, NodeId, Term};

    fn entry(term: u64, byte: u8) -> LogEntry {
        LogEntry {
            term: Term(term),
            command: Command::new(vec![byte]),
        }
    }

    fn update(from: Option<(u64, Vec<LogEntry>)>) -> PersistUpdate {
        PersistUpdate {
            current_term: Term(4),
            voted_for: Some(NodeId(2)),
            snapshot: None,
            log: from.map(|(from, entries)| LogUpdate {
                from: LogIndex(from),
                entries,
            }),
        }
    }

    fn snapshot(last_index: u64, last_term: u64) -> Snapshot {
        Snapshot {
            last_index: LogIndex(last_index),
            last_term: Term(last_term),
            data: vec![u8::try_from(last_index).unwrap_or(0)],
        }
    }

    // Farkın uygulanması: term/oy aynen yazılır; ekleme, kuyruk değiştirme ve yalnızca kesme log'u
    // beklenen hâle getirir. Log farkı yoksa log'a dokunulmaz. Log'un ötesini gösteren bir `from`
    // boşluk bırakmaz (savunma amaçlı).
    #[test]
    fn updates_replay_onto_the_full_state() {
        let mut disk = PersistentState::default();
        disk.apply(&update(Some((1, vec![entry(1, 10), entry(1, 11)]))));
        assert_eq!(disk.current_term, Term(4));
        assert_eq!(disk.voted_for, Some(NodeId(2)));
        assert_eq!(disk.log, vec![entry(1, 10), entry(1, 11)]);

        disk.apply(&update(Some((3, vec![entry(2, 12)]))));
        assert_eq!(disk.log, vec![entry(1, 10), entry(1, 11), entry(2, 12)]);

        disk.apply(&update(Some((2, vec![entry(3, 20)]))));
        assert_eq!(disk.log, vec![entry(1, 10), entry(3, 20)]);

        disk.apply(&update(None));
        assert_eq!(disk.log, vec![entry(1, 10), entry(3, 20)]);
        assert_eq!(disk.entry(LogIndex(2)), Some(&entry(3, 20)));
        assert_eq!(disk.entry(LogIndex(0)), None);
        assert_eq!(disk.last_index(), LogIndex(2));

        disk.apply(&update(Some((2, Vec::new()))));
        assert_eq!(disk.log, vec![entry(1, 10)]);

        disk.apply(&update(Some((9, vec![entry(4, 30)]))));
        assert_eq!(disk.log, vec![entry(1, 10), entry(4, 30)]);
    }

    // Snapshot'lı farklar (§7): snapshot kapsadığı girdileri log'dan atar; sonraki log farkları
    // mutlak index'lerle, snapshot'tan sonraki log'a uygulanır. Kurulan bir snapshot'la gelen
    // "snapshot'tan sonrasını baştan yaz" farkı log'u yeni tabana göre kurar; snapshot'tan sonraki
    // girdiler korunabilir ya da atılabilir.
    #[test]
    fn snapshots_drop_the_entries_they_cover() {
        let mut disk = PersistentState::default();
        let entries = vec![entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)];
        disk.apply(&update(Some((1, entries))));

        // Sıkıştırma: 1..=2 snapshot'a alınır, log 3. index'ten başlar.
        disk.apply(&PersistUpdate {
            snapshot: Some(snapshot(2, 1)),
            ..update(None)
        });
        assert_eq!(disk.snapshot_index(), LogIndex(2));
        assert_eq!(disk.log, vec![entry(2, 3), entry(2, 4)]);
        assert_eq!(disk.entry(LogIndex(2)), None, "compacted");
        assert_eq!(disk.entry(LogIndex(3)), Some(&entry(2, 3)));
        assert_eq!(disk.last_index(), LogIndex(4));

        // Mutlak index'lerle ekleme ve kuyruk değiştirme.
        disk.apply(&update(Some((5, vec![entry(3, 5)]))));
        assert_eq!(disk.log, vec![entry(2, 3), entry(2, 4), entry(3, 5)]);
        disk.apply(&update(Some((4, vec![entry(4, 6)]))));
        assert_eq!(disk.log, vec![entry(2, 3), entry(4, 6)]);

        // Kurulan bir snapshot (Figure 13): 1..=3 snapshot'ta, 4. girdi korunur.
        disk.apply(&PersistUpdate {
            snapshot: Some(snapshot(3, 2)),
            ..update(Some((4, vec![entry(4, 6)])))
        });
        assert_eq!(disk.snapshot_index(), LogIndex(3));
        assert_eq!(disk.log, vec![entry(4, 6)]);

        // Log'un ötesindeki bir snapshot: log'un tamamı atılır.
        disk.apply(&PersistUpdate {
            snapshot: Some(snapshot(9, 5)),
            ..update(Some((10, Vec::new())))
        });
        assert_eq!(disk.snapshot_index(), LogIndex(9));
        assert!(disk.log.is_empty());
        assert_eq!(disk.snapshot, Some(snapshot(9, 5)));
    }
}
