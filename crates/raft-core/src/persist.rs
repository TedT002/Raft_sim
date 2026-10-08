//! Diske yazılması ZORUNLU olan kalıcı durum (Figure 2'nin "Persistent state" kutusu) ve her
//! adımda diske yazılan fark.

use crate::log::LogEntry;
use crate::types::{LogIndex, NodeId, Term};

/// Bir düğümün diske kalıcı biçimde yazması gereken durumu: `currentTerm`, `votedFor` ve `log[]`.
/// `Default` değeri, Figure 2'deki ilk açılış durumuna karşılık gelir (term 0, oy yok, boş log);
/// alanlar eklendiğinde bu anlam korunmalıdır.
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
    /// Log girdileri; dilimdeki 0. konum 1. index'tir (Figure 2: `log[]`).
    ///
    /// Kalıcı olması zorunludur: bir takipçi, girdiyi diske yazmadan "aldım" derse ve sonra
    /// çökerse, lider o girdiyi çoğunlukta sanıp commit edebilir, ama girdi aslında yok olmuştur.
    pub log: Vec<LogEntry>,
}

impl PersistentState {
    /// Diske yazılmış bir farkı bu duruma uygular: term ve oy aynen yazılır, varsa log farkı
    /// uygulanır (`from`'dan önceki girdiler korunur, `from` ve sonrası farkın girdileriyle
    /// değiştirilir).
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
        if let Some(log) = &update.log {
            let keep = usize::try_from(log.from.0.saturating_sub(1))
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
    /// Log'daki değişiklik; bu adımda log değişmediyse `None`.
    pub log: Option<LogUpdate>,
}

/// Log'un bir kuyruğunun değiştirilmesi: `from` index'i (dahil) ve sonrası silinir, yerine
/// `entries` eklenir. Saf ekleme `from = eski son index + 1` demektir; yalnızca kesme `entries`'in
/// boş olmasıdır; çakışma çözümü (§5.3) ikisinin birleşimidir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogUpdate {
    /// Değişen ilk index (en az 1).
    pub from: LogIndex,
    /// `from`'dan itibaren log'un yeni hâli.
    pub entries: Vec<LogEntry>,
}

#[cfg(test)]
mod tests {
    use super::{LogUpdate, PersistUpdate, PersistentState};
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
            log: from.map(|(from, entries)| LogUpdate {
                from: LogIndex(from),
                entries,
            }),
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

        disk.apply(&update(Some((2, Vec::new()))));
        assert_eq!(disk.log, vec![entry(1, 10)]);

        disk.apply(&update(Some((9, vec![entry(4, 30)]))));
        assert_eq!(disk.log, vec![entry(1, 10), entry(4, 30)]);
    }
}
