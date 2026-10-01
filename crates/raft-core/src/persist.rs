//! Diske yazılması ZORUNLU olan kalıcı durum: Figure 2'nin "Persistent state" kutusu.

use crate::types::{NodeId, Term};

/// Bir düğümün diske kalıcı biçimde yazması gereken durumu: `currentTerm`, `votedFor` ve `log[]`
/// (log, Faz 3'te alan olarak eklenecek). `Default` değeri, Figure 2'deki ilk açılış durumuna
/// karşılık gelir (term 0, oy yok, boş log); alanlar eklendiğinde bu anlam korunmalıdır.
///
/// Bu tip TEK disk formatıdır: `Output::Persist(state)` ile yazılır, çökme sonrası
/// `Input::Restart(state)` ile aynen geri verilir. Sans-IO çekirdek diski kendisi okuyup yazamadığı
/// için (G/Ç yasak) sürücü (simülatör veya gerçek çalıştırıcı) bu tipi taşıyıcı olarak kullanır.
/// "Diskte ne varsa o" ilkesi, fsync edilmemiş bir değişikliğin çökmeden sağ çıkmasını, dolayısıyla
/// "votedFor persist edilmedi" gibi dayanıklılık hatalarının simülasyonda gizlenmesini engeller:
/// `Restart` bütün durumu yalnızca bu değerden yeniden kurar (bkz. `Input::Restart`, R1).
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
}
