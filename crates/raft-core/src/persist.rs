//! Diske yazılması ZORUNLU olan kalıcı durum: Figure 2'nin "Persistent state" kutusu.

/// Bir düğümün diske kalıcı biçimde yazması gereken durumu: `currentTerm`, `votedFor`, `log[]`
/// (Faz 2/3'te alan olarak eklenecek; Faz 0'da kasıtlı olarak boş). `Default` değeri, Figure 2'deki
/// ilk açılış durumuna karşılık gelir (term 0, oy yok, boş log); alanlar eklendiğinde bu anlam
/// korunmalıdır.
///
/// Bu tip TEK disk formatıdır: `Output::Persist(state)` ile yazılır, çökme sonrası
/// `Input::Restart(state)` ile aynen geri verilir. Sans-IO çekirdek diski kendisi okuyup yazamadığı
/// için (G/Ç yasak) sürücü (simülatör veya gerçek çalıştırıcı) bu tipi taşıyıcı olarak kullanır.
/// "Diskte ne varsa o" ilkesi, fsync edilmemiş bir değişikliğin çökmeden sağ çıkmasını, dolayısıyla
/// "votedFor persist edilmedi" gibi dayanıklılık hatalarının simülasyonda gizlenmesini engeller —
/// Faz 2'de `Restart` tüm durumu bu değerden yeniden kurduğunda (bkz. `Input::Restart`, R1).
///
/// Boş bir `enum` değil `struct { }` (süslü parantezli) olarak tanımlanır: hiç değeri olamayan
/// (uninhabited) bir tip, `Output::Persist`/`Input::Restart`'ı asla inşa edilemez hâle getirir ve
/// `-D warnings` altında `unreachable_patterns` gibi uyarılara yol açma riski taşır.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistentState {}
