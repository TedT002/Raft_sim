//! # sim: deterministik simülasyon çatısı
//!
//! Bu crate, `raft-core`'daki sans-IO düğümleri gerçek zamanlı bir sistemmiş gibi koşturan, ama
//! tamamen deterministik (aynı `seed` aynı sonucu üretir) bir test ortamı sağlayacak. Faz 0'da bu
//! crate boş bir iskelettir. Faz 1'de şu sorumluluklar dolacak:
//!
//! - **Sanal saat:** `u64` tick tabanlı mantıksal zaman; gerçek saate (`Instant`/`SystemTime`)
//!   asla dokunulmaz.
//! - **Olay kuyruğu:** `BinaryHeap<Reverse<...>>`, sıralama anahtarı `(time, seq)`. Anahtar
//!   yalnızca `time` olsaydı, eşit zamanlı olayların sırası `BinaryHeap`'in belirtilmemiş iç
//!   düzenine kalırdı: aynı derleyicide tekrarlanır ama ekleme sırasını (FIFO) korumaz ve standart
//!   kütüphanenin uygulaması değişince sessizce değişebilir; o zaman yayımlanmış bir seed artık
//!   aynı koşuyu üretmez. Bu yüzden her olaya eklenme sırasına göre artan bir `seq` sayacı
//!   damgalanır ve eşitlik açıkça FIFO olarak `seq` ile bozulur.
//! - **Alt-seed türetimi:** Tek bir ana `seed`'den ağ, disk, her düğüm ve fuzz senaryosu için
//!   ayrı, birbirinden bağımsız alt-seed'ler türetilecek; bir bileşene sonradan eklenen yeni bir
//!   rastgele çağrı, başka bir bileşenin RNG akışını kaydırmayacak.
//! - **Ağ simülasyonu:** mesaj kaybı, gecikme (dolayısıyla sıra değişimi), çoğaltma ve ağ
//!   bölünmesi.
//! - **Trace ve hash:** her olay kaydedilir; özet, sürümler arası kararlı bir hasher (FNV-1a) ile
//!   alınır.
//!
//! Sonraki fazlarda eklenecekler: düğüm çökmesi/yeniden başlatma (Faz 2) ve `fsync` olana kadar
//! "beklemede" kalan, çökmede kaybolabilen yazmalarıyla simüle disk (Faz 3).
//!
//! Bu crate `raft-core`'a ve `checker`'a bağımlıdır (bağımlılık yönü: `sim -> raft-core`,
//! `sim -> checker`); tersi asla olmaz — sans-IO çekirdek hiçbir workspace crate'ini bilmemelidir.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

// Faz 0: kasıtlı olarak hiçbir genel öğe yok (bu fazın kapsamı yalnızca iskelet). İlk tipler
// Faz 1'de eklenecek.
