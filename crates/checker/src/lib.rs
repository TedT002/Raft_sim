//! # checker: Raft güvenlik invariant'ları ve linearizability kontrolcüsü
//!
//! Bu crate, bir simülasyon koşusunun Raft'ın güvenlik özelliklerini (Election Safety, Leader
//! Append-Only, Log Matching, Leader Completeness, State Machine Safety) ve istemci geçmişinin
//! linearizability'sini (Wing & Gong yaklaşımı, KV modeli üzerinde) ihlal edip etmediğini
//! bağımsız biçimde denetler. Şu an (Faz 2) yalnızca [`ElectionSafety`] vardır; log invariant'ları
//! Faz 3'te, linearizability kontrolcüsü Faz 4'te eklenecek.
//!
//! **Kasıtlı olarak `raft-core`'a bağımlı DEĞİLDİR.** Bir "kâhin" (oracle) her zaman denetlediği
//! uygulamadan bağımsız tipler ve mantık üzerinde çalışmalıdır: `checker` düz değerlerle (ör. term
//! ve düğüm kimliği için `u64`) ve kendi nötr görünüm (view) tipleriyle çalışır (ör. ileride bir
//! düğümün log'unun basit bir izdüşümü).
//! Eğer `checker`, `raft-core`'un tiplerini doğrudan kullansaydı, `raft-core`'daki bir tasarım
//! hatası (ör. yanlış bir alan) hem uygulamaya hem denetleyiciye aynen sızabilir ve invariant
//! testleri o hatayı yakalayamayabilirdi.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Doctest'ler de uyarısız olmalı (clippy doctest'leri görmez).
#![doc(test(attr(deny(warnings))))]

mod election;

// Genel API düz (flat) olarak kökten dışa aktarılır; modüller ileride yeniden düzenlenebilir.
pub use election::{ElectionSafety, ElectionSafetyViolation};
