//! # checker: Raft güvenlik invariant'ları ve linearizability kontrolcüsü
//!
//! Bu crate, bir simülasyon koşusunun Raft'ın güvenlik özelliklerini (Election Safety, Leader
//! Append-Only, Log Matching, Leader Completeness, State Machine Safety) ve istemci geçmişinin
//! linearizability'sini (Wing & Gong yaklaşımı, KV modeli üzerinde) ihlal edip etmediğini
//! bağımsız biçimde denetleyecek. Faz 0'da bu crate boş bir iskelettir; ilk invariant kontrolleri
//! Faz 2/3'te eklenecek.
//!
//! **Kasıtlı olarak `raft-core`'a bağımlı DEĞİLDİR.** Bir "kâhin" (oracle) her zaman denetlediği
//! uygulamadan bağımsız tipler ve mantık üzerinde çalışmalıdır: `checker` kendi nötr görünüm
//! (view) tiplerini tanımlayacak (ör. bir düğümün rolü/term'i/log'unun basit bir izdüşümü).
//! Eğer `checker`, `raft-core`'un tiplerini doğrudan kullansaydı, `raft-core`'daki bir tasarım
//! hatası (ör. yanlış bir alan) hem uygulamaya hem denetleyiciye aynen sızabilir ve invariant
//! testleri o hatayı yakalayamayabilirdi.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

// Faz 0: kasıtlı olarak hiçbir genel öğe yok (bu fazın kapsamı yalnızca iskelet). İlk
// invariant/linearizability tipleri Faz 2/3/4'te eklenecek.
