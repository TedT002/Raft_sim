//! # checker: Raft güvenlik invariant'ları ve linearizability kontrolcüsü
//!
//! Bu crate, bir simülasyon koşusunun Raft'ın güvenlik özelliklerini (Election Safety, Leader
//! Append-Only, Log Matching, Leader Completeness, State Machine Safety) ve istemci geçmişinin
//! linearizability'sini (Wing & Gong yaklaşımı, KV modeli üzerinde) ihlal edip etmediğini
//! bağımsız biçimde denetler. Figure 3'ün beş güvenlik özelliği: [`ElectionSafety`],
//! [`LeaderAppendOnly`], [`LogMatching`], [`LeaderCompleteness`] ve [`StateMachineSafety`].
//! İstemci geçmişi için KV modeli üzerinde linearizability kontrolcüsü: [`check_kv`] (Wing & Gong
//! araması, Lowe'un just-in-time iyileştirmesi, bellekleme ve anahtar bazlı bölme).
//!
//! **Kasıtlı olarak `raft-core`'a bağımlı DEĞİLDİR.** Bir "kâhin" (oracle) her zaman denetlediği
//! uygulamadan bağımsız tipler ve mantık üzerinde çalışmalıdır: `checker` düz değerlerle (ör. term
//! ve düğüm kimliği için `u64`) ve kendi nötr görünüm (view) tipleriyle çalışır (ör. bir düğümün
//! log girdisinin basit bir izdüşümü olan [`EntryView`]).
//! Eğer `checker`, `raft-core`'un tiplerini doğrudan kullansaydı, `raft-core`'daki bir tasarım
//! hatası (ör. yanlış bir alan) hem uygulamaya hem denetleyiciye aynen sızabilir ve invariant
//! testleri o hatayı yakalayamayabilirdi.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Doctest'ler de uyarısız olmalı (clippy doctest'leri görmez).
#![doc(test(attr(deny(warnings))))]

mod append_only;
mod completeness;
mod election;
mod linearizability;
mod log_matching;
mod state_machine;
mod view;

// Genel API düz (flat) olarak kökten dışa aktarılır; modüller ileride yeniden düzenlenebilir.
pub use append_only::{LeaderAppendOnly, LeaderAppendOnlyViolation};
pub use completeness::{LeaderCompleteness, LeaderCompletenessViolation};
pub use election::{ElectionSafety, ElectionSafetyViolation};
pub use linearizability::{
    KvInput, KvOperation, KvOutput, LinearizabilityError, MalformedReason, check_kv,
};
pub use log_matching::{LogMatching, LogMatchingViolation};
pub use state_machine::{StateMachineSafety, StateMachineSafetyViolation};
pub use view::EntryView;
