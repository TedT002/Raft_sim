//! # raft-core: sans-IO Raft durum makinesi
//!
//! Bu crate, Raft konsensüs algoritmasının (Ongaro & Ousterhout, 2014) **saf** durum makinesidir:
//! saati bilmez, ağa/diske dokunmaz, iş parçacığı açmaz. Tek genel giriş noktası şudur:
//!
//! ```text
//! pub fn step(&mut self, input: Input) -> Vec<Output>
//! ```
//!
//! Sürücü (deterministik simülatör ya da gerçek çalıştırıcı) dışarıdan bir `Input` verir, çekirdek
//! yapılması gereken eylemleri sıralı bir `Vec<Output>` olarak geri döndürür. Bu ayrım ("sans-IO")
//! sayesinde:
//!
//! - **Determinizm** garanti edilir: çekirdeğin davranışı yalnızca `self` ve verilen `Input`'e
//!   bağlıdır; gerçek saat (`Instant`/`SystemTime`) veya iş parçacığı zamanlamasına bağlı hiçbir
//!   yan etki yoktur.
//! - Simülatör her adımı tam kontrol eder: mesaj kaybı, gecikme, düğüm çökmesi gibi hataları
//!   istediği yerde enjekte edip aynı `seed` ile birebir tekrar oynatabilir.
//! - `HashMap`/`HashSet` yerine `BTreeMap`/`BTreeSet` kullanılır: `Hash*` koleksiyonların yineleme
//!   sırası çalıştırmadan çalıştırmaya değişebilir (rastgele hash tohumu), bu da "aynı seed aynı
//!   sonucu üretir" sözleşmesini sessizce bozar.
//! - Rastgelelik (seçim zaman aşımı) `rand::thread_rng()` gibi sistem entropisine dayanan
//!   kaynaklardan DEĞİL, kurucuya verilen bir `seed`'den kurulan `ChaCha8Rng`'den gelir.
//!
//! Çekirdek **lider seçimini** (§5.2: roller ([`Role`]), term'ler, `[T, 2T)` aralığından rastgele
//! seçim zaman aşımı, oy verme ve seçim kısıtı §5.4.1), **log replikasyonunu** (§5.3: tutarlılık
//! denetimli `AppendEntries`, çakışan kuyruğun değiştirilmesi, `nextIndex`/`matchIndex`; §5.4.2
//! commit kuralı; commit edilen girdilerin sırayla uygulanması) ve **istemci arayüzünün
//! çekirdekteki kısmını** uygular (§8: lider olmayan düğümün [`ClientResponse::NotLeader`] cevabı
//! ve yeni liderin term başında eklediği no-op girdi). Aynı isteğin bir kez uygulanması (oturumlar
//! ve tekilleştirme) durum makinesinin işidir: komutlar çekirdek için opaktır (C1).
//!
//! Yorumlarda geçen etiketler bu crate'in sözleşme maddeleridir (değişmezler ve kenar durumlar):
//!
//! - **N1:** Bir düğüm kendi eşi olamaz: `RaftNode::new`, `id`'yi `peers` kümesinden çıkarır.
//! - **N2:** `id()`, `peers()` ve `config()`, kurucuya verilen (normalize edilmiş) değerleri
//!   döndürür.
//! - **N3:** `step` tam (total) bir fonksiyondur: hiçbir girdide panik atmaz.
//! - **C1:** `Command` baytları olduğu gibi taşır; çekirdek onları hiç yorumlamaz. Tek istisna
//!   boş komuttur: no-op'a ayrılmıştır ([`Command::noop`]).
//! - **S1:** Lider olmayan düğüm bir istemci isteğini log'a eklemez ve aynı adımda tek bir
//!   `ClientResponse::NotLeader { hint }` üretir; `hint`, bu term'de AppendEntries aldığı lider
//!   (bilinmiyorsa `None`). Lider isteği kabul eder ve cevap üretmez: sonuç, komut commit edilip
//!   uygulandığında durum makinesinden gelir.
//! - **S2:** Lider olan düğüm, term'inin başında log'una kendi term'inden bir no-op girdi ekler
//!   (§8) ve onu ilk AppendEntries'le gönderir.
//! - **O1:** `step` çıktıları sırayla yürütülür; bir `Persist`, aynı adımın sonraki tüm
//!   çıktılarından önce kalıcı hâle getirilmelidir.
//! - **O2:** Bir adım kalıcı durumu (`currentTerm`, `votedFor` ya da log) değiştirdiyse İLK
//!   çıktısı, değişikliği (farkı) taşıyan tek bir `Persist`'tir; değiştirmediyse hiç `Persist`
//!   yoktur. Farkı diskteki duruma uygulamak (`PersistentState::apply`) tam olarak bellekteki
//!   durumu verir. Ardından `Send`'ler, sonra `Apply`'lar, en sonda (varsa) `ClientResponse`
//!   gelir.
//! - **R1:** `Input::Restart` yalnızca diskte kalıcı olan durumu taşır; kurtarma ondan başlar.
//!   Düğüm Follower olarak açılır ve bu adım hiç çıktı üretmez.
//! - **T1:** Term asla azalmaz. Daha yüksek term taşıyan herhangi bir mesaj görülünce düğüm o
//!   term'i benimser ve Follower'a döner (§5.1). Tek istisna `Restart`'tır: verilen disk durumunu
//!   olduğu gibi yükler. Doğru bir sürücüde disk her zaman son persist edilen durumu taşıdığından
//!   term yine azalmaz.
//! - **E1:** Bir term'de en fazla bir oy: `votedFor` bir term içinde bir kez yazılır, sonra
//!   değişmez (§5.2).
//! - **E2:** Oylar küme olarak sayılır: yalnızca eşlerden ve yalnızca mevcut term'e ait olumlu
//!   cevaplar; aynı düğümün tekrarlanan oyu bir kez sayılır.
//! - **L1:** Takipçi log'unu yalnızca gerçek bir çakışmada (aynı index, farklı term) keser;
//!   gecikmiş ya da tekrarlanmış bir AppendEntries log'u kısaltmaz (§5.3).
//! - **L2:** Lider, lider olduğu term boyunca kendi log'unu yalnızca uzatır (Leader Append-Only).
//! - **M1:** commitIndex yalnızca artar. Lider yalnızca kendi term'indeki bir girdiyi kopyalarını
//!   sayarak commit eder; önceki term'lerin girdileri dolaylı olarak commit olur (§5.4.2, Figure
//!   8).
//! - **A1:** Commit edilen girdiler index sırasıyla, her biri bir kez `Apply` olarak verilir;
//!   yeniden başlatmadan sonra (lastApplied geçici olduğu için) baştan yeniden verilir.
//!
//! Aşağıdaki örnek, gerçek bir sürücünün (ör. simülatör) çekirdekle nasıl konuşacağını gösterir:
//! üç düğümlü bir kümenin bir düğümünü kurar, seçim zaman aşımı dolana kadar `Tick` verir ve dönen
//! her `Output`'u SIRAYLA (bkz. `Output` belgesi, O1) işler. `match` kasıtlı olarak `_` kolu
//! TAŞIMAZ: `Output`'a yeni bir varyant eklendiğinde bu doctest derlenmez, böylece sürücü kodunun
//! eksik kalması derleme zamanında yakalanır.
//!
//! ```
//! use raft_core::{Config, Input, Message, NodeId, Output, RaftNode, Role};
//! use std::collections::BTreeSet;
//!
//! let peers: BTreeSet<NodeId> = [NodeId(2), NodeId(3)].into_iter().collect();
//! // Seed, düğümün seçim zaman aşımlarını belirler: aynı seed her zaman aynı davranışı verir.
//! let mut node = RaftNode::new(NodeId(1), peers, Config::default(), [7; 32]);
//!
//! // Zaman aşımı [T, 2T) tick içinde dolar ve düğüm aday olur.
//! let mut outputs = Vec::new();
//! while outputs.is_empty() {
//!     outputs = node.step(Input::Tick);
//! }
//! assert_eq!(node.role(), Role::Candidate);
//! // O2: yeni term ve kendine verilen oy, oy istekleri gönderilmeden ÖNCE diske yazılır.
//! assert!(matches!(outputs[0], Output::Persist(_)));
//!
//! for output in outputs {
//!     match output {
//!         Output::Send { to, msg } => {
//!             // Sürücü burada mesajı ağa (gerçek veya simüle) verir.
//!             assert!(matches!(msg, Message::RequestVote(_)));
//!             let _ = to;
//!         }
//!         Output::Persist(state) => {
//!             // O1: Bu adımdaki sonraki `Send`'lerden ÖNCE diske yazılmış olmalı.
//!             let _ = state;
//!         }
//!         Output::Apply { index, command } => {
//!             // Durum makinesine (raft-core dışında yaşayan KV store gibi) uygulanır.
//!             let _ = (index, command);
//!         }
//!         Output::ClientResponse(response) => {
//!             // İstemciye geri döndürülür.
//!             let _ = response;
//!         }
//!     }
//! }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Doctest'ler de uyarısız olmalı: clippy doctest'leri görmez, bu öznitelik onları derleyici
// uyarılarına karşı korur (örnekler README gibi okunur; uyarılı örnek kötü örnektir).
#![doc(test(attr(deny(warnings))))]

// Mutasyon testi (Faz 5): `mutation-*` özellikleri çekirdeğe bilerek hata ekler. İkisi aynı anda
// açılırsa hangi hatanın yakalandığı belirsizleşir ve mutasyon tablosu anlamını yitirir: bu
// derleme zamanı denetimi buna izin vermez.
const ENABLED_MUTATIONS: usize = cfg!(feature = "mutation-no-election-restriction") as usize
    + cfg!(feature = "mutation-commit-old-terms") as usize
    + cfg!(feature = "mutation-forget-vote") as usize
    + cfg!(feature = "mutation-truncate-on-append") as usize
    + cfg!(feature = "mutation-skip-prev-log-term") as usize
    + cfg!(feature = "mutation-apply-before-commit") as usize;
// `<= 1` yerine `matches!`: varsayılan derlemede sabit 0'dır ve clippy, türün en küçük değeriyle
// yapılan her zaman doğru bir karşılaştırmayı (`absurd_extreme_comparisons`) hata sayar.
const _: () = assert!(
    matches!(ENABLED_MUTATIONS, 0 | 1),
    "enable at most one mutation-* feature at a time"
);

/// Bu derlemede açık olan çekirdek mutasyonunun Cargo özelliği; varsayılan derlemede `None`.
///
/// Mutasyon testi içindir (bkz. `docs/mutation-table.md`). Bir crate'in derleme zamanı koruması
/// yalnızca kendi özelliklerini görür: çekirdeğin bir mutasyonu doğrudan (`raft-core/mutation-…`)
/// açılırsa, `sim` onu ancak bu sabit üzerinden fark eder. `sim` kendi mutasyonunu bununla
/// birleştirip derlemede tek bir mutasyon bulunduğunu denetler ve yeniden üretme komutlarına doğru
/// özelliği yazar.
pub const ENABLED_MUTATION: Option<&str> = if cfg!(feature = "mutation-no-election-restriction") {
    Some("mutation-no-election-restriction")
} else if cfg!(feature = "mutation-commit-old-terms") {
    Some("mutation-commit-old-terms")
} else if cfg!(feature = "mutation-forget-vote") {
    Some("mutation-forget-vote")
} else if cfg!(feature = "mutation-truncate-on-append") {
    Some("mutation-truncate-on-append")
} else if cfg!(feature = "mutation-skip-prev-log-term") {
    Some("mutation-skip-prev-log-term")
} else if cfg!(feature = "mutation-apply-before-commit") {
    Some("mutation-apply-before-commit")
} else {
    None
};

mod config;
mod input;
mod log;
mod message;
mod node;
mod output;
mod persist;
mod role;
mod types;

// Genel API düz (flat) olarak kökten dışa aktarılır: tüketiciler `raft_core::Input` gibi
// modül yoluna değil, doğrudan crate köküne başvurur. Bu sayede modül dosyaları (types.rs,
// message.rs, ...) ileride yeniden düzenlenebilir/bölünebilir; dış API imzası değişmez.
pub use config::{Config, ConfigError};
pub use input::Input;
pub use log::LogEntry;
pub use message::{
    AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse,
};
pub use node::RaftNode;
pub use output::{ClientResponse, Output};
pub use persist::{LogUpdate, PersistUpdate, PersistentState};
pub use role::Role;
pub use types::{Command, LogIndex, NodeId, Term};
