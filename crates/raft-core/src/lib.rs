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
//! - Rastgelelik gerektiğinde (Faz 2: seçim zaman aşımı) `rand::thread_rng()` gibi sistem
//!   entropisine dayanan kaynaklar DEĞİL, kurucuya verilen bir `seed`'den türetilen `ChaCha8Rng`
//!   kullanılacak.
//!
//! Bu crate şu an (Faz 0) bir **iskelettir**: tipler ve `step`'in imzası tanımlıdır, ama içinde
//! henüz hiçbir Raft mantığı (rol geçişleri, oylama, log replikasyonu) yoktur. `step` her girdi
//! için boş bir liste döndürür ve düğümü değiştirmeden bırakır; bu, Faz 2'den (lider seçimi)
//! itibaren üzerine gerçek davranış eklenecek geçerli bir başlangıç noktasıdır (Faz 1 yalnızca
//! simülatörü kurar).
//!
//! Yorumlarda geçen etiketler bu crate'in sözleşme maddeleridir (değişmezler ve kenar durumlar):
//!
//! - **N1:** Bir düğüm kendi eşi olamaz: `RaftNode::new`, `id`'yi `peers` kümesinden çıkarır.
//! - **N2:** `id()` ve `peers()`, kurucuya verilen (normalize edilmiş) değerleri döndürür.
//! - **N3:** `step` tam (total) bir fonksiyondur: hiçbir girdide panik atmaz.
//! - **C1:** `Command` baytları olduğu gibi taşır; çekirdek onları hiç yorumlamaz.
//! - **O1:** `step` çıktıları sırayla yürütülür; bir `Persist`, aynı adımın sonraki tüm
//!   çıktılarından önce kalıcı hâle getirilmelidir.
//! - **R1:** `Input::Restart` yalnızca diskte kalıcı olan durumu taşır; kurtarma ondan başlar.
//!
//! Aşağıdaki örnek, gerçek bir sürücünün (ör. simülatör) çekirdekle nasıl konuşacağını gösterir:
//! üç düğümlü bir küme kurar, bir `Tick` işler ve dönen her `Output`'u SIRAYLA (bkz. `Output`
//! belgesi, O1) işler. `match` kasıtlı olarak `_` kolu TAŞIMAZ: `Output`'a yeni bir varyant
//! eklendiğinde bu doctest derlenmez, böylece sürücü kodunun eksik kalması derleme zamanında
//! yakalanır.
//!
//! ```
//! use raft_core::{Input, NodeId, Output, RaftNode};
//! use std::collections::BTreeSet;
//!
//! let peers: BTreeSet<NodeId> = [NodeId(2), NodeId(3)].into_iter().collect();
//! let mut node = RaftNode::new(NodeId(1), peers);
//!
//! let outputs = node.step(Input::Tick);
//! for output in outputs {
//!     match output {
//!         Output::Send { to, msg } => {
//!             // Sürücü burada mesajı ağa (gerçek veya simüle) verir.
//!             let _ = (to, msg);
//!         }
//!         Output::Persist(state) => {
//!             // O1: Bu adımdaki sonraki `Send`'lerden ÖNCE diske yazılmış olmalı.
//!             let _ = state;
//!         }
//!         Output::Apply(command) => {
//!             // Durum makinesine (raft-core dışında yaşayan KV store gibi) uygulanır.
//!             let _ = command;
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

mod input;
mod message;
mod node;
mod output;
mod persist;
mod types;

// Genel API düz (flat) olarak kökten dışa aktarılır: tüketiciler `raft_core::Input` gibi
// modül yoluna değil, doğrudan crate köküne başvurur. Bu sayede modül dosyaları (types.rs,
// message.rs, ...) ileride yeniden düzenlenebilir/bölünebilir; dış API imzası değişmez.
pub use input::Input;
pub use message::Message;
pub use node::RaftNode;
pub use output::{ClientResponse, Output};
pub use persist::PersistentState;
pub use types::{Command, NodeId};
