//! Raft adaptörü: `raft_core::RaftNode`'u simülatöre bağlar ve her olaydan sonra invariant'ları
//! denetleyen [`RaftCluster`]'ı sunar.
//!
//! Adaptörün iki görevi var:
//!
//! 1. `RaftNode` için [`SimNode`]'u uygular: `NodeInput`/`NodeOutput` ile raft-core'un
//!    `Input`/`Output`'u arasında birebir çeviri. Mesajlar ve kalıcı durum için kanonik trace
//!    kodlamaları da buradadır.
//! 2. [`RaftCluster`]: simülasyonu olay olay sürer ve HER olaydan sonra (bir düğüm adımı ve
//!    çıktılarının uygulanması) şunları denetler:
//!    - **Election Safety** (bağımsız kâhin: `checker::ElectionSafety`): bir term'de en fazla bir
//!      lider, koşunun bütün geçmişi boyunca.
//!    - **Dayanıklılık:** her adımın SONUNDA ayaktaki her düğümün diskindeki durum, bellekteki
//!      `currentTerm` ve `votedFor` ile aynı olmalı. Bir adım durumu değiştirip `Persist` etmeyi
//!      unutursa, hatayı ortaya çıkaracak bir çökmeyi beklemeden hemen yakalanır.
//!
//! Bu denetimin göremediği bir şey var: bir adımın İÇİNDEKİ sıra. Figure 2'nin "cevap vermeden
//! önce kalıcı depoya yaz" kuralı, `Persist`'in aynı adımın `Send`'lerinden önce gelmesini ister
//! (O1). Simülatör bir adımın çıktılarını bölünmeden uygular (adımın ortasında çökme yoktur); bu
//! yüzden önce `Send`, sonra `Persist` üreten bir düğüm adım sonunda yine tutarlı görünür. Sıra
//! raft-core'un sözleşme testleriyle korunur. Çıktıların ortasında çökme modeli, `fsync`'li diskle
//! birlikte Faz 3'te ele alınacak.
//!
//! Neden her olaydan sonra: bir ihlal geçici olabilir. Örneğin bir düğüm yanlışlıkla lider olup
//! bir sonraki olayda daha yüksek bir term görerek düşebilir. Yalnızca koşunun sonunda bakmak bu
//! ara durumu kaçırırdı.

use std::collections::{BTreeMap, BTreeSet};

use checker::{ElectionSafety, ElectionSafetyViolation};
use raft_core::{
    AppendEntries, AppendEntriesResponse, Config, Input, Message, NodeId, Output, PersistentState,
    RaftNode, RequestVote, RequestVoteResponse, Role, Term,
};

use crate::error::{ConfigError, LifecycleError, PartitionError};
use crate::network::{NetworkConfig, SimNetwork};
use crate::node::{NodeInput, NodeOutput, SimNode};
use crate::rng::{Component, SeedTree};
use crate::simulation::{SimConfig, Simulation};
use crate::trace::TraceEncode;

impl SimNode for RaftNode {
    type Msg = Message;
    type Durable = PersistentState;

    fn step(
        &mut self,
        input: NodeInput<Message, PersistentState>,
    ) -> Vec<NodeOutput<Message, PersistentState>> {
        let input = match input {
            NodeInput::Tick => Input::Tick,
            NodeInput::Message { from, msg } => Input::Message { from, msg },
            NodeInput::Restart(state) => Input::Restart(state),
        };
        // `RaftNode::step` yazımı raft-core'un kendi `step`'ini çağırır (yerleşik metot, trait
        // metodundan önce gelir). Çıktılar sırası korunarak çevrilir (O1).
        RaftNode::step(self, input)
            .into_iter()
            .filter_map(|output| match output {
                Output::Send { to, msg } => Some(NodeOutput::Send { to, msg }),
                Output::Persist(state) => Some(NodeOutput::Persist(state)),
                // Faz 2'de çekirdek bunları hiç üretmez (log ve istemci arayüzü yok). Faz 3'te
                // `Apply` simülatördeki durum makinesine (KV) ve State Machine Safety denetimine
                // bağlanacak. `_` kolu bilerek yok: `Output`'a yeni bir varyant eklenince burası
                // derlenmez ve adaptör bilinçli olarak güncellenir.
                Output::Apply { .. } | Output::ClientResponse(_) => None,
            })
            .collect()
    }
}

/// Sabit genişlikli, little-endian bir `u64` yazar.
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

impl TraceEncode for Message {
    // Kanonik kodlama: varyant etiketi ve ardından alanlar sabit sırayla (u64'ler little-endian,
    // bool tek bayt). Yapılar desenle açılır (`..` YOK): Faz 3'te bir mesaja alan eklendiğinde bu
    // kod derlenmez ve yeni alan kodlamaya bilinçli olarak eklenir. Aksi hâlde yalnızca o alanda
    // farklılaşan iki mesaj aynı özeti verir ve trace bir sapmayı gizleyebilirdi.
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Message::RequestVote(RequestVote {
                term,
                last_log_index,
                last_log_term,
            }) => {
                out.push(1);
                put_u64(out, term.0);
                put_u64(out, last_log_index.0);
                put_u64(out, last_log_term.0);
            }
            Message::RequestVoteResponse(RequestVoteResponse { term, vote_granted }) => {
                out.push(2);
                put_u64(out, term.0);
                out.push(u8::from(*vote_granted));
            }
            Message::AppendEntries(AppendEntries { term }) => {
                out.push(3);
                put_u64(out, term.0);
            }
            Message::AppendEntriesResponse(AppendEntriesResponse { term, success }) => {
                out.push(4);
                put_u64(out, term.0);
                out.push(u8::from(*success));
            }
        }
    }
}

impl TraceEncode for PersistentState {
    // Term (u64) ve oy: 0 = yok, 1 + düğüm kimliği = var. Etiket baytı sayesinde "oy yok" ile
    // "düğüm 0'a oy" birbirine karışmaz. Desen yine `..` olmadan açılır (log Faz 3'te eklenecek).
    fn encode(&self, out: &mut Vec<u8>) {
        let PersistentState {
            current_term,
            voted_for,
        } = self;
        put_u64(out, current_term.0);
        match voted_for {
            None => out.push(0),
            Some(node) => {
                out.push(1);
                put_u64(out, node.0);
            }
        }
    }
}

/// Bir term'de gözlenen seçim: o term'de aday olan düğümler ve (varsa) kazanan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Election {
    /// Bu term'de aday olarak gözlenen düğümler.
    pub candidates: BTreeSet<NodeId>,
    /// Bu term'in lideri (seçilemediyse `None`).
    pub leader: Option<NodeId>,
}

/// Bir invariant ihlali.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Violation {
    /// Aynı term'de iki lider (Figure 3, Election Safety).
    #[error(transparent)]
    ElectionSafety(#[from] ElectionSafetyViolation),
    /// Ayaktaki bir düğümün diski, bellekteki kalıcı durumundan farklı: bir değişiklik persist
    /// edilmeden kalmış.
    #[error("durability violated: node {node:?} holds {memory:?} in memory but {disk:?} on disk")]
    Durability {
        /// Düğüm.
        node: NodeId,
        /// Bellekteki (olması gereken) kalıcı durum.
        memory: PersistentState,
        /// Diskteki durum.
        disk: PersistentState,
    },
}

/// `RaftCluster` üzerindeki bir işlemin hatası.
///
/// `Violation` uygulamanın bir invariant'ı çiğnediği anlamına gelir (bulunmak istenen hata);
/// diğer varyantlar kümenin yanlış kullanıldığını söyler (ör. var olmayan bir düğümü çökertmek).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterError {
    /// Bir invariant çiğnendi.
    #[error("invariant violated at t={time}: {violation}")]
    Violation {
        /// İhlalin görüldüğü mantıksal zaman.
        time: u64,
        /// İhlalin kendisi.
        violation: Violation,
    },
    /// Geçersiz bir çökme ya da yeniden başlatma isteği.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// Geçersiz bir bölünme tanımı.
    #[error(transparent)]
    Partition(#[from] PartitionError),
}

/// Raft düğümlerinden oluşan ve her olaydan sonra invariant'ları denetleyen simülasyon.
///
/// Bütün rastgelelik tek bir ana seed'den türetilir: ağ `Component::Network`, her düğüm
/// `Component::Node(id)` akışını alır. Aynı seed, aynı ayarlar ve aynı çağrılar (çökme, bölünme,
/// ...) birebir aynı koşuyu verir.
///
/// ```
/// use sim::{NetworkConfig, RaftCluster, RaftConfig};
///
/// let mut cluster = RaftCluster::new(42, 3, NetworkConfig::reliable(2), RaftConfig::default())?;
/// // Her olaydan sonra Election Safety ve dayanıklılık denetlenir; ihlal bir hata olarak döner.
/// cluster.run_until(200)?;
/// assert_eq!(cluster.leaders().len(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct RaftCluster {
    sim: Simulation<RaftNode, SimNetwork>,
    election_safety: ElectionSafety,
    // BTreeMap: term sırasıyla gezilir, her koşuda aynı sırayla.
    elections: BTreeMap<Term, Election>,
}

impl RaftCluster {
    /// `1..=size` kimlikli düğümlerden oluşan bir küme kurar. Her düğüm boş bir diskle açılan bir
    /// Follower'dır ve ilk tick'ini 1 anında alır.
    ///
    /// # Errors
    ///
    /// Ağ ayarları geçersizse [`ConfigError`].
    pub fn new(
        master_seed: u64,
        size: u64,
        network: NetworkConfig,
        raft: Config,
    ) -> Result<Self, ConfigError> {
        let seeds = SeedTree::new(master_seed);
        let ids: BTreeSet<NodeId> = (1..=size).map(NodeId).collect();
        let network = SimNetwork::new(network, seeds.rng_for(Component::Network))?;
        let nodes = ids.iter().map(|&id| {
            let seed = seeds.seed_bytes_for(Component::Node(id));
            (id, RaftNode::new(id, ids.clone(), raft, seed))
        });
        let sim = Simulation::new(SimConfig::default(), network, nodes)?;
        Ok(Self {
            sim,
            election_safety: ElectionSafety::new(),
            elections: BTreeMap::new(),
        })
    }

    /// Alttaki simülasyon (salt okunur): trace, zaman, düğümler, diskler.
    #[must_use]
    pub fn sim(&self) -> &Simulation<RaftNode, SimNetwork> {
        &self.sim
    }

    /// Şu anki mantıksal zaman.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.sim.now()
    }

    /// Bir düğüm. Çökmüş bir düğümün bellek durumu yeniden başlatmaya kadar donmuştur.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&RaftNode> {
        self.sim.node(id)
    }

    /// Düğüm ayakta mı?
    #[must_use]
    pub fn is_up(&self, id: NodeId) -> bool {
        self.sim.is_up(id)
    }

    /// Şu an lider rolündeki ayaktaki düğümler ve term'leri, `NodeId` sırasıyla. Bölünme sırasında
    /// birden fazla olabilir: azınlıkta kalan eski bir lider, daha yüksek bir term görene kadar
    /// kendini lider sanır (farklı term'lerde oldukları için Election Safety ihlali değildir).
    #[must_use]
    pub fn leaders(&self) -> Vec<(NodeId, Term)> {
        self.sim
            .hosts()
            .filter(|host| host.up && host.node.role() == Role::Leader)
            .map(|host| (host.id, host.node.current_term()))
            .collect()
    }

    /// Koşu boyunca gözlenen seçimler, term sırasıyla.
    #[must_use]
    pub fn elections(&self) -> &BTreeMap<Term, Election> {
        &self.elections
    }

    /// Kuyruktaki en erken olayı işler ve invariant'ları denetler. Kuyruk boşsa `Ok(false)`.
    ///
    /// # Errors
    ///
    /// Olaydan sonra bir invariant çiğnenmişse [`ClusterError::Violation`].
    pub fn step(&mut self) -> Result<bool, ClusterError> {
        let progressed = self.sim.step();
        if progressed {
            self.check()?;
        }
        Ok(progressed)
    }

    /// `time` anına kadar bütün olayları işler; HER olaydan sonra invariant'ları denetler.
    ///
    /// # Errors
    ///
    /// Bir olaydan sonra bir invariant çiğnenmişse [`ClusterError::Violation`]; koşu o olayda
    /// durur.
    pub fn run_until(&mut self, time: u64) -> Result<(), ClusterError> {
        while self.sim.step_until(time) {
            self.check()?;
        }
        Ok(())
    }

    /// Bir düğümü çökertir (bkz. [`Simulation::crash`]). Hiçbir düğüm adımlanmadığından
    /// invariant'lar değişmez.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da zaten çökmüşse [`ClusterError::Lifecycle`].
    pub fn crash(&mut self, id: NodeId) -> Result<(), ClusterError> {
        Ok(self.sim.crash(id)?)
    }

    /// Çökmüş bir düğümü diskindeki durumla yeniden başlatır (bkz. [`Simulation::restart`]) ve
    /// invariant'ları HEMEN denetler: yeniden başlatma da bir düğüm adımıdır. Örneğin kalıcı durumu
    /// yanlış yükleyen bir düğüm, diskten farklı bir bellek durumuyla açılır ve bu anında
    /// yakalanır.
    ///
    /// # Errors
    ///
    /// Düğüm yoksa ya da zaten ayaktaysa [`ClusterError::Lifecycle`]; yeniden başlatmadan sonra bir
    /// invariant çiğnenmişse [`ClusterError::Violation`].
    pub fn restart(&mut self, id: NodeId) -> Result<(), ClusterError> {
        self.sim.restart(id)?;
        self.check()
    }

    /// Ağı gruplara böler (bkz. [`Simulation::partition`]).
    ///
    /// # Errors
    ///
    /// Bölünme tanımı geçersizse [`ClusterError::Partition`].
    pub fn partition(&mut self, groups: &[&[NodeId]]) -> Result<(), ClusterError> {
        Ok(self.sim.partition(groups)?)
    }

    /// Bölünmeyi kaldırır.
    pub fn heal(&mut self) {
        self.sim.heal();
    }

    /// Ayaktaki her düğüm için invariant'ları denetler ve seçim kaydını günceller.
    fn check(&mut self) -> Result<(), ClusterError> {
        let time = self.sim.now();
        for host in self.sim.hosts() {
            // Çökmüş bir düğümün bellek durumu yoktur (yeniden başlatmada diskten kurulur); eski
            // rolü ve term'i denetlenmez. Diski korunur ve düğüm açılınca yeniden denetime girer.
            if !host.up {
                continue;
            }
            let node = host.node;
            let memory = node.persistent_state();
            if *host.disk != memory {
                return Err(ClusterError::Violation {
                    time,
                    violation: Violation::Durability {
                        node: host.id,
                        memory,
                        disk: host.disk.clone(),
                    },
                });
            }
            let term = node.current_term();
            match node.role() {
                Role::Leader => {
                    self.election_safety
                        .observe_leader(term.0, host.id.0)
                        .map_err(|violation| ClusterError::Violation {
                            time,
                            violation: violation.into(),
                        })?;
                    self.elections.entry(term).or_default().leader = Some(host.id);
                }
                Role::Candidate => {
                    self.elections
                        .entry(term)
                        .or_default()
                        .candidates
                        .insert(host.id);
                }
                Role::Follower => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterError, RaftCluster, Violation};
    use crate::network::NetworkConfig;
    use crate::trace::digest;
    use raft_core::{
        AppendEntries, AppendEntriesResponse, Config, LogIndex, Message, NodeId, PersistentState,
        RequestVote, RequestVoteResponse, Term,
    };

    // Kanonik kodlama her alanı kapsar: yalnızca tek bir alanda (ya da yalnızca varyantta) farklı
    // olan mesajların ve disk kayıtlarının özetleri farklıdır.
    #[test]
    fn encodings_distinguish_every_field() {
        let request = |term, index, last_term| {
            Message::RequestVote(RequestVote {
                term: Term(term),
                last_log_index: LogIndex(index),
                last_log_term: Term(last_term),
            })
        };
        let messages = [
            request(1, 0, 0),
            request(2, 0, 0),
            request(1, 1, 0),
            request(1, 0, 1),
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(1),
                vote_granted: false,
            }),
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(1),
                vote_granted: true,
            }),
            Message::AppendEntries(AppendEntries { term: Term(1) }),
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(1),
                success: false,
            }),
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(1),
                success: true,
            }),
        ];
        let mut digests: Vec<u64> = messages.iter().map(digest).collect();
        digests.sort_unstable();
        digests.dedup();
        assert_eq!(digests.len(), messages.len());

        let state = |term, voted_for: Option<u64>| PersistentState {
            current_term: Term(term),
            voted_for: voted_for.map(NodeId),
        };
        let states = [
            state(0, None),
            state(0, Some(0)),
            state(1, None),
            state(0, Some(1)),
        ];
        let mut digests: Vec<u64> = states.iter().map(digest).collect();
        digests.sort_unstable();
        digests.dedup();
        assert_eq!(digests.len(), states.len());
    }

    // Election Safety gerçekten her adıma bağlı: kâhine sahte bir geçmiş yüklenir (term 1..=20'nin
    // lideri, var olmayan düğüm 99). Kümenin seçtiği ilk gerçek lider bu geçmişle çakışır ve ihlal
    // olarak bildirilir. `check()` içindeki gözlem silinir ya da hatası yutulursa bu test kırılır;
    // kâhinin kendi birim testleri bu bağı göremez.
    #[test]
    fn election_safety_is_checked_after_every_event() {
        let mut cluster = RaftCluster::new(1, 3, NetworkConfig::reliable(1), Config::default())
            .expect("valid config");
        for term in 1..=20 {
            cluster
                .election_safety
                .observe_leader(term, 99)
                .expect("a fresh term");
        }
        let error = cluster
            .run_until(400)
            .expect_err("the first real leader collides with node 99");
        let ClusterError::Violation {
            violation: Violation::ElectionSafety(violation),
            ..
        } = error
        else {
            panic!("expected an election safety violation, got {error:?}");
        };
        assert_eq!(violation.first, 99);
        assert!((1..=20).contains(&violation.term));
    }

    // Dayanıklılık denetimi gerçekten çalışır: bir düğümün diski bellekten ayrılırsa (sanki bir
    // adım `votedFor`'u değiştirip persist etmeyi unutmuş gibi), bir sonraki olayda ihlal
    // bildirilir; bu olay o düğüme ait olmasa bile.
    #[test]
    fn a_disk_that_diverges_from_memory_is_reported() {
        let mut cluster = RaftCluster::new(1, 3, NetworkConfig::reliable(1), Config::default())
            .expect("valid config");
        cluster.run_until(5).expect("no violation");
        let lost_vote = PersistentState {
            current_term: Term(0),
            voted_for: Some(NodeId(3)),
        };
        *cluster.sim.disk_mut(NodeId(2)).expect("node 2 exists") = lost_vote.clone();
        let error = cluster.step().expect_err("the divergence must be reported");
        assert_eq!(
            error,
            ClusterError::Violation {
                time: 6,
                violation: Violation::Durability {
                    node: NodeId(2),
                    memory: PersistentState::default(),
                    disk: lost_vote,
                },
            }
        );
    }
}
