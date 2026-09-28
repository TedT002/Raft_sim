//! Faz 1'in geçici test protokolü (Raft DEĞİL) ve testlerin ortak yardımcıları.
//!
//! Ping/Pong yalnızca `sim/tests` altında yaşar ve `raft-core`'a girmez: simülatörün protokolden
//! bağımsız olduğunu, yani Raft'ı hiç bilmeden bir düğüm kümesini sürebildiğini gösterir. Her düğüm
//! her tick'te rastgele bir eşine `Ping` yollar; `Ping` alan `Pong` ile cevap verir.

// Her entegrasyon testi dosyası bu modülü ayrı bir crate olarak derler ve yardımcıların yalnızca
// bir kısmını kullanır. Kullanılmayanlar için `dead_code` uyarısı `-D warnings` altında hata
// olurdu.
#![allow(dead_code)]

use sim::{
    ChaCha8Rng, Component, DropReason, NetworkConfig, NodeId, NodeInput, NodeOutput, SeedTree,
    SimConfig, SimNetwork, SimNode, Simulation, TraceEncode, TraceKind, uniform_inclusive,
};

/// Ping/Pong mesajları. `nonce`, gönderenin her Ping'e verdiği artan numaradır.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PingPong {
    /// "Orada mısın?"
    Ping {
        /// Gönderenin Ping sayacı.
        nonce: u64,
    },
    /// Bir Ping'e cevap (Ping'in `nonce`'unu geri taşır).
    Pong {
        /// Cevaplanan Ping'in sayacı.
        nonce: u64,
    },
}

impl TraceEncode for PingPong {
    // Kanonik kodlama: tür etiketi + nonce (little-endian).
    fn encode(&self, out: &mut Vec<u8>) {
        let (tag, nonce) = match self {
            PingPong::Ping { nonce } => (1_u8, nonce),
            PingPong::Pong { nonce } => (2_u8, nonce),
        };
        out.push(tag);
        out.extend_from_slice(&nonce.to_le_bytes());
    }
}

/// Ping/Pong düğümü.
#[derive(Debug, Clone)]
pub struct PingPongNode {
    peers: Vec<NodeId>,
    rng: ChaCha8Rng,
    next_nonce: u64,
    /// Gönderilen her Ping'in hedefi ve nonce'u, sırasıyla. Yalnızca düğümün kendi RNG'sine
    /// bağlıdır.
    pub pings_sent: Vec<(NodeId, u64)>,
    /// Alınan Pong sayısı.
    pub pongs_received: u64,
}

impl PingPongNode {
    /// `peers` içinden kendisi çıkarılır ve eşler sıralanır: rastgele seçim hep aynı listeden
    /// yapılsın.
    pub fn new(id: NodeId, peers: impl IntoIterator<Item = NodeId>, rng: ChaCha8Rng) -> Self {
        let mut peers: Vec<NodeId> = peers.into_iter().filter(|&peer| peer != id).collect();
        peers.sort_unstable();
        peers.dedup();
        Self {
            peers,
            rng,
            next_nonce: 0,
            pings_sent: Vec::new(),
            pongs_received: 0,
        }
    }
}

impl SimNode for PingPongNode {
    type Msg = PingPong;

    fn step(&mut self, input: NodeInput<PingPong>) -> Vec<NodeOutput<PingPong>> {
        match input {
            NodeInput::Tick => {
                let Some(last) = self.peers.len().checked_sub(1) else {
                    return Vec::new();
                };
                // Rastgelelik YALNIZCA düğümün kendi RNG'sinden gelir (SeedTree'deki Node
                // bileşeni).
                let index = uniform_inclusive(&mut self.rng, 0, last as u64) as usize;
                let to = self.peers[index];
                let nonce = self.next_nonce;
                self.next_nonce += 1;
                self.pings_sent.push((to, nonce));
                vec![NodeOutput::Send {
                    to,
                    msg: PingPong::Ping { nonce },
                }]
            }
            NodeInput::Message {
                from,
                msg: PingPong::Ping { nonce },
            } => vec![NodeOutput::Send {
                to: from,
                msg: PingPong::Pong { nonce },
            }],
            NodeInput::Message {
                msg: PingPong::Pong { .. },
                ..
            } => {
                self.pongs_received += 1;
                Vec::new()
            }
        }
    }
}

/// Ping/Pong simülasyonu.
pub type PingPongSim = Simulation<PingPongNode, SimNetwork>;

/// `1..=n` kimlikli düğümlerden oluşan bir küme. Bütün rastgelelik tek ana seed'den türetilir:
/// ağ `Component::Network`, her düğüm `Component::Node(id)` akışını kullanır.
pub fn cluster(master_seed: u64, n: u64, network: NetworkConfig) -> PingPongSim {
    let seeds = SeedTree::new(master_seed);
    build(
        master_seed,
        n,
        &[],
        network,
        seeds.rng_for(Component::Network),
    )
}

/// `cluster` ile aynı, ama ağın RNG'si dışarıdan verilir (alt-seed bağımsızlığı testi için).
pub fn cluster_with_network_rng(
    master_seed: u64,
    n: u64,
    network: NetworkConfig,
    network_rng: ChaCha8Rng,
) -> PingPongSim {
    build(master_seed, n, &[], network, network_rng)
}

/// `cluster` ile aynı, ama her düğümün eş listesine simülasyonda OLMAYAN `ghosts` da eklenir:
/// bu eşlere giden Ping'ler "bilinmeyen hedef" olarak düşer.
pub fn cluster_with_ghost_peers(
    master_seed: u64,
    n: u64,
    ghosts: &[NodeId],
    network: NetworkConfig,
) -> PingPongSim {
    let seeds = SeedTree::new(master_seed);
    build(
        master_seed,
        n,
        ghosts,
        network,
        seeds.rng_for(Component::Network),
    )
}

fn build(
    master_seed: u64,
    n: u64,
    ghosts: &[NodeId],
    network: NetworkConfig,
    network_rng: ChaCha8Rng,
) -> PingPongSim {
    let seeds = SeedTree::new(master_seed);
    let ids: Vec<NodeId> = (1..=n).map(NodeId).collect();
    let network = SimNetwork::new(network, network_rng).expect("valid network config");
    let nodes = ids.iter().map(|&id| {
        let peers = ids.iter().chain(ghosts).copied();
        let node = PingPongNode::new(id, peers, seeds.rng_for(Component::Node(id)));
        (id, node)
    });
    Simulation::new(SimConfig::default(), network, nodes).expect("valid simulation config")
}

/// Kayıplı, çoğaltmalı, değişken gecikmeli "kötü" bir ağ: determinizm testleri için.
pub const LOSSY: NetworkConfig = NetworkConfig {
    drop_prob: 0.1,
    duplicate_prob: 0.1,
    min_delay: 1,
    max_delay: 5,
};

/// Trace'teki bir gönderim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendRecord {
    /// Gönderimin benzersiz numarası.
    pub msg_id: u64,
    /// Gönderen.
    pub from: NodeId,
    /// Alıcı.
    pub to: NodeId,
    /// Gönderim zamanı.
    pub at: u64,
}

/// Trace'teki bir teslim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryRecord {
    /// Teslim edilen gönderimin numarası.
    pub msg_id: u64,
    /// Gönderen.
    pub from: NodeId,
    /// Alıcı.
    pub to: NodeId,
    /// Gönderim zamanı.
    pub sent_at: u64,
    /// Teslim zamanı.
    pub at: u64,
}

/// Trace'teki bir düşüş.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropRecord {
    /// Düşen gönderimin numarası.
    pub msg_id: u64,
    /// Gönderim zamanı.
    pub sent_at: u64,
    /// Düşme zamanı.
    pub at: u64,
    /// Neden.
    pub reason: DropReason,
}

/// Trace'teki bütün gönderimler.
pub fn sends(sim: &PingPongSim) -> Vec<SendRecord> {
    sim.trace()
        .events()
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Send {
                msg_id, from, to, ..
            } => Some(SendRecord {
                msg_id,
                from,
                to,
                at: event.time,
            }),
            _ => None,
        })
        .collect()
}

/// Trace'teki bütün teslimler.
pub fn deliveries(sim: &PingPongSim) -> Vec<DeliveryRecord> {
    sim.trace()
        .events()
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Deliver {
                msg_id,
                from,
                to,
                sent_at,
                ..
            } => Some(DeliveryRecord {
                msg_id,
                from,
                to,
                sent_at,
                at: event.time,
            }),
            _ => None,
        })
        .collect()
}

/// Trace'teki bütün düşüşler.
pub fn drops(sim: &PingPongSim) -> Vec<DropRecord> {
    sim.trace()
        .events()
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Drop {
                msg_id,
                sent_at,
                reason,
                ..
            } => Some(DropRecord {
                msg_id,
                sent_at,
                at: event.time,
                reason,
            }),
            _ => None,
        })
        .collect()
}

/// Verilen nedenle düşen kayıt sayısı.
pub fn count_drops(sim: &PingPongSim, reason: DropReason) -> usize {
    drops(sim)
        .iter()
        .filter(|drop| drop.reason == reason)
        .count()
}
