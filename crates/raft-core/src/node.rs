//! Sans-IO Raft düğümü: tek genel API `step(Input) -> Vec<Output>`.

use std::collections::BTreeSet;

use crate::input::Input;
use crate::output::Output;
use crate::types::NodeId;

/// Bir Raft düğümünün durum makinesi.
///
/// `raft-core` içindeki TEK genel giriş noktası `step`'tir: saat, ağ ve disk tamamen dışarıdadır
/// (sans-IO mimarisi). Bu sayede sürücü (simülatör veya gerçek çalıştırıcı) her adımı tam kontrol
/// eder, testler deterministik olur ve `raft-core` hiçbir G/Ç bağımlılığı taşımaz.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftNode {
    // Private: dışarıdan yalnızca `id()` erişimcisiyle okunur, doğrudan alan erişimi yoktur.
    id: NodeId,
    // Private ve BTreeSet: yineleme sırası deterministik olsun diye (bkz. crate kuralı,
    // `HashMap`/`HashSet` yasak). Faz 2'de çoğunluk (majority) hesaplarken `peers.len() + 1`
    // kullanılacak; bu yüzden `id`'nin kendisi bu kümede bulunmamalı (N1).
    peers: BTreeSet<NodeId>,
}

impl RaftNode {
    /// Yeni bir düğüm oluşturur.
    ///
    /// N1: `id`, `peers` kümesinde varsa çıkarılır ("kendi kendinin eşi olamaz"). Gerekçe: Faz 2'de
    /// çoğunluk sayımı `peers().len() + 1` (kendisi + eşler) biçiminde yapılacak; `peers` içinde
    /// yanlışlıkla bir kendi-girdisi kalırsa bu sayım bir fazla sayar ve çoğunluk yanlış
    /// hesaplanır.
    #[must_use]
    pub fn new(id: NodeId, mut peers: BTreeSet<NodeId>) -> Self {
        peers.remove(&id);
        Self { id, peers }
    }

    /// Kurucuya verilen düğüm kimliğini döndürür (N2).
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Normalize edilmiş (kendisi çıkarılmış, sıralı) eş kümesini döndürür (N2).
    #[must_use]
    pub fn peers(&self) -> &BTreeSet<NodeId> {
        &self.peers
    }

    /// Bir girdiyi işler, sürücünün SIRAYLA yürütmesi gereken çıktı listesini döndürür.
    ///
    /// `#[must_use]`: dönen listenin tamamen yok sayılması (hiçbir mesaj gitmez, hiçbir durum
    /// persist edilmez) neredeyse her zaman bir sürücü hatasıdır ve derleyici bunu uyarır. Listeyi
    /// sırasız ya da eksik yürütmek (O1 ihlali) ise derleyicinin göremeyeceği, sürücünün
    /// sorumluluğundaki bir hatadır.
    ///
    /// N3: `step` tam (total) bir fonksiyondur — her `Input` değeri için panik atmadan döner.
    /// Faz 0'da mantık henüz yok: boş bir `Vec` döner ve `self`'i değiştirmeden bırakır. Gövde,
    /// panik atan bir "henüz yazılmadı" yer tutucu makrosuyla bırakılmadı, çünkü simülatör her
    /// düğümü her tick'te adımlar; bir panik tüm koşuyu ve determinizm testlerini çökertirdi.
    /// Boş liste, sans-IO sözleşmesi altında "yapılacak bir şey yok" anlamına gelen geçerli bir
    /// cevaptır.
    #[must_use = "outputs must be executed in order: Persist before the Sends that depend on it"]
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        let _ = input;
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::RaftNode;
    use crate::input::Input;
    use crate::message::Message;
    use crate::persist::PersistentState;
    use crate::types::{Command, NodeId};
    use std::collections::BTreeSet;

    // Kapsama bekçisi: `Input`'in her varyantı için ayrık bir isim döndürür. Kasıtlı olarak `_`
    // kolu YOK — Faz 2/3'te `Input`'e yeni bir varyant eklenirse bu fonksiyon derlenmez; derleme
    // hatası seni bu teste getirir. Yeni varyant için hem aşağıdaki girdiyi hem de beklenen isim
    // kümesini güncelle (yalnızca buraya kol eklemek küme karşılaştırmasını geçirmeye yetmez).
    fn input_name(input: &Input) -> &'static str {
        match input {
            Input::Tick => "Tick",
            Input::Message { .. } => "Message",
            Input::ClientRequest(_) => "ClientRequest",
            Input::Restart(_) => "Restart",
        }
    }

    // Aynı bekçi mantığı `Message` için: 4 RPC varyantının hepsi kapsanmalı.
    fn message_name(msg: &Message) -> &'static str {
        match msg {
            Message::RequestVote => "RequestVote",
            Message::RequestVoteResponse => "RequestVoteResponse",
            Message::AppendEntries => "AppendEntries",
            Message::AppendEntriesResponse => "AppendEntriesResponse",
        }
    }

    // N3: her `Input` varyantı (ve her `Message` varyantı `Message{..}` içinde) için `step`'in boş
    // `Vec` döndürdüğünü ve düğümün adımdan önceki hâliyle AYNI kaldığını doğrular. Ayrıca "kapsama
    // bekçisi" (`input_name`/`message_name`) ile kullanılan varyant isim kümelerinin tam beklenen
    // kümelere eşit olduğunu kontrol eder: yeni bir varyant eklenip bu test güncellenmezse ya
    // derleme kırılır (exhaustive match) ya da küme karşılaştırması başarısız olur.
    #[test]
    fn step_is_a_no_op_for_every_input_variant() {
        let peers: BTreeSet<NodeId> = [NodeId(2), NodeId(3)].into_iter().collect();

        let messages = [
            Message::RequestVote,
            Message::RequestVoteResponse,
            Message::AppendEntries,
            Message::AppendEntriesResponse,
        ];

        let mut covered_message_names: BTreeSet<&'static str> = BTreeSet::new();
        for msg in &messages {
            covered_message_names.insert(message_name(msg));

            let mut node = RaftNode::new(NodeId(1), peers.clone());
            let before = node.clone();
            let input = Input::Message {
                from: NodeId(2),
                msg: msg.clone(),
            };
            let outputs = node.step(input);
            assert!(outputs.is_empty());
            assert_eq!(node, before);
        }
        let expected_message_names: BTreeSet<&'static str> = [
            "RequestVote",
            "RequestVoteResponse",
            "AppendEntries",
            "AppendEntriesResponse",
        ]
        .into_iter()
        .collect();
        assert_eq!(covered_message_names, expected_message_names);

        let mut covered_input_names: BTreeSet<&'static str> = BTreeSet::new();

        let tick = Input::Tick;
        covered_input_names.insert(input_name(&tick));
        let mut node = RaftNode::new(NodeId(1), peers.clone());
        let before = node.clone();
        assert!(node.step(tick).is_empty());
        assert_eq!(node, before);

        let msg_input = Input::Message {
            from: NodeId(2),
            msg: Message::RequestVote,
        };
        covered_input_names.insert(input_name(&msg_input));
        let mut node = RaftNode::new(NodeId(1), peers.clone());
        let before = node.clone();
        assert!(node.step(msg_input).is_empty());
        assert_eq!(node, before);

        let client_request = Input::ClientRequest(Command::new(vec![1, 2, 3]));
        covered_input_names.insert(input_name(&client_request));
        let mut node = RaftNode::new(NodeId(1), peers.clone());
        let before = node.clone();
        assert!(node.step(client_request).is_empty());
        assert_eq!(node, before);

        let restart = Input::Restart(PersistentState::default());
        covered_input_names.insert(input_name(&restart));
        let mut node = RaftNode::new(NodeId(1), peers.clone());
        let before = node.clone();
        assert!(node.step(restart).is_empty());
        assert_eq!(node, before);

        let expected_input_names: BTreeSet<&'static str> =
            ["Tick", "Message", "ClientRequest", "Restart"]
                .into_iter()
                .collect();
        assert_eq!(covered_input_names, expected_input_names);
    }

    // N1: kurucu, `id`'yi `peers` kümesinden çıkarır; aksi hâlde Faz 2'nin çoğunluk sayımı
    // (`peers().len() + 1`) kendini iki kez sayardı.
    #[test]
    fn new_removes_self_from_peers() {
        let peers: BTreeSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
        let node = RaftNode::new(NodeId(1), peers);

        let expected: BTreeSet<NodeId> = [NodeId(2), NodeId(3)].into_iter().collect();
        assert_eq!(node.peers(), &expected);
    }

    // N2: erişimciler kurucuya verilen değerleri (normalize edilmiş hâliyle) birebir yansıtır.
    #[test]
    fn accessors_return_constructor_values() {
        let peers: BTreeSet<NodeId> = [NodeId(2), NodeId(3)].into_iter().collect();
        let node = RaftNode::new(NodeId(1), peers.clone());

        assert_eq!(node.id(), NodeId(1));
        assert_eq!(node.peers(), &peers);
    }
}
