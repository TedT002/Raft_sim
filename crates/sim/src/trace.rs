//! Trace: işlenen her olayın kanonik kaydı ve sürümler arası kararlı özeti.
//!
//! Trace özeti bir simülasyon koşusunun kimliğidir: aynı seed ve aynı ayarlarla iki koşu aynı özeti
//! vermelidir (determinizm testi). Bu yüzden her olay açıkça tanımlanmış bir bayt kodlamasıyla
//! (sabit etiketler, little-endian sabit genişlikli sayılar, uzunluk önekli listeler) FNV-1a'ya
//! katılır. `Debug` çıktısı (biçimi garanti değildir) ve `#[derive(Hash)]` bilerek kullanılmaz.
//! Kodlama önek-özgürdür (prefix-free): her olayın uzunluğu etiketinden ve uzunluk öneklerinden
//! belirlenir, dolayısıyla farklı iki olay dizisi aynı bayt akışını üretemez.

use raft_core::NodeId;

use crate::fnv::{Fnv1a64, fnv1a64};

/// Bir değerin trace'e girecek kanonik bayt kodlaması: düğümler arası mesajlar (`SimNode::Msg`) ve
/// diske yazılan kalıcı durum (`SimNode::Durable`) için.
///
/// Aynı değer her zaman aynı baytları üretmelidir; farklı değerler mümkünse farklı baytlar. Kodlama
/// bir kez yayımlandıktan sonra değiştirilirse, o tipi kullanan koşuların özetleri de değişir.
pub trait TraceEncode {
    /// Değerin kanonik baytlarını `out`'un sonuna ekler.
    fn encode(&self, out: &mut Vec<u8>);
}

/// Kalıcı durumu olmayan düğümler (`type Durable = ()`) için: boş kodlama.
impl TraceEncode for () {
    fn encode(&self, _out: &mut Vec<u8>) {}
}

/// Bir mesajın ya da kalıcı durumun trace özeti: kanonik baytlarının FNV-1a 64 değeri.
#[must_use]
pub fn digest<M: TraceEncode + ?Sized>(value: &M) -> u64 {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    fnv1a64(&bytes)
}

/// Bir mesajın (ya da kopyasının) neden teslim edilmediği.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Ağın rastgele kaybı (`drop_prob`).
    Random,
    /// Gönderim anında iki uç farklı bölünme gruplarındaydı.
    PartitionAtSend,
    /// Mesaj yoldayken uçlar en az bir an ayrı gruplara düştü; bölünme teslim anından önce
    /// iyileşmiş olsa bile ("kablo kesildi": kesik bir bağlantıdaki paket kaybolur).
    PartitionInFlight,
    /// Hedef düğüm simülasyonda yok.
    UnknownDestination,
    /// Teslim zamanı `u64` zaman ekseninin sonunu aşıyor; mesaj sessizce yok edilmez, açıkça düşer.
    TimeOverflow,
    /// Hedef düğüm teslim anında çökmüş durumdaydı. Düğüm sonradan yeniden başlasa bile bu mesajı
    /// almaz: kapalı bir makine kendisine gelen paketi saklamaz.
    NodeDown,
}

impl DropReason {
    /// Kanonik etiket (değiştirmek trace özetlerini değiştirir).
    const fn tag(self) -> u8 {
        match self {
            DropReason::Random => 1,
            DropReason::PartitionAtSend => 2,
            DropReason::PartitionInFlight => 3,
            DropReason::UnknownDestination => 4,
            DropReason::TimeOverflow => 5,
            DropReason::NodeDown => 6,
        }
    }
}

/// Trace'e kaydedilen olayın türü ve ayrıntıları.
///
/// `msg_id` her gönderime verilen, simülasyon boyunca artan benzersiz numaradır: bir teslimi ya da
/// düşüşü, kendisini üreten gönderime kesin olarak bağlar (aynı içerikli iki mesaj aynı anda
/// gönderilse bile).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceKind {
    /// Bir düğüme mantıksal zaman ilerlemesi (tick) verildi.
    Tick {
        /// Tick'i alan düğüm.
        node: NodeId,
    },
    /// Bir düğüm bir mesaj gönderdi (ağın kararından önce).
    Send {
        /// Gönderimin benzersiz numarası.
        msg_id: u64,
        /// Gönderen.
        from: NodeId,
        /// Alıcı.
        to: NodeId,
        /// Mesaj özeti.
        digest: u64,
    },
    /// Bir mesaj (ya da çoğaltılmış kopyası) teslim edildi.
    Deliver {
        /// Teslim edilen gönderimin numarası.
        msg_id: u64,
        /// Gönderen.
        from: NodeId,
        /// Alıcı.
        to: NodeId,
        /// Mesaj özeti.
        digest: u64,
        /// Mesajın gönderildiği zaman.
        sent_at: u64,
    },
    /// Bir mesaj (ya da kopyası) teslim edilmeden düştü.
    Drop {
        /// Düşen gönderimin numarası.
        msg_id: u64,
        /// Gönderen.
        from: NodeId,
        /// Alıcı.
        to: NodeId,
        /// Mesaj özeti.
        digest: u64,
        /// Mesajın gönderildiği zaman.
        sent_at: u64,
        /// Neden düştüğü.
        reason: DropReason,
    },
    /// Ağ bölündü. Gruplar kanonik biçimde (her grup sıralı, gruplar sıralı) kaydedilir: aynı
    /// anlama gelen iki bölünme aynı özeti vermeli.
    Partition {
        /// Bölünme grupları, kanonik sırayla.
        groups: Vec<Vec<NodeId>>,
    },
    /// Bölünme sona erdi.
    Heal,
    /// Bir düğüm kalıcı durumunu diske yazdı. Özet (`digest`) yazılan durumun kanonik
    /// kodlamasınındır: iki koşu, düğümlerin iç durumu ayrıştığı anda (ilk farklı mesajı
    /// beklemeden) farklı trace özeti verir.
    Persist {
        /// Diske yazan düğüm.
        node: NodeId,
        /// Yazılan durumun özeti.
        digest: u64,
    },
    /// Bir düğüm çöktü: tick almaz, mesajları teslim anında düşer.
    Crash {
        /// Çöken düğüm.
        node: NodeId,
    },
    /// Çökmüş bir düğüm diskindeki durumla yeniden başlatıldı.
    Restart {
        /// Yeniden başlatılan düğüm.
        node: NodeId,
    },
    /// Bir düğümün en eski bekleyen yazmasının `fsync`'i tamamlandı: yazma artık kalıcı.
    Sync {
        /// Yazması kalıcı olan düğüm.
        node: NodeId,
    },
    /// Bir çökmenin kaybettirdikleri: bekleyen yazmalardan kaçı yine de diske ulaştı, kaçı kayboldu
    /// ve fsync bekleyen kaç çıktı (mesaj ya da uygulama) hiç bırakılamadan yok oldu. Yalnızca
    /// kaybolacak bir şey varken kaydedilir.
    CrashLoss {
        /// Çöken düğüm.
        node: NodeId,
        /// Diske ulaşmış sayılan bekleyen yazmalar ("kısmen yazılır").
        kept_writes: u64,
        /// Kaybolan bekleyen yazmalar.
        lost_writes: u64,
        /// Bırakılamadan yok olan çıktılar.
        dropped_outputs: u64,
    },
    /// Bir düğüm sırası gelen bir yerel etkiyi bıraktı (ör. commit edilmiş bir girdinin durum
    /// makinesine uygulanması).
    Apply {
        /// Etkiyi bırakan düğüm.
        node: NodeId,
        /// Etkinin özeti.
        digest: u64,
    },
    /// Bir düğüme istemci isteği verildi.
    Client {
        /// İsteği alan düğüm.
        node: NodeId,
        /// İsteğin özeti.
        digest: u64,
    },
}

/// Belirli bir zamanda gerçekleşen bir trace olayı.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEvent {
    /// Olayın gerçekleştiği mantıksal zaman.
    pub time: u64,
    /// Olayın türü ve ayrıntıları.
    pub kind: TraceKind,
}

impl TraceEvent {
    /// Olayı kanonik baytlarıyla özete katar.
    fn encode_into(&self, hasher: &mut Fnv1a64) {
        hasher.write_u64(self.time);
        match &self.kind {
            TraceKind::Tick { node } => {
                hasher.write_u8(1);
                hasher.write_u64(node.0);
            }
            TraceKind::Send {
                msg_id,
                from,
                to,
                digest,
            } => {
                hasher.write_u8(2);
                hasher.write_u64(*msg_id);
                hasher.write_u64(from.0);
                hasher.write_u64(to.0);
                hasher.write_u64(*digest);
            }
            TraceKind::Deliver {
                msg_id,
                from,
                to,
                digest,
                sent_at,
            } => {
                hasher.write_u8(3);
                hasher.write_u64(*msg_id);
                hasher.write_u64(from.0);
                hasher.write_u64(to.0);
                hasher.write_u64(*digest);
                hasher.write_u64(*sent_at);
            }
            TraceKind::Drop {
                msg_id,
                from,
                to,
                digest,
                sent_at,
                reason,
            } => {
                hasher.write_u8(4);
                hasher.write_u64(*msg_id);
                hasher.write_u64(from.0);
                hasher.write_u64(to.0);
                hasher.write_u64(*digest);
                hasher.write_u64(*sent_at);
                hasher.write_u8(reason.tag());
            }
            TraceKind::Partition { groups } => {
                hasher.write_u8(5);
                // Uzunluk önekleri: [[1],[2,3]] ile [[1,2],[3]] aynı baytlara düşmesin. Uzunluklar
                // platforma bağlı `usize` olarak değil, her zaman u64 olarak yazılır.
                hasher.write_u64(groups.len() as u64);
                for group in groups {
                    hasher.write_u64(group.len() as u64);
                    for node in group {
                        hasher.write_u64(node.0);
                    }
                }
            }
            TraceKind::Heal => hasher.write_u8(6),
            TraceKind::Persist { node, digest } => {
                hasher.write_u8(7);
                hasher.write_u64(node.0);
                hasher.write_u64(*digest);
            }
            TraceKind::Crash { node } => {
                hasher.write_u8(8);
                hasher.write_u64(node.0);
            }
            TraceKind::Restart { node } => {
                hasher.write_u8(9);
                hasher.write_u64(node.0);
            }
            TraceKind::Sync { node } => {
                hasher.write_u8(10);
                hasher.write_u64(node.0);
            }
            TraceKind::CrashLoss {
                node,
                kept_writes,
                lost_writes,
                dropped_outputs,
            } => {
                hasher.write_u8(11);
                hasher.write_u64(node.0);
                hasher.write_u64(*kept_writes);
                hasher.write_u64(*lost_writes);
                hasher.write_u64(*dropped_outputs);
            }
            TraceKind::Apply { node, digest } => {
                hasher.write_u8(12);
                hasher.write_u64(node.0);
                hasher.write_u64(*digest);
            }
            TraceKind::Client { node, digest } => {
                hasher.write_u8(13);
                hasher.write_u64(node.0);
                hasher.write_u64(*digest);
            }
        }
    }
}

/// Bir koşunun olay kaydı ve sürekli (running) FNV-1a özeti.
#[derive(Debug, Clone, Default)]
pub struct Trace {
    events: Vec<TraceEvent>,
    hasher: Fnv1a64,
}

impl Trace {
    /// Boş bir trace.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Olayı hem kayda hem özete ekler.
    pub(crate) fn record(&mut self, event: TraceEvent) {
        event.encode_into(&mut self.hasher);
        self.events.push(event);
    }

    /// Kaydedilen olaylar, gerçekleşme sırasıyla.
    #[must_use]
    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }

    /// Şu ana kadarki olayların özeti.
    #[must_use]
    pub fn hash(&self) -> u64 {
        self.hasher.finish()
    }

    /// Olay sayısı.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Hiç olay yok mu?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{Trace, TraceEvent, TraceKind};
    use raft_core::NodeId;

    fn partition(groups: &[&[u64]]) -> TraceEvent {
        TraceEvent {
            time: 1,
            kind: TraceKind::Partition {
                groups: groups
                    .iter()
                    .map(|g| g.iter().map(|&id| NodeId(id)).collect())
                    .collect(),
            },
        }
    }

    // Aynı olaylar aynı özet; olay sırası özete dahildir.
    #[test]
    fn hash_depends_on_events_and_their_order() {
        let tick = |node| TraceEvent {
            time: 1,
            kind: TraceKind::Tick { node: NodeId(node) },
        };
        let mut a = Trace::new();
        a.record(tick(1));
        a.record(tick(2));
        let mut b = Trace::new();
        b.record(tick(1));
        b.record(tick(2));
        let mut swapped = Trace::new();
        swapped.record(tick(2));
        swapped.record(tick(1));
        assert_eq!(a.hash(), b.hash());
        assert_ne!(a.hash(), swapped.hash());
        assert_eq!(a.len(), 2);
    }

    // Yaşam döngüsü ve disk olayları birbirinden ayrışır: aynı düğüm için her tür farklı bir
    // etiketle kodlanır; özetler ve sayaçlar da hash'e girer.
    #[test]
    fn lifecycle_events_have_distinct_encodings() {
        let node = NodeId(3);
        let kinds = [
            TraceKind::Tick { node },
            TraceKind::Crash { node },
            TraceKind::Restart { node },
            TraceKind::Persist { node, digest: 1 },
            TraceKind::Persist { node, digest: 2 },
            TraceKind::Sync { node },
            TraceKind::CrashLoss {
                node,
                kept_writes: 0,
                lost_writes: 1,
                dropped_outputs: 0,
            },
            TraceKind::CrashLoss {
                node,
                kept_writes: 1,
                lost_writes: 0,
                dropped_outputs: 0,
            },
            TraceKind::Apply { node, digest: 1 },
            TraceKind::Client { node, digest: 1 },
        ];
        let count = kinds.len();
        let mut hashes: Vec<u64> = kinds
            .into_iter()
            .map(|kind| {
                let mut trace = Trace::new();
                trace.record(TraceEvent { time: 1, kind });
                trace.hash()
            })
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        assert_eq!(hashes.len(), count);
    }

    // Uzunluk önekleri sayesinde grup sınırları özete girer: [[1],[2,3]] ≠ [[1,2],[3]].
    #[test]
    fn partition_group_boundaries_are_part_of_the_hash() {
        let mut a = Trace::new();
        a.record(partition(&[&[1], &[2, 3]]));
        let mut b = Trace::new();
        b.record(partition(&[&[1, 2], &[3]]));
        assert_ne!(a.hash(), b.hash());
    }
}
