//! Seed yönetimi: tek bir ana seed'den bileşen başına bağımsız, yeniden üretilebilir RNG akışları.
//!
//! Neden tek bir paylaşılan RNG değil: ağ, disk ve her düğüm aynı RNG'den çekseydi, bir bileşene
//! eklenen tek bir yeni rastgele çağrı diğer bütün bileşenlerin akışını kaydırırdı. Küçük bir kod
//! değişikliği, ilgisiz bir düğümün bütün kararlarını değiştirir ve yayımlanmış seed'ler anlamını
//! yitirirdi. Bunun yerine her bileşen, yalnızca `(ana seed, bileşen)` ikilisinden türetilen KENDİ
//! akışına sahiptir.

use raft_core::NodeId;
pub use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};

use crate::fnv::Fnv1a64;

/// Türetmeyi sürümlere ayıran alan etiketi (domain separation). Türetme yöntemi bir gün değişirse
/// etiket de değişir; böylece eski ve yeni seed'ler asla birbirine karışmaz.
const DOMAIN: &[u8] = b"raftsim/seed-tree/v1";

/// Kendi rastgele akışına sahip bir simülasyon bileşeni.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Component {
    /// Simüle ağ: kayıp, çoğaltma ve gecikme kararları.
    Network,
    /// Simüle disk (Faz 3).
    Disk,
    /// Hata senaryosu üreteci (Faz 5).
    Scenario,
    /// Tek bir düğümün kendi kararları (ör. Raft'ta seçim zaman aşımı).
    Node(NodeId),
}

impl Component {
    /// Bileşenin kanonik kodlaması: sabit bir etiket baytı ve varsa düğüm kimliği (little-endian).
    /// Etiketler elle sabitlenmiştir; enum varyantlarının sırası değişse bile türetilen seed'ler
    /// değişmez (derive edilen sıraya güvenmiyoruz).
    fn encode(self, hasher: &mut Fnv1a64) {
        match self {
            Component::Network => hasher.write_u8(1),
            Component::Disk => hasher.write_u8(2),
            Component::Scenario => hasher.write_u8(3),
            Component::Node(NodeId(id)) => {
                hasher.write_u8(4);
                hasher.write_u64(id);
            }
        }
    }
}

/// Tek bir ana seed'den bileşen başına alt-seed ve RNG türeten yapı.
///
/// Türetme yöntemi (değiştirmek yayımlanmış bütün seed'leri geçersiz kılar):
/// 1. `FNV-1a(DOMAIN ‖ ana seed (LE) ‖ bileşen kodlaması)` ile 64 bitlik bir özet alınır.
/// 2. Özet, SplitMix64'ün karıştırma adımından geçirilir: FNV'nin girdideki küçük farkları çıktıya
///    yeterince dağıtmadığı durumlara karşı (ör. ardışık düğüm kimlikleri) bitler iyice
///    karıştırılır. Sonuç, bileşenin **alt-seed**'idir.
/// 3. Alt-seed, SplitMix64 üreteci dört kez adımlanarak ChaCha8'in 32 baytlık anahtarına
///    genişletilir. `SeedableRng::seed_from_u64` bilerek kullanılmaz: onun algoritması
///    `rand_core`'un bir sonraki sürümünde değişebilir; bizimki ise burada, testlerle sabitlenmiş
///    hâldedir.
///
/// Sonuç yalnızca `(ana seed, bileşen)` ikilisine bağlıdır: hangi sırayla ve kaç kez istendiğine,
/// başka bir bileşenin kaç sayı çektiğine bağlı DEĞİLDİR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedTree {
    master: u64,
}

impl SeedTree {
    /// Verilen ana seed ile bir seed ağacı kurar.
    #[must_use]
    pub const fn new(master: u64) -> Self {
        Self { master }
    }

    /// Ana seed.
    #[must_use]
    pub const fn master(&self) -> u64 {
        self.master
    }

    /// Bileşenin 64 bitlik alt-seed'i.
    #[must_use]
    pub fn seed_for(&self, component: Component) -> u64 {
        let mut hasher = Fnv1a64::new();
        hasher.write(DOMAIN);
        hasher.write_u64(self.master);
        component.encode(&mut hasher);
        splitmix64_mix(hasher.finish())
    }

    /// Bileşenin kendi RNG'si: alt-seed'den genişletilmiş anahtarla kurulan bir `ChaCha8Rng`.
    /// ChaCha8 akışı platformlar ve sürümler arasında değer-kararlıdır.
    #[must_use]
    pub fn rng_for(&self, component: Component) -> ChaCha8Rng {
        ChaCha8Rng::from_seed(expand_seed(self.seed_for(component)))
    }
}

/// SplitMix64'ün karıştırma (finalizer) adımı: yakın girdileri birbirinden uzak çıktılara çevirir.
fn splitmix64_mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// 64 bitlik alt-seed'i ChaCha'nın 32 baytlık anahtarına genişletir: SplitMix64 üretecini 4 kez
/// adımlar ve her çıktıyı little-endian yazar.
fn expand_seed(seed: u64) -> [u8; 32] {
    let mut state = seed;
    let mut key = [0_u8; 32];
    // 32 bayt = tam 4 adet 8 baytlık parça; kalan kısım (ikinci eleman) her zaman boştur.
    let (chunks, _) = key.as_chunks_mut::<8>();
    for chunk in chunks {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        *chunk = splitmix64_mix(state).to_le_bytes();
    }
    key
}

/// `[lo, hi]` kapalı aralığından bir tam sayı. Her zaman TAM OLARAK bir `next_u64` çeker.
///
/// Neden sabit tek çekiliş: bir kararın tükettiği rastgele sayı adedi sonuca göre değişseydi, aynı
/// akıştan sonraki bütün kararlar kayardı. Bunun bedeli çok küçük bir mod sapmasıdır: `n`
/// genişlikli bir aralıkta olasılıklar en fazla `n / 2^64` kadar farklılaşır, simülasyon için
/// önemsizdir. `lo > hi` geçersiz bir çağrıdır ve `lo` döner. Yapılandırmalar bunu zaten doğrulama
/// aşamasında engeller; burada panik atılmaz.
pub fn uniform_inclusive<R: Rng + ?Sized>(rng: &mut R, lo: u64, hi: u64) -> u64 {
    let draw = rng.next_u64();
    if lo > hi {
        return lo;
    }
    match (hi - lo).checked_add(1) {
        Some(span) => lo + draw % span,
        // Aralık bütün u64 uzayı: çekilen sayının kendisi zaten eşit dağılımlıdır.
        None => draw,
    }
}

/// `p` olasılıkla `true`. Her zaman TAM OLARAK bir `next_u64` çeker; `p = 0` iken bile
/// (hep `false`) ve `p = 1` iken bile (hep `true`), çekiliş sayısı sabit kalsın diye.
///
/// Çekilen sayının üst 53 biti `[0, 1)` aralığında bir `f64`'e çevrilir: `f64`'ün anlamlı kısmı 53
/// bit olduğundan dönüşüm kayıpsızdır ve IEEE 754 sayesinde her platformda aynı sonucu verir.
pub fn chance<R: Rng + ?Sized>(rng: &mut R, p: f64) -> bool {
    let draw = rng.next_u64();
    let unit = (draw >> 11) as f64 * (1.0 / (1_u64 << 53) as f64);
    unit < p
}

#[cfg(test)]
mod tests {
    use super::{ChaCha8Rng, Component, SeedTree, chance, uniform_inclusive};
    use raft_core::NodeId;
    use rand_chacha::rand_core::Rng;

    // Alt-seed yalnızca (ana seed, bileşen) ikilisine bağlıdır: istenme sırası ve sayısı fark
    // etmez.
    #[test]
    fn seeds_depend_only_on_master_and_component() {
        let tree = SeedTree::new(7);
        let network_first = tree.seed_for(Component::Network);
        let node_after = tree.seed_for(Component::Node(NodeId(1)));
        let node_again = tree.seed_for(Component::Node(NodeId(1)));
        let network_again = tree.seed_for(Component::Network);
        assert_eq!(network_first, network_again);
        assert_eq!(node_after, node_again);
        assert_eq!(
            SeedTree::new(7).seed_for(Component::Disk),
            tree.seed_for(Component::Disk)
        );
    }

    // Farklı bileşenler ve farklı ana seed'ler farklı akışlar almalı (ardışık düğüm kimlikleri
    // dahil).
    #[test]
    fn different_components_and_masters_get_different_seeds() {
        let tree = SeedTree::new(7);
        let mut seeds = vec![
            tree.seed_for(Component::Network),
            tree.seed_for(Component::Disk),
            tree.seed_for(Component::Scenario),
            SeedTree::new(8).seed_for(Component::Network),
        ];
        seeds.extend((1..=16).map(|id| tree.seed_for(Component::Node(NodeId(id)))));
        let total = seeds.len();
        seeds.sort_unstable();
        seeds.dedup();
        assert_eq!(
            seeds.len(),
            total,
            "every component must get its own sub-seed"
        );
    }

    // Bir bileşenin RNG'sinden istenildiği kadar çekmek, başka bir bileşenin akışını değiştirmez.
    #[test]
    fn drawing_from_one_component_does_not_shift_another() {
        let tree = SeedTree::new(99);
        let untouched: Vec<u64> = {
            let mut node = tree.rng_for(Component::Node(NodeId(1)));
            (0..8).map(|_| node.next_u64()).collect()
        };
        let mut network = tree.rng_for(Component::Network);
        for _ in 0..1000 {
            let _ = network.next_u64();
        }
        let mut node = tree.rng_for(Component::Node(NodeId(1)));
        let after: Vec<u64> = (0..8).map(|_| node.next_u64()).collect();
        assert_eq!(untouched, after);
    }

    // "Altın" değerler: türetme yöntemi (FNV + SplitMix64 + genişletme) ya da ChaCha8 akışı
    // istemeden değişirse bu test kırılır. Değişiklik bilinçliyse yayımlanmış seed'lerin artık
    // başka koşular ürettiği kabul edilir ve değerler de bilinçli olarak güncellenir.
    #[test]
    fn derivation_is_pinned_by_golden_values() {
        let tree = SeedTree::new(42);
        assert_eq!(tree.seed_for(Component::Network), 0x9d38_3dfe_3e0c_d261);
        assert_eq!(
            tree.seed_for(Component::Node(NodeId(1))),
            0x83b7_7c44_5f91_1caf
        );
        assert_eq!(
            tree.rng_for(Component::Network).next_u64(),
            0x18ab_bf2f_102d_0b34
        );
    }

    // Aralık uçları dahildir, lo == hi sabit döner, tüm u64 aralığı da çalışır.
    #[test]
    fn uniform_inclusive_respects_bounds() {
        let mut rng = SeedTree::new(1).rng_for(Component::Scenario);
        let mut seen_lo = false;
        let mut seen_hi = false;
        for _ in 0..10_000 {
            let value = uniform_inclusive(&mut rng, 3, 6);
            assert!((3..=6).contains(&value));
            seen_lo |= value == 3;
            seen_hi |= value == 6;
        }
        assert!(
            seen_lo && seen_hi,
            "both ends of the range must be reachable"
        );
        assert_eq!(uniform_inclusive(&mut rng, 5, 5), 5);
        let _ = uniform_inclusive(&mut rng, 0, u64::MAX);
        assert_eq!(
            uniform_inclusive(&mut rng, 9, 2),
            9,
            "invalid range returns lo"
        );
    }

    // Her yardımcı, sonuç ne olursa olsun tam olarak bir sayı çeker: akış hiçbir koşulda kaymaz.
    #[test]
    fn helpers_consume_exactly_one_draw() {
        let base: ChaCha8Rng = SeedTree::new(3).rng_for(Component::Network);
        let expected_next = {
            let mut reference = base.clone();
            let _ = reference.next_u64();
            reference.next_u64()
        };
        for p in [0.0, 0.5, 1.0] {
            let mut rng = base.clone();
            let _ = chance(&mut rng, p);
            assert_eq!(
                rng.next_u64(),
                expected_next,
                "chance({p}) must draw exactly once"
            );
        }
        for (lo, hi) in [(0, 0), (1, 10), (0, u64::MAX), (9, 2)] {
            let mut rng = base.clone();
            let _ = uniform_inclusive(&mut rng, lo, hi);
            assert_eq!(
                rng.next_u64(),
                expected_next,
                "uniform({lo},{hi}) must draw exactly once"
            );
        }
    }

    // p = 0 asla, p = 1 her zaman; p = 0.5 kabaca yarı yarıya (seed sabit olduğundan sonuç da
    // sabit).
    #[test]
    fn chance_edges_and_rough_frequency() {
        let mut rng = SeedTree::new(5).rng_for(Component::Network);
        assert!((0..1000).all(|_| !chance(&mut rng, 0.0)));
        assert!((0..1000).all(|_| chance(&mut rng, 1.0)));
        let hits = (0..10_000).filter(|_| chance(&mut rng, 0.5)).count();
        assert!(
            (4_800..=5_200).contains(&hits),
            "got {hits} hits out of 10000"
        );
    }
}
