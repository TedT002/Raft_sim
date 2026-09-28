//! FNV-1a 64: trace özetleri ve seed türetimi için kendi yazdığımız kararlı hasher.
//!
//! Neden kendimiz yazıyoruz: `std::collections::hash_map::DefaultHasher`'ın algoritması
//! belirtilmemiştir ve Rust sürümleri arasında değişebilir. Trace özeti ise bir koşunun kimliğidir;
//! bugün yayımlanan bir seed'in yıllar sonra da aynı özeti üretmesi gerekir. FNV-1a birkaç
//! satırdır, tanımı sabittir ve hiçbir bağımlılık getirmez.

/// FNV-1a 64 başlangıç değeri (offset basis).
const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64 çarpanı (FNV prime).
const PRIME: u64 = 0x0000_0100_0000_01b3;

/// Bir bayt akışını adım adım özetleyen 64-bit FNV-1a.
///
/// Bilerek `std::hash::Hasher` UYGULAMAZ: `#[derive(Hash)]` bir değeri `Hasher`'a hangi baytlarla
/// beslediğini garanti etmez (ör. `usize` genişliği platforma bağlıdır). Trace'e giren her şey
/// burada açıkça, kanonik baytlarla (`write_u8`, little-endian `write_u64`) yazılır.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fnv1a64 {
    state: u64,
}

impl Fnv1a64 {
    /// Boş bir özetle başlar.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: OFFSET_BASIS,
        }
    }

    /// Baytları özete katar. FNV-1a sırası: önce XOR, sonra çarp.
    pub fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= u64::from(byte);
            self.state = self.state.wrapping_mul(PRIME);
        }
    }

    /// Tek bir baytı özete katar.
    pub fn write_u8(&mut self, value: u8) {
        self.write(&[value]);
    }

    /// Bir `u64`'ü little-endian baytlarıyla katar: bayt sırası platformdan bağımsız olsun diye.
    pub fn write_u64(&mut self, value: u64) {
        self.write(&value.to_le_bytes());
    }

    /// Şu ana kadar yazılanların özeti. Hasher'ı tüketmez; yazmaya devam edilebilir.
    #[must_use]
    pub const fn finish(&self) -> u64 {
        self.state
    }
}

impl Default for Fnv1a64 {
    fn default() -> Self {
        Self::new()
    }
}

/// Bir bayt dizisinin FNV-1a 64 özeti (tek seferlik kolaylık fonksiyonu).
#[must_use]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hasher = Fnv1a64::new();
    hasher.write(bytes);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::{Fnv1a64, fnv1a64};

    // FNV-1a 64'ün yayımlanmış test vektörleri: uygulamamız tanıma birebir uymalı. Buradaki bir
    // hata, bütün trace özetlerini sessizce "başka bir hash"e çevirirdi.
    #[test]
    fn matches_published_test_vectors() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    // Parça parça yazmak, hepsini bir kerede yazmakla aynı özeti vermeli: trace olayları tek tek
    // eklendiği için sürekli (running) özet buna dayanır.
    #[test]
    fn incremental_writes_equal_one_shot() {
        let mut hasher = Fnv1a64::new();
        hasher.write(b"foo");
        hasher.write(b"bar");
        assert_eq!(hasher.finish(), fnv1a64(b"foobar"));

        let mut numbers = Fnv1a64::new();
        numbers.write_u8(7);
        numbers.write_u64(0x0102_0304_0506_0708);
        assert_eq!(
            numbers.finish(),
            fnv1a64(&[7, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01])
        );
    }
}
