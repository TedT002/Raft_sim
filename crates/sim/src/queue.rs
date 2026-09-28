//! Olay kuyruğu: olaylar `(time, seq)` sırasıyla, deterministik olarak işlenir.
//!
//! Neden `seq`: `BinaryHeap` kararsız (unstable) bir yapıdır; aynı öncelikteki öğeleri eklenme
//! sırasıyla çıkaracağını garanti etmez. Anahtar yalnızca `time` olsaydı, aynı anda gerçekleşen iki
//! olayın hangisinin önce işleneceği heap'in belirtilmemiş iç düzenine kalırdı. Bu düzen aynı
//! derleyicide tekrarlanır ama FIFO değildir ve standart kütüphanenin uygulaması değişince sessizce
//! değişebilir; o zaman yayımlanmış bir seed artık aynı koşuyu üretmez. Her eklemede artan `seq`
//! sayacı eşitliği açıkça "önce eklenen önce işlenir" kuralıyla bozar.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

/// Kuyruktaki bir olay: ne zaman işleneceği, kaçıncı eklendiği ve kendisi.
///
/// Eşitlik bütün alanlara bakar (olayın kendisi dahil). Kuyruktaki SIRALAMA ise ayrı ve gizli bir
/// sarmalayıcıda (`Entry`) tanımlıdır; bkz. aşağısı.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scheduled<E> {
    /// Olayın işleneceği mantıksal zaman (tick).
    pub time: u64,
    /// Kuyruğa kaçıncı sırada eklendiği.
    pub seq: u64,
    /// Olayın kendisi.
    pub event: E,
}

/// Heap'e özel sarmalayıcı: sıralama YALNIZCA `(time, seq)` üzerindendir, olayın kendisi hiç
/// karşılaştırılmaz. Böylece olay tipinin (içindeki mesajlar dahil) `Ord` uygulaması gerekmez:
/// `(time, seq, event)` demeti mesajlara anlamsız bir sıralama eklemeyi zorunlu kılardı. `seq`
/// benzersiz olduğu için iki farklı giriş asla eşit sayılmaz.
///
/// Bu anahtar karşılaştırması bilerek public `Scheduled`'a konmadı: public bir tipin `PartialEq`'i
/// yalnızca anahtara baksaydı, olayları farklı iki kayıt "eşit" görünürdü. Farklılık yakalamak için
/// yazılmış bir test çatısında bu sinsi bir tuzak olurdu.
#[derive(Debug, Clone)]
struct Entry<E>(Scheduled<E>);

impl<E> Entry<E> {
    /// Sıralama anahtarı: önce zaman, eşitlikte eklenme sırası (FIFO).
    fn key(&self) -> (u64, u64) {
        (self.0.time, self.0.seq)
    }
}

impl<E> PartialEq for Entry<E> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl<E> Eq for Entry<E> {}

impl<E> PartialOrd for Entry<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> Ord for Entry<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

/// En erken olayı önce veren öncelik kuyruğu.
///
/// `BinaryHeap` bir max-heap'tir; `Reverse` ile en küçük `(time, seq)` en üste gelir.
#[derive(Debug, Clone)]
pub struct EventQueue<E> {
    heap: BinaryHeap<Reverse<Entry<E>>>,
    next_seq: u64,
}

impl<E> EventQueue<E> {
    /// Boş bir kuyruk.
    #[must_use]
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }

    /// Olayı `time` anına ekler ve ona verilen `seq` değerini döndürür.
    pub fn push(&mut self, time: u64, event: E) -> u64 {
        let seq = self.next_seq;
        // 2^64 ekleme pratikte imkânsızdır; yine de taşma paniği yerine doygun toplama.
        self.next_seq = self.next_seq.saturating_add(1);
        self.heap
            .push(Reverse(Entry(Scheduled { time, seq, event })));
        seq
    }

    /// En erken (eşitlikte en önce eklenen) olayı çıkarır.
    pub fn pop(&mut self) -> Option<Scheduled<E>> {
        self.heap.pop().map(|Reverse(Entry(scheduled))| scheduled)
    }

    /// Sıradaki olayın zamanı (kuyruk boşsa `None`).
    #[must_use]
    pub fn peek_time(&self) -> Option<u64> {
        self.heap
            .peek()
            .map(|Reverse(Entry(scheduled))| scheduled.time)
    }

    /// Kuyruktaki olay sayısı.
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Kuyruk boş mu?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::EventQueue;

    // Sıralama testi (Faz 1 zorunlu): aynı zamanlı 16 olay, eklenme sırasıyla (FIFO) çıkmalı.
    // `seq` kaldırılırsa heap eşit anahtarlı öğeleri kendi iç düzeniyle çıkarır ve bu test kırılır.
    #[test]
    fn equal_time_events_pop_in_insertion_order() {
        let mut queue = EventQueue::new();
        for payload in 0..16 {
            queue.push(5, payload);
        }
        let popped: Vec<i32> = std::iter::from_fn(|| queue.pop().map(|s| s.event)).collect();
        assert_eq!(popped, (0..16).collect::<Vec<_>>());
    }

    // Zaman birincil anahtardır; eşitlikte eklenme sırası belirler.
    #[test]
    fn time_is_primary_and_seq_breaks_ties() {
        let mut queue = EventQueue::new();
        queue.push(10, 'a');
        queue.push(5, 'b');
        queue.push(7, 'c');
        queue.push(5, 'd');
        assert_eq!(queue.peek_time(), Some(5));
        let popped: Vec<(u64, char)> =
            std::iter::from_fn(|| queue.pop().map(|s| (s.time, s.event))).collect();
        assert_eq!(popped, vec![(5, 'b'), (5, 'd'), (7, 'c'), (10, 'a')]);
        assert!(queue.is_empty());
    }

    // Verilen `seq` değerleri eklenme sırasına göre kesin artandır.
    #[test]
    fn push_returns_increasing_sequence_numbers() {
        let mut queue = EventQueue::new();
        let seqs: Vec<u64> = (0..5).map(|t| queue.push(t, ())).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
        assert_eq!(queue.len(), 5);
    }
}
