//! Linearizability (Herlihy & Wing, 1990): eşzamanlı bir geçmiş, her işlem çağrısı ile dönüşü
//! arasındaki BİR anda, tek başına ve sırayla gerçekleşmiş gibi açıklanabiliyorsa linearizable'dır.
//! Açıklama gerçek zamana da uymalıdır: dönüşü bir başkasının çağrısından önce olan işlem, o
//! işlemden önce sıralanır.
//!
//! Kontrolcü Wing & Gong'un aramasını (1993) Lowe'un "just-in-time linearization"
//! iyileştirmesiyle uygular (Porcupine ve Knossos da bu ailedendir):
//!
//! - Çağrı ve dönüş olayları zaman sırasıyla çift bağlı bir listede durur.
//! - Arama, listenin başından ilerler. Bir çağrıya gelince o işlemi şimdi doğrusallaştırmayı
//!   (modelde uygulamayı) dener: sonucu modelinkiyle uyuşursa işlemin çağrı ve dönüş olaylarını
//!   listeden çıkarır ve baştan devam eder. Uyuşmazsa sıradaki olaya geçer.
//! - Henüz doğrusallaştırılmamış bir işlemin DÖNÜŞÜNE gelmek, o işlemin artık daha sonraya
//!   konamayacağı demektir: son kararı geri alır (geri izleme) ve bir sonraki adayı dener.
//! - Bellekleme (memoization): "hangi işlemler doğrusallaştırıldı" kümesi ve modelin değeri aynı
//!   olan bir duruma ikinci kez gelmenin anlamı yoktur; sonucu aynıdır. Bu küme, aramayı üstel
//!   patlamadan büyük ölçüde korur.
//!
//! Belirsiz (indeterminate) işlem: çağrısı var, dönüşü yok (ör. istemci zaman aşımında vazgeçti).
//! Etkisi çağrısından sonraki herhangi bir anda olmuş da olabilir, hiç olmamış da. Dönüşü sonsuzda
//! sayılır: hiçbir zaman "artık çok geç" olmaz, doğrusallaştırılması zorunlu değildir. Arama,
//! dönüşü olan bütün işlemler doğrusallaştırılınca başarıyla biter.
//!
//! Gözlenmemiş belirsiz yazmalar aramaya hiç girmez (bkz. `worth_searching`): kararı değiştirmez,
//! ama hata enjeksiyonlu koşularda yüzlercesi biriken bu işlemler aramayı üstel olarak büyütürdü.
//!
//! P-compositionality (Herlihy & Wing'in "locality" özelliği): KV modeli birbirinden bağımsız
//! yazmaçların (anahtarların) çarpımıdır ve her işlem tek bir anahtara dokunur. Bir geçmiş, ancak
//! ve ancak her anahtarın alt geçmişi linearizable ise linearizable'dır. Kontrolcü geçmişi
//! anahtarlara böler: üstel arama, bütün geçmiş yerine her anahtarın küçük alt geçmişinde yapılır.

use std::collections::{BTreeMap, BTreeSet};

/// Bir KV işleminin girdisi. Her işlem tek bir anahtara dokunur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvInput {
    /// `key`'in değerini `value` yap.
    Put {
        /// Anahtar.
        key: Vec<u8>,
        /// Yeni değer.
        value: Vec<u8>,
    },
    /// `key`'in değerini oku.
    Get {
        /// Anahtar.
        key: Vec<u8>,
    },
    /// `key`'i sil.
    Delete {
        /// Anahtar.
        key: Vec<u8>,
    },
    /// `key`'in değerinin sonuna `value` ekle (değer yoksa boş değer sayılır). İdempotent
    /// değildir: iki kez uygulanan bir istek sonraki okumada görünür.
    Append {
        /// Anahtar.
        key: Vec<u8>,
        /// Eklenen baytlar.
        value: Vec<u8>,
    },
}

impl KvInput {
    /// İşlemin dokunduğu anahtar.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            KvInput::Put { key, .. }
            | KvInput::Get { key }
            | KvInput::Delete { key }
            | KvInput::Append { key, .. } => key,
        }
    }
}

/// Bir KV işleminin istemciye dönen sonucu.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvOutput {
    /// Yazma işlemi (`Put`, `Delete`, `Append`) uygulandı.
    Ok,
    /// Okunan değer (`Get`); anahtar yoksa `None`.
    Value(Option<Vec<u8>>),
}

/// Geçmişteki bir işlem.
///
/// `call` ve `ret`, tek ve kesin artan bir sayaçtan gelen damgalardır (iki olay aynı damgayı
/// taşımaz). Gerçek zaman sırası yalnızca bunlarla tanımlanır: `a`, `b`'den ÖNCEDİR ancak ve ancak
/// `a.ret < b.call`. Aksi hâlde iki işlem eşzamanlıdır ve herhangi bir sırada açıklanabilir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvOperation {
    /// İşlemi yapan istemci (yalnızca raporlama için; denetim istemcilere bakmaz).
    pub client: u64,
    /// Çağrı damgası.
    pub call: u64,
    /// Dönüş damgası; belirsiz bir işlem için `None`.
    pub ret: Option<u64>,
    /// İşlem.
    pub input: KvInput,
    /// Dönen sonuç; belirsiz bir işlem için `None`.
    pub output: Option<KvOutput>,
}

/// Biçimsiz bir geçmiş kaydının nedeni.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MalformedReason {
    /// Dönüş damgası çağrı damgasından büyük değil.
    #[error("the return is not after the call")]
    ReturnNotAfterCall,
    /// Dönüşü olan işlemin sonucu yok.
    #[error("a returned operation has no output")]
    MissingOutput,
    /// Dönüşü olmayan (belirsiz) işlemin bir sonucu var.
    #[error("an indeterminate operation has an output")]
    OutputWithoutReturn,
    /// Sonucun türü işleme uymuyor (`Get` değer, diğerleri `Ok` döner).
    #[error("the output does not fit the operation")]
    OutputMismatch,
}

/// Linearizability denetiminin hatası.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LinearizabilityError {
    /// Geçmişte biçimsiz bir kayıt var; denetim yapılmadı.
    #[error("malformed history: operation {index}: {reason}")]
    Malformed {
        /// Kaydın geçmişteki sırası.
        index: usize,
        /// Neden.
        reason: MalformedReason,
    },
    /// Bir anahtarın alt geçmişi hiçbir sırayla açıklanamıyor.
    #[error(
        "linearizability violated: key {:?} ({} operations: {operations:?})",
        String::from_utf8_lossy(key),
        operations.len()
    )]
    NotLinearizable {
        /// Anahtar.
        key: Vec<u8>,
        /// O anahtarın aramaya giren işlemlerinin geçmişteki sıraları (ayıklanan belirsiz
        /// işlemler yoktur; bkz. `worth_searching`).
        operations: Vec<usize>,
    },
}

/// Bir anahtarın modeldeki değeri: yok ya da baytlar.
type Value = Option<Vec<u8>>;

/// İşlemi modelde uygular. Sonuç (`output`), modelin vereceği sonuçla uyuşmuyorsa `None`; belirsiz
/// işlemin sonucu (`None`) her şeyle uyuşur. Uyuşursa işlemden sonraki değer.
fn apply(value: &Value, input: &KvInput, output: Option<&KvOutput>) -> Option<Value> {
    let wrote_ok = || output.is_none_or(|output| *output == KvOutput::Ok);
    match input {
        KvInput::Put { value: new, .. } => wrote_ok().then(|| Some(new.clone())),
        KvInput::Delete { .. } => wrote_ok().then_some(None),
        KvInput::Append { value: suffix, .. } => wrote_ok().then(|| {
            let mut appended = value.clone().unwrap_or_default();
            appended.extend_from_slice(suffix);
            Some(appended)
        }),
        KvInput::Get { .. } => match output {
            None => Some(value.clone()),
            Some(KvOutput::Value(seen)) if seen == value => Some(value.clone()),
            Some(_) => None,
        },
    }
}

/// Geçmişin linearizable olup olmadığını denetler (bkz. modül belgesi).
///
/// # Errors
///
/// Biçimsiz bir kayıt varsa [`LinearizabilityError::Malformed`]; bir anahtarın alt geçmişi
/// açıklanamıyorsa [`LinearizabilityError::NotLinearizable`] (anahtarlar sırayla denetlenir, ilk
/// açıklanamayan bildirilir).
pub fn check_kv(history: &[KvOperation]) -> Result<(), LinearizabilityError> {
    for (index, operation) in history.iter().enumerate() {
        validate(operation).map_err(|reason| LinearizabilityError::Malformed { index, reason })?;
    }
    // Tamamlanmış okumaların gördüğü değerler, anahtar başına (bkz. `worth_searching`).
    let mut reads: BTreeMap<&[u8], Vec<&[u8]>> = BTreeMap::new();
    for operation in history {
        if let (KvInput::Get { key }, Some(KvOutput::Value(Some(value)))) =
            (&operation.input, &operation.output)
        {
            reads.entry(key.as_slice()).or_default().push(value);
        }
    }
    // BTreeMap: anahtarlar her koşuda aynı sırayla denetlenir; ilk bildirilen ihlal sabittir.
    let mut by_key: BTreeMap<&[u8], Vec<usize>> = BTreeMap::new();
    for (index, operation) in history.iter().enumerate() {
        if operation.ret.is_none() && !worth_searching(operation, &reads) {
            continue;
        }
        by_key.entry(operation.input.key()).or_default().push(index);
    }
    for (key, operations) in by_key {
        if !linearizable(history, &operations) {
            return Err(LinearizabilityError::NotLinearizable {
                key: key.to_vec(),
                operations,
            });
        }
    }
    Ok(())
}

/// Belirsiz bir işlem aramaya girmeli mi?
///
/// - Belirsiz bir okuma hiçbir şeyi değiştirmez ve hiçbir şeyi kanıtlamaz: girmez.
/// - Belirsiz bir silme girer: etkisi (değerin yokluğu) baytlarla tanınamaz.
/// - Belirsiz bir yazma (`Put`, `Append`) ancak yazdığı baytlar TAMAMLANMIŞ bir okumanın sonucunda
///   (bitişik bir alt dizi olarak) görünüyorsa girer. Boş bir değer her okumada "görünür": hiç
///   ayıklanmaz.
///
/// Son kural kararı değiştirmez. Gerekçe bu dört işleme özgüdür: modele yeni bir işlem eklenirse
/// (ör. karşılaştır-ve-yaz ya da uzunluk okuma) yeniden kanıtlanmalıdır. Bir yazmanın etkisini
/// taşıyan her değer onun baytlarını içerir: `Put` değeri o baytlarla başlatır, `Append` onları
/// sona ekler, sonraki eklemeler ise yalnızca sona ekler. Bu baytları hiçbir okuma görmediyse,
/// yazmayı doğrusallaştıran her açıklamada yazma ile onu ezen (`Put`/`Delete`) işlem arasında
/// hiçbir okuma yoktur. Öyleyse yazmayı açıklamadan çıkarmak hiçbir okumanın sonucunu değiştirmez;
/// yazma işlemleri zaten hep `Ok` döner. Belirsiz bir işlemin hiç etki etmemesi de izinlidir:
/// geçmiş, yazmayla linearizable ise onsuz da öyledir (tersi her zaman doğrudur).
fn worth_searching(operation: &KvOperation, reads: &BTreeMap<&[u8], Vec<&[u8]>>) -> bool {
    match &operation.input {
        KvInput::Get { .. } => false,
        KvInput::Delete { .. } => true,
        KvInput::Put { key, value } | KvInput::Append { key, value } => {
            reads.get(key.as_slice()).is_some_and(|seen| {
                seen.iter()
                    .any(|read| value.is_empty() || read.windows(value.len()).any(|w| w == value))
            })
        }
    }
}

/// Bir kaydın kendi içinde tutarlı olup olmadığı.
fn validate(operation: &KvOperation) -> Result<(), MalformedReason> {
    match (operation.ret, &operation.output) {
        (Some(ret), _) if ret <= operation.call => Err(MalformedReason::ReturnNotAfterCall),
        (Some(_), None) => Err(MalformedReason::MissingOutput),
        (None, Some(_)) => Err(MalformedReason::OutputWithoutReturn),
        (_, None) => Ok(()),
        (Some(_), Some(output)) => {
            let fits = matches!(
                (&operation.input, output),
                (KvInput::Get { .. }, KvOutput::Value(_))
                    | (
                        KvInput::Put { .. } | KvInput::Delete { .. } | KvInput::Append { .. },
                        KvOutput::Ok
                    )
            );
            if fits {
                Ok(())
            } else {
                Err(MalformedReason::OutputMismatch)
            }
        }
    }
}

/// Listedeki bir olay: bir işlemin çağrısı ya da dönüşü.
#[derive(Debug, Clone, Copy)]
struct Event {
    /// İşlemin alt geçmişteki sırası.
    operation: usize,
    is_call: bool,
    // Çift bağlı listenin komşuları (olay dizisindeki konumlar). 0 başlangıç bekçisidir; `NONE`
    // listenin sonu.
    prev: usize,
    next: usize,
}

const NONE: usize = usize::MAX;

/// Doğrusallaştırılmış işlemler kümesi: bit dizisi. Bellekleme anahtarının parçası olduğu için
/// sıralı (`Ord`) ve kopyalanabilir bir değer tipidir.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Linearized(Vec<u64>);

impl Linearized {
    fn new(len: usize) -> Self {
        Self(vec![0; len.div_ceil(64)])
    }

    fn set(&mut self, bit: usize, on: bool) {
        if let Some(word) = self.0.get_mut(bit / 64) {
            let mask = 1_u64 << (bit % 64);
            if on {
                *word |= mask;
            } else {
                *word &= !mask;
            }
        }
    }
}

/// Bir anahtarın alt geçmişi (`operations`, geçmişteki sıralar) linearizable mı?
fn linearizable(history: &[KvOperation], operations: &[usize]) -> bool {
    // Olaylar: (damga, sıralama anahtarı, işlem, çağrı mı). Aynı damgayı iki olay taşımamalı, ama
    // taşırsa çağrı dönüşten önce gelir: eşit damgalı bir dönüş ve çağrı eşzamanlı sayılır
    // (öncelik yalnızca `ret < call` ile kurulur).
    let mut stamps: Vec<(u64, u8, usize, bool)> = Vec::with_capacity(2 * operations.len());
    for (position, &index) in operations.iter().enumerate() {
        let operation = &history[index];
        stamps.push((operation.call, 0, position, true));
        if let Some(ret) = operation.ret {
            stamps.push((ret, 1, position, false));
        }
    }
    stamps.sort_unstable();

    // events[0] başlangıç bekçisidir; gerçek olaylar 1'den başlar.
    let mut events = Vec::with_capacity(stamps.len() + 1);
    events.push(Event {
        operation: NONE,
        is_call: false,
        prev: NONE,
        next: if stamps.is_empty() { NONE } else { 1 },
    });
    let mut return_of = vec![NONE; operations.len()];
    for (offset, &(_, _, operation, is_call)) in stamps.iter().enumerate() {
        let at = offset + 1;
        if !is_call {
            return_of[operation] = at;
        }
        events.push(Event {
            operation,
            is_call,
            prev: at - 1,
            next: if at == stamps.len() { NONE } else { at + 1 },
        });
    }

    let mut remaining_returns = return_of.iter().filter(|&&at| at != NONE).count();
    let mut value: Value = None;
    let mut linearized = Linearized::new(operations.len());
    let mut seen: BTreeSet<(Linearized, Value)> = BTreeSet::new();
    // Geri izleme yığını: doğrusallaştırılan çağrı olayı ve ondan önceki değer.
    let mut decisions: Vec<(usize, Value)> = Vec::new();
    let mut cursor = events[0].next;
    loop {
        if remaining_returns == 0 {
            return true;
        }
        // Dönüşü olan bir işlem listede kaldıkça imleç listenin sonuna varmadan bir dönüşe
        // rastlar: çağrılar yalnızca ilerletir, dönüş geri izletir.
        let Some(&event) = events.get(cursor) else {
            return false;
        };
        if event.is_call {
            let operation = &history[operations[event.operation]];
            if let Some(next_value) = apply(&value, &operation.input, operation.output.as_ref()) {
                let mut next_linearized = linearized.clone();
                next_linearized.set(event.operation, true);
                if seen.insert((next_linearized.clone(), next_value.clone())) {
                    decisions.push((cursor, std::mem::replace(&mut value, next_value)));
                    linearized = next_linearized;
                    lift(&mut events, cursor, return_of[event.operation]);
                    if return_of[event.operation] != NONE {
                        remaining_returns -= 1;
                    }
                    cursor = events[0].next;
                    continue;
                }
            }
            cursor = event.next;
        } else {
            // Doğrusallaştırılmamış bir işlemin dönüşü: son karar geri alınır.
            let Some((call, previous)) = decisions.pop() else {
                return false;
            };
            let operation = events[call].operation;
            value = previous;
            linearized.set(operation, false);
            unlift(&mut events, call, return_of[operation]);
            if return_of[operation] != NONE {
                remaining_returns += 1;
            }
            cursor = events[call].next;
        }
    }
}

/// Bir işlemin çağrı (ve varsa dönüş) olayını listeden çıkarır. Olaylar kendi `prev`/`next`
/// değerlerini korur: `unlift` onları aynı yere geri takar (dancing links).
fn lift(events: &mut [Event], call: usize, ret: usize) {
    unlink(events, call);
    if ret != NONE {
        unlink(events, ret);
    }
}

/// `lift`'in tersi; çıkarılma sırasının tersiyle geri takar.
fn unlift(events: &mut [Event], call: usize, ret: usize) {
    if ret != NONE {
        relink(events, ret);
    }
    relink(events, call);
}

fn unlink(events: &mut [Event], at: usize) {
    let Event { prev, next, .. } = events[at];
    events[prev].next = next;
    if next != NONE {
        events[next].prev = prev;
    }
}

fn relink(events: &mut [Event], at: usize) {
    let Event { prev, next, .. } = events[at];
    events[prev].next = at;
    if next != NONE {
        events[next].prev = at;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KvInput, KvOperation, KvOutput, LinearizabilityError, MalformedReason, Value, apply,
        check_kv,
    };
    use proptest::prelude::*;
    use proptest::test_runner::{Config as ProptestConfig, RngSeed};
    use std::collections::BTreeMap;

    fn put(key: &str, value: &str) -> KvInput {
        KvInput::Put {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn get(key: &str) -> KvInput {
        KvInput::Get {
            key: key.as_bytes().to_vec(),
        }
    }

    fn delete(key: &str) -> KvInput {
        KvInput::Delete {
            key: key.as_bytes().to_vec(),
        }
    }

    fn append(key: &str, value: &str) -> KvInput {
        KvInput::Append {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn seen(value: Option<&str>) -> KvOutput {
        KvOutput::Value(value.map(|value| value.as_bytes().to_vec()))
    }

    /// Tamamlanmış bir işlem: `[call, ret]` aralığında.
    fn done(client: u64, call: u64, ret: u64, input: KvInput, output: KvOutput) -> KvOperation {
        KvOperation {
            client,
            call,
            ret: Some(ret),
            input,
            output: Some(output),
        }
    }

    /// Belirsiz bir işlem: çağrıldı, dönmedi.
    fn pending(client: u64, call: u64, input: KvInput) -> KvOperation {
        KvOperation {
            client,
            call,
            ret: None,
            input,
            output: None,
        }
    }

    fn rejected(history: &[KvOperation]) -> bool {
        matches!(
            check_kv(history),
            Err(LinearizabilityError::NotLinearizable { .. })
        )
    }

    // Sıralı geçmişler: yazılan okunur, silinen kaybolur, eklenen sona eklenir.
    #[test]
    fn sequential_histories_follow_the_model() {
        let history = [
            done(1, 1, 2, put("k", "a"), KvOutput::Ok),
            done(1, 3, 4, get("k"), seen(Some("a"))),
            done(1, 5, 6, append("k", "b"), KvOutput::Ok),
            done(2, 7, 8, get("k"), seen(Some("ab"))),
            done(2, 9, 10, delete("k"), KvOutput::Ok),
            done(1, 11, 12, get("k"), seen(None)),
        ];
        assert_eq!(check_kv(&history), Ok(()));
        assert_eq!(check_kv(&[]), Ok(()));
    }

    // Tamamlanmış bir yazmadan SONRA başlayan okuma eski değeri göremez (bayat okuma).
    #[test]
    fn a_stale_read_after_a_completed_write_is_rejected() {
        let history = [
            done(1, 1, 2, put("k", "a"), KvOutput::Ok),
            done(2, 3, 4, get("k"), seen(None)),
        ];
        assert!(rejected(&history));
    }

    // Yazmayla eşzamanlı bir okuma eski ya da yeni değeri görebilir; ama bir okuma yeniyi gördükten
    // sonra başlayan okuma eskiye dönemez.
    #[test]
    fn a_concurrent_read_may_see_either_value_but_never_goes_back() {
        for observed in [None, Some("a")] {
            let history = [
                done(1, 1, 4, put("k", "a"), KvOutput::Ok),
                done(2, 2, 3, get("k"), seen(observed)),
            ];
            assert_eq!(check_kv(&history), Ok(()), "{observed:?}");
        }
        let flip_back = [
            done(1, 1, 10, put("k", "a"), KvOutput::Ok),
            done(2, 2, 3, get("k"), seen(Some("a"))),
            done(3, 4, 5, get("k"), seen(None)),
        ];
        assert!(rejected(&flip_back));
    }

    // Çift uygulama: tek bir ekleme iki kez görünemez. Kaybolan güncelleme: tamamlanmış iki
    // eklemenin biri okumada eksik olamaz.
    #[test]
    fn double_applies_and_lost_updates_are_rejected() {
        let doubled = [
            done(1, 1, 2, append("k", "x"), KvOutput::Ok),
            done(2, 3, 4, get("k"), seen(Some("xx"))),
        ];
        assert!(rejected(&doubled));
        let lost = [
            done(1, 1, 3, append("k", "x"), KvOutput::Ok),
            done(2, 2, 4, append("k", "y"), KvOutput::Ok),
            done(3, 5, 6, get("k"), seen(Some("y"))),
        ];
        assert!(rejected(&lost));
        let both = [
            done(1, 1, 3, append("k", "x"), KvOutput::Ok),
            done(2, 2, 4, append("k", "y"), KvOutput::Ok),
            done(3, 5, 6, get("k"), seen(Some("yx"))),
        ];
        assert_eq!(
            check_kv(&both),
            Ok(()),
            "concurrent appends in either order"
        );
    }

    // Belirsiz işlem: etkisi olmuş da olmamış da olabilir. Ama bir okuma onu gördüyse sonraki
    // okumalar da görmelidir (araya başka yazma girmedikçe); ve hiç çağrılmadan önce görünemez.
    #[test]
    fn an_indeterminate_operation_may_or_may_not_take_effect() {
        for observed in [None, Some("a")] {
            let history = [
                pending(1, 1, put("k", "a")),
                done(2, 5, 6, get("k"), seen(observed)),
            ];
            assert_eq!(check_kv(&history), Ok(()), "{observed:?}");
        }
        let pinned = [
            pending(1, 1, put("k", "a")),
            done(2, 2, 3, get("k"), seen(Some("a"))),
            done(2, 4, 5, get("k"), seen(None)),
        ];
        assert!(rejected(&pinned));
        let before_call = [
            done(2, 1, 2, get("k"), seen(Some("a"))),
            pending(1, 3, put("k", "a")),
        ];
        assert!(rejected(&before_call));
        let late_effect = [
            pending(1, 1, append("k", "a")),
            done(2, 2, 3, get("k"), seen(None)),
            done(2, 10, 11, get("k"), seen(Some("a"))),
        ];
        assert_eq!(check_kv(&late_effect), Ok(()), "the effect may come late");
    }

    // Gözlenmemiş belirsiz yazmalar aramaya girmez (kararı değiştirmeden): hiç görülmemiş bir
    // değer yazan yüzlerce vazgeçilmiş işlem bile geçmişi linearizable bırakır ve denetim hızlı
    // biter. Görülen bir belirsiz yazma ise hesaba katılır: okuma onu gördükten sonra başlayan bir
    // okuma onu kaybedemez.
    #[test]
    fn unobserved_indeterminate_writes_are_pruned_without_changing_the_verdict() {
        let mut history = vec![done(9, 1, 2, put("k", "base"), KvOutput::Ok)];
        for client in 0..200 {
            history.push(pending(
                client,
                3 + client,
                put("k", &format!("lost{client}")),
            ));
        }
        history.push(done(9, 300, 301, get("k"), seen(Some("base"))));
        assert_eq!(check_kv(&history), Ok(()));

        let seen_then_lost = [
            pending(1, 1, put("k", "a")),
            done(2, 2, 3, get("k"), seen(Some("a"))),
            done(2, 4, 5, get("k"), seen(None)),
        ];
        assert!(rejected(&seen_then_lost));
    }

    // Boş değer yazan belirsiz bir işlem ayıklanmaz: boş değer, yazmanın baytları olarak her
    // okumada "görünür". Burada okumanın gördüğü boş değeri yalnızca o belirsiz yazma açıklar.
    #[test]
    fn an_indeterminate_write_of_an_empty_value_is_kept() {
        let history = [
            done(1, 1, 2, put("k", "a"), KvOutput::Ok),
            pending(2, 3, put("k", "")),
            done(1, 4, 5, get("k"), seen(Some(""))),
        ];
        assert_eq!(check_kv(&history), Ok(()));
    }

    // Fark testinin kendisi boş geçmemeli: rastgele geçmiş üreticisi hem linearizable olan hem de
    // olmayan geçmişler üretmeli. Belirlenimci bir örneklemde iki karar da yeterince görülür ve
    // kontrolcü her birinde kaba kuvvet tanımıyla aynı karara varır.
    #[test]
    fn the_differential_sample_sees_both_verdicts() {
        use proptest::strategy::ValueTree;
        use proptest::test_runner::TestRunner;
        let mut runner = TestRunner::deterministic();
        let (mut accepted, mut rejected) = (0, 0);
        for _ in 0..256 {
            let history = random_history()
                .new_tree(&mut runner)
                .expect("a sample")
                .current();
            let verdict = brute_force(&history);
            assert_eq!(check_kv(&history).is_ok(), verdict, "{history:?}");
            if verdict {
                accepted += 1;
            } else {
                rejected += 1;
            }
        }
        assert!(accepted >= 30, "accepted {accepted}, rejected {rejected}");
        assert!(rejected >= 30, "accepted {accepted}, rejected {rejected}");
    }

    // Anahtarlar bağımsızdır: bir anahtardaki ihlal, o anahtarın işlemleriyle bildirilir; diğer
    // anahtarın geçerli işlemleri karışmaz.
    #[test]
    fn keys_are_checked_independently() {
        let history = [
            done(1, 1, 2, put("a", "1"), KvOutput::Ok),
            done(1, 3, 4, put("b", "2"), KvOutput::Ok),
            done(2, 5, 6, get("a"), seen(Some("1"))),
            done(2, 7, 8, get("b"), seen(Some("9"))),
        ];
        assert_eq!(
            check_kv(&history),
            Err(LinearizabilityError::NotLinearizable {
                key: b"b".to_vec(),
                operations: vec![1, 3],
            })
        );
    }

    // Biçimsiz kayıtlar denetlenmeden reddedilir.
    #[test]
    fn malformed_histories_are_rejected() {
        let cases = [
            (
                done(1, 5, 5, put("k", "a"), KvOutput::Ok),
                MalformedReason::ReturnNotAfterCall,
            ),
            (
                KvOperation {
                    output: None,
                    ..done(1, 1, 2, put("k", "a"), KvOutput::Ok)
                },
                MalformedReason::MissingOutput,
            ),
            (
                KvOperation {
                    output: Some(KvOutput::Ok),
                    ..pending(1, 1, put("k", "a"))
                },
                MalformedReason::OutputWithoutReturn,
            ),
            (
                done(1, 1, 2, get("k"), KvOutput::Ok),
                MalformedReason::OutputMismatch,
            ),
            (
                done(1, 1, 2, put("k", "a"), seen(None)),
                MalformedReason::OutputMismatch,
            ),
        ];
        for (operation, reason) in cases {
            assert_eq!(
                check_kv(&[operation]),
                Err(LinearizabilityError::Malformed { index: 0, reason })
            );
        }
    }

    /// Kaba kuvvet tanımı: dönüşü olan bütün işlemler ve belirsiz işlemlerin herhangi bir alt
    /// kümesi, gerçek zamana uyan bir sırayla modele uygulandığında bütün sonuçlar uyuşuyorsa
    /// geçmiş linearizable'dır. Arama yok, bellekleme yok, anahtar bölme yok: denetçinin her
    /// iyileştirmesinin ölçütüdür. Yalnızca küçük geçmişler içindir (üstel).
    fn brute_force(history: &[KvOperation]) -> bool {
        let indeterminate: Vec<usize> = (0..history.len())
            .filter(|&index| history[index].ret.is_none())
            .collect();
        for mask in 0..(1_u32 << indeterminate.len()) {
            let mut chosen: Vec<usize> = (0..history.len())
                .filter(|&index| {
                    history[index].ret.is_some()
                        || indeterminate
                            .iter()
                            .position(|&candidate| candidate == index)
                            .is_some_and(|bit| mask & (1 << bit) != 0)
                })
                .collect();
            if permutations_fit(history, &mut chosen, 0) {
                return true;
            }
        }
        false
    }

    /// `order[fixed..]`'in permütasyonlarından biri gerçek zamana uyuyor ve modelle uyuşuyor mu?
    fn permutations_fit(history: &[KvOperation], order: &mut Vec<usize>, fixed: usize) -> bool {
        if fixed == order.len() {
            return respects_real_time(history, order) && model_fits(history, order);
        }
        for swap in fixed..order.len() {
            order.swap(fixed, swap);
            if permutations_fit(history, order, fixed + 1) {
                return true;
            }
            order.swap(fixed, swap);
        }
        false
    }

    /// Sırada sonra gelen hiçbir işlem, önce gelenin çağrısından önce dönmüş olamaz.
    fn respects_real_time(history: &[KvOperation], order: &[usize]) -> bool {
        order.iter().enumerate().all(|(position, &first)| {
            order[position + 1..].iter().all(|&second| {
                history[second]
                    .ret
                    .is_none_or(|ret| ret >= history[first].call)
            })
        })
    }

    /// Sıradaki işlemler, ölçütün KENDİ modeliyle uygulandığında bütün sonuçlar uyuşuyor mu?
    ///
    /// Ölçüt kontrolcünün `apply`'ını kullanmaz: kullansaydı modeldeki bir hata iki tarafa aynen
    /// sızardı ve fark testi onu göremezdi. Burada var olmayan anahtar, haritada olmayan
    /// anahtardır.
    fn model_fits(history: &[KvOperation], order: &[usize]) -> bool {
        let mut values: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        order.iter().all(|&index| {
            let operation = &history[index];
            let key = operation.input.key().to_vec();
            let expected = match &operation.input {
                KvInput::Put { value, .. } => {
                    values.insert(key, value.clone());
                    KvOutput::Ok
                }
                KvInput::Append { value, .. } => {
                    values.entry(key).or_default().extend_from_slice(value);
                    KvOutput::Ok
                }
                KvInput::Delete { .. } => {
                    values.remove(&key);
                    KvOutput::Ok
                }
                KvInput::Get { .. } => KvOutput::Value(values.get(&key).cloned()),
            };
            operation
                .output
                .as_ref()
                .is_none_or(|output| *output == expected)
        })
    }

    // Kaba kuvvet ölçütünün kendisi de elle yazılmış örneklerde doğru karar verir.
    #[test]
    fn the_brute_force_definition_agrees_with_the_hand_written_cases() {
        let stale = [
            done(1, 1, 2, put("k", "a"), KvOutput::Ok),
            done(2, 3, 4, get("k"), seen(None)),
        ];
        assert!(!brute_force(&stale));
        let concurrent = [
            done(1, 1, 4, put("k", "a"), KvOutput::Ok),
            done(2, 2, 3, get("k"), seen(None)),
        ];
        assert!(brute_force(&concurrent));
    }

    fn config() -> ProptestConfig {
        ProptestConfig {
            cases: 256,
            failure_persistence: None,
            rng_seed: RngSeed::Fixed(0x11_7e_a2),
            ..ProptestConfig::default()
        }
    }

    /// Yapı gereği linearizable bir geçmiş. İşlemler doğrusallaştırma sırasıyla üretilir: k.
    /// işlemin noktası `10·k`'dir; çağrısı noktadan biraz önce, dönüşü biraz sonradır. Sonuçlar,
    /// işlemler bu sırayla modele uygulanarak hesaplanır; bu sıra gerçek zamana zaten uyar. Bazı
    /// işlemler belirsizdir: etkisi ya noktasında olur ya hiç olmaz (ikisi de geçerlidir).
    /// Yazılan değerler işleme özgüdür (ayırt edilebilsinler).
    fn linearizable_history() -> impl Strategy<Value = Vec<KvOperation>> {
        let operation = (
            0_u8..4,   // tür
            0_u8..2,   // anahtar
            0_u64..25, // çağrının noktadan önceki payı
            0_u64..25, // dönüşün noktadan sonraki payı
            0_u8..8,   // 0 ise belirsiz
            any::<bool>(),
        );
        prop::collection::vec(operation, 1..14).prop_map(|specs| {
            let mut values: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
            let mut history = Vec::new();
            for (k, (kind, key, before, after, settle, takes_effect)) in
                specs.into_iter().enumerate()
            {
                let point = 100 + 10 * u64::try_from(k).unwrap_or(u64::MAX);
                let key = vec![b'a' + key];
                let token = vec![0xa0, u8::try_from(k).unwrap_or(u8::MAX)];
                let input = match kind {
                    0 => KvInput::Put { key, value: token },
                    1 => KvInput::Append { key, value: token },
                    2 => KvInput::Delete { key },
                    _ => KvInput::Get { key },
                };
                let indeterminate = settle == 0;
                let current = values.get(input.key()).cloned().unwrap_or(None);
                let output = match &input {
                    KvInput::Get { .. } => KvOutput::Value(current.clone()),
                    _ => KvOutput::Ok,
                };
                if !indeterminate || takes_effect {
                    let next = apply(&current, &input, None).unwrap_or(None);
                    values.insert(input.key().to_vec(), next);
                }
                history.push(KvOperation {
                    client: u64::try_from(k).unwrap_or(0) % 3,
                    call: point - before,
                    ret: (!indeterminate).then_some(point + after),
                    input,
                    output: (!indeterminate).then_some(output),
                });
            }
            renumber(&mut history);
            history
        })
    }

    /// Damgaları sıraları korunarak benzersiz yapar (çağrılar eşit damgalı dönüşlerden önce):
    /// kontrolcü benzersiz damga bekler.
    fn renumber(history: &mut [KvOperation]) {
        let mut stamps: Vec<(u64, u8, usize)> = Vec::new();
        for (index, operation) in history.iter().enumerate() {
            stamps.push((operation.call, 0, index));
            if let Some(ret) = operation.ret {
                stamps.push((ret, 1, index));
            }
        }
        stamps.sort_unstable();
        for (stamp, (_, kind, index)) in (1_u64..).zip(stamps) {
            if kind == 0 {
                history[index].call = stamp;
            } else {
                history[index].ret = Some(stamp);
            }
        }
    }

    /// Rastgele (çoğunlukla linearizable OLMAYAN) küçük bir geçmiş: aralıklar ve okuma sonuçları
    /// rastgeledir, değerler küçük bir kümeden gelir.
    fn random_history() -> impl Strategy<Value = Vec<KvOperation>> {
        let operation = (0_u8..4, 0_u8..2, 0_u64..20, 1_u64..10, 0_u8..4, 0_u8..3);
        prop::collection::vec(operation, 1..7).prop_map(|specs| {
            let mut history: Vec<KvOperation> = specs
                .into_iter()
                .enumerate()
                .map(|(k, (kind, key, call, length, settle, read))| {
                    let key = vec![b'a' + key];
                    let token = vec![b'0' + u8::try_from(k % 3).unwrap_or(0)];
                    let input = match kind {
                        0 => KvInput::Put { key, value: token },
                        1 => KvInput::Append { key, value: token },
                        2 => KvInput::Delete { key },
                        _ => KvInput::Get { key },
                    };
                    let output = match &input {
                        KvInput::Get { .. } => KvOutput::Value(match read {
                            0 => None,
                            1 => Some(vec![b'0']),
                            _ => Some(vec![b'1']),
                        }),
                        _ => KvOutput::Ok,
                    };
                    let indeterminate = settle == 0;
                    KvOperation {
                        client: 0,
                        call,
                        ret: (!indeterminate).then_some(call + length),
                        input,
                        output: (!indeterminate).then_some(output),
                    }
                })
                .collect();
            renumber(&mut history);
            history
        })
    }

    proptest! {
        #![proptest_config(config())]

        // Yapı gereği linearizable her geçmiş kabul edilir.
        #[test]
        fn constructed_linearizable_histories_are_accepted(history in linearizable_history()) {
            prop_assert_eq!(check_kv(&history), Ok(()));
        }

        // Böyle bir geçmişte tamamlanmış bir okumanın sonucu hiç yazılmamış bir değere çevrilirse
        // geçmiş reddedilir: o değeri hiçbir sıra üretemez.
        #[test]
        fn a_read_of_a_value_never_written_is_rejected(
            history in linearizable_history(),
            pick in any::<prop::sample::Index>(),
        ) {
            let reads: Vec<usize> = (0..history.len())
                .filter(|&index| {
                    history[index].ret.is_some()
                        && matches!(history[index].input, KvInput::Get { .. })
                })
                .collect();
            prop_assume!(!reads.is_empty());
            let mut corrupted = history;
            let index = reads[pick.index(reads.len())];
            corrupted[index].output = Some(KvOutput::Value(Some(vec![0xee])));
            prop_assert!(rejected(&corrupted));
        }

        // Fark testi (differential): küçük rastgele geçmişlerde kontrolcünün kararı kaba kuvvet
        // tanımınınkiyle aynıdır (anahtar bölme, bellekleme ve just-in-time sıralama dahil).
        #[test]
        fn the_checker_agrees_with_the_brute_force_definition(history in random_history()) {
            prop_assert_eq!(check_kv(&history).is_ok(), brute_force(&history));
        }
    }
}
