//! Temel değer tipleri: düğüm kimliği, term, log index'i ve opak komut baytları.

/// Bir Raft düğümünü küme içinde benzersiz biçimde tanımlayan kimlik.
///
/// `Ord`/`PartialOrd` türetilmiştir: `BTreeMap`/`BTreeSet` anahtarı olarak kullanılabilsin diye.
/// `raft-core` içinde yineleme sırası her zaman deterministik OLMALI (`HashMap`/`HashSet` bu
/// crate'te yasaktır; bkz. `scripts/check_forbidden.sh`); `NodeId`'nin tam (total) sıralaması,
/// örneğin eş kümesi (`peers`) üzerinde gezinirken her platformda ve her çalıştırmada aynı sırayı
/// garanti eder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

/// Raft term'i (§5.1): seçim dönemlerini numaralandıran, yalnızca ileri giden mantıksal saat.
///
/// Her term bir seçimle başlar ve en fazla bir lideri olur (Election Safety). Term'ler, düğümlerin
/// eskimiş bilgiyi tanımasını sağlar: daha küçük term'li bir istek reddedilir; daha büyük bir term
/// gören düğüm onu hemen benimser ve Follower'a döner (Figure 2, "All Servers").
///
/// `Default` değeri `Term(0)`'dır: Figure 2'deki ilk açılış değeri ("initialized to 0 on first
/// boot"). `Ord`: term'ler yalnızca büyüklükleriyle karşılaştırılır.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(pub u64);

/// Log index'i (§5.3). Girdiler 1'den numaralanır; `LogIndex(0)` "hiç girdi yok" demektir: boş bir
/// log'un `lastLogIndex`'i 0'dır (Figure 2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(pub u64);

/// İstemcinin durum makinesine uygulanmasını istediği komutun opak bayt gösterimi.
///
/// `raft-core` bu baytların içeriğini asla yorumlamaz/ayrıştırmaz: KV durum makinesi (Put/Delete
/// gibi semantik) Faz 3'te `raft-core`'un DIŞINDA, `Output::Apply` çıktısını tüketen tarafta
/// yaşayacak. Çekirdek yalnızca bu baytları log'a yazar ve sırayla iletir. Bu ayrım çekirdeği
/// uygulamadan (KV anlamından) bağımsız tutar: Raft yalnızca hangi komutun hangi sırayla
/// uygulanacağına karar verir, komutun ne yaptığına değil.
///
/// Alan kasıtlı olarak private'tır: iç temsil ileride değişirse (ör. gerekirse ek metadata), komutu
/// `Command(bytes)` ile doğrudan kuran ya da `cmd.0` ile alana erişen kod kırılmasın; herkes
/// `new`/`as_bytes`/`into_bytes` üzerinden geçer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Command(Vec<u8>);

impl Command {
    /// Ham baytlardan yeni bir komut oluşturur. Baytların anlamı `raft-core`'u ilgilendirmez.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Komutun baytlarına ödünç (borrowed) erişim verir; kopya almadan inceleme içindir.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Komutu tüketip iç bayt vektörünü sahipliğiyle birlikte döndürür.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::Command;

    // C1: `Command` yalnızca baytları taşıyan şeffaf bir kap olmalı; içerik ne verilirse
    // `as_bytes`/`into_bytes` ile birebir aynen geri gelmeli. raft-core baytların anlamını
    // hiç yorumlamadığından bu "round-trip" özelliği, opaklık sözleşmesinin tek kanıtıdır.
    #[test]
    fn command_round_trips_bytes() {
        let original = vec![1_u8, 2, 3, 4, 5];

        let cmd = Command::new(original.clone());
        assert_eq!(cmd.as_bytes(), &original[..]);

        let cmd2 = Command::new(original.clone());
        assert_eq!(cmd2.into_bytes(), original);
    }
}
