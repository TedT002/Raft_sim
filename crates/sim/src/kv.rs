//! KV durum makinesi: Raft'ın commit ettiği komutları uygulayan, raft-core'un dışındaki taraf.
//!
//! Raft komutların anlamını bilmez (C1): yalnızca hangi baytların hangi sırayla uygulanacağına
//! karar verir. Anlam burada, `Apply`'ı tüketen tarafta yaşar. Okumalar da (`Get`) log üzerinden
//! geçer: bir okuma, log'daki yerinde uygulanır ve o andaki değeri döndürür. Basit ve doğrudur;
//! okumayı lidere sormak (log'a yazmadan) bayat bir liderin eski değer döndürmesine yol açabilirdi.
//!
//! İstemci oturumları (§8, tezin §6.3'ü): her istek `(client, seq)` taşır. Zaman aşımına uğrayan
//! istemci aynı isteği aynı `seq` ile yeniden dener; ilk deneme de commit edilmiş olabilir. Durum
//! makinesi her istemcinin uyguladığı son `seq`'i ve sonucunu saklar: aynı `seq` ikinci kez
//! gelirse yeniden UYGULAMAZ, saklı sonucu döndürür. Böylece her istek en fazla bir kez etki eder.
//! Bu tekilleştirme olmasaydı, cevabı kaybolup yeniden denenen bir `Append` iki kez eklenirdi.
//!
//! Boş komut, liderin term başındaki no-op girdisidir (`Command::noop`): durum makinesi onu alır ve
//! hiçbir şey yapmaz.

use std::collections::BTreeMap;

use raft_core::Command;

use crate::fnv::Fnv1a64;

/// Durum makinesinin komutu: bir istemci işlemi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvCommand {
    /// `key`'in değerini `value` yap.
    Put {
        /// Anahtar.
        key: Vec<u8>,
        /// Yeni değer.
        value: Vec<u8>,
    },
    /// `key`'i sil (yoksa etkisizdir).
    Delete {
        /// Anahtar.
        key: Vec<u8>,
    },
    /// `key`'in değerini oku. Log üzerinden geçer: sonuç, komutun log'daki yerindeki değerdir.
    Get {
        /// Anahtar.
        key: Vec<u8>,
    },
    /// `key`'in değerinin sonuna `value` ekle (değer yoksa boş sayılır). İdempotent değildir: iki
    /// kez uygulanan bir istek sonraki okumada görünür. Tekilleştirme hatalarını görünür kılan
    /// işlem budur.
    Append {
        /// Anahtar.
        key: Vec<u8>,
        /// Eklenen baytlar.
        value: Vec<u8>,
    },
}

impl KvCommand {
    /// Komutun dokunduğu anahtar.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            KvCommand::Put { key, .. }
            | KvCommand::Delete { key }
            | KvCommand::Get { key }
            | KvCommand::Append { key, .. } => key,
        }
    }

    /// Kanonik kodlama: bir etiket baytı (1 = Put, 2 = Delete, 3 = Get, 4 = Append), ardından her
    /// alan için little-endian `u64` uzunluk ve baytlar.
    fn encode_into(&self, bytes: &mut Vec<u8>) {
        match self {
            KvCommand::Put { key, value } => {
                bytes.push(1);
                put_field(bytes, key);
                put_field(bytes, value);
            }
            KvCommand::Delete { key } => {
                bytes.push(2);
                put_field(bytes, key);
            }
            KvCommand::Get { key } => {
                bytes.push(3);
                put_field(bytes, key);
            }
            KvCommand::Append { key, value } => {
                bytes.push(4);
                put_field(bytes, key);
                put_field(bytes, value);
            }
        }
    }

    fn decode_from(rest: &mut &[u8]) -> Result<Self, KvDecodeError> {
        let (&tag, tail) = rest.split_first().ok_or(KvDecodeError::Truncated)?;
        *rest = tail;
        Ok(match tag {
            1 => {
                let key = take_field(rest)?;
                let value = take_field(rest)?;
                KvCommand::Put { key, value }
            }
            2 => KvCommand::Delete {
                key: take_field(rest)?,
            },
            3 => KvCommand::Get {
                key: take_field(rest)?,
            },
            4 => {
                let key = take_field(rest)?;
                let value = take_field(rest)?;
                KvCommand::Append { key, value }
            }
            other => return Err(KvDecodeError::UnknownTag(other)),
        })
    }
}

/// Bir istemci oturumundaki istek: kim (`client`), oturumdaki kaçıncı istek (`seq`) ve komut.
///
/// `seq` istemci başına 1'den başlayıp artar; istemci bir isteği yeniden denerken AYNI `seq`'i
/// kullanır (§8). İstemci 0, [`crate::RaftCluster::submit`]'in kendi iç oturumuna ayrılmıştır.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRequest {
    /// İstemci kimliği.
    pub client: u64,
    /// Oturumdaki sıra numarası.
    pub seq: u64,
    /// Komut.
    pub command: KvCommand,
}

impl KvRequest {
    /// İsteğin log'a giren bayt kodlaması: little-endian `u64` istemci, `u64` sıra numarası,
    /// ardından komut (bkz. `KvCommand`). Hiçbir zaman boş değildir; boş komut no-op'a ayrılmıştır.
    ///
    /// Neden kendi kodlamamız: komut baytları log'a, diske ve trace özetine girer; aynı istek her
    /// sürümde aynı baytları vermelidir. Elle yazılmış, belgelenmiş bir biçim bunu bir serileştirme
    /// kütüphanesinin sürüm davranışına bağlı bırakmaz.
    #[must_use]
    pub fn encode(&self) -> Command {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.client.to_le_bytes());
        bytes.extend_from_slice(&self.seq.to_le_bytes());
        self.command.encode_into(&mut bytes);
        Command::new(bytes)
    }

    /// Bayt kodlamasını çözer.
    ///
    /// # Errors
    ///
    /// Bilinmeyen bir etiket, eksik ya da fazla bayt varsa [`KvDecodeError`].
    pub fn decode(bytes: &[u8]) -> Result<Self, KvDecodeError> {
        let mut rest = bytes;
        let client = take_u64(&mut rest)?;
        let seq = take_u64(&mut rest)?;
        let command = KvCommand::decode_from(&mut rest)?;
        if rest.is_empty() {
            Ok(Self {
                client,
                seq,
                command,
            })
        } else {
            Err(KvDecodeError::TrailingBytes)
        }
    }
}

fn put_field(bytes: &mut Vec<u8>, field: &[u8]) {
    let len = u64::try_from(field.len()).unwrap_or(u64::MAX);
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(field);
}

fn take_u64(rest: &mut &[u8]) -> Result<u64, KvDecodeError> {
    let (value, tail) = rest
        .split_first_chunk::<8>()
        .ok_or(KvDecodeError::Truncated)?;
    *rest = tail;
    Ok(u64::from_le_bytes(*value))
}

fn take_field(rest: &mut &[u8]) -> Result<Vec<u8>, KvDecodeError> {
    let len = take_u64(rest)?;
    let len = usize::try_from(len).map_err(|_| KvDecodeError::Truncated)?;
    if rest.len() < len {
        return Err(KvDecodeError::Truncated);
    }
    let (field, tail) = rest.split_at(len);
    *rest = tail;
    Ok(field.to_vec())
}

/// Çözülemeyen bir komut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KvDecodeError {
    /// Komut beklenenden kısa.
    #[error("the command is truncated")]
    Truncated,
    /// Bilinmeyen komut etiketi.
    #[error("unknown command tag {0}")]
    UnknownTag(u8),
    /// Komuttan sonra fazladan bayt var.
    #[error("the command has trailing bytes")]
    TrailingBytes,
}

/// Bir komutun istemciye dönen sonucu.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvResult {
    /// Yazma komutu (`Put`, `Delete`, `Append`) uygulandı.
    Ok,
    /// Okunan değer (`Get`); anahtar yoksa `None`.
    Value(Option<Vec<u8>>),
}

/// Log'daki bir girdinin durum makinesine uygulanmasının sonucu.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvApplied {
    /// No-op girdi (§8): durum değişmez, kimseye cevap yoktur.
    Noop,
    /// İstek ilk kez uygulandı.
    Executed {
        /// İstemci.
        client: u64,
        /// Sıra numarası.
        seq: u64,
        /// Sonuç.
        result: KvResult,
    },
    /// Aynı `(client, seq)` daha önce uygulandı: yeniden uygulanmadı, saklı sonuç döner (§8).
    Duplicate {
        /// İstemci.
        client: u64,
        /// Sıra numarası.
        seq: u64,
        /// İlk uygulamanın sonucu.
        result: KvResult,
    },
    /// İstemci bu isteği çoktan geride bıraktı (daha büyük bir `seq` uygulandı): hiçbir şey
    /// yapılmaz. İstemci artık bu isteğin cevabını beklemiyordur.
    Stale {
        /// İstemci.
        client: u64,
        /// Sıra numarası.
        seq: u64,
    },
    /// Çözülemeyen bayt: durum değişmez. Raft için komutlar opak baytlardır; anlamsız bir komutu
    /// reddetmek durum makinesinin işidir. Her düğüm aynı komutları aynı sırayla uyguladığı için
    /// hepsi aynı komutu aynı biçimde yok sayar; durumlar ayrışmaz.
    Invalid(KvDecodeError),
}

/// Bir istemcinin oturumu: uygulanan son sıra numarası ve sonucu.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Session {
    seq: u64,
    result: KvResult,
}

/// Bir düğümün KV durum makinesi: commit edilen komutların sırayla uygulanmasıyla oluşan
/// anahtar-değer tablosu ve istemci oturumları.
///
/// Geçicidir: düğüm çökünce kaybolur ve yeniden başlatmadan sonra girdiler baştan uygulanarak
/// yeniden kurulur (Raft'ın `lastApplied`'ı da geçicidir). Oturumlar da log'dan yeniden kurulduğu
/// için tekilleştirme çökmeden sağ çıkar. `BTreeMap`: içerik her koşuda aynı sırayla gezilir ve iki
/// düğümün tabloları doğrudan karşılaştırılabilir.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    sessions: BTreeMap<u64, Session>,
}

impl KvStore {
    /// Commit edilmiş bir girdinin komutunu uygular (bkz. [`KvApplied`]).
    pub fn apply(&mut self, command: &Command) -> KvApplied {
        if command.is_noop() {
            return KvApplied::Noop;
        }
        let KvRequest {
            client,
            seq,
            command,
        } = match KvRequest::decode(command.as_bytes()) {
            Ok(request) => request,
            Err(error) => return KvApplied::Invalid(error),
        };
        // §8: oturum, isteğin daha önce uygulanıp uygulanmadığını söyler. Son `seq`'i ve sonucunu
        // saklamak yeter. Neden: bir istemcinin sonra kabul edilen isteği log'da hep daha büyük
        // bir index'tedir (Log Matching: önce kabul edilen girdiyi taşıyan her log, onu yaratan
        // liderin o anki önekini taşır ve sonra yaratılan girdi o önekte olamaz). Bu yüzden
        // commit edilen istekler uygulanırken bir istemcinin `seq`'leri hiç geri gitmez: `Stale`
        // dalı doğru bir Raft'ta yalnızca savunmadır. Bu, `RaftCluster::submit`'in iç oturumu
        // (istemci 0) için de geçerlidir, ama o oturum isteklerini beklemeden eşzamanlı verir:
        // orada görülen bir `Stale` bir Raft hatasının izi olurdu. KV onu sessizce atlar, ama
        // State Machine Safety ham komutları yine de denetler.
        // Mutasyon `mutation-no-dedup` (Faz 5) oturumu yok sayar: yeniden denenen bir istek ikinci
        // kez uygulanır.
        let session = if cfg!(feature = "mutation-no-dedup") {
            None
        } else {
            self.sessions.get(&client)
        };
        if let Some(session) = session {
            if seq == session.seq {
                return KvApplied::Duplicate {
                    client,
                    seq,
                    result: session.result.clone(),
                };
            }
            if seq < session.seq {
                return KvApplied::Stale { client, seq };
            }
        }
        let result = self.execute(command);
        self.sessions.insert(
            client,
            Session {
                seq,
                result: result.clone(),
            },
        );
        KvApplied::Executed {
            client,
            seq,
            result,
        }
    }

    fn execute(&mut self, command: KvCommand) -> KvResult {
        match command {
            KvCommand::Put { key, value } => {
                self.data.insert(key, value);
                KvResult::Ok
            }
            KvCommand::Delete { key } => {
                self.data.remove(&key);
                KvResult::Ok
            }
            KvCommand::Get { key } => KvResult::Value(self.data.get(&key).cloned()),
            KvCommand::Append { key, value } => {
                self.data.entry(key).or_default().extend_from_slice(&value);
                KvResult::Ok
            }
        }
    }

    /// Bir anahtarın değeri.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(Vec::as_slice)
    }

    /// Anahtar sayısı.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Tablo boş mu?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Bir istemcinin uygulanan son sıra numarası (hiç isteği uygulanmadıysa `None`).
    #[must_use]
    pub fn last_seq(&self, client: u64) -> Option<u64> {
        self.sessions.get(&client).map(|session| session.seq)
    }

    /// Durum makinesinin kanonik bayt kodlaması: bir snapshot'ın verisi (§7). Tablo ve istemci
    /// oturumları, `BTreeMap` sırasıyla: aynı durum her zaman aynı baytları verir (snapshot'lı
    /// koşuların trace'i de deterministik kalır). İki kopyanın durumunu karşılaştırmak için bu
    /// kodlama değil [`KvStore::fingerprint`] kullanılır (bkz. orada neden).
    ///
    /// Oturumlar snapshot'ın PARÇASIDIR (tezin §6.3'ü): snapshot'tan kurulan bir düğüm, aynı isteği
    /// ikinci kez uygulamamak için hangi isteklerin uygulandığını bilmelidir. Mutasyon
    /// `mutation-snapshot-without-sessions` (Faz 6) oturumları yazmaz: snapshot'tan kurulan bir
    /// düğüm, yeniden denenen bir isteği bir kez daha uygular.
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        put_count(&mut bytes, self.data.len());
        for (key, value) in &self.data {
            put_field(&mut bytes, key);
            put_field(&mut bytes, value);
        }
        if cfg!(feature = "mutation-snapshot-without-sessions") {
            put_count(&mut bytes, 0);
            return bytes;
        }
        put_count(&mut bytes, self.sessions.len());
        for (client, Session { seq, result }) in &self.sessions {
            bytes.extend_from_slice(&client.to_le_bytes());
            bytes.extend_from_slice(&seq.to_le_bytes());
            match result {
                KvResult::Ok => bytes.push(0),
                KvResult::Value(None) => bytes.push(1),
                KvResult::Value(Some(value)) => {
                    bytes.push(2);
                    put_field(&mut bytes, value);
                }
            }
        }
        bytes
    }

    /// Durum makinesinin parmak izi: tablo ve oturumların tamamının FNV-1a özeti. Snapshot
    /// güvenliğini denetleyen kâhin (`RaftCluster`) bunu kullanır.
    ///
    /// Neden [`KvStore::snapshot`]'tan ayrı: kâhin, denetlediği kodun kodlayıcısına
    /// güvenmemelidir. Snapshot kodlayıcısı durumun bir parçasını düşürürse (ör. oturumları), aynı
    /// kodlayıcıyla alınan bir özet bu farkı göremezdi; parmak izi her zaman bütün durumu kapsar.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        // Uzunluk önekli alanlar: ardışık iki alanın sınırı belirsiz kalmasın.
        fn field(hasher: &mut Fnv1a64, bytes: &[u8]) {
            hasher.write_u64(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            hasher.write(bytes);
        }
        let mut hasher = Fnv1a64::new();
        hasher.write_u64(u64::try_from(self.data.len()).unwrap_or(u64::MAX));
        for (key, value) in &self.data {
            field(&mut hasher, key);
            field(&mut hasher, value);
        }
        hasher.write_u64(u64::try_from(self.sessions.len()).unwrap_or(u64::MAX));
        for (client, Session { seq, result }) in &self.sessions {
            hasher.write_u64(*client);
            hasher.write_u64(*seq);
            match result {
                KvResult::Ok => hasher.write_u8(0),
                KvResult::Value(None) => hasher.write_u8(1),
                KvResult::Value(Some(value)) => {
                    hasher.write_u8(2);
                    field(&mut hasher, value);
                }
            }
        }
        hasher.finish()
    }

    /// [`KvStore::snapshot`]'ın kodlamasından bir durum makinesi kurar.
    ///
    /// # Errors
    ///
    /// Eksik, fazla ya da bilinmeyen bir bayt varsa [`KvDecodeError`].
    pub fn restore(bytes: &[u8]) -> Result<Self, KvDecodeError> {
        let mut rest = bytes;
        let mut store = KvStore::default();
        for _ in 0..take_u64(&mut rest)? {
            let key = take_field(&mut rest)?;
            let value = take_field(&mut rest)?;
            store.data.insert(key, value);
        }
        for _ in 0..take_u64(&mut rest)? {
            let client = take_u64(&mut rest)?;
            let seq = take_u64(&mut rest)?;
            let (&tag, tail) = rest.split_first().ok_or(KvDecodeError::Truncated)?;
            rest = tail;
            let result = match tag {
                0 => KvResult::Ok,
                1 => KvResult::Value(None),
                2 => KvResult::Value(Some(take_field(&mut rest)?)),
                other => return Err(KvDecodeError::UnknownTag(other)),
            };
            store.sessions.insert(client, Session { seq, result });
        }
        if rest.is_empty() {
            Ok(store)
        } else {
            Err(KvDecodeError::TrailingBytes)
        }
    }
}

/// Bir sayıyı (öğe sayısı) little-endian `u64` olarak yazar.
fn put_count(bytes: &mut Vec<u8>, count: usize) {
    let count = u64::try_from(count).unwrap_or(u64::MAX);
    bytes.extend_from_slice(&count.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::{KvApplied, KvCommand, KvDecodeError, KvRequest, KvResult, KvStore};
    use raft_core::Command;

    fn request(client: u64, seq: u64, command: KvCommand) -> Command {
        KvRequest {
            client,
            seq,
            command,
        }
        .encode()
    }

    fn put(key: &[u8], value: &[u8]) -> KvCommand {
        KvCommand::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn append(key: &[u8], value: &[u8]) -> KvCommand {
        KvCommand::Append {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn get(key: &[u8]) -> KvCommand {
        KvCommand::Get { key: key.to_vec() }
    }

    // Snapshot gidiş-dönüş (§7): tablo ve oturumlar aynen geri gelir; geri kurulan durum makinesi
    // aynı isteğin yeniden denemesini yine oturumdan cevaplar (tekilleştirme snapshot'tan sağ
    // çıkar). Bozuk bir kodlama reddedilir.
    #[test]
    fn a_snapshot_restores_the_table_and_the_sessions() {
        let mut store = KvStore::default();
        let _ = store.apply(&request(1, 1, put(b"k", b"v")));
        let _ = store.apply(&request(2, 1, append(b"k", b"+")));
        let _ = store.apply(&request(3, 1, get(b"k")));
        let _ = store.apply(&request(3, 2, get(b"missing")));
        let bytes = store.snapshot();
        let mut restored = KvStore::restore(&bytes).expect("a valid snapshot");
        assert_eq!(restored, store);
        assert_eq!(restored.snapshot(), bytes);
        assert_eq!(
            restored.apply(&request(2, 1, append(b"k", b"+"))),
            KvApplied::Duplicate {
                client: 2,
                seq: 1,
                result: KvResult::Ok,
            }
        );
        assert_eq!(restored.get(b"k"), Some(&b"v+"[..]));
        assert_eq!(
            KvStore::restore(&KvStore::default().snapshot()),
            Ok(KvStore::default())
        );
        assert_eq!(
            KvStore::restore(&bytes[..bytes.len() - 1]),
            Err(KvDecodeError::Truncated)
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            KvStore::restore(&trailing),
            Err(KvDecodeError::TrailingBytes)
        );
        // Parmak izi bütün durumu kapsar: aynı tabloyu taşıyan ama oturumları farklı iki durum
        // makinesi ayrışır; aynı durum aynı izi verir.
        assert_eq!(restored.fingerprint(), store.fingerprint());
        let mut table = KvStore::default();
        let _ = table.apply(&request(1, 1, put(b"k", b"v")));
        let mut other_session = KvStore::default();
        let _ = other_session.apply(&request(1, 7, put(b"k", b"v")));
        assert_eq!(table.get(b"k"), other_session.get(b"k"));
        assert_ne!(table.fingerprint(), other_session.fingerprint());
    }

    // Kodlama gidiş-dönüş: her istek kendi baytlarından aynen geri çözülür; boş anahtar ve değer
    // de. Hiçbir istek boş değildir (boş komut no-op'tur).
    #[test]
    fn requests_round_trip_through_their_encoding() {
        let commands = [
            put(b"k", b"value"),
            put(b"", b""),
            KvCommand::Delete { key: b"k".to_vec() },
            get(b"k"),
            append(b"k", b"tail"),
        ];
        for (seq, command) in (1_u64..).zip(commands) {
            let original = KvRequest {
                client: 7,
                seq,
                command,
            };
            let encoded = original.encode();
            assert!(!encoded.is_noop());
            assert_eq!(KvRequest::decode(encoded.as_bytes()), Ok(original));
        }
    }

    // Bozuk kodlamalar panik değil hata üretir.
    #[test]
    fn malformed_requests_are_rejected() {
        let header = [0_u8; 16];
        assert_eq!(KvRequest::decode(&[]), Err(KvDecodeError::Truncated));
        assert_eq!(KvRequest::decode(&header), Err(KvDecodeError::Truncated));
        let mut unknown = header.to_vec();
        unknown.push(9);
        assert_eq!(
            KvRequest::decode(&unknown),
            Err(KvDecodeError::UnknownTag(9))
        );
        let mut trailing = request(1, 1, get(b"k")).into_bytes();
        trailing.push(0);
        assert_eq!(
            KvRequest::decode(&trailing),
            Err(KvDecodeError::TrailingBytes)
        );
        let mut huge = header.to_vec();
        huge.extend_from_slice(&[2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(KvRequest::decode(&huge), Err(KvDecodeError::Truncated));
    }

    // Uygulama: Put yazar ya da üzerine yazar, Append sona ekler, Delete siler, Get o anki değeri
    // döndürür; no-op ve çözülemeyen komut durumu değiştirmez.
    #[test]
    fn the_store_applies_commands_in_order() {
        let mut store = KvStore::default();
        let results: Vec<KvApplied> = [
            request(1, 1, put(b"a", b"1")),
            request(1, 2, append(b"a", b"2")),
            Command::noop(),
            request(2, 1, get(b"a")),
            request(2, 2, KvCommand::Delete { key: b"a".to_vec() }),
            request(2, 3, get(b"a")),
        ]
        .iter()
        .map(|command| store.apply(command))
        .collect();
        assert_eq!(
            results,
            vec![
                KvApplied::Executed {
                    client: 1,
                    seq: 1,
                    result: KvResult::Ok
                },
                KvApplied::Executed {
                    client: 1,
                    seq: 2,
                    result: KvResult::Ok
                },
                KvApplied::Noop,
                KvApplied::Executed {
                    client: 2,
                    seq: 1,
                    result: KvResult::Value(Some(b"12".to_vec()))
                },
                KvApplied::Executed {
                    client: 2,
                    seq: 2,
                    result: KvResult::Ok
                },
                KvApplied::Executed {
                    client: 2,
                    seq: 3,
                    result: KvResult::Value(None)
                },
            ]
        );
        let before = store.clone();
        assert_eq!(
            store.apply(&Command::new(vec![7])),
            KvApplied::Invalid(KvDecodeError::Truncated)
        );
        assert_eq!(store, before);
    }

    // §8: aynı `(client, seq)` ikinci kez gelirse yeniden uygulanmaz ve İLK sonucu döndürür (araya
    // başka istemcilerin yazmaları girse bile); geride kalmış bir `seq` hiçbir şey yapmaz.
    // Oturumlar istemci başınadır: başka bir istemcinin aynı `seq`'i ayrı bir istektir.
    #[test]
    fn a_request_is_applied_at_most_once() {
        let mut store = KvStore::default();
        let first = store.apply(&request(1, 1, append(b"k", b"x")));
        assert!(matches!(first, KvApplied::Executed { .. }));
        let _ = store.apply(&request(2, 1, get(b"k")));
        assert_eq!(
            store.apply(&request(1, 1, append(b"k", b"x"))),
            KvApplied::Duplicate {
                client: 1,
                seq: 1,
                result: KvResult::Ok
            }
        );
        assert_eq!(store.get(b"k"), Some(&b"x"[..]), "applied once");
        let read = store.apply(&request(1, 2, get(b"k")));
        let _ = store.apply(&request(2, 2, append(b"k", b"y")));
        assert_eq!(
            store.apply(&request(1, 2, get(b"k"))),
            KvApplied::Duplicate {
                client: 1,
                seq: 2,
                result: KvResult::Value(Some(b"x".to_vec()))
            },
            "the first result, not the current value"
        );
        assert!(matches!(read, KvApplied::Executed { .. }));
        assert_eq!(
            store.apply(&request(1, 1, append(b"k", b"x"))),
            KvApplied::Stale { client: 1, seq: 1 }
        );
        assert_eq!(store.get(b"k"), Some(&b"xy"[..]));
        assert_eq!(store.last_seq(1), Some(2));
        assert_eq!(store.last_seq(3), None);
    }
}
