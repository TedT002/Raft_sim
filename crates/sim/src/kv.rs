//! KV durum makinesi: Raft'ın commit ettiği komutları uygulayan, raft-core'un dışındaki taraf.
//!
//! Raft komutların anlamını bilmez (C1): yalnızca hangi baytların hangi sırayla uygulanacağına
//! karar verir. Anlam burada, `Apply`'ı tüketen tarafta yaşar. Okumalar Faz 4'te log üzerinden
//! gelecek; şimdilik yazma komutları vardır.

use std::collections::BTreeMap;

use raft_core::Command;

/// Durum makinesinin komutu.
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
}

impl KvCommand {
    /// Komutun bayt kodlaması: bir etiket baytı (1 = Put, 2 = Delete), ardından her alan için
    /// little-endian `u64` uzunluk ve baytlar.
    ///
    /// Neden kendi kodlamamız: komut baytları log'a, diske ve trace özetine girer; aynı komut her
    /// sürümde aynı baytları vermelidir. Elle yazılmış, belgelenmiş bir biçim bunu bir serileştirme
    /// kütüphanesinin sürüm davranışına bağlı bırakmaz.
    #[must_use]
    pub fn encode(&self) -> Command {
        let mut bytes = Vec::new();
        match self {
            KvCommand::Put { key, value } => {
                bytes.push(1);
                put_field(&mut bytes, key);
                put_field(&mut bytes, value);
            }
            KvCommand::Delete { key } => {
                bytes.push(2);
                put_field(&mut bytes, key);
            }
        }
        Command::new(bytes)
    }

    /// Bayt kodlamasını çözer.
    ///
    /// # Errors
    ///
    /// Bilinmeyen bir etiket, eksik ya da fazla bayt varsa [`KvDecodeError`].
    pub fn decode(bytes: &[u8]) -> Result<Self, KvDecodeError> {
        let (&tag, mut rest) = bytes.split_first().ok_or(KvDecodeError::Truncated)?;
        let command = match tag {
            1 => {
                let key = take_field(&mut rest)?;
                let value = take_field(&mut rest)?;
                KvCommand::Put { key, value }
            }
            2 => KvCommand::Delete {
                key: take_field(&mut rest)?,
            },
            other => return Err(KvDecodeError::UnknownTag(other)),
        };
        if rest.is_empty() {
            Ok(command)
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

fn take_field(rest: &mut &[u8]) -> Result<Vec<u8>, KvDecodeError> {
    let (len, tail) = rest
        .split_first_chunk::<8>()
        .ok_or(KvDecodeError::Truncated)?;
    let len = usize::try_from(u64::from_le_bytes(*len)).map_err(|_| KvDecodeError::Truncated)?;
    if tail.len() < len {
        return Err(KvDecodeError::Truncated);
    }
    let (field, tail) = tail.split_at(len);
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

/// Bir düğümün KV durum makinesi: commit edilen komutların sırayla uygulanmasıyla oluşan
/// anahtar-değer tablosu.
///
/// Geçicidir: düğüm çökünce kaybolur ve yeniden başlatmadan sonra girdiler baştan uygulanarak
/// yeniden kurulur (Raft'ın `lastApplied`'ı da geçicidir). `BTreeMap`: içerik her koşuda aynı
/// sırayla gezilir ve iki düğümün tabloları doğrudan karşılaştırılabilir.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KvStore {
    /// Commit edilmiş bir komutu uygular.
    ///
    /// Çözülemeyen bir komut durumu değiştirmez ve hata döner: Raft için komutlar opak baytlardır,
    /// anlamsız bir komutu reddetmek durum makinesinin işidir. Her düğüm aynı komutları aynı
    /// sırayla uyguladığı için hepsi aynı komutu aynı biçimde yok sayar; durumlar ayrışmaz.
    ///
    /// # Errors
    ///
    /// Komut çözülemezse [`KvDecodeError`].
    pub fn apply(&mut self, command: &Command) -> Result<(), KvDecodeError> {
        match KvCommand::decode(command.as_bytes())? {
            KvCommand::Put { key, value } => {
                self.data.insert(key, value);
            }
            KvCommand::Delete { key } => {
                self.data.remove(&key);
            }
        }
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::{KvCommand, KvDecodeError, KvStore};
    use raft_core::Command;

    // Kodlama gidiş-dönüş: her komut kendi baytlarından aynen geri çözülür; boş anahtar ve değer
    // de.
    #[test]
    fn commands_round_trip_through_their_encoding() {
        let commands = [
            KvCommand::Put {
                key: b"k".to_vec(),
                value: b"value".to_vec(),
            },
            KvCommand::Put {
                key: Vec::new(),
                value: Vec::new(),
            },
            KvCommand::Delete { key: b"k".to_vec() },
        ];
        for command in commands {
            let encoded = command.encode();
            assert_eq!(KvCommand::decode(encoded.as_bytes()), Ok(command));
        }
    }

    // Bozuk kodlamalar panik değil hata üretir.
    #[test]
    fn malformed_commands_are_rejected() {
        assert_eq!(KvCommand::decode(&[]), Err(KvDecodeError::Truncated));
        assert_eq!(KvCommand::decode(&[9]), Err(KvDecodeError::UnknownTag(9)));
        assert_eq!(KvCommand::decode(&[2, 5]), Err(KvDecodeError::Truncated));
        let mut bytes = KvCommand::Delete { key: b"k".to_vec() }
            .encode()
            .into_bytes();
        bytes.push(0);
        assert_eq!(KvCommand::decode(&bytes), Err(KvDecodeError::TrailingBytes));
        let huge_length = [2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        assert_eq!(
            KvCommand::decode(&huge_length),
            Err(KvDecodeError::Truncated)
        );
    }

    // Uygulama: Put yazar ya da üzerine yazar, Delete siler; çözülemeyen komut durumu değiştirmez.
    #[test]
    fn the_store_applies_puts_and_deletes() {
        let mut store = KvStore::default();
        let put = |key: &[u8], value: &[u8]| {
            KvCommand::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            }
            .encode()
        };
        store.apply(&put(b"a", b"1")).expect("valid command");
        store.apply(&put(b"b", b"2")).expect("valid command");
        store.apply(&put(b"a", b"3")).expect("valid command");
        store
            .apply(&KvCommand::Delete { key: b"b".to_vec() }.encode())
            .expect("valid command");
        assert_eq!(store.get(b"a"), Some(&b"3"[..]));
        assert_eq!(store.get(b"b"), None);
        assert_eq!(store.len(), 1);
        let before = store.clone();
        assert!(store.apply(&Command::new(vec![7])).is_err());
        assert_eq!(store, before);
    }
}
