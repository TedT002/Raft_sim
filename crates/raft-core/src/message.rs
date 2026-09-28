//! Düğümler arası protokol mesajları: Raft makalesinin Figure 2'sindeki RPC'ler.

/// İki düğüm arasında değiş tokuş edilen Raft RPC'si.
///
/// Faz 0'da alan taşımayan (field-less) yer tutucu varyantlardır; isimler §5.2/§5.3'teki RPC
/// sözlüğünü şimdiden sabitler ki Faz 2 bunları alanlı (struct) varyantlara çevirirken `sim` ve
/// testlerdeki exhaustive (`_` kolu olmayan) `match` ifadeleri değişikliği derleme zamanında
/// yakalasın. `InstallSnapshot` kasıtlı olarak burada YOK: o Faz 6'nın (snapshot/log compaction)
/// kapsamı.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// §5.2: Aday, oy istemek için diğer düğümlere gönderir. Alıcı, oyu yalnızca adayın log'u en az
    /// kendi log'u kadar güncelse verir (seçim kısıtı, §5.4.1).
    RequestVote,
    /// §5.2: `RequestVote`'a verilen cevap; Faz 2'de `term` ve `voteGranted` alanlarını taşıyacak
    /// (Figure 2).
    RequestVoteResponse,
    /// §5.3: Lider, log girdilerini çoğaltmak için gönderir; girdisi boş olanı heartbeat'tir
    /// (§5.2).
    AppendEntries,
    /// §5.3: `AppendEntries`'e verilen cevap. Faz 2'de `term` ve `success` taşır (heartbeat cevabı:
    /// cevapta daha yüksek bir term gören lider Follower'a düşer, §5.1). Faz 3'te ek olarak kabul
    /// edilen son log index'ini taşıması planlanıyor: mesajlar çoğaltılıp gecikebildiği için bir
    /// cevap, hangi isteğe ait olduğu bilinerek eşlenemez (Figure 2'de yalnızca `term` ve `success`
    /// vardır).
    AppendEntriesResponse,
}
