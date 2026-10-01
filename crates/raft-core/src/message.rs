//! Düğümler arası protokol mesajları: Raft makalesinin Figure 2'sindeki RPC'ler.

use crate::types::{LogIndex, Term};

/// İki düğüm arasında değiş tokuş edilen Raft RPC'si.
///
/// Her varyant, Figure 2'deki argümanları taşıyan ayrı bir yapıyı sarar. Varyant isimleri Faz 0'da
/// §5.2/§5.3'teki RPC sözlüğüyle sabitlendi; `sim` ve testlerdeki exhaustive (`_` kolu olmayan)
/// `match` ifadeleri her şekil değişikliğini derleme zamanında yakalar. `InstallSnapshot` kasıtlı
/// olarak burada YOK: o Faz 6'nın (snapshot/log compaction) kapsamı.
///
/// Figure 2'deki `candidateId` ve `leaderId` argümanları bilerek taşınmaz: göndereni taşıma katmanı
/// zaten bildirir (`Input::Message { from, .. }`). Aynı bilginin iki kopyası birbirini tutmayabilir
/// ve "hangisine güvenilecek?" sorusunu doğururdu; tek kaynak `from`'dur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// §5.2: Aday, oy istemek için diğer düğümlere gönderir. Alıcı, oyu yalnızca adayın log'u en az
    /// kendi log'u kadar güncelse verir (seçim kısıtı, §5.4.1).
    RequestVote(RequestVote),
    /// §5.2: `RequestVote`'a verilen cevap: `term` ve `voteGranted` (Figure 2).
    RequestVoteResponse(RequestVoteResponse),
    /// §5.3: Lider, log girdilerini çoğaltmak için gönderir; girdisi boş olanı heartbeat'tir
    /// (§5.2). Faz 2'de yalnızca heartbeat vardır.
    AppendEntries(AppendEntries),
    /// §5.3: `AppendEntries`'e verilen cevap: `term` ve `success` (heartbeat cevabı: cevapta daha
    /// yüksek bir term gören lider Follower'a düşer, §5.1). Faz 3'te ek olarak kabul edilen son log
    /// index'ini taşıması planlanıyor: mesajlar çoğaltılıp gecikebildiği için bir cevap, hangi
    /// isteğe ait olduğu bilinerek eşlenemez (Figure 2'de yalnızca `term` ve `success` vardır).
    AppendEntriesResponse(AppendEntriesResponse),
}

impl Message {
    /// Mesajın taşıdığı term.
    ///
    /// Her RPC ve her cevap bir term taşır. Figure 2'nin "All Servers" kuralı hepsine aynı biçimde
    /// uygulanır: alınan term daha yüksekse alıcı onu benimser ve Follower'a döner (§5.1).
    #[must_use]
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote(request) => request.term,
            Message::RequestVoteResponse(response) => response.term,
            Message::AppendEntries(request) => request.term,
            Message::AppendEntriesResponse(response) => response.term,
        }
    }
}

/// `RequestVote` argümanları (Figure 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    /// Adayın term'i.
    pub term: Term,
    /// Adayın son log girdisinin index'i; seçim kısıtı için (§5.4.1).
    pub last_log_index: LogIndex,
    /// Adayın son log girdisinin term'i; seçim kısıtı için (§5.4.1).
    pub last_log_term: Term,
}

/// `RequestVote` cevabı (Figure 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    /// Cevaplayanın güncel term'i: aday, kendi term'i eskiyse bunu görüp Follower'a döner.
    pub term: Term,
    /// Oy verildi mi?
    pub vote_granted: bool,
}

/// `AppendEntries` argümanları (Figure 2). Faz 2'de yalnızca girdisiz heartbeat gönderilir;
/// `prevLogIndex`, `prevLogTerm`, `entries[]` ve `leaderCommit` log replikasyonuyla (Faz 3)
/// gelecek.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    /// Liderin term'i.
    pub term: Term,
}

/// `AppendEntries` cevabı (Figure 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// Cevaplayanın güncel term'i: eski bir lider bunu görüp Follower'a döner.
    pub term: Term,
    /// İstek kabul edildi mi? Faz 2'de yalnızca term'e bağlıdır (eski term'li istek reddedilir);
    /// log tutarlılık kontrolü (`prevLogIndex`/`prevLogTerm`) Faz 3'te eklenecek.
    pub success: bool,
}
