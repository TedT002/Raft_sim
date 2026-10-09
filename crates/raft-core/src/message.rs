//! Düğümler arası protokol mesajları: Raft makalesinin Figure 2'sindeki RPC'ler.

use crate::log::LogEntry;
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
    /// (§5.2).
    AppendEntries(AppendEntries),
    /// §5.3: `AppendEntries`'e verilen cevap: `term` ve `success` (heartbeat cevabı: cevapta daha
    /// yüksek bir term gören lider Follower'a düşer, §5.1). Figure 2'de yalnızca `term` ve
    /// `success` vardır; ek olarak bir index taşır (bkz. `AppendEntriesResponse::match_index`):
    /// mesajlar çoğaltılıp gecikebildiği için bir cevap, hangi isteğe ait olduğu bilinerek
    /// eşlenemez.
    AppendEntriesResponse(AppendEntriesResponse),
    /// Tezin §6.4'ü (ReadIndex): lider, bekleyen bir okumayı cevaplamadan önce HÂLÂ lider olduğunu
    /// doğrulamak için bu turu gönderir. Figure 2'de yoktur; tez okuma için "yeni bir heartbeat
    /// turu" ister. Ayrı bir mesaj olmasının nedeni: cevap turun numarasını geri taşır, böylece
    /// geciken ya da çoğaltılmış eski bir cevap, okuma geldikten SONRA başlayan bir turu onaylamış
    /// sayılmaz. AppendEntries'e alan eklemek de olurdu; ama o zaman okumasız her koşunun mesajları
    /// da değişirdi.
    Probe(Probe),
    /// `Probe`'a verilen cevap: cevaplayanın term'i ve turun numarası.
    ProbeResponse(ProbeResponse),
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
            Message::Probe(request) => request.term,
            Message::ProbeResponse(response) => response.term,
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

/// `AppendEntries` argümanları (Figure 2). Girdisi boş olanı heartbeat'tir (§5.2); tutarlılık
/// denetimi (`prev_log_index`/`prev_log_term`) ve commit bilgisi (`leader_commit`) heartbeat'te de
/// taşınır.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    /// Liderin term'i.
    pub term: Term,
    /// Yeni girdilerden hemen önceki girdinin index'i (boş log için 0).
    pub prev_log_index: LogIndex,
    /// `prev_log_index`'teki girdinin term'i. Takipçi, kendi log'unda bu (index, term) çiftini
    /// bulamazsa isteği reddeder; Log Matching özelliği bu tümevarımsal denetime dayanır (§5.3).
    pub prev_log_term: Term,
    /// Eklenecek girdiler (heartbeat'te boş). Verimlilik için bir mesajda birden fazla girdi
    /// gönderilebilir.
    pub entries: Vec<LogEntry>,
    /// Liderin `commitIndex`'i: takipçi, kendi commitIndex'ini buna göre ilerletir.
    pub leader_commit: LogIndex,
}

/// `AppendEntries` cevabı (Figure 2'deki `term` ve `success`, ek olarak `match_index`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// Cevaplayanın güncel term'i: eski bir lider bunu görüp Follower'a döner.
    pub term: Term,
    /// İstek kabul edildi mi? Term eskiyse ya da `prev_log_index`'te `prev_log_term`'lü bir girdi
    /// yoksa hayır.
    pub success: bool,
    /// Takipçinin log'unun liderinkiyle eşleşmesine dair bir index.
    ///
    /// - Başarılıysa: eşleşmenin KESİN olduğu son index (`prev_log_index + entries.len()`). Lider
    ///   bunu doğrudan `matchIndex` yapar; cevap isteği yeniden göndermeye gerek kalmadan anlatır.
    /// - Başarısızsa: eşleşmenin OLABİLECEĞİ en büyük index (takipçinin son index'i, en fazla
    ///   `prev_log_index - 1`). Lider `nextIndex`'i birer birer azaltmak yerine bir hamlede buraya
    ///   çeker; geride kalmış bir takipçi yüzlerce tur yerine tek turda yakalanır. Bu, §5.3'ün
    ///   sonunda anlatılan iyileştirmenin basit bir hâlidir.
    pub match_index: LogIndex,
}

/// Liderliği doğrulama turu (ReadIndex, tezin §6.4'ü; bkz. `Message::Probe`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    /// Liderin term'i.
    pub term: Term,
    /// Turun numarası: lider her yeni turda bir artırır (liderlik başına 1'den).
    pub round: u64,
}

/// `Probe` cevabı.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeResponse {
    /// Cevaplayanın güncel term'i. Liderinkinden yüksekse lider geride kaldığını öğrenir (T1).
    pub term: Term,
    /// Onaylanan turun numarası: tur cevaplayanın GÜNCEL term'inden geldiyse istekteki `round`,
    /// değilse 0 (onay değildir; turlar 1'den sayılır). Yalnızca o zaman, aynı term'deki bir cevap
    /// "cevaplayan, bu term'in liderini o turdan sonra tanıdı" demektir: başka bir term'in turuna
    /// verilen cevap, aynı numaralı yeni bir turun onayı sanılamaz.
    pub round: u64,
}
