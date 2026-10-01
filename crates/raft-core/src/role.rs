//! Bir düğümün o anki rolü (§5.1, Figure 4).

/// Raft'ta her düğüm her an bu üç rolden birindedir (§5.1).
///
/// Geçişler (Figure 4): Follower, seçim zaman aşımı dolunca Candidate olur. Candidate çoğunluğun
/// oyunu alırsa Leader olur; aynı ya da daha yüksek term'li bir liderden haber alırsa Follower'a
/// döner; zaman aşımında yeni bir seçim başlatır. Leader daha yüksek bir term görünce Follower'a
/// döner.
///
/// Rol geçici (volatile) durumdur: diske yazılmaz. Yeniden başlayan her düğüm Follower'dır; aksi
/// hâlde çöküp kalkan eski bir lider, yerine seçilen yeni lideri bilmeden liderlik etmeye
/// çalışırdı.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// Pasif rol: liderden heartbeat bekler, adaylara oy verir.
    Follower,
    /// Seçim başlatmış, oy toplayan düğüm.
    Candidate,
    /// Seçimi kazanmış düğüm: düzenli heartbeat'lerle liderliğini duyurur.
    Leader,
}
