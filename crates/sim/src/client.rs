//! Simüle istemciler: bir KV iş yükü üretir, istekleri düğümlere verir, cevapları bekler, zaman
//! aşımında yeniden dener ve geçmişi linearizability kontrolcüsünün diliyle kaydeder.
//!
//! İstemciler ağdan geçmez: düğümlere doğrudan bağlıdırlar (bkz. `RaftCluster::submit_request`).
//! Ağın kayıplarını taklit etmek için cevapların bir kısmı seed'li olarak kaybolur. Böylece commit
//! edilip uygulanmış bir isteğin cevabı istemciye ulaşmayabilir ve istemci aynı isteği aynı
//! `(client, seq)` ile yeniden dener. Durum makinesinin tekilleştirmesi (§8) tam da bu durumu
//! korur: istek ikinci kez gelse de bir kez etki eder.
//!
//! Her istemci sırayla çalışır: bir işlemi bitirmeden (sonuç ya da vazgeçme) sonrakine geçmez.
//! Geçmişte bir işlemin çağrı damgası ilk denemesinin, dönüş damgası ilk sonucun (`Done` cevabının)
//! anıdır; `NotLeader` cevabı işlemi bitirmez, yalnızca yönlendirir. Aradaki bütün denemeler bu
//! aralığın içindedir; işlemin etkisi hangi denemeden gelirse gelsin aralığın içindeki bir
//! noktadadır. Vazgeçilen ya da koşu bittiğinde hâlâ bekleyen işlem belirsizdir (dönüşü yok):
//! etkisi olmuş da olabilir, olmamış da. Tek istisna: vazgeçilen işlemin bir düğüme ulaşan her
//! denemesinin reddi (`NotLeader`) istemciye ulaştıysa işlem kesin başarısızdır, hiçbir zaman etki
//! etmeyecektir ve geçmişe hiç girmez (Jepsen'in `:fail`'i). Reddi kaybolan bir deneme kabul
//! edilmiş sayılır (tutucu taraf); kapalı bir düğüme denk gelen deneme hiçbir sunucuya
//! ulaşmamıştır. Böylece kontrolcü, sonucu bilinen işlemleri belirsiz sanıp boşuna aramaz.
//!
//! Zaman: sürücü kümeyi tick tick ilerletir. Her tick'te önce kümenin o ana kadarki olayları
//! işlenir, sonra cevaplar istemcilere ulaşır, en sonda istemciler kimlik sırasıyla davranır.
//! Bütün kararlar kendi alt-seed akışından (`Component::Workload`) gelir: aynı seed aynı iş yükünü
//! ve aynı geçmişi üretir.

use std::collections::{BTreeMap, BTreeSet};

use checker::{KvInput, KvOperation, KvOutput};
use raft_core::NodeId;

use crate::error::{ConfigError, LifecycleError};
use crate::kv::{KvCommand, KvRequest, KvResult};
use crate::raft::{ClientReply, ClusterError, RaftCluster, ReplyOutcome};
use crate::rng::{ChaCha8Rng, Component, SeedTree, chance, uniform_inclusive};

/// İş yükündeki işlem karışımı: her işlem türünün ağırlığı. Bir işlemin türü, ağırlıkların toplamı
/// üzerinden tek bir çekilişle seçilir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpMix {
    /// Okuma (`Get`) ağırlığı.
    pub get: u64,
    /// Yazma (`Put`) ağırlığı.
    pub put: u64,
    /// Ekleme (`Append`) ağırlığı.
    pub append: u64,
    /// Silme (`Delete`) ağırlığı.
    pub delete: u64,
}

impl OpMix {
    /// %40 okuma, %25 yazma, %25 ekleme, %10 silme.
    pub const DEFAULT: Self = Self {
        get: 8,
        put: 5,
        append: 5,
        delete: 2,
    };

    /// Ağırlıkların toplamı (taşmada doygun).
    fn total(&self) -> u64 {
        self.get
            .saturating_add(self.put)
            .saturating_add(self.append)
            .saturating_add(self.delete)
    }
}

impl Default for OpMix {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// İstemci iş yükünün ayarları.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClientConfig {
    /// İstemci sayısı; kimlikler `1..=clients` (0, `RaftCluster::submit`'in iç oturumudur).
    pub clients: u64,
    /// Anahtar sayısı; anahtarlar `k0`, `k1`, ...
    pub keys: u64,
    /// Bir işlem bittikten sonra yenisine kadar geçen en kısa süre (tick).
    pub min_think: u64,
    /// ... ve en uzun süre (tick).
    pub max_think: u64,
    /// Cevapsız bir denemenin ardından yeniden denemeden önce beklenen süre (tick, ≥ 1).
    pub timeout: u64,
    /// Bir işlemin en fazla deneme sayısı (≥ 1): bir düğüme ulaşan denemeler sayılır (kapalı
    /// düğüme denk gelenler sayılmaz). Son deneme de cevapsız kalırsa istemci vazgeçer ve işlem
    /// belirsiz kalır.
    pub max_attempts: u32,
    /// Bir cevabın istemciye ulaşmadan kaybolma olasılığı, `[0, 1]`.
    pub reply_loss_prob: f64,
    /// İşlem karışımı.
    pub ops: OpMix,
}

impl Default for ClientConfig {
    /// 3 istemci, 4 anahtar, işlemler arasında 2..10 tick, iki seçim zaman aşımı kadar
    /// (`2 · 20` tick) zaman aşımı, 6 deneme, %10 cevap kaybı ve varsayılan işlem karışımı.
    fn default() -> Self {
        Self {
            clients: 3,
            keys: 4,
            min_think: 2,
            max_think: 10,
            timeout: 40,
            max_attempts: 6,
            reply_loss_prob: 0.1,
            ops: OpMix::DEFAULT,
        }
    }
}

impl ClientConfig {
    /// Ayarları doğrular.
    ///
    /// # Errors
    ///
    /// Sayılardan biri (işlem ağırlıklarının toplamı dahil) sıfırsa, düşünme aralığı ters ise ya
    /// da olasılık `[0, 1]` dışındaysa [`ConfigError`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (name, value) in [
            ("clients", self.clients),
            ("keys", self.keys),
            ("timeout", self.timeout),
            ("max_attempts", u64::from(self.max_attempts)),
            ("ops", self.ops.total()),
        ] {
            if value == 0 {
                return Err(ConfigError::ZeroSetting(name));
            }
        }
        if self.min_think > self.max_think {
            return Err(ConfigError::ThinkRange {
                min: self.min_think,
                max: self.max_think,
            });
        }
        let p = self.reply_loss_prob;
        if !(p.is_finite() && (0.0..=1.0).contains(&p)) {
            return Err(ConfigError::InvalidProbability {
                name: "reply_loss_prob",
                value: p,
            });
        }
        Ok(())
    }
}

/// İş yükünün sayaçları: bir taramanın gerçekten bir şeyleri sınadığını göstermek için.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientStats {
    /// Başlatılan işlemler.
    pub invoked: u64,
    /// Bir cevapla tamamlanan işlemler.
    pub completed: u64,
    /// Vazgeçilen işlemler (belirsiz kalır).
    pub abandoned: u64,
    /// Vazgeçilen ama her denemesi açıkça reddedildiği için kesin başarısız olan işlemler (geçmişe
    /// girmez).
    pub failed: u64,
    /// Yeniden denemeler (ilk denemeler hariç).
    pub retries: u64,
    /// Alınan `NotLeader` cevapları.
    pub not_leader: u64,
    /// Kaybolan cevaplar.
    pub lost_replies: u64,
    /// Sonucu oturumdan gelen tamamlanmalar: istek daha önce uygulanmıştı (§8).
    pub deduplicated: u64,
    /// Kapalı bir düğüme verilemeyen denemeler (deneme hakkından düşmez).
    pub refused: u64,
}

impl std::ops::AddAssign for ClientStats {
    /// Sayaçları toplar (ör. bir taramanın seed'leri üzerinden).
    fn add_assign(&mut self, other: Self) {
        self.invoked += other.invoked;
        self.completed += other.completed;
        self.abandoned += other.abandoned;
        self.failed += other.failed;
        self.retries += other.retries;
        self.not_leader += other.not_leader;
        self.lost_replies += other.lost_replies;
        self.deduplicated += other.deduplicated;
        self.refused += other.refused;
    }
}

/// Bir istemcinin durumu.
#[derive(Debug, Clone)]
enum ClientState {
    /// Boşta: `ready_at` anından itibaren yeni bir işlem başlatabilir.
    Idle { ready_at: u64 },
    /// Bir işlemin cevabını bekliyor.
    Waiting(Pending),
}

/// Cevabı beklenen işlem.
#[derive(Debug, Clone)]
struct Pending {
    seq: u64,
    command: KvCommand,
    /// İşlemin geçmişteki yeri.
    operation: usize,
    attempts: u32,
    /// Bu andan itibaren cevapsız deneme zaman aşımına uğramış sayılır.
    deadline: u64,
    /// Kabul edilmiş olabilecek denemeler, düğüm başına sayı: verilen her deneme sayılır, o
    /// düğümden gelen her `NotLeader` cevabı bir tane düşer. Hiçbiri kalmadıysa hiçbir deneme log'a
    /// girmemiştir. Sayı tutulur (küme değil): aynı düğüme iki deneme gitmiş, biri kabul edilip
    /// biri reddedilmiş olabilir.
    outstanding: BTreeMap<NodeId, u32>,
    /// Bir sonraki deneme bir `NotLeader` ipucunu izliyor (zaman aşımı değil).
    redirected: bool,
}

#[derive(Debug, Clone)]
struct Client {
    id: u64,
    next_seq: u64,
    /// Bildiği lider: son başarılı cevabın ya da son `NotLeader` ipucunun düğümü.
    leader: Option<NodeId>,
    state: ClientState,
    completed: u64,
}

/// Simüle istemcileri süren ve geçmişlerini kaydeden sürücü (bkz. modül belgesi).
///
/// Sürücü, `1..=clients` istemci kimliklerinin ve `k0`, `k1`, ... anahtarlarının tek sahibidir:
/// geçmiş, bu anahtarlara yapılan BÜTÜN işlemleri içermelidir. Aynı anahtarlara başka yoldan (ör.
/// `RaftCluster::submit`) yazmak geçmişi eksik bırakır ve kontrolcü linearizable bir koşuyu
/// reddedebilir.
#[derive(Debug, Clone)]
pub struct ClientDriver {
    config: ClientConfig,
    rng: ChaCha8Rng,
    clients: Vec<Client>,
    history: Vec<KvOperation>,
    /// Kesin başarısız işlemlerin geçmişteki yerleri: dışa verilen geçmişte yoktur.
    failed: BTreeSet<usize>,
    next_stamp: u64,
    issuing: bool,
    stats: ClientStats,
}

impl ClientDriver {
    /// İstemcileri kurar. Rastgelelik `master_seed`'in `Component::Workload` akışından gelir:
    /// kümeyle aynı ana seed verilirse koşu tek bir seed'le yeniden üretilir.
    ///
    /// # Errors
    ///
    /// Ayarlar geçersizse [`ConfigError`].
    pub fn new(master_seed: u64, config: ClientConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let clients = (1..=config.clients)
            .map(|id| Client {
                id,
                next_seq: 1,
                leader: None,
                state: ClientState::Idle { ready_at: 0 },
                completed: 0,
            })
            .collect();
        Ok(Self {
            config,
            rng: SeedTree::new(master_seed).rng_for(Component::Workload),
            clients,
            history: Vec::new(),
            failed: BTreeSet::new(),
            next_stamp: 1,
            issuing: true,
            stats: ClientStats::default(),
        })
    }

    /// Kümeyi `time` anına kadar tick tick ilerletir; her tick'te cevapları teslim eder ve
    /// istemcileri davrandırır.
    ///
    /// # Errors
    ///
    /// Kümede bir invariant çiğnenirse [`ClusterError::Violation`]; koşu o olayda durur.
    pub fn run_until(&mut self, cluster: &mut RaftCluster, time: u64) -> Result<(), ClusterError> {
        while cluster.now() < time {
            let now = cluster.now().saturating_add(1);
            cluster.run_until(now)?;
            self.tick(cluster, now)?;
        }
        Ok(())
    }

    /// Yeni işlem başlatmayı açar ya da kapatır. Kapalıyken bekleyen işlemler sürer (cevap ya da
    /// vazgeçme), yenileri başlamaz.
    pub fn set_issuing(&mut self, issuing: bool) {
        self.issuing = issuing;
    }

    /// Cevabı beklenen işlem var mı?
    #[must_use]
    pub fn busy(&self) -> bool {
        self.clients
            .iter()
            .any(|client| matches!(client.state, ClientState::Waiting(_)))
    }

    /// Kaydedilen geçmiş: işlemler başlatılma sırasıyla, linearizability kontrolcüsünün tipleriyle
    /// (bkz. `checker::check_kv`). Kesin başarısız işlemler yoktur.
    #[must_use]
    pub fn history(&self) -> Vec<KvOperation> {
        self.history
            .iter()
            .enumerate()
            .filter(|(index, _)| !self.failed.contains(index))
            .map(|(_, operation)| operation.clone())
            .collect()
    }

    /// Sayaçlar.
    #[must_use]
    pub fn stats(&self) -> ClientStats {
        self.stats
    }

    /// İstemci başına tamamlanan işlem sayıları, kimlik sırasıyla.
    #[must_use]
    pub fn completed_per_client(&self) -> Vec<u64> {
        self.clients.iter().map(|client| client.completed).collect()
    }

    /// Bir tick: cevaplar istemcilere ulaşır, sonra istemciler kimlik sırasıyla davranır.
    fn tick(&mut self, cluster: &mut RaftCluster, now: u64) -> Result<(), ClusterError> {
        for reply in cluster.take_client_replies() {
            // Her cevap için TAM OLARAK bir çekiliş: akışın çekiliş sayısı cevabın içeriğine
            // bağlı olmasın.
            if chance(&mut self.rng, self.config.reply_loss_prob) {
                self.stats.lost_replies += 1;
                continue;
            }
            self.deliver(reply, now);
        }
        for index in 0..self.clients.len() {
            self.act(cluster, index, now)?;
        }
        Ok(())
    }

    /// Bir cevabı istemcisine ulaştırır. Beklenmeyen cevaplar (vazgeçilmiş bir işlemin geç kalan
    /// cevabı, aynı isteğin ikinci cevabı) yok sayılır.
    fn deliver(&mut self, reply: ClientReply, now: u64) {
        let Some(client) = self
            .clients
            .iter_mut()
            .find(|client| client.id == reply.client)
        else {
            return;
        };
        let ClientState::Waiting(pending) = &mut client.state else {
            return;
        };
        if pending.seq != reply.seq {
            return;
        }
        match reply.outcome {
            ReplyOutcome::NotLeader { hint } => {
                self.stats.not_leader += 1;
                if let Some(count) = pending.outstanding.get_mut(&reply.node) {
                    *count = count.saturating_sub(1);
                }
                client.leader = hint;
                pending.redirected = true;
                // Reddedilen deneme hiç uygulanmayacak: zaman aşımını beklemeden yeniden dene.
                // Lider bilinmiyorsa (ör. seçim sürüyor) kısa bir süre bekle: hemen denemek,
                // seçim bitmeden bütün deneme hakkını tüketirdi.
                pending.deadline = if hint.is_some() {
                    now
                } else {
                    now.saturating_add(self.config.timeout / 4)
                };
            }
            ReplyOutcome::Done { result, duplicate } => {
                let stamp = self.next_stamp;
                self.next_stamp += 1;
                if let Some(operation) = self.history.get_mut(pending.operation) {
                    operation.ret = Some(stamp);
                    operation.output = Some(output_of(&result));
                }
                self.stats.completed += 1;
                if duplicate {
                    self.stats.deduplicated += 1;
                }
                client.completed += 1;
                client.leader = Some(reply.node);
                let think =
                    uniform_inclusive(&mut self.rng, self.config.min_think, self.config.max_think);
                client.state = ClientState::Idle {
                    ready_at: now.saturating_add(think),
                };
            }
        }
    }

    /// Bir istemcinin bu tick'teki davranışı: zamanı gelen yeni işlemi başlatır, zaman aşımına
    /// uğrayan denemeyi yeniler ya da deneme hakkı bitmişse vazgeçer.
    fn act(
        &mut self,
        cluster: &mut RaftCluster,
        index: usize,
        now: u64,
    ) -> Result<(), ClusterError> {
        let Some(client) = self.clients.get_mut(index) else {
            return Ok(());
        };
        match &mut client.state {
            ClientState::Idle { ready_at } => {
                if !self.issuing || now < *ready_at {
                    return Ok(());
                }
                let seq = client.next_seq;
                client.next_seq += 1;
                let command = random_command(
                    &mut self.rng,
                    self.config.keys,
                    self.config.ops,
                    client.id,
                    seq,
                );
                let stamp = self.next_stamp;
                self.next_stamp += 1;
                self.history.push(KvOperation {
                    client: client.id,
                    call: stamp,
                    ret: None,
                    input: input_of(&command),
                    output: None,
                });
                self.stats.invoked += 1;
                let mut pending = Pending {
                    seq,
                    command,
                    operation: self.history.len() - 1,
                    attempts: 0,
                    deadline: now,
                    outstanding: BTreeMap::new(),
                    redirected: false,
                };
                let result = attempt(
                    cluster,
                    &mut self.rng,
                    client,
                    &mut pending,
                    now,
                    self.config.timeout,
                    &mut self.stats,
                );
                client.state = ClientState::Waiting(pending);
                result
            }
            ClientState::Waiting(pending) => {
                if now < pending.deadline {
                    return Ok(());
                }
                if pending.attempts >= self.config.max_attempts {
                    // Vazgeç. Kabul edilmiş olabilecek bir deneme kaldıysa işlem geçmişte belirsiz
                    // kalır (dönüşü yok); kalmadıysa kesin başarısızdır ve geçmişten çıkar.
                    if pending.outstanding.values().all(|&count| count == 0) {
                        self.failed.insert(pending.operation);
                        self.stats.failed += 1;
                    } else {
                        self.stats.abandoned += 1;
                    }
                    let think = uniform_inclusive(
                        &mut self.rng,
                        self.config.min_think,
                        self.config.max_think,
                    );
                    client.state = ClientState::Idle {
                        ready_at: now.saturating_add(think),
                    };
                    return Ok(());
                }
                self.stats.retries += 1;
                let mut pending = pending.clone();
                // Zaman aşımına uğrayan denemenin hedefi artık lider olmayabilir (çökmüş ya da
                // azınlıkta kalmış). Ona yapışmak deneme hakkını tüketirdi: yeniden deneme
                // rastgele bir düğüme gider, o da lider değilse ipucuyla doğru lidere yönlendirir.
                // `NotLeader` ipucunu izleyen deneme ise ipucuna gider.
                if !std::mem::take(&mut pending.redirected) {
                    client.leader = None;
                }
                let result = attempt(
                    cluster,
                    &mut self.rng,
                    client,
                    &mut pending,
                    now,
                    self.config.timeout,
                    &mut self.stats,
                );
                client.state = ClientState::Waiting(pending);
                result
            }
        }
    }
}

/// Bir denemeyi bilinen lidere (yoksa rastgele bir düğüme) verir. Rastgele düğüm her denemede
/// çekilir (lider biliniyor olsa bile): akışın çekiliş sayısı istemcinin bildiklerine bağlı
/// olmasın.
fn attempt(
    cluster: &mut RaftCluster,
    rng: &mut ChaCha8Rng,
    client: &mut Client,
    pending: &mut Pending,
    now: u64,
    timeout: u64,
    stats: &mut ClientStats,
) -> Result<(), ClusterError> {
    let nodes: Vec<NodeId> = cluster.node_ids().collect();
    let upper = u64::try_from(nodes.len().saturating_sub(1)).unwrap_or(0);
    let drawn = usize::try_from(uniform_inclusive(rng, 0, upper)).unwrap_or(0);
    let Some(target) = client.leader.or_else(|| nodes.get(drawn).copied()) else {
        return Ok(());
    };
    let request = KvRequest {
        client: client.id,
        seq: pending.seq,
        command: pending.command.clone(),
    };
    match cluster.submit_request(target, request) {
        Ok(()) => {
            pending.attempts += 1;
            pending.deadline = now.saturating_add(timeout);
            *pending.outstanding.entry(target).or_default() += 1;
            Ok(())
        }
        Err(ClusterError::Lifecycle(LifecycleError::Down(_))) => {
            // Bağlantı reddedildi: düğüm kapalı. İstek hiçbir sunucuya ulaşmadı, deneme hakkı
            // harcanmaz. Bildiği lider de geçersiz; bir sonraki tick'te başka bir düğüm denenir.
            stats.refused += 1;
            client.leader = None;
            pending.deadline = now.saturating_add(1);
            Ok(())
        }
        Err(other) => Err(other),
    }
}

/// Rastgele bir işlem: tür, karışımın ağırlıklarıyla seçilir. Yazılan ve eklenen değerler işleme
/// özgüdür (`"istemci.sıra;"`): değerler ayırt edilebilir olunca kontrolcünün araması hızlanır ve
/// hatalar daha kesin görünür. Her işlem TAM OLARAK iki çekiliş yapar (anahtar ve tür).
fn random_command(rng: &mut ChaCha8Rng, keys: u64, ops: OpMix, client: u64, seq: u64) -> KvCommand {
    let key = format!("k{}", uniform_inclusive(rng, 0, keys.saturating_sub(1))).into_bytes();
    let token = format!("{client}.{seq};").into_bytes();
    let draw = uniform_inclusive(rng, 0, ops.total().saturating_sub(1));
    if draw < ops.get {
        KvCommand::Get { key }
    } else if draw < ops.get.saturating_add(ops.put) {
        KvCommand::Put { key, value: token }
    } else if draw < ops.get.saturating_add(ops.put).saturating_add(ops.append) {
        KvCommand::Append { key, value: token }
    } else {
        KvCommand::Delete { key }
    }
}

/// Komutun kontrolcüdeki karşılığı.
fn input_of(command: &KvCommand) -> KvInput {
    match command.clone() {
        KvCommand::Put { key, value } => KvInput::Put { key, value },
        KvCommand::Delete { key } => KvInput::Delete { key },
        KvCommand::Get { key } => KvInput::Get { key },
        KvCommand::Append { key, value } => KvInput::Append { key, value },
    }
}

/// Sonucun kontrolcüdeki karşılığı.
fn output_of(result: &KvResult) -> KvOutput {
    match result {
        KvResult::Ok => KvOutput::Ok,
        KvResult::Value(value) => KvOutput::Value(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientConfig, ClientDriver, OpMix};
    use crate::error::ConfigError;

    // Geçersiz ayarlar reddedilir; varsayılanlar geçerlidir.
    #[test]
    fn client_configs_are_validated() {
        assert_eq!(ClientConfig::default().validate(), Ok(()));
        let zero = ClientConfig {
            clients: 0,
            ..ClientConfig::default()
        };
        assert_eq!(zero.validate(), Err(ConfigError::ZeroSetting("clients")));
        let range = ClientConfig {
            min_think: 5,
            max_think: 1,
            ..ClientConfig::default()
        };
        assert_eq!(
            range.validate(),
            Err(ConfigError::ThinkRange { min: 5, max: 1 })
        );
        let loss = ClientConfig {
            reply_loss_prob: 1.5,
            ..ClientConfig::default()
        };
        assert!(matches!(
            loss.validate(),
            Err(ConfigError::InvalidProbability {
                name: "reply_loss_prob",
                ..
            })
        ));
        let no_ops = ClientConfig {
            ops: OpMix {
                get: 0,
                put: 0,
                append: 0,
                delete: 0,
            },
            ..ClientConfig::default()
        };
        assert_eq!(no_ops.validate(), Err(ConfigError::ZeroSetting("ops")));
        assert!(ClientDriver::new(1, zero).is_err());
    }
}
