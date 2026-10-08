//! İstemci arayüzü testleri (Faz 4): `NotLeader` cevabı ve ipucu, cevabı kaybolan bir isteğin
//! yeniden denenip bir kez uygulanması (§8), istemci iş yükünün tekrarlanabilirliği ve
//! simülatörden gelen gerçek geçmişlerin linearizability denetimi.
//!
//! Senaryolu testler kümeyi doğrudan sürer ve geçmişi elle kaydeder: her olayın (çağrı, dönüş)
//! damgası tek bir artan sayaçtan gelir, böylece gerçek zaman sırası açıktır.

use sim::{
    ClientConfig, ClientDriver, ClientReply, ClusterConfig, DiskConfig, KvCommand, KvInput,
    KvOperation, KvOutput, KvRequest, KvResult, LogIndex, NetworkConfig, NodeId, RaftCluster,
    ReplyOutcome, check_kv,
};

/// Seçim zaman aşımı tabanı (`RaftConfig::default()`).
const T: u64 = 20;

/// Başarısız bir seed'in bağlamı: seed ile tek satırlık yeniden üretme komutu.
fn context(test: &str, seed: u64) -> String {
    format!("seed {seed} [reproduce: cargo test -p sim --test clients -- {test} --exact]")
}

/// Gecikmesiz diskli, güvenilir ağlı (gecikme 1) 3 düğümlü bir küme.
fn quiet_cluster(seed: u64) -> RaftCluster {
    let config = ClusterConfig {
        disk: DiskConfig::instant(),
        ..ClusterConfig::new(3, NetworkConfig::reliable(1))
    };
    RaftCluster::new(seed, config).expect("valid config")
}

/// Bir tick ilerler; bir invariant çiğnenirse bağlamla birlikte bildirir.
fn tick(cluster: &mut RaftCluster, context: &str) {
    let next = cluster.now() + 1;
    if let Err(error) = cluster.run_until(next) {
        panic!("{context}: {error}");
    }
}

/// Koşul sağlanana kadar birer tick ilerler; `max_ticks` içinde sağlanmazsa testi düşürür.
fn wait_for(
    cluster: &mut RaftCluster,
    context: &str,
    max_ticks: u64,
    what: &str,
    mut condition: impl FnMut(&mut RaftCluster) -> bool,
) {
    let deadline = cluster.now() + max_ticks;
    while !condition(cluster) {
        assert!(
            cluster.now() < deadline,
            "{context}: {what} did not happen within {max_ticks} ticks"
        );
        tick(cluster, context);
    }
}

/// Tek bir ayaktaki lider olana ve no-op'u bütün ayaktaki düğümlerde commit edilene kadar bekler.
fn settled_leader(cluster: &mut RaftCluster, context: &str) -> NodeId {
    wait_for(cluster, context, 20 * T, "a settled leader", |c| {
        let leaders = c.leaders();
        leaders.len() == 1
            && c.node_ids().filter(|&id| c.is_up(id)).all(|id| {
                c.node(id)
                    .is_some_and(|node| node.commit_index() >= LogIndex(1))
            })
    });
    cluster.leaders()[0].0
}

/// Elle kaydedilen bir geçmiş: damgalar tek bir sayaçtan gelir.
#[derive(Default)]
struct History {
    operations: Vec<KvOperation>,
    stamp: u64,
}

impl History {
    fn next_stamp(&mut self) -> u64 {
        self.stamp += 1;
        self.stamp
    }

    /// Bir işlemin çağrısını kaydeder; işlemin geçmişteki yerini döndürür.
    fn call(&mut self, client: u64, command: &KvCommand) -> usize {
        let call = self.next_stamp();
        let input = match command.clone() {
            KvCommand::Put { key, value } => KvInput::Put { key, value },
            KvCommand::Delete { key } => KvInput::Delete { key },
            KvCommand::Get { key } => KvInput::Get { key },
            KvCommand::Append { key, value } => KvInput::Append { key, value },
        };
        self.operations.push(KvOperation {
            client,
            call,
            ret: None,
            input,
            output: None,
        });
        self.operations.len() - 1
    }

    /// Bir işlemin dönüşünü kaydeder.
    fn ret(&mut self, operation: usize, result: &KvResult) {
        let stamp = self.next_stamp();
        let output = match result {
            KvResult::Ok => KvOutput::Ok,
            KvResult::Value(value) => KvOutput::Value(value.clone()),
        };
        self.operations[operation].ret = Some(stamp);
        self.operations[operation].output = Some(output);
    }
}

/// `(client, seq)` için bir cevap gelene kadar bekler ve onu döndürür.
fn await_reply(cluster: &mut RaftCluster, context: &str, client: u64, seq: u64) -> ClientReply {
    let mut found = None;
    wait_for(cluster, context, 10 * T, "a reply", |c| {
        if found.is_none() {
            found = c
                .take_client_replies()
                .into_iter()
                .find(|reply| reply.client == client && reply.seq == seq);
        }
        found.is_some()
    });
    found.expect("the reply arrived")
}

fn append(key: &str, value: &str) -> KvCommand {
    KvCommand::Append {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

fn get(key: &str) -> KvCommand {
    KvCommand::Get {
        key: key.as_bytes().to_vec(),
    }
}

// §8: lider olmayan düğüm isteği log'a eklemez ve bildiği lideri söyler; istemci isteği oraya
// verince sonuç, komut commit edilip uygulandığında liderden gelir. Takipçinin KV tablosu da
// sonunda aynı değeri taşır (istek ona da çoğaltıldı ama cevabı lider verdi).
#[test]
fn a_follower_points_the_client_to_the_leader() {
    const TEST: &str = "a_follower_points_the_client_to_the_leader";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let mut cluster = quiet_cluster(seed);
        let leader = settled_leader(&mut cluster, &context);
        let follower = cluster
            .node_ids()
            .find(|&id| id != leader)
            .expect("a follower");
        let request = KvRequest {
            client: 1,
            seq: 1,
            command: append("k", "x"),
        };
        if let Err(error) = cluster.submit_request(follower, request.clone()) {
            panic!("{context}: {error}");
        }
        let reply = await_reply(&mut cluster, &context, 1, 1);
        assert_eq!(
            reply.outcome,
            ReplyOutcome::NotLeader { hint: Some(leader) },
            "{context}"
        );
        assert_eq!(reply.node, follower, "{context}");
        assert!(
            cluster
                .node(follower)
                .is_some_and(|node| node.log().iter().all(|entry| entry.command.is_noop())),
            "{context}: a rejected request is not appended"
        );

        if let Err(error) = cluster.submit_request(leader, request) {
            panic!("{context}: {error}");
        }
        let reply = await_reply(&mut cluster, &context, 1, 1);
        assert_eq!(
            reply.outcome,
            ReplyOutcome::Done {
                result: KvResult::Ok,
                duplicate: false
            },
            "{context}"
        );
        assert_eq!(reply.node, leader, "{context}");
        wait_for(
            &mut cluster,
            &context,
            10 * T,
            "the follower applies",
            |c| c.kv(follower).and_then(|kv| kv.get(b"k")) == Some(&b"x"[..]),
        );
    }
}

// Cevabı kaybolan istek bir kez uygulanır (§8). İstemci lidere bir ekleme verir; girdi
// takipçilere ulaşır ama lider onayları almadan çöker ve cevap hiç gelmez. Yeni lider girdiyi
// taşır ve no-op'uyla commit eder: ekleme uygulanmıştır. İstemci aynı isteği aynı `(client, seq)`
// ile yeni lidere yeniden verir; ikinci kopya log'a girer ama durum makinesi onu yeniden
// UYGULAMAZ, ilk sonucu döndürür. Sonraki okuma eklemeyi bir kez görür.
//
// Geçmiş linearizable'dır. Tekilleştirme kapatılırsa ekleme iki kez uygulanır, okuma "xx" görür
// ve kontrolcü geçmişi reddeder: Faz 4'ün "bilerek boz" hedefi budur.
#[test]
fn a_retried_request_is_applied_once() {
    const TEST: &str = "a_retried_request_is_applied_once";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let mut cluster = quiet_cluster(seed);
        let first_leader = settled_leader(&mut cluster, &context);
        let mut history = History::default();

        let command = append("k", "x");
        let operation = history.call(1, &command);
        let request = KvRequest {
            client: 1,
            seq: 1,
            command,
        };
        if let Err(error) = cluster.submit_request(first_leader, request.clone()) {
            panic!("{context}: {error}");
        }
        // Girdi takipçilere ulaşır; onayları yoldayken lider çöker.
        tick(&mut cluster, &context);
        let followers: Vec<NodeId> = cluster
            .node_ids()
            .filter(|&id| id != first_leader)
            .collect();
        assert!(
            followers
                .iter()
                .all(|&id| cluster.node(id).is_some_and(|node| node.log().len() == 2)),
            "{context}: the entry must reach the followers before the crash"
        );
        if let Err(error) = cluster.crash(first_leader) {
            panic!("{context}: {error}");
        }
        assert!(cluster.take_client_replies().is_empty(), "{context}");

        // Yeni lider seçilir ve no-op'uyla eski girdiyi commit eder: ekleme uygulanır.
        let new_leader = settled_leader(&mut cluster, &context);
        wait_for(&mut cluster, &context, 10 * T, "the append applied", |c| {
            c.kv(new_leader).and_then(|kv| kv.get(b"k")) == Some(&b"x"[..])
        });

        // İstemci aynı isteği yeni lidere yeniden verir.
        if let Err(error) = cluster.submit_request(new_leader, request) {
            panic!("{context}: {error}");
        }
        let reply = await_reply(&mut cluster, &context, 1, 1);
        let ReplyOutcome::Done { result, duplicate } = reply.outcome else {
            panic!("{context}: the retry must complete: {:?}", reply.outcome);
        };
        history.ret(operation, &result);

        // Başka bir istemci okur.
        let read = get("k");
        let reading = history.call(2, &read);
        let request = KvRequest {
            client: 2,
            seq: 1,
            command: read,
        };
        if let Err(error) = cluster.submit_request(new_leader, request) {
            panic!("{context}: {error}");
        }
        let reply = await_reply(&mut cluster, &context, 2, 1);
        let ReplyOutcome::Done { result, .. } = reply.outcome else {
            panic!("{context}: the read must complete: {:?}", reply.outcome);
        };
        history.ret(reading, &result);

        assert_eq!(
            check_kv(&history.operations),
            Ok(()),
            "{context}: {:?}",
            history.operations
        );
        assert!(duplicate, "{context}: the retry is served from the session");
        assert_eq!(
            result,
            KvResult::Value(Some(b"x".to_vec())),
            "{context}: applied once"
        );
    }
}

// Çökme, düğüme açık istekleri koparır. Lider bir isteği kabul eder ve onu takipçilere çoğaltır,
// ama istek commit edilip uygulanmadan çöker. Yeni lider isteği commit eder. Eski lider yeniden
// başlayıp log'unu baştan uyguladığında isteği de uygular; ama o bağlantı artık yoktur, o
// düğümden hiç cevap gelmez. (İstemci zaman aşımında başka bir düğümde yeniden dener; sonucu
// tekilleştirme oradan verir.)
#[test]
fn a_crash_drops_the_requests_held_by_the_node() {
    const TEST: &str = "a_crash_drops_the_requests_held_by_the_node";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let mut cluster = quiet_cluster(seed);
        let leader = settled_leader(&mut cluster, &context);
        let request = KvRequest {
            client: 1,
            seq: 1,
            command: append("k", "x"),
        };
        if let Err(error) = cluster.submit_request(leader, request) {
            panic!("{context}: {error}");
        }
        tick(&mut cluster, &context);
        if let Err(error) = cluster.crash(leader) {
            panic!("{context}: {error}");
        }
        let new_leader = settled_leader(&mut cluster, &context);
        wait_for(&mut cluster, &context, 10 * T, "the request applied", |c| {
            c.kv(new_leader).and_then(|kv| kv.get(b"k")) == Some(&b"x"[..])
        });
        if let Err(error) = cluster.restart(leader) {
            panic!("{context}: {error}");
        }
        wait_for(
            &mut cluster,
            &context,
            10 * T,
            "the old leader re-applies",
            |c| c.kv(leader).and_then(|kv| kv.get(b"k")) == Some(&b"x"[..]),
        );
        let replies = cluster.take_client_replies();
        assert!(replies.is_empty(), "{context}: {replies:?}");
    }
}

// Belirsiz işlem simülatörde. İstemci lidere bir ekleme verir; lider cevap veremeden çöker ve
// istemci vazgeçer: işlemin dönüşü yoktur. İki varyant: lider girdiyi takipçilere ulaştıramadan
// çöker (girdi kaybolur: lider istekten önce yalıtılır, AppendEntries gönderim anında düşer;
// çökenin yoldaki mesajları yine teslim edildiği için yalnızca çökmek yetmezdi) ya da ulaştırdıktan
// sonra çöker (yeni lider onu no-op'uyla commit eder).
// Sonraki okuma ilkinde hiçbir şey, ikincisinde eklemeyi görür. İki geçmiş de linearizable'dır:
// belirsiz bir işlemin etkisi olmuş da olabilir, olmamış da. İki sonucun da gerçekten üretildiği
// ayrıca doğrulanır.
#[test]
fn an_unanswered_operation_is_indeterminate() {
    const TEST: &str = "an_unanswered_operation_is_indeterminate";
    for seed in 0..10 {
        let context = context(TEST, seed);
        let mut reads = Vec::new();
        for replicated in [false, true] {
            let context = format!("{context}, replicated: {replicated}");
            let mut cluster = quiet_cluster(seed);
            let leader = settled_leader(&mut cluster, &context);
            let others: Vec<NodeId> = cluster.node_ids().filter(|&id| id != leader).collect();
            if !replicated && let Err(error) = cluster.partition(&[&[leader], &others]) {
                panic!("{context}: {error}");
            }
            let mut history = History::default();
            let command = append("k", "x");
            let _ = history.call(1, &command);
            let request = KvRequest {
                client: 1,
                seq: 1,
                command,
            };
            if let Err(error) = cluster.submit_request(leader, request) {
                panic!("{context}: {error}");
            }
            if replicated {
                tick(&mut cluster, &context);
            }
            if let Err(error) = cluster.crash(leader) {
                panic!("{context}: {error}");
            }
            cluster.heal();
            let new_leader = settled_leader(&mut cluster, &context);
            let read = get("k");
            let reading = history.call(2, &read);
            let request = KvRequest {
                client: 2,
                seq: 1,
                command: read,
            };
            if let Err(error) = cluster.submit_request(new_leader, request) {
                panic!("{context}: {error}");
            }
            let reply = await_reply(&mut cluster, &context, 2, 1);
            let ReplyOutcome::Done { result, .. } = reply.outcome else {
                panic!("{context}: the read must complete: {:?}", reply.outcome);
            };
            history.ret(reading, &result);
            assert_eq!(check_kv(&history.operations), Ok(()), "{context}");
            reads.push(result);
        }
        assert_eq!(
            reads,
            vec![KvResult::Value(None), KvResult::Value(Some(b"x".to_vec()))],
            "{context}: both outcomes of the indeterminate operation must occur"
        );
    }
}

/// İstemci iş yüküyle bir koşu: kayıpsız ama gecikmeli bir ağ, %20 cevap kaybı. Ortada lider bir
/// kez çöker ve bir süre sonra döner.
fn workload_run(seed: u64) -> (RaftCluster, ClientDriver) {
    let network = NetworkConfig {
        drop_prob: 0.0,
        duplicate_prob: 0.0,
        min_delay: 1,
        max_delay: 3,
    };
    let mut cluster = RaftCluster::new(seed, ClusterConfig::new(3, network)).expect("valid config");
    let config = ClientConfig {
        reply_loss_prob: 0.2,
        ..ClientConfig::default()
    };
    let mut driver = ClientDriver::new(seed, config).expect("valid config");
    driver.run_until(&mut cluster, 300).expect("no violation");
    let leader = cluster.leaders().first().map(|&(id, _)| id);
    if let Some(leader) = leader {
        cluster.crash(leader).expect("the leader is up");
    }
    driver.run_until(&mut cluster, 450).expect("no violation");
    if let Some(leader) = leader {
        cluster.restart(leader).expect("the node is down");
    }
    driver.run_until(&mut cluster, 800).expect("no violation");
    (cluster, driver)
}

// İstemci iş yükü deterministiktir: aynı seed aynı geçmişi ve aynı trace'i üretir; farklı seed'ler
// farklı geçmişler üretir.
#[test]
fn the_workload_is_reproducible() {
    let (first_cluster, first) = workload_run(5);
    let (second_cluster, second) = workload_run(5);
    assert_eq!(first.history(), second.history());
    assert_eq!(first.stats(), second.stats());
    assert_eq!(
        first_cluster.sim().trace_hash(),
        second_cluster.sim().trace_hash()
    );
    let (_, other) = workload_run(6);
    assert_ne!(first.history(), other.history());
}

// Gerçek geçmişler: istemci iş yüküyle koşan kümelerin geçmişi linearizable'dır. Tarama boş
// geçmemeli: işlemler tamamlanır, cevaplar kaybolur, istekler yeniden denenir ve bir kısmı
// oturumdan (tekilleştirme) cevaplanır; seçimlerde `NotLeader` cevapları görülür, lider çökünce
// istekler kapalı düğüme takılır ve bazı işlemlerden vazgeçilir (belirsiz işlemler).
#[test]
fn workload_histories_are_linearizable() {
    const TEST: &str = "workload_histories_are_linearizable";
    let mut totals = sim::ClientStats::default();
    for seed in 0..20 {
        let context = context(TEST, seed);
        let (cluster, driver) = workload_run(seed);
        if let Err(error) = check_kv(&driver.history()) {
            panic!("{context}: {error}");
        }
        assert!(
            !cluster.leaders().is_empty(),
            "{context}: the cluster recovers a leader"
        );
        totals += driver.stats();
    }
    // Eşikler ölçülen değerlerin (1760 işlemden 1678'i tamamlandı, 27'si belirsiz, 7'si kesin
    // başarısız; 1369 yeniden deneme, 583 kayıp cevap, 318 tekilleştirilmiş cevap, 594 NotLeader,
    // kapalı düğüme 231 deneme) çok altındadır.
    assert!(totals.completed * 10 >= totals.invoked * 8, "{totals:?}");
    assert!(totals.abandoned >= 10, "{totals:?}");
    assert!(totals.lost_replies >= 200, "{totals:?}");
    assert!(totals.retries >= 300, "{totals:?}");
    assert!(totals.deduplicated >= 100, "{totals:?}");
    assert!(totals.not_leader >= 100, "{totals:?}");
    assert!(totals.refused >= 50, "{totals:?}");
}
