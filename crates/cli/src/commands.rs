//! Alt komutların gövdeleri.
//!
//! Çıktı her zaman verilen yazıcıya gider (`main`'de stdout): komutlar testlerde de aynı biçimde
//! koşar ve bir yazma hatası (ör. çıktı `head`'e bağlanmışken kapanan boru) panik değil hata
//! olarak döner.
//!
//! Her komut önce kararını verir (koşular biter, başarısız seed'ler bellidir), sonra raporunu
//! yazar. Okuyan taraf boruyu erken kapatırsa raporun geri kalanı yazılamaz ama karar değişmez:
//! çıkış kodu yine koşuların sonucudur (bkz. `finish`).

use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::thread;

use sim::{Fault, RaftCluster, Run, RunError, Scenario, ScenarioConfig, Shrunk, run, shrink};

use crate::args::{Cli, Command, FaultList, FuzzArgs, Profile, ReplayArgs};
use crate::{chart, svg};

/// Küçültmede denenecek en fazla koşu.
const SHRINK_BUDGET: usize = 400;

/// `--shrink` ile küçültülen en fazla başarısız seed (seed sırasıyla ilkleri). Neden sınır: çok
/// seed'de başarısız olan bir hatada her küçültme yüzlerce koşu harcar ve rapor hepsi bitene kadar
/// basılmazdı. İlk birkaç küçük senaryo hatayı anlamaya yeter; seçim seed sırasıyla yapıldığı için
/// deterministiktir.
const SHRINK_LIMIT: usize = 10;

/// Bir komutun kendisinin başarısızlığı. Bir koşunun başarısızlığı bir hata değil, komutun
/// sonucudur (çıkış kodu 1).
#[derive(Debug)]
pub enum CommandError {
    /// Argümanlar ayrıştırıldı ama senaryoyla uyuşmuyor (ör. var olmayan bir hata sırası). `main`
    /// bunu clap'in argüman hataları gibi stderr'e yazar ve 2 ile çıkar.
    Usage(String),
    /// Rapor yazılamadı (kapanan boru dışında bir nedenle; bkz. `finish`).
    Write(io::Error),
    /// Koşuları yürütecek iş parçacığı açılamadı.
    Spawn(io::Error),
    /// Zaman çizelgesi (`--svg`) dosyaya yazılamadı.
    Svg {
        /// Dosya.
        path: PathBuf,
        /// Neden.
        error: io::Error,
    },
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Usage(message) => formatter.write_str(message),
            CommandError::Write(error) => write!(formatter, "cannot write the output: {error}"),
            CommandError::Spawn(error) => {
                write!(formatter, "cannot start a worker thread: {error}")
            }
            CommandError::Svg { path, error } => {
                write!(formatter, "cannot write {}: {error}", path.display())
            }
        }
    }
}

/// Komutu yürütür ve çıkış kodunu döndürür: 0 başarı, 1 başarısız bir koşu.
///
/// # Errors
///
/// Argümanlar senaryoyla uyuşmazsa, rapor ya da çizim dosyası (`replay --svg`) yazılamazsa ya da iş
/// parçacığı açılamazsa [`CommandError`].
pub fn execute(cli: Cli, out: &mut impl Write) -> Result<ExitCode, CommandError> {
    match cli.command {
        Command::Fuzz(args) => fuzz(&args, out),
        Command::Replay(args) => replay(&args, out),
    }
}

/// Raporu yazmanın sonucunu kararla birleştirir. Okuyan taraf boruyu erken kapattıysa (ör.
/// `raftsim fuzz ... | head`) raporun kalanı yazılamaz ama karar çoktan verildi: başarısız bir
/// seed varken 0 ile çıkmak, `pipefail` kullanan bir betikte hatayı gizlerdi.
fn finish(verdict: ExitCode, written: io::Result<()>) -> Result<ExitCode, CommandError> {
    match written {
        Ok(()) => Ok(verdict),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(verdict),
        Err(error) => Err(CommandError::Write(error)),
    }
}

/// Koşuların kararı: hepsi geçtiyse 0, en az biri başarısızsa 1.
fn verdict(failed: bool) -> ExitCode {
    if failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

/// Bu derlemenin mutasyon özelliği, yeniden üretme komutu için (varsayılan derlemede boş). Mutant
/// bir derlemenin bulduğu seed, ancak aynı özellikle derlenince aynı hatayı verir.
fn features() -> String {
    sim::ENABLED_MUTATION
        .map(|feature| format!(" --features {feature}"))
        .unwrap_or_default()
}

/// Tek satırlık yeniden üretme komutu. Mutant bir derlemede (bkz. `sim::ENABLED_MUTATION`)
/// mutasyonun özelliği de eklenir: seed ancak aynı derlemede aynı hatayı verir.
fn reproduce(seed: u64, profile: Profile) -> String {
    format!(
        "cargo run -p cli{} -- replay --seed {seed}{}",
        features(),
        profile.flag()
    )
}

/// Küçültülmüş bir senaryonun yeniden üretme komutu. Bütün hatalar elendiyse `--faults none`
/// yazılır (bkz. `FaultList`); basılan her komut `replay`'in ayrıştırabileceği bir komuttur
/// (testli).
fn reproduce_shrunk(seed: u64, profile: Profile, kept: &[usize], horizon: u64) -> String {
    format!(
        "{} --faults {} --horizon {horizon}",
        reproduce(seed, profile),
        FaultList(kept.to_vec())
    )
}

fn fuzz(args: &FuzzArgs, out: &mut impl Write) -> Result<ExitCode, CommandError> {
    let config = args.profile.config();
    let (passed, failures) =
        fuzz_seeds(args.seeds.clone(), config, args.threads).map_err(CommandError::Spawn)?;
    let shrunk = if args.shrink {
        let first = &failures[..failures.len().min(SHRINK_LIMIT)];
        shrink_failures(first, config, args.threads).map_err(CommandError::Spawn)?
    } else {
        Vec::new()
    };
    let written = write_fuzz_report(args, passed, &failures, &shrunk, out);
    finish(verdict(!failures.is_empty()), written)
}

/// Bir küçültmenin sonucu ve özgün senaryonun hata sayısı (raporda "N hatadan M'ine").
struct ShrinkReport {
    faults: usize,
    shrunk: Shrunk,
}

fn write_fuzz_report(
    args: &FuzzArgs,
    passed: u64,
    failures: &[(u64, RunError)],
    shrunk: &[ShrinkReport],
    out: &mut impl Write,
) -> io::Result<()> {
    for (position, (seed, error)) in failures.iter().enumerate() {
        writeln!(out, "seed {seed}: FAILED ({}): {error}", error.signature())?;
        writeln!(out, "  reproduce: {}", reproduce(*seed, args.profile))?;
        let Some(ShrinkReport { faults, shrunk }) = shrunk.get(position) else {
            continue;
        };
        writeln!(
            out,
            "  shrunk to {} of {faults} faults (fault phase {} ticks, {} runs{}): {}",
            shrunk.kept.len(),
            shrunk.scenario.config.horizon,
            shrunk.runs,
            // Bütçe tükendiyse sonuç daha da küçültülebilir; bunu söylemeden basmak, sonucu
            // olduğundan "minimal" gösterirdi.
            if shrunk.complete {
                ""
            } else {
                ", budget exhausted"
            },
            shrunk.error
        )?;
        writeln!(
            out,
            "  reproduce the shrunk scenario: {}",
            reproduce_shrunk(
                *seed,
                args.profile,
                &shrunk.kept,
                shrunk.scenario.config.horizon
            )
        )?;
    }
    if args.shrink && failures.len() > shrunk.len() {
        writeln!(
            out,
            "shrank the first {} of {} failing seeds",
            shrunk.len(),
            failures.len()
        )?;
    }
    let total = args.seeds.end.saturating_sub(args.seeds.start);
    writeln!(
        out,
        "fuzzed {total} seed{} ({}..{}): {passed} passed, {} failed",
        if total == 1 { "" } else { "s" },
        args.seeds.start,
        args.seeds.end,
        failures.len()
    )
}

/// Seed'leri iş parçacıklarına dağıtır. Her koşu tek bir iş parçacığında ve yalnızca seed'ine
/// bağlı olarak koşar; başarısızlıklar seed sırasıyla döner. Böylece rapor, iş parçacığı
/// sayısından ve zamanlamadan bağımsızdır.
///
/// Seed'ler aralıktan tembel üretilir ve yalnızca başarısızlıklar saklanır (geçenler sayılır):
/// bellek, aralığın boyuyla değil başarısız seed sayısıyla büyür. Çok büyük bir aralık da önce bir
/// listeye toplanmaya çalışılmaz.
fn fuzz_seeds(
    seeds: Range<u64>,
    config: ScenarioConfig,
    threads: u16,
) -> io::Result<(u64, Vec<(u64, RunError)>)> {
    let passed = AtomicU64::new(0);
    let failures = Mutex::new(Vec::new());
    parallel_for(seeds.end.saturating_sub(seeds.start), threads, |index| {
        // `index < end - start` olduğu için toplam taşmaz.
        let seed = seeds.start + index;
        match run(&Scenario::generate(seed, config)).outcome {
            Ok(_) => {
                passed.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => lock(&failures).push((seed, error)),
        }
    })?;
    let mut failures = failures
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    failures.sort_by_key(|(seed, _)| *seed);
    Ok((passed.into_inner(), failures))
}

/// Başarısız her senaryoyu küçültür. Küçültmeler de iş parçacıklarına dağıtılır; her biri kendi
/// içinde sıralı ve deterministiktir, sonuçlar başarısızlıkların sırasıyla döner.
fn shrink_failures(
    failures: &[(u64, RunError)],
    config: ScenarioConfig,
    threads: u16,
) -> io::Result<Vec<ShrinkReport>> {
    let reports = Mutex::new(Vec::with_capacity(failures.len()));
    let count = u64::try_from(failures.len()).unwrap_or(u64::MAX);
    parallel_for(count, threads, |index| {
        let Some((seed, error)) = usize::try_from(index)
            .ok()
            .and_then(|index| failures.get(index))
        else {
            return;
        };
        let scenario = Scenario::generate(*seed, config);
        let shrunk = shrink(&scenario, error, SHRINK_BUDGET);
        let report = ShrinkReport {
            faults: scenario.faults.len(),
            shrunk,
        };
        lock(&reports).push((index, report));
    })?;
    let mut reports = reports.into_inner().unwrap_or_else(PoisonError::into_inner);
    reports.sort_by_key(|(index, _)| *index);
    Ok(reports.into_iter().map(|(_, report)| report).collect())
}

/// `0..count` sıralarını iş parçacıklarına dağıtır: her iş parçacığı sıradaki işi ortak bir
/// sayaçtan alır. Hangi işin hangi iş parçacığında ve hangi sırayla koştuğu belirsizdir; bu yüzden
/// `work` sonucunu sırasıyla birlikte saklar ve çağıran sonuçları sıralar.
///
/// İş parçacığı sayısı iş sayısını aşmaz: boşta bekleyecek iş parçacığı açılmaz (`--threads 60000`
/// ile tek bir seed koşturmak 60000 iş parçacığı açmaz). İlk iş parçacığı bile açılamazsa G/Ç
/// hatası döner; sonrakiler açılamazsa iş, açılabilenlerle biter.
fn parallel_for(count: u64, threads: u16, work: impl Fn(u64) + Sync) -> io::Result<()> {
    let next = AtomicU64::new(0);
    let workers = u64::from(threads).clamp(1, count.max(1));
    let (next, work) = (&next, &work);
    thread::scope(|scope| {
        let mut spawned = 0_u64;
        for worker in 0..workers {
            let spawn = thread::Builder::new()
                .name(format!("raftsim-worker-{worker}"))
                .spawn_scoped(scope, move || {
                    loop {
                        // `Relaxed` yeterli: sayaç yalnızca her sıranın TEK bir iş parçacığına
                        // verilmesini sağlar. Sonuçların görünürlüğünü, sonuçları toplayan kilit ve
                        // `scope`'un sonundaki birleşme (join) garanti eder.
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= count {
                            break;
                        }
                        work(index);
                    }
                });
            match spawn {
                Ok(_) => spawned += 1,
                Err(error) if spawned == 0 => return Err(error),
                Err(_) => break,
            }
        }
        Ok(())
    })
}

/// Kilidi alır. Kilit, bir iş parçacığı onu tutarken panik atarsa zehirlenir; içindeki sonuçlar
/// yine de geçerlidir (her ekleme tek bir `push`'tur), bu yüzden zehir yok sayılır.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn replay(args: &ReplayArgs, out: &mut impl Write) -> Result<ExitCode, CommandError> {
    let original = Scenario::generate(args.seed, args.profile.config());
    // Tutulacak hataların ÖZGÜN sıraları: `--faults` verildiyse sıralanmış ve tekilleştirilmiş
    // hâli, yoksa hepsi. Geçersiz bir sıra sessizce yok sayılmaz: yanlış yazılmış bir sıra hatasız
    // bir senaryoya dönüşür ve "hata yeniden üretilemiyor" yanılgısına yol açardı.
    let indices: Vec<usize> = match &args.faults {
        Some(FaultList(list)) => {
            let mut list = list.clone();
            list.sort_unstable();
            list.dedup();
            if let Some(&bad) = list.iter().find(|&&index| index >= original.faults.len()) {
                return Err(CommandError::Usage(format!(
                    "fault index {bad} is out of range: seed {} has {} faults in the {} profile \
                     (indices 0..{})",
                    args.seed,
                    original.faults.len(),
                    args.profile.name(),
                    original.faults.len()
                )));
            }
            list
        }
        None => (0..original.faults.len()).collect(),
    };
    let mut scenario = original.keep_faults(&indices);
    if let Some(horizon) = args.horizon {
        scenario = scenario.with_horizon(horizon);
    }
    let result = run(&scenario);
    if let Some(path) = &args.svg {
        draw(args, &result, path)?;
    }
    let written = write_replay(args, &scenario, &indices, &result, out);
    finish(verdict(result.outcome.is_err()), written)
}

/// `--svg`: koşunun zaman çizelgesini çizer ve dosyaya yazar. Rapordan ÖNCE yapılır: koşudan
/// sonra anlaşılan bir argüman hatası (koşu bittikten sonra başlayan bir pencere) da, diğer
/// argüman hataları gibi stdout'a hiçbir şey yazılmadan reddedilir.
fn draw(args: &ReplayArgs, result: &Run, path: &Path) -> Result<(), CommandError> {
    let chart = match &result.cluster {
        Some(cluster) => chart::collect(
            chart_titles(args, result),
            cluster,
            &result.outcome,
            args.window.as_ref(),
        )
        .map_err(CommandError::Usage)?,
        // Küme kurulamadıysa (geçersiz ayar ya da kurucuda panik) çizilecek bir koşu yoktur. Dosya
        // yine yazılır (başlık ve hata): aynı yolda kalmış eski bir çizim yeni sanılmasın.
        None => chart::without_run(chart_titles(args, result), &result.outcome),
    };
    fs::write(path, svg::render(&chart)).map_err(|error| CommandError::Svg {
        path: path.to_owned(),
        error,
    })
}

/// Çizelgenin başlığı ve alt başlığı: seed, profil, (varsa) derlemenin mutasyonu ve sonuç; altında
/// koşuyu yeniden üreten komut. Resim tek başına paylaşıldığında da hangi koşuyu gösterdiği ve
/// nasıl yeniden üretileceği okunabilsin.
fn chart_titles(args: &ReplayArgs, result: &Run) -> (String, String) {
    let mut title = format!("seed {}, profile {}", args.seed, args.profile.name());
    if let Some(feature) = sim::ENABLED_MUTATION {
        title.push_str(&format!(", built with {feature}"));
    }
    match &result.outcome {
        Ok(_) => title.push_str(": passed"),
        Err(error) => title.push_str(&format!(": FAILED ({})", error.signature())),
    }
    let mut command = reproduce(args.seed, args.profile);
    if let Some(faults) = &args.faults {
        command.push_str(&format!(" --faults {faults}"));
    }
    if let Some(horizon) = args.horizon {
        command.push_str(&format!(" --horizon {horizon}"));
    }
    (title, command)
}

fn write_replay(
    args: &ReplayArgs,
    scenario: &Scenario,
    indices: &[usize],
    result: &Run,
    out: &mut impl Write,
) -> io::Result<()> {
    if args.trace {
        // Hata satırları ÖZGÜN sıralarla basılır: küçültülmüş bir senaryodan elle bir hata daha
        // çıkarmak isteyen, `--faults` listesine yazacağı sırayı buradan okur. `with_horizon`
        // hata listesinin bir önekini bıraktığı için `zip` doğal olarak kısalır.
        for (index, scheduled) in indices.iter().zip(&scenario.faults) {
            writeln!(
                out,
                "fault {index}: t={} {}",
                scheduled.at,
                describe(scheduled.fault, scenario.config.nodes)
            )?;
        }
        if let Some(cluster) = &result.cluster {
            for event in cluster.sim().trace().events() {
                writeln!(out, "{:>7} {:?}", event.time, event.kind)?;
            }
            write_state(cluster, out)?;
        }
    }
    match &result.outcome {
        Ok(stats) => writeln!(
            out,
            "seed {}: passed ({} faults, {} operations completed, trace hash {:#018x})",
            args.seed,
            scenario.faults.len(),
            stats.clients.completed,
            stats.trace_hash
        ),
        Err(error) => writeln!(
            out,
            "seed {}: FAILED ({}): {error}",
            args.seed,
            error.signature()
        ),
    }
}

/// Kümenin son hâli (koşu başarısızsa hatanın görüldüğü an): her düğümün rolü, term'i,
/// commitIndex'i ve log'unun term'leri; ardından her term'in lideri. Trace mesajların yalnızca
/// özetini taşır; bir hatayı anlamanın en kısa yolu bu tablodur. Ör. bir Leader Completeness
/// ihlalinde yeni liderin log'unda, commit edilmiş girdinin index'inde başka bir term'in girdisi
/// görünür.
fn write_state(cluster: &RaftCluster, out: &mut impl Write) -> io::Result<()> {
    writeln!(out, "state at t={}:", cluster.now())?;
    for id in cluster.node_ids() {
        let Some(node) = cluster.node(id) else {
            continue;
        };
        writeln!(
            out,
            "  node {}{}: {:?} in term {}, commit index {}, log {}",
            id.0,
            // Çökmüş bir düğümün belleği çöktüğü anda donar (bkz. `RaftCluster::node`).
            if cluster.is_up(id) { "" } else { " (down)" },
            node.role(),
            node.current_term().0,
            node.commit_index().0,
            log_summary(node),
        )?;
    }
    let leaders: Vec<String> = cluster
        .elections()
        .iter()
        .filter_map(|(term, election)| {
            election
                .leader
                .map(|leader| format!("{}:{}", term.0, leader.0))
        })
        .collect();
    writeln!(out, "  leaders (term:node): {}", leaders.join(" "))
}

/// Bir düğümün log'unun özeti: varsa snapshot (`snapshot≤12:t3`, §7) ve ardından gelen girdilerin
/// term aralıkları.
fn log_summary(node: &sim::RaftNode) -> String {
    let terms = node.log().iter().map(|entry| entry.term.0);
    match node.snapshot() {
        None => log_terms(1, terms),
        Some(snapshot) => {
            let base = snapshot.last_index.0;
            let rest = log_terms(base + 1, terms);
            let rest = if rest == "empty" {
                String::new()
            } else {
                format!(" {rest}")
            };
            format!("snapshot≤{base}:t{}{rest}", snapshot.last_term.0)
        }
    }
}

/// Log'un term'leri; aynı term'li ardışık girdiler tek aralıkta toplanır: `1-2:t1 3:t2`. Boş log
/// `empty` yazılır. İlk girdinin index'i `first`'tür (snapshot yoksa 1, §5.3).
fn log_terms(first: u64, terms: impl Iterator<Item = u64>) -> String {
    // (ilk index, son index, term)
    let mut runs: Vec<(u64, u64, u64)> = Vec::new();
    for (index, term) in (first..).zip(terms) {
        match runs.last_mut() {
            Some((_, last, run_term)) if *run_term == term => *last = index,
            _ => runs.push((index, index, term)),
        }
    }
    if runs.is_empty() {
        return "empty".to_owned();
    }
    let parts: Vec<String> = runs
        .iter()
        .map(|&(first, last, term)| {
            if first == last {
                format!("{first}:t{term}")
            } else {
                format!("{first}-{last}:t{term}")
            }
        })
        .collect();
    parts.join(" ")
}

/// Bir hata niyetinin okunur hâli. Çökme ve yeniden başlatmanın hedefi yürütme anındaki duruma göre
/// seçilir (bkz. `sim::Fault`); seçilen düğüm trace'teki `Crash`/`Restart` olayında görünür.
fn describe(fault: Fault, nodes: u64) -> String {
    match fault {
        Fault::Crash { pick } => format!("crash an up node (pick {pick})"),
        Fault::CrashLeader => "crash the leader".to_owned(),
        Fault::Restart { pick } => format!("restart a down node (pick {pick})"),
        Fault::Partition { mask } => {
            let (left, right): (Vec<u64>, Vec<u64>) =
                (1..=nodes.min(64)).partition(|&id| mask & (1 << (id - 1)) != 0);
            format!("partition {left:?} | {right:?}")
        }
        Fault::Heal => "heal".to_owned(),
        Fault::Loss { permille } => format!("set the drop rate to {permille}‰"),
        Fault::IsolateLeader => "isolate the leader".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommandError, describe, finish, lock, log_terms, parallel_for, reproduce, reproduce_shrunk,
    };
    use crate::args::{Cli, Command, FaultList, Profile, ReplayArgs};
    use clap::Parser;
    use sim::Fault;
    use std::io;
    use std::process::ExitCode;
    use std::sync::Mutex;

    // Bölünme maskesi okunur gruplara çevrilir: k. düğüm, maskenin (k - 1). biti 1 ise ilk
    // gruptadır.
    #[test]
    fn faults_are_described_readably() {
        assert_eq!(
            describe(Fault::Partition { mask: 0b10101 }, 5),
            "partition [1, 3, 5] | [2, 4]"
        );
        assert_eq!(describe(Fault::Heal, 5), "heal");
        assert_eq!(
            describe(Fault::Loss { permille: 50 }, 5),
            "set the drop rate to 50‰"
        );
    }

    // Log, term aralıklarıyla özetlenir: index'ler 1'den başlar, aynı term'li ardışık girdiler tek
    // aralıktır.
    #[test]
    fn logs_are_summarized_by_term_runs() {
        assert_eq!(
            log_terms(1, [1, 1, 3, 3, 3, 5].into_iter()),
            "1-2:t1 3-5:t3 6:t5"
        );
        assert_eq!(log_terms(1, [2].into_iter()), "1:t2");
        assert_eq!(log_terms(1, std::iter::empty()), "empty");
        assert_eq!(log_terms(13, [4, 4].into_iter()), "13-14:t4");
    }

    /// Basılan bir `cargo run -p cli ... -- replay ...` komutunun `--` sonrasını `raftsim`
    /// argümanları olarak ayrıştırır.
    fn parse_printed(line: &str) -> ReplayArgs {
        let (_, arguments) = line
            .split_once(" -- ")
            .expect("the command passes its arguments to raftsim after --");
        let cli =
            Cli::try_parse_from(std::iter::once("raftsim").chain(arguments.split_whitespace()))
                .expect("the printed command parses");
        let Command::Replay(args) = cli.command else {
            panic!("the printed command is a replay: {line}");
        };
        args
    }

    // Basılan yeniden üretme komutları geri ayrıştırılır ve aynı senaryoyu seçer: küçültme bütün
    // hataları elediğinde de (`--faults none`), profil varsayılan olmadığında da.
    #[test]
    fn printed_reproduce_commands_parse_back() {
        let plain = parse_printed(&reproduce(7, Profile::Chaos));
        assert_eq!(
            (plain.seed, plain.profile, plain.faults, plain.horizon),
            (7, Profile::Chaos, None, None)
        );
        for (kept, horizon, profile) in [
            (Vec::new(), 0, Profile::Chaos),
            (vec![4], 120, Profile::Chaos),
            (vec![5, 6, 8, 52], 410, Profile::Figure8),
        ] {
            let line = reproduce_shrunk(3, profile, &kept, horizon);
            let args = parse_printed(&line);
            assert_eq!(args.seed, 3, "{line}");
            assert_eq!(args.profile, profile, "{line}");
            assert_eq!(args.faults, Some(FaultList(kept)), "{line}");
            assert_eq!(args.horizon, Some(horizon), "{line}");
            assert!(!args.trace, "{line}");
        }
    }

    // Okuyan taraf boruyu kapatınca karar korunur: başarısız bir koşu yine 1 ile biter, sahte bir
    // başarı olmaz. Başka bir yazma hatası ise komutun kendi hatasıdır.
    #[test]
    fn a_closed_pipe_keeps_the_verdict() {
        let broken = || Err(io::Error::from(io::ErrorKind::BrokenPipe));
        assert_eq!(
            finish(ExitCode::from(1), broken()).ok(),
            Some(ExitCode::from(1))
        );
        assert_eq!(
            finish(ExitCode::SUCCESS, broken()).ok(),
            Some(ExitCode::SUCCESS)
        );
        assert_eq!(
            finish(ExitCode::from(1), Ok(())).ok(),
            Some(ExitCode::from(1))
        );
        assert!(matches!(
            finish(ExitCode::SUCCESS, Err(io::Error::other("disk full"))),
            Err(CommandError::Write(_))
        ));
    }

    // Her sıra tam bir kez işlenir: iş parçacığı sayısı iş sayısını aştığında da (fazlası açılmaz),
    // tek iş parçacığıyla da, hiç iş yokken de.
    #[test]
    fn parallel_for_visits_every_index_once() {
        for (count, threads) in [(0, 4), (1, 8), (5, 1), (100, 7)] {
            let seen = Mutex::new(Vec::new());
            parallel_for(count, threads, |index| lock(&seen).push(index)).expect("threads start");
            let mut seen = seen.into_inner().expect("no worker panicked");
            seen.sort_unstable();
            assert_eq!(seen, (0..count).collect::<Vec<_>>(), "{count} {threads}");
        }
    }
}
