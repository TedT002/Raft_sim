//! `raftsim` ikilisini dıştan (kara kutu) sınayan testler: argümanlar, çıkış kodları ve çıktının
//! determinizmi.
//!
//! Yeni bir crate eklemeden (ör. `assert_cmd`) yalnızca `std::process::Command` kullanılır;
//! `env!("CARGO_BIN_EXE_raftsim")` Cargo'nun test derlemesi için ürettiği ikilinin tam yolunu
//! derleme zamanında verir, bu sayede `PATH`'e veya çalışma dizinine bağımlı olmayız.

use std::process::{Command, Output};

fn raftsim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_raftsim"))
        .args(args)
        .output()
        .expect("raftsim binary should be spawnable in the test sandbox")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout should be UTF-8")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr should be UTF-8")
}

// Yardım metni iki alt komutu da tanıtır; sürüm, paket sürümüdür.
#[test]
fn help_and_version_describe_the_tool() {
    let help = raftsim(&["--help"]);
    assert!(help.status.success());
    let text = stdout(&help);
    assert!(text.contains("fuzz") && text.contains("replay"), "{text}");
    let version = raftsim(&["--version"]);
    assert_eq!(
        stdout(&version),
        format!("raftsim {}\n", env!("CARGO_PKG_VERSION"))
    );
}

// Geçersiz argümanlar 2 ile reddedilir ve hata stderr'e yazılır: boş aralık, bozuk sayı,
// bilinmeyen alt komut, eksik zorunlu argüman, bozuk hata listesi ve seed'in hata programında
// olmayan bir hata sırası (sessizce yok sayılsaydı hatasız bir senaryo koşardı).
#[test]
fn invalid_arguments_exit_with_code_2() {
    for args in [
        &["fuzz", "--seeds", "5..5"][..],
        &["fuzz", "--seeds", "x..3"],
        &["fuzz", "--seeds", "0..3", "--threads", "0"],
        &["replay"],
        &["explode"],
        &["replay", "--seed", "3", "--faults", "x"],
        &["replay", "--seed", "3", "--faults", ""],
        &["replay", "--seed", "3", "--faults", "1,9999"],
    ] {
        let output = raftsim(args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(!stderr(&output).is_empty(), "{args:?}");
    }
    assert!(stderr(&raftsim(&["fuzz", "--seeds", "5..5"])).contains("empty range"));
    assert!(
        stderr(&raftsim(&["replay", "--seed", "3", "--faults", "9999"]))
            .contains("fault index 9999 is out of range")
    );
}

// Bir seed aralığı taranır; hepsi geçer ve özet satırı sayıları verir. Çıktı iş parçacığı
// sayısından bağımsızdır: her koşu yalnızca seed'ine bağlıdır ve sonuçlar seed sırasıyla yazılır.
// Diğer profiller (figure8, reads, snapshots) de taranır.
#[test]
fn fuzz_reports_a_passing_range_independently_of_threads() {
    let single = raftsim(&["fuzz", "--seeds", "0..4", "--threads", "1"]);
    assert!(single.status.success(), "{}", stdout(&single));
    assert_eq!(
        stdout(&single),
        "fuzzed 4 seeds (0..4): 4 passed, 0 failed\n"
    );
    let parallel = raftsim(&["fuzz", "--seeds", "0..4", "--threads", "3"]);
    assert_eq!(stdout(&parallel), stdout(&single));
    for profile in ["figure8", "reads", "snapshots"] {
        let other = raftsim(&["fuzz", "--seeds", "0..4", "--profile", profile]);
        assert!(other.status.success(), "{profile}: {}", stdout(&other));
        assert_eq!(
            stdout(&other),
            "fuzzed 4 seeds (0..4): 4 passed, 0 failed\n"
        );
    }
}

// Yeniden oynatma deterministiktir: aynı seed iki kez aynı çıktıyı (trace özeti dahil) verir.
// `--trace` hata programını, olay izini ve kümenin son hâlini de yazar; `--faults` ve `--horizon`
// küçültülmüş bir senaryoyu koşturur.
#[test]
fn replay_is_deterministic_and_can_print_the_trace() {
    let first = raftsim(&["replay", "--seed", "3"]);
    let second = raftsim(&["replay", "--seed", "3"]);
    assert!(first.status.success());
    assert_eq!(stdout(&first), stdout(&second));
    assert!(
        stdout(&first).starts_with("seed 3: passed ("),
        "{}",
        stdout(&first)
    );
    assert!(stdout(&first).contains("trace hash 0x"));

    let traced = raftsim(&["replay", "--seed", "3", "--trace"]);
    let text = stdout(&traced);
    assert!(text.starts_with("fault 0: t="), "{text}");
    assert!(
        text.contains("Tick {") && text.contains("Deliver {"),
        "{text}"
    );
    assert!(
        text.contains("state at t=") && text.contains("leaders (term:node): "),
        "{text}"
    );
    assert!(
        text.ends_with(&stdout(&first)),
        "the summary line closes the trace"
    );

    let shrunk = raftsim(&[
        "replay",
        "--seed",
        "3",
        "--faults",
        "0,2",
        "--horizon",
        "100",
    ]);
    assert!(shrunk.status.success(), "{}", stdout(&shrunk));
    assert!(stdout(&shrunk).starts_with("seed 3: passed (2 faults,"));

    // Küçültme bütün hataları elediğinde basılan komut (`--faults none`) da çalışır.
    let none = raftsim(&[
        "replay",
        "--seed",
        "3",
        "--faults",
        "none",
        "--horizon",
        "0",
    ]);
    assert!(none.status.success(), "{}", stderr(&none));
    assert!(stdout(&none).starts_with("seed 3: passed (0 faults,"));

    // `--trace` hataları ÖZGÜN sıralarıyla basar: küçültülmüş bir senaryodan bir hata daha
    // çıkarmak isteyen, `--faults` listesindeki sırayı buradan okur.
    let kept = raftsim(&["replay", "--seed", "3", "--faults", "5,2", "--trace"]);
    let text = stdout(&kept);
    let faults: Vec<&str> = text
        .lines()
        .filter(|line| line.starts_with("fault "))
        .collect();
    assert_eq!(faults.len(), 2, "{text}");
    assert!(faults[0].starts_with("fault 2: t="), "{text}");
    assert!(faults[1].starts_with("fault 5: t="), "{text}");
}
