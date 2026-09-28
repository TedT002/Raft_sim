//! `raftsim` ikilisinin Faz 0 davranışını dıştan (kara kutu) sınayan duman testleri.
//!
//! Yeni bir crate eklemeden (ör. `assert_cmd`) yalnızca `std::process::Command` kullanılır;
//! `env!("CARGO_BIN_EXE_raftsim")` Cargo'nun test derlemesi için ürettiği ikilinin tam yolunu
//! derleme zamanında verir, bu sayede `PATH`'e veya çalışma dizinine bağımlı olmayız.

use std::process::Command;

// `src/main.rs`'in bastığı tam WIP satırı. Bu test crate'i de `cli` paketine ait olduğu için
// `CARGO_PKG_VERSION` ikilininkiyle aynıdır; bu sayede çıktı "içeriyor mu" diye değil, BİREBİR
// karşılaştırılabilir (fazladan bir satır veya yanlış akışa yazılan bir mesaj da yakalanır).
fn wip_line() -> String {
    format!(
        "raftsim {}: work in progress (phase 0 skeleton, no subcommands yet)",
        env!("CARGO_PKG_VERSION")
    )
}

// Argümansız çalıştırma: stdout'a TAM OLARAK tek satır (WIP satırı) yazılmalı, stderr boş kalmalı
// ve çıkış kodu 0 olmalı. Bu aynı zamanda ikilinin adının gerçekten `raftsim` olduğunu da kanıtlar
// (Cargo.toml'daki `[[bin]] name`).
#[test]
fn prints_work_in_progress_and_exits_zero_without_args() {
    let output = Command::new(env!("CARGO_BIN_EXE_raftsim"))
        .output()
        .expect("raftsim binary should be spawnable in the test sandbox");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
    let wip = wip_line();
    assert_eq!(stdout, format!("{wip}\n"));
    assert!(
        output.stderr.is_empty(),
        "stderr must stay empty on the success path"
    );
}

// Argümanlı çalıştırma (henüz desteklenmeyen bir alt komut örneği): çıkış kodu 2 olmalı, stdout
// boş kalmalı ve stderr tam olarak WIP satırı + "desteklenmiyor" mesajından oluşmalı. Sessizce 0
// dönmek burada "replay çalıştı" gibi yanlış bir izlenim verirdi; bu test tam olarak o sahte
// başarıyı engelleyen davranışı sınar.
#[test]
fn rejects_arguments_with_exit_code_2() {
    let output = Command::new(env!("CARGO_BIN_EXE_raftsim"))
        .args(["replay", "--seed", "1"])
        .output()
        .expect("raftsim binary should be spawnable in the test sandbox");

    assert_eq!(output.status.code(), Some(2));
    assert!(
        output.stdout.is_empty(),
        "stdout must stay empty on the error path"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
    let wip = wip_line();
    assert_eq!(
        stderr,
        format!(
            "{wip}\nraftsim: arguments are not supported yet (subcommands arrive in phase 5)\n"
        )
    );
}
