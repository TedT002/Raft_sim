//! # raftsim CLI
//!
//! Bu ikili, ileride `raftsim fuzz --seeds A..B` ve `raftsim replay --seed N` alt komutlarını
//! sunacak (Faz 5; README'deki yol haritası). Paket adı `cli`, ikili adı `raftsim` olduğundan
//! başarısız bir simülasyonu yeniden üretme komutu `cargo run -p cli -- replay --seed <N>` olur.
//! Faz 0'da henüz hiçbir alt komut yoktur: argüman ayrıştırma kütüphanesi (ör. `clap`) bilerek
//! eklenmemiştir, çünkü bu fazın kapsamı yalnızca iskelet ve derlenebilirliktir.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    // Argüman sayısını `env::args_os()` ile sayıyoruz (UTF-8'e çevirmeden): bu şekilde geçersiz
    // UTF-8 içeren bir argüman (ör. bozuk bir yerelde gelen bayt dizisi) asla panik sebebi olmaz;
    // yalnızca "argüman var mı yok mu" sorusuna cevap arıyoruz, içeriğini henüz okumuyoruz.
    let arg_count = std::env::args_os().len();

    // Sürüm, derleme zamanında Cargo.toml'daki `version.workspace = true`'dan gelir; böylece
    // ikili sürümü ile paket sürümü asla birbirinden kopmaz.
    let version = env!("CARGO_PKG_VERSION");
    let wip_line =
        format!("raftsim {version}: work in progress (phase 0 skeleton, no subcommands yet)");

    if arg_count <= 1 {
        // Gelenek gereği ilk eleman (argv[0]) programın adı/yoludur, ama bu bir garanti değildir:
        // keyfi bir değer olabilir, hatta hiç olmayabilir (argc = 0). `<= 1` karşılaştırması iki
        // durumu da "gerçek argüman yok" sayar.
        println!("{wip_line}");
        ExitCode::SUCCESS
    } else {
        // Neden burada sessizce 0 ile çıkmıyoruz: `raftsim replay --seed 1` gibi henüz
        // desteklenmeyen bir alt komutu sessizce yok sayıp başarı kodu dönmek, kullanıcıya
        // "replay çalıştı" yanılsaması verir (sahte başarı). Bunun yerine WIP satırını ve
        // net bir hata mesajını stderr'e yazıp ayrı bir çıkış koduyla (2) başarısızlığı
        // açıkça bildiriyoruz.
        eprintln!("{wip_line}");
        eprintln!("raftsim: arguments are not supported yet (subcommands arrive in phase 5)");
        ExitCode::from(2)
    }
}
