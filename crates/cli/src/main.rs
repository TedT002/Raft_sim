//! # raftsim CLI
//!
//! Deterministik kaos senaryolarını koşturan komut satırı aracı:
//!
//! - `raftsim fuzz --seeds A..B [--threads N] [--profile chaos|figure8] [--shrink]`: her seed'in
//!   senaryosunu (bkz. `sim::Scenario`) koşturur; HER olaydan sonra Raft invariant'ları, sonda
//!   istemci geçmişinin linearizability'si denetlenir. Seed'ler iş parçacıklarına dağıtılır ama her
//!   koşu tek iş parçacıklıdır ve yalnızca seed'ine bağlıdır: sonuç, iş parçacığı sayısından
//!   bağımsızdır. Başarısız her seed, seed'iyle ve tek satırlık yeniden üretme komutuyla
//!   raporlanır; `--shrink` her başarısız senaryoyu aynı hatayı veren daha küçük bir senaryoya
//!   indirir.
//! - `raftsim replay --seed N [--profile chaos|figure8] [--trace] [--faults i,j,...|none]
//!   [--horizon H]`: bir seed'in koşusunu birebir tekrarlar; isteğe bağlı olarak hata programını,
//!   olay izini ve kümenin son hâlini yazdırır. `--faults` ve `--horizon`, küçültülmüş (shrink) bir
//!   senaryoyu yeniden üretir.
//!
//! `--profile` senaryo ayarlarını seçer: `chaos` (varsayılan; kayıplı ağ, çökmeler, bölünmeler)
//! ya da `figure8` (mesaj başına tek girdi ve sık lider değişimi, §5.4.2'nin tuzağı için).
//!
//! Paket adı `cli`, ikili adı `raftsim` olduğundan yeniden üretme komutu
//! `cargo run -p cli -- replay --seed <N>` olur. Çıkış kodları: 0 başarı, 1 başarısız bir koşu, 2
//! geçersiz argüman (clap) ya da senaryoyla uyuşmayan bir argüman (ör. var olmayan bir hata
//! sırası), 3 aracın kendi hatası (çıktı yazılamadı, iş parçacığı açılamadı). 1 ile 3 bilerek
//! ayrıdır: CI'da bulunan bir hata, altyapının bir aksaklığıyla karışmamalı.

#![forbid(unsafe_code)]

mod args;
mod commands;

use std::io::{self, Write};
use std::process::ExitCode;

use clap::Parser;

use commands::CommandError;

fn main() -> ExitCode {
    let cli = args::Cli::parse();
    let mut out = io::stdout().lock();
    let code = match commands::execute(cli, &mut out) {
        Ok(code) => code,
        Err(error @ CommandError::Usage(_)) => {
            // clap'in kendi argüman hatalarıyla aynı yer (stderr) ve aynı çıkış kodu (2).
            let _ = writeln!(io::stderr(), "error: {error}");
            return ExitCode::from(2);
        }
        Err(error) => {
            // stdout yazılamıyorsa hata stderr'e gider; o da yazılamazsa yapılacak bir şey yok.
            let _ = writeln!(io::stderr(), "raftsim: {error}");
            return ExitCode::from(3);
        }
    };
    match out.flush() {
        Ok(()) => code,
        // Çıktı bir boruya bağlıyken okuyan taraf erken kapandı (ör. `raftsim replay --trace |
        // head`): bu bir hata değil, okuyanın tercihi. Karar zaten verildi; çıkış kodu koşuların
        // sonucudur (bkz. `commands::finish`).
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => code,
        Err(error) => {
            let _ = writeln!(io::stderr(), "raftsim: cannot write the output: {error}");
            ExitCode::from(3)
        }
    }
}
