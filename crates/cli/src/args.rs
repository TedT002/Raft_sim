//! Komut satırı argümanları (`clap`).

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use sim::ScenarioConfig;

/// Deterministic simulation testing for a Raft implementation.
#[derive(Debug, Parser)]
#[command(name = "raftsim", version, about)]
pub struct Cli {
    /// Alt komut.
    #[command(subcommand)]
    pub command: Command,
}

/// Alt komutlar.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the chaos scenario of every seed in a range and report the failing seeds.
    Fuzz(FuzzArgs),
    /// Replay the chaos scenario of one seed exactly.
    Replay(ReplayArgs),
}

/// Senaryo profili: hangi ayarlarla üretilip koşulacağı (bkz. `sim::ScenarioConfig`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Profile {
    /// The default chaos mix: lossy network, crashes, partitions, client retries.
    Chaos,
    /// Figure 8 style faults: one entry per AppendEntries and frequent leader changes.
    Figure8,
    /// The chaos mix where most reads skip the log (ReadIndex) and leaders crash often.
    Reads,
    /// The chaos mix with log compaction: nodes snapshot their state every 16 entries.
    Snapshots,
}

impl Profile {
    /// Profilin senaryo ayarları.
    #[must_use]
    pub fn config(self) -> ScenarioConfig {
        match self {
            Profile::Chaos => ScenarioConfig::chaos(),
            Profile::Figure8 => ScenarioConfig::figure8(),
            Profile::Reads => ScenarioConfig::reads(),
            Profile::Snapshots => ScenarioConfig::snapshots(),
        }
    }

    /// Profilin komut satırındaki adı.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Profile::Chaos => "chaos",
            Profile::Figure8 => "figure8",
            Profile::Reads => "reads",
            Profile::Snapshots => "snapshots",
        }
    }

    /// Yeniden üretme komutuna eklenecek bayrak (varsayılan profil için boş).
    #[must_use]
    pub fn flag(self) -> &'static str {
        match self {
            Profile::Chaos => "",
            Profile::Figure8 => " --profile figure8",
            Profile::Reads => " --profile reads",
            Profile::Snapshots => " --profile snapshots",
        }
    }
}

/// `fuzz` argümanları.
#[derive(Debug, Args)]
pub struct FuzzArgs {
    /// Seeds to run, as `A..B` (B excluded).
    #[arg(long, value_parser = parse_range)]
    pub seeds: Range<u64>,
    /// Scenario profile.
    #[arg(long, value_enum, default_value_t = Profile::Chaos)]
    pub profile: Profile,
    /// Worker threads (results do not depend on it).
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..))]
    pub threads: u16,
    /// Shrink every failing scenario to a smaller one that fails the same way.
    #[arg(long)]
    pub shrink: bool,
}

/// `replay` argümanları.
#[derive(Debug, Args)]
pub struct ReplayArgs {
    /// Seed of the scenario.
    #[arg(long)]
    pub seed: u64,
    /// Scenario profile.
    #[arg(long, value_enum, default_value_t = Profile::Chaos)]
    pub profile: Profile,
    /// Print every trace event of the run.
    #[arg(long)]
    pub trace: bool,
    /// Keep only these faults (indices into the seed's fault list), e.g. `--faults 0,3,7`, or
    /// `--faults none` to drop every fault.
    #[arg(long, value_parser = parse_faults)]
    pub faults: Option<FaultList>,
    /// End the fault phase at this tick.
    #[arg(long)]
    pub horizon: Option<u64>,
    /// Draw the run's timeline (roles, terms, crashes, partitions, the violation) as an SVG file.
    #[arg(long, value_name = "FILE")]
    pub svg: Option<PathBuf>,
    /// Draw only the ticks `A..B` (B excluded), with every message delivered in that window.
    #[arg(long, value_parser = parse_range, requires = "svg")]
    pub window: Option<Range<u64>>,
}

/// `--faults` değeri: tutulacak hataların sıraları; boş liste `none` yazılır.
///
/// Neden ayrı bir tip: küçültme bütün hataları elerse yeniden üretme komutu `--faults none` basar
/// (boş bir değer `--faults ` hâlinde kabuktan kaybolurdu). `Vec<usize>` alanını clap çoklu değer
/// sayar ve `none` gibi bir kelimeyi kabul etmezdi; basılan komut da çalışmazdı. Ayrıştırma ve
/// yazma (`Display`) tek tipte durur: biri değişip öteki unutulamaz, gidiş-dönüş testlenir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultList(pub Vec<usize>);

impl fmt::Display for FaultList {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return formatter.write_str("none");
        }
        for (position, index) in self.0.iter().enumerate() {
            if position > 0 {
                formatter.write_str(",")?;
            }
            write!(formatter, "{index}")?;
        }
        Ok(())
    }
}

/// `none` ya da virgülle ayrılmış sıralar (`0,3,7`).
fn parse_faults(text: &str) -> Result<FaultList, String> {
    if text == "none" {
        return Ok(FaultList(Vec::new()));
    }
    text.split(',')
        .map(|part| {
            part.parse().map_err(|error| {
                format!("bad fault index {part:?} (expected `none` or a list like 0,3,7): {error}")
            })
        })
        .collect::<Result<Vec<usize>, String>>()
        .map(FaultList)
}

/// `A..B` biçiminde bir aralık (`B` hariç, `A < B`): seed'ler ya da çizilecek tick'ler.
fn parse_range(text: &str) -> Result<Range<u64>, String> {
    let (start, end) = text
        .split_once("..")
        .ok_or_else(|| format!("expected a range like 0..100, got {text:?}"))?;
    let start: u64 = start
        .parse()
        .map_err(|error| format!("bad range start {start:?}: {error}"))?;
    let end: u64 = end
        .parse()
        .map_err(|error| format!("bad range end {end:?}: {error}"))?;
    if start >= end {
        return Err(format!(
            "empty range {text:?}: the start must be below the end"
        ));
    }
    Ok(start..end)
}

#[cfg(test)]
mod tests {
    use super::{FaultList, parse_faults, parse_range};

    // Aralık ayrıştırma: geçerli aralık, boş ve ters aralık, bozuk sayılar.
    #[test]
    fn ranges_are_parsed_strictly() {
        assert_eq!(parse_range("0..100"), Ok(0..100));
        assert_eq!(parse_range("7..8"), Ok(7..8));
        assert!(parse_range("5..5").is_err());
        assert!(parse_range("9..3").is_err());
        assert!(parse_range("x..3").is_err());
        assert!(parse_range("3").is_err());
        assert!(parse_range("-1..3").is_err());
    }

    // Hata listesi: `none` boş listedir; sıralar virgülle ayrılır; bozuk ya da eksik bir sıra
    // reddedilir. Yazılan biçim ayrıştırılanla aynıdır (gidiş-dönüş).
    #[test]
    fn fault_lists_round_trip() {
        assert_eq!(parse_faults("none"), Ok(FaultList(Vec::new())));
        assert_eq!(parse_faults("3"), Ok(FaultList(vec![3])));
        assert_eq!(parse_faults("0,3,7"), Ok(FaultList(vec![0, 3, 7])));
        for bad in ["", "1,", ",1", "x", "1;2", "-1", "None"] {
            assert!(parse_faults(bad).is_err(), "{bad:?}");
        }
        for list in [Vec::new(), vec![5], vec![0, 2, 9]] {
            let list = FaultList(list);
            assert_eq!(parse_faults(&list.to_string()), Ok(list));
        }
    }
}
