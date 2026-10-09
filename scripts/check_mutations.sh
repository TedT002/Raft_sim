#!/usr/bin/env bash
# check_mutations.sh — docs/mutation-table.md'deki her mutasyonun HÂLÂ yakalandığını doğrular.
#
# Her satır için `raftsim` ilgili `mutation-*` özelliğiyle derlenir ve satırdaki seed, satırdaki
# profille yeniden oynatılır. Koşu başarısız olmalı (çıkış kodu 1) ve hata imzası tablodaki
# "Caught by" sütunuyla birebir aynı olmalıdır. Böylece tablo, kod değiştikçe sessizce eskiyemez:
# bir mutasyon artık yakalanmıyorsa ya da başka bir denetime takılıyorsa CI kırılır.
#
# Aynı seed özelliksiz (doğru) derlemede de oynatılır ve GEÇMELİDİR: yoksa satır, mutasyonun değil
# Raft'ın kendisinin (ya da senaryonun) bir hatasını gösteriyor olabilirdi. Ayrıca cli'ın her
# `mutation-*` özelliğinin tabloda bir satırı olmalı: yeni bir mutasyon tabloya girmeden kalamaz.
#
# Gereksinimler: bash >= 4.4, cargo. Her satır ayrı bir özellikle derlendiği için yavaştır (release
# derlemesi); CI bunu ayrı bir işte koşar. Mutant derlemeler ayrı bir hedef dizinine
# (target/mutations) gider: aksi hâlde target/release/raftsim, son denenen mutantla üzerine yazılmış
# olarak kalırdı ve sonradan doğrudan çalıştırılan ikili sessizce hatalı bir Raft koştururdu.
set -euo pipefail

export LC_ALL=C
unset CDPATH
root="$(cd "$(dirname "$0")/.." && pwd)"
table="$root/docs/mutation-table.md"
manifest="$root/crates/cli/Cargo.toml"

# Bir tablo hücresini sadeleştirir: kenar boşlukları ve ters tırnaklar gider.
cell() {
  local value="$1"
  value="${value//\`/}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

# cli'ın tanımladığı mutasyon özellikleri ([features] bölümündeki `mutation-…` anahtarları).
declare -A declared=()
while IFS= read -r feature; do
  declared["$feature"]=1
done < <(grep -oE '^mutation-[a-z0-9-]+' "$manifest")

declare -A listed=()
rows=0
failures=0
while IFS='|' read -r _ feature _ _ check profile seed _; do
  feature="$(cell "$feature")"
  check="$(cell "$check")"
  profile="$(cell "$profile")"
  seed="$(cell "$seed")"
  rows=$((rows + 1))
  listed["$feature"]=1
  if [[ -z "${declared[$feature]+set}" ]]; then
    echo "not ok - $feature is in the table but not a feature of crates/cli/Cargo.toml"
    failures=$((failures + 1))
    continue
  fi

  status=0
  output="$(cd "$root" && cargo run --release --locked --quiet \
    --target-dir "$root/target/mutations" -p cli --features "$feature" -- \
    replay --seed "$seed" --profile "$profile" 2>&1)" || status=$?
  if ((status == 1)) && grep -qF "FAILED ($check)" <<<"$output"; then
    echo "ok - $feature is caught by '$check' (profile $profile, seed $seed)"
  else
    echo "not ok - $feature: expected a failure caught by '$check' (profile $profile, seed $seed)"
    echo "  exit code $status, output: $output"
    failures=$((failures + 1))
  fi

  status=0
  output="$(cd "$root" && cargo run --release --locked --quiet -p cli -- \
    replay --seed "$seed" --profile "$profile" 2>&1)" || status=$?
  if ((status == 0)) && grep -qF "seed $seed: passed" <<<"$output"; then
    echo "ok - without $feature the same run passes"
  else
    echo "not ok - without $feature the run must pass (profile $profile, seed $seed)"
    echo "  exit code $status, output: $output"
    failures=$((failures + 1))
  fi
done < <(grep '^| `mutation-' "$table")

for feature in "${!declared[@]}"; do
  if [[ -z "${listed[$feature]+set}" ]]; then
    echo "not ok - feature $feature of crates/cli/Cargo.toml has no row in the table"
    failures=$((failures + 1))
  fi
done

# Tablo en az yedi satır içermeli: altı Raft mutasyonu (seçim kısıtı, önceki term'leri sayarak
# commit, votedFor'u persist etmeme, log'u kısaltma, prevLogTerm kontrolünü atlama, commit'ten
# önce uygulama) ve istemci tekilleştirmesini kapatan mutasyon. Satırların silinmesi de böylece
# fark edilir.
if ((rows < 7)); then
  echo "check_mutations: FAIL - expected at least 7 mutation rows in $table, found $rows"
  exit 1
fi
if ((failures > 0)); then
  echo "check_mutations: FAIL - $failures check(s) failed for the $rows mutations in the table"
  exit 1
fi
echo "check_mutations: OK - all $rows mutations are caught as listed in docs/mutation-table.md"
