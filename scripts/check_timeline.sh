#!/usr/bin/env bash
# check_timeline.sh — README'deki zaman çizelgesinin (docs/figure8-seed3.svg) kodun bugün çizdiğiyle
# HÂLÂ aynı olduğunu doğrular.
#
# Resim, `mutation-commit-old-terms` ile derlenmiş `raftsim`in Figure 8 profilindeki seed 3'ün
# küçültülmüş senaryosunu `replay --svg` ile çizmesidir. Çizim deterministiktir: aynı kod, aynı seed
# ve aynı argümanlar bayt bayt aynı dosyayı verir. Betik resmi yeniden üretip depodakiyle
# karşılaştırır. Simülatör, senaryo üreteci, Raft ya da çizim değişir de resim yenilenmezse CI
# kırılır: README'deki RESİM sessizce eskiyemez. (README'nin düzyazısındaki sayılar, ör. tick'ler
# ve term'ler, denetlenmez; `--update` sonrası elle karşılaştırılmalıdır.) Mutant koşu başarısız
# olmalıdır (çıkış kodu 1): resim ihlali gösterir.
#
# Kullanım: scripts/check_timeline.sh            (karşılaştırır)
#           scripts/check_timeline.sh --update   (resmi yeniden yazar)
#
# Gereksinimler: bash >= 4.4, cargo. Mutant derleme, check_mutations.sh gibi ayrı bir hedef dizinine
# (target/mutations) gider: target/release/raftsim hatalı bir Raft'la üzerine yazılmasın.
set -euo pipefail

export LC_ALL=C
unset CDPATH
root="$(cd "$(dirname "$0")/.." && pwd)"
image="$root/docs/figure8-seed3.svg"
fresh="$root/target/figure8-seed3.svg"
# README'deki hikâyenin koşusu: `fuzz --profile figure8 --shrink`ın seed 3 için bastığı komut.
faults="5,6,8,9,10,11,14,15,17,18,19,21,22,23,24,27,31,32,40,41,42,45,52"
args=(replay --seed 3 --profile figure8 --faults "$faults" --horizon 410)

mkdir -p "$root/target"
# Önceki bir koşudan kalmış dosya silinir: koşu çizmeden biterse eski bir dosya karşılaştırılıp sahte
# bir "OK" verilmesin.
rm -f "$fresh"
status=0
(cd "$root" && cargo run --release --locked --quiet --target-dir "$root/target/mutations" -p cli \
  --features mutation-commit-old-terms -- "${args[@]}" --svg "$fresh" >/dev/null) || status=$?
if ((status != 1)); then
  echo "check_timeline: FAIL - the mutant run must fail (exit code 1), got exit code $status"
  exit 1
fi
if [[ ! -f "$fresh" ]]; then
  echo "check_timeline: FAIL - the run did not draw $fresh"
  exit 1
fi

if [[ "${1:-}" == "--update" ]]; then
  cp "$fresh" "$image"
  echo "check_timeline: updated docs/figure8-seed3.svg" \
    "(check the README's prose about it: ticks, terms and nodes are not checked)"
  exit 0
fi
if [[ ! -f "$image" ]]; then
  echo "check_timeline: FAIL - docs/figure8-seed3.svg is missing (run with --update)"
  exit 1
fi
if ! cmp -s "$image" "$fresh"; then
  diff -u "$image" "$fresh" | head -n 40 || true
  echo "check_timeline: FAIL - docs/figure8-seed3.svg differs from what the code draws" \
    "(run scripts/check_timeline.sh --update)"
  exit 1
fi
echo "check_timeline: OK - docs/figure8-seed3.svg matches what the code draws"
