#!/usr/bin/env bash
# check_deps.sh — workspace bağımlılıklarını mekanik olarak üç açıdan denetler:
#
# 1. İç (path) kenarların yönü. İzinli yön: cli -> sim -> raft-core, cli -> checker,
#    sim -> checker. raft-core hiçbir workspace crate'ine bağımlı olmaz (sans-IO çekirdek dış dünyayı
#    bilmez); checker da raft-core'u bilmez (kâhin, denetlediği uygulamadan bağımsız kalır).
# 2. Dış bağımlılıklar: her doğrudan dış bağımlılık crates.io'dan gelmeli ve onaylı listede olmalı.
# 3. Cargo.lock: dolaylı olanlar dahil derlenen her paket crates.io'dan gelmeli.
#
# Amaç: biri Cargo.toml'a bir satır eklediğinde, yönü ihlal eden bir kenarın (ör. `raft-core -> sim`)
# ya da onaysız bir crate'in insan incelemesine bel bağlamadan CI'da hemen yakalanması.
#
# Çıkış kodları: 0 = bütün denetimler temiz, 1 = en az biri başarısız (fark ve ihlaller basılır),
# 2 = yapılandırma hatası (cargo/jq/Cargo.lock yok, `cargo metadata` başarısız, çıktı
# ayrıştırılamadı).
set -euo pipefail

# Yerel ayardan bağımsız sıralama ve CDPATH tuzağına karşı (bkz. check_forbidden.sh).
export LC_ALL=C
unset CDPATH

# Betiğin bulunduğu dizinin bir üstü = repo kökü.
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

# NEDEN cd: rustup hangi toolchain'in çalışacağını `--manifest-path`'e bakarak DEĞİL, cwd'den
# yukarı doğru rust-toolchain.toml arayarak seçer. Betik başka bir dizinden (ör. /tmp) çağrılsa
# bile sabitlenmiş toolchain'in (1.98.1) kullanılması için repo köküne geçiyoruz.
cd -- "$repo_root"

if ! command -v cargo >/dev/null 2>&1; then
  echo "check_deps: error: 'cargo' not found in PATH" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "check_deps: error: 'jq' not found in PATH" >&2
  exit 2
fi

# NEDEN stderr ayrı bir dosyaya: cargo/rustup'ın stderr'e yazdığı her şey (ör. taze bir
# makinede toolchain kurulurken "info: syncing channel updates…", manifest'teki bir yazım
# hatası için "unused manifest key" uyarısı) JSON'a karışırsa jq anlaşılmaz bir ayrıştırma
# hatası verir. stdout yalnızca JSON'dur; stderr yalnızca hata durumunda gösterilir.
err_file=$(mktemp)
trap 'rm -f -- "$err_file"' EXIT

# NEDEN --no-deps: yalnızca workspace üyelerinin KENDİ Cargo.toml'larında beyan ettiği
# bağımlılıkları istiyoruz. --no-deps olmadan cargo, registry bağımlılık grafiğinin tamamını
# çözer (ve gerekirse ağdan indirir); bu hem gereksiz hem de CI'a ağ kaynaklı bir kırılganlık katar.
if ! metadata=$(cargo metadata --format-version 1 --no-deps \
  --manifest-path "$repo_root/Cargo.toml" 2>"$err_file"); then
  echo "check_deps: error: 'cargo metadata' failed:" >&2
  cat -- "$err_file" >&2
  exit 2
fi

# Path bağımlılıkları = workspace içi (iç) kenarlar. Registry bağımlılıklarının (rand, serde, ...)
# `path` alanı yoktur (null); bu yüzden `select(.path != null)` onları bilerek dışarıda bırakır ve
# sonraki fazlarda onaylanan dış crate'ler bu betiği etkilemez. `kind` (normal/dev/build)
# filtrelenmez: tüm türler denetlenir, çünkü bir dev-dependency üzerinden bile yanlış yönde bir iç
# bağımlılık sızabilir. `.name`, yeniden adlandırılmış (`rename`) bağımlılıkta bile gerçek paket
# adıdır.
if ! edges=$(jq -r '
  .packages[] as $p
  | $p.dependencies[]?
  | select(.path != null)
  | "\($p.name)->\(.name)"
' <<<"$metadata"); then
  echo "check_deps: error: could not parse 'cargo metadata' output" >&2
  exit 2
fi
actual=$(printf '%s\n' "$edges" | sort -u)

# Faz 0'da sabitlenen TEK izinli kenar kümesi. Bağımlılık yönü bilinçli bir mimari karardır: yeni
# bir iç kenar ancak tartışılıp onaylandıktan sonra eklenir ve bu liste aynı commit'te güncellenir.
expected=$(printf '%s\n' \
  'cli->checker' \
  'cli->sim' \
  'sim->checker' \
  'sim->raft-core')

# Dış (path olmayan) doğrudan bağımlılıklar: her biri crates.io'dan gelmeli ve projenin onaylı
# listesinde olmalı. Neden: determinizm, bağımlılıkların davranışına da bağlıdır (ör. bir RNG
# crate'inin akışı). Onaysız bir crate'in sessizce girmesini insan incelemesine bırakmıyoruz. Listeye
# yeni bir crate eklemek bilinçli bir karardır ve bu satırı da değiştirir.
#
# Liste projenin kabul ettiği crate'lerdir; listede olmak kullanılacağı anlamına gelmez. Örneğin
# `rand` şu an bilerek kullanılmıyor: simülatör, örnekleme algoritmaları sürümler arasında
# değişebilecek `rand` yerine `rand_chacha` üzerine kendi tek-çekilişli yardımcılarını kurar.
approved=(bincode clap proptest rand rand_chacha serde thiserror tracing)
crates_io='registry+https://github.com/rust-lang/crates.io-index'
if ! external=$(jq -r '
  .packages[] as $p
  | $p.dependencies[]?
  | select(.path == null)
  | "\($p.name) \(.name) \(.source // "unknown")"
' <<<"$metadata"); then
  echo "check_deps: error: could not parse 'cargo metadata' output" >&2
  exit 2
fi

status=0

if [ "$actual" = "$expected" ]; then
  echo "check_deps: OK - workspace edges match the allowed direction"
else
  diff -u --label expected --label actual \
    <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") || true
  echo "check_deps: FAIL - workspace dependency edges do not match the allowed direction"
  status=1
fi

violations=()
count=0
while read -r package dependency source; do
  [ -n "$package" ] || continue
  count=$((count + 1))
  allowed=false
  for name in "${approved[@]}"; do
    if [ "$dependency" = "$name" ]; then
      allowed=true
    fi
  done
  if [ "$allowed" != true ] || [ "$source" != "$crates_io" ]; then
    violations+=("  $package -> $dependency ($source)")
  fi
done <<<"$external"

# "1 crate" / "2 crates" gibi doğru tekil-çoğul.
plural() {
  if [ "$1" -eq 1 ]; then
    printf '%s %s' "$1" "$2"
  else
    printf '%s %ss' "$1" "$2"
  fi
}

if [ "${#violations[@]}" -eq 0 ]; then
  # Aynı crate birden fazla üye tarafından bildirilebilir: bildirim sayısı ile farklı crate sayısı
  # ayrı raporlanır.
  distinct=$(awk 'NF { print $2 }' <<<"$external" | sort -u | wc -l)
  echo "check_deps: OK - $(plural "$count" "external dependency declaration")" \
    "($(plural "$((distinct))" crate)), all from crates.io and on the approved list"
else
  printf '%s\n' "${violations[@]}"
  echo "check_deps: FAIL - external dependencies must come from crates.io and be on the approved" \
    "list:"
  echo "  ${approved[*]}"
  status=1
fi

# Cargo.lock: dolaylı olanlar dahil BÜTÜN paketlerin kaynağı crates.io olmalı. `--no-deps` yalnızca
# Cargo.toml bildirimlerini gördüğü için `[patch.crates-io]` ya da `.cargo/config.toml` ile yapılan
# bir git yönlendirmesi yukarıdaki denetimi atlatabilirdi; kilit dosyası ise gerçekte neyin
# derlendiğini söyler. Workspace üyelerinin `source` satırı yoktur.
lock_file="$repo_root/Cargo.lock"
if [ ! -f "$lock_file" ]; then
  echo "check_deps: error: '$lock_file' not found (it must be committed)" >&2
  exit 2
fi
# grep eşleşme bulamayınca 1 döner (ör. hiç dış paket yoksa); bu bir hata değil, `|| true` bu yüzden.
foreign_sources=$(grep -E '^source = ' "$lock_file" | grep -vxF "source = \"$crates_io\"" |
  sort -u || true)
if [ -z "$foreign_sources" ]; then
  echo "check_deps: OK - every package in Cargo.lock comes from crates.io"
else
  # Her satırı iki boşlukla girintile (ilk satır printf'in önekiyle, diğerleri yeni satırdan sonra).
  printf '  %s\n' "${foreign_sources//$'\n'/$'\n'  }"
  echo "check_deps: FAIL - Cargo.lock contains packages from sources other than crates.io"
  status=1
fi

exit "$status"
