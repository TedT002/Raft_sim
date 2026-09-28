#!/usr/bin/env bash
# check_deps.sh — workspace içindeki path (iç) bağımlılıkların yönünü mekanik olarak denetler.
# İzinli yön: cli -> sim -> raft-core, cli -> checker, sim -> checker. raft-core hiçbir workspace
# crate'ine bağımlı olmaz (sans-IO çekirdek dış dünyayı bilmez); checker da raft-core'u bilmez
# (kâhin, denetlediği uygulamadan bağımsız kalır). Amaç: `raft-core -> sim` veya
# `checker -> raft-core` gibi yönü ihlal eden bir kenarın, biri Cargo.toml'a bir satır eklediğinde
# insan incelemesine bel bağlamadan CI'da hemen yakalanması.
#
# Çıkış kodları: 0 = kenarlar birebir izinli liste, 1 = fark var (diff basılır),
# 2 = yapılandırma hatası (cargo/jq yok, `cargo metadata` başarısız, çıktı ayrıştırılamadı).
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

if [ "$actual" = "$expected" ]; then
  echo "check_deps: OK - workspace edges match the allowed direction"
  exit 0
fi

diff -u --label expected --label actual \
  <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") || true
echo "check_deps: FAIL - workspace dependency edges do not match the allowed direction"
exit 1
