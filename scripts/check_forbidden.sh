#!/usr/bin/env bash
# check_forbidden.sh — raft-core crate'indeki (ya da verilen DIR altındaki) *.rs dosyalarını,
# determinizmi SESSİZCE bozan yasaklı kalıplar için tarar: gerçek saat, entropi kaynaklı
# rastgelelik, yineleme sırası rastgele koleksiyonlar, iş parçacıkları ve paylaşılan durum, G/Ç
# ve `unsafe`. Bir "tripwire" (tuzak teli) betiğidir: yanlış-pozitif kabul edilebilir (tutucu
# davranır), yanlış-negatif en aza indirilmeye çalışılır. Asıl anlamsal koruma, derleyici
# düzeyindeki crates/raft-core/clippy.toml ile insan incelemesidir.
#
# Bilinen sınırlamalar (bilerek kabul edildi):
#   - Blok yorumları (/* ... */) ayrıştırılmaz: içlerindeki kalıplar "ihlal" sayılabilir
#     (yanlış-pozitif, güvenli taraf).
#   - Bir string literal içindeki, öncesinde boşluk olan "//" satırın geri kalanını yorum sanıp
#     keser (nadir yanlış-negatif).
#   - Süslü parantezli import'lar tek tek kalıpları atlatabilir: `use std::{time::Instant}`
#     1 numaralı kalıbı (`std::time`) atlatır ama genellikle sonraki bir kalıp (`Instant`) yine
#     yakalar; `use std::{thread}` + `thread::spawn(..)` ise HİÇ yakalanmaz. Bu boşlukları
#     crates/raft-core/clippy.toml kapatır.
#   - Yalnızca DIR altındaki normal dosyalar taranır: sembolik bağlantı olan *.rs dosyaları
#     (`find -type f`) ve `#[path = ...]` / `include!` ile DIR dışından gelen kod taranmaz.
#
# Gereksinimler: bash >= 4.4 (`mapfile -d ''`), GNU find/sort (`-print0`, `-z`), POSIX awk
# (mawk yeterli; gawk'a özgü hiçbir özellik kullanılmaz).
set -euo pipefail

# Ortam farkları sonucu sessizce değiştirmesin diye:
# - LC_ALL=C: find/sort/awk yerel ayardan (ör. tr_TR.UTF-8) bağımsız, bayt bayt çalışır. Aksi
#   hâlde sıralama ve çok baytlı karakter işleme makineden makineye değişebilir; bu betiğin
#   var olma sebebi de tam olarak bu tür sessiz ortam farklarını yakalamaktır.
# - CDPATH: dışa aktarılmış bir CDPATH, `cd` ile göreli bir yolu beklenmedik bir dizine
#   çözebilir ve o yolu stdout'a basarak `$(cd ... && pwd)` sonucunu bozabilir.
export LC_ALL=C
unset CDPATH

if ((BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 4))); then
  echo "check_forbidden: error: bash >= 4.4 is required (found $BASH_VERSION)" >&2
  exit 2
fi

# Betik nerede saklanırsa saklansın repo kökü = betiğin bulunduğu dizinin bir üstü; böylece
# hangi cwd'den çağrılırsa çağrılsın (repo kökü, /tmp, başka bir yer) aynı sonucu verir.
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

usage() {
  cat <<'EOF'
Usage: check_forbidden.sh [DIR]

Scans *.rs files under DIR (default: crates/raft-core) for patterns that would
silently break raftsim's determinism guarantees: wall-clock time, entropy-seeded
randomness, hash-ordered collections, threads and shared state, I/O, unsafe.

Exit codes:
  0  no forbidden pattern found
  1  at least one forbidden pattern found
  2  usage or configuration error (bad DIR, unlistable tree, no *.rs files)
EOF
}

# -h/--help tek argüman olarak verildiğinde her zaman öncelikli işlenir.
if [ "$#" -eq 1 ] && { [ "$1" = "-h" ] || [ "$1" = "--help" ]; }; then
  usage
  exit 0
fi

if [ "$#" -gt 1 ]; then
  usage >&2
  exit 2
fi

# `${1-...}` (iki nokta YOK): varsayılan yalnızca argüman HİÇ verilmediğinde kullanılır. Boş bir
# dizge ("") bilinçli olarak varsayılana düşmez, aşağıdaki dizin kontrolünde exit 2 alır; aksi
# hâlde `check_forbidden.sh "$DIR"` çağrısında DIR yanlışlıkla boş kalırsa betik sessizce başka
# bir ağacı tarayıp yanlış bir "OK" verebilirdi.
#
# Varsayılan tarama kökü `crates/raft-core/src` değil crate'in kendisidir: raft-core'a ileride
# tests/, benches/ veya build.rs eklendiğinde onlar da kendiliğinden kapsama girer.
target_dir=${1-"$repo_root/crates/raft-core"}

if [ ! -d "$target_dir" ]; then
  echo "check_forbidden: error: '$target_dir' is not a directory" >&2
  exit 2
fi

# Girdi dizinini mutlak yola çeviriyoruz: çıktıda repo_root önekini tutarlı biçimde sıyırmak
# için (göreli bir DIR verilirse find çıktısı da göreli olurdu) ve awk'a verilen her dosya adı
# "/" ile başlasın diye — awk, `ad=değer` biçimindeki bir işleneni dosya değil değişken ataması
# sayar; mutlak yollar bu tuzağa hiç düşmez.
abs_dir=$(cd -- "$target_dir" && pwd)

# Dosya listesi önce geçici bir dosyaya yazılır: `< <(find ...)` gibi bir süreç ikamesi içindeki
# hata `set -e`/`pipefail`'e GÖRÜNMEZ; okunamayan bir alt dizin sessizce atlanır ve betik
# yanlışlıkla yeşil çıkardı. Burada find'ın çıkış kodu doğrudan kontrol edilir.
list_file=$(mktemp)
trap 'rm -f -- "$list_file"' EXIT
if ! find "$abs_dir" -type f -name '*.rs' -print0 >"$list_file"; then
  echo "check_forbidden: error: could not list every file under '$target_dir'" >&2
  exit 2
fi

# NUL ile ayrılmış liste: dosya adında boşluk veya yeni satır olsa bile güvenli. LC_ALL=C altında
# `sort` salt bayt değerine göre sıralar; böylece aynı dosya kümesi her makinede ve CI'da aynı
# sırada taranır ve çıktı deterministik olur.
sort -z -o "$list_file" "$list_file"
mapfile -d '' -t files <"$list_file"

if [ "${#files[@]}" -eq 0 ]; then
  echo "check_forbidden: error: no *.rs files found under '$target_dir'" >&2
  exit 2
fi

# Tüm tarama tek bir awk sürecinde yapılır ve yalnızca POSIX ERE kullanır: mawk'ta \b, \y, \<
# gibi kelime sınırı kısayolları yoktur. Onların yerine, iki yanına birer boşluk eklenmiş metinde
# W = [^A-Za-z0-9_] karakter sınıfı kullanılır.
#
# repo_root awk'a `-v` ile DEĞİL ortam değişkeniyle geçirilir: `-v`, değerdeki ters bölü
# kaçışlarını (\n, \t ...) yorumlar ve yolu bozabilir; ENVIRON[] değeri olduğu gibi okur. Dosya
# sayısı (nfiles) bash'ten gelir: awk içinde `FNR == 1` ile saymak boş dosyaları atlardı.
REPO_ROOT="$repo_root" awk -v nfiles="${#files[@]}" '
  function add(label, pat) {
    n++
    labels[n] = label
    pats[n] = pat
  }

  BEGIN {
    n = 0
    W = "[^A-Za-z0-9_]"

    # --- Temel 11 kalıp. Tablo sırası, aynı satırdaki birden çok ihlalin çıktı
    #     sırasını da belirler.
    add("std::time",     W "std::time" W)
    add("Instant",       W "Instant" W)
    add("SystemTime",    W "SystemTime" W)
    add("thread_rng",    W "thread_rng" W)
    add("StdRng",        W "StdRng" W)
    add("SmallRng",      W "SmallRng" W)
    # HashMap/HashSet bilerek sınırsız alt dizgedir: FxHashMap, AHashMap gibi türevleri de yakalar.
    add("HashMap",       "HashMap")
    add("HashSet",       "HashSet")
    add("tokio",         W "tokio" W)
    # `e?println!`: eprintln! de aynı etiketle yakalanır.
    add("println!",      W "e?println!")
    # `unsafe` bir kelime olarak aranır: `#![forbid(unsafe_code)]` eşleşmez ("_" kelime karakteri).
    add("unsafe",        W "unsafe" W)

    # --- Genişletilmiş kalıplar: async, iş parçacıkları, G/Ç ve entropi kaynakları.
    #     Sağında sınır OLMAYAN kalıplar (std::thread, rand::random, thread_local!, Atomic*)
    #     önek eşleşmesidir: std::thread::spawn, rand::random_range gibi alt yolları da yakalar.
    add("async",         W "async" W)
    add("std::thread",   W "std::thread")
    add("Mutex",         W "Mutex" W)
    # print!/dbg! etiketi üç makroyu kapsar (print!, eprint!, dbg!); println! ve eprintln! ise
    # 10 numaralı kalıptadır.
    add("print!/dbg!",   W "(e?print|dbg)!")
    add("std::fs",       W "std::fs" W)
    add("std::net",      W "std::net" W)
    add("std::env",      W "std::env" W)
    add("std::process",  W "std::process" W)
    add("rand::rng",     W "rand::rng" W)
    add("rand::random",  W "rand::random")
    add("ThreadRng",     W "ThreadRng" W)
    add("OsRng",         W "OsRng" W)
    add("getrandom",     W "getrandom" W)
    add("from_entropy",  W "from_entropy" W)
    add("from_os_rng",   W "from_os_rng" W)

    # --- Ek kalıplar: G/Ç, iş parçacığına özgü veya küresel değiştirilebilir durum ve rastgele
    #     anahtarlı hash. Faz 5 ile birlikte seed koşuları aynı süreçte paralel yürüyecek: iş
    #     parçacıkları arasında paylaşılan küresel durum bir koşunun sonucunu diğerine sızdırır,
    #     rastgele anahtarlı bir hasher ise aynı seed ile farklı bir koşu üretir.
    #     (Not: bu awk programı tek tırnak içinde durduğu için yorumlarda kesme işareti yok.)
    add("std::io",       W "std::io" W)
    add("thread_local!", W "thread_local!")
    add("Atomic*",       W "Atomic[A-Z]")
    add("OnceLock",      W "OnceLock" W)
    add("LazyLock",      W "LazyLock" W)
    add("RandomState",   W "RandomState" W)
    add("DefaultHasher", W "DefaultHasher" W)

    violations = 0
    prefix = ENVIRON["REPO_ROOT"] "/"
  }

  {
    line = $0

    if (line ~ /^[ \t]*\/\//) {
      # Adım 1: satır (baştaki boşluk hariç) doğrudan "//" ile başlıyorsa (//, /// veya //!)
      # satırın TAMAMI yorumdur: taranacak kod yok.
      code = ""
    } else if (match(line, /[ \t]\/\//)) {
      # Adım 2: satır sonu yorumunu at. Yalnızca bir boşluk/sekmeden HEMEN SONRA gelen "//"
      # yorum başlangıcı sayılır; böylece "http://..." gibi, öncesinde boşluk olmayan bir string
      # içeriği silinmez. Silmemek tutucu taraftır: ihlal kaçırmaz.
      code = substr(line, 1, RSTART - 1)
    } else {
      code = line
    }

    # Çıktıdaki <code> alanı için baştaki ve sondaki boşlukları kırp.
    gsub(/^[ \t]+/, "", code)
    gsub(/[ \t]+$/, "", code)

    # Adım 3: iki yana birer boşluk ekle; kelime sınırı artık W ile ifade edilebilir.
    padded = " " code " "

    # Dosya repo içindeyse yolu repo köküne göreli göster: makineden makineye değişmeyen,
    # okunabilir ve deterministik bir çıktı için.
    path = FILENAME
    if (index(path, prefix) == 1) {
      path = substr(path, length(prefix) + 1)
    }

    for (i = 1; i <= n; i++) {
      if (padded ~ pats[i]) {
        print path ":" FNR ": [" labels[i] "] " code
        violations++
      }
    }
  }

  END {
    if (violations > 0) {
      printf("check_forbidden: FAIL - %d violation(s) in %d file(s) scanned\n", violations, nfiles)
      exit 1
    }
    printf("check_forbidden: OK - %d file(s) scanned, no forbidden patterns\n", nfiles)
    exit 0
  }
' "${files[@]}"
