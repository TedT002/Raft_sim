#!/usr/bin/env bash
# test_check_forbidden.sh — check_forbidden.sh için öz-test (self-test).
#
# Gerçek dosya fixture'ları oluşturur, check_forbidden.sh'i çağırır; çıkış kodunu, stdout ve
# stderr içeriğini doğrular. Her vaka kendi geçici dizininde çalışır ki vakalar birbirini
# etkilemesin. Çıktı TAP benzeridir ("ok - ..." / "not ok - ..."); son satır bir özet verir ve
# betik yalnızca hiçbir vaka başarısız olmadıysa 0 ile çıkar.
#
# Bu betik, bir CI kapısının bekçisidir: check_forbidden.sh sessizce bozulursa (bir kalıp
# eşleşmeyi bırakırsa, alt dizinler taranmazsa, sıralama kaybolursa) CI'ın bunu fark etmesinin
# tek yolu buradaki vakalardır.
set -euo pipefail

# check_forbidden.sh ile aynı gerekçeyle: yerel ayardan bağımsız karşılaştırmalar, CDPATH tuzağı
# yok.
export LC_ALL=C
unset CDPATH

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
script="$repo_root/scripts/check_forbidden.sh"

# NEDEN mktemp + trap EXIT: testler dosya sisteminde kalıcı iz bırakmamalı; betik başarıyla ya da
# hatayla (set -e altında erken çıkış dahil) bitse de geçici kök dizin daima temizlenir.
# `chmod -R u+rwx` önce gelir: "okunamayan alt dizin" vakası izinleri bilerek kısıtlar.
tmp_root=$(mktemp -d)
trap 'chmod -R u+rwx -- "$tmp_root" 2>/dev/null || true; rm -rf -- "$tmp_root"' EXIT

pass=0
fail=0
skipped=0

ok() {
  pass=$((pass + 1))
  printf 'ok - %s\n' "$1"
}

not_ok() {
  fail=$((fail + 1))
  printf 'not ok - %s\n' "$1"
}

# Atlanan vaka "geçti" SAYILMAZ: ayrı sayılır ve özet satırında görünür (TAP'teki # SKIP gibi).
skip() {
  skipped=$((skipped + 1))
  printf 'ok - %s # SKIP %s\n' "$1" "$2"
}

# Her çağrıda benzersiz bir alt dizin üretir (vakalar arası izolasyon).
# NEDEN mktemp (bir sayaç değişkeni yerine): bu fonksiyon `d=$(next_case_dir)` biçiminde komut
# ikamesi içinde çağrılır ve komut ikamesi bash'te ALT KABUKTA çalışır: orada artırılan bir sayaç
# dış kabuktaki kopyayı DEĞİŞTİRMEZ, her çağrı aynı dizini üretir ve vakalar birbirinin
# fixture'ını kirletir. mktemp durumsuzdur; bu hata sınıfını kökten önler.
#
# Aynı alt kabuk kuralı sayaçlar için de geçerlidir: ok/not_ok/skip ASLA `( ... )` veya `$( ... )`
# içinde çağrılmamalıdır; yoksa artış kaybolur ve başarısız bir vaka "0 failed" diye raporlanır.
next_case_dir() {
  mktemp -d -p "$tmp_root" case_XXXXXX
}

# run_and_check_exit NAME EXPECTED_EXIT CMD...
# CMD'yi çalıştırır; stdout'u out_stdout'a, stderr'i out_stderr'e koyar (her durumda) ve çıkış
# kodunu beklenenle karşılaştırır. Eşleşirse 0, eşleşmezse 1 döner. Çağıran, içerik
# doğrulamalarını yalnızca çıkış kodu doğruysa yapar: yanlış kodda içerik kontrolü anlamsızdır.
out_stdout=""
out_stderr=""
run_and_check_exit() {
  local name="$1" expected="$2"
  shift 2
  local actual=0
  local stderr_file="$tmp_root/.stderr_capture"
  out_stdout=$("$@" 2>"$stderr_file") || actual=$?
  out_stderr=$(<"$stderr_file")
  rm -f -- "$stderr_file"
  if [ "$actual" -eq "$expected" ]; then
    ok "$name (exit $expected)"
    return 0
  fi
  not_ok "$name (expected exit $expected, got $actual)"
  return 1
}

# assert_contains NAME HAYSTACK NEEDLE
assert_contains() {
  local name="$1" haystack="$2" needle="$3"
  case "$haystack" in
    *"$needle"*) ok "$name (contains '$needle')" ;;
    *) not_ok "$name (expected to contain '$needle', got: $haystack)" ;;
  esac
}

# assert_empty NAME VALUE
assert_empty() {
  local name="$1" value="$2"
  if [ -z "$value" ]; then
    ok "$name (empty)"
  else
    not_ok "$name (expected empty, got: $value)"
  fi
}

# assert_equals NAME EXPECTED ACTUAL — bayt bayt eşitlik; fark varsa ikisi de TAP yorumu
# olarak basılır.
assert_equals() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    ok "$name (exact match)"
  else
    not_ok "$name (output differs)"
    printf '# expected:\n%s\n# actual:\n%s\n' "$expected" "$actual"
  fi
}

# Tek satırlık bir fixture'da kalıbın yakalandığını (exit 1) ve doğru [etiket]le raporlandığını
# doğrular.
run_single_pattern_case() {
  local name="$1" code="$2" label="$3"
  local d
  d=$(next_case_dir)
  printf '%s\n' "$code" >"$d/a.rs"
  if run_and_check_exit "$name" 1 "$script" "$d"; then
    assert_contains "$name label" "$out_stdout" "[$label]"
  fi
}

# ---------------------------------------------------------------------------------------------
# Vaka grubu 1: temel 11 kalıbın her biri, gerçekçi birer kod satırıyla.
# ---------------------------------------------------------------------------------------------
run_single_pattern_case "pattern std::time"  'let t = std::time::Duration::ZERO;' "std::time"
run_single_pattern_case "pattern Instant"    'let i = Instant::now();'            "Instant"
run_single_pattern_case "pattern SystemTime" 'let s = SystemTime::now();'         "SystemTime"
run_single_pattern_case "pattern thread_rng" 'let mut r = thread_rng();'          "thread_rng"
run_single_pattern_case "pattern StdRng"     'type R = StdRng;'                   "StdRng"
run_single_pattern_case "pattern SmallRng"   'type R = SmallRng;'                 "SmallRng"
run_single_pattern_case "pattern HashMap"    'use std::collections::HashMap;'     "HashMap"
run_single_pattern_case "pattern HashSet"    'use std::collections::HashSet;'     "HashSet"
run_single_pattern_case "pattern tokio"      'use tokio::spawn;'                  "tokio"
run_single_pattern_case "pattern println!"   'println!("hi");'                    "println!"
run_single_pattern_case "pattern unsafe"     'unsafe { }'                         "unsafe"

# eprintln! aynı [println!] etiketiyle yakalanmalı (10 numaralı kalıp `e?println!`).
run_single_pattern_case "eprintln! caught as println!" 'eprintln!("x");' "println!"
# FxHashMap, HashMap alt dizge eşleşmesiyle yakalanmalı (bilerek sınırsız alt dizge).
run_single_pattern_case "FxHashMap caught as HashMap" 'type M = FxHashMap<u8, u8>;' "HashMap"

# ---------------------------------------------------------------------------------------------
# Vaka grubu 2: genişletilmiş kalıplar (async, iş parçacıkları, G/Ç, entropi kaynakları).
# ---------------------------------------------------------------------------------------------
run_single_pattern_case "pattern async"       'pub async fn foo() {}'      "async"
run_single_pattern_case "pattern std::thread" 'std::thread::spawn(|| {});' "std::thread"
run_single_pattern_case "pattern Mutex"       'let m = Mutex::new(0);'     "Mutex"
# print!/dbg! etiketi üç makroyu kapsar: print!, eprint!, dbg! (println!/eprintln! grup 1'de).
run_single_pattern_case "pattern print!"       'print!("x");'                                "print!/dbg!"
run_single_pattern_case "pattern eprint!"      'eprint!("x");'                               "print!/dbg!"
run_single_pattern_case "pattern dbg!"         'dbg!(x);'                                    "print!/dbg!"
run_single_pattern_case "pattern std::fs"      'std::fs::read_to_string("x").unwrap();'      "std::fs"
run_single_pattern_case "pattern std::net"     'std::net::TcpStream::connect("x").unwrap();' "std::net"
run_single_pattern_case "pattern std::env"     'std::env::var("X").unwrap();'                "std::env"
run_single_pattern_case "pattern std::process" 'std::process::exit(1);'                      "std::process"
run_single_pattern_case "pattern rand::rng"    'let mut r = rand::rng();'                    "rand::rng"
# rand::random* önek eşleşmesidir: çıplak çağrıyı ve random_range gibi türevleri de yakalamalı.
run_single_pattern_case "pattern rand::random (bare call)" 'let x: u8 = rand::random();' "rand::random"
run_single_pattern_case "pattern rand::random (prefix, random_range)" \
  'let x: u8 = rand::random_range(0..10);' "rand::random"
run_single_pattern_case "pattern ThreadRng"    'fn f(r: ThreadRng) {}'                    "ThreadRng"
run_single_pattern_case "pattern OsRng"        'let mut rng = OsRng;'                     "OsRng"
run_single_pattern_case "pattern getrandom"    'getrandom::getrandom(&mut buf).unwrap();' "getrandom"
run_single_pattern_case "pattern from_entropy" 'let rng = ChaCha8Rng::from_entropy();'    "from_entropy"
run_single_pattern_case "pattern from_os_rng"  'let rng = ChaCha8Rng::from_os_rng();'     "from_os_rng"

# ---------------------------------------------------------------------------------------------
# Vaka grubu 3: ek kalıplar — G/Ç, iş parçacığına özgü/küresel değiştirilebilir durum ve
# rastgele anahtarlı hash.
# ---------------------------------------------------------------------------------------------
run_single_pattern_case "pattern std::io"       'let out = std::io::stdout();'                  "std::io"
run_single_pattern_case "pattern thread_local!" 'thread_local!(static X: u8 = 0);'              "thread_local!"
run_single_pattern_case "pattern Atomic*"       'static N: AtomicU64 = AtomicU64::new(0);'      "Atomic*"
run_single_pattern_case "pattern OnceLock"      'static C: OnceLock<u8> = OnceLock::new();'     "OnceLock"
run_single_pattern_case "pattern LazyLock"      'static L: LazyLock<u8> = LazyLock::new(|| 1);' "LazyLock"
run_single_pattern_case "pattern RandomState"   'let s = RandomState::new();'                   "RandomState"
run_single_pattern_case "pattern DefaultHasher" 'let h = DefaultHasher::new();'                 "DefaultHasher"

# ---------------------------------------------------------------------------------------------
# Vaka grubu 4: yorum, tanımlayıcı ve URL kenar durumları (yanlış-pozitif OLMAMALI).
# ---------------------------------------------------------------------------------------------
d=$(next_case_dir)
{
  printf '// HashMap\n'
  printf '/// uses Instant\n'
  printf '//! tokio\n'
  printf 'let x = 1; // unsafe HashMap\n'
} >"$d/a.rs"
run_and_check_exit "comment-only mentions are ignored" 0 "$script" "$d" || true

d=$(next_case_dir)
printf '#![forbid(unsafe_code)]\n' >"$d/a.rs"
run_and_check_exit "forbid attribute is not a violation" 0 "$script" "$d" || true

d=$(next_case_dir)
printf 'let unsafe_count = 0; let not_tokio_x = 1; let atomic_ops = 2; let my_io = 3;\n' >"$d/a.rs"
run_and_check_exit "identifiers containing a word are ignored" 0 "$script" "$d" || true

d=$(next_case_dir)
printf 'let u = "http://example.com"; let m = HashMap::new();\n' >"$d/a.rs"
if run_and_check_exit "URL in string is not a comment" 1 "$script" "$d"; then
  assert_contains "URL in string label" "$out_stdout" "[HashMap]"
fi

d=$(next_case_dir)
{
  printf '// line 1: benign\n'
  printf '// line 2: benign\n'
  printf 'unsafe { }\n'
} >"$d/a.rs"
if run_and_check_exit "violation on line 3 reported correctly" 1 "$script" "$d"; then
  assert_contains "line/path format" "$out_stdout" "a.rs:3: ["
fi

# ---------------------------------------------------------------------------------------------
# Vaka grubu 5: "altın" çıktı — sıra, biçim, alt dizin ve özet satırı TEK karşılaştırmada.
# Dosyalar bilerek sözlük sırasından FARKLI bir sırayla oluşturulur (z, sub/b, a): find'ın
# döndürdüğü sıra dosya sistemine bağlıdır, dolayısıyla çıktının a, sub/b, z sırasıyla gelmesi
# `LC_ALL=C sort -z` adımını gerçekten sınar. sub/b.rs alt dizin taramasını sınar (find yerine
# yalnızca "$DIR"/*.rs taransaydı bu ihlal kaçardı). z.rs'nin 2. satırı aynı satırda iki kalıbı
# tetikler: çıktı, kalıp tablosunun sırasını izlemelidir (önce std::time, sonra Instant).
# ---------------------------------------------------------------------------------------------
golden=$(next_case_dir)
mkdir -p "$golden/sub"
printf 'unsafe { }\nlet t = std::time::Instant::now();\n' >"$golden/z.rs"
printf 'let i = Instant::now();\n' >"$golden/sub/b.rs"
printf '// clean line\nuse std::collections::HashMap;\n' >"$golden/a.rs"
expected_golden="$golden/a.rs:2: [HashMap] use std::collections::HashMap;
$golden/sub/b.rs:1: [Instant] let i = Instant::now();
$golden/z.rs:1: [unsafe] unsafe { }
$golden/z.rs:2: [std::time] let t = std::time::Instant::now();
$golden/z.rs:2: [Instant] let t = std::time::Instant::now();
check_forbidden: FAIL - 5 violation(s) in 3 file(s) scanned"
if run_and_check_exit "golden fixture (order, format, subdirectory, summary)" 1 \
  "$script" "$golden"; then
  assert_equals "golden stdout" "$expected_golden" "$out_stdout"
  assert_empty "golden stderr" "$out_stderr"
fi

# Determinizm: altın fixture iki kez koşulur. Çıkış kodları ayrıca kontrol edilir: çöken iki koşu
# iki BOŞ çıktı üretir ve "bayt bayt aynı" görünürdü; bu yüzden exit 1 ve boş olmayan çıktı şart.
rc1=0
run1=$("$script" "$golden" 2>/dev/null) || rc1=$?
rc2=0
run2=$("$script" "$golden" 2>/dev/null) || rc2=$?
if [ "$rc1" -eq 1 ] && [ "$rc2" -eq 1 ] && [ -n "$run1" ] && [ "$run1" = "$run2" ]; then
  ok "determinism: two runs on the golden fixture are byte-identical (exit 1, non-empty)"
else
  not_ok "determinism: two runs differ or did not fail as expected (rc1=$rc1, rc2=$rc2)"
fi

# ---------------------------------------------------------------------------------------------
# Vaka grubu 6: kullanım ve yapılandırma hataları (çıkış kodu + mesaj + doğru akış).
# ---------------------------------------------------------------------------------------------
if run_and_check_exit "-h prints usage and exits 0" 0 "$script" -h; then
  assert_contains "-h usage goes to stdout" "$out_stdout" "Usage: check_forbidden.sh"
  assert_empty "-h stderr" "$out_stderr"
fi

if run_and_check_exit "--help prints usage and exits 0" 0 "$script" --help; then
  assert_contains "--help usage goes to stdout" "$out_stdout" "Usage: check_forbidden.sh"
fi

d=$(next_case_dir)
if run_and_check_exit "too many arguments is a usage error" 2 "$script" "$d" "$d"; then
  assert_contains "too many arguments: usage goes to stderr" "$out_stderr" \
    "Usage: check_forbidden.sh"
  assert_empty "too many arguments: stdout" "$out_stdout"
fi

d=$(next_case_dir)
if run_and_check_exit "empty dir (no *.rs files) is a config error" 2 "$script" "$d"; then
  assert_contains "empty dir message" "$out_stderr" "check_forbidden: error: no *.rs files"
fi

if run_and_check_exit "missing dir is a config error" 2 "$script" "$tmp_root/does-not-exist"; then
  assert_contains "missing dir message" "$out_stderr" "check_forbidden: error:"
fi

# Boş dizge, varsayılan ağaca DÜŞMEMELİ: `check_forbidden.sh "$DIR"` çağrısında DIR yanlışlıkla
# boş kalırsa başka bir ağacı tarayıp yanlış bir "OK" vermek yerine hata vermeli.
if run_and_check_exit "empty-string DIR is a config error" 2 "$script" ""; then
  assert_contains "empty-string DIR message" "$out_stderr" "is not a directory"
fi

# Göreli DIR, çağıranın cwd'sine göre çözülmeli. `cd` yalnızca çalıştırılan komutun kendi
# sürecinde (bash -c) olur; sayaçlar dış kabukta kalır.
d=$(next_case_dir)
printf 'use std::collections::HashMap;\n' >"$d/a.rs"
# shellcheck disable=SC2016 # "$1".."$3" bilerek iç bash'in konumsal argümanlarıdır.
if run_and_check_exit "relative DIR is resolved against the caller's cwd" 1 \
  bash -c 'cd -- "$1" && exec "$2" "$3"' _ "$tmp_root" "$script" "$(basename -- "$d")"; then
  assert_contains "relative DIR label" "$out_stdout" "[HashMap]"
fi

# Okunamayan alt dizin: find'ın hatası yutulsaydı o alt ağaç hiç taranmaz ve betik yanlışlıkla
# yeşil çıkardı. Beklenen: exit 2 ve anlamlı bir hata. root olarak çalışırken dosya izinleri
# uygulanmadığı için vaka atlanır (atlanan vaka geçti sayılmaz).
d=$(next_case_dir)
printf 'fn clean() {}\n' >"$d/a.rs"
mkdir "$d/locked"
chmod 000 "$d/locked"
if [ "$(id -u)" -eq 0 ]; then
  skip "unreadable subdirectory is a config error" "running as root; permissions are not enforced"
elif run_and_check_exit "unreadable subdirectory is a config error" 2 "$script" "$d"; then
  assert_contains "unreadable subdirectory message" "$out_stderr" "could not list every file"
fi
chmod 755 "$d/locked"

# ---------------------------------------------------------------------------------------------
# Vaka grubu 7: gerçek ağaç (argümansız çağrı => crates/raft-core) temiz olmalı. Koşulsuz
# çalışır: ağaç taşınır veya silinirse bu vaka exit 2 ile kırmızıya döner (sessiz SKIP yok).
# ---------------------------------------------------------------------------------------------
run_and_check_exit "real tree (crates/raft-core) is clean" 0 "$script" || true

summary="test_check_forbidden: $pass passed, $fail failed"
if [ "$skipped" -gt 0 ]; then
  summary="$summary, $skipped skipped"
fi
printf '%s\n' "$summary"

if [ "$fail" -ne 0 ]; then
  exit 1
fi
exit 0
