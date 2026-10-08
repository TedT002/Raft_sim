#!/usr/bin/env bash
# Rust kaynaklarında satır uzunluğu denetimi: hiçbir satır 100 KARAKTERİ aşamaz.
#
# Neden ayrı bir denetim: rustfmt kodu 100 sütuna sarar ama yorumları sarmaz (yorum sarma kararsız
# bir özellik). Bu projenin yorumları Türkçedir ve "ç, ğ, ı, ö, ş, ü" gibi harfler UTF-8'de iki bayt
# tutar: bayt sayan araçlar (LC_ALL=C ile awk ya da wc -c) tam sınırdaki satırları yanlış işaretler.
# Bash, UTF-8 yerelinde `${#line}` ile karakter sayar; denetim bunu kullanır ve ek araç istemez.
#
# Kullanım: scripts/check_line_length.sh [dizin...]   (varsayılan: crates)
set -euo pipefail

export LC_ALL=C.UTF-8
root="$(cd "$(dirname "$0")/.." && pwd)"
limit=100

if (($# == 0)); then
  set -- "$root/crates"
fi

status=0
checked=0
while IFS= read -r -d '' file; do
  checked=$((checked + 1))
  number=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    number=$((number + 1))
    if ((${#line} > limit)); then
      echo "check_line_length: ${file#"$root"/}:$number: ${#line} characters (limit $limit)"
      status=1
    fi
  done <"$file"
done < <(find "$@" -name '*.rs' -type f -print0 | sort -z)

if ((status == 0)); then
  echo "check_line_length: OK - $checked file(s) scanned, no line over $limit characters"
else
  echo "check_line_length: FAIL - wrap the lines above (comments are not wrapped by rustfmt)"
fi
exit "$status"
