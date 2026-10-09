//! Bir koşunun zaman çizelgesi, bağımlılıksız bir SVG olarak (`raftsim replay --svg`).
//!
//! Her düğüm bir şerittir; şerit, düğümün o andaki hâline göre boyanır: takipçi, aday, lider
//! (term'iyle) ya da çökmüş. Üstte hatalar (çökme, yeniden başlatma, bölünme, ...) işaretlenir;
//! koşu başarısız olduysa ihlalin anı kırmızı bir çizgiyle gösterilir. İstenirse teslim edilen
//! mesajlar da çizilir (gönderen şeritten alıcı şeride, gönderim anından teslim anına).
//!
//! Neden SVG: dosya kendi kendine yeterlidir (tarayıcıda ve GitHub'da açılır), metin olduğu için
//! deterministiktir (aynı koşu bayt bayt aynı dosyayı verir) ve yeni bir bağımlılık gerektirmez.
//! Bir TUI kütüphanesi onaylı bağımlılık listesinde değildir.
//!
//! Çizim saf bir fonksiyondur ([`render`]): koşudan toplanan [`Chart`]'ı alır, SVG metnini
//! döndürür. Koşudan veri toplamak `chart` modülünün işidir.

use std::fmt::Write;

/// Bir düğümün bir aralıktaki hâli.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Çökmüş.
    Down,
    /// Takipçi.
    Follower,
    /// Aday.
    Candidate,
    /// Lider.
    Leader,
}

/// Bir şeridin bir aralığı: `node` düğümü `[from, to)` boyunca `lane` hâlinde ve `term`'de.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    /// Düğüm (şeridin sırası `Chart::nodes`'taki sırasıdır).
    pub node: u64,
    /// Başlangıç (tick, dahil).
    pub from: u64,
    /// Bitiş (tick, hariç).
    pub to: u64,
    /// Hâl.
    pub lane: Lane,
    /// Term.
    pub term: u64,
}

/// Üst şeritteki bir işaret: bir hata ya da bölünmenin başlangıcı/bitişi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    /// An (tick).
    pub at: u64,
    /// Kısa etiket.
    pub label: String,
}

/// Teslim edilmiş bir mesaj: gönderen, alıcı, gönderim ve teslim anları.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arrow {
    /// Gönderen düğüm.
    pub from: u64,
    /// Alıcı düğüm.
    pub to: u64,
    /// Gönderim anı (tick).
    pub sent: u64,
    /// Teslim anı (tick).
    pub delivered: u64,
}

/// Çizilecek her şey.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Chart {
    /// Başlık (ör. seed, profil ve sonuç).
    pub title: String,
    /// Alt başlık (ör. koşuyu yeniden üreten komut); çok uzunsa kısaltılır.
    pub subtitle: String,
    /// Düğümler, şerit sırasıyla.
    pub nodes: Vec<u64>,
    /// Çizilen zaman penceresi `[start, end)`.
    pub window: (u64, u64),
    /// Şerit aralıkları.
    pub segments: Vec<Segment>,
    /// Hatalar.
    pub markers: Vec<Marker>,
    /// Teslim edilmiş mesajlar (yalnızca istenirse).
    pub arrows: Vec<Arrow>,
    /// Koşu başarısız olduysa ihlalin anı ve kısa açıklaması.
    pub failure: Option<(u64, String)>,
}

// Ölçüler (piksel). Etiket sütunu; şeritlerin üst kenarı (üstünde başlık, lejant ve eğik hata
// etiketleri durur); şerit yüksekliği ve aralığı; çizim genişliği; sağ kenar boşluğu (sağ kenara
// yakın eğik etiketler taşmasın); eksen yüksekliği.
const LABELS: f64 = 64.0;
const TOP: f64 = 150.0;
const LANE: f64 = 26.0;
const GAP: f64 = 10.0;
const PLOT: f64 = 1100.0;
const MARGIN: f64 = 64.0;
const AXIS: f64 = 34.0;

// Stiller (SVG öznitelikleri). Renkler açık arka plan üzerinde okunur.
const FONT: &str = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
const TITLE: &str = r##"font-size="13" fill="#1f2328""##;
const SUBTITLE: &str = r##"font-size="10" fill="#57606a""##;
const LEGEND: &str = r##"fill="#1f2328""##;
const NODE_LABEL: &str = r##"fill="#1f2328" text-anchor="end""##;
const TERM_LABEL: &str = r##"fill="#ffffff" text-anchor="middle""##;
const MARKER_LINE: &str = r##"stroke="#8c959f" stroke-width="0.6" stroke-dasharray="2 3""##;
const MESSAGE_LINE: &str = r##"stroke="#0969da" stroke-opacity="0.35" stroke-width="0.8""##;
const FAILURE_LINE: &str = r##"stroke="#d1242f" stroke-width="2""##;
const FAILURE_LABEL: &str = r##"fill="#d1242f" text-anchor="end""##;
const FAILURE_LABEL_START: &str = r##"fill="#d1242f""##;
const AXIS_LINE: &str = r##"stroke="#57606a""##;
const AXIS_LABEL: &str = r##"fill="#57606a" text-anchor="middle""##;
const AXIS_TITLE: &str = r##"fill="#57606a" text-anchor="end""##;

/// Şerit renkleri: açık arka plan üzerinde okunur, renk körlüğünde de ayrışan dört ton.
fn fill(lane: Lane) -> &'static str {
    match lane {
        Lane::Down => "#4a4a4a",
        Lane::Follower => "#d7dde5",
        Lane::Candidate => "#e9a23b",
        Lane::Leader => "#2f8f5b",
    }
}

/// Alt başlığın en fazla karakter sayısı: çizim genişliğine 10 piksellik yazıyla sığan kadar.
const SUBTITLE_CHARS: usize = 180;

/// En fazla `limit` karaktere sığmayan bir metni kısaltır ve kısaltıldığını `…` ile gösterir.
fn shorten(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let mut short: String = text.chars().take(limit.saturating_sub(1)).collect();
    short.push('…');
    short
}

/// XML metin kaçışı: etiketlerde `<`, `>`, `&` ve tırnaklar olabilir.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// Öğe yazıcıları. `write!` bir `String`'e yazarken başarısız olamaz; sonuç bilerek yok sayılır.

/// `<line>`: iki uç ve stil öznitelikleri.
fn line(svg: &mut String, (x1, y1): (f64, f64), (x2, y2): (f64, f64), style: &str) {
    let _ = writeln!(
        svg,
        r#"<line x1="{x1:.1}" y1="{y1:.1}" x2="{x2:.1}" y2="{y2:.1}" {style}/>"#
    );
}

/// `<text>`: konum, stil öznitelikleri ve XML'e kaçışlanmış metin.
fn text(svg: &mut String, (x, y): (f64, f64), style: &str, content: &str) {
    let _ = writeln!(
        svg,
        r#"<text x="{x:.1}" y="{y:.1}" {style}>{}</text>"#,
        escape(content)
    );
}

/// `<rect>`: sol üst köşe, boyut ve dolgu rengi.
fn rect(svg: &mut String, (x, y): (f64, f64), (width, height): (f64, f64), fill: &str) {
    let _ = writeln!(
        svg,
        r#"<rect x="{x:.1}" y="{y:.1}" width="{width:.1}" height="{height:.1}" fill="{fill}"/>"#
    );
}

/// Bir anın yatay konumu: pencere çizim genişliğine doğrusal olarak eşlenir; pencere dışı anlar
/// kenarlara kırpılır.
fn x_of(chart: &Chart, at: u64) -> f64 {
    let (start, end) = chart.window;
    let span = end.saturating_sub(start).max(1);
    let at = at.clamp(start, end);
    // Kayan nokta yalnızca çizim içindir: koşunun kendisi tamamen tam sayıdır.
    #[allow(clippy::cast_precision_loss)]
    let ratio = (at - start) as f64 / span as f64;
    LABELS + ratio * PLOT
}

/// Bir düğümün şeridinin üst kenarı.
fn y_of(chart: &Chart, node: u64) -> Option<f64> {
    let row = chart.nodes.iter().position(|&id| id == node)?;
    #[allow(clippy::cast_precision_loss)]
    let row = row as f64;
    Some(TOP + row * (LANE + GAP))
}

/// Eksen çentikleri arası süre: pencereye ~10 çentik düşecek kadar yuvarlak bir değer.
fn tick_step(span: u64) -> u64 {
    let raw = (span / 10).max(1);
    let mut step = 1;
    while step * 10 <= raw {
        step *= 10;
    }
    if step * 5 <= raw {
        step * 5
    } else if step * 2 <= raw {
        step * 2
    } else {
        step
    }
}

/// Zaman çizelgesini SVG metni olarak çizer. Aynı `Chart` her zaman aynı baytları verir.
#[must_use]
pub fn render(chart: &Chart) -> String {
    let lanes_height = {
        #[allow(clippy::cast_precision_loss)]
        let count = chart.nodes.len().max(1) as f64;
        count * (LANE + GAP)
    };
    let width = LABELS + PLOT + MARGIN;
    let height = TOP + lanes_height + AXIS + 28.0;
    let mut svg = String::new();
    let _ = write!(
        svg,
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0}" height="{height:.0}""#
    );
    let _ = writeln!(
        svg,
        r#" viewBox="0 0 {width:.0} {height:.0}" font-family="{FONT}" font-size="11">"#
    );
    rect(&mut svg, (0.0, 0.0), (width, height), "#ffffff");
    text(&mut svg, (LABELS, 18.0), TITLE, &chart.title);
    text(
        &mut svg,
        (LABELS, 34.0),
        SUBTITLE,
        &shorten(&chart.subtitle, SUBTITLE_CHARS),
    );
    render_legend(&mut svg);
    render_markers(&mut svg, chart, lanes_height);
    render_lanes(&mut svg, chart);
    render_arrows(&mut svg, chart);
    render_axis(&mut svg, chart, lanes_height);
    if let Some((at, reason)) = &chart.failure {
        let bottom = TOP + lanes_height;
        let label_y = bottom + AXIS + 18.0;
        if (chart.window.0..chart.window.1).contains(at) {
            let x = x_of(chart, *at);
            line(&mut svg, (x, TOP - 6.0), (x, bottom), FAILURE_LINE);
            text(&mut svg, (x - 4.0, label_y), FAILURE_LABEL, reason);
        } else {
            // Pencerenin dışındaki bir ihlalin çizgisi kenara kırpılsaydı, ihlal o kenardaki anda
            // olmuş gibi görünürdü: yalnızca etiketi, hangi yönde olduğunu gösteren bir okla
            // kenarda yazılır.
            let (x, style, label) = if *at < chart.window.0 {
                (LABELS, FAILURE_LABEL_START, format!("← {reason}"))
            } else {
                (LABELS + PLOT, FAILURE_LABEL, format!("{reason} →"))
            };
            text(&mut svg, (x, label_y), style, &label);
        }
    }
    svg.push_str("</svg>\n");
    svg
}

fn render_legend(svg: &mut String) {
    let entries = [
        (Lane::Follower, "follower"),
        (Lane::Candidate, "candidate"),
        (Lane::Leader, "leader (term)"),
        (Lane::Down, "down"),
    ];
    // Başlığın altındaki satır: uzun bir başlıkla çakışmasın.
    let mut x = LABELS;
    for (lane, label) in entries {
        rect(svg, (x, 46.0), (12.0, 12.0), fill(lane));
        text(svg, (x + 16.0, 56.0), LEGEND, label);
        x += 120.0;
    }
}

/// Eğik etiketler arasındaki en küçük yatay uzaklık (piksel): daha yakın bir etiket öncekinin
/// üstüne binerdi.
const LABEL_SPACING: f64 = 11.0;

/// Bir işaret etiketinin en fazla karakter sayısı. -55° eğik, 10 piksellik bir karakter ~4,9 piksel
/// yükselir: 16 karakterlik bir etiket şeritlerin üstünden (y = TOP - 8) lejantın altına kadar
/// sığar ve başlığı, alt başlığı ya da lejantı hiçbir pencerede örtmez. Ayrıntı `--trace`'tedir.
const MARKER_CHARS: usize = 16;

fn render_markers(svg: &mut String, chart: &Chart, lanes_height: f64) {
    let bottom = TOP + lanes_height;
    // Aynı andaki işaretler tek etikette birleşir: ilkinin etiketi ve kaç işaret daha olduğu (ör.
    // sakinleşmenin iyileştirmesi ve yeniden başlatmaları: `heal +2`). İşaretler zaman
    // sırasıyla gelir.
    let mut grouped: Vec<(u64, &str, usize)> = Vec::new();
    for marker in &chart.markers {
        if marker.at < chart.window.0 || marker.at >= chart.window.1 {
            continue;
        }
        match grouped.last_mut() {
            Some((at, _, more)) if *at == marker.at => *more += 1,
            _ => grouped.push((marker.at, &marker.label, 0)),
        }
    }
    let mut last_label: Option<f64> = None;
    for (at, first, more) in grouped {
        let label = if more == 0 {
            shorten(first, MARKER_CHARS)
        } else {
            let suffix = format!(" +{more}");
            let room = MARKER_CHARS.saturating_sub(suffix.chars().count());
            shorten(first, room) + &suffix
        };
        let x = x_of(chart, at);
        line(svg, (x, TOP - 4.0), (x, bottom), MARKER_LINE);
        // Etiket eğik yazılır: sık hatalarda bile birbirinin üstüne binmesin. Bir öncekine çok
        // yakın bir işaretin yalnızca çizgisi çizilir (o bölge `--window` ile açılarak okunur).
        if last_label.is_some_and(|previous| x - previous < LABEL_SPACING) {
            continue;
        }
        last_label = Some(x);
        let y = TOP - 8.0;
        let style =
            format!(r##"fill="#57606a" font-size="10" transform="rotate(-55 {x:.1} {y:.1})""##);
        text(svg, (x, y), &style, &label);
    }
}

fn render_lanes(svg: &mut String, chart: &Chart) {
    for &node in &chart.nodes {
        if let Some(y) = y_of(chart, node) {
            let label = format!("node {node}");
            text(
                svg,
                (LABELS - 8.0, y + LANE / 2.0 + 4.0),
                NODE_LABEL,
                &label,
            );
        }
    }
    for segment in &chart.segments {
        if segment.to <= chart.window.0 || segment.from >= chart.window.1 {
            continue;
        }
        let Some(y) = y_of(chart, segment.node) else {
            continue;
        };
        let x1 = x_of(chart, segment.from);
        let x2 = x_of(chart, segment.to);
        let width = (x2 - x1).max(0.5);
        rect(svg, (x1, y), (width, LANE), fill(segment.lane));
        // Lider aralığına, sığıyorsa term'i yazılır: hangi liderliğin hangi term'de olduğu
        // (ör. Figure 8'deki gibi aynı düğümün art arda liderlikleri) bir bakışta görünsün.
        if segment.lane == Lane::Leader && width >= 26.0 {
            let label = format!("t{}", segment.term);
            text(
                svg,
                (x1 + width / 2.0, y + LANE / 2.0 + 4.0),
                TERM_LABEL,
                &label,
            );
        }
    }
}

fn render_arrows(svg: &mut String, chart: &Chart) {
    for arrow in &chart.arrows {
        // Pencerede TESLİM edilen mesajlar çizilir; gönderimi pencereden önceyse ok sol kenardan
        // girer. Pencereden sonra teslim edilen bir mesaj çizilmez: ucu sağ kenara kırpılsaydı,
        // mesaj o anda teslim edilmiş gibi görünürdü.
        if !(chart.window.0..chart.window.1).contains(&arrow.delivered) {
            continue;
        }
        let (Some(from), Some(to)) = (y_of(chart, arrow.from), y_of(chart, arrow.to)) else {
            continue;
        };
        line(
            svg,
            (x_of(chart, arrow.sent), from + LANE / 2.0),
            (x_of(chart, arrow.delivered), to + LANE / 2.0),
            MESSAGE_LINE,
        );
    }
}

fn render_axis(svg: &mut String, chart: &Chart, lanes_height: f64) {
    let (start, end) = chart.window;
    let y = TOP + lanes_height + 4.0;
    line(svg, (LABELS, y), (LABELS + PLOT, y), AXIS_LINE);
    let step = tick_step(end.saturating_sub(start));
    let mut at = start.div_ceil(step) * step;
    while at <= end {
        let x = x_of(chart, at);
        line(svg, (x, y), (x, y + 5.0), AXIS_LINE);
        text(svg, (x, y + 18.0), AXIS_LABEL, &at.to_string());
        at += step;
    }
    text(svg, (LABELS + PLOT, y + 32.0), AXIS_TITLE, "tick");
}

#[cfg(test)]
mod tests {
    use super::{Arrow, Chart, Lane, Marker, Segment, escape, render, shorten, tick_step};

    fn chart() -> Chart {
        Chart {
            title: "seed 3 <figure8>".to_owned(),
            subtitle: "raftsim replay --seed 3".to_owned(),
            nodes: vec![1, 2],
            // 1000 tick'lik pencere: bir tick 1,1 piksel, 50 ile 51'deki işaretler üst üste düşer.
            window: (0, 1000),
            segments: vec![
                Segment {
                    node: 1,
                    from: 0,
                    to: 40,
                    lane: Lane::Follower,
                    term: 0,
                },
                Segment {
                    node: 1,
                    from: 40,
                    to: 100,
                    lane: Lane::Leader,
                    term: 2,
                },
                Segment {
                    node: 2,
                    from: 0,
                    to: 100,
                    lane: Lane::Down,
                    term: 0,
                },
            ],
            markers: vec![
                Marker {
                    at: 50,
                    label: "crash".to_owned(),
                },
                Marker {
                    at: 50,
                    label: "split".to_owned(),
                },
                Marker {
                    at: 51,
                    label: "heal".to_owned(),
                },
            ],
            arrows: vec![Arrow {
                from: 1,
                to: 2,
                sent: 60,
                delivered: 62,
            }],
            failure: Some((90, "violation: leader completeness".to_owned())),
        }
    }

    // Çizim deterministiktir ve geçerli bir SVG kabuğu üretir: aynı veri aynı baytları verir;
    // şeritler, lider term'i, hata işareti, mesaj ve ihlal çizgisi yer alır; metin XML'e uygun
    // kaçışlıdır.
    #[test]
    fn a_chart_renders_deterministically() {
        let svg = render(&chart());
        assert_eq!(svg, render(&chart()));
        assert!(svg.starts_with("<svg xmlns=\"http://www.w3.org/2000/svg\""));
        assert!(svg.ends_with("</svg>\n"));
        assert!(svg.contains(">t2</text>"), "the leader's term is labelled");
        assert!(
            svg.contains(">crash +1</text>"),
            "same-tick markers share a label"
        );
        assert!(svg.contains(">raftsim replay --seed 3</text>"));
        // 51'deki işaretin çizgisi çizilir ama etiketi 50'dekinin üstüne bineceği için yazılmaz.
        assert!(!svg.contains(">heal</text>"));
        assert_eq!(svg.matches("stroke-dasharray").count(), 2);
        assert!(svg.contains("stroke=\"#0969da\""), "the message is drawn");
        assert!(svg.contains("#d1242f"), "the violation is marked");
        assert!(svg.contains("seed 3 &lt;figure8&gt;"));
        assert!(!svg.contains("<figure8>"));
        // Her öğe kendi satırındadır ve açılan her öğe kapanır.
        assert_eq!(
            svg.matches("<text ").count(),
            svg.matches("</text>").count()
        );
        assert!(svg.lines().all(|line| line.starts_with('<')));
    }

    // Pencere dışındaki aralıklar, işaretler ve mesajlar çizilmez.
    #[test]
    fn a_window_clips_what_is_drawn() {
        let mut zoomed = chart();
        zoomed.window = (60, 100);
        let svg = render(&zoomed);
        assert!(
            !svg.contains(">crash +1</text>"),
            "the markers at 50 are outside the window"
        );
        assert!(svg.contains(">t2</text>"));
        zoomed.window = (70, 100);
        assert!(
            !render(&zoomed).contains("stroke=\"#0969da\""),
            "the message delivered at 62 is outside the window"
        );
        // Pencereden SONRA teslim edilen mesaj da çizilmez (ucu kenara kırpılırdı).
        zoomed.window = (0, 61);
        assert!(!render(&zoomed).contains("stroke=\"#0969da\""));
        // İhlal (90) pencerenin dışında: çizgisi çizilmez, etiketi sağ kenarda okla durur.
        assert!(!render(&zoomed).contains("stroke=\"#d1242f\" stroke-width=\"2\""));
        assert!(render(&zoomed).contains(">violation: leader completeness →</text>"));
    }

    // Uzun ve aynı andaki işaretler, hangi pencerede olursa olsun başlığın, alt başlığın ve
    // lejantın üstüne binmeyecek kadar kısa yazılır: ilk etiket kısaltılır, birleşenler `+N` olur.
    #[test]
    fn marker_labels_stay_short() {
        let mut long = chart();
        long.window = (0, 100);
        long.markers = vec![
            Marker {
                at: 10,
                label: "split 1,2,3|4,5,6,7".to_owned(),
            },
            Marker {
                at: 40,
                label: "heal".to_owned(),
            },
            Marker {
                at: 40,
                label: "network".to_owned(),
            },
            Marker {
                at: 40,
                label: "restart 5".to_owned(),
            },
        ];
        let svg = render(&long);
        assert!(svg.contains(">split 1,2,3|4,5…</text>"), "{svg}");
        assert!(svg.contains(">heal +2</text>"), "{svg}");
        for line in svg.lines().filter(|line| line.contains("rotate(-55")) {
            let label = &line
                [line.find('>').expect("a text element") + 1..line.find("</").expect("closed")];
            assert!(label.chars().count() <= 16, "{label}");
        }
    }

    #[test]
    fn axis_steps_are_round_numbers() {
        assert_eq!(tick_step(100), 10);
        assert_eq!(tick_step(1200), 100);
        assert_eq!(tick_step(450), 20);
        assert_eq!(tick_step(5), 1);
        assert_eq!(escape(r#"a<b & "c">"#), "a&lt;b &amp; &quot;c&quot;&gt;");
        let long = "x".repeat(200);
        assert_eq!(shorten(&long, 180).chars().count(), 180);
        assert!(shorten(&long, 180).ends_with('…'));
        assert_eq!(shorten("short", 180), "short");
        assert_eq!(shorten("split 1,2,3|4,5,6,7", 16), "split 1,2,3|4,5…");
    }
}
