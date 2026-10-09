//! Bir koşudan zaman çizelgesinin verisini toplar (`raftsim replay --svg`); çizim `svg` modülünün
//! işidir.
//!
//! Kaynaklar: düğümlerin hâlleri (ayakta mı, rol, term) kümenin zaman çizelgesinden
//! (`RaftCluster::timeline`), hatalar ve teslim edilen mesajlar trace'ten, ihlal koşunun
//! sonucundan. Hepsi koşunun deterministik çıktılarıdır: aynı seed aynı çizelgeyi, dolayısıyla
//! bayt bayt aynı SVG'yi verir.

use std::collections::BTreeMap;
use std::ops::Range;

use sim::{NodeStatus, RaftCluster, Role, RunError, RunStats, StatusChange, TraceKind};

use crate::svg::{Arrow, Chart, Lane, Marker, Segment};

/// Koşunun çizelgesi. `window` verilirse yalnızca o tick aralığı çizilir ve aralıkta teslim
/// edilen mesajlar da eklenir (bütün koşunun mesajları okunamayacak kadar çoktur).
///
/// # Errors
///
/// Pencere koşu bittikten sonra başlıyorsa, kullanıcıya gösterilecek açıklama: boş bir çizelge
/// "hiçbir şey olmadı" sanılabilirdi.
pub fn collect(
    (title, subtitle): (String, String),
    cluster: &RaftCluster,
    outcome: &Result<RunStats, RunError>,
    window: Option<&Range<u64>>,
) -> Result<Chart, String> {
    // Aralıklar yarı açıktır: koşunun son anı da çizilsin.
    let end = cluster.now().saturating_add(1);
    let drawn = match window {
        None => (0, end),
        Some(range) if range.start >= end => {
            return Err(format!(
                "the window {}..{} starts after the run ended at t={}",
                range.start,
                range.end,
                cluster.now()
            ));
        }
        Some(range) => (range.start, range.end.min(end)),
    };
    let failure = outcome.as_ref().err().map(|error| {
        // Bir invariant ihlali olayın anında görülür; geçmişin denetimi (linearizability) ve
        // canlılık ise koşunun sonunda.
        let at = match error {
            RunError::Violation { time, .. } => *time,
            _ => cluster.now(),
        };
        (at, format!("{} at t={at}", error.signature()))
    });
    Ok(Chart {
        title,
        subtitle,
        nodes: cluster.node_ids().map(|id| id.0).collect(),
        window: drawn,
        segments: segments(cluster.timeline(), end),
        markers: markers(cluster),
        arrows: if window.is_some() {
            arrows(cluster, drawn)
        } else {
            Vec::new()
        },
        failure,
    })
}

/// Küme kurulamadığında (bkz. `sim::Run::cluster`) çizilecek şey: şeritsiz bir çizelge, başlık ve
/// (varsa) hata.
pub fn without_run(
    (title, subtitle): (String, String),
    outcome: &Result<RunStats, RunError>,
) -> Chart {
    Chart {
        title,
        subtitle,
        window: (0, 1),
        failure: outcome.as_ref().err().map(|error| (0, error.signature())),
        ..Chart::default()
    }
}

/// Bir düğümün hâlinin şeridi: çökmüş bir düğüm, belleğindeki rol ne olursa olsun çökmüş çizilir.
fn lane(status: NodeStatus) -> Lane {
    if !status.up {
        return Lane::Down;
    }
    match status.role {
        Role::Follower => Lane::Follower,
        Role::Candidate => Lane::Candidate,
        Role::Leader => Lane::Leader,
    }
}

/// Zaman çizelgesini şerit aralıklarına çevirir, düğüm sırasıyla: bir düğümün her hâli, o düğümün
/// bir sonraki değişimine (sonuncusu `end`'e) kadar sürer. Aynı tick'teki art arda değişimlerin
/// (ör. bir olayda aday olup aynı tick'in başka bir olayında lider olmak) sıfır uzunluklu
/// aralıkları çizilmez. Aynı şeritteki bitişik aralıklar birleşir (ör. term'i değişen bir
/// takipçi): çizimde aynı renktedirler ve ayrı dikdörtgenler aralarında dikiş izi bırakırdı.
/// Liderin aralıkları yalnızca aynı term'deyse birleşir: her liderlik kendi term'iyle görünür.
fn segments(timeline: &[StatusChange], end: u64) -> Vec<Segment> {
    // BTreeMap: düğümler sırayla gezilir, çıktı her koşuda aynıdır.
    let mut open: BTreeMap<u64, (u64, NodeStatus)> = BTreeMap::new();
    let mut lanes: BTreeMap<u64, Vec<Segment>> = BTreeMap::new();
    let mut close = |node: u64, (from, status): (u64, NodeStatus), to: u64| {
        if to <= from {
            return;
        }
        let segment = Segment {
            node,
            from,
            to,
            lane: lane(status),
            term: status.term.0,
        };
        let row = lanes.entry(node).or_default();
        match row.last_mut() {
            Some(last)
                if last.lane == segment.lane
                    && last.to == from
                    && (segment.lane != Lane::Leader || last.term == segment.term) =>
            {
                last.to = to;
                last.term = segment.term;
            }
            _ => row.push(segment),
        }
    };
    for change in timeline {
        if let Some(previous) = open.insert(change.node.0, (change.at, change.status)) {
            close(change.node.0, previous, change.at);
        }
    }
    for (node, previous) in open {
        close(node, previous, end);
    }
    lanes.into_values().flatten().collect()
}

/// Hatalar, trace'teki etkileriyle: hata programının niyetleri değil, gerçekten olanlar (ör. lider
/// yokken "lideri çökert" hiçbir şey yapmaz ve çizilmez). Sakinleşmenin iyileştirmesi ve yeniden
/// başlatmaları da, saat sıçramaları da çizilir.
fn markers(cluster: &RaftCluster) -> Vec<Marker> {
    cluster
        .sim()
        .trace()
        .events()
        .iter()
        .filter_map(|event| {
            let label = match &event.kind {
                TraceKind::Crash { node } => format!("crash {}", node.0),
                TraceKind::Restart { node } => format!("restart {}", node.0),
                TraceKind::Partition { groups } => {
                    let groups: Vec<String> = groups
                        .iter()
                        .map(|group| {
                            let ids: Vec<String> =
                                group.iter().map(|id| id.0.to_string()).collect();
                            ids.join(",")
                        })
                        .collect();
                    format!("split {}", groups.join("|"))
                }
                TraceKind::Heal => "heal".to_owned(),
                TraceKind::Network { .. } => "network".to_owned(),
                TraceKind::ClockJump { node, ticks } => format!("clock {} +{ticks}", node.0),
                _ => return None,
            };
            Some(Marker {
                at: event.time,
                label,
            })
        })
        .collect()
}

/// Pencerede teslim edilen mesajlar: gönderimi pencereden önce olsa bile teslimi pencerede olan
/// mesaj çizilir (ok pencerenin sol kenarından girer). Pencereden sonra teslim edilen bir mesaj
/// alınmaz: ucu sağ kenara kırpılsaydı, mesaj o anda teslim edilmiş gibi görünürdü.
fn arrows(cluster: &RaftCluster, (start, end): (u64, u64)) -> Vec<Arrow> {
    cluster
        .sim()
        .trace()
        .events()
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Deliver {
                from, to, sent_at, ..
            } if (start..end).contains(&event.time) => Some(Arrow {
                from: from.0,
                to: to.0,
                sent: sent_at,
                delivered: event.time,
            }),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{collect, segments};
    use crate::svg::{Lane, Segment};
    use sim::{
        ClusterConfig, NetworkConfig, NodeId, NodeStatus, RaftCluster, Role, RunStats,
        StatusChange, Term,
    };

    fn titles() -> (String, String) {
        ("title".to_owned(), "subtitle".to_owned())
    }

    fn change(at: u64, node: u64, up: bool, role: Role, term: u64) -> StatusChange {
        StatusChange {
            at,
            node: NodeId(node),
            status: NodeStatus {
                up,
                role,
                term: Term(term),
            },
        }
    }

    // Her hâl bir sonraki değişime kadar sürer; sonuncusu koşunun sonuna kadar. Aynı tick'teki
    // ara hâl (aday) sıfır uzunluklu olduğu için çizilmez; çökmüş düğüm, donmuş rolü lider olsa
    // bile çökmüş çizilir. Term'i değişen takipçinin aralıkları birleşir; ardışık iki liderlik
    // (farklı term'ler) birleşmez.
    #[test]
    fn a_timeline_becomes_lane_segments() {
        let timeline = [
            change(0, 1, true, Role::Follower, 0),
            change(0, 2, true, Role::Follower, 0),
            change(10, 2, true, Role::Follower, 1),
            change(30, 1, true, Role::Candidate, 1),
            change(30, 1, true, Role::Leader, 1),
            change(50, 2, true, Role::Leader, 2),
            change(60, 2, true, Role::Leader, 3),
            change(70, 1, false, Role::Leader, 1),
        ];
        let segment = |node, from, to, lane, term| Segment {
            node,
            from,
            to,
            lane,
            term,
        };
        assert_eq!(
            segments(&timeline, 100),
            vec![
                segment(1, 0, 30, Lane::Follower, 0),
                segment(1, 30, 70, Lane::Leader, 1),
                segment(1, 70, 100, Lane::Down, 1),
                segment(2, 0, 50, Lane::Follower, 1),
                segment(2, 50, 60, Lane::Leader, 2),
                segment(2, 60, 100, Lane::Leader, 3),
            ]
        );
    }

    // Bir kümenin koşusu: lider seçilir, lider çöker. Çizelge her düğümün şeridini, liderin
    // aralığını, çökmenin işaretini taşır; pencere verilince yalnızca o aralığın mesajları gelir.
    // Koşu bittikten sonra başlayan bir pencere reddedilir.
    #[test]
    fn a_run_is_collected_into_a_chart() {
        let mut cluster = RaftCluster::new(4, ClusterConfig::new(3, NetworkConfig::reliable(2)))
            .expect("valid config");
        cluster.run_until(200).expect("no violation");
        let (leader, _) = cluster.leaders()[0];
        cluster.crash(leader).expect("the leader is up");
        cluster.run_until(300).expect("no violation");
        let outcome: Result<RunStats, _> = Ok(RunStats::default());

        let chart = collect(titles(), &cluster, &outcome, None).expect("a chart");
        assert_eq!(chart.nodes, vec![1, 2, 3]);
        assert_eq!(chart.window, (0, 301));
        assert!(
            chart
                .segments
                .iter()
                .any(|s| s.node == leader.0 && s.lane == Lane::Leader)
        );
        assert!(
            chart
                .segments
                .iter()
                .any(|s| s.node == leader.0 && s.lane == Lane::Down)
        );
        assert!(
            chart
                .markers
                .iter()
                .any(|m| m.label == format!("crash {}", leader.0))
        );
        assert!(chart.arrows.is_empty(), "messages only in a window");
        assert_eq!(chart.failure, None);

        let zoomed = collect(titles(), &cluster, &outcome, Some(&(100..150))).expect("a chart");
        assert_eq!(zoomed.window, (100, 150));
        assert!(!zoomed.arrows.is_empty());
        assert!(
            zoomed
                .arrows
                .iter()
                .all(|a| a.delivered >= 100 && a.sent < 150)
        );

        let error = collect(titles(), &cluster, &outcome, Some(&(301..400)))
            .expect_err("the run ended at t=300");
        assert!(error.contains("after the run ended at t=300"), "{error}");
    }
}
