use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap},
    Terminal,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use shared::{Alert, AnomalyKind, TOPIC_ALERTS};
use uuid::Uuid;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;


#[derive(Parser)]
#[command(name = "alarm-watcher", about = "Live TUI dashboard for Flink alerts")]
struct Args {
    #[arg(long, default_value = "localhost:9092")]
    broker: String,

    #[arg(long, default_value = "alarm-watcher-dev")]
    group: String,

    #[arg(long, default_value = "latest")]
    offset: String,

    #[arg(long, default_value_t = 200)]
    buffer: usize,

    #[arg(long, default_value = "mongodb://admin:secret@localhost:27017/?authSource=admin")]
    mongo_uri: String,

    #[arg(long, default_value = "anomaly_detection")]
    mongo_db: String,

    #[arg(long, default_value = "alerts")]
    mongo_collection: String,

    /// How often the MongoDB stats panel refreshes, in seconds.
    #[arg(long, default_value_t = 5)]
    stats_interval: u64,
}

const ALL_KINDS: [AnomalyKind; 6] = [
    AnomalyKind::LargeAmount,
    AnomalyKind::ImpossibleTravel,
    AnomalyKind::HighFrequency,
    AnomalyKind::NewGeography,
    AnomalyKind::LimitExhaustion,
    AnomalyKind::Structuring,
];

/// Short two-letter codes used as bar-chart labels and in the filter status.
fn kind_code(kind: &AnomalyKind) -> &'static str {
    match kind {
        AnomalyKind::LargeAmount      => "LA",
        AnomalyKind::ImpossibleTravel => "IT",
        AnomalyKind::HighFrequency    => "HF",
        AnomalyKind::NewGeography     => "NG",
        AnomalyKind::LimitExhaustion  => "LE",
        AnomalyKind::Structuring      => "ST",
    }
}

struct AppState {
    alerts:         Vec<Alert>,
    by_kind:        HashMap<AnomalyKind, u64>,
    sev_hist:       [u64; 5],
    total_received: u64,
    max_buf:        usize,
}

impl AppState {
    fn new(max_buf: usize) -> Self {
        Self {
            alerts: Vec::new(),
            by_kind: HashMap::new(),
            sev_hist: [0; 5],
            total_received: 0,
            max_buf,
        }
    }

    fn push(&mut self, alert: Alert) {
        self.total_received += 1;
        *self.by_kind.entry(alert.anomaly_kind.clone()).or_insert(0) += 1;
        let bucket = ((alert.severity * 5.0) as usize).min(4);
        self.sev_hist[bucket] += 1;
        self.alerts.push(alert);
        if self.alerts.len() > self.max_buf {
            self.alerts.remove(0);
        }
    }
}

// ── Filters ───────────────────────────────────────────────────────────────────

const SEV_STEPS: [f64; 4] = [0.0, 0.25, 0.5, 0.75];

#[derive(Default)]
struct Filters {
    /// Case-insensitive substring match on card id, merchant and description.
    text:    Option<String>,
    kind:    Option<AnomalyKind>,
    min_sev: f64,
}

impl Filters {
    fn matches(&self, a: &Alert) -> bool {
        if a.severity < self.min_sev {
            return false;
        }
        if let Some(kind) = &self.kind {
            if &a.anomaly_kind != kind {
                return false;
            }
        }
        if let Some(text) = &self.text {
            let needle = text.to_lowercase();
            let hit = a.card_id.to_lowercase().contains(&needle)
                || a.transaction.merchant.to_lowercase().contains(&needle)
                || a.description.to_lowercase().contains(&needle);
            if !hit {
                return false;
            }
        }
        true
    }

    fn is_active(&self) -> bool {
        self.text.is_some() || self.kind.is_some() || self.min_sev > 0.0
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn cycle_kind(&mut self) {
        self.kind = match &self.kind {
            None => Some(ALL_KINDS[0].clone()),
            Some(current) => {
                let idx = ALL_KINDS.iter().position(|k| k == current).unwrap_or(0);
                if idx + 1 < ALL_KINDS.len() {
                    Some(ALL_KINDS[idx + 1].clone())
                } else {
                    None
                }
            }
        };
    }

    fn cycle_min_sev(&mut self) {
        let idx = SEV_STEPS.iter().position(|&s| s == self.min_sev).unwrap_or(0);
        self.min_sev = SEV_STEPS[(idx + 1) % SEV_STEPS.len()];
    }

    fn summary(&self) -> String {
        let mut parts = Vec::new();
        if let Some(t) = &self.text {
            parts.push(format!("text=\"{t}\""));
        }
        if let Some(k) = &self.kind {
            parts.push(format!("type={}", kind_code(k)));
        }
        if self.min_sev > 0.0 {
            parts.push(format!("sev≥{:.2}", self.min_sev));
        }
        parts.join(" ")
    }
}

// ── MongoDB stats ─────────────────────────────────────────────────────────────

#[derive(Default, Clone)]
struct MongoStats {
    loaded:      bool,
    error:       Option<String>,
    total:       u64,
    last_hour:   u64,
    /// (anomaly_kind as stored, count, avg severity, max severity)
    by_kind:     Vec<(String, i64, f64, f64)>,
    sev_buckets: [u64; 5],
    top_cards:   Vec<(String, i64)>,
}

fn doc_count(d: &Document) -> i64 {
    d.get_i64("count")
        .or_else(|_| d.get_i32("count").map(i64::from))
        .unwrap_or(0)
}

async fn fetch_mongo_stats(coll: &Collection<Document>) -> Result<MongoStats> {
    let total = coll.count_documents(doc! {}).await?;

    let hour_ago = mongodb::bson::DateTime::from_millis(
        mongodb::bson::DateTime::now().timestamp_millis() - 3_600_000,
    );
    let last_hour = coll
        .count_documents(doc! { "stored_at": { "$gte": hour_ago } })
        .await?;

    let mut by_kind = Vec::new();
    let mut cur = coll
        .aggregate(vec![
            doc! { "$group": {
                "_id":   "$anomaly_kind",
                "count": { "$sum": 1 },
                "avg":   { "$avg": "$severity" },
                "max":   { "$max": "$severity" },
            }},
            doc! { "$sort": { "count": -1 } },
        ])
        .await?;
    while let Some(d) = cur.try_next().await? {
        by_kind.push((
            d.get_str("_id").unwrap_or("?").to_string(),
            doc_count(&d),
            d.get_f64("avg").unwrap_or(0.0),
            d.get_f64("max").unwrap_or(0.0),
        ));
    }

    let mut sev_buckets = [0u64; 5];
    let mut cur = coll
        .aggregate(vec![doc! { "$bucket": {
            "groupBy":    "$severity",
            "boundaries": [0.0, 0.2, 0.4, 0.6, 0.8, 1.0001],
            "default":    "out_of_range",
            "output":     { "count": { "$sum": 1 } },
        }}])
        .await?;
    while let Some(d) = cur.try_next().await? {
        if let Ok(lower) = d.get_f64("_id") {
            let idx = ((lower / 0.2).round() as usize).min(4);
            sev_buckets[idx] = doc_count(&d).max(0) as u64;
        }
    }

    let mut top_cards = Vec::new();
    let mut cur = coll
        .aggregate(vec![
            doc! { "$group": { "_id": "$card_id", "count": { "$sum": 1 } } },
            doc! { "$sort": { "count": -1 } },
            doc! { "$limit": 5 },
        ])
        .await?;
    while let Some(d) = cur.try_next().await? {
        top_cards.push((d.get_str("_id").unwrap_or("?").to_string(), doc_count(&d)));
    }

    Ok(MongoStats {
        loaded: true,
        error: None,
        total,
        last_hour,
        by_kind,
        sev_buckets,
        top_cards,
    })
}

async fn mongo_stats_loop(
    coll:     Collection<Document>,
    stats:    Arc<Mutex<MongoStats>>,
    interval: Duration,
) {
    loop {
        match fetch_mongo_stats(&coll).await {
            Ok(fresh) => {
                if let Ok(mut s) = stats.lock() {
                    *s = fresh;
                }
            }
            Err(e) => {
                if let Ok(mut s) = stats.lock() {
                    s.error = Some(e.to_string());
                }
            }
        }
        tokio::time::sleep(interval).await;
    }
}

// ── Kafka consumer (runs on a background Tokio task) ─────────────────────────

async fn consume_loop(
    broker: String,
    group:  String,
    offset: String,
    state:  Arc<Mutex<AppState>>,
    alerts_collection: Collection<Document>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers",       &broker)
        .set("group.id",                &group)
        .set("auto.offset.reset",       &offset)
        .set("enable.auto.commit",      "true")
        .set("auto.commit.interval.ms", "1000")
        .create()
    {
        Ok(c)  => c,
        Err(e) => { eprintln!("Consumer create failed: {e}"); return; }
    };

    if let Err(e) = consumer.subscribe(&[TOPIC_ALERTS]) {
        eprintln!("Subscribe failed: {e}");
        return;
    }

    loop {
        let msg = match consumer.recv().await {
            Ok(m)  => m,
            Err(e) => { eprintln!("Recv error: {e}"); continue; }
        };

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s.to_string(),
            _           => { let _ = consumer.commit_message(&msg, CommitMode::Async); continue; }
        };

        match serde_json::from_str::<Alert>(&payload) {
            Ok(alert) => {
                match serde_json::from_str::<serde_json::Value>(&payload) {
                    Ok(value) => {
                        match mongodb::bson::to_document(&value) {
                            Ok(mut document) => {
                                document.insert("stored_at", mongodb::bson::DateTime::now());

                                if let Err(e) = alerts_collection.insert_one(document).await {
                                    eprintln!("MongoDB insert error: {e}");
                                }
                            }
                            Err(e) => {
                                eprintln!("BSON conversion error: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("JSON parse for MongoDB error: {e}");
                    }
                }

                if let Ok(mut s) = state.lock() {
                    s.push(alert);
                }
            }
            Err(e) => {
                eprintln!("Alert parse error: {e}");
            }
        }

        let _ = consumer.commit_message(&msg, CommitMode::Async);
    }
}

// ── Rendering ─────────────────────────────────────────────────────────────────

fn severity_color(s: f64) -> Color {
    if s >= 0.8      { Color::Red }
    else if s >= 0.5 { Color::Yellow }
    else             { Color::Green }
}

fn kind_color(kind: &AnomalyKind) -> Color {
    match kind {
        AnomalyKind::LargeAmount      => Color::Red,
        AnomalyKind::ImpossibleTravel => Color::Red,
        AnomalyKind::LimitExhaustion  => Color::Red,
        AnomalyKind::HighFrequency    => Color::Yellow,
        AnomalyKind::NewGeography     => Color::Yellow,
        AnomalyKind::Structuring      => Color::Cyan,
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

/// One row of a horizontal bar chart: label, bar proportional to count/max
/// (at least one block when non-zero, so small buckets stay visible), exact
/// count and share of total.
fn histogram_line(
    label:     String,
    count:     u64,
    max:       u64,
    total:     u64,
    bar_color: Color,
    bar_max:   f64,
) -> Line<'static> {
    let pct = if total > 0 {
        count as f64 / total as f64 * 100.0
    } else {
        0.0
    };
    let frac = if max > 0 { count as f64 / max as f64 } else { 0.0 };
    let bar_len = ((frac * bar_max).round() as usize).max(usize::from(count > 0));

    Line::from(vec![
        Span::styled(label, Style::default().fg(Color::Gray)),
        Span::styled("█".repeat(bar_len), Style::default().fg(bar_color)),
        Span::styled(
            format!(" {count} "),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::styled(format!("{pct:.1}%"), Style::default().fg(Color::DarkGray)),
    ])
}

fn severity_panel_lines(buckets: &[u64; 5], panel_width: u16) -> Vec<Line<'static>> {
    let labels = ["0.0–0.2", "0.2–0.4", "0.4–0.6", "0.6–0.8", "0.8–1.0"];
    let mids   = [0.1, 0.3, 0.5, 0.7, 0.9];

    let total: u64 = buckets.iter().sum();
    let max        = buckets.iter().copied().max().unwrap_or(0);

    // borders (2) + " 0.0–0.2 " label (9) + " 123456 100.0%" numbers (~15)
    let bar_max = panel_width.saturating_sub(26).max(4) as f64;

    labels
        .iter()
        .zip(buckets.iter())
        .zip(mids.iter())
        .map(|((label, &count), &mid)| {
            histogram_line(format!(" {label} "), count, max, total, severity_color(mid), bar_max)
        })
        .collect()
}

fn kind_panel_lines(by_kind: &HashMap<AnomalyKind, u64>, panel_width: u16) -> Vec<Line<'static>> {
    let total: u64 = by_kind.values().sum();
    let max        = by_kind.values().copied().max().unwrap_or(0);

    // borders (2) + " LA " label (4) + numbers (~15)
    let bar_max = panel_width.saturating_sub(21).max(4) as f64;

    ALL_KINDS
        .iter()
        .map(|kind| {
            let count = by_kind.get(kind).copied().unwrap_or(0);
            histogram_line(format!(" {} ", kind_code(kind)), count, max, total, kind_color(kind), bar_max)
        })
        .collect()
}

fn mongo_panel_lines(mongo: &MongoStats) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    if !mongo.loaded {
        lines.push(Line::from(Span::styled(
            " connecting to MongoDB…",
            Style::default().fg(Color::DarkGray),
        )));
        if let Some(e) = &mongo.error {
            lines.push(Line::from(Span::styled(
                format!(" error: {e}"),
                Style::default().fg(Color::Red),
            )));
        }
        return lines;
    }

    lines.push(Line::from(vec![
        Span::raw(" total "),
        Span::styled(
            format!("{}", mongo.total),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::raw("   last 1h "),
        Span::styled(
            format!("{}", mongo.last_hour),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
    ]));

    if let Some(e) = &mongo.error {
        lines.push(Line::from(Span::styled(
            format!(" stale (last refresh failed: {e})"),
            Style::default().fg(Color::Red),
        )));
    }

    lines.push(Line::from(Span::styled(
        format!(" {:<18} {:>7} {:>6} {:>6}", "kind", "n", "avg", "max"),
        Style::default().fg(Color::Gray).add_modifier(Modifier::BOLD),
    )));

    for (kind, count, avg, max) in &mongo.by_kind {
        lines.push(Line::from(vec![
            Span::styled(
                format!(" {:<18}", kind),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw(format!(" {:>7}", count)),
            Span::styled(format!(" {:>6.2}", avg), Style::default().fg(severity_color(*avg))),
            Span::styled(format!(" {:>6.2}", max), Style::default().fg(severity_color(*max))),
        ]));
    }

    if !mongo.top_cards.is_empty() {
        lines.push(Line::from(Span::styled(
            " top cards:",
            Style::default().fg(Color::Gray).add_modifier(Modifier::BOLD),
        )));
        for (card, count) in &mongo.top_cards {
            lines.push(Line::from(vec![
                Span::styled(format!("  {:<14}", card), Style::default().fg(Color::White)),
                Span::raw(format!(" {:>5}", count)),
            ]));
        }
    }

    lines
}

fn alert_detail_lines(a: &Alert) -> Vec<Line<'static>> {
    let label = Style::default().fg(Color::Gray);
    let value = Style::default().fg(Color::White);

    let injected = match &a.transaction.injected_anomaly {
        Some(kind) => Span::styled(
            format!("{kind}"),
            Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD),
        ),
        None => Span::styled("none (organic)", Style::default().fg(Color::DarkGray)),
    };

    vec![
        Line::from(vec![
            Span::styled(" alert     ", label),
            Span::styled(a.alert_id.to_string(), value),
        ]),
        Line::from(vec![
            Span::styled(" time      ", label),
            Span::styled(
                a.timestamp.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                value,
            ),
        ]),
        Line::from(vec![
            Span::styled(" card/user ", label),
            Span::styled(format!("{}  {}", a.card_id, a.user_id), value),
        ]),
        Line::from(vec![
            Span::styled(" type      ", label),
            Span::styled(
                format!("{}", a.anomaly_kind),
                Style::default().fg(kind_color(&a.anomaly_kind)).add_modifier(Modifier::BOLD),
            ),
            Span::styled("   severity ", label),
            Span::styled(
                format!("{:.2}", a.severity),
                Style::default().fg(severity_color(a.severity)).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(""),
        Line::from(Span::styled(format!(" {}", a.description), value)),
        Line::from(""),
        Line::from(vec![
            Span::styled(" amount    ", label),
            Span::styled(format!("{:.2} PLN", a.transaction.amount_pln), value),
            Span::styled("   remaining ", label),
            Span::styled(format!("{:.2} PLN", a.transaction.remaining_limit_pln), value),
        ]),
        Line::from(vec![
            Span::styled(" merchant  ", label),
            Span::styled(a.transaction.merchant.clone(), value),
        ]),
        Line::from(vec![
            Span::styled(" location  ", label),
            Span::styled(
                format!("{:.4}, {:.4}", a.transaction.location.lat, a.transaction.location.lon),
                value,
            ),
        ]),
        Line::from(vec![Span::styled(" injected  ", label), injected]),
    ]
}

#[allow(clippy::too_many_arguments)]
fn draw(
    term:         &mut Terminal<CrosstermBackend<io::Stdout>>,
    state:        &AppState,
    mongo:        &MongoStats,
    table_state:  &mut TableState,
    filters:      &Filters,
    filter_input: &str,
    is_filtering: bool,
    detail:       Option<&Alert>,
    selected_id:  &mut Option<Uuid>,
) -> Result<usize> {
    let mut shown_count = 0;

    term.draw(|f| {
        let area = f.size();   // ratatui 0.27 uses f.size()

        let outer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),   // header
                Constraint::Min(6),      // body
                Constraint::Length(3),   // footer
            ])
            .split(area);

        let clock = Local::now().format("%H:%M:%S").to_string();
        let filter_info = if is_filtering {
            format!("  │  filter: {filter_input}▌")
        } else if filters.is_active() {
            format!("  │  filter: {}", filters.summary())
        } else {
            String::new()
        };
        let header_text = format!(
            " alarm-watcher  │  received: {}  │  buffered: {}  │  {}{}",
            state.total_received,
            state.alerts.len(),
            clock,
            filter_info,
        );
        let header = Paragraph::new(header_text)
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(header, outer[0]);

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(outer[1]);

        // ── Alerts table (left) ──────────────────────────────────────────────
        let hdr_cells = ["Time", "Card ID", "Anomaly type", "Sev", "Description"]
            .iter()
            .map(|h| {
                Cell::from(*h)
                    .style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Gray))
            });
        let hdr_row = Row::new(hdr_cells).height(1).bottom_margin(0);

        let shown: Vec<&Alert> = state.alerts.iter().rev()
            .filter(|a| filters.matches(a))
            .collect();
        shown_count = shown.len();

        // New alerts arrive at the top and shift everything down, so anchor
        // the cursor to the selected alert's id, not to its row index.
        if let Some(id) = selected_id {
            if let Some(pos) = shown.iter().position(|a| a.alert_id == *id) {
                table_state.select(Some(pos));
            }
        }

        // Keep selection inside the (possibly shrunken) filtered list.
        if let Some(sel) = table_state.selected() {
            if shown.is_empty() {
                table_state.select(None);
            } else if sel >= shown.len() {
                table_state.select(Some(shown.len() - 1));
            }
        }

        *selected_id = table_state
            .selected()
            .and_then(|i| shown.get(i))
            .map(|a| a.alert_id);

        let rows: Vec<Row> = shown.iter().map(|a| {
            let ts      = a.timestamp.with_timezone(&Local).format("%H:%M:%S").to_string();
            let card    = a.card_id[..a.card_id.len().min(12)].to_string();
            let kind    = format!("{}", a.anomaly_kind);
            let sev_str = format!("{:.2}", a.severity);
            let desc    = a.description[..a.description.len().min(50)].to_string();

            let sev_color  = severity_color(a.severity);
            let kind_color = kind_color(&a.anomaly_kind);

            Row::new(vec![
                Cell::from(ts)      .style(Style::default().fg(Color::DarkGray)),
                Cell::from(card)    .style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                Cell::from(kind)    .style(Style::default().fg(kind_color)),
                Cell::from(sev_str) .style(Style::default().fg(sev_color).add_modifier(Modifier::BOLD)),
                Cell::from(desc)    .style(Style::default().fg(Color::Gray)),
            ])
            .height(1)
        }).collect();

        let alert_table = Table::new(
            rows,
            [
                Constraint::Length(10),  // time
                Constraint::Length(13),  // card
                Constraint::Length(18),  // kind
                Constraint::Length(5),   // sev
                Constraint::Min(10),     // description
            ],
        )
        .header(hdr_row)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Alerts — newest first ({} shown) ", shown.len()))
                .title_style(Style::default().fg(Color::Cyan)),
        )
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::REVERSED)
                .fg(Color::White),
        )
        .highlight_symbol("▶ ");

        f.render_stateful_widget(alert_table, body[0], table_state);

        // ── Right column: charts + MongoDB stats ─────────────────────────────
        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(8),   // by-kind histogram (6 rows + borders)
                Constraint::Length(7),   // severity histogram (5 rows + borders)
                Constraint::Min(8),      // MongoDB stats
            ])
            .split(body[1]);

        let kind_panel = Paragraph::new(kind_panel_lines(&state.by_kind, right[0].width))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" By type — session ")
                    .title_style(Style::default().fg(Color::Cyan)),
            );
        f.render_widget(kind_panel, right[0]);

        // Severity histogram: all-time from MongoDB once loaded, session until then.
        let (sev_source, sev_title) = if mongo.loaded {
            (&mongo.sev_buckets, " Severity — MongoDB, all time ")
        } else {
            (&state.sev_hist, " Severity — session ")
        };

        let sev_panel = Paragraph::new(severity_panel_lines(sev_source, right[1].width))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(sev_title)
                    .title_style(Style::default().fg(Color::Yellow)),
            );
        f.render_widget(sev_panel, right[1]);

        let mongo_panel = Paragraph::new(mongo_panel_lines(mongo))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" MongoDB — all time ")
                    .title_style(Style::default().fg(Color::Green)),
            );
        f.render_widget(mongo_panel, right[2]);

        // ── Footer ───────────────────────────────────────────────────────────
        let footer_line = if is_filtering {
            Line::from(vec![
                Span::raw("  Filter text: "),
                Span::styled(
                    format!("{filter_input}▌"),
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                ),
                Span::raw("   "),
                Span::styled("Enter", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" apply   "),
                Span::styled("Esc", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" cancel"),
            ])
        } else {
            let mut spans = vec![
                Span::raw("  "),
                Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" quit  "),
                Span::styled("↑↓", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" scroll  "),
                Span::styled("Enter", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" details  "),
                Span::styled("f", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" text  "),
                Span::styled("t", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(format!(
                    " type[{}]  ",
                    filters.kind.as_ref().map_or("all", kind_code),
                )),
                Span::styled("s", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(format!(" sev[≥{:.2}]  ", filters.min_sev)),
                Span::styled("c", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" clear"),
            ];
            if filters.is_active() {
                spans.push(Span::styled(
                    format!("   active: {}", filters.summary()),
                    Style::default().fg(Color::Yellow),
                ));
            }
            Line::from(spans)
        };

        let footer = Paragraph::new(footer_line)
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(footer, outer[2]);

        // ── Detail popup ─────────────────────────────────────────────────────
        // Renders a snapshot taken when the popup was opened, so incoming
        // alerts can't change its contents under the reader.
        if let Some(alert) = detail {
            let popup = centered_rect(72, 60, area);
            f.render_widget(Clear, popup);
            let detail_panel = Paragraph::new(alert_detail_lines(alert))
                .wrap(Wrap { trim: false })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Alert detail — Esc to close ")
                        .title_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                );
            f.render_widget(detail_panel, popup);
        }
    })?;

    Ok(shown_count)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let state = Arc::new(Mutex::new(AppState::new(args.buffer)));
    let mongo_stats = Arc::new(Mutex::new(MongoStats::default()));

    let mongo_client = Client::with_uri_str(&args.mongo_uri)
        .await
        .context("connect to MongoDB")?;

    let alerts_collection = mongo_client
        .database(&args.mongo_db)
        .collection::<Document>(&args.mongo_collection);

    {
        let s      = state.clone();
        let broker = args.broker.clone();
        let group  = args.group.clone();
        let offset = args.offset.clone();
        let collection = alerts_collection.clone();
        tokio::spawn(async move { consume_loop(broker, group, offset, s, collection).await });
    }

    {
        let stats      = mongo_stats.clone();
        let collection = alerts_collection.clone();
        let interval   = Duration::from_secs(args.stats_interval.max(1));
        tokio::spawn(async move { mongo_stats_loop(collection, stats, interval).await });
    }

    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend).context("create terminal")?;

    let mut table_state  = TableState::default();
    let mut filters      = Filters::default();
    let mut filter_input = String::new();
    let mut is_filtering = false;
    // Snapshot of the alert shown in the detail popup; pinned so incoming
    // alerts don't change what the user is reading.
    let mut detail: Option<Alert> = None;
    let mut selected_id: Option<Uuid> = None;
    let mut shown_count: usize;

    // The alert at position `idx` of the filtered, newest-first list.
    let nth_filtered = |filters: &Filters, idx: usize| -> Option<Alert> {
        let s = state.lock().ok()?;
        s.alerts.iter().rev().filter(|a| filters.matches(a)).nth(idx).cloned()
    };

    loop {
        {
            let s = state.lock().unwrap();
            let m = mongo_stats.lock().unwrap().clone();
            shown_count = draw(
                &mut term, &s, &m, &mut table_state,
                &filters, &filter_input, is_filtering,
                detail.as_ref(), &mut selected_id,
            )?;
        }

        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if is_filtering {
                    match key.code {
                        KeyCode::Enter => {
                            filters.text = if filter_input.is_empty() {
                                None
                            } else {
                                Some(filter_input.clone())
                            };
                            filter_input.clear();
                            is_filtering = false;
                            table_state.select(Some(0));
                            selected_id = None;
                        }
                        KeyCode::Esc => {
                            filter_input.clear();
                            is_filtering = false;
                        }
                        KeyCode::Backspace => { filter_input.pop(); }
                        KeyCode::Char(c)   => { filter_input.push(c); }
                        _ => {}
                    }
                } else {
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('c')
                            if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                        KeyCode::Esc if detail.is_some() => {
                            detail = None;
                        }
                        KeyCode::Enter => {
                            if detail.is_some() {
                                detail = None;
                            } else {
                                let idx = table_state.selected().unwrap_or(0);
                                detail = nth_filtered(&filters, idx);
                                if detail.is_some() && table_state.selected().is_none() {
                                    table_state.select(Some(0));
                                }
                            }
                        }
                        KeyCode::Char('f') => {
                            is_filtering = true;
                        }
                        KeyCode::Char('t') => {
                            filters.cycle_kind();
                            table_state.select(Some(0));
                            selected_id = None;
                        }
                        KeyCode::Char('s') => {
                            filters.cycle_min_sev();
                            table_state.select(Some(0));
                            selected_id = None;
                        }
                        KeyCode::Char('c') => {
                            filters.clear();
                            table_state.select(Some(0));
                            selected_id = None;
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            let max = shown_count.saturating_sub(1);
                            let next = table_state.selected()
                                .map_or(0, |i| (i + 1).min(max));
                            table_state.select(Some(next));
                            selected_id = nth_filtered(&filters, next).map(|a| a.alert_id);
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            let prev = table_state.selected()
                                .map_or(0, |i| i.saturating_sub(1));
                            table_state.select(Some(prev));
                            selected_id = nth_filtered(&filters, prev).map(|a| a.alert_id);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(term.backend_mut(), LeaveAlternateScreen)?;
    term.show_cursor()?;

    Ok(())
}
