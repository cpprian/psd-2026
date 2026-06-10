use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{BarChart, Block, Borders, Cell, Paragraph, Row, Table, TableState},
    Terminal,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use mongodb::{bson::Document, Client, Collection};
use shared::{Alert, AnomalyKind, TOPIC_ALERTS};
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
}

struct AppState {
    alerts:        Vec<Alert>,
    by_kind:       HashMap<String, u64>,
    sev_hist:      [u64; 5],
    total_received: u64,
    max_buf:       usize,
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
        *self.by_kind.entry(format!("{}", alert.anomaly_kind)).or_insert(0) += 1;
        let bucket = ((alert.severity * 5.0) as usize).min(4);
        self.sev_hist[bucket] += 1;
        self.alerts.push(alert);
        if self.alerts.len() > self.max_buf {
            self.alerts.remove(0);
        }
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

fn draw(
    term:        &mut Terminal<CrosstermBackend<io::Stdout>>,
    state:       &AppState,
    table_state: &mut TableState,
    filter:      &Option<String>,
    filter_input: &str,
    is_filtering: bool,
) -> Result<()> {
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

        let clock   = Local::now().format("%H:%M:%S").to_string();
        let filter_info = match (filter, is_filtering) {
            (_, true)       => format!("  │  filter: {}▌", filter_input),
            (Some(f), _)    => format!("  │  filter: {f}"),
            (None, _)       => String::new(),
        };
        let header_text = format!(
            " alarm-watcher  │  total received: {}  │  showing: {}  │  {}{}",
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
            .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
            .split(outer[1]);

        let hdr_cells = ["Time", "Card ID", "Anomaly type", "Sev", "Description"]
            .iter()
            .map(|h| {
                Cell::from(*h)
                    .style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Gray))
            });
        let hdr_row = Row::new(hdr_cells).height(1).bottom_margin(0);

        let shown: Vec<&Alert> = state.alerts.iter().rev()
            .filter(|a| {
                filter.as_ref().map_or(true, |f| a.card_id.contains(f.as_str()))
            })
            .collect();

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

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
            .split(body[1]);

        let mut kind_vec: Vec<(String, u64)> = state.by_kind
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        kind_vec.sort_by(|a, b| b.1.cmp(&a.1));

        let kind_refs: Vec<(&str, u64)> = kind_vec
            .iter()
            .map(|(k, v)| (k.as_str(), *v))
            .collect();

        let kind_chart = BarChart::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" By anomaly type "),
            )
            .data(&kind_refs)
            .bar_width(3)
            .bar_gap(1)
            .bar_style(Style::default().fg(Color::Cyan))
            .value_style(
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            );
        f.render_widget(kind_chart, right[0]);

        let sev_labels = ["0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"];
        let sev_refs: Vec<(&str, u64)> = sev_labels
            .iter()
            .zip(state.sev_hist.iter())
            .map(|(l, v)| (*l, *v))
            .collect();

        let sev_chart = BarChart::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Severity distribution "),
            )
            .data(&sev_refs)
            .bar_width(4)
            .bar_gap(1)
            .bar_style(Style::default().fg(Color::Yellow))
            .value_style(Style::default().fg(Color::White));
        f.render_widget(sev_chart, right[1]);

        let footer_line = if is_filtering {
            Line::from(vec![
                Span::raw("  Filter: "),
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
            Line::from(vec![
                Span::raw("  "),
                Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" quit   "),
                Span::styled("↑↓", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" scroll   "),
                Span::styled("f", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" filter by card   "),
                Span::styled("c", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" clear filter"),
                match filter {
                    Some(f) => Span::styled(
                        format!("   active: {f}"),
                        Style::default().fg(Color::Yellow),
                    ),
                    None => Span::raw(""),
                },
            ])
        };

        let footer = Paragraph::new(footer_line)
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(footer, outer[2]);
    })?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let state = Arc::new(Mutex::new(AppState::new(args.buffer)));

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

    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend).context("create terminal")?;

    let mut table_state  = TableState::default();
    let mut filter:       Option<String> = None;
    let mut filter_input: String         = String::new();
    let mut is_filtering: bool           = false;

    loop {
        {
            let s = state.lock().unwrap();
            draw(&mut term, &s, &mut table_state, &filter, &filter_input, is_filtering)?;
        }

        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if is_filtering {
                    match key.code {
                        KeyCode::Enter => {
                            filter = if filter_input.is_empty() {
                                None
                            } else {
                                Some(filter_input.clone())
                            };
                            filter_input.clear();
                            is_filtering = false;
                            table_state.select(Some(0));
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
                        KeyCode::Char('f') => {
                            is_filtering = true;
                        }
                        KeyCode::Char('c') => {
                            filter = None;
                            table_state.select(Some(0));
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            let s = state.lock().unwrap();
                            let max = s.alerts.len().saturating_sub(1);
                            let next = table_state.selected()
                                .map_or(0, |i| (i + 1).min(max));
                            drop(s);
                            table_state.select(Some(next));
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            let prev = table_state.selected()
                                .map_or(0, |i| i.saturating_sub(1));
                            table_state.select(Some(prev));
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