/// alarm-watcher — real-time TUI dashboard for the "alerts" Kafka topic.
///
/// Reads Alert messages produced by Student B's Flink detector and renders
/// a live terminal UI using ratatui:
///   - Top panel:  live alert feed (scrollable table)
///   - Left panel: per-anomaly-kind counts (bar chart)
///   - Right panel: severity histogram
///
/// Keybindings:
///   q / Ctrl-C  quit
///   ↑ / ↓       scroll alert table
///   f           filter by card ID (type, Enter to confirm)

use anyhow::Result;
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
    widgets::{
        BarChart, Block, Borders, Cell, Paragraph, Row, Scrollbar, ScrollbarOrientation,
        ScrollbarState, Table, TableState,
    },
    Terminal,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use shared::{Alert, TOPIC_ALERTS};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::error;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "alarm-watcher", about = "Real-time TUI for Flink alerts")]
struct Args {
    #[arg(long, default_value = "localhost:9092")]
    broker: String,

    #[arg(long, default_value = "alarm-watcher-dev")]
    group: String,

    /// Maximum number of alerts to keep in memory
    #[arg(long, default_value_t = 500)]
    buffer: usize,
}

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct AppState {
    alerts:     Vec<Alert>,
    by_kind:    HashMap<String, u64>,
    /// Severity histogram buckets: [0.0,0.2), [0.2,0.4), …, [0.8,1.0]
    severity_hist: [u64; 5],
    total: u64,
}

impl AppState {
    fn push(&mut self, alert: Alert, max_buf: usize) {
        let kind = format!("{}", alert.anomaly_kind);
        *self.by_kind.entry(kind).or_insert(0) += 1;
        let bucket = ((alert.severity * 5.0) as usize).min(4);
        self.severity_hist[bucket] += 1;
        self.total += 1;
        self.alerts.push(alert);
        if self.alerts.len() > max_buf {
            self.alerts.remove(0);
        }
    }
}

// ── Kafka consumer task ────────────────────────────────────────────────────────

async fn consume_loop(broker: String, group: String, state: Arc<Mutex<AppState>>, buf: usize) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers",   &broker)
        .set("group.id",            &group)
        .set("auto.offset.reset",   "latest")
        .set("enable.auto.commit",  "true")
        .create()
    {
        Ok(c) => c,
        Err(e) => { error!("Consumer create failed: {e}"); return; }
    };

    if let Err(e) = consumer.subscribe(&[TOPIC_ALERTS]) {
        error!("Subscribe failed: {e}"); return;
    }

    loop {
        let msg = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => { error!("Recv error: {e}"); continue; }
        };

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s,
            _ => continue,
        };

        match serde_json::from_str::<Alert>(payload) {
            Ok(alert) => {
                if let Ok(mut s) = state.lock() {
                    s.push(alert, buf);
                }
                let _ = consumer.commit_message(&msg, CommitMode::Async);
            }
            Err(e) => {
                error!("Alert parse error: {e}\nraw: {payload}");
            }
        }
    }
}

// ── TUI rendering ─────────────────────────────────────────────────────────────

fn severity_color(s: f64) -> Color {
    if s >= 0.8      { Color::Red }
    else if s >= 0.5 { Color::Yellow }
    else             { Color::Green }
}

fn draw(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &AppState,
    table_state: &mut TableState,
    filter: &Option<String>,
) -> Result<()> {
    terminal.draw(|f| {
        let size = f.area();

        // ── Main vertical split: header / body / footer ───────────────────
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),  // header
                Constraint::Min(10),    // body
                Constraint::Length(3),  // footer
            ])
            .split(size);

        // ── Header ────────────────────────────────────────────────────────
        let now = Local::now().format("%H:%M:%S").to_string();
        let header_text = format!(
            " alarm-watcher  │  total: {}  │  {}{}",
            state.total,
            now,
            filter.as_ref().map(|f| format!("  │  filter: {f}")).unwrap_or_default()
        );
        let header = Paragraph::new(header_text)
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(header, chunks[0]);

        // ── Body: table on the left, charts on the right ─────────────────
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
            .split(chunks[1]);

        // Alert table
        let header_cells = ["Time", "Card", "Kind", "Severity", "Description"]
            .iter()
            .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
        let header_row = Row::new(header_cells).height(1).bottom_margin(0);

        let filtered: Vec<&Alert> = state.alerts.iter().rev()
            .filter(|a| filter.as_ref().map_or(true, |f| a.card_id.contains(f.as_str())))
            .collect();

        let rows = filtered.iter().map(|a| {
            let ts = a.timestamp.with_timezone(&Local).format("%H:%M:%S").to_string();
            let sev = format!("{:.2}", a.severity);
            let color = severity_color(a.severity);
            Row::new(vec![
                Cell::from(ts),
                Cell::from(a.card_id[..a.card_id.len().min(12)].to_string()),
                Cell::from(format!("{}", a.anomaly_kind)),
                Cell::from(sev).style(Style::default().fg(color)),
                Cell::from(a.description[..a.description.len().min(40)].to_string()),
            ])
        });

        let table = Table::new(
            rows,
            [
                Constraint::Length(10),
                Constraint::Length(14),
                Constraint::Length(18),
                Constraint::Length(9),
                Constraint::Min(20),
            ],
        )
        .header(header_row)
        .block(Block::default().borders(Borders::ALL).title(" Alerts (newest first) "))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

        f.render_stateful_widget(table, body[0], table_state);

        // Right panel: kind bar chart + severity histogram
        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
            .split(body[1]);

        // By-kind bar chart
        let mut kind_data: Vec<(&str, u64)> = state.by_kind
            .iter()
            .map(|(k, v)| (k.as_str(), *v))
            .collect();
        kind_data.sort_by(|a, b| b.1.cmp(&a.1));
        let kind_refs: Vec<(&str, u64)> = kind_data.iter().map(|(k, v)| (*k, *v)).collect();

        let kind_chart = BarChart::default()
            .block(Block::default().borders(Borders::ALL).title(" By kind "))
            .data(&kind_refs)
            .bar_width(3)
            .bar_gap(1)
            .bar_style(Style::default().fg(Color::Cyan))
            .value_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));
        f.render_widget(kind_chart, right[0]);

        // Severity histogram
        let sev_labels = ["0–0.2", "0.2–0.4", "0.4–0.6", "0.6–0.8", "0.8–1.0"];
        let sev_data: Vec<(&str, u64)> = sev_labels.iter().zip(state.severity_hist.iter())
            .map(|(l, v)| (*l, *v))
            .collect();

        let sev_chart = BarChart::default()
            .block(Block::default().borders(Borders::ALL).title(" Severity distribution "))
            .data(&sev_data)
            .bar_width(5)
            .bar_gap(1)
            .bar_style(Style::default().fg(Color::Yellow))
            .value_style(Style::default().fg(Color::White));
        f.render_widget(sev_chart, right[1]);

        // ── Footer ────────────────────────────────────────────────────────
        let footer = Paragraph::new(Line::from(vec![
            Span::raw("  "),
            Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" quit  "),
            Span::styled("↑↓", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" scroll  "),
            Span::styled("f", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" filter by card"),
        ]))
        .block(Block::default().borders(Borders::ALL));
        f.render_widget(footer, chunks[2]);
    })?;
    Ok(())
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let state = Arc::new(Mutex::new(AppState::default()));

    // Spawn Kafka consumer on a background task.
    {
        let state  = state.clone();
        let broker = args.broker.clone();
        let group  = args.group.clone();
        let buf    = args.buffer;
        tokio::spawn(async move { consume_loop(broker, group, state, buf).await });
    }

    // Set up terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend  = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;

    let mut table_state = TableState::default();
    let mut filter: Option<String> = None;
    let mut filter_input = String::new();
    let mut filtering = false;

    loop {
        {
            let s = state.lock().unwrap();
            draw(&mut term, &s, &mut table_state, &filter)?;
        }

        if event::poll(Duration::from_millis(250))? {
            match event::read()? {
                Event::Key(key) => {
                    if filtering {
                        match key.code {
                            KeyCode::Enter => {
                                filter = if filter_input.is_empty() {
                                    None
                                } else {
                                    Some(filter_input.clone())
                                };
                                filter_input.clear();
                                filtering = false;
                            }
                            KeyCode::Esc => {
                                filter_input.clear();
                                filtering = false;
                            }
                            KeyCode::Backspace => { filter_input.pop(); }
                            KeyCode::Char(c)   => { filter_input.push(c); }
                            _ => {}
                        }
                    } else {
                        match key.code {
                            KeyCode::Char('q') => break,
                            KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => break,
                            KeyCode::Char('f') => { filtering = true; }
                            KeyCode::Down => {
                                let s = state.lock().unwrap();
                                let i = table_state.selected().map_or(0, |i| {
                                    (i + 1).min(s.alerts.len().saturating_sub(1))
                                });
                                table_state.select(Some(i));
                            }
                            KeyCode::Up => {
                                let i = table_state.selected().map_or(0, |i| i.saturating_sub(1));
                                table_state.select(Some(i));
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Restore terminal.
    disable_raw_mode()?;
    execute!(term.backend_mut(), LeaveAlternateScreen)?;
    term.show_cursor()?;

    Ok(())
}