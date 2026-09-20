use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct CycleLane {
    pub lane_id: String,
    pub cycle_id: String,
    #[serde(default)]
    pub objective_id: String,
    pub phase: String,
    pub state: String,
    #[serde(default)]
    pub role: String,
    pub started_at: String,
    #[serde(default)]
    pub latest_action: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct CompletedStage {
    #[serde(default)]
    pub objective_id: String,
    pub phase: String,
    pub completed_at: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Cycle {
    #[serde(default)]
    pub implemented_stages: Vec<String>,
    #[serde(default)]
    pub implemented_branches: Vec<String>,
    #[serde(default)]
    pub active_lanes: Vec<CycleLane>,
    #[serde(default)]
    pub last_completed: Option<CompletedStage>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct RecentOutcomes {
    pub window_seconds: u64,
    pub succeeded: u64,
    pub deferred: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Objective {
    pub id: String,
    pub status: String,
    pub description: String,
    pub priority: f64,
    #[serde(default)]
    pub failure_count: u64,
    #[serde(default)]
    pub eligible_at: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub latest_action: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Counters {
    #[serde(default)]
    pub model_calls: u64,
    #[serde(default)]
    pub tool_calls: u64,
    #[serde(default)]
    pub succeeded: u64,
    #[serde(default)]
    pub deferred: u64,
    #[serde(default)]
    pub failed: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub model_latency_ms: u64,
    #[serde(default)]
    pub tool_latency_ms: u64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Activity {
    pub kind: String,
    pub id: String,
    pub timestamp: String,
    #[serde(default)]
    pub objective_id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub duration_ms: u64,
}

pub(crate) type ActivityKey = (String, String);

impl Activity {
    pub(crate) fn key(&self) -> ActivityKey {
        (self.kind.clone(), self.id.clone())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TelemetryData {
    #[serde(default)]
    cycle: Cycle,
    #[serde(default)]
    recent_outcomes: RecentOutcomes,
    #[serde(default)]
    phase: String,
    #[serde(default)]
    phase_started_at: Option<String>,
    #[serde(default)]
    active_objective: Option<Objective>,
    #[serde(default)]
    objectives: Vec<Objective>,
    #[serde(default)]
    frontier: HashMap<String, usize>,
    #[serde(default)]
    counters: Counters,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TelemetryResponse {
    schema_version: u32,
    generated_at: String,
    started_at: String,
    uptime_seconds: u64,
    health: String,
    data: TelemetryData,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ActivityPage {
    schema_version: u32,
    #[serde(default)]
    items: Vec<Activity>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct ActivityRecord {
    pub schema_version: u32,
    pub kind: String,
    pub id: String,
    #[serde(default)]
    pub objective_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub started_at: String,
    #[serde(default)]
    pub finished_at: Option<String>,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub content: Value,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ActivityFilter {
    #[default]
    All,
    Model,
    Tool,
    Objective,
    Failure,
}

impl ActivityFilter {
    pub fn next(self) -> Self {
        match self {
            Self::All => Self::Model,
            Self::Model => Self::Tool,
            Self::Tool => Self::Objective,
            Self::Objective => Self::Failure,
            Self::Failure => Self::All,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Model => "model",
            Self::Tool => "tool",
            Self::Objective => "objective",
            Self::Failure => "failure",
        }
    }
    fn accepts(self, a: &Activity) -> bool {
        match self {
            Self::All => true,
            Self::Model => a.kind == "model",
            Self::Tool => a.kind == "tool",
            Self::Objective => a.kind == "objective",
            Self::Failure => matches!(
                a.status.as_str(),
                "failed" | "unknown" | "interrupted" | "quarantined"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutodState {
    pub socket: PathBuf,
    pub available: bool,
    pub health: String,
    pub generated_at: String,
    pub started_at: String,
    pub uptime_secs: u64,
    pub phase: String,
    pub phase_started_at: Option<String>,
    pub cycle: Cycle,
    pub recent_outcomes: RecentOutcomes,
    pub active_objective: Option<Objective>,
    pub objectives: Vec<Objective>,
    pub frontier: HashMap<String, usize>,
    pub counters: Counters,
    pub activities: Vec<Activity>,
    pub error: Option<String>,
    pub filter: ActivityFilter,
    pub selected: usize,
    pub selected_key: Option<ActivityKey>,
    pub follow_latest: bool,
    pub detail: Option<ActivityRecord>,
    pub detail_error: Option<String>,
    pub detail_scroll: u16,
}

impl Default for AutodState {
    fn default() -> Self {
        Self::new(PathBuf::from("/run/autod/telemetry.sock"))
    }
}
impl AutodState {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            available: false,
            health: "unavailable".into(),
            generated_at: String::new(),
            started_at: String::new(),
            uptime_secs: 0,
            phase: "unknown".into(),
            phase_started_at: None,
            cycle: Cycle::default(),
            recent_outcomes: RecentOutcomes::default(),
            active_objective: None,
            objectives: vec![],
            frontier: HashMap::new(),
            counters: Counters::default(),
            activities: vec![],
            error: Some("waiting for AUTOD telemetry".into()),
            filter: ActivityFilter::All,
            selected: 0,
            selected_key: None,
            follow_latest: true,
            detail: None,
            detail_error: None,
            detail_scroll: 0,
        }
    }
    pub fn visible_activities(&self) -> Vec<&Activity> {
        self.activities
            .iter()
            .filter(|a| self.filter.accepts(a))
            .collect()
    }
    pub fn selected_index(&self) -> Option<usize> {
        let visible = self.visible_activities();
        if visible.is_empty() {
            return None;
        }
        if self.follow_latest {
            Some(0)
        } else {
            self.selected_key
                .as_ref()
                .and_then(|key| visible.iter().position(|activity| activity.key() == *key))
        }
    }
    pub fn selected_activity(&self) -> Option<&Activity> {
        self.selected_index()
            .and_then(|index| self.visible_activities().get(index).copied())
    }
    pub fn selected_activity_key(&self) -> Option<ActivityKey> {
        self.selected_activity().map(Activity::key)
    }
    pub fn move_selection(&mut self, delta: isize) {
        let visible_keys: Vec<ActivityKey> = self
            .visible_activities()
            .iter()
            .map(|activity| activity.key())
            .collect();
        let n = visible_keys.len();
        if n == 0 {
            self.selected = 0;
            self.selected_key = None;
            self.clear_detail();
            return;
        }
        let current = self.selected_index().unwrap_or(0);
        self.selected = (current as isize + delta).clamp(0, n as isize - 1) as usize;
        self.selected_key = Some(visible_keys[self.selected].clone());
        self.follow_latest = self.selected == 0;
        self.clear_detail();
    }
    pub fn cycle_filter(&mut self) {
        self.filter = self.filter.next();
        self.selected = 0;
        self.selected_key = None;
        self.follow_latest = true;
        self.clear_detail();
    }
    pub(crate) fn apply(&mut self, response: TelemetryResponse, activities: Vec<Activity>) {
        self.available = true;
        self.error = None;
        self.health = response.health;
        self.generated_at = response.generated_at;
        self.started_at = response.started_at;
        self.uptime_secs = response.uptime_seconds;
        self.phase = if response.data.phase.is_empty() {
            "idle".into()
        } else {
            response.data.phase
        };
        self.phase_started_at = response.data.phase_started_at;
        self.cycle = response.data.cycle;
        self.recent_outcomes = response.data.recent_outcomes;
        self.active_objective = response.data.active_objective;
        self.objectives = response.data.objectives;
        self.frontier = response.data.frontier;
        self.counters = response.data.counters;
        let previous_key = self.selected_activity_key();
        self.activities = activities;
        let visible_keys: Vec<ActivityKey> = self
            .visible_activities()
            .iter()
            .map(|activity| activity.key())
            .collect();
        if self.follow_latest {
            self.selected = 0;
            self.selected_key = visible_keys.first().cloned();
            if previous_key != self.selected_key {
                self.clear_detail();
            }
        } else if let Some(key) = self.selected_key.as_ref() {
            if let Some(index) = visible_keys.iter().position(|candidate| candidate == key) {
                self.selected = index;
            } else {
                self.selected = self.selected.min(visible_keys.len().saturating_sub(1));
            }
        }
    }

    fn clear_detail(&mut self) {
        self.detail = None;
        self.detail_error = None;
        self.detail_scroll = 0;
    }
}

#[derive(Debug)]
pub enum Update {
    Snapshot(Box<TelemetryResponse>, Vec<Activity>),
    Unavailable(String),
    Detail {
        key: ActivityKey,
        result: Box<Result<ActivityRecord, String>>,
    },
}
pub struct AutodMonitor {
    socket: PathBuf,
    interval: Duration,
}
impl AutodMonitor {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            interval: Duration::from_secs(1),
        }
    }
    pub async fn run(self, tx: mpsc::Sender<Update>) {
        let mut failures = 0u32;
        loop {
            match fetch_snapshot(&self.socket).await {
                Ok((s, a)) => {
                    failures = 0;
                    if tx.send(Update::Snapshot(Box::new(s), a)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    failures = failures.saturating_add(1);
                    if tx.send(Update::Unavailable(e)).await.is_err() {
                        break;
                    }
                }
            }
            let delay = if failures == 0 {
                self.interval
            } else {
                Duration::from_secs(1u64 << failures.min(5))
            };
            tokio::time::sleep(delay).await;
        }
    }
}
pub async fn fetch_detail(
    socket: PathBuf,
    kind: String,
    id: String,
) -> Result<ActivityRecord, String> {
    request_json(&socket, &format!("/v1/telemetry/activity/{kind}/{id}")).await
}
async fn fetch_snapshot(socket: &Path) -> Result<(TelemetryResponse, Vec<Activity>), String> {
    let response: TelemetryResponse = request_json(socket, "/v1/telemetry").await?;
    if response.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported AUTOD telemetry schema {} (expected {})",
            response.schema_version, SCHEMA_VERSION
        ));
    }
    let page: ActivityPage = request_json(socket, "/v1/telemetry/activity?limit=200").await?;
    if page.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported AUTOD activity schema {}",
            page.schema_version
        ));
    }
    Ok((response, page.items))
}
async fn request_json<T: for<'de> Deserialize<'de>>(
    socket: &Path,
    path: &str,
) -> Result<T, String> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| socket_error(socket, &e))?;
    let request=format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAccept: application/json\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut raw = vec![];
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|e| e.to_string())?;
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("malformed HTTP response")?;
    let header = String::from_utf8_lossy(&raw[..split]);
    let status = header
        .lines()
        .next()
        .and_then(|s| s.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if status != 200 {
        return Err(format!("AUTOD telemetry returned HTTP {status}"));
    }
    let encoded = &raw[split + 4..];
    let body = if header
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        decode_chunked(encoded)?
    } else {
        encoded.to_vec()
    };
    serde_json::from_slice(&body).map_err(|e| format!("malformed AUTOD telemetry: {e}"))
}

fn decode_chunked(encoded: &[u8]) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    let mut rest = encoded;
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("malformed chunked AUTOD response")?;
        let size_text = std::str::from_utf8(&rest[..line_end]).map_err(|_| "invalid chunk size")?;
        let size = usize::from_str_radix(size_text.split(';').next().unwrap_or(""), 16)
            .map_err(|_| "invalid chunk size")?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Ok(body);
        }
        if rest.len() < size + 2 || &rest[size..size + 2] != b"\r\n" {
            return Err("truncated chunked AUTOD response".into());
        }
        body.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}
fn socket_error(path: &Path, e: &io::Error) -> String {
    match e.kind() {
        io::ErrorKind::NotFound => format!("AUTOD telemetry socket not found: {}", path.display()),
        io::ErrorKind::PermissionDenied => format!(
            "permission denied opening AUTOD telemetry: {}",
            path.display()
        ),
        _ => format!("AUTOD telemetry unavailable at {}: {e}", path.display()),
    }
}
pub(crate) fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}
#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> TelemetryResponse {
        serde_json::from_str(
            r#"{
                "schema_version": 1,
                "generated_at": "2026-09-20T13:30:00Z",
                "started_at": "2026-09-20T13:00:00Z",
                "uptime_seconds": 1800,
                "health": "running",
                "data": {}
            }"#,
        )
        .unwrap()
    }

    fn activity(kind: &str, id: &str) -> Activity {
        Activity {
            kind: kind.into(),
            id: id.into(),
            ..Default::default()
        }
    }

    #[test]
    fn filters_and_selection_are_bounded() {
        let mut s = AutodState::default();
        s.activities = vec![
            Activity {
                kind: "model".into(),
                status: "completed".into(),
                ..Default::default()
            },
            Activity {
                kind: "tool".into(),
                status: "failed".into(),
                ..Default::default()
            },
        ];
        s.filter = ActivityFilter::Failure;
        assert_eq!(s.visible_activities().len(), 1);
        s.move_selection(99);
        assert_eq!(s.selected, 0);
        s.cycle_filter();
        assert_eq!(s.filter, ActivityFilter::All)
    }

    #[test]
    fn live_selection_tracks_newest_and_pinned_selection_tracks_identity() {
        let mut s = AutodState::default();
        let old = activity("model", "old");
        let newest = activity("model", "newest");
        s.apply(snapshot(), vec![newest.clone(), old.clone()]);

        assert!(s.follow_latest);
        assert_eq!(s.selected_activity().unwrap().id, "newest");

        s.move_selection(1);
        assert!(!s.follow_latest);
        assert_eq!(s.selected_activity().unwrap().id, "old");

        s.apply(
            snapshot(),
            vec![activity("tool", "new-event"), newest, old.clone()],
        );
        assert_eq!(s.selected, 2);
        assert_eq!(s.selected_activity().unwrap().id, "old");
    }

    #[test]
    fn pinned_detail_survives_activity_expiry_until_newest_is_selected() {
        let mut s = AutodState::default();
        let old = activity("model", "old");
        s.apply(snapshot(), vec![activity("model", "newest"), old.clone()]);
        s.move_selection(1);
        s.detail = Some(ActivityRecord {
            id: "old".into(),
            ..Default::default()
        });

        s.apply(snapshot(), vec![activity("model", "newest")]);
        assert!(!s.follow_latest);
        assert!(s.selected_activity().is_none());
        assert_eq!(s.detail.as_ref().unwrap().id, "old");

        s.move_selection(-1);
        assert!(s.follow_latest);
        assert_eq!(s.selected_activity().unwrap().id, "newest");
        assert!(s.detail.is_none());
    }
    #[test]
    fn schema_mismatch_message_is_clear() {
        assert!(format!(
            "unsupported AUTOD telemetry schema {} (expected {})",
            2, SCHEMA_VERSION
        )
        .contains("expected 1"));
    }

    #[test]
    fn decodes_cycle_telemetry_v1() {
        let response: TelemetryResponse = serde_json::from_str(
            r#"{
                "schema_version":1,
                "generated_at":"2026-09-19T12:00:00Z",
                "started_at":"2026-09-19T11:00:00Z",
                "uptime_seconds":3600,
                "health":"running",
                "data":{
                    "cycle":{
                        "implemented_stages":["observe","discover","execute"],
                        "implemented_branches":["wait","recovery"],
                        "active_lanes":[{"lane_id":"global","cycle_id":"cycle-1","phase":"discover","state":"active","role":"scout","started_at":"2026-09-19T12:00:00Z","latest_action":"identify candidate work"}],
                        "last_completed":{"phase":"observe","completed_at":"2026-09-19T11:59:59Z"}
                    },
                    "recent_outcomes":{"window_seconds":900,"succeeded":1,"deferred":2,"failed":0},
                    "phase":"discover","objectives":[],"frontier":{},"counters":{}
                }
            }"#,
        )
        .unwrap();
        assert_eq!(response.data.cycle.active_lanes[0].phase, "discover");
        assert_eq!(response.data.cycle.last_completed.unwrap().phase, "observe");
        assert_eq!(response.data.recent_outcomes.deferred, 2);
    }

    #[test]
    fn decodes_chunked_http_body() {
        let encoded = b"7\r\n{\"a\":1}\r\n0\r\n\r\n";
        assert_eq!(decode_chunked(encoded).unwrap(), br#"{"a":1}"#);
        assert!(decode_chunked(b"5\r\nno").is_err());
    }
}
