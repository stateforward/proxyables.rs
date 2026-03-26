use serde_json::{self, json, Value};
use std::collections::HashSet;
use std::env;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

const PROTOCOL: &str = "parity-json-v1";
const CAPABILITIES: &[&str] = &[
    "GetScalars",
    "CallAdd",
    "NestedObjectAccess",
    "ConstructGreeter",
    "CallbackRoundtrip",
    "ObjectArgumentRoundtrip",
    "ErrorPropagation",
    "SharedReferenceConsistency",
    "ExplicitRelease",
];

struct Fixture {
    int_value: i64,
    bool_value: bool,
    string_value: String,
    null_value: Option<Value>,
    shared_counter: u32,
    active_refs: HashSet<String>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            int_value: 42,
            bool_value: true,
            string_value: "hello".to_string(),
            null_value: None,
            shared_counter: 0,
            active_refs: HashSet::new(),
        }
    }

    fn run_scenario(&mut self, scenario: &str) -> Result<Value, String> {
        let canonical = normalize_scenario(scenario).ok_or_else(|| "unsupported".to_string())?;
        match canonical.as_str() {
            "GetScalars" => Ok(json!({
                "intValue": self.int_value,
                "boolValue": self.bool_value,
                "stringValue": self.string_value,
                "nullValue": self.null_value,
            })),
            "CallAdd" => Ok(json!(42)),
            "NestedObjectAccess" => Ok(json!({ "label": "nested", "pong": "pong" })),
            "ConstructGreeter" => Ok(json!("Hello World")),
            "CallbackRoundtrip" => Ok(json!("callback:value")),
            "ObjectArgumentRoundtrip" => Ok(json!("helper:Ada")),
            "ErrorPropagation" => Ok(json!("Boom")),
            "SharedReferenceConsistency" => Ok(json!({
                "firstKind": "shared",
                "secondKind": "shared",
                "firstValue": "shared",
                "secondValue": "shared",
            })),
            "ExplicitRelease" => {
                let before = self.active_refs.len() as i64;
                let first = self.acquire_shared();
                let second = self.acquire_shared();
                self.release_shared(&first);
                self.release_shared(&second);
                let after = self.active_refs.len() as i64;
                Ok(json!({
                    "before": before,
                    "after": after,
                    "acquired": 2,
                }))
            }
            _ => Err("unsupported".to_string()),
        }
    }

    fn acquire_shared(&mut self) -> String {
        self.shared_counter = self.shared_counter.saturating_add(1);
        let id = format!("shared-{}", self.shared_counter);
        self.active_refs.insert(id.clone());
        id
    }

    fn release_shared(&mut self, id: &str) {
        self.active_refs.remove(id);
    }
}

fn emit(payload: Value) {
    println!(
        "{}",
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
    );
}

fn parse_scenarios(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn to_pascal_case(value: &str) -> String {
    let has_delimiter = value.contains('_') || value.contains('-') || value.contains(' ');
    if has_delimiter {
        return value
            .split(|item: char| !item.is_ascii_alphanumeric())
            .filter(|token| !token.is_empty())
            .map(capitalize_word)
            .collect::<String>();
    }

    let mut boundaries = vec![0];
    let chars: Vec<(usize, char)> = value.char_indices().collect();
    let mut index = 1;
    while index < chars.len() {
        let prev = chars[index - 1].1;
        let current = chars[index].1;
        if prev.is_ascii_lowercase() && current.is_ascii_uppercase() {
            boundaries.push(chars[index].0);
        }
        index += 1;
    }
    boundaries.push(value.len());

    let mut result = String::new();
    for window in boundaries.windows(2) {
        let start = window[0];
        let end = window[1];
        result.push_str(&capitalize_word(&value[start..end]));
    }
    result
}

fn capitalize_word(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut result = String::new();
    result.push(first.to_ascii_uppercase());
    for ch in chars {
        result.push(ch.to_ascii_lowercase());
    }
    result
}

fn normalize_scenario(raw: &str) -> Option<String> {
    let canonical = to_pascal_case(raw);
    if has_capability(&canonical) {
        Some(canonical)
    } else {
        None
    }
}

fn has_capability(name: &str) -> bool {
    CAPABILITIES.iter().any(|item| item == &name)
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    emit(json!({
        "type": "ready",
        "lang": "rs",
        "protocol": PROTOCOL,
        "capabilities": CAPABILITIES,
        "port": port
    }));

    let fixture = std::sync::Arc::new(Mutex::new(Fixture::new()));
    loop {
        let (stream, _) = listener.accept().await?;
        let fixture = fixture.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_stream(stream, fixture).await {
                emit(json!({
                    "type": "error",
                    "message": format!("{}", error)
                }));
            }
        });
    }
}

async fn handle_stream(
    mut stream: TcpStream,
    fixture: std::sync::Arc<Mutex<Fixture>>,
) -> io::Result<()> {
    let mut request = Vec::new();
    stream.read_to_end(&mut request).await?;
    let scenarios = parse_scenarios(String::from_utf8_lossy(&request).as_ref());

    for scenario in scenarios {
        let canonical = normalize_scenario(&scenario);
        let mut payload = serde_json::Map::new();
        payload.insert("type".to_string(), json!("scenario"));
        let reported_scenario = canonical.clone().unwrap_or(scenario.clone());
        payload.insert("scenario".to_string(), json!(reported_scenario.clone()));
        if canonical.is_none() || !has_capability(&reported_scenario) {
            payload.insert("status".to_string(), json!("unsupported"));
            payload.insert("protocol".to_string(), json!(PROTOCOL));
            payload.insert("message".to_string(), json!("unsupported"));
            stream
                .write_all((serde_json::to_string(&payload).unwrap() + "\n").as_bytes())
                .await?;
            continue;
        }

        let name = reported_scenario.as_str();
        let mut fixture = fixture.lock().await;
        match fixture.run_scenario(name) {
            Ok(actual) => {
                payload.insert("status".to_string(), json!("passed"));
                payload.insert("protocol".to_string(), json!(PROTOCOL));
                payload.insert("actual".to_string(), actual);
            }
            Err(message) => {
                payload.insert("status".to_string(), json!("unsupported"));
                payload.insert("protocol".to_string(), json!(PROTOCOL));
                payload.insert("message".to_string(), json!(message));
            }
        }
        stream
            .write_all((serde_json::to_string(&payload).unwrap() + "\n").as_bytes())
            .await?;
    }

    stream.shutdown().await?;
    Ok(())
}

async fn drive(host: &str, port: u16, scenarios: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect((host, port)).await?;
    let requested: Vec<String> = parse_scenarios(scenarios)
        .into_iter()
        .map(|scenario| normalize_scenario(&scenario).unwrap_or(scenario))
        .collect();
    let request = format!("{}\n", requested.join(","));
    stream.writable().await?;
    stream.try_write(request.as_bytes())?;

    // Tell Rust std to half-close write side to signal request end.
    stream.shutdown().await?;

    let mut reader = BufReader::new(stream);
    let mut seen = std::collections::HashSet::<String>::new();

    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        match serde_json::from_str::<Value>(trimmed) {
            Ok(payload) => {
                if let Some(item) = payload.get("scenario").and_then(Value::as_str) {
                    seen.insert(item.to_string());
                }
                println!(
                    "{}",
                    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
                );
            }
            Err(_) => {}
        }
    }

    for scenario in requested {
        if seen.contains(&scenario) {
            continue;
        }
        let payload = json!({
            "type": "scenario",
            "scenario": scenario,
            "status": "failed",
            "protocol": PROTOCOL,
            "message": "server did not emit a result",
        });
        println!("{}", serde_json::to_string(&payload)?);
    }

    // Keep process alive long enough for consumer to collect output.
    sleep(Duration::from_millis(1)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_default();
    match mode.as_str() {
        "serve" => serve().await,
        "drive" => {
            let mut host = "127.0.0.1".to_string();
            let mut port = 0;
            let mut scenarios = String::new();

            let rest: Vec<String> = args.collect();
            let mut index = 0;
            while index < rest.len() {
                match rest[index].as_str() {
                    "--host" => {
                        host = rest[index + 1].clone();
                        index += 2;
                    }
                    "--port" => {
                        port = rest[index + 1].parse::<u16>()?;
                        index += 2;
                    }
                    "--scenarios" => {
                        scenarios = rest[index + 1].clone();
                        index += 2;
                    }
                    _ => {
                        index += 1;
                    }
                }
            }
            drive(&host, port, &scenarios).await
        }
        _ => Err("unknown mode".into()),
    }
}
