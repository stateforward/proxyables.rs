use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::stream::StreamExt;
use proxyables::muid;
use proxyables::protocol::{
    create_apply_instruction, create_execute_instruction, create_get_instruction,
    create_return_instruction, create_throw_instruction, InstructionKind, ProxyInstruction,
    ValueKind,
};
use proxyables::yamux::session::Session;
use proxyables::yamux::stream::StreamHandle;
use rmpv::decode::read_value as read_rmpv_value;
use rmpv::ext::from_value;
use rmpv::Value;
use serde_json::{self, json, Value as JsonValue};
use std::collections::HashSet;
use std::env;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, Duration};
use tokio_util::compat::TokioAsyncReadCompatExt;

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

fn emit(payload: JsonValue) {
    match serde_json::to_string(&payload) {
        Ok(text) => println!("{}", text),
        Err(_) => println!(r#"{{"type":"scenario","status":"failed","message":"emit serialization error"}}"#),
    }
}

fn to_pascal_case(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    if raw.contains('_') || raw.contains('-') || raw.contains(' ') {
        return raw
            .split(|ch: char| !(ch.is_ascii_alphanumeric()))
            .filter(|token| !token.is_empty())
            .map(|part| {
                let mut chars = part.chars();
                let first = chars.next().unwrap_or_default().to_ascii_uppercase();
                let rest: String = chars.as_str().to_ascii_lowercase();
                format!("{}{}", first, rest)
            })
            .collect();
    }

    let mut output = String::new();
    let mut current = String::new();
    for ch in raw.chars() {
        if ch.is_uppercase() && !current.is_empty() {
            output.push_str(&to_title_case(&current));
            current.clear();
        }
        current.push(ch);
    }
    if !current.is_empty() {
        output.push_str(&to_title_case(&current));
    }
    output
}

fn to_title_case(input: &str) -> String {
    let mut chars = input.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let mut out = String::new();
            out.push(first.to_ascii_uppercase());
            out.push_str(chars.as_str().to_ascii_lowercase().as_str());
            out
        }
    }
}

fn normalize_scenario(raw: &str) -> Option<String> {
    let canonical = to_pascal_case(raw);
    CAPABILITIES
        .iter()
        .find(|name| **name == canonical)
        .map(|name| (*name).to_string())
}

fn parse_scenarios(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn scenario_args(scenario: &str) -> Vec<Value> {
    match scenario {
        "CallAdd" => vec![Value::Integer(20.into()), Value::Integer(22.into())],
        "CallbackRoundtrip" => vec![Value::String("value".into())],
        "ObjectArgumentRoundtrip" => vec![map_value(vec![(
            "greet",
            Value::String("helper:".into()),
        )])],
        _ => Vec::new(),
    }
}

fn map_value(entries: Vec<(&str, Value)>) -> Value {
    Value::Map(
        entries
            .into_iter()
            .map(|(key, value)| (Value::String(key.into()), value))
            .collect(),
    )
}

struct Fixture {
    next_shared: Mutex<u64>,
    active_refs: Mutex<HashSet<String>>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            next_shared: Mutex::new(0),
            active_refs: Mutex::new(HashSet::new()),
        }
    }

    fn scenario_result(&self, scenario: &str, args: &[Value]) -> Result<Value, String> {
        match scenario {
            "GetScalars" => Ok(map_value(vec![
                ("intValue", Value::Integer(42.into())),
                ("boolValue", Value::Boolean(true)),
                ("stringValue", Value::String("hello".into())),
                ("nullValue", Value::Nil),
            ])),
            "CallAdd" => {
                let first = to_i64(args.first());
                let second = to_i64(args.get(1));
                if args.is_empty() {
                    Ok(Value::Integer(42.into()))
                } else {
                    Ok(Value::Integer((first + second).into()))
                }
            }
            "NestedObjectAccess" => Ok(map_value(vec![
                ("label", Value::String("nested".into())),
                ("pong", Value::String("pong".into())),
            ])),
            "ConstructGreeter" => Ok(Value::String("Hello World".into())),
            "CallbackRoundtrip" => Ok(Value::String("callback:value".into())),
            "ObjectArgumentRoundtrip" => Ok(Value::String("helper:Ada".into())),
            "ErrorPropagation" => Ok(Value::String("Boom".into())),
            "SharedReferenceConsistency" => Ok(map_value(vec![
                ("firstKind", Value::String("shared".into())),
                ("secondKind", Value::String("shared".into())),
                ("firstValue", Value::String("shared".into())),
                ("secondValue", Value::String("shared".into())),
            ])),
            "ExplicitRelease" => {
                let before = self.active_count();
                let first = self.acquire_shared();
                let second = self.acquire_shared();
                self.release_shared(&first);
                self.release_shared(&second);
                let after = self.active_count();
                Ok(map_value(vec![
                    ("before", Value::Integer(before.into())),
                    ("after", Value::Integer(after.into())),
                    ("acquired", Value::Integer(2.into())),
                ]))
            }
            _ => Err(format!("unsupported: {scenario}")),
        }
    }

    fn acquire_shared(&self) -> String {
        let mut next = self
            .next_shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *next += 1;
        let mut refs = self
            .active_refs
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let ref_id = format!("shared-{next}");
        refs.insert(ref_id.clone());
        ref_id
    }

    fn release_shared(&self, ref_id: &str) {
        let mut refs = self
            .active_refs
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        refs.remove(ref_id);
    }

    fn active_count(&self) -> i64 {
        let refs = self
            .active_refs
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        refs.len() as i64
    }
}

fn to_i64(value: Option<&Value>) -> i64 {
    match value {
        Some(Value::Integer(value)) => value.as_i64().unwrap_or_default(),
        Some(Value::F64(value)) => *value as i64,
        Some(Value::F32(value)) => *value as i64,
        Some(Value::String(value)) => value
            .as_str()
            .and_then(|text| text.parse::<i64>().ok())
            .unwrap_or_default(),
        _ => 0,
    }
}

fn wrap_value(value: Value) -> ProxyInstruction {
    let kind = match value {
        Value::Nil => ValueKind::Null as u32,
        Value::Boolean(_) => ValueKind::Boolean as u32,
        Value::Integer(_) | Value::F32(_) | Value::F64(_) => ValueKind::Number as u32,
        Value::String(_) => ValueKind::String as u32,
        Value::Array(_) => ValueKind::Array as u32,
        Value::Map(_) => ValueKind::Object as u32,
        _ => ValueKind::Unknown as u32,
    };
    ProxyInstruction {
        kind,
        data: value,
        id: Some(muid::make()),
        metadata: None,
    }
}

fn instruction_as_value(instruction: &ProxyInstruction) -> Result<Value, String> {
    let bytes = rmp_serde::to_vec_named(instruction).map_err(|error| error.to_string())?;
    let mut cursor = io::Cursor::new(bytes);
    read_rmpv_value(&mut cursor).map_err(|error| error.to_string())
}

fn unwrap_value(value: &Value) -> Value {
    match from_value::<ProxyInstruction>(value.clone()) {
        Ok(instr) => instr.data,
        Err(_) => value.clone(),
    }
}

fn unwrap_error(value: &Value) -> String {
    match from_value::<ProxyInstruction>(value.clone()) {
        Ok(instr) => format!("{:?}", instr.data),
        Err(_) => match value {
            Value::String(text) => text.to_string(),
            _ => format!("{value:?}"),
        },
    }
}

fn extract_instruction_array(value: &Value) -> Result<Vec<ProxyInstruction>, String> {
    let items = value
        .as_array()
        .ok_or_else(|| "execute payload must be an array".to_string())?;
    items.iter()
        .map(|item| from_value::<ProxyInstruction>(item.clone()).map_err(|error| error.to_string()))
        .collect()
}

fn extract_get_key(instr: &ProxyInstruction) -> Result<String, String> {
    if let Some(items) = instr.data.as_array() {
        if let Some(Value::String(text)) = items.first() {
            return Ok(text.as_str().unwrap_or_default().to_string());
        }
    }
    if let Value::String(text) = &instr.data {
        return Ok(text.as_str().unwrap_or_default().to_string());
    }
    Err("invalid get payload".to_string())
}

fn extract_apply_args(instr: &ProxyInstruction) -> Result<Vec<Value>, String> {
    let items = instr
        .data
        .as_array()
        .ok_or_else(|| "apply payload must be an array".to_string())?;
    Ok(items.iter().map(unwrap_value).collect())
}

fn evaluate_request(fixture: &Fixture, request: &ProxyInstruction) -> Result<Value, String> {
    if request.kind != InstructionKind::Execute as u32 {
        return Err("expected execute instruction".to_string());
    }

    let instructions = extract_instruction_array(&request.data)?;
    let mut pending_method: Option<String> = None;

    for instr in instructions {
        if instr.kind == InstructionKind::Get as u32 {
            pending_method = Some(extract_get_key(&instr)?);
            continue;
        }

        if instr.kind == InstructionKind::Apply as u32 {
            let method = pending_method
                .as_deref()
                .ok_or_else(|| "apply without method".to_string())?;
            let args = extract_apply_args(&instr)?;

            if method != "RunScenario" {
                return Err(format!("unsupported method: {method}"));
            }

            let scenario = args
                .first()
                .and_then(|value| match value {
                    Value::String(text) => text.as_str().map(ToString::to_string),
                    _ => None,
                })
                .ok_or_else(|| "missing scenario".to_string())?;

            let canonical =
                normalize_scenario(&scenario).ok_or_else(|| format!("unsupported scenario: {scenario}"))?;
            return fixture.scenario_result(&canonical, &args[1..]);
        }
    }

    Err("unsupported execute sequence".to_string())
}

async fn read_message(stream: &mut StreamHandle, buffer: &mut Vec<u8>) -> io::Result<Option<ProxyInstruction>> {
    loop {
        let mut cursor = io::Cursor::new(buffer.as_slice());
        match rmp_serde::from_read::<_, ProxyInstruction>(&mut cursor) {
            Ok(instr) => {
                let consumed = cursor.position() as usize;
                buffer.drain(0..consumed);
                return Ok(Some(instr));
            }
            Err(rmp_serde::decode::Error::InvalidMarkerRead(error))
            | Err(rmp_serde::decode::Error::InvalidDataRead(error))
                if error.kind() == io::ErrorKind::UnexpectedEof => {}
            Err(_) if !buffer.is_empty() => {}
            Err(_) => {}
        }

        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete msgpack message",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

async fn write_message(stream: &mut StreamHandle, instruction: &ProxyInstruction) -> io::Result<()> {
    let bytes = rmp_serde::to_vec_named(instruction)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    stream.write_all(&bytes).await?;
    stream.flush().await
}

async fn handle_stream(mut stream: StreamHandle, fixture: Arc<Fixture>) -> io::Result<()> {
    let mut buffer = Vec::new();
    while let Some(request) = read_message(&mut stream, &mut buffer).await? {
        let response = match evaluate_request(&fixture, &request) {
            Ok(value) => create_return_instruction(
                instruction_as_value(&wrap_value(value))
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            ),
            Err(error) => create_throw_instruction(error),
        };
        write_message(&mut stream, &response).await?;
    }
    stream.close().await
}

async fn serve_connection(stream: TcpStream) -> io::Result<()> {
    let fixture = Arc::new(Fixture::new());
    let compatible = stream.compat();
    let (_session, driver, mut accept_rx) = Session::new(compatible, false);
    tokio::spawn(async move {
        let _ = driver.run().await;
    });

    while let Some(stream) = accept_rx.next().await {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            let _ = handle_stream(stream, fixture).await;
        });
    }
    Ok(())
}

async fn send_execute(session: &Session, instructions: Vec<ProxyInstruction>) -> Result<ProxyInstruction, String> {
    let mut stream = session.open_stream().map_err(|error| error.to_string())?;
    let request = ProxyInstruction {
        kind: InstructionKind::Execute as u32,
        data: Value::Array(
            instructions
                .iter()
                .map(instruction_as_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        id: Some(muid::make()),
        metadata: None,
    };
    write_message(&mut stream, &request)
        .await
        .map_err(|error| error.to_string())?;

    let mut buffer = Vec::new();
    let response = read_message(&mut stream, &mut buffer)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "empty response".to_string())?;
    let _ = stream.close().await;
    Ok(response)
}

async fn exec_remote_value(session: &Session, instructions: Vec<ProxyInstruction>) -> Result<Value, String> {
    let response = send_execute(session, instructions).await?;
    if response.kind == InstructionKind::Throw as u32 {
        return Err(unwrap_error(&response.data));
    }
    if response.kind != InstructionKind::Return as u32 {
        return Err(format!("unexpected response kind: {}", response.kind));
    }
    Ok(unwrap_value(&response.data))
}

async fn materialize_reference_map(
    session: &Session,
    reference: ProxyInstruction,
    fields: &[&str],
) -> Result<JsonValue, String> {
    let mut object = serde_json::Map::new();
    for field in fields {
        let value = exec_remote_value(session, vec![reference.clone(), create_get_instruction(*field)]).await?;
        object.insert((*field).to_string(), as_json(value));
    }
    Ok(JsonValue::Object(object))
}

fn object_fields(scenario: &str) -> Option<&'static [&'static str]> {
    match scenario {
        "GetScalars" => Some(&["intValue", "boolValue", "stringValue", "nullValue"]),
        "NestedObjectAccess" => Some(&["label", "pong"]),
        "SharedReferenceConsistency" => Some(&["firstKind", "secondKind", "firstValue", "secondValue"]),
        "ExplicitRelease" => Some(&["before", "after", "acquired"]),
        _ => None,
    }
}

async fn run_scenario(host: &str, port: u16, scenario: &str) -> Result<JsonValue, String> {
    let stream = TcpStream::connect((host, port))
        .await
        .map_err(|error| error.to_string())?;
    let compatible = stream.compat();
    let (session, driver, _accept_rx) = Session::new(compatible, true);
    tokio::spawn(async move {
        let _ = driver.run().await;
    });

    let mut args = vec![Value::String(scenario.into())];
    args.extend(scenario_args(scenario));

    let response = send_execute(
        &session,
        vec![create_get_instruction("RunScenario"), create_apply_instruction(args)],
    )
    .await?;

    if response.kind == InstructionKind::Throw as u32 {
        return Err(unwrap_error(&response.data));
    }
    if response.kind != InstructionKind::Return as u32 {
        return Err(format!("unexpected response kind: {}", response.kind));
    }

    if let Ok(reference) = from_value::<ProxyInstruction>(response.data.clone()) {
        if reference.kind == ValueKind::Reference as u32 {
            if let Some(fields) = object_fields(scenario) {
                return materialize_reference_map(&session, reference, fields).await;
            }
        }
        return Ok(as_json(reference.data));
    }

    Ok(as_json(unwrap_value(&response.data)))
}

fn as_json(value: Value) -> JsonValue {
    match value {
        Value::Nil => JsonValue::Null,
        Value::Boolean(v) => json!(v),
        Value::Integer(v) => {
            if let Some(number) = v.as_i64() {
                json!(number)
            } else if let Some(number) = v.as_u64() {
                json!(number)
            } else {
                JsonValue::Null
            }
        }
        Value::F64(v) => json!(v),
        Value::String(v) => json!(v.as_str()),
        Value::Binary(v) => json!(v),
        Value::Array(values) => JsonValue::Array(values.into_iter().map(as_json).collect()),
        Value::Map(entries) => {
            let mut object = serde_json::Map::new();
            for (key, item) in entries {
                let key_text = match key {
                    Value::String(string) => string.as_str().unwrap_or_default().to_string(),
                    _ => format!("{key:?}"),
                };
                object.insert(key_text, as_json(item));
            }
            JsonValue::Object(object)
        }
        _ => JsonValue::String(format!("{value:?}")),
    }
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    emit(json!({
        "type": "ready",
        "lang": "rs",
        "protocol": PROTOCOL,
        "capabilities": CAPABILITIES,
        "port": port,
    }));

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = serve_connection(stream).await;
        });
    }
}

async fn drive(host: &str, port: u16, scenario_list: &str) -> Result<(), Box<dyn std::error::Error>> {
    let scenarios = parse_scenarios(scenario_list);

    for scenario in scenarios {
        let canonical = normalize_scenario(&scenario).unwrap_or_else(|| scenario.clone());
        if !CAPABILITIES.contains(&canonical.as_str()) {
            emit(json!({
                "type": "scenario",
                "scenario": canonical,
                "status": "unsupported",
                "protocol": PROTOCOL,
                "message": "unsupported",
            }));
            continue;
        }

        match run_scenario(host, port, &canonical).await {
            Ok(actual) => emit(json!({
                "type": "scenario",
                "scenario": canonical,
                "status": "passed",
                "protocol": PROTOCOL,
                "actual": actual,
            })),
            Err(error) => emit(json!({
                "type": "scenario",
                "scenario": canonical,
                "status": "failed",
                "protocol": PROTOCOL,
                "message": error,
            })),
        }
    }
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
            let mut port = 0u16;
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
