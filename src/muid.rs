
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Mutex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// EPOCH = 1700000000000
const EPOCH: u64 = 1700000000000;
const MACHINE_ID_BITS: u64 = 14;
const COUNTER_BITS: u64 = 9;
const MAX_MACHINE_ID: u64 = (1 << MACHINE_ID_BITS) - 1;
const MAX_COUNTER: u64 = (1 << COUNTER_BITS) - 1;

lazy_static::lazy_static! {
    static ref STATE: Mutex<GeneratorState> = Mutex::new(GeneratorState::new());
}

struct GeneratorState {
    last_timestamp: u64,
    counter: u64,
    machine_id: u64,
}

impl GeneratorState {
    fn new() -> Self {
        let hostname = hostname::get().unwrap_or_default();
        let mut hasher = DefaultHasher::new();
        hostname.hash(&mut hasher);
        let machine_id = hasher.finish() & MAX_MACHINE_ID;
        
        Self {
            last_timestamp: 0,
            counter: 0,
            machine_id,
        }
    }
}

pub fn make() -> String {
    let mut state = STATE.lock().unwrap();
    let mut current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 - EPOCH;
    
    if current_timestamp < state.last_timestamp {
        current_timestamp = state.last_timestamp;
    }
    
    if current_timestamp == state.last_timestamp {
        state.counter = (state.counter + 1) & MAX_COUNTER;
        if state.counter == 0 {
            // Overflow, wait
            while current_timestamp == state.last_timestamp {
                 current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 - EPOCH;
            }
        }
    } else {
        state.counter = 0;
    }
    
    state.last_timestamp = current_timestamp;
    
    let id_int = (current_timestamp << (MACHINE_ID_BITS + COUNTER_BITS))
        | (state.machine_id << COUNTER_BITS)
        | state.counter;
        
    to_base32(id_int)
}

fn to_base32(mut num: u64) -> String {
    let alphabet = b"0123456789abcdefghijklmnopqrstuv";
    let mut res = Vec::new();
    if num == 0 { return "0".to_string(); }
    while num > 0 {
        res.push(alphabet[(num % 32) as usize] as char);
        num /= 32;
    }
    res.into_iter().rev().collect()
}
