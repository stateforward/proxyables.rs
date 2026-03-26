use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u32)]
pub enum ValueKind {
    Function = 0x9ed64249,
    Array = 0x8a58ad26,
    String = 0x17c16538,
    Number = 0x1bd670a0,
    Boolean = 0x65f46ebf,
    Symbol = 0xf3fb51d1,
    Object = 0xb8c60cba,
    BigInt = 0x8a67a5ca,
    Unknown = 0x9b759fb9,
    Null = 0x77074ba4,
    Undefined = 0x9b61ad43,
    Reference = 0x5a1b3c4d,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u32)]
pub enum InstructionKind {
    Local = 0x9c436708,
    Get = 0x540ca757,
    Set = 0xc6270703,
    Apply = 0x24bc4a3b,
    Construct = 0x40c09172,
    Execute = 0xa01e3d98,
    Throw = 0x7a78762f,
    Return = 0x85ee37bf,
    Next = 0x5cb68de8,
    Release = 0x1a2b3c4d,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyInstruction {
    pub kind: u32,         // Serialized as generic primitive mostly, but we use u32
    pub data: rmpv::Value, // Use RMPV Value for dynamic typing, or serde_json::Value
    pub id: Option<String>,
    pub metadata: Option<rmpv::Value>,
}

// NOTE: To match TS exactly we might need custom serialization for bitwise matching.
// But mostly simple numbers work.

pub fn create_instruction(kind: InstructionKind, data: rmpv::Value) -> ProxyInstruction {
    ProxyInstruction {
        kind: kind as u32,
        data,
        id: Some(crate::muid::make()),
        metadata: None,
    }
}

pub fn create_get_instruction(key: impl Into<String>) -> ProxyInstruction {
    create_instruction(
        InstructionKind::Get,
        rmpv::Value::Array(vec![rmpv::Value::String(key.into().into())]),
    )
}

pub fn create_apply_instruction(args: Vec<rmpv::Value>) -> ProxyInstruction {
    create_instruction(InstructionKind::Apply, rmpv::Value::Array(args))
}

pub fn create_construct_instruction(args: Vec<rmpv::Value>) -> ProxyInstruction {
    create_instruction(InstructionKind::Construct, rmpv::Value::Array(args))
}

pub fn create_execute_instruction(instructions: Vec<ProxyInstruction>) -> ProxyInstruction {
    create_instruction(
        InstructionKind::Execute,
        rmpv::Value::Array(
            instructions
                .into_iter()
                .map(|instruction| {
                    rmpv::ext::to_value(instruction).expect("instruction should serialize")
                })
                .collect(),
        ),
    )
}

pub fn create_return_instruction(value: rmpv::Value) -> ProxyInstruction {
    create_instruction(InstructionKind::Return, value)
}

pub fn create_throw_instruction(message: impl Into<String>) -> ProxyInstruction {
    create_instruction(
        InstructionKind::Throw,
        rmpv::Value::String(message.into().into()),
    )
}

pub fn create_release_instruction(id: impl Into<String>) -> ProxyInstruction {
    create_instruction(
        InstructionKind::Release,
        rmpv::Value::Array(vec![rmpv::Value::String(id.into().into())]),
    )
}
