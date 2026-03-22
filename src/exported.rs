
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::channel::mpsc;
use futures::{StreamExt, FutureExt};
use std::sync::Arc;
use rmpv::Value;
use std::io;
use std::marker::Unpin;

use crate::yamux::stream::StreamHandle;
use crate::registry::Registry;
use crate::protocol::{ProxyInstruction, InstructionKind, ValueKind, ProxyInstruction as PInstr};

pub struct ExportedProxyable {
    registry: Registry,
    accept_receiver: mpsc::UnboundedReceiver<StreamHandle>,
}

impl ExportedProxyable {
    pub fn new(accept_receiver: mpsc::UnboundedReceiver<StreamHandle>, registry: Registry) -> Self {
        Self {
            registry,
            accept_receiver,
        }
    }

    pub async fn next_connection(&mut self) -> Option<impl std::future::Future<Output = ()> + Send + 'static> {
        let stream_handle = self.accept_receiver.next().await?;
        let registry = self.registry.clone();
        
        Some(async move {
            if let Err(e) = handle_rpc_stream(stream_handle, registry).await {
                // In a real server, we might log this or just ignore generic connection drops
                let _ = e; 
            }
        })
    }
}

async fn handle_rpc_stream(mut stream: StreamHandle, registry: Registry) -> io::Result<()> {
    // We need to read MsgPack values continuously.
    // Since StreamHandle implements AsyncRead, we can use a loop.
    // However, rmp_serde::from_read works on Sync Read.
    // We need to read length-prefixed chunks OR use a streaming deserializer that works with bytes.
    
    // Better approach for Async:
    // 1. Read a chunk of bytes.
    // 2. Try to decode from buffer.
    // 3. If successful, advance buffer.
    // 4. Repeat.
    
    // rmp_serde doesn't easily support "partial" decode without a cursor.
    // We can use `rmpv::decode::read_value` which might be easier to verify complete object?
    
    // Let's implement a simple framing:
    // [Length u32][MsgPack Payload]
    // OR just rely on MsgPack self-delimiting property?
    // rmpv::decode::read_value works on `Read`. 
    // We can wrap `stream` in `futures::io::BufReader`? No, that's async.
    
    // Simplest Robust Async MsgPack reader:
    // Read 1 byte (Marker). Determine length. Read rest.
    // This is reimplementing MsgPack parser.
    
    // Alternative: Read into a growing vector. Try to decode.
    // If UnexpectedEof, wait for more data.
    
    let mut buf = Vec::new();
    let mut tmp_buf = [0u8; 1024];
    
    loop {
        // Try decoding from buf
        let position = {
            let mut cursor = io::Cursor::new(&buf);
            match rmp_serde::from_read::<_, PInstr>(&mut cursor) {
                Ok(instr) => {
                    let pos = cursor.position() as usize;
                    // Process instruction
                    let res = process_instruction(instr, &registry).await;
                    
                    // Send response
                    let data = rmp_serde::to_vec(&res).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    stream.write_all(&data).await?;
                    
                    Some(pos)
                },
                Err(rmp_serde::decode::Error::InvalidMarkerRead(ref e)) | Err(rmp_serde::decode::Error::InvalidDataRead(ref e)) => {
                     // Check if it's EOF
                     if e.kind() == io::ErrorKind::UnexpectedEof {
                         // Need more data
                         None
                     } else {
                         return Err(io::Error::new(io::ErrorKind::InvalidData, "MsgPack Decode Error"));
                     }
                }
                 Err(_) => {
                    // Assume incomplete.
                    None
                }
            }
        };

        if let Some(pos) = position {
            // Consumed `pos` bytes.
            buf.drain(0..pos);
            continue; // Try parsing next immediately
        }

        // Need more data
        let n = stream.read(&mut tmp_buf).await?;
        if n == 0 {
            if buf.is_empty() {
                break; // Clean EOF
            } else {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete MsgPack"));
            }
        }
        buf.extend_from_slice(&tmp_buf[0..n]);
    }
    
    Ok(())
}

async fn process_instruction(instr: PInstr, registry: &Registry) -> PInstr {
    // Dispatch
    if instr.kind == InstructionKind::Get as u32 {
         // Data should be [target_id, prop_name]? 
         // Or just { target, name }
         // This depends on how arguments are serialized in Protocol.
         // Let's assume `instr.data` holds an Array: [target_id, property] or similar.
         // Or `instr.id` is the target?
         // In TS: `id` field of instruction is the target MUID.
         
         if let Some(target_id) = &instr.id {
             if let Some(target) = registry.get(target_id) {
                 // Argument decoding
                 // instr.data should be the property name string?
                 if let Some(name) = instr.data.as_str() {
                     match target.get(name).await {
                         Ok(val) => return create_result(val, true),
                         Err(e) => return create_error(e),
                     }
                 }
             }
         }
    }
    
    if instr.kind == InstructionKind::Execute as u32 || instr.kind == InstructionKind::Apply as u32 {
        if let Some(target_id) = &instr.id {
             if let Some(target) = registry.get(target_id) {
                 // instr.metadata might contain method name?
                 // Or instr.data is [method, args...] or just args?
                 // Standard Proxyables:
                 // Apply: data is Args Array. access via object (function).
                 // Execute: same?
                 
                 // If `data` is array of args.
                 // We need method name. 
                 // If it's `get` result -> function.
                 // In TS, ApplyInstruction has `data: args`.
                 
                 // How do we know WHICH method?
                 // The "Target" might be a Bound Function (a separate object in registry).
                 // Or we use `call(name, args)`.
                 
                 // For now, let's assume the Target is the Object, and we need a name.
                 // If Protocol doesn't send name in Apply, then Target must be the Function itself.
                 // That implies `get` returns a Reference/ID pointing to a "Method Object"?
                 // Yes, `Bind` in TS creates a new Proxy/Reference.
                 
                 // So: `call` on target with empty name?
                 // Let's pass empty name for now or refactor ProxyTarget trait.
                 
                 if let Some(args_vec) = instr.data.as_array() {
                     let args = args_vec.clone(); // Vec<Value>
                     match target.call("", args).await {
                         Ok(val) => return create_result(val, true),
                         Err(e) => return create_error(e),
                     }
                 }
             }
        }
    }

    // Default Error
    create_error("Unknown Instruction or Target".to_string())
}

fn create_result(val: Value, success: bool) -> PInstr {
    PInstr {
        kind: if success { InstructionKind::Return as u32 } else { InstructionKind::Throw as u32 },
        data: val,
        id: None,
        metadata: None,
    }
}

fn create_error(msg: String) -> PInstr {
    PInstr {
        kind: InstructionKind::Throw as u32,
        data: Value::String(msg.into()),
        id: None,
        metadata: None,
    }
}
