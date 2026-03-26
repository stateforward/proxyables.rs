
use futures::io::{AsyncReadExt, AsyncWriteExt};
use std::io;
use rmpv::Value;
use crate::yamux::session::Session;
use crate::protocol::{ProxyInstruction, InstructionKind};

// A handle to the remote proxy
#[derive(Clone)]
pub struct ImportedProxyable {
    session: Session,
}

impl ImportedProxyable {
    pub fn new(session: Session) -> Self {
        Self {
            session,
        }
    }

    /// Execute a remote method call
    pub async fn call(&self, target_id: Option<String>, _method: String, args: Vec<Value>) -> Result<Value, String> {
        // This helper specifically for "call" needs to mirror that flow if we want high fidelity.
        // But for "Direct" dispatch (if supported):
        // We'll stick to: Open Stream -> Send EXECUTE/APPLY -> Read Request.
        self.apply(target_id, args).await
    }

    pub async fn apply(&self, target_id: Option<String>, args: Vec<Value>) -> Result<Value, String> {
        self.perform_request(ProxyInstruction {
            kind: InstructionKind::Apply as u32,
            id: target_id,
            data: Value::Array(args),
            metadata: None,
        }).await
    }
    
    pub async fn get(&self, target_id: Option<String>, property: String) -> Result<Value, String> {
        self.perform_request(ProxyInstruction {
            kind: InstructionKind::Get as u32,
            id: target_id,
            data: Value::String(property.into()),
            metadata: None,
        }).await
    }
    
    async fn perform_request(&self, instr: ProxyInstruction) -> Result<Value, String> {
        // 1. Open Stream
        let mut stream = self.session.open_stream().map_err(|e| e.to_string())?;
        
        // 2. Send Instruction
        let data = rmp_serde::to_vec(&instr).map_err(|e| e.to_string())?;
        stream.write_all(&data).await.map_err(|e| e.to_string())?;
        
        // 3. Read Response
        // We expect ONE response.
        // We must implement framing reading similar to Exported.
        // For request/response, strictly one object back.
        
        // Simple read for now:
        // We assume response fits in buffer or we use proper reader.
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        
        loop {
            let n = stream.read(&mut tmp).await.map_err(|e| e.to_string())?;
            if n == 0 {
                // EOF.
                if buf.is_empty() {
                    return Err("Connection closed before response".to_string());
                }
                break; // Try decode what we have
            }
            buf.extend_from_slice(&tmp[0..n]);
            
            // Try decode
            let mut curs = io::Cursor::new(&buf);
            match rmp_serde::from_read::<_, ProxyInstruction>(&mut curs) {
                 Ok(res) => {
                     // Check kind
                     if res.kind == InstructionKind::Return as u32 {
                         return Ok(res.data);
                     } else if res.kind == InstructionKind::Throw as u32 {
                         return Err(format!("Remote Error: {:?}", res.data));
                     } else {
                         return Err(format!("Unexpected response kind: {}", res.kind));
                     }
                 },
                 Err(_) => continue, // Need more data
            }
        }
        
        // If we broke loop with data in buf but failed decode?
        Err("Incomplete or invalid response".to_string())
    }
}
