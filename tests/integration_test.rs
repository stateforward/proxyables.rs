
use proxyables::proxyable::Proxyable;
use proxyables::registry::ProxyTarget;
use rmpv::Value;
use std::sync::Arc;
use tokio::io::duplex;
use async_trait::async_trait;

// 1. Define Target
struct MyTarget;

#[async_trait]
impl ProxyTarget for MyTarget {
    async fn call(&self, _name: &str, args: Vec<Value>) -> Result<Value, String> {
        // Expecting "add" logic for testing, but we dispatch by method name usually.
        // Since manual impl, we ignore name or check it.
        // For this test, let's assume we implement `add(a, b)`
        
        if args.len() == 2 {
            let a = args[0].as_i64().unwrap_or(0);
            let b = args[1].as_i64().unwrap_or(0);
            return Ok(Value::Integer((a + b).into()));
        }
        Err("Invalid args".into())
    }

    async fn get(&self, name: &str) -> Result<Value, String> {
        if name == "val" {
            Ok(Value::Integer(42.into()))
        } else {
            // Return "function" reference? 
            // For primitive Protocol test, let's just return a string "func_ref"
            Ok(Value::String("func_ref".into()))
        }
    }
}

use tokio_util::compat::TokioAsyncReadCompatExt;

// ...


use proxyables::proxy;

#[proxy]
#[allow(dead_code)]
trait MyApi {
   async fn add(&self, a: i64, b: i64) -> i64;
}

#[tokio::test]
async fn test_e2e_rust_port() {
    // ... setup ...
    let (client_stream, server_stream) = duplex(1024);
    let client_stream = client_stream.compat();
    let server_stream = server_stream.compat();

    let root = Arc::new(MyTarget);
    tokio::spawn(async move {
        Proxyable::export(server_stream, root).await.unwrap();
    });

    let (imported, driver) = Proxyable::import_from(client_stream);
    tokio::spawn(async move {
        driver.await.unwrap();
    });

    // MAGIC SUGAR
    let proxy = MyApiProxy::new(imported);
    
    // Call typed method
    let res = proxy.add(10, 20).await;
    match res {
        Ok(val) => assert_eq!(val.as_i64(), Some(30)),
        Err(e) => panic!("Call failed: {}", e),
    }
}
