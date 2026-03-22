
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use rmpv::Value;
use crate::muid;

#[async_trait]
pub trait ProxyTarget: Send + Sync {
    async fn call(&self, name: &str, args: Vec<Value>) -> Result<Value, String>;
    async fn get(&self, name: &str) -> Result<Value, String>;
}

#[derive(Clone)]
pub struct Registry {
    // Stores the objects and their reference counts
    objects: Arc<Mutex<HashMap<String, (Arc<dyn ProxyTarget>, usize)>>>,
    // Reverse lookup to prevent duplicate exports of same object? 
    // In Rust, checking "same object" for dyn ProxyTarget is hard (fat pointers).
    // We will rely on explicit registration returning a new ID every time or user managing IDs.
    // For now, simple ID -> Object mapping.
}

impl Registry {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, target: Arc<dyn ProxyTarget>) -> String {
        let id = muid::make();
        self.register_with_id(id.clone(), target);
        id
    }
    
    pub fn register_with_id(&self, id: String, target: Arc<dyn ProxyTarget>) {
       let mut map = self.objects.lock().unwrap();
       map.insert(id, (target, 1));
    }
    
    // Increment reference count for an existing object (e.g. sending reference again)
    pub fn retain(&self, id: &str) -> bool {
        let mut map = self.objects.lock().unwrap();
        if let Some((_, count)) = map.get_mut(id) {
            *count += 1;
            true
        } else {
            false
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn ProxyTarget>> {
        let map = self.objects.lock().unwrap();
        map.get(id).map(|(obj, _)| obj.clone())
    }

    // Decrement ref count, remove if 0
    pub fn release(&self, id: &str) {
        let mut map = self.objects.lock().unwrap();
        if let Some((_, count)) = map.get_mut(id) {
            *count -= 1;
            if *count == 0 {
                map.remove(id);
            }
        }
    }
}
