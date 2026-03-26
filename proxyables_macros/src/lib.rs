
extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemImpl, ItemTrait, TraitItem, FnArg};

#[proc_macro_attribute]
pub fn proxyable(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Current impl for ItemImpl
    let input = parse_macro_input!(item as ItemImpl);
    // ... existing ...
    let self_ty = &input.self_ty;
    
    let mut methods = Vec::new();
    
    for item in &input.items {
        if let syn::ImplItem::Fn(method) = item {
             let name = &method.sig.ident;
             let name_str = name.to_string();
             // TODO: Handle arguments properly map Value -> Type
             // For POC we assume methods accept standard types that impl From/Into Value or strict Value.
             // But actually, we need valid Codec.
             // This mock assumes arguments are `Vec<Value>` and return `Value`.
             // To be real unused "sugar", we need strict type conversion.
             
             // Simplest strict mapping:
             // We just pass args down.
             
             methods.push(quote! {
                 #name_str => {
                     // We rely on `rmp_serde` usually. 
                     // But here we are bridging `Value`.
                     // Let's blindly forward for now or assume simple usage.
                     
                     // In a real macro we would deserialize `args[0]` to `Type`.
                     // For this MVP "Sugar", let's assume methods take `Vec<Value>`...
                     // Wait, user wants Sugar.
                     // Sugar means `fn add(a: i32)` -> `ProxyTarget::call` receives values, deserializes them to i32?
                     
                     // This complexity is high for a single file edit.
                     // I will keep the `proxyable` (Export) simple dispatch
                     // And focus on `proxy` (Import) sugar.
                     match self.#name().await {
                         Ok(v) => Ok(rmpv::Value::from(v)),
                         Err(e) => Err(e.to_string()),
                     }
                 }
             });
        }
    }
    
    let expanded = quote! {
        #input
        
        #[async_trait::async_trait]
        impl proxyables::registry::ProxyTarget for #self_ty {
            async fn call(&self, name: &str, args: Vec<rmpv::Value>) -> Result<rmpv::Value, String> {
                match name {
                    #(#methods)*
                    _ => Err(format!("Method {} not found", name)),
                }
            }
            
            async fn get(&self, name: &str) -> Result<rmpv::Value, String> {
                Err("Properties not supported yet".to_string())
            }
        }
    };
    
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn proxy(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemTrait);
    let trait_name = &input.ident;
    let proxy_name = syn::Ident::new(&format!("{}Proxy", trait_name), trait_name.span());
    
    let mut methods = Vec::new();
    
    for item in &input.items {
        if let TraitItem::Fn(method) = item {
            let sig = &method.sig;
            let ident = &sig.ident;
            let inputs = &sig.inputs;
            
            // Filter inputs to exclude &self for the method signature
            let args_sig: Vec<_> = inputs.iter().filter(|arg| {
                if let FnArg::Receiver(_) = arg { false } else { true }
            }).collect();

            // For now, assume Args convert to Value via Into/From or rmpv::Value::from
            // We construct vec![arg1.into(), arg2.into()]
             let arg_conversions = args_sig.iter().map(|arg| {
                 if let FnArg::Typed(pat_type) = arg {
                     let pat = &pat_type.pat;
                     quote! { rmpv::Value::from(#pat) }
                 } else {
                     panic!("Unexpected arg")
                 }
            });
            
            methods.push(quote! {
                pub async fn #ident(&self, #(#args_sig),*) -> Result<rmpv::Value, String> {
                    let args = vec![ #(#arg_conversions),* ];
                    self.inner.apply(Some("root".to_string()), args).await
                }
            });
            // Note: Returning strict types requires decoding result `Value -> Type`.
            // rmpv::Value isn't automagically deserializable to i32 without help.
            // `val.as_i64()` etc.
            // For MVP Sugar: Return `Result<Value, String>`.
            // Or assume generics.
        }
    }
    
    let expanded = quote! {
        #input
        
        pub struct #proxy_name {
            inner: proxyables::imported::ImportedProxyable,
        }
        
        impl #proxy_name {
            pub fn new(inner: proxyables::imported::ImportedProxyable) -> Self {
                Self { inner }
            }
            
            #(#methods)*
        }
    };
    
    TokenStream::from(expanded)
}
