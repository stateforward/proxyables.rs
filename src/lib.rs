pub mod yamux;
pub mod protocol;
pub mod muid;
pub mod proxyable;
pub use proxyables_macros::{proxyable, proxy};
pub mod registry;
pub mod exported;
pub mod imported;

pub use exported::ExportedProxyable;
pub use imported::ImportedProxyable;
pub use proxyable::Proxyable;
pub use registry::{ProxyTarget, Registry};
