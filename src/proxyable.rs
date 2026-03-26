use futures::io::{AsyncRead, AsyncWrite};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{select, FutureExt};
use std::sync::Arc;
// use std::pin::Pin;
use std::future::Future;

use crate::exported::ExportedProxyable;
use crate::imported::ImportedProxyable;
use crate::registry::{ProxyTarget, Registry};
use crate::yamux::session::Session;

pub struct Proxyable;

impl Proxyable {
    pub fn Export<S>(
        stream: S,
        target: Arc<dyn ProxyTarget>,
    ) -> impl Future<Output = std::io::Result<()>> + Send + 'static
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let registry = Registry::new();
        registry.register_with_id("root".to_string(), target);

        let (_session, driver, accept_rx) = Session::new(stream, false);
        let mut exported = ExportedProxyable::new(accept_rx, registry);

        // Combine everything into one future
        async move {
            let mut driver_fut = Box::pin(driver.run()).fuse();
            let mut handlers = FuturesUnordered::new();

            // We need to continuously:
            // 1. Run driver
            // 2. Check for new connections from exported.next_connection()
            // 3. Drive handlers

            // Note: `exported.next_connection()` is async. It waits for `accept_rx`.
            // `driver` feeds `accept_rx`.

            loop {
                let mut next_conn = Box::pin(exported.next_connection()).fuse();

                select! {
                     res = driver_fut => {
                         // Driver finished (error or EOF session closed)
                         return res;
                     },
                     conn_opt = next_conn => {
                         if let Some(conn) = conn_opt {
                             handlers.push(conn);
                         } else {
                             // Accepted closed?
                             break;
                         }
                     },
                     _ = handlers.next() => {
                         // A handler finished.
                     }
                }
            }
            Ok(())
        }
    }

    pub fn ImportFrom<S>(
        stream: S,
    ) -> (
        ImportedProxyable,
        impl Future<Output = std::io::Result<()>> + Send + 'static,
    )
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (session, driver, _) = Session::new(stream, true);
        let proxy = ImportedProxyable::new(session);

        (proxy, driver.run())
    }
}
