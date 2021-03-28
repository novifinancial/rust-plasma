// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use plasma_store::PlasmaClient;
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::{self, Duration},
};
use tracing::{debug, error, info};

use super::{Handler, Result, ServerOptions, Store, PLASMA_CONNECT_RETRIES};

#[derive(Debug)]
pub struct Listener {
    /// TCP listener bound to the address provided during server startup. The server
    /// will listen for connections at this address.
    listener: TcpListener,

    /// Shared handle to the Plasma Store. Contains a reference to Plasma Store client
    /// as well as other info needed to ensure data is read from / written to the store
    /// in a consistent manner.
    store: Arc<Store>,

    /// Limit the max number of connections to the server. Before attempting to accept a
    /// new connection, a permit is acquired from the semaphore. If none are available,
    /// the listener waits for one. When handlers complete processing a connection, the
    /// permit is returned to the semaphore.
    limit_connections: Arc<Semaphore>,
}

impl Listener {
    pub async fn new(options: ServerOptions) -> Result<Listener> {
        // Bind a TCP listener
        let address = format!("127.0.0.1:{}", options.port);
        info!("starting server on {}", address);
        let listener = TcpListener::bind(&address).await?;

        // create a semaphore to enforce connection limit
        let limit_connections = Arc::new(Semaphore::new(options.max_connections as usize));

        // connect to the plasma store
        let plasma_socket = options.plasma_socket.as_str();
        let plasma_client = PlasmaClient::new(plasma_socket, PLASMA_CONNECT_RETRIES)?;
        info!("connected to plasma store at {}", options.plasma_socket);

        // create an object store
        let plasma_timeout_ms = options.plasma_timeout;
        let store = Arc::new(Store::new(plasma_client, plasma_timeout_ms));

        Ok(Listener {
            listener,
            store,
            limit_connections,
        })
    }

    /// Start listening for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// Returns `Err` if accepting returns an error.
    pub async fn start(&mut self) -> Result<()> {
        info!("accepting inbound connections");

        loop {
            // Wait for a permit to become available
            //
            // `acquire()` returns `Err` when the semaphore has been closed. We don't ever
            // close the semaphore, so `unwrap()` is safe.
            self.limit_connections.acquire().await.unwrap().forget();

            // Accept a new socket. This will attempt to perform error handling. The `accept`
            // method internally attempts to recover errors, so an error here is non-recoverable.
            let socket = self.accept().await?;
            debug!("accepted connection from {}", socket.peer_addr().unwrap());

            // Create the necessary per-connection handler state. The handler needs a handle to
            // the max connections semaphore. When the handler is done processing the connection,
            // a permit is added back to the semaphore.
            let mut handler =
                Handler::new(socket, self.store.clone(), self.limit_connections.clone());

            // Spawn a new task to process the connections
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run().await {
                    error!("{}", err);
                }
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An incremental backoff strategy is used.
    /// After the first failure, the task waits for 1 second. After the second failure, the task
    /// waits for 2 seconds. Each subsequent failure increases the wait time by 1 second. If
    /// accepting fails on the 5th try after waiting for 4 seconds, an error is returned.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        loop {
            // Perform the accept operation. If a socket is successfully accepted, return it.
            // Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    // If accept has failed too many times. Return the error.
                    debug!("failed to accept connection: {}", err);
                    if backoff > 4 {
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Increment the back off
            backoff += 1;
        }
    }
}
