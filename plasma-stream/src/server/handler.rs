// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use super::{Dispatcher, Request, Store};
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Semaphore};
use tracing::debug;

// CONNECTION HANDLER
// ================================================================================================

/// Per-connection handler
#[derive(Debug)]
pub struct Handler {
    /// TCP connection for this handler.
    socket: TcpStream,
    /// Shared handle to the Plasma Store.
    store: Arc<Store>,
    /// Limit the max number of connections to the server.
    limit_connections: Arc<Semaphore>,
}

impl Handler {
    pub fn new(socket: TcpStream, store: Arc<Store>, limit_connections: Arc<Semaphore>) -> Self {
        Handler {
            socket,
            store,
            limit_connections,
        }
    }

    /// Process a single connection.
    ///
    /// Requests are read from the socket and processed until there are no requests left.
    pub async fn run(&mut self) -> crate::Result<()> {
        // read requests until no more requests are available
        loop {
            // If no request was read then the peer closed the socket. There is no further work
            // to do and the task can be terminated.
            let request = match Request::read_from(&mut self.socket).await? {
                Some(request) => request,
                None => return Ok(()),
            };
            let peer_addr = self.socket.peer_addr()?;
            debug!("Received request from {}\n{}", peer_addr, request);

            // make sure the received request is valid
            request.validate()?;

            // process the request
            match request {
                Request::Copy(object_ids) => {
                    // for COPY request, just send the objects to the requesting peer
                    self.store
                        .build_sender(peer_addr, object_ids, false)
                        .run(&mut self.socket)
                        .await?;
                }
                Request::Take(object_ids) => {
                    // for TAKE request, send the objects, but also delete them afterwards
                    self.store
                        .build_sender(peer_addr, object_ids, true)
                        .run(&mut self.socket)
                        .await?;
                }
                Request::Sync(requests) => {
                    // for SYNC request, use use a dispatcher to process peer requests
                    let dispatcher = Dispatcher {
                        store: self.store.clone(),
                    };
                    dispatcher.run(requests, &mut self.socket).await?;
                }
            };
        }
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Add a permit back to the semaphore. Doing so unblocks the listener if the max
        // number of connections has been reached.
        self.limit_connections.add_permits(1);
        debug!("closed connection to {}", self.socket.peer_addr().unwrap());
    }
}
