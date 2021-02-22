// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use crate::{errors::SyncError, status_codes, PeerRequest, Request, Store};
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tracing::error;

pub struct Dispatcher {
    /// Shared handle to the Plasma Store.
    pub store: Arc<Store>,
}

// SYNC REQUEST DISPATCHER
// ================================================================================================

impl Dispatcher {
    /// Dispatches requests to the peer Plasma Stream nodes, collects the replies, and writes
    /// the result of peer requests into `client_socket`. Each peer requests may move one or more
    /// objects between plasma stores on local or and peer machines. Currently, the only two
    /// possible peer request: COPY and TAKE. Both of them transfer objects from a peer to the
    /// local plasma store.
    pub async fn run(
        &self,
        requests: Vec<PeerRequest>,
        client_socket: &mut TcpStream,
    ) -> Result<(), SyncError> {
        // make sure none of the peer requests is for the local address
        let local_address = client_socket
            .local_addr()
            .map_err(SyncError::ClientConnectionError)?;
        for request in requests.iter() {
            if request.contains_peer(&local_address) {
                return Err(SyncError::PeerAddressIsSelf);
            }
        }

        // use separate task to fullfil each peer request; this is done to enable parallel
        // streaming of objects from multiple peers
        let mut handles = Vec::new();
        for request in requests.into_iter() {
            let store = self.store.clone();
            let handle = tokio::spawn(async move { process_peer_request(store, request).await });
            handles.push(handle);
        }

        // wait for all requests to finish and collect the results into a response; if there
        // were errors, log them, but don't propagate them forward.
        let mut response = vec![status_codes::SUCCESS; handles.len()];
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(result) => {
                    if let Err(err) = result {
                        error!("{}", err);
                        response[i] = err.response_code();
                    }
                }
                Err(err) => {
                    error!("peer request {} panicked: {}", i, err);
                    response[i] = status_codes::PEER_REQUEST_PANICKED;
                }
            }
        }

        // write the response into client socket, and if there is an error propagate it forward
        client_socket
            .write_all(&response)
            .await
            .map_err(SyncError::ClientConnectionError)
    }
}

// HELPER FUNCTIONS
// ================================================================================================

async fn process_peer_request(store: Arc<Store>, request: PeerRequest) -> Result<(), SyncError> {
    match request {
        PeerRequest::Copy { from, objects } => {
            // build the receiver and prepare it to receive objects
            let receiver = store.build_receiver(from, objects.clone());
            receiver.prepare().map_err(SyncError::ReceiverError)?;

            // open the socket and send COPY request
            let mut socket = TcpStream::connect(from)
                .await
                .map_err(|err| SyncError::PeerConnectionFailed(from, err))?;
            let request = Request::Copy(objects);
            request
                .write_into(&mut socket)
                .await
                .map_err(|err| SyncError::PeerRequestNotSent(from, err))?;

            // read the response and close connection when done
            receiver
                .run(&mut socket)
                .await
                .map_err(SyncError::ReceiverError)?;
            socket.shutdown().await.or_else(|err| {
                error!("connection to {} did not shut down cleanly: {}", from, err);
                Ok(())
            })?;
        }
        PeerRequest::Take { from, objects } => {
            // build the receiver and prepare it to receive objects
            let receiver = store.build_receiver(from, objects.clone());
            receiver.prepare().map_err(SyncError::ReceiverError)?;

            // open the socket and send TAKE request
            let mut socket = TcpStream::connect(from)
                .await
                .map_err(|err| SyncError::PeerConnectionFailed(from, err))?;
            let request = Request::Take(objects);
            request
                .write_into(&mut socket)
                .await
                .map_err(|err| SyncError::PeerRequestNotSent(from, err))?;

            // read the response and close connection when done
            receiver
                .run(&mut socket)
                .await
                .map_err(SyncError::ReceiverError)?;
            socket.shutdown().await.or_else(|err| {
                error!("connection to {} did not shut down cleanly: {}", from, err);
                Ok(())
            })?;
        }
    }
    Ok(())
}
