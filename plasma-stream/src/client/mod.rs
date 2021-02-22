// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use crate::{
    errors::{ClientError, PeerResult},
    ObjectId, PeerRequest, Request,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpStream, ToSocketAddrs},
};

// CLIENT
// ================================================================================================

pub struct Client {
    socket: TcpStream,
}

impl Client {
    /// Connects to the Plasma Stream server at the specified address.
    pub async fn connect<T: ToSocketAddrs>(address: T) -> Result<Self, std::io::Error> {
        let socket = TcpStream::connect(address).await?;
        let client = Client { socket };
        Ok(client)
    }

    /// Retrieves objects with the specified IDs from the remote plasma store.
    pub fn copy(&self, _object_ids: &[ObjectId]) {
        // TODO: implement
        unimplemented!("not yet implemented");
    }

    /// Retrieves objects with the specified IDs from Plasma Stream server. The retrieved
    /// objects are deleted from the remote plasma store.
    pub fn take(&self, _object_ids: &[ObjectId]) {
        // TODO: implement
        unimplemented!("not yet implemented");
    }

    /// Instructs the Plasma Stream server to execute the specified requests.
    pub async fn sync(&mut self, requests: Vec<PeerRequest>) -> Result<(), ClientError> {
        let num_requests = requests.len();
        let request = Request::Sync(requests);
        request.validate().map_err(ClientError::MalformedRequest)?;

        // send the request
        request.write_into(&mut self.socket).await.map_err(|err| {
            ClientError::ConnectionError(String::from("failed to send a request"), err)
        })?;

        // read the response; there should be exactly one byte returned for every
        // peer request sent
        let mut response = vec![0u8; num_requests];
        self.socket.read_exact(&mut response).await.map_err(|err| {
            ClientError::ConnectionError(String::from("failed to get a response"), err)
        })?;

        // check if the response contains any errors
        parse_sync_response(&response)
    }
}

// HELPER FUNCTIONS
// ================================================================================================

fn parse_sync_response(response: &[u8]) -> Result<(), ClientError> {
    let mut results = Vec::with_capacity(response.len());
    let mut err_count = 0;
    for peer_response in response {
        let result = PeerResult::from(*peer_response);
        if !result.is_ok() {
            err_count += 1;
        }
        results.push(result);
    }

    if err_count > 0 {
        Err(ClientError::SyncError(results))
    } else {
        Ok(())
    }
}
