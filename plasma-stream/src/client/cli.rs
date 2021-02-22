// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use plasma_stream::{Client, ObjectId, PeerRequest};
use std::{convert::TryInto, io::prelude::*, net::SocketAddr, time::Instant};
use structopt::StructOpt;

// COMMAND LINE ARGUMENTS
// ================================================================================================

#[derive(StructOpt, Debug)]
#[structopt(name = "plasma stream cli", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A simple CLI client for Plasma Stream server")]
pub struct ClientOptions {
    /// Address of the Plasma Stream server
    #[structopt(short, long)]
    address: String,
}

// PROGRAM ENTRY POINT
// ================================================================================================

#[tokio::main]
pub async fn main() -> plasma_stream::Result<()> {
    // read command-line args
    let options = ClientOptions::from_args();
    let address = options.address;

    // connect to the server
    let mut client = Client::connect(address.clone()).await?;
    println!("connected to {}", address);

    // read line from command line, convert it to a SYNC request, and execute it
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        match parse_request(line.unwrap()) {
            Ok(requests) => {
                let now = Instant::now();
                match client.sync(requests).await {
                    Ok(_) => println!("> completed in {} ms", now.elapsed().as_millis()),
                    Err(err) => println!("> {}", err),
                }
            }
            Err(err) => println!("> {}", err),
        }
    }

    Ok(())
}

// PARSER
// ================================================================================================
fn parse_request(line: String) -> Result<Vec<PeerRequest>, String> {
    let tokens: Vec<&str> = line.split(' ').collect();

    if tokens.len() < 3 {
        return Err(String::from(
            "invalid request; must be [COPY|TAKE] [server address] [object ID list]",
        ));
    }

    let req_type = tokens[0].to_string();
    let address: SocketAddr = tokens[1]
        .to_string()
        .parse()
        .map_err(|err| format!("server address {} is invalid: {}", tokens[1], err))?;

    let mut object_ids = Vec::with_capacity(tokens.len() - 2);
    for token in tokens.into_iter().skip(2) {
        let id: ObjectId = hex::decode(token)
            .map_err(|err| format!("object ID '{}' is invalid: {}", token, err))?
            .try_into()
            .map_err(|_| format!("object ID '{}' is invalid: must be 20 bytes long", token))?;
        object_ids.push(id);
    }

    let peer_req = match req_type.as_str() {
        "copy" | "COPY" => PeerRequest::Copy {
            from: address,
            objects: object_ids,
        },
        "take" | "TAKE" => PeerRequest::Take {
            from: address,
            objects: object_ids,
        },
        _ => return Err(String::from("requests must start with either COPY or TAKE")),
    };

    Ok(vec![peer_req])
}
