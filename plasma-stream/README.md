# Plasma Stream
This crate contains components of Plasma Stream system. Plasma stream can be used to efficiently move data between [Plasma Stores](../plasma) on different machines.

The components are:
* Plasma stream server - an executable which runs on a machine in parallel with Plasma Store.
* Plasma stream client - a library which can be imported into other applications to make requests to a remote Plasma Stream server.
* Plasma stream CLI - an executable which can be used to connect to and make requests against a Plasma Stream server.

### Plasma Stream server
Plasma Stream server can be started using `plasma-stream-server` executable. Executing `./plasma-stream-server -h` will display the list of available parameters and their meaning like so:

```
Plasma Stream server

USAGE:
    plasma-stream-server [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --max-connections <max-connections>    Maximum number of TCP connections accepted by this server [default: 128]
    -s, --plasma-socket <plasma-socket>        Unix socket bound to the local Plasma Store [default: /tmp/plasma]
    -t, --plasma-timeout <plasma-timeout>      The amount of time in milliseconds to wait before requests to Plasma
                                               Store time out [default: 10]
    -p, --port <port>                          TCP port for the porter to listen on [default: 2021]
```

Before starting a Plasma Stream server, you should start a Plasma Store server on same machine. Otherwise, Plasma Stream server will fail to start.

### Plasma Stream client
A Plasma Stream client can be used to programmatically interact with a Plasma Stream server. For example:
```Rust
use std::convert::TryInto;
use stream::{Client, PeerRequest};

#[tokio::main]
pub async fn main() -> stream::Result<()> {
    
    // connect to a Plasma Stream server running on the local machine
    let mut client = Client::connect("127.0.0.1:2021").await?;

    // create an object ID; object IDs must be 20 bytes
    let oid = hex::decode("0102030405060708090a0b0c0d0e0f1011121314").unwrap();

    // build a set of peer requests which we want Plasma Stream server to execute
    // against other Plasma Stream servers; in this specific instance we want to
    // ask our server to copy an object from the server located at 127.0.0.1:2022
    let peer_requests = vec![
        PeerRequest::Copy {
            from: "127.0.0.1:2022".parse().unwrap(),
            objects: vec![oid.try_into().unwrap()]
        },
    ];

    // send the request to the Plasma Stream server using SYNC command
    client.sync(peer_requests).await?;
    Ok(())
}
```

API of Plasma Stream client is very simple. To connect a client to a server you can use `Client::connect()` function as shown in the example above.

To make requests against the server, you can use specialized methods of `Client` struct. Currently, the only implemented method is `sync()` which corresponds to a `SYNC` command. In the future, support for other protocol commands will be added.

### Plasma Stream CLI
Plasma stream CLI can be started using `plasma-stream-cli` executable. Executing `./plasma-stream-cli -h` will display instructions on how to start it:
```
A simple CLI client for Plasma Stream server

USAGE:
    plasma-stream-cli --address <address>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -a, --address <address>    Address of the Plasma Stream server
```

Once CLI starts, you can send `SYNC` requests to the connected Plasma Stream server. `SYNC` requests instruct the Plasma Stream server to retrieve Plasma object buffers from remote machines. Currently, CLI accepts only a single peer request at a time. For example:

```
COPY 127.0.0.1:2022 0102030405060708090a0b0c0d0e0f1011121314
```
Instructs the Plasma Stream server to connect to a Plasma Stream server at `127.0.0.1:2022` and copy object buffer with ID `0102030405060708090a0b0c0d0e0f1011121314` from it.

## Plasma stream protocol
Plasma Stream protocol describes a small number of requests which Plasma Stream servers can make to each other. These requests are described below.

### COPY
A `COPY` request can be used to retrieve a set of Plasma object buffers from a given server. The request has the following form:
```
COPY oid1 oid2 ...
```
Where `oid1`, `oid2` etc. are the 20-byte IDs of the requested objects. A valid request must meet the following limits:

* At lest one object must be requested;
* At most 65,536 objects can be requested;
* No object should have data larger than 16 TB;
* No object should have metadata larger than 64 KB;
* All object IDs in the list must be unique;

### TAKE
A `TAKE` request is similar to a `COPY` request, except the requested objects are deleted from the source server after they are transferred to the requesting server. All the limits listed for the `COPY` request apply here as well.

Note: there is actually no guarantee that the objects will be deleted. A "best-effort" attempt will be made to delete the objects, but if the deleting fails for any reason, the object may remain in the source Plasma Store.

### SYNC
A `SYNC` request can be used to instruct a Plasma Stream server to retrieve data from other Plasma Stream servers. The request has the following form:
```
SYNC
[COPY|TAKE] peer_address1 oid1 oid2 ...
[COPY|TAKE] peer_address2 oid3 oid4 ...
...
```
Where `peer_address1`, `peer_address2` etc. are the addresses of peer Plasma Stream servers from which the data should be retrieved. A valid `SYNC` request must meet the following limits:

* It must contain at least one peer request;
* It can contain at most 1024 peer requests;
* Each peer requests must request at least one object;
* No peer requests should request more than 65,536 objects;
* No object should have data larger than 16 TB;
* No object should have metadata larger than 64 KB;
* All object IDs, across all peer requests must be unique;