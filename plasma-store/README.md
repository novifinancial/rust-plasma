# Arrow Plasma bindings
This crate contains Rust bindings for Arrow Plasma, an in-memory object store which enables efficient memory sharing across processes on the same machine.

Arrow Plasma resources:
* [Blog post](https://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/)
* [C++ tutorial](https://github.com/apache/arrow/blob/master/cpp/apidoc/tutorials/plasma.md)
* [Github project](https://github.com/apache/arrow/)

## Plasma server
Plasma object store runs in a separate process and before clients can make API requests to it, the process can be started like so:
```
./plasma-store-server -m 1000000000 -s /tmp/plasma
```
where:
* The `-m` flag specifies the size of the object store in bytes.
* The `-s` flag specifies the path of the Unix domain socket that the store will listen at.

To build plasma server, Arrow project can be compiled using the following script:
```
git clone https://github.com/apache/arrow.git
cd arrow/cpp
mkdir release
cd release
cmake .. -DARROW_PLASMA=ON
make
```
Once this is done, `plasma-store-server` executable will be in `arrow/cpp/release/release` directory. For general instructions on building Apace Arrow see [here](https://arrow.apache.org/docs/developers/cpp/building.html).

### Huge page support
On Linux it is possible to use the Plasma store with huge pages for increased throughput. To do this, we first need to create a file system and activate huge pages like so:
```
sudo mkdir -p /mnt/hugepages
gid=`id -g`
uid=`id -u`
sudo mount -t hugetlbfs -o uid=$uid -o gid=$gid none /mnt/hugepages
sudo bash -c "echo $gid > /proc/sys/vm/hugetlb_shm_group"
sudo bash -c "echo 20000 > /proc/sys/vm/nr_hugepages"
```
Once this is done, you can start the Plasma store with the `-d` flag for the mount point of the huge page file system and the `-h` flag which indicates that huge pages are activated:
```
./plasma-store-server -m 1000000000 -s /tmp/plasma -d /mnt/hugepages -h
```

## Plasma client API
Plasma client Rust API provides three primary structs to interact with Plasma store:

* `PlasmaClient` which can be used to retrieve object from the store and create new objects in the store.
* `ObjectId` which uniquely identifies an object in the store.
* `ObjectBuffer` which contains data and metadata for a single object in the store.

An example below is a simple program which illustrates how to create an object in the store, and then retrieve it from the store:
```Rust
use plasma::{ObjectId, PlasmaClient};

fn main() {
    // connect to the Plasma store
    let pc = PlasmaClient::new("/tmp/plasma", 0).unwrap();

    // crate an object in the store and allocate 1GB of memory for it
    let oid = ObjectId::rand();
    let data_size = 1024 * 1024 * 1024;
    let meta = [1, 2, 3, 4];
    let mut ob = pc.create(oid.clone(), data_size, &meta).unwrap();

    // write some data into the object; we'll write the data as a sequence of u128's
    let buf_mut = unsafe {
        let (_, middle, _) = ob.data_mut().align_to_mut::<u128>();
        middle
    };
    for i in 0..buf_mut.len() {
        buf_mut[i] = i as u128;
    }
    // once the data is written, seal the object to make it available to other clients
    ob.seal().unwrap();

    // connect to the Plasma store as another client
    let pc2 = PlasmaClient::new("/tmp/plasma", 0).unwrap();

    // get the object from the store and re-interpret it as a list of u128's
    let ob2 = pc2.get(oid.clone(), 5).unwrap().unwrap();
    let buf2 = unsafe {
        let (_, middle, _) = ob2.data().align_to::<u128>();
        middle
    };

    // sum up all the values and output the result
    let mut total = 0;
    for i in 0..buf2.len() {
        total += buf2[i];
    }
    println!("Result: {}", total);
}
```

### PlasmaClient
`PlasmaClient` provides an interface to interact with the local PlasmaStore. It can be crated using `PlasmaClient::new()` function which takes the following parameters:
* `store_socket_name` The name of the UNIX domain socket to use to connect to the Plasma store.
* `num_retries` number of attempts to connect to IPC socket, default 50.

When a client is crated, it is automatically connected to the store. It also automatically disconnects from the store when the client struct is deallocated.

Plasma client exposes a number of useful methods to interact with the store, the most important ones of which are:

* `get(oid: ObjectId, timeout_ms: i64)` - retrieves an object with the specified ID from the store. This function will block until the object has been created and sealed in the Plasma store or the timeout expires.
* `create(oid: ObjectId, data_size: usize, meta: &[u8])` - Creates an object in the Plasma Store. Any metadata for this object must be passed in when the object is created. `data_size` specifies the size of the object's data buffer in bytes. The returned object must be either sealed or aborted when done with.
* `create_and_seal(oid: ObjectId, data: &[u8], meta: &[u8])` - creates and seals an object in the object store. This is an optimization which allows small objects to be created quickly with fewer messages to the store.
* `delete(oid: &ObjectId)` - deletes an object from the object store. This currently assumes that the object is present, has been sealed and not used by another client. Otherwise, it is a no operation.
* `contains(oid: &ObjectId)` - checks if the object store contains a particular object and the object has been sealed.


### ObjectId
Object IDs are unique identifiers for objects in a Plasma store. Each object ID are 20 bytes long and can be crated as follows:

* `ObjectId::rand()` will create a random object ID;
* `ObjectId::new(bytes: [u8; 20])` will create a new object ID from a sequence of 20 bytes.

### ObjectBuffer
`ObjectBuffer` struct is a representation of a single object in Plasma store. As described above, object buffers can be retrieved from the store using `get()` function, and created using `create()` functions.

An object buffer exposes a number of useful methods and properties, most important of which are:

* `data() -> &[u8]` - returns read-only data buffer of this object buffer.
* `data_mut() - &mut [u8]` - returns mutable data buffer of this object buffer. Mutable buffers can be obtained only for objects which have been created but not yet sealed.
* `meta() -> &[u8]` - returns metadata buffer of the object buffer.
* `seal()` - Seals a created object in the object store. The object will be immutable after this call.
* `abort()` - aborts an unsealed object in the object store. If the abort succeeds, then it will be as if the object was never created at all.

Unlike in C++ implementation, there is no need to manually release retrieved or created object buffers. They are released automatically when references to them go out of scope.

License
-------

This project is [MIT licensed](./LICENSE).