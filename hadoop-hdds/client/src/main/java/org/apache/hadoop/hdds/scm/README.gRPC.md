<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# The gRPC client internals
This document is about the current gRPC client internals. It aims to give enough
context about gRPC and the Ratis client with which it shares its interface to
understand the current behaviour, and implementation better.
Besides this, at the end one can find information about possibilities that gRPC
can provide, and why we are not using it at the moment.


## Responsibilities of client code using the client
There are two types of requests in the client, sync and async. These requests
are considered to be synced from the caller's point of view, and they just block
the thread in which the caller sends the request.
If the client code is using the same client from multiple threads, the requests
are sent in via multiple message streams on the same channel.

### When client code uses synced calls
There are no additional responsibilities on the client code about single
requests, the requests will be sent and the thread is blocked until the response
arrives.
However, if there are multiple dependent calls that are sent from multiple 
threads, the client code has to externally synchronize the dependent calls in
between its threads.

### When client code uses async calls
There are two async calls `WriteChunk` and `PutBlock` that can be sent via
the `ContainerProtocolCalls` though nothing prevents a client code to send 
other type of commands in an asynchronous fashion by calling the gRPC client's
sendCommandAsync directly.
When using async calls, the client code has to ensure ordering between requests
if they need to be ordered. 
For example `PutBlock` requires all related `WriteChunk` to finish successfully
before it is called, in order to have consistent data and metadata stored on the
DataNode in all the cases. The gRPC client itself does not do any measures to
guarantee such an ordering. More details about the reasons will be shared later
in this doc. In a nutshell, it brings in performance and complexity penalties 
with no real gains.

Retries are not handled nor provided for async calls, for these calls, the
client code has to implement the retry logic on its own, as that may involve
re-sending multiple requests, and may affect request ordering as well.


## How clients can be created
The `XCeiverClientManager` class is managing creation of the client instances,
as defined in the `XCeiverClientFactory` interface.
A user of this class can acquire a client with the help of acquire methods, and
then release it when it is not using it anymore.

It is important to note that the lifecycle of the clients are up to the user,
and if a client is not released it may leak, as the clients are reference
counted and evicted from the cache if they are not references for more than
`scm.container.client.idle.threshold` milliseconds ago. The default idle
threshold is 10 seconds.

The client is getting connected to the DataNode upon creation, and at every
request it checks if the connection is still up and ready. In case the
connection was terminated for any reason, the client simply tries to reconnect
to the DataNode.


## Client to server communication in a nutshell
In gRPC there are 3 things that is part of the communication, stub, channel, and
messages.

Stub provides tha server side API on the client side.
Channel manages the communication properties, and the socket level connection.
Messages in our case are sent via bi-directional streaming with the help of
StreamObservers.

When we initiate a connection we create a stub, and a channel, and when we send
a request (a message) we create the bi-directional stream, we send in a 
request proto message, and we get back a response proto message, then we close
the stream.
We never send multiple requests via the same stream, although we can if we want
to, there are reasons that will be discussed with more details later on.

The server side of communication processes the requests in the same thread in
which they arrive, via `HDDSDispatcher` in `GrpcXceiverService`, and this also
means that the requests are processed in more threads as the gRPC server side
provides multi-threaded processing per streams.


## Client to server communication in more detail

### How requests are handled on client and server side
The gRPC client, also known as the standalone client, is used only for
reads in the current environment, but during the Erasure Coded storage
development we already selected the standalone client for writes. That effort
initiated the analysis of the client side, and this writeup.

We have two type of requests, sync and async requests from the client point of
view.
There are only two type of requests the client may -by design - send in an async
way, `PutBlock` and `WriteChunk`.
All the other requests are sent in a synced way from client side, and the client
is blocked until the response arrives from the server side. (The implementation
of the requests in `ContainerProtocolCalls` ensures this.)

The one request per message communication strategy ensures that even though
clients are loaded from a cache, and multiple clients or multiple threads can
use the same stub and channel to talk to one server, all the requests sent from
one client to the same server are running in parallel on the server side.

### Client side caching of gRPC clients
In Ozone there is the `XCeiverClientManager` class, which has an internal cache
for the actual clients that are connecting to DataNodes. The cache has a key
structure that consist of the `PipeLineID`, the Pipeline's `ReplicationType`,
and for reads in topology aware configurations the hostname of the closest host
in the PipeLine.

For Erasure Coded writes, the cache key also contains the hostname of the node,
and the server port as well, for testing it was necessary to distinguish between
different DataNodes running locally, but in a real environment, this will not
affect the number of clients cached as Pipelines used are having different ids
per host:port, but the same id for the same host:port during a write.


## The problem with out-of-order `WriteChunk` requests
Let's say we process `PutBlock` first, and then for the block we process a
`WriteChunk` after `PutBlock` has finished. In this case `PutBlock` will persist
metadata of the block for example length of the block. Now if an out-of-order
`WriteChunk` fails, there can be inconsistencies between the metadata and the
block data itself, as `PutBlock` happened before all data was written.

In order to avoid this problem, the initial implementation ensures that even the
`WriteChunk` and `PutBlock` requests are sent in synchronized from the client
side, by synchronizing these calls as well internally. Until Erasure Coded 
storage, it was not a priority to deal with this, as all write operations were
redirected to the Ratis client prior to that.

Another edge case is when there is a successful out-of-order `WriteChunk`, but
the recorded length of the block in the metadata is less and does not account
for the out-of-order `WriteChunk`. In this case there won't be a problem seen
from the DataNode side, as the metadata is consistent, but the additional data
will not be visible for readers, and will be treated as garbage at the end of
the block file. From the client's point of view though data will be missing and
the block file won't be complete.


## Experiment with really async calls
In order to avoid the ordering problem of `WriteChunk` and `PutBlock` requests,
it seems to be tempting to rely on gRPC's guarantees about the processing order
of messages sent via the same StreamObserver pair, so experiments with this
has happened as part of HDDS-5954.

### Promising first results
During an experiment, by creating a simple server side, that does nothing
but sends back a default response, and a simple client side, that sends in
simple default requests, there was a comparison running.
The comparison was between the way of creating StreamObserver pair per request,
and re-using the same StreamObserver pair to send in the same amount of
requests.

The measures showed that there is a significant time difference between the two
ways of sending 10k messages via gRPC, and re-using the StreamObserver pair is
two orders of magnitude faster. It is also that much faster if one uses multiple 
StreamObserver pairs from multiple threads, and in this case, the ordering
guarantees were held on a per-thread basis.

### The reality a.k.a. facepalm
After going further and creating a mock server side and write a test against it
using the real client implementation, reality kicks in hard.
The server side implementation was prepared to inject customizable delays on a
per-request-type basis, and suddenly the gain with reusing the same 
StreamObserver pair became a bottleneck in a multi-threaded environment, and
an insignificant gain in a single threaded environment.

Here is what happens when requests are sent from multiple threads: the 
StreamObserver ensures ordering of requests, so the requests become synced
and single threaded within the same pair of StreamObservers, even though they
were sent by multiple threads. Not to mention the necessity of locking around
request sending, as if multiple threads write to the same stream messages can
get overlapped. This means that via the same StreamObserver pairs, messages can
be sent, and responses can arrive in order, but the processing time of all
requests is an accumulation of the processing time of the single requests.

### Conclusion from the experiment
Even though we may re-use StreamObservers, we loose the ability to send data
in parallel via the client, and we do not gain with re-use. Not to mention the
already synced requests which when arrive from different threads, now are being
processed in that thread without waiting for other requests, but would be
waiting for each other if we start to use the same StreamObserver pairs for 
them.


## So how to solve the ordering problem?
On the gRPC client level this problem can not be solved, as that would be way
more complex and complicated to handle different requests via different
StreamObserver pairs, and caching the StreamObservers could become a nightmare.
Also, even if let's say we cache the StreamObservers, and we use one message
stream to do `WriteChunks` then the `PutBlock` call during writing a block, we
would lose the possibly async nature of these calls by the synchronization
inside gRPC that ensures the ordering. Oh, by the way, that synchronization
comes from the fact that the server side processes the request on the same 
thread where it has arrived, but if we move that to a background thread, and
send the response when it is ready, we loose the ordering guarantee of gRPC...

So, all in all, the solution that hurt us the least is to ensure that,
all the client code that uses the gRPC client is aware the need and responsible
for ordering and ensures that all the `WriteChunk` requests are finished
successfully before issuing a `PutBlock` request.


## A possible solution
During experiments, there was a solution that started to show its shape slowly,
and got frightening at the end. Here is what we could end up with.

We have one StreamObserver pair, or a pool of a few pairs, which we can use to 
send sync requests over, as sync requests are mostly coming from the main client
thread, one pair of StreamObservers might be enough, for parallel use we might
supply a pool, noting the fact that `onNext(request)` method have to be guarded
by a lock, as multiple messages can not be sent properly on the same channel.

We have a separate way of posting async requests, where we create a 
StreamObserver pair for every request, and complete it upon sending the request.
In this case the client has to return an empty `CompleteableFuture` object, to
which later on it has to set the response for the request.

This sounds easy, but when it comes to implementation it turns out that it is
not that simple. Either we have request IDs, and the server side response also
carries the request ID, so we can pair up the response with the request, or we
use some other technics (like sliding window) to ensure the responses are sent
back in the order of requests even though they were processed asynchronously, or
even both technics together.

### Why we just do not engage in this
This whole question came up as part of Erasure Coded storage development, and
in the EC write path, we have to synchronize at multiple points of a write.
Once a stripe from an EC data block group is written, we need to ensure that all
the data and parity stripes were written successfully, before we do a `PutBlock`
operation after the stripe write, this is to avoid a costly and enormously
complex recovery after write algorithm.
So that EC writes does not really need anything to be implemented at the moment
as the external synchronization of the async calls are already coded into the
logic behind `ECKeyOutputStream`.