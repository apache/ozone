---
title: "Java API"
date: "2017-09-14"
menu:
   main:
      parent: "Client"
---

Introduction
-------------

Ozone ships with it own client library, that supports both RPC(Remote
Procedure call) and REST(Representational State Transfer). This library is
the primary user interface to ozone.

It is trivial to switch from RPC to REST or vice versa, by setting the
property _ozone.client.protocol_ in the configuration or by calling the
appropriate factory method.

## Creating an Ozone client
The Ozone client factory creates the ozone client. It allows the user to
specify the protocol of communication. For example, to get an REST client, we
can use

{{< highlight java >}}
OzoneClient ozClient = OzoneClientFactory.getRestClient();
{{< /highlight >}}

And to get a a RPC client we can call

{{< highlight java >}}
OzoneClient ozClient = OzoneClientFactory.getRpcClient();
{{< /highlight >}}

If the user want to create a client based on the configuration, then they can
call

{{< highlight java >}}
OzoneClient ozClient = OzoneClientFactory.getClient();
{{< /highlight >}}

and an appropriate client based on configuration will be returned.

## Writing data using Ozone Client

The hierarchy of data inside ozone is a volume, bucket and a key. A volume
is a collection of buckets. A bucket is a collection of keys. To write data
to the ozone, you need a volume, bucket and a key.

### Creating a Volume

Once we have a client, we need to get a reference to the ObjectStore.  This
is done via

{{< highlight java >}}
ObjectStore objectStore = ozClient.getObjectStore();
{{< /highlight >}}

An object store represents an active cluster against which the client is working.

{{< highlight java >}}
// Let us create a volume to store our game assets.
// This uses default arguments for creating that volume.
objectStore.createVolume(“assets”);

// Let us verify that the volume got created.
OzoneVolume assets = objectStore.getVolume(“assets”);
{{< /highlight >}}


It is possible to pass an array of arguments to the createVolume by creating volume arguments.

### Creating a Bucket

Once you have a volume, you can create buckets inside the volume.

{{< highlight bash >}}
// Let us create a bucket called videos.
assets.createBucket(“videos”);
Ozonebucket video = assets.getBucket(“videos”);
{{< /highlight >}}

At this point we have a usable volume and a bucket. Our volume is called assets and bucket is called videos.

Now we can create a Key.

### Reading and Writing a Key

With a bucket object the users can now read and write keys. The following code reads a video called intro.mp4 from the local disk and stores in the video bucket that we just created.

{{< highlight bash >}}
// read data from the file, this is a user provided function.
byte [] vidoeData = readFile(“into.mp4”);

// Create an output stream and write data.
OzoneOutputStream videoStream = video.createKey(“intro.mp4”, 1048576);
videoStream.write(videoData);

// Close the stream when it is done.
 videoStream.close();


// We can use the same bucket to read the file that we just wrote, by creating an input Stream.
// Let us allocate a byte array to hold the video first.
byte[] data = new byte[(int)1048576];
OzoneInputStream introStream = video.readKey(“intro.mp4”);
// read intro.mp4 into the data buffer
introStream.read(data);
introStream.close();
{{< /highlight >}}


Here is a complete example of the code that we just wrote. Please note the close functions being called in this program.

{{< highlight java >}}
OzoneClient ozClient = OzoneClientFactory.getClient();

// Let us create a volume to store our game assets.
// This default arguments for creating that volume.
objectStore.createVolume(“assets”);

// Let us verify that the volume got created.
OzoneVolume assets = objectStore.getVolume(“assets”);

// Let us create a bucket called videos.
assets.createBucket(“videos”);
Ozonebucket video = assets.getBucket(“videos”);

// read data from the file, this is assumed to be a user provided function.
byte [] vidoeData = readFile(“into.mp4”);

// Create an output stream and write data.
OzoneOutputStream videoStream = video.createKey(“intro.mp4”, 1048576);
videoStream.write(videoData);

// Close the stream when it is done.
 videoStream.close();


// We can use the same bucket to read the file that we just wrote, by creating an input Stream.
// Let us allocate a byte array to hold the video first.

byte[] data = new byte[(int)1048576];
OzoneInputStream introStream = video.readKey(“into.mp4”);
introStream.read(data);

// Close the stream when it is done.
introStream.close();

// Close the client.
ozClient.close();
{{< /highlight >}}
