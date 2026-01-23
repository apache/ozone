# The Goal

The goal of Zero Downtime Upgrade (ZDU) is to allow the software running an existing Ozone cluster to be upgraded while the cluster remains operational. There should be no gaps in service and the upgrade should be transparent to applications using the cluster.

Ozone is already designed to be fault tolerant, so the rolling restart of SCM, OM and Datanodes is already possible without impacting users of the cluster. The challenge with ZDU is therefore related to wire and disk compatibility, as different components within the cluster can be running different software versions concurrently. This design will focus on how we solve the wire and disk compatibility issues.

# Component Upgrade Order

To simplify reasoning about components of different types running in different versions, we should reduce the number of possible version combinations allowed as much as possible. Clients are considered external to the Ozone cluster, therefore we cannot control their version. However, we already have a framework to handle client/server cross compatibility, so rolling upgrade only needs to focus on compatibility of internal components. For internal Ozone components, we can define and enforce an order that the components must be upgraded in. Consider the following Ozone service diagram:

![][image1]

Here the arrows represent client to server interactions between components, with the arrow pointing from the client to the server. The red arrow is external clients interacting with Ozone. The shield means that the client needs to see a consistent API surface despite leader changes in mixed version clusters so that APIs do not seem to disappear and reappear based on the node serving the request. The orange lines represent client to server interactions for internal Ozone components. For components connected by this internal line, **we can control the order that they are upgraded such that the server is always newer and handles all compatibility issues**. This greatly reduces the matrix of possible versions we may see within Ozone and mostly eliminates the need for internal Ozone components to be aware of each other’s versions, as long as servers remain backwards compatible. This order is:

1. Upgrade all SCMs to the new version  
2. Upgrade Recon to the new version  
3. Upgrade all Datanodes to the new version  
4. Upgrade all OMs to the new version  
5. Upgrade all S3 gateways to the new version

Note that in this ordering, Recon will still have a new client/old server relationship with OM for a period of time. The OM sync process in Recon is the only API that needs to account for this, and it is not on the main data read, write, delete, or recovery path. Recon should be upgraded with the SCMs because its container report processing from the datanodes shares SCM code, so we do not want Recon to handle a different version matrix among datanodes than SCM.

# Software Version Framework

The previous section defines an upgrade order to handle API compatibility between internal components of different types without the need for explicit versioning. For internal components of the same type, we need to provide stronger guarantees when they are in mixed versions:

* Components of the same type must persist the same data  
* Components of the same type must expose a consistent API surface

To accomplish these goals, we need a versioning framework to track component specific versions and ensure components of the same type operate in unison. Note that this versioning framework will not extend beyond Ozone into lower level libraries like Ratis, Hadoop RPC, gRPC, and protobuf. We are dependent on these libraries providing their own cross compatibility guarantees for ZDU to function.

### Versioning in the Existing Upgrade Framework

Before discussing versioning in the context of ZDU, we should first review the versioning framework currently present which allows for upgrades and downgrades within Ozone, and cross compatibility between Ozone and external clients of various versions.

Ozone components currently define their version in two classes: ComponentVersion and LayoutFeature. Any change to the on-disk format increments the Layout Feature/Version, which is internal to the component. You can see examples of the Layout Version in classes such as HDDSLayoutFeature, OMLayoutFeature and ReconLayoutFeature. Any change to the API layer which may affect external clients will increment the ComponentVersion. Component versions are defined in classes like OzoneManagerVersion and DatanodeVersion. One change may have an impact in both areas and need to increment both versions.

The existing upgrade framework uses the following terminology:

**Component version**: The logical versioning system used to track incompatible changes to components that affect client/server network compatibility. Currently it is only used in communication with clients outside of Ozone, not within Ozone components itself. The component version is hardcoded in the software and does not change.

**Layout Feature/Version:** The logical versioning system used to track incompatible changes to components that affect their internal disk layout. This is used to track downgrade compatibility.

**Software Layout Version (SLV):** The highest layout version within the code. When the cluster is finalized, it will use this layout version.

**Metadata Layout Version (MLV):** The layout version that is persisted to the disk, which indicates what format the component should use when writing changes. This may be less than or equal to the software layout version. 

**Pre-finalized:** State a component enters when the MLV is less than the SLV after an upgrade. At this time existing features are fully operational. New features are blocked, but the cluster can be downgraded to the old software version. Pre-finalized status does not affect component version, which always reflects the version of the software currently running.

**Finalized:** State a component enters when the MLV is equal to the SLV. A component makes this transition from pre-finalized to finalized when it receives a finalize command from the admin. At this time all new features are fully operational,  but downgrade is not allowed. Finalized status does not affect component version, which always reflects the version of the software currently running.

In the existing upgrade framework, OM and SCM can be finalized in any order. SCM will finalize before instructing datanodes to finalize. Recon currently has no finalization process, and S3 gateway does not need finalization because it is stateless.

### Versioning in the New Upgrade Framework

In practice, tracking network and disk changes separately has proven difficult to reason about. Developers are often confused about whether one or both versions need to be changed for a feature, and each version’s relationship with finalization. Before adding complexity to the upgrade flow with ZDU, it will be beneficial to simplify the two versioning schemes into one version that gets incremented for any incompatible change. This gives us the following new definitions:

**Component version**: The logical versioning system used to track incompatible changes to a component, regardless whether they affect disk or network compatibility between the same or different types of components. This will extend the existing component version framework.

**Software version:** The Component Version of the bits that are installed. This is always the highest component version contained in the code that is running.

**Apparent version:** The Component Version the software is acting as, which is persisted to the disk.

**Pre-finalized:** State a component enters when the apparent version on disk is less than the software version. At this time all other machines may or may not be running the new bits, new features are blocked, and downgrade is allowed.

**Finalized:** State a component enters when the apparent version is equal to the software version. A component makes this transition from pre-finalized to finalized when it receives a finalize command from the admin. At this time all machines are running the new bits, and even though this component is finalized, different types of components may not be. Downgrade is not allowed after this point.

This simplified version framework lets us enforce **three invariants** to reason about the upgrade process among internal components (OM, SCM, Datanode, Recon):

* **Internal components of the same type will always communicate using the same apparent version.**  
* **At the time of finalization, all internal components must be running the new bits.**  
* **For internal client/server relationships, the server will always finalize before the client.**

Later sections on upgrade flow and ordering will detail how these invariants are enforced. Note that external clients can be in any version and we will support full client/server version cross compatibility between internal components and external clients.

### Usage of the Versioning Framework During Upgrades

When a cluster is running, its version will be stored on disk as the apparent version. An upgrade is triggered when a process is started with a newer version than the apparent version written to disk. On startup, the process can read the apparent version from disk and notice that its software version is higher. Since it has not been finalized, it will then “act as” this earlier apparent version until it is later finalized. In this state, the code must be implemented such that the API surface, the API behaviour and the on-disk format of persisted data are identical to the older versions. Even though the new version can have new features, APIs, and persist different data to disk, they must all be feature gated and unavailable until the upgrade is finalized. This will maintain a consistent API surface for clients despite internal components having different versions. This will be the case for ZDU upgrades and non-rolling upgrades.

For external clients, the apparent version is what will be communicated from the server to provide their view of the server’s version. This differs from the current model where clients receive the static component version which is always defined by the latest version the software supports. While a client from another cluster could in theory attempt to use some of the new features, which would result in an error, this is unlikely to happen as the Ozone clients are version aware and should similarly be coded so they don’t attempt incompatible calls supported by newer versions.

For internal components, the “new client old server” invariant makes version passing among internal components of different types mostly unnecessary. For example, SCM does not need to worry about whether the OM client it is communicating with has the new bits or whether the OM has been finalized. SCM’s server will always be newer and finalized before OM. Therefore it can remain backwards compatible and will work with the OM in either case. One exception is that datanodes will need to add their software and apparent versions to SCM heartbeats so SCM can instruct them to finalize if needed, or fence them out of the cluster if they are running the wrong software version at the time of finalization.

### Migrating to the New Unified Component Version Framework

The existing component version enum will be the basis of the new unified versioning framework. This is because it is shared with external clients who can be in any version and may contact the cluster at any time. The existing layout feature enum is internal to components, and therefore easier to control during the migration.

To migrate to one single layout version, we will add a new software version “100” to each existing component version enum. Version 100 will universally indicate the first version that is ZDU ready, and the point from which this unified version will be used to track all changes through the existing component version enum.

Note that the version number we use for this migration must be larger than both the largest existing component version and largest existing layout version to prevent either one from appearing to go back in time before the migration is finalized. 100 was chosen as an easily identifiable number that can be used across all components to indicate the epoch from which they all have migrated to the unified framework and support rolling upgrade.

This migration will be transparent in client/server interactions for network changes. This will simply appear as a new larger version with all the previous versions in the existing component version enum still intact.

This migration will need some handling for disk changes. When the upgraded component starts up with software version 100 and sees a version less than that persisted to the disk, it must use the old LayoutFeature enum to look up that version until the cluster is finalized. After finalization, version 100 will be written to the disk and all versions from here on can be referenced from ComponentVersion.

# Strategy To Achieve ZDU

## Prerequisites

Before an Ozone cluster can use ZDU in an upgrade, the initial version being upgraded must also support ZDU. All software versions from 100 onward will be ZDU ready, and any Ozone changes after version 100 have to be made in a ZDU compatible way. We can say that version 100 is the minimal eligible version for ZDU. For example, a cluster would need to be upgraded from version 5 version to 105 with the existing non-rolling upgrade process. All upgrades starting from version 105 could then optionally be done with ZDU or non-rolling upgrades.

## Invariants

This is a summary of invariants for internal components outlined in earlier sections which will be maintained during the upgrade flow. These provide a framework for developers to reason about changes during the upgrade:

* Internal components of different types will always have the server in the same or newer version as the client.  
  * The only exception is Recon to OM communication.  
* Internal components of the same type will always operate at the same apparent version.  
  * This implies that they expose the same API surface and persist data in the same format.  
* At the time of finalization, all internal components must be running the new bits.  
* Internal components of different types will always have the server side finalize before the client side.

## Order of Operations During the Upgrade

1. Deploy the new software version to SCM and rolling restart the SCMs.  
2. Deploy the new software version to Recon and restart Recon.  
3. Deploy the new software version to all datanodes and rolling restart the DNs.  
4. Deploy the new software version to all OMs and rolling restart the OMs.  
5. Deploy the new software and rolling restart all client processes like S3 Gateway, HTTPFS, Prometheus etc. These processes are all Ozone clients and sit somewhat outside of the core Ozone cluster.  
   1. At this stage, the cluster is operating with the new software version, but is still “acting as” the older apparent version. No data will be written to disk in a new format, and new features will be unavailable.  
6. The finalize command is sent to SCM by the admin \- this is what is used to switch the cluster to act as the new version. Upon receipt of the finalize command:  
   1. SCM will finalize itself over Ratis, saving the new finalized version.  
   2. It will notify datanodes over the heartbeat to finalize.  
   3. After all healthy datanodes have been finalized, OM can be finalized. To do this, OM will have been polling SCM periodically to see if it should finalize. Only after SCM and all datanodes have been finalized will OM get a “ready to finalize” response from the poll. The OM leader will then send a finalize command over Ratis to all OMs.  
   4. As OM is the entry point to the cluster for external clients, finalizing OM unlocks any new features in the upgraded version.

Before the cluster is finalized, it is possible to downgrade to the previous version by stopping the cluster and restarting it with the older software version. No data in a new format will have been persisted that the older software version will not understand. The restart with the downgraded software can either be done non-rolling, or rolling by restarting components in the reverse of the order outlined above.

During the upgrade, the cluster’s fault tolerance will not change. As nodes are being restarted with the new versions, we still require 2 OMs and SCMs active at all times to remain available. If any nodes fail to start in the new version, our existing fault tolerance accounts for this. The node should be brought online either by resolving the issue or downgrading it before others are restarted. Note that all nodes must be running the newest software version for finalization to begin, but the cluster remains fully operational with existing features until then.

Initially, this design considered pausing some background operations to remove risk during upgrade. Snapshots are an area with complex storage requirements that must be mirrored across the OMs. However a ZDU can take several days and removing the ability to take or delete snapshots during that time would impact backup and disaster recovery schedules which would not be acceptable. Similarly block deletion was considered and similar concerns were uncovered around freeing space on clusters with capacity issues. It would also not be wise to suspend replication for an extended period.

DIsk and datanode balancing could be safely suspended if required. For disk balancing, the process is all within the same datanode process, so mixed component versions are not a concern. Cross node balancing uses the container replication mechanism internally, and we would not gain much by pausing it during upgrades either.

# The Finalization Process Per Component

After a cluster has all components started with the new software version, the cluster will be operating without any of the features in the new version until it is finalized. The finalization process is started when an administrator issues a “finalize” command to the cluster. This will be received by SCM, which will coordinate the process for the whole cluster. The existing OM Finalize command will be deprecated and become a no-op for compatibility. The cluster will make best effort checks that the admin has properly updated the software of all components before processing the finalization command, but if they have not done so it is user error and the result is ultimately undefined.

Also note that finalization is asynchronous. Not all components can be updated simultaneously so the cluster will be finalized a short time after the command has been issued.

## SCM {#scm}

The first component to finalize is SCM. This will unlock any new APIs in SCM, but these will not be called until other components who are clients of SCM are also finalized. To coordinate between all SCM instances and ensure they switch to any new behaviour simultaneously, the finalize command is sent over Ratis, and hence will be replayed on the followers alongside other commands in the correct order.

When SCM receives a finalize command, it will perform best effort verifications to ensure that other components are running the latest software version before it proceeds. This can potentially catch cases where an admin has incorrectly updated the software before running finalize and some components are still in the old version, but is not guaranteed in all cases.

1. SCM will make sure that all registered datanodes are heartbeating a software version that matches the latest datanode component version shipped with the SCM code.  
   1. If SCM’s node set contains any nodes with an older software version, finalization will fail.  
   2. Datanodes which are dead/offline during finalization and are running an old software version will need to be updated to the new software version before SCM allows them to re-register.  
2. SCM will check that the other SCMs in its Ratis ring are running the same software version.  
   1. Before submitting the finalize request to Ratis, the leader SCM will call the same “finalization status” API used by clients on each SCM to get their software versions. If these do not match its version, it will fail the request.  
   2. Note that this is a best effort check. It is possible that a finalize request is applied to the log in one version and the SCM is restarted in a different version and then the request is applied.  
3. When applying the finalize transaction through Ratis, SCM will verify that the software version in the request matches its own software version.  
   1. If the version matches, SCM can safely apply the request.  
   2. If the version is less than SCM’s software version, it will no-op the request.  
      1. This would happen if finalize was sent from an SCM running the old bits. We cannot crash this SCM in this case because it would never recover trying to apply the transaction.  
      2. In this case the leader and follower still have matching apparent versions so it is safe to keep running.  
   3. If the version is greater than SCM’s software version, it will terminate itself.  
      1. To recover, the SCM must be restarted with the new software version which will match the leader issuing the request.  
      2. We cannot no-op this request because the leader and follower would then have different apparent versions, violating one of our upgrade invariants.

The following table shows what would happen if finalize is incorrectly sent to SCMs before they are all on the same software version.

| Leader | Follower 1 | Follower 2 | Outcome |
| :---- | :---- | :---- | :---- |
| Apparent ver: 100Software ver: 105 | Apparent ver: 100Software ver: 105 | Apparent ver: 100Software ver: 105 | Finalize should succeed and move all SCMs’ apparent versions move to 105 atomically |
| Apparent ver: 100Software ver: 100 | Apparent ver: 100Software ver: 105 | Apparent ver: 100Software ver: 105 | Finalize ignored on leader. Passed to followers and ignored there too. |
| Apparent ver: 100Software ver: 105 | Apparent ver: 100Software ver: 105 | Apparent ver: 100Software ver: 100 | Finalize will fail on follower 2 and it should exit. It must be upgraded to version 105 to proceed. |
| Apparent ver: 105Software ver: 105 | Apparent ver: 105Software ver: 105 | Apparent ver: 105Software ver: 105 | Cluster is already finalized, but the command can be forwarded in case there are any lagging followers and get ignored on the followers. |

## Datanodes

Unlike SCM, all datanodes are not in a Ratis ring together and therefore it is impossible to finalize all datanodes at the same time. SCM will issue the finalize command to datanodes after it has been finalized over the Datanode heartbeat. SCM will not consider the datanodes as finalized until all registered DNs have reported success.

Any Datanodes that are offline (dead) need not be considered as they will need to re-register. If the cluster is finalized and the Datanode is not finalized, its registration will be rejected and it will be instructed to finalize and register again before it can join the cluster.

If a Datanode attempts to register with a software version greater than the max DatanodeVersion known to SCM, then it will be rejected. This can only occur if the Datanode has been upgraded to a version newer than the SCM version. As the ZDU requirement is for SCM to be upgraded first, Datanodes having a higher version is invalid.

If a Datanode attempts to register and its software version is less than the one known by SCM, it indicates that Datanode is still running the old software version. If SCM is unfinalized, it is expected that datanodes can register with the older version, as the DNs will be upgraded after SCM. If SCM is finalized, any Datanodes attempting to register at an older software version should be rejected permanently until the datanode is upgraded so that the versions match.

The following table captures the states SCM can be in and how it will respond to different Datanode versions:

| SCM State | Datanode State |  |
| :---- | :---- | :---- |
| Software version 100 Apparent version: 100 | Software Version: 100Apparent Version: 100 | This is the expected state of a cluster where upgrade is not ongoing. |
| Software version 105 Apparent version: 100 | Software Version: 100Apparent Version: 100 | This is an expected state. SCM software has been upgraded and the datanode has not. It is expected the datanode will upgrade later. |
| Software version 105 Apparent version: 105 | Software Version: 100Apparent Version: 100 | SCM is upgraded and finalized and the datanode is still at the old version. This should only happen when the datanode was offline during the upgrade. As it has an older software version, SCM will block it from registering until its software has been updated. |
| Software version 105 Apparent version: 100 | Software Version: 105Apparent Version: 100 | SCM is upgraded and not finalized, as is the datanode. This is an expected state until all datanodes reach this condition and SCM receives a finalize command from the admin. |
| Software version 105 Apparent version: 105 | Software Version: 105Apparent Version: 100 | SCM is upgraded and finalized and the datanode is upgraded but not finalized. This can only happen if the datanode is offline during the finalization process. SCM will instruct the Datanode to finalize upon registration and it will not be able to register until it does. |
| Software version 105 Apparent version: 100 | Software Version: 110Apparent Version: 100 | SCM is upgraded and not finalized. The datanode is upgraded, but to a greater version than SCM.SCM should reject the datanode on registration as its version is newer than SCM. |
| Software version 105 Apparent version: 105 | Software Version: 110Apparent Version: 100 | SCM is upgraded and finalized. The datanode is upgraded, but to a greater version than SCM.SCM should reject the datanode on registration as its version is newer than SCM. This should only happen if the datanode was offline during the SCM finalization process. |
| Software version 105 Apparent version: 105 | Software Version: 110Apparent Version: 105 | SCM is upgraded and finalized. The datanode is upgraded, and finalized to the same version as SCM, but its software version is greater than SCM.SCM should reject the datanode on registration as its version is newer than SCM. |

On each heartbeat, a Datanode will report its software version and apparent version. When the datanode software version matches the max Datanode Software version known to SCM, SCM knows that datanode is running the latest software version.

When a datanode is reporting the same software and apparent version, and that version matches the max datanode version known to SCM, SCM knows the datanode is running the latest version and is finalized.

SCM will not allow the finalize command to be passed to any datanode (or finalize itself) until all DNs are running the latest version.

SCM will consider Datanode finalization complete when all healthy datanodes are reporting the correct software version and report a matching Apparent Version.

### Mixed Datanode Versions During Write

As Datanodes potentially finalize at different times, there will be a period where clients will be writing to a pipeline with a mixture of finalized and unfinalized Datanodes. Coupling the version to pipelines and/or subgroups of nodes becomes difficult to manage at scale, and the upgrade framework’s current method of closing all pipelines and containers on finalization will not work in a ZDU scenario.

To ensure that all datanodes in a write pipeline execute a write with the same version, they will need to learn the version to use from the client. They cannot learn it from peer datanodes because datanode peers do not communicate on the erasure coding write path. Clients already learn the component version of the datanodes in the pipeline (which will be their apparent version in the new framework) in the datanode details for the pipeline they get back from SCM, which is propagated through OM.

Due to the finalization requirement, nodes will always have the code to execute things at a lower component version than their software version. This means that even if a datanode is finalized, it is still able to execute writes as a lower version if required. SCM is tracking the finalization status of all registered datanodes, so it knows the lowest apparent version among all datanodes in a pipeline. SCM can give this out as the component version in the datanode details, which clients will forward to datanodes so they all use the same version when doing writes for that client. Deciding the version to use by taking the minimum apparent version of all datanodes in a pipeline should be done server side by SCM. This means clients are only doing a simple passthrough of an integer to the datanodes, and gives us freedom to change how this version is calculated in future Ozone versions without worrying about the logic in older clients.

Consider this example where we want to move from datanode component version 100 to 105:

1. SCM is giving out block allocations tagged with DN version 100 because not all datanodes have been finalized to version 105\.  
2. Clients pass this version to the datanodes they interact with.  
   * If anything on the write path needs to make a switch based on this version, it can. For many features it won't though, so this is just a no-op pass through.  
3. SCM sends the finalize command to all datanodes.  
   * It still only hands out block allocations in version 100 since this is the lowest common version among all DNs.  
4. SCM learns that all live datanodes have been finalized, meaning the lowest common version among all DNs is now 105\.  
5. SCM begins handing out block allocations tagged with version 105\.

Old clients from versions before the first one with rolling upgrade support (v100) will not pass a version to the datanodes. If a datanode does not receive a version from the client, it needs to execute the request as version 100 (the first component version to support rolling upgrade) to be safe. In practice this should not be much of an issue. Version changes that affect the write path are going to be rare, like a new container schema. For example, say we are on version 105, and versions 100 to 105 don't require special handling when writing data. Then no code on the write path will check them and clients less than v100 will have exactly the same effect on the server as those in the range v100-105.

It was considered to have SCM give clients the finalized version instead of the minimum version to pass to datanodes after SCM sends the finalize command to datanodes. In this case, datanodes would finalize as soon as they get the write request if they have not already from the heartbeat, and then begin processing the request. This approach does not provide much benefit over the current proposal:

* Datanode finalization is already designed to be asynchronous within each datanode, because the finalize request can come in from the SCM heartbeat at any time. Adding a blocking element to the write is therefore not necessary for correctness.  
* Slow clients who allocated blocks before datanodes finalized and took a long time to get back to the datanode to actually write them will still write using the older version anyways.

    Significantly, it has one security drawback: Rogue/custom clients could instruct a datanode to finalize with just a block token. Block tokens are the only way clients authenticate to the datanode. They are scoped to the current block and are not intended to permit node-wide changes like finalization. A client with permissions to write a block could use the token to create a command  with a higher version and get the datanode to finalize ahead of SCM, causing the datanode to be fenced out of the cluster. To get around this, we would need to add a new \[Access Mode\](https://github.com/apache/ozone/blob/a534ac2f38891a088bfa8c821e8c228b16864a82/hadoop-hdds/interface-client/src/main/proto/hdds.proto\#L394) to the block token to specifically permit finalization, which does not seem worth it given the marginal value this feature would provide.

### Mixed Datanode Versions During Replication

Datanodes act in a client server relationship between themselves for replication. In general the replication process is quite simple \- commands are sent to a datanode hosting a container replica, and it is told to push the data to another node. Therefore incompatible changes are unlikely. However they can be handled in a similar way to above. 

The Datanode’s apparent version can be sent along with the replication request. If the server is at a newer version (e.g. source/client is at 100 and server/target is at 105\) then the server can process the request as of version 100\. 

If the client is ahead of the server, this would trigger the finalize command on the receiving server Datanode, as it needs to finalize anyway. There is no initial handshake in the container replication process for the new client to learn the old server’s version and downgrade its request to match. After the datanode server finalizes, it will begin processing the request with the new version that matches the client. Note that the datanode sending the replica possesses a container token, which is internal to the cluster and therefore sufficient to permit finalization, unlike block tokens.

## OM

When OM is started in a pre-finalized state, it will poll a new SCM endpoint periodically to see if it should finalize. This will be a small metadata based RPC call that does not need to run very frequently, perhaps once a minute. SCM will only reply OK after SCM has finalized and all Datanodes have finalized. 

At this stage the HDDS layer of the cluster is ready for any new features or on-disk formats. As clients learn the cluster version from OM, OM must be finalized before any of the new features are unlocked.

Upon receiving the “ok to finalize” reply from SCM, OM will finalize and send the finalize command to all followers over Ratis. This ensures that all OMs will replay the finalize request in the correct order with other commands and ensure consistency across the cluster. The software version the OMs are finalizing too will be sent with the finalize command and the same logic can be applied as in the [SCM table](#scm) to determine what to do if some OMs are not at the correct version. Once all OMs are finalized, OM will stop polling the SCM endpoint.

## S3G

S3 Gateway can be considered a client of the Ozone cluster itself. It is stateless and interacts with the wider Ozone cluster via the usual Ozone client. Since it does not write anything to disk, it does not have a finalization framework and hence is not able to version its APIs using the same mechanism as the stateful components and will always act as its latest known software version. It also uses the Ozone client to make its calls to the cluster, which is version aware. This means S3G will be able to return clean errors if an API is used before finalization, and it cannot affect consistency inside the core Ozone cluster. 

Most changes to S3G will be additive in nature \- adding new APIs or fields / options that can be passed over existing APIs. A backwards incompatible change to the REST API side of S3 gateway would imply an incompatible change to the S3 spec itself. To use any of these new features requires some external service to send requests to S3G using the new features. Users of S3G should therefore not make calls to “new features” until the upgrade is complete, otherwise they will get errors. This means that it will be OK for the version of S3G to be updated and called by existing clients during the upgrade and the new features will simply not get used.

If a customer has more complex requirements there are options to keep some S3G instances running at the old version and direct all traffic to them at the load balancer. This would be similar to blue-green deployments in more traditional applications.

## Recon

Recon acts as both a client and server. It makes requests to both SCM and OM and Datanodes heartbeat to it in a similar way as to SCM. In the upgrade flow, Recon will be upgraded at the start along with SCM. This keeps things simple for the Datanodes, as they will be heartbeating to both SCM and Recon, which will both be at the same version. Similar to SCM, Recon will need to be able to handle any new and old format heartbeats from datanodes, as the datanodes will be at mixed versions as the upgrade progresses.

Recon also makes outgoing calls to OM via a Rest API and to SCM using RPC calls. It will need to be able to handle any old or new version responses from those APIs. OM will initially be at the older version before being finalized. For the Recon to SCM call, SCM and Recon are both upgraded at the same time, but SCM would be “acting as” the older version until it is finalized. So again Recon must handle both old and new format responses.

Recon currently doesn’t have a finalization framework, so it does not have the ability to “act as” an older version. If we come across a situation where Recon needs the finalization framework, it can be added. Recon could then finalize in a similar way to OM, by polling the SCM finalization endpoint. Until that time it is proposed to avoid complicating it by adding a finalization framework.

In the worst case scenario, Recon will write something to disk in a new format, and the upgrade is aborted and rolled back. Then the older software will be unable to read the new format data. In that case, if the change cannot be easily undone manually, Recon can be reconstructed from scratch using a fresh OM snapshot.

# Development Practices to Ensure Compatibility

At any point in the upgrade flow, we can have the pre and post upgrade external client version talking to a pre and post upgrade server version. Additionally the post upgrade server version can be finalized or not.

If the server is not finalized, then it should be “acting as” the pre upgrade version and its exposed API and Apparent Software Version returned to any clients will represent the pre-upgrade server version. In that respect, the unfinalized upgrade software version can be considered the same as the pre-upgrade version from a client’s perspective.

Additionally a server must also be able to handle older client requests, as it is possible an older client version from outside the cluster will continue using the cluster, even after the upgrade completes. This is already the case in Ozone and backward compatibility for clients is already supported.

For both client and server, the onus is only on the newest version to make decisions about compatibility. Any old version has no knowledge of the new feature, and it cannot enforce decisions about future unknowns.

## Client Behaviour

Client changes can be categorized into two areas. Adapting an existing API, or making calls to a new API. In either case, the client gets to know the current OM version when it is initialized. Using that version it can make decisions about which APIs to call, or which fields to pass in an API call.

For an existing API, if a new field is made available at OM, then the client should only pass it if OM is reporting an Apparent Version that is greater than the version which introduced the field.

For something like Atomic Rewrite, which added an extra field to an existing API, expecting different server behaviour if passed, then the client should give an error to the user if an attempt to use the feature occurs before OM can support it.

As an older OM version would happily accept unknown fields in the protobuf messages, the onus is on the client to ensure it doesn’t expect behaviour which the server cannot yet support.

These practices should already be used when making client side changes, and therefore there isn’t much change to development on the client side.

## Server Behaviour

During an upgrade, the server processes must have the ability to “act as” the pre-upgrade version. This means that any change to an API behaviour, expected fields or resulting data persisted by the server process must be feature gated using the version framework, and the old logic retained. Only if the server’s Apparent Version is equal or greater than the feature gate version should the new behaviour be permitted.

In general, no attempt should be made to use a new feature by an Ozone client, as it has its own feature gates. If something slips through due to a bug in the client, the server should return an error if it can. The only way the server could return an error is by running at the latest version but in an unfinalized state, so it knows of the new feature, but it’s not yet available.

As with the clients, this is not a drastic departure from current development techniques, as the need to support older clients requires care when changing existing APIs. It may mean more frequent additions to the component versions than before, because any extension of the API will require a version so the API surface remains consistent during the rolling upgrade.

### Handling Apply Transaction in Mixed OM Versions

The current upgrade process requires a "prepare for upgrade” step before the OMs are stopped in the old version and started in the new version. This flushes all Ratis transactions from the log to the state machine and puts the OM in a read-only mode, which it can leave when all the OMs are restarted in the new version. This prevents OMs from applying requests from the Ratis log in different versions and potentially diverging their state machines, but the read-only requirement will not work for ZDU.

The long term mitigation for this is to complete the leader execution project, which will ensure that all OMs make the same state machine changes as the leader regardless of their version. This project is going to take a while to complete and we do not want to block ZDU on its completion. Instead, we can use the unified versioning framework inside the OM apply transaction methods. Any time a change is made to a request’s apply transaction processing that changes what would be written to the state machine across versions, it needs to be behind a version flag. In the short term, this will result in more use of version flags than we currently have in the OM request processing. However, in the long term these flags can be removed for each request after it is migrated to leader execution. We can also consider adding a combination of AI and/or manual inspection protocols before releases to assess whether any apply transaction versioning was missed.

## Removing Old Code

If we want to support rolling upgrades from any past version to any current version of Ozone, old processing code with disk or network compatibility concerns cannot be removed. This code will be required to run before its replacement while the last apparent version is being used during the upgrade. If we would like to remove old processing code, we will need to create a mandatory version that all upgrades must pass through, which would be the last one to contain such code. The next release after this version may then remove the code. See [Leader Execution](#leader-execution) for a case where this may be desirable.

## Testing

Ozone has existing test tools that can be used for compatibility testing at both the unit, integration, and acceptance test levels. These would only require minimal extension to work with ZDU and the new versioning system.

Unit and integration tests are run by a single Java process in the same version as the code being tested, so it is not possible to have truly mixed versions in this environment. The current approach used for client cross compatibility and disk compatibility testing is allowing tests to inject a custom component version or layout version into the client or server to see how it responds. This will remain the approach with the new versioning framework.

Acceptance tests currently use docker to pull past releases and orchestrate an upgrade and downgrade between those and the version under test while reading and writing data and possibly testing new features specific to a release. This framework was designed to support [pluggable methods for upgrading](https://github.com/apache/ozone/blob/de5c0a385ed873425ae94245d8b5b28040ab99ef/hadoop-ozone/dist/src/main/compose/upgrade/upgrades), so we will add a new driver that orchestrates a rolling upgrade while running the defined tests at each stage.

## Examples From Past Development To Ensure Compatibility

This section talks about some changes made to Ozone over the recent past, and how they have been handled for compatibility. This should help the reader understand the approach to developing changes compatible with ZDU. In looking at these examples, it becomes clear that the current approach to development is already performed in a mostly compatible way, so we just need to be more rigorous in reviews to ensure that remains the case.

### Container Reports Adding the isEmpty flag

At some point in the past a new flag was added to the Full Container Reporting from datanodes to SCM. This flag, “is Container Empty” was to counter a bug in SCM, where it marked containers as empty erroneously. Fixing the bug required a change in two places:

1. The Datanode producing the container report  
2. Container report processing in SCM

Consider whether this change needs a software or layout upgrade.

Protobuf is forward and backwards compatible. This means that if a newer client produces a message containing a new field (isEmpty), and the server receives this message, the server will not fail. It simply does not know the field exists and it gets ignored in the message. In this case, while the datanode has been upgraded to a version that fixes the bug, SCM has not and the bug is still present until SCM is upgraded.

In a similar way, if the server is newer than a client sending a message, then the server needs to check if the new field is present, and have fallback handling if it is not. With the currently proposed ZDU framework, this is always the case.

In this example, the safe approach would be for SCM to default to setting isEmpty to false if it is not passed in the container report, knowing that a datanode with a matching software version would always explicitly send the field. As the new SCM version knows there is a problem with empty container handling it is safe for it to assume the containers are non-empty until told otherwise.

Considering this example, I don’t believe that this scenario requires a specific “version gate” flag. Many protobuf field additions are like this, where the server can simply default to the “old handling” if the expected field is not passed. Or in the case of a bug, do the best thing for safety provided it doesn’t change persisted data.

Each case must be considered on its own merits and there isn’t a hard rule aside from “do not change persisted data”.

### Atomic Key Rewrite

Atomic Key Rewrite was added to OM to allow Tiering to replace keys in the cluster only if they had not been modified since some previous read of the key.

This change involved:

1. A new field, expectedDataGeneration passed from the client to server when writing a key  
2. Persisting that field in the openKey entry in the open key table  
3. Checking the previously stored field on key commit against the latest version of the key to ensure it had not changed.

This problem **does need** a version gate flag from two perspectives.

1. If a new client is talking to a pre-atomic rewrite OM, then the old OM would happily accept the expectedDataGeneration in the existing create key protobuf message. However, it would not know to check for its existence and simply ignore it. The client would therefore believe it is performing “atomic rewrites” when it is not. Unlike with the isEmpty flag example, the old OM ignoring the flag can do some harm (the client could overwrite a newer version of a key than it intended).  
2. There is a change to the persisted disk data. If a new version client sends a message to a new version OM, it will persist the new expectedDataGeneration field in the database. If this transaction is replayed on an older OM, it would not persist the field as it does not know about it. This can result in database mismatches between OM. If a failover occurred to the old version OM before the key is committed, then the atomic overwrite cannot be checked as the field is not replicated, and the code is not there to check it anyway.

In this case, it is essential that OM “acts as of” its old version and rejects any requests from a client which contain the expectedDataGeneration field. A client should ideally also fail to allow the new API to be used at the client side as it can ask OM what its current version is. The client check is somewhat optional in this case provided integrity is enforced at the server level. However in other cases, the client can decide which API to call depending on the server version returned.

### List Keys Light

List Keys Light was added to combat a GC problem which caused OM to struggle when returning large file listings.

This is a read only API and there are no concerns about calls to it modifying data on OM.

When running an Ozone client with a version that knows about listKeys Light, it will attempt to use it rather than the legacy version. However, if the OM is older, it will not know about the new API and it would fail to process requests from the new client.

Therefore a OM version was added called LIGHTWEIGHT\_LIST\_STATUS. When the Ozone client is started, it first sends an RPC to OM to get its version. Then it can decide to use List Keys Light if it is available, or fall back to the old version if it is not.

With the proposal in this document, we would also tag List Keys Light in OM so that it is not available until OM is “acting as” a version which is greater or equal to when List Keys Light was introduced. This is not strictly necessary from a data integrity standpoint, but it brings consistency to the availability of new APIs across OM leader changes and makes it clear to developers that all such APIs should be version gated for ZDU. It also should not be possible to call the new API from the client, because the OM version returned to the client during upgrade would be the “acting as” version, rather than the latest version of the component the software supports.

## Examples From Future Development To Ensure Compatibility

This section covers a few features that may land in Ozone after rolling upgrade support is added, and how the new framework is equipped to handle them

### Leader Execution {#leader-execution}

Leader execution makes a switch in the Ozone Manager request processing (and potentially SCM after that) to compute the changes to the Ratis state machine (RocksDB) on the leader before submitting the request to Ratis, and using Ratis to reach consensus on those changes. This differs from the current model where the request is submitted to Ratis for consensus, and while applying it to the state machine, all OMs compute the required DB changes independently.

Leader execution is being designed with a switch so that requests can be migrated one by one, and flows can be switched from the old to the new at the beginning of request processing. This is the place where we would put a check for the OM’s apparent version. If our apparent version supports leader execution, we would use the leader execution flow. Until then, the old flow must be used. This supports downgrade so that older OMs can still read the entries from the Ratis log. Note that finalization itself goes through Ratis on the OMs, so they would all switch to the new flow simultaneously.

### Event Notification

Event notification adds support for plugins to read a ledger of events stored in the OM and push them to various other services. The ledger is a new RocksDB column family. This will need a component version to block plugins from running until finalization, otherwise events may seem to appear and disappear as OM leader changes while the software is being upgraded. An action bound to the finalization event can start the plugins when the version is finalized.  The component version must also block writing of events to the OM DB until finalization, because before this point the OMs may be in mixed software versions and older OMs could end up with different ledgers.

This is an example of a feature where rolling upgrade imposes stricter versioning requirements than non-rolling upgrade. If only non-rolling upgrade is supported, this feature would require no versioning. This is because all the OMs would gain or lose the support for writing and pushing events at the same time, and the old OM on downgrade would simply ignore the extra ledger column family.

### New Container Schema

Container schemas refer to the layout of individual container replicas stored on the datanodes. We are currently on our third container schema since Ozone’s inception, and it’s likely that there will be more in the future. Ozone’s approach has been to leave existing data in a backwards compatible format and do any migration as an optional background process after the upgrade is complete. This will not change with rolling upgrades.

Datanodes will choose which container schema to use based on the Datanode component version supplied by the client at the time of write to ensure that all replicas of the container have the same schema. Once a container is created, all writes to that container will use its schema version, regardless of the version the client passes. The finalization process outlined in this design ensures that all datanodes are in an apparent version that can handle this schema version at the time it is written. Note that container schemas currently only affect storage format and are invisible to clients reading and writing data from those containers.

[image1]: <data:image/png;base64,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>