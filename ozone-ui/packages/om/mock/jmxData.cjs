/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Captured Ozone Manager JMX responses, used by the json-server mock (server.cjs)
 * to serve the OM UI without a live cluster. Bean payloads mirror real
 * `GET /jmx?qry=...` responses; a few very large class-path strings are trimmed
 * (they are not surfaced in the UI).
 */

const ozoneManagerInfo = {
  name: 'Hadoop:service=OzoneManager,name=OzoneManagerInfo,component=ServerRuntime',
  modelerType: 'org.apache.hadoop.ozone.om.OzoneManager',
  RpcPort: '9862',
  Namespace: 'ozone1783424901',
  // Array of [hostName, nodeId, ratisPort, role, leaderReadiness] tuples, matching
  // OMMXBean.getRatisRoles() (List<List<String>>) on a real OM.
  RatisRoles: [
    ['node1.test.site.com', 'om1546336043', '9872', 'FOLLOWER', 'LEADER_AND_READY'],
    ['node2.test.site.com', 'om1546336047', '9872', 'LEADER', 'LEADER_AND_READY'],
    ['node3.test.site.com', 'om1546336039', '9872', 'FOLLOWER', 'LEADER_AND_READY'],
  ],
  RatisLogDirectory: '/var/lib/hadoop-ozone/om/ratis',
  RocksDbDirectory: '/var/lib/hadoop-ozone/om/data',
  Version: '2.3.0, r0a1b2c3d4e5f60718293a4b5c6d7e8f901234567',
  SoftwareVersion: '2.3.0',
  StartedTimeInMillis: 1785178223133,
};

const runtime = {
  name: 'java.lang:type=Runtime',
  modelerType: 'sun.management.RuntimeImpl',
  BootClassPathSupported: true,
  VmName: 'OpenJDK 64-Bit Server VM',
  VmVendor: 'AdoptOpenJDK',
  VmVersion: '25.232-b09',
  LibraryPath: ':/opt/ozone/current/lib/hadoop-ozone/share/ozone/lib',
  Uptime: 78876304,
  ManagementSpecVersion: '1.2',
  SpecName: 'Java Virtual Machine Specification',
  SpecVendor: 'Oracle Corporation',
  SpecVersion: '1.8',
  Name: '2455265@node1.test.site.com',
  ClassPath: '/etc/hadoop-ozone/conf:<...trimmed...>',
  StartTime: 1785178198793,
  SystemProperties: [
    { key: 'java.runtime.name', value: 'OpenJDK Runtime Environment' },
    { key: 'java.runtime.version', value: '1.8.0_232-b09' },
    { key: 'java.version', value: '1.8.0_232' },
    { key: 'java.vm.name', value: 'OpenJDK 64-Bit Server VM' },
    { key: 'java.vm.vendor', value: 'AdoptOpenJDK' },
    { key: 'java.vm.version', value: '25.232-b09' },
    { key: 'java.home', value: '/usr/lib/jvm/java-1.8.0/jre' },
    { key: 'java.io.tmpdir', value: '/tmp' },
    { key: 'user.name', value: 'hdfs' },
    { key: 'user.timezone', value: 'UTC' },
    { key: 'os.name', value: 'Linux' },
    { key: 'os.arch', value: 'amd64' },
    { key: 'os.version', value: '5.4.243-1.el7.elrepo.x86_64' },
    { key: 'file.encoding', value: 'UTF-8' },
    { key: 'hadoop.home.dir', value: '/opt/ozone/current/lib/hadoop-ozone' },
    { key: 'hadoop.id.str', value: 'hdds-hdfs' },
    { key: 'hadoop.log.dir', value: '/var/log/hadoop-ozone' },
    { key: 'hadoop.log.file', value: 'ozone.log' },
    { key: 'hadoop.root.logger', value: 'INFO,console' },
    { key: 'hadoop.security.logger', value: 'INFO,NullAppender' },
    { key: 'hadoop.policy.file', value: 'hadoop-policy.xml' },
    { key: 'proc_om', value: '' },
    { key: 'sun.java.command', value: 'org.apache.hadoop.ozone.om.OzoneManagerStarter' },
    {
      key: 'java.library.path',
      value: ':/opt/ozone/current/lib/hadoop-ozone/share/ozone/lib',
    },
    {
      key: 'org.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads',
      value: 'false',
    },
    { key: 'sun.security.krb5.disableReferrals', value: 'true' },
    { key: 'jdk.tls.ephemeralDHKeySize', value: '2048' },
  ],
  InputArguments: [
    '-Dproc_om',
    '-Dorg.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads=false',
    '-Xmx2511M',
    '-Xloggc:/var/log/hadoop-ozone/gc-OM-2026-07-27_18-49-49.log',
    '-verbose:gc',
    '-XX:+PrintGCDetails',
    '-XX:+PrintGCTimeStamps',
    '-XX:+PrintGCDateStamps',
    '-XX:+UseConcMarkSweepGC',
    '-XX:CMSInitiatingOccupancyFraction=70',
    '-XX:+CMSParallelRemarkEnabled',
    '-Dsun.security.krb5.disableReferrals=true',
    '-Djdk.tls.ephemeralDHKeySize=2048',
    '-Dcom.sun.management.jmxremote.ssl.enabled.protocols=TLSv1.2',
    '-XX:OnOutOfMemoryError=/opt/ozone/bin/oom-handler.sh',
    '-Dlog4j.configurationFile=/etc/hadoop-ozone/conf/om-audit-log4j2.properties',
    '-Djava.library.path=:/opt/ozone/current/lib/hadoop-ozone/share/ozone/lib',
    '-Dhadoop.log.dir=/var/log/hadoop-ozone',
    '-Dhadoop.log.file=ozone.log',
    '-Dhadoop.home.dir=/opt/ozone/current/lib/hadoop-ozone',
    '-Dhadoop.id.str=hdds-hdfs',
    '-Dhadoop.root.logger=INFO,console',
    '-Dhadoop.policy.file=hadoop-policy.xml',
    '-Dhadoop.security.logger=INFO,NullAppender',
  ],
  ObjectName: 'java.lang:type=Runtime',
};

const ratisRaftServer = {
  name: 'Ratis:service=RaftServer,group=group-0A1B2C3D4E5F,id=om1546336043',
  modelerType: 'org.apache.ratis.server.impl.RaftServerImpl$RaftServerJmxAdapter',
  Id: 'om1546336043',
  LeaderId: 'om1546336047',
  Role: ' FOLLOWER',
  Groups: ['group-0A1B2C3D4E5F'],
  Followers: [],
  CurrentTerm: 5,
  GroupId: 'group-0A1B2C3D4E5F',
};

const leaderElectionCount = {
  name: 'ratis:name=ratis.leader_election.om1546336043@group-0A1B2C3D4E5F.electionCount',
  modelerType: 'com.codahale.metrics.JmxReporter$JmxCounter',
  Count: 1,
};

const leaderElectionElapsed = {
  name: 'ratis:name=ratis.leader_election.om1546336043@group-0A1B2C3D4E5F.lastLeaderElectionElapsedTime',
  modelerType: 'com.codahale.metrics.JmxReporter$JmxGauge',
  Value: 78848822,
};

const deletingServiceMetrics = {
  name: 'Hadoop:service=OzoneManager,name=DeletingServiceMetrics',
  modelerType: 'DeletingServiceMetrics',
  'tag.Context': 'ozone',
  'tag.Hostname': 'node1.test.site.com',
  MetricsResetTimeStamp: 1785178221,
  KeysReclaimedInInterval: 0,
  ReclaimedSizeInInterval: 0,
  LastAOSPurgeTermId: 5,
  LastAOSPurgeTransactionId: 23245,
  NumKeysPurged: 1275,
};

// OM metrics bean (RPC operation counters + object counts). All keys use the exact
// JMX names OM exposes (field name with the first letter capitalised — see
// OMMetrics.java), so the mock exercises the parser the same way a live OM would.
// Notable cases the parser must handle correctly are called out inline.
const omMetrics = {
  name: 'Hadoop:service=OzoneManager,name=OMMetrics',
  modelerType: 'OMMetrics',
  'tag.Context': 'ozone',
  'tag.Hostname': 'node1.test.site.com',

  // Object counts (summary cards).
  NumVolumes: 1,
  NumBuckets: 2,
  NumKeys: 485,
  TotalDataCommitted: 62259,

  // Key — active, with a failing op (Delete) and a failure-only op (List). NumKeyOps
  // is OM's cross-category aggregate (spans Get/FSO/MPU too) and must NOT be used as
  // the per-type total nor shown as an operation.
  NumKeyOps: 2895,
  NumKeyAllocate: 965,
  NumKeyAllocateFails: 0,
  NumKeyCommits: 965,
  NumKeyCommitFails: 0,
  NumKeyDeletes: 965,
  NumKeyDeleteFails: 5,
  NumKeyHSyncs: 0,
  NumKeyLists: 0,
  NumKeyListFails: 100,
  NumKeyLookup: 0,
  NumKeyLookupFails: 0,
  NumKeyRenames: 0,
  NumKeyRenameFails: 0,

  // Get — service list plus GetKeyInfo, whose failure is exposed under the misspelled
  // name `GetNumGetKeyInfoFails` (no `Num` prefix); it must join the GetKeyInfo row.
  NumGetServiceLists: 990,
  NumGetKeyInfo: 1200,
  GetNumGetKeyInfoFails: 7,

  // Volume — CheckAccess has an -es plural (`NumVolumeCheckAccesses`) whose failure
  // (`NumVolumeCheckAccessFails`) must join it into a single row.
  NumVolumeOps: 24,
  NumVolumeCreates: 1,
  NumVolumeCreateFails: 0,
  NumVolumeInfos: 20,
  NumVolumeInfoFails: 0,
  NumVolumeLists: 3,
  NumVolumeListFails: 0,
  NumVolumeCheckAccesses: 30,
  NumVolumeCheckAccessFails: 4,

  // Bucket.
  NumBucketOps: 60,
  NumBucketCreates: 2,
  NumBucketCreateFails: 0,
  NumBucketInfos: 48,
  NumBucketInfoFails: 0,
  NumBucketLists: 10,
  NumBucketListFails: 0,

  // List.
  NumListStatus: 45,
  NumListStatusFails: 0,

  // Snapshot — request counters plus state/gauge counts. NumSnapshotActive/Deleted/
  // CacheSize describe snapshot state, not requests, and must be excluded from the
  // total (OM does not expose any `NumSnapshotOps`).
  NumSnapshotCreates: 8,
  NumSnapshotCreateFails: 1,
  NumSnapshotDeletes: 3,
  NumSnapshotDeleteFails: 0,
  NumSnapshotActive: 5,
  NumSnapshotDeleted: 2,
  NumSnapshotCacheSize: 4,

  // Trash — `NumTrashFails` is OM's single aggregate trash-processing failure
  // counter (no matching request); it must show as a warning failure row, not a
  // request named "Fails".
  NumTrashRenames: 12,
  NumTrashDeletes: 6,
  NumTrashFails: 3,

  // ACL operations (Add / Set / Remove). OM exposes no ACL failure counters.
  NumAddAcl: 40,
  NumSetAcl: 15,
  NumRemoveAcl: 5,

  // Multipart upload (Initiate / Commit / Complete / Abort) — Commit is failing.
  NumInitiateMultipartUploads: 20,
  NumInitiateMultipartUploadFails: 0,
  NumCommitMultipartUploadParts: 18,
  NumCommitMultipartUploadPartFails: 2,
  NumCompleteMultipartUploads: 18,
  NumCompleteMultipartUploadFails: 0,
  NumAbortMultipartUploads: 2,
  NumAbortMultipartUploadFails: 0,

  // Ratis read-path counters (previously dropped): Linearizable / Leader / Follower.
  NumLinearizableRead: 5000,
  NumLeaderSkipLinearizableRead: 120,
  NumFollowerReadLocalLeaseSuccess: 800,
};

/**
 * Ordered match table. The mock server picks the first entry whose `test`
 * matches the requested `qry` and returns `{ beans }`.
 */
module.exports = [
  { test: /component=ServerRuntime/i, beans: [ozoneManagerInfo] },
  { test: /java\.lang:type=Runtime/i, beans: [runtime] },
  { test: /service=RaftServer/i, beans: [ratisRaftServer] },
  { test: /electionCount/i, beans: [leaderElectionCount] },
  { test: /lastLeaderElectionElapsedTime/i, beans: [leaderElectionElapsed] },
  { test: /name=OMMetrics/i, beans: [omMetrics] },
  { test: /DeletingServiceMetrics/i, beans: [deletingServiceMetrics] },
];
