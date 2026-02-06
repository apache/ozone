/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import static java.util.Collections.emptyMap;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs.Builder;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "ommg",
    aliases = "om-metadata-generator",
    description = "Create metadata operation to the OM.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OmMetadataGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the volume which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-s", "--size"},
      description = "The size in byte of a file for the Create File/Key op. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "0",
      converter = StorageSizeConverter.class)
  private StorageSize dataSize;

  @Option(names = {"--buffer"},
      description = "Size of buffer used to generated the key content.",
      defaultValue = "4096")
  private int bufferSize;

  @Option(
      names = "--batch-size",
      description = "The number of key/file requests per LIST_KEY/LIST_STATUS"
          + " request.",
      defaultValue = "1000")
  private int batchSize;

  @Option(
      names = "--random",
      description = "random read/write if given. This means that it"
          + " is possible to read/write the same file at the same time",
      defaultValue = "false")
  private boolean randomOp;

  @Option(names = {"-o", "--operation"},
      description = "The operation to perform, --ophelp Print detail")
  private Operation operation;

  @Option(names = {"--ops"},
      description = "The operations list to perform, --ophelp Print detail")
  private String operationsList;

  @Option(names = {"--opsnum"},
      description = "The number of threads per operations, the values sum"
          + " must equal the number of threads, --ophelp Print detail")
  private String operationsNum;

  @Option(names = {"--ophelp"},
      description = "Print operation help, or list available operation")
  private boolean opHelp;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID;

  @Option(
      names = "--clients",
      description = "The number of Ozone clients used. If zero or less, " +
          "will fallback to 1",
      defaultValue = "1")
  private int clientsNo;

  @Option(
      names = "--enable-follower-read-affinity",
      description = "Only useful when OM follower reads is enabled. " +
          "If this is enabled, the clients will be initialized to only point " +
          "to the OM followers in a round-robin fashion. If disabled, " +
          "each client points to a random OM node, including leader.",
      defaultValue = "false"
  )
  private boolean enableFollowerAffinity;

  @Mixin
  private FreonReplicationOptions replication;

  private ThreadLocal<OmKeyArgs.Builder> omKeyArgsBuilder;
  private OzoneClient[] ozoneClients;
  private ContentGenerator contentGenerator;
  private final byte[] readBuffer = new byte[4096];
  private ReplicationConfig replicationConfig;
  private Operation[] operations;
  private boolean mixedOperation = false;

  @Override
  public Void call() throws Exception {
    if (opHelp || operation == null) {
      System.out.println(getUsage());
      return null;
    }
    if (operation.equals(Operation.MIXED)) {
      initMixedOperation();
      mixedOperation = true;
    }
    init();
    contentGenerator = new ContentGenerator(dataSize.toBytes(), bufferSize);
    omKeyArgsBuilder = ThreadLocal.withInitial(this::createKeyArgsBuilder);
    OzoneConfiguration conf = createOzoneConfiguration();
    replicationConfig = replication.fromParamsOrConfig(conf);

    if (clientsNo <= 0) {
      clientsNo = 1;
    }

    try (OzoneClient ozClient = createOzoneClient(omServiceID, conf)) {
      ensureVolumeAndBucketExist(ozClient, volumeName, bucketName);

      ozoneClients = new OzoneClient[clientsNo];
      List<String> followerOMNodeIds = null;
      int currentFollowerAffinityIndex = 0;
      if (enableFollowerAffinity) {
        followerOMNodeIds = getOMFollowerNodeIds(ozClient);
      }
      for (int i = 0; i < clientsNo; i++) {
        OzoneClient ozoneClient = createOzoneClient(omServiceID, conf);
        if (enableFollowerAffinity) {
          // Point the client proxy to the followers in a round-robin fashion
          // This balances the read loads on the OM followers
          changeInitialProxyForFollowerRead(ozoneClient,
              followerOMNodeIds.get(currentFollowerAffinityIndex));
          currentFollowerAffinityIndex = (currentFollowerAffinityIndex + 1) % followerOMNodeIds.size();
        }
        ozoneClients[i] = ozoneClient;
      }
      runTests(this::applyOperation);
    } finally {
      if (ozoneClients != null) {
        for (OzoneClient ozoneClient : ozoneClients) {
          if (ozoneClient != null) {
            ozoneClient.close();
          }
        }
      }
      omKeyArgsBuilder.remove();
    }

    return null;
  }

  private void initMixedOperation() {
    if (operationsList == null || operationsNum == null) {
      throw new IllegalArgumentException(
          "--ops and --opsnum must be given, if --operation is MIXED");
    }
    List<Operation> ops =
        Arrays.stream(operationsList.split(",")).map(Operation::valueOf)
            .collect(Collectors.toList());
    List<Integer> opsNum =
        Arrays.stream(operationsNum.split(",")).map(Integer::valueOf)
            .collect(Collectors.toList());
    if (ops.size() != opsNum.size()
        || opsNum.stream().mapToInt(x -> x).sum() != getThreadNo()) {
      throw new IllegalArgumentException(
          "the --opsnum values sum must equal the number of threads");
    }

    int index = 0;
    // if --ops is A,B,C --opsnum is 3,2,1
    // so the operations will be [A, A, A, B, B, C]
    // so the thread with seq id [0, 2] will execute A,
    // the thread with seq id [3, 4] will execute B,
    // the thread with seq id [5, 5] will execute C
    operations = new Operation[getThreadNo()];
    for (int i = 0; i < ops.size(); i++) {
      Operation op = ops.get(i);
      int num = opsNum.get(i);
      for (int j = 0; j < num; j++) {
        operations[index] = op;
        index++;
      }
    }
  }

  public static String getUsage() {
    return String.join("\n", ImmutableList.of(
        "A tool to measure the Ozone om performance",
        "support Operation: ",
        "  " + EnumSet.allOf(Operation.class).stream().map(Enum::toString)
            .collect(Collectors.joining(", ")),
        "\nExample: ",
        "# create 25000 keys, run time 180s",
        "$ bin/ozone freon ommg --operation CREATE_KEY -n 25000"
            + " --duration  180s\n",
        "# read 25000 keys, run time 180s",
        "$ bin/ozone freon ommg --operation READ_KEY -n 25000"
            + " --duration 180s\n",
        "# 20 threads, list 1000 keys each request, and run time 180s",
        "$ bin/ozone freon ommg --operation LIST_KEYS -t 20 --batch-size 1000"
            + " --duration 180s\n",
        "# 10 threads, 1 threads list keys, 5 threads create file,"
            + " 4 threads lookup file and run time 180s",
        "$ bin/ozone freon ommg"
            + " --operation MIXED --ops CREATE_FILE,LOOKUP_FILE,LIST_STATUS"
            + " --opsnum 5,4,1 -t 10 -n 1000 --duration 180s\n",
        "Note that: You must create a sufficient number of "
            + "objects before executing read-related tests\n"
    ));
  }

  private OmKeyArgs.Builder createKeyArgsBuilder() {
    UserGroupInformation ugi = null;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setReplicationConfig(replicationConfig)
        .setLocationInfoList(new ArrayList<>())
        .setAcls(OzoneAclUtil.getAclList(ugi, ALL, ALL));
  }

  private String getPath(long counter) {
    // Ensure that the dictionary order of path String
    // is the same as the order of numeric.
    // This is useful for LIST_KEYS/LIST_STATUS.
    // The file "0..0001" + 1000 will be "0..1001"
    //
    // The size is 19, because the decimal long type can have up to 19 digits
    return StringUtils.leftPad(String.valueOf(counter), 19, '0');
  }

  @Override
  public Supplier<String> realTimeStatusSupplier() {
    final Map<String, Long> maxValueRecorder = new HashMap<>();
    final Map<String, Long> valueRecorder = new HashMap<>();
    final Map<String, Instant> instantsRecorder = new HashMap<>();
    return () -> {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, Timer> entry
          : getMetrics().getTimers(MetricFilter.ALL).entrySet()) {
        String name = entry.getKey();
        long maxValue = maxValueRecorder.getOrDefault(name, -1L);
        long preValue = valueRecorder.getOrDefault(name, 0L);
        Instant preInstant = instantsRecorder.getOrDefault(name, Instant.now());

        long curValue = entry.getValue().getCount();
        Instant now =  Instant.now();
        long duration = Duration.between(preInstant, now).getSeconds();
        long rate = ((curValue - preValue) / (duration == 0 ? 1 : duration));
        maxValue = Math.max(rate, maxValue);

        maxValueRecorder.put(name, maxValue);
        valueRecorder.put(name, curValue);
        instantsRecorder.put(name, now);
        sb.append(' ')
            .append(name)
            .append(": rate ")
            .append(rate)
            .append(" max ")
            .append(maxValue);
      }
      sb.append("  ");
      return sb.toString();
    };
  }

  private void applyOperation(long counter) throws Exception {
    OmKeyArgs keyArgs;
    final long threadSeqId = getThreadSequenceId();
    String startKeyName;
    if (mixedOperation) {
      operation = operations[(int)threadSeqId];
    }
    if (randomOp) {
      counter = ThreadLocalRandom.current().nextLong(getTestNo());
    }
    final String keyName = getPath(counter);
    // Use counter instead of thread sequence ID so that a single operation can be
    // executed by different clients in mixed operations scenario
    final OzoneClient ozoneClient = getOzoneClient(counter);
    final ClientProtocol clientProtocol = ozoneClient.getProxy();
    final OzoneManagerProtocol ozoneManagerClient = clientProtocol.getOzoneManagerClient();
    switch (operation) {
    case CREATE_KEY:
      getMetrics().timer(operation.name()).time(() -> performWriteOperation(() ->
          clientProtocol.createKey(volumeName, bucketName, keyName, dataSize.toBytes(),
              replicationConfig, emptyMap()), contentGenerator));
      break;
    case CREATE_STREAM_KEY:
      getMetrics().timer(operation.name()).time(() -> performWriteOperation(() ->
          clientProtocol.createStreamKey(volumeName, bucketName, keyName, dataSize.toBytes(),
              replicationConfig, emptyMap()), contentGenerator));
      break;
    case LOOKUP_KEY:
      keyArgs = omKeyArgsBuilder.get().setKeyName(keyName).build();
      getMetrics().timer(operation.name()).time(() -> ozoneManagerClient.lookupKey(keyArgs));
      break;
    case GET_KEYINFO:
      keyArgs = omKeyArgsBuilder.get().setKeyName(keyName).build();
      getMetrics().timer(operation.name()).time(() -> ozoneManagerClient.getKeyInfo(keyArgs, false));
      break;
    case HEAD_KEY:
      keyArgs = omKeyArgsBuilder.get()
          .setKeyName(keyName).setHeadOp(true).build();
      getMetrics().timer(operation.name()).time(() -> ozoneManagerClient.getKeyInfo(keyArgs, false));
      break;
    case READ_KEY:
      getMetrics().timer(operation.name()).time(() -> performReadOperation(() ->
          clientProtocol.getKey(volumeName, bucketName, keyName), readBuffer));
      break;
    case READ_FILE:
      getMetrics().timer(operation.name()).time(() -> performReadOperation(() ->
          clientProtocol.readFile(volumeName, bucketName, keyName), readBuffer));
      break;
    case CREATE_FILE:
      getMetrics().timer(operation.name()).time(() -> performWriteOperation(() ->
          clientProtocol.createFile(volumeName, bucketName, keyName, dataSize.toBytes(),
              replicationConfig, true, false), contentGenerator));
      break;
    case CREATE_STREAM_FILE:
      getMetrics().timer(operation.name()).time(() -> performWriteOperation(() ->
          clientProtocol.createStreamFile(volumeName, bucketName, keyName, dataSize.toBytes(),
              replicationConfig, true, false), contentGenerator));
      break;
    case LOOKUP_FILE:
      keyArgs = omKeyArgsBuilder.get().setKeyName(keyName).build();
      getMetrics().timer(operation.name()).time(() -> ozoneManagerClient.lookupFile(keyArgs));
      break;
    case LIST_KEYS:
      startKeyName = getPath(threadSeqId * batchSize);
      getMetrics().timer(operation.name()).time(() -> {
        List<OmKeyInfo> keyInfoList =
            ozoneManagerClient.listKeys(volumeName, bucketName, startKeyName, "", batchSize).getKeys();
        if (keyInfoList.size() + 1 < batchSize) {
          throw new NoSuchFileException("There are not enough keys for testing you should use "
                  + "CREATE_KEY to create at least batch-size * threads = " + batchSize * getThreadNo());
        }
        return null;
      });
      break;
    case LIST_KEYS_LIGHT:
      startKeyName = getPath(threadSeqId * batchSize);
      getMetrics().timer(operation.name()).time(() -> {
        List<BasicOmKeyInfo> keyInfoList =
            ozoneManagerClient.listKeysLight(volumeName, bucketName, startKeyName, "", batchSize).getKeys();
        if (keyInfoList.size() + 1 < batchSize) {
          throw new NoSuchFileException("There are not enough keys for testing you should use "
                  + "CREATE_KEY to create at least batch-size * threads = " + batchSize * getThreadNo());
        }
        return null;
      });
      break;
    case LIST_STATUS:
      startKeyName = getPath(threadSeqId * batchSize);
      keyArgs = omKeyArgsBuilder.get().setKeyName("").build();
      getMetrics().timer(operation.name()).time(() -> {
        List<OzoneFileStatus> fileStatusList = ozoneManagerClient.listStatus(
            keyArgs, false, startKeyName, batchSize);
        if (fileStatusList.size() + 1 < batchSize) {
          throw new NoSuchFileException("There are not enough files for testing you should use "
                  + "CREATE_FILE to create at least batch-size * threads = " + batchSize * getThreadNo());
        }
        return null;
      });
      break;
    case LIST_STATUS_LIGHT:
      startKeyName = getPath(threadSeqId * batchSize);
      keyArgs = omKeyArgsBuilder.get().setKeyName("").build();
      getMetrics().timer(operation.name()).time(() -> {
        List<OzoneFileStatusLight> fileStatusList = ozoneManagerClient.listStatusLight(
            keyArgs, false, startKeyName, batchSize, false);
        if (fileStatusList.size() + 1 < batchSize) {
          throw new NoSuchFileException("There are not enough files for testing you should use "
              + "CREATE_FILE to create at least batch-size * threads = " + batchSize * getThreadNo());
        }
        return null;
      });
      break;
    case INFO_BUCKET:
      getMetrics().timer(operation.name()).time(() -> ozoneManagerClient.getBucketInfo(volumeName, bucketName)
      );
      break;
    case INFO_VOLUME:
      getMetrics().timer(operation.name()).time(() -> ozoneManagerClient.getVolumeInfo(volumeName));
      break;
    default:
      throw new IllegalStateException("Unrecognized write command " +
          "type request " + operation);
    }
  }

  private OzoneClient getOzoneClient(long counter) {
    int index = (int) (counter % ozoneClients.length);
    return ozoneClients[index];
  }

  @FunctionalInterface
  interface WriteOperation {
    OutputStream createStream() throws IOException;
  }

  @FunctionalInterface
  interface ReadOperation {
    InputStream createStream() throws IOException;
  }

  private Void performWriteOperation(WriteOperation writeOp, ContentGenerator contentGen) throws IOException {
    try (OutputStream stream = writeOp.createStream()) {
      contentGen.write(stream);
    }
    return null;
  }

  @SuppressWarnings("checkstyle:EmptyBlock")
  private Void performReadOperation(ReadOperation readOp, byte[] buffer) throws IOException {
    try (InputStream stream = readOp.createStream()) {
      while (stream.read(buffer) >= 0) {
      }
      return null;
    }
  }

  @Override
  public boolean allowEmptyPrefix() {
    return true;
  }

  enum Operation {
    CREATE_FILE,
    CREATE_STREAM_FILE,
    LOOKUP_FILE,
    READ_FILE,
    LIST_STATUS,
    LIST_STATUS_LIGHT,
    CREATE_KEY,
    CREATE_STREAM_KEY,
    LOOKUP_KEY,
    GET_KEYINFO,
    HEAD_KEY,
    READ_KEY,
    LIST_KEYS,
    LIST_KEYS_LIGHT,
    INFO_BUCKET,
    INFO_VOLUME,
    MIXED,
  }

  private List<String> getOMFollowerNodeIds(OzoneClient ozoneClient) throws IOException {
    List<OMRoleInfo> omRoleInfos = ozoneClient.getProxy().getOmRoleInfos();
    String leaderOMNodeId = null;
    List<String> followerOMNodeIds = new ArrayList<>();
    for (OMRoleInfo omRoleInfo : omRoleInfos) {
      if (omRoleInfo.getServerRole().equals("LEADER")) {
        if (leaderOMNodeId != null) {
          // This situation should be rare
          throw new IllegalStateException("There are more than one leader detected, please retry again");
        }
        leaderOMNodeId = omRoleInfo.getNodeId();
      } else if (omRoleInfo.getServerRole().equals("FOLLOWER")) {
        followerOMNodeIds.add(omRoleInfo.getNodeId());
      }
    }
    if (followerOMNodeIds.isEmpty()) {
      throw new IllegalArgumentException("There is no follower in the OM service, please retry again");
    }
    return followerOMNodeIds;
  }

  private void changeInitialProxyForFollowerRead(OzoneClient ozoneClient, String omNodeId) {
    OzoneManagerProtocolClientSideTranslatorPB ozoneManagerClient =
        (OzoneManagerProtocolClientSideTranslatorPB)
            ozoneClient.getProxy().getOzoneManagerClient();

    OmTransport transport = ozoneManagerClient.getTransport();

    if (transport instanceof Hadoop3OmTransport) {
      Hadoop3OmTransport hadoop3OmTransport =
          (Hadoop3OmTransport) ozoneManagerClient.getTransport();
      HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider =
          hadoop3OmTransport.getOmFollowerReadFailoverProxyProvider();

      if (followerReadFailoverProxyProvider != null) {
        followerReadFailoverProxyProvider.changeInitialProxyForTest(omNodeId);
      }
    }
  }
}
