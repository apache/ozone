/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;


import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.FILE_DIR_SEPARATOR;

/**
 * Ozone key generator/reader for performance test.
 */

@CommandLine.Command(name = "ockrw",
        aliases = "ozone-client-key-read-write-ops",
        description = "Read and write keys with the help of the ozone clients.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OzoneClientKeyReadWriteOps extends BaseFreonGenerator
        implements Callable<Void> {

  @CommandLine.Option(names = {"-v", "--volume"},
          description = "Name of the volume which contains the test data. " +
                  "Will be created if missing.",
          defaultValue = "vol1")
  private String volumeName;

  @CommandLine.Option(names = {"-b", "--bucket"},
          description = "Name of the bucket which contains the test data.",
          defaultValue = "bucket1")
  private String bucketName;

  @CommandLine.Option(names = {"-m", "--read-metadata-only"},
          description = "If only read key's metadata. " +
                  "Supported values are Y, F.",
          defaultValue = "false")
  private boolean readMetadataOnly;

  @CommandLine.Option(names = {"-r", "--range-client-read"},
          description = "range of read operation of each client.",
          defaultValue = "0")
  private int readRange;


  @CommandLine.Option(names = {"-w", "--range-client-write"},
          description = "range of write operation of each client.",
          defaultValue = "0")
  private int writeRange;

//    @CommandLine.Option(names = {"-e", "--end-index-each-client-read"},
//          description = "end-index of each client's read operation.",
//          defaultValue = "0")
//  private int endIndexForRead;

//  @CommandLine.Option(names = {"-j", "--end-index-each-client-write"},
//          description = "end-index of each client's write operation.",
//          defaultValue = "0")
//  private int endIndexForWrite;

  @CommandLine.Option(names = {"-g", "--size"},
          description = "Generated data size (in bytes) of " +
                  "each key/file to be " +
                  "written.",
          defaultValue = "256")
  private int writeSizeInBytes;

  @CommandLine.Option(names = {"-k", "--keySorted"},
          description = "Generated sorted key or not. The key name " +
                  "will be generated via md5 hash if choose " +
                  "to use unsorted key.",
          defaultValue = "false")
  private boolean keySorted;

  @CommandLine.Option(names = {"-x", "--mix-workload"},
          description = "Set to True if you would like to " +
                  "generate mix workload (Read and Write).",
          defaultValue = "false")
  private boolean isMixWorkload;

  @CommandLine.Option(names = {"--percentage-read"},
          description = "Percentage of read tasks in mix workload.",
          defaultValue = "0")
  private int percentageRead;

  @CommandLine.Option(names = {"--clients"},
          description =
                  "Number of clients, defaults 1.",
          defaultValue = "1")
  private int clientsCount = 1;

  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;

  @CommandLine.Option(
          names = "--debug",
          description = "Enable debugging message.",
          defaultValue = "false"
  )
  private boolean debug;

  private Timer timer;

  private OzoneClient[] rpcClients;

  private byte[] keyContent;

  private static final Logger LOG =
          LoggerFactory.getLogger(OzoneClientKeyReadWriteOps.class);

  private final String readTask = "READ_TASK";
  private final String writeTask = "WRITE_TASK";
  private KeyGeneratorUtil kg;

  @Override
  public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    rpcClients = new OzoneClient[clientsCount];
    for (int i = 0; i < clientsCount; i++) {
      rpcClients[i] = createOzoneClient(omServiceID, ozoneConfiguration);
    }

    ensureVolumeAndBucketExist(rpcClients[0], volumeName, bucketName);
    timer = getMetrics().timer("key-read-write");
    if (writeSizeInBytes >= 0) {
      keyContent = RandomUtils.nextBytes(writeSizeInBytes);
    }
    if (kg == null) {
      kg = new KeyGeneratorUtil();
    }
    runTests(this::readWriteKeys);

    for (int i = 0; i < clientsCount; i++) {
      if (rpcClients[i] != null) {
        rpcClients[i].close();
      }
    }
    return null;
  }

  public void readWriteKeys(long counter) throws Exception {
    int clientIndex = (int)((counter) % clientsCount);
    if (debug) {
      LOG.error("*** *** *** counter = " +
              counter + ", clientIndex = " + clientIndex);
    }

    OzoneClient client = rpcClients[clientIndex];
    String operationType = decideReadOrWriteTask();
    String keyName = getKeyName(operationType, clientIndex);
    timer.time(() -> {
      try {
        switch (operationType) {
        case readTask:
          processReadTasks(keyName, client);
          break;
        case writeTask:
          processWriteTasks(keyName, client);
          break;
        default:
          break;
        }
      } catch (Exception ex) {
        LOG.error(ex.getMessage());
      }
    });
  }

  public void processReadTasks(String keyName, OzoneClient client)
          throws Exception {
    OzoneBucket ozbk = client.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    if (readMetadataOnly) {
      ozbk.getKey(keyName);
    } else {
      byte[] data = new byte[writeSizeInBytes];
      try (OzoneInputStream introStream = ozbk.readKey(keyName)) {
        int readBytes = introStream.read(data);
      }
    }
  }
  public void processWriteTasks(String keyName, OzoneClient client)
          throws Exception {
    OzoneBucket ozbk = client.getObjectStore().getVolume(volumeName)
            .getBucket(bucketName);

    try (OzoneOutputStream out = ozbk.createKey(keyName, writeSizeInBytes)) {
      out.write(keyContent);
      out.flush();
    }
  }
  public String decideReadOrWriteTask() {
    if (!isMixWorkload) {
      if (readRange > 0) {
        return readTask;
      } else if (writeRange > 0) {
        return writeTask;
      }
    }
    //mix workload
    int tmp = ThreadLocalRandom.current().nextInt(100) + 1; // 1 ~ 100
    if (tmp < percentageRead) {
      return readTask;
    } else {
      return writeTask;
    }
  }

  public String getKeyName(String operationType, int clientIndex) {
    int startIdx, endIdx;
    switch (operationType) {
    case readTask:
      // separate tasks evenly to each client
      startIdx = clientIndex * (readRange / clientsCount);
      endIdx = startIdx + (readRange / clientsCount) - 1;
      break;
    case writeTask:
      // separate tasks evenly to each client
      startIdx = clientIndex * (writeRange / clientsCount);
      endIdx = startIdx + (writeRange / clientsCount) - 1;
      break;
    default:
      startIdx = 0;
      endIdx = 0;
      break;
    }
    StringBuilder keyNameSb = new StringBuilder();
    int randomIdxWithinRange = ThreadLocalRandom.current().
            nextInt(endIdx + 1 - startIdx) + startIdx;
    if (keySorted) {
      keyNameSb.append(getPrefix()).append(FILE_DIR_SEPARATOR).
              append(randomIdxWithinRange);
    } else {
      keyNameSb.append(getPrefix()).append(FILE_DIR_SEPARATOR).
              append(kg.generateMd5KeyName(randomIdxWithinRange));
    }
    return keyNameSb.toString();
  }

}
