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

import java.io.IOException;
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
  @CommandLine.Option(names = {"-s", "--start-index"},
          description = "Start index of keys of read/write operation.",
          defaultValue = "0")
  private int startIndex;

  @CommandLine.Option(names = {"-r", "--range"},
          description = "index range of read/write operations.",
          defaultValue = "0")
  private int range;
  @CommandLine.Option(names = {"--size"},
          description = "Object size (in bytes) " +
                  "to be generated.",
          defaultValue = "1")
  private int objectSizeInBytes;

  @CommandLine.Option(names = {"--keySorted"},
          description = "Generated sorted key or not. The key name " +
                  "will be generated via md5 hash if choose " +
                  "to use unsorted key.",
          defaultValue = "false")
  private boolean keySorted;

  @CommandLine.Option(names = {"--percentage-read"},
          description = "Percentage of read tasks in mix workload.",
          defaultValue = "100")
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

  private Timer timer;

  private OzoneBucket[] ozoneBuckets;

  private byte[] keyContent;

  private static final Logger LOG =
          LoggerFactory.getLogger(OzoneClientKeyReadWriteOps.class);

  /**
   * Task type of read task, or write task.
   */
  public enum TaskType {
    READ_TASK,
    WRITE_TASK
  }
  private KeyGeneratorUtil kg;


  @Override
  public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    OzoneClient[] ozoneClients = new OzoneClient[clientsCount];
    for (int i = 0; i < clientsCount; i++) {
      ozoneClients[i] = createOzoneClient(omServiceID, ozoneConfiguration);
    }

    ensureVolumeAndBucketExist(ozoneClients[0], volumeName, bucketName);
    ozoneBuckets = new OzoneBucket[clientsCount];
    for (int i = 0; i < clientsCount; i++) {
      ozoneBuckets[i] = ozoneClients[i].getObjectStore().getVolume(volumeName)
              .getBucket(bucketName);
    }

    timer = getMetrics().timer("key-read-write");
    if (objectSizeInBytes >= 0) {
      keyContent = RandomUtils.nextBytes(objectSizeInBytes);
    }
    if (kg == null) {
      kg = new KeyGeneratorUtil();
    }
    runTests(this::readWriteKeys);

    for (int i = 0; i < clientsCount; i++) {
      if (ozoneClients[i] != null) {
        ozoneClients[i].close();
      }
    }
    return null;
  }

  public void readWriteKeys(long counter) throws Exception {
    int clientIndex = (int)((counter) % clientsCount);
    OzoneBucket ozoneBucket = ozoneBuckets[clientIndex];
    TaskType taskType = decideReadOrWriteTask();
    String keyName = getKeyName(clientIndex);

    timer.time(() -> {
      try {
        switch (taskType) {
        case READ_TASK:
          processReadTasks(keyName, ozoneBucket);
          break;
        case WRITE_TASK:
          processWriteTasks(keyName, ozoneBucket);
          break;
        default:
          break;
        }
      } catch (RuntimeException ex) {
        LOG.error(ex.getMessage());
        throw ex;
      } catch (IOException ex) {
        LOG.error(ex.getMessage());
        throw new RuntimeException(ex.getMessage());
      }

    });
  }

  public void processReadTasks(String keyName, OzoneBucket ozoneBucket)
          throws RuntimeException, IOException {
    if (readMetadataOnly) {
      ozoneBucket.getKey(keyName);
    } else {
      byte[] data = new byte[objectSizeInBytes];
      try (OzoneInputStream inputStream = ozoneBucket.readKey(keyName)) {
        inputStream.read(data);
      }
    }
  }
  public void processWriteTasks(String keyName, OzoneBucket ozoneBucket)
          throws RuntimeException, IOException {
    try (OzoneOutputStream out =
                 ozoneBucket.createKey(keyName, objectSizeInBytes)) {
      out.write(keyContent);
    } catch (Exception ex) {
      throw ex;
    }
  }
  public TaskType decideReadOrWriteTask() {
    if (!isMixWorkload()) {
      if (percentageRead == 100) {
        return TaskType.READ_TASK;
      } else {
        return TaskType.WRITE_TASK;
      }
    }
    //mix workload
    int tmp = ThreadLocalRandom.current().nextInt(100) + 1; // 1 ~ 100
    if (tmp < percentageRead) {
      return TaskType.READ_TASK;
    } else {
      return TaskType.WRITE_TASK;
    }
  }

  public String getKeyName(int clientIndex) {
    int start, end;
    // separate tasks evenly to each client
    if (range < clientsCount) {
      start = startIndex + clientIndex;
      end = start;
    } else {
      start = startIndex + clientIndex * (range / clientsCount);
      end = start + (range / clientsCount) - 1;
    }

    StringBuilder keyNameSb = new StringBuilder();
    int randomIdxWithinRange = ThreadLocalRandom.current().
            nextInt(end + 1 - start) + start;
    if (keySorted) {
      keyNameSb.append(getPrefix()).append(FILE_DIR_SEPARATOR).
              append(randomIdxWithinRange);
    } else {
      keyNameSb.append(getPrefix()).append(FILE_DIR_SEPARATOR).
              append(kg.generateMd5KeyName(randomIdxWithinRange));
    }
    return keyNameSb.toString();
  }

  public boolean isMixWorkload() {
    return !(percentageRead == 0 || percentageRead == 100);
  }

}
