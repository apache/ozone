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
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.HashMap;

import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.FILE_DIR_SEPARATOR;

/**
 * Ozone key generator/reader for performance test.
 */

@CommandLine.Command(name = "ockrw",
        aliases = "ozone-client-key-read-write-ops",
        description = "Generate keys with a fixed name and ranges that can" 
        + " be written and read as sub-ranges from multiple clients.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OzoneClientKeyReadWriteOps extends BaseFreonGenerator
        implements Callable<Void> {

  @CommandLine.Option(names = {"-v", "--volume"},
          description = "Name of the volume which contains the test data. " +
                  "Will be created if missing.",
          defaultValue = "ockrwvolume")
  private String volumeName;

  @CommandLine.Option(names = {"-b", "--bucket"},
          description = "Name of the bucket which contains the test data.",
          defaultValue = "ockrwbucket")
  private String bucketName;

  @CommandLine.Option(names = {"-m", "--read-metadata-only"},
          description = "If only read key's metadata.",
          defaultValue = "false")
  private boolean readMetadataOnly;

  @CommandLine.Option(names = {"-s", "--start-index"},
          description = "Start index of keys of read/write operation." + 
          "This can allow adding keys incrementally or parallel from multiple"
          + " clients. Example: Write keys 0-1000000 followed by "
          + "keys 1000001-2000000.",
          defaultValue = "0")
  private int startIndex;

  @CommandLine.Option(names = {"-r", "--range"},
          description = "Range of read/write operations. This in co-ordination"
          + " with --start-index can specify the range to read. "
          + "Example: Read from --start-index 1000 and read --range 1000 keys.",
          defaultValue = "1")
  private int range;

  @CommandLine.Option(names = {"--size"},
          description = "Object size (in bytes) " +
                  "to read/write. If user sets a read size which is larger"
                  + " than the key size, it only reads bytes up to key size.",
          defaultValue = "1")
  private int objectSizeInBytes;

  @CommandLine.Option(names = {"--contiguous"},
          description = "By default, the keys are randomized lexically" 
          + " by calculating the md5 of the key name. If this option is set," 
          + " the keys are written lexically contiguously.",
          defaultValue = "false")
  private boolean keySorted;

  @CommandLine.Option(names = {"--percentage-read"},
          description = "Percentage of read tasks in mix workload."
          + " The remainder of the percentage will writes to keys."
          + " Example --percentage-read 90 will result in 10% writes.",
          defaultValue = "100")
  private int percentageRead;

  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;

  private Timer timer;

  private OzoneClient[] ozoneClients;

  private int clientCount;

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
    clientCount =  getThreadNo();
    ozoneClients = new OzoneClient[clientCount];
    for (int i = 0; i < clientCount; i++) {
      ozoneClients[i] = createOzoneClient(omServiceID, ozoneConfiguration);
    }

    ensureVolumeAndBucketExist(ozoneClients[0], volumeName, bucketName);

    timer = getMetrics().timer("key-read-write");
    if (objectSizeInBytes >= 0) {
      keyContent = RandomUtils.nextBytes(objectSizeInBytes);
    }
    if (kg == null) {
      kg = new KeyGeneratorUtil();
    }
    runTests(this::readWriteKeys);

    for (int i = 0; i < clientCount; i++) {
      if (ozoneClients[i] != null) {
        ozoneClients[i].close();
      }
    }
    return null;
  }

  public void readWriteKeys(long counter) throws RuntimeException, IOException {
    int clientIndex = (int)((counter) % clientCount);
    TaskType taskType = decideReadOrWriteTask();
    String keyName = getKeyName();

    timer.time(() -> {
      try {
        switch (taskType) {
        case READ_TASK:
          processReadTasks(keyName, ozoneClients[clientIndex]);
          break;
        case WRITE_TASK:
          processWriteTasks(keyName, ozoneClients[clientIndex]);
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

  public void processReadTasks(String keyName, OzoneClient client)
          throws RuntimeException, IOException {
    OzoneKeyDetails keyDetails = client.getProxy().getKeyDetails(volumeName, bucketName, keyName);            
    if (!readMetadataOnly) {
      byte[] data = new byte[objectSizeInBytes];
      try (OzoneInputStream introStream = keyDetails.getContent()) {
         introStream.read(data);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  public void processWriteTasks(String keyName, OzoneClient ozoneClient)
          throws RuntimeException, IOException {
    try (OzoneOutputStream out =
                 ozoneClient.getProxy().createKey(volumeName, bucketName, keyName, objectSizeInBytes, null, new HashMap())) {
      out.write(keyContent);
    } catch (Exception ex) {
      throw ex;
    }
  }

  public TaskType decideReadOrWriteTask() {
    if (percentageRead == 100) {
      return TaskType.READ_TASK;
    } else if (percentageRead == 0) {
      return TaskType.WRITE_TASK;
    }
    //mix workload
    int tmp = ThreadLocalRandom.current().nextInt(1,101);
    if (tmp  <= percentageRead) {
      return TaskType.READ_TASK;
    } else {
      return TaskType.WRITE_TASK;
    }
  }

  public String getKeyName() {
    StringBuilder keyNameSb = new StringBuilder();
    int randomIdxWithinRange = ThreadLocalRandom.current().
            nextInt(startIndex, startIndex + range);

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
