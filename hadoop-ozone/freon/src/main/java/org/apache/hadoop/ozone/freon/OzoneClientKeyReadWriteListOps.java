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

import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.FILE_DIR_SEPARATOR;

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Ozone key generator/reader for performance test.
 */

@CommandLine.Command(name = "ockrw",
        aliases = "ozone-client-key-read-write-list-ops",
        description = "Generate keys with a fixed name and ranges that can" 
        + " be written, read and listed as sub-ranges from multiple clients.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OzoneClientKeyReadWriteListOps extends BaseFreonGenerator
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
  private long startIndex;

  @CommandLine.Option(names = {"-r", "--range"},
          description = "Range of read/write operations. This in co-ordination"
          + " with --start-index can specify the range to read. "
          + "Example: Read from --start-index 1000 and read --range 1000 keys.",
          defaultValue = "1")
  private long range;

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

  @CommandLine.Option(names = {"--linear"},
      description = "Generate key names in a linear manner, there is no random"
      + "selection of keys", defaultValue = "false")
  private boolean linear;

  @CommandLine.Option(names = {"--percentage-read"},
          description = "Percentage of read tasks in mix workload."
          + " The remainder of the percentage will be divided between write"
          + " and list tasks. By default this is 0%, to populate a range.",
          required = false,
          defaultValue = "0")
  private int percentageRead;

  @CommandLine.Option(names = {"--percentage-list"},
          description = "Percentage of list tasks in mix workload."
          + " The remainder of the percentage will be divided between write"
          + " and read tasks.",
          required = false,
          defaultValue = "0")
  private int percentageList;

  @CommandLine.Option(names = {"--max-list-result"},
      description = "Maximum number of keys to be fetched during the list task."
          + " It ensures the size of the result will not exceed this limit.",
      defaultValue = "1000")
  private int maxListResult;

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
          LoggerFactory.getLogger(OzoneClientKeyReadWriteListOps.class);

  private static final AtomicLong NEXT_NUMBER = new AtomicLong();

  private KeyGeneratorUtil kg;

  @Override
  public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    clientCount =  getThreadNo();
    ozoneClients = new OzoneClient[clientCount];
    try {
      for (int i = 0; i < clientCount; i++) {
        ozoneClients[i] = createOzoneClient(omServiceID, ozoneConfiguration);
      }

      ensureVolumeAndBucketExist(ozoneClients[0], volumeName, bucketName);

      timer = getMetrics().timer("key-read-write-list");
      if (objectSizeInBytes >= 0) {
        keyContent = RandomUtils.secure().randomBytes(objectSizeInBytes);
      }
      if (kg == null) {
        kg = new KeyGeneratorUtil();
      }
      runTests(this::readWriteListKeys);
    } finally {
      for (int i = 0; i < clientCount; i++) {
        if (ozoneClients[i] != null) {
          ozoneClients[i].close();
        }
      }
    }
    return null;
  }

  private void readWriteListKeys(long counter) throws RuntimeException {
    int clientIndex = (int)((counter) % clientCount);
    TaskType taskType = decideReadWriteOrListTask();
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
        case LIST_TASK:
          processListTasks(ozoneClients[clientIndex]);
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
    OzoneKeyDetails keyDetails = client.getProxy().
            getKeyDetails(volumeName, bucketName, keyName);
    if (!readMetadataOnly) {
      try (InputStream input = keyDetails.getContent()) {
        byte[] ignored = IOUtils.readFully(input, objectSizeInBytes);
      }
    }
  }

  public void processWriteTasks(String keyName, OzoneClient ozoneClient)
          throws RuntimeException, IOException {
    try (OutputStream out = ozoneClient.getProxy().createKey(
        volumeName, bucketName, keyName, objectSizeInBytes, null,
        new HashMap<>())) {
      out.write(keyContent);
    }
  }

  public void processListTasks(OzoneClient ozoneClient)
      throws RuntimeException, IOException {
    ozoneClient.getProxy()
        .listKeys(volumeName, bucketName, getPrefix(), null, maxListResult);
  }

  public TaskType decideReadWriteOrListTask() {
    int tmp = ThreadLocalRandom.current().nextInt(1, 101);
    if (tmp <= percentageRead) {
      return TaskType.READ_TASK;
    } else if (tmp <= percentageRead + percentageList) {
      return TaskType.LIST_TASK;
    } else {
      return TaskType.WRITE_TASK;
    }
  }

  public String getKeyName() {
    StringBuilder keyNameSb = new StringBuilder();
    long next;
    if (linear) {
      next = startIndex + NEXT_NUMBER.getAndUpdate(x -> (x + 1) % range);
    }  else {
      next = ThreadLocalRandom.current().
          nextLong(startIndex, startIndex + range);
    }

    if (keySorted) {
      keyNameSb.append(getPrefix()).append(FILE_DIR_SEPARATOR).
              append(next);
    } else {
      keyNameSb.append(getPrefix()).append(FILE_DIR_SEPARATOR).
              append(kg.generateMd5KeyName(next));
    }
    return keyNameSb.toString();
  }

  @Override
  public boolean allowEmptyPrefix() {
    return true;
  }

  /**
   * Task type of read task, or write task.
   */
  public enum TaskType {
    READ_TASK,
    WRITE_TASK,
    LIST_TASK
  }
}
