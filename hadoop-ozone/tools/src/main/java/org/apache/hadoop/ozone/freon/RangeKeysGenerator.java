/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.HashMap;

import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.PURE_INDEX;
import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.MD5;
import static org.apache.hadoop.ozone.freon.KeyGeneratorUtil.FILE_DIR_SEPARATOR;

/**
 * Ozone range keys generator for performance test.
 */
@CommandLine.Command(name = "ork",
        description = "write range keys with the help of the ozone clients.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class RangeKeysGenerator extends BaseFreonGenerator
        implements Callable<Void> {

  private static final Logger LOG =
          LoggerFactory.getLogger(RangeKeysGenerator.class);

  @CommandLine.Option(names = {"-v", "--volume"},
          description = "Name of the volume which contains the test data. " +
                  "Will be created if missing.",
          defaultValue = "ockrwvolume")
  private String volumeName;

  @CommandLine.Option(names = {"-b", "--bucket"},
          description = "Name of the bucket which contains the test data.",
          defaultValue = "ockrwbucket")
  private String bucketName;

  @CommandLine.Option(names = {"-r", "--range-each-client-write"},
          description = "Write range for each client.",
          defaultValue = "0")
  private int range;

  @CommandLine.Option(names = {"-s", "--key-start-index"},
          description = "Start index of key.",
          defaultValue = "0")
  private int startIndex;


  @CommandLine.Option(names = {"-k", "--key-encode"},
          description = "The algorithm to generate key names. " +
                  "Options are pureIndex, md5",
          defaultValue = "md5")
  private String encodeFormat;

  @CommandLine.Option(names = {"-g", "--size"},
          description = "Generated object size (in bytes) " +
                  "to be written.",
          defaultValue = "1")
  private int objectSizeInBytes;

  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;
  private KeyGeneratorUtil kg;
  private int clientCount;
  private OzoneClient[] ozoneClients;
  private byte[] keyContent;
  private Timer timer;


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
    if (objectSizeInBytes >= 0) {
      keyContent = RandomUtils.nextBytes(objectSizeInBytes);
    }
    timer = getMetrics().timer("key-read-write");

    kg = new KeyGeneratorUtil();
    runTests(this::generateRangeKeys);
    for (int i = 0; i < clientCount; i++) {
      if (ozoneClients[i] != null) {
        ozoneClients[i].close();
      }
    }

    return null;
  }

  public void generateRangeKeys(long count) throws Exception {
    int clientIndex = (int)(count % clientCount);
    OzoneClient client = ozoneClients[clientIndex];
    int start = startIndex + (int)count * range;
    int end = start + range;

    timer.time(() -> {
      switch (encodeFormat) {
      case PURE_INDEX:
        loopRunner(kg.pureIndexKeyNameFunc(), client, start, end);
        break;
      case MD5:
        loopRunner(kg.md5KeyNameFunc(), client, start, end);
        break;
      default:
        loopRunner(kg.md5KeyNameFunc(), client, start, end);
        break;
      }
      return null;
    });
  }


  public void loopRunner(Function<Integer, String> keyNameGeneratorfunc,
                         OzoneClient client, int start, int end)
          throws Exception {
    String keyName;
    for (int i = start; i < end + 1; i++) {
      keyName = getPrefix() + FILE_DIR_SEPARATOR +
              keyNameGeneratorfunc.apply(i);
      try (OzoneOutputStream out = client.getProxy().
                        createKey(volumeName, bucketName, keyName,
                                objectSizeInBytes, null, new HashMap())) {
        out.write(keyContent);
      }
    }
  }
}
