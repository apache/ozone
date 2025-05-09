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

import com.codahale.metrics.Timer;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "ockg",
    aliases = "ozone-client-key-generator",
    description = "Generate keys with the help of the ozone clients.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OzoneClientKeyGenerator extends BaseFreonGenerator
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
      description = "Size of the generated key. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "10KB",
      converter = StorageSizeConverter.class)
  private StorageSize keySize;

  @Option(names = {"--buffer"},
      description = "Size of buffer used to generated the key content.",
      defaultValue = "4096")
  private int bufferSize;

  @Option(names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID;

  @Mixin
  private FreonReplicationOptions replication;

  @Option(
      names = {"--enable-streaming", "--stream"},
      description = "Specify whether the write will be through ratis streaming"
  )
  private boolean enableRatisStreaming = false;

  private Timer timer;

  private OzoneBucket bucket;
  private ContentGenerator contentGenerator;
  private Map<String, String> metadata;
  private ReplicationConfig replicationConfig;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    contentGenerator = new ContentGenerator(keySize.toBytes(), bufferSize);
    metadata = new HashMap<>();

    replicationConfig = replication.fromParamsOrConfig(ozoneConfiguration);

    try (OzoneClient rpcClient = createOzoneClient(omServiceID,
        ozoneConfiguration)) {
      ensureVolumeAndBucketExist(rpcClient, volumeName, bucketName);
      bucket = rpcClient.getObjectStore().getVolume(volumeName)
          .getBucket(bucketName);

      timer = getMetrics().timer("key-create");

      if (enableRatisStreaming) {
        runTests(this::createStreamKey);
      } else {
        runTests(this::createKey);
      }
    }
    return null;
  }

  private void createKey(long counter) throws Exception {
    final String key = generateObjectName(counter);

    timer.time(() -> {
      try (OutputStream stream = bucket.createKey(key, keySize.toBytes(),
          replicationConfig, metadata)) {
        contentGenerator.write(stream);
        stream.flush();
      }
      return null;
    });
  }

  private void createStreamKey(long counter) throws Exception {
    final ReplicationConfig conf = ReplicationConfig.fromProtoTypeAndFactor(
        ReplicationType.RATIS, ReplicationFactor.THREE);
    final String key = generateObjectName(counter);

    timer.time(() -> {
      try (OzoneDataStreamOutput stream = bucket.createStreamKey(
          key, keySize.toBytes(), conf, metadata)) {
        contentGenerator.write(stream);
      }
      return null;
    });
  }
}
