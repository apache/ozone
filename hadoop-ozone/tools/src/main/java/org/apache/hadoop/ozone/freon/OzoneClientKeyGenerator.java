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

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;

import com.codahale.metrics.Timer;
import picocli.CommandLine.Command;
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
public class OzoneClientKeyGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-s", "--size"},
      description = "Size of the generated key (in bytes)",
      defaultValue = "10240")
  private long keySize;

  @Option(names = {"--buffer"},
      description = "Size of buffer used to generated the key content.",
      defaultValue = "4096")
  private int bufferSize;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  @Option(names = {"--replication"},
      description =
          "Replication configuration of the new key. (this is replication "
              + "specific. for RATIS/STANDALONE you can use ONE or THREE, " +
              "for EC you can use rs-3-2-1024k, rs-6-3-1024 or rs-10-4-1024k.)"
              + " Default is specified in the cluster-wide config.")
  private String replication;

  @Option(names = {"--type"},
      description = "Replication type of the new key. (use RATIS or " +
          "STAND_ALONE or EC) Default is specified in the cluster-wide config.")
  private ReplicationType replicationType;

  private Timer timer;

  private OzoneBucket bucket;
  private ContentGenerator contentGenerator;
  private Map<String, String> metadata;
  private ReplicationConfig replicationConfig;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    contentGenerator = new ContentGenerator(keySize, bufferSize);
    metadata = new HashMap<>();
    replicationConfig = ReplicationConfig.parse(replicationType, replication,
        ozoneConfiguration);

    try (OzoneClient rpcClient = createOzoneClient(omServiceID,
        ozoneConfiguration)) {
      ensureVolumeAndBucketExist(rpcClient, volumeName, bucketName);
      bucket = rpcClient.getObjectStore().getVolume(volumeName)
          .getBucket(bucketName);

      timer = getMetrics().timer("key-create");

      runTests(this::createKey);
    }
    return null;
  }

  private void createKey(long counter) throws Exception {
    final String key = generateObjectName(counter);

    timer.time(() -> {
      try(OutputStream stream = bucket.createKey(key, keySize,
          replicationConfig, metadata)) {
        contentGenerator.write(stream);
        stream.flush();
      }
      return null;
    });
  }
}
