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

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Synthetic read/write key operations workload generator tool.
 */
@Command(name = "obrwk",
    aliases = "om-bucket-read-write-key-ops",
    description = "Creates keys, performs respective read/write " +
        "operations to measure lock performance.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
@SuppressWarnings("java:S2245") // no need for secure random
public class OmBucketReadWriteKeyOps extends AbstractOmBucketReadWriteOps {

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

  @Option(names = {"-k", "--key-count-for-read"},
      description = "Number of keys to be created for read operations.",
      defaultValue = "100")
  private int keyCountForRead;

  @Option(names = {"-w", "--key-count-for-write"},
      description = "Number of keys to be created for write operations.",
      defaultValue = "10")
  private int keyCountForWrite;

  @Option(names = {"-o", "--om-service-id"},
      description = "OM Service ID"
  )
  private String omServiceID = null;

  @CommandLine.Mixin
  private FreonReplicationOptions replication;

  private Map<String, String> metadata;

  private ReplicationConfig replicationConfig;

  private OzoneBucket bucket;

  @Override
  protected void display() {
    print("volumeName: " + volumeName);
    print("bucketName: " + bucketName);
    print("keyCountForRead: " + keyCountForRead);
    print("keyCountForWrite: " + keyCountForWrite);
    print("omServiceID: " + omServiceID);
  }

  @Override
  protected void initialize(OzoneConfiguration ozoneConfiguration)
      throws Exception {

    replicationConfig = replication.fromParamsOrConfig(ozoneConfiguration);
    metadata = new HashMap<>();

    try (OzoneClient rpcClient = createOzoneClient(omServiceID,
        ozoneConfiguration)) {
      ensureVolumeAndBucketExist(rpcClient, volumeName, bucketName);
      bucket = rpcClient.getObjectStore().getVolume(volumeName)
          .getBucket(bucketName);

      runTests(this::mainMethod);
    }
  }

  private void mainMethod(long counter) throws Exception {

    int readResult = readOperations(keyCountForRead);
    int writeResult = writeOperations(keyCountForWrite);

    print("Total Keys Read: " + readResult);
    print("Total Keys Written: " + writeResult * keyCountForWrite);

    // TODO: print read/write lock metrics (HDDS-6435, HDDS-6436).
  }

  @Override
  protected String createPath(String path) {
    return "".concat(OzoneConsts.OM_KEY_PREFIX).concat(path);
  }

  @Override
  protected int getReadCount(int readCount, String readPath)
      throws IOException {
    List<OzoneFileStatus> ozoneFileStatusList = bucket.listStatus(
        OzoneConsts.OM_KEY_PREFIX + readPath + OzoneConsts.OM_KEY_PREFIX, true,
        "/", keyCountForRead);
    readCount += ozoneFileStatusList.size();
    return readCount;
  }

  @Override
  protected OutputStream create(String keyName) throws IOException {
    return bucket.createKey(keyName, getSizeInBytes(), replicationConfig,
        metadata);
  }
}
