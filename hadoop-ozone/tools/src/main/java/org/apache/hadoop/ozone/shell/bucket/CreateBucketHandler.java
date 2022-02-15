/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.shell.bucket;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.shell.OzoneAddress;

import org.apache.hadoop.ozone.shell.SetSpaceQuotaOptions;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;

/**
 * create bucket handler.
 */
@Command(name = "create",
    description = "creates a bucket in a given volume")
public class CreateBucketHandler extends BucketHandler {

  @Option(names = {"--bucketkey", "-k"},
      description = "bucket encryption key name")
  private String bekName;

  @Option(names = {"--enforcegdpr", "-g"},
      description = "if true, indicates GDPR enforced bucket, " +
          "false/unspecified indicates otherwise")
  private Boolean isGdprEnforced;

  @Option(names = {"--user", "-u"},
          description = "Owner of the bucket. Defaults to current" +
              " user if not specified")
  private String ownerName;

  enum AllowedBucketLayouts { FILE_SYSTEM_OPTIMIZED, OBJECT_STORE }

  @Option(names = { "--layout", "-l" },
      description = "Allowed Bucket Layouts: ${COMPLETION-CANDIDATES}",
      defaultValue = "OBJECT_STORE")
  private AllowedBucketLayouts allowedBucketLayout;

  @Option(names = {"--replication", "-r"},
      description = "Replication value. Example: 3 (for Ratis type) or 1 ( for"
          + " standalone type). In the case of EC, pass DATA-PARITY, eg 3-2," +
          " 6-3, 10-4")
  private String replication;

  @Option(names = {"--type", "-t"},
      description = "Replication type. Supported types are RATIS, STANDALONE,"
          + " EC")
  private String replicationType;

  @CommandLine.Mixin
  private SetSpaceQuotaOptions quotaOptions;

  /**
   * Executes create bucket.
   */
  @Override
  public void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    if (ownerName == null) {
      ownerName = UserGroupInformation.getCurrentUser().getShortUserName();
    }

    BucketArgs.Builder bb;
    BucketLayout bucketLayout =
        BucketLayout.valueOf(allowedBucketLayout.toString());
    bb = new BucketArgs.Builder().setStorageType(StorageType.DEFAULT)
        .setVersioning(false).setBucketLayout(bucketLayout)
        .setOwner(ownerName);
    // TODO: New Client talking to old server, will it create a LEGACY bucket?

    if (isGdprEnforced != null) {
      bb.addMetadata(OzoneConsts.GDPR_FLAG, String.valueOf(isGdprEnforced));
    }

    if (bekName != null) {
      if (!bekName.isEmpty()) {
        bb.setBucketEncryptionKey(bekName);
      } else {
        throw new IllegalArgumentException("Bucket encryption key name must" +
            " " + "be specified to enable bucket encryption!");
      }
      if (isVerbose()) {
        out().printf("Bucket Encryption enabled with Key Name: %s%n",
            bekName);
      }
    }

    if(replicationType!=null) {
      if (replication != null) {
        ReplicationConfig replicationConfig = ReplicationConfig
            .parse(ReplicationType.valueOf(replicationType),
                replication, new OzoneConfiguration());
        boolean isEC = replicationConfig
            .getReplicationType() == HddsProtos.ReplicationType.EC;
        bb.setDefaultReplicationConfig(new DefaultReplicationConfig(
            ReplicationType.fromProto(replicationConfig.getReplicationType()),
            isEC ?
                null :
                ReplicationFactor.valueOf(replicationConfig.getRequiredNodes()),
            isEC ? (ECReplicationConfig) replicationConfig : null));
      } else {
        throw new IOException(
            "Replication can't be null. Replication type passed was : "
                + replicationType);
      }
    }

    if (!Strings.isNullOrEmpty(quotaOptions.getQuotaInBytes())) {
      bb.setQuotaInBytes(OzoneQuota.parseSpaceQuota(
          quotaOptions.getQuotaInBytes()).getQuotaInBytes());
    }

    if (!Strings.isNullOrEmpty(quotaOptions.getQuotaInNamespace())) {
      bb.setQuotaInNamespace(OzoneQuota.parseNameSpaceQuota(
          quotaOptions.getQuotaInNamespace()).getQuotaInNamespace());
    }
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    vol.createBucket(bucketName, bb.build());

    if (isVerbose()) {
      OzoneBucket bucket = vol.getBucket(bucketName);
      printObjectAsJson(bucket);
    }
  }
}
