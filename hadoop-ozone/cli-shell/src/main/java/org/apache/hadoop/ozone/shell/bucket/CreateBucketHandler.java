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

package org.apache.hadoop.ozone.shell.bucket;

import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.SetSpaceQuotaOptions;
import org.apache.hadoop.ozone.shell.ShellReplicationOptions;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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

  @Option(names = { "--layout", "-l" }, converter = LayoutConverter.class,
          description = "Allowed Bucket Layouts: fso (for file system optimized buckets FILE_SYSTEM_OPTIMIZED), " +
                  "obs (for object store optimized OBJECT_STORE) and legacy (LEGACY is Deprecated)")
  private BucketLayout allowedBucketLayout;

  @CommandLine.Mixin
  private ShellReplicationOptions replication;

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

    BucketArgs.Builder bb =
        new BucketArgs.Builder().setStorageType(StorageType.DEFAULT)
            .setVersioning(false).setOwner(ownerName);
    if (allowedBucketLayout != null) {
      bb.setBucketLayout(allowedBucketLayout);
    }
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

    replication.fromParams(getConf()).ifPresent(config ->
        bb.setDefaultReplicationConfig(new DefaultReplicationConfig(config)));

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

  private static class LayoutConverter implements CommandLine.ITypeConverter<BucketLayout> {
    @Override
    public BucketLayout convert(String value) {
      if (value == null) {
        return null;
      }
      switch (value) {
      case "fso":
        return BucketLayout.FILE_SYSTEM_OPTIMIZED;
      case "obs":
        return BucketLayout.OBJECT_STORE;
      default:
        for (BucketLayout candidate : BucketLayout.values()) {
          if (candidate.name().equalsIgnoreCase(value)) {
            return candidate;
          }
        }
        throw new IllegalArgumentException("Unknown bucket layout: " + value);
      }
    }
  }
}
