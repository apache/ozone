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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Ozone file system tests to validate default bucket layout configuration
 * and behaviour.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneFSBucketLayout implements NonHATests.TestCase {

  private ObjectStore objectStore;
  private OzoneClient client;
  private String rootPath;
  private String volumeName;

  private static final String UNKNOWN_LAYOUT = "INVALID";
  private static final Map<String, String> ERROR_MAP = new HashMap<>();

  // Initialize error map.
  static {
    ERROR_MAP.put(BucketLayout.OBJECT_STORE.name(),
        "Buckets created with OBJECT_STORE layout do not support file " +
            "system semantics.");
    ERROR_MAP.put(UNKNOWN_LAYOUT, "Unsupported value provided for " +
        OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT);
  }

  static Collection<String> validDefaultBucketLayouts() {
    return Arrays.asList(
        // Empty Config
        "",
        // Supported bucket layouts.
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name(),
        BucketLayout.LEGACY.name()
    );
  }

  static Collection<String> invalidDefaultBucketLayouts() {
    return Arrays.asList(
        // Invalid Config
        UNKNOWN_LAYOUT,
        // Unsupported Bucket Layout for OFS
        BucketLayout.OBJECT_STORE.name()
    );
  }

  @BeforeAll
  void setUp() throws Exception {
    client = cluster().newClient();
    objectStore = client.getObjectStore();
    rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, cluster().getConf().get(OZONE_OM_ADDRESS_KEY));
    volumeName = TestDataUtil.createVolumeAndBucket(client).getVolumeName();
  }

  @AfterAll
  void tearDown() {
    IOUtils.closeQuietly(client);
  }

  @ParameterizedTest
  @MethodSource("invalidDefaultBucketLayouts")
  void fileSystemWithUnsupportedDefaultBucketLayout(String layout) {
    OzoneConfiguration conf = configWithDefaultBucketLayout(layout);

    OMException e = assertThrows(OMException.class, () -> {
      FileSystem fileSystem = null;
      try {
        fileSystem = FileSystem.newInstance(conf);
      } finally {
        if (fileSystem != null) {
          fileSystem.close();
        }
      }
    });
    assertThat(e.getMessage())
        .contains(ERROR_MAP.get(layout));
  }

  @ParameterizedTest
  @MethodSource("validDefaultBucketLayouts")
  void fileSystemWithValidBucketLayout(String layout) throws IOException {
    OzoneConfiguration conf = configWithDefaultBucketLayout(layout);

    try (FileSystem fs = FileSystem.newInstance(conf)) {
      // Create a new directory, which in turn creates a new bucket.
      String bucketName = getBucketName(layout);
      fs.mkdirs(new Path(new Path(OZONE_ROOT, volumeName), bucketName));

      // Make sure the bucket layout of created bucket matches the config.
      OzoneBucket bucketInfo =
          objectStore.getClientProxy().getBucketDetails(volumeName, bucketName);

      String expectedLayout = layout.isEmpty()
          ? OzoneConfigKeys.OZONE_CLIENT_FS_BUCKET_LAYOUT_DEFAULT
          : layout;
      assertEquals(expectedLayout, bucketInfo.getBucketLayout().name());
    }
  }

  private String getBucketName(String layout) {
    String bucketSuffix = layout.isEmpty()
        ? "empty"
        : layout.toLowerCase().replaceAll("_", "-");

    return "bucket-" + bucketSuffix;
  }

  private OzoneConfiguration configWithDefaultBucketLayout(String layout) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setFsDefaultBucketLayout(layout);
    conf.setFromObject(clientConfig);
    return conf;
  }

}
