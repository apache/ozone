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

package org.apache.hadoop.ozone.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.junit.jupiter.api.Test;

/**
 * Tests the behavior of OzoneClientUtils APIs.
 */
public class TestOzoneClientUtils {
  private ReplicationConfig ecReplicationConfig =
      new ECReplicationConfig("rs-3-2-1024K");
  private ReplicationConfig ratis3ReplicationConfig =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private ReplicationConfig ratis1ReplicationConfig =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);

  @Test()
  public void testNegativeLength() throws IOException {
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    String keyName = "dummy";
    ClientProtocol clientProtocol = mock(ClientProtocol.class);
    assertThrows(IllegalArgumentException.class, () ->
        OzoneClientUtils.getFileChecksumWithCombineMode(volume, bucket, keyName,
        -1, OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC,
        clientProtocol));
  }

  @Test
  public void testEmptyKeyName() throws IOException {
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    String keyName = "";
    ClientProtocol clientProtocol = mock(ClientProtocol.class);
    FileChecksum checksum =
        OzoneClientUtils.getFileChecksumWithCombineMode(volume, bucket, keyName,
            1, OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC,
            clientProtocol);

    assertNull(checksum);
  }

  @Test
  public void testResolveClientSideRepConfigWhenBucketHasEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 3, null,
            ecReplicationConfig, new OzoneConfiguration());
    // Bucket default is EC.
    assertEquals(ecReplicationConfig, replicationConfig);
  }

  /**
   * When bucket replication is null and it should respect fs passed value.
   */
  @Test
  public void testResolveClientSideRepConfigWhenBucketHasNull() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 3, null, null,
            new OzoneConfiguration());
    // Passed replication is 3 - Ozone mapped replication is ratis THREE
    assertEquals(ratis3ReplicationConfig, replicationConfig);
  }

  /**
   * When bucket replication is null and it should return null if fs passed
   * value is invalid.
   */
  @Test
  public void testResolveClientSideRepConfigWhenFSPassedReplicationIsInvalid() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) -1, null, null,
            new OzoneConfiguration());
    // client configured value also null.
    // This API caller should leave the decision to server.
    assertNull(replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side values are not valid, we
   * would just return null, so servers can make decision in this case.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsInvalidButBucketDefaultNonEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) -1, null, ratis3ReplicationConfig,
            new OzoneConfiguration());
    // Configured client config also null.
    assertNull(replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side value is valid, we
   * would should return client side valid value.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsValidButBucketDefaultNonEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 1, null, ratis3ReplicationConfig,
            new OzoneConfiguration());
    // Passed value is replication one - Ozone mapped value is ratis ONE
    assertEquals(ratis1ReplicationConfig, replicationConfig);
  }

  /**
   * When bucket default is EC and client side value also valid, we would just
   * return bucket default EC.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsValidButBucketDefaultEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 3, ratis3ReplicationConfig,
            ecReplicationConfig, new OzoneConfiguration());
    // Bucket default is EC
    assertEquals(ecReplicationConfig, replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side passed value also not valid
   * but configured value is valid, we would just return configured value.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsInvalidAndBucketDefaultNonEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) -1, ratis3ReplicationConfig, ratis1ReplicationConfig,
            new OzoneConfiguration());
    // Configured value is ratis THREE
    assertEquals(ratis3ReplicationConfig, replicationConfig);
  }

  /**
   * Tests validateAndGetClientReplicationConfig with user passed valid config
   * values.
   */
  @Test
  public void testValidateAndGetRepConfWhenValidUserPassedValues() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .validateAndGetClientReplicationConfig(ReplicationType.RATIS, "1",
            new OzoneConfiguration());
    // Configured value is ratis ONE
    assertEquals(ratis1ReplicationConfig, replicationConfig);
  }

  /**
   * Tests validateAndGetClientReplicationConfig with user passed null values.
   */
  @Test
  public void testValidateAndGetRepConfWhenValidUserPassedNullValues() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .validateAndGetClientReplicationConfig(null, null,
            new OzoneConfiguration());
    assertNull(replicationConfig);
  }

  /**
   * Tests validateAndGetClientReplicationConfig with user passed null values
   * but client config has valid values.
   */
  @Test
  public void testValidateAndGetRepConfWhenValidConfigValues() {
    OzoneConfiguration clientSideConfig = new OzoneConfiguration();
    clientSideConfig.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE, "EC");
    clientSideConfig.set(OzoneConfigKeys.OZONE_REPLICATION, "rs-3-2-1024K");
    ReplicationConfig replicationConfig = OzoneClientUtils
        .validateAndGetClientReplicationConfig(null, null, clientSideConfig);
    assertEquals(ecReplicationConfig, replicationConfig);
  }

  /**
   * Tests validateAndGetClientReplicationConfig with user passed null values
   * but client config has valid values.
   */
  @Test
  public void testValidateAndGetRepConfWhenNullTypeFromUser() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .validateAndGetClientReplicationConfig(null, "3",
            new OzoneConfiguration());
    assertNull(replicationConfig);
  }

  /**
   * Tests validateAndGetClientReplicationConfig with user passed null
   * replication but valid type.
   */
  @Test
  public void testValidateAndGetRepConfWhenNullReplicationFromUser() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .validateAndGetClientReplicationConfig(ReplicationType.EC, null,
            new OzoneConfiguration());
    assertNull(replicationConfig);
  }

  /**
   * Tests validateAndGetClientReplicationConfig with user pass null values but
   * config has only replication configured.
   */
  @Test
  public void testValidateAndGetRepConfWhenNullTypeConfigValues() {
    OzoneConfiguration clientSideConfig = new OzoneConfiguration();
    clientSideConfig.set(OzoneConfigKeys.OZONE_REPLICATION, "rs-3-2-1024K");
    //By default config values are null. Let's don't set type to keep it as
    // null.
    ReplicationConfig replicationConfig = OzoneClientUtils
        .validateAndGetClientReplicationConfig(null, null, clientSideConfig);
    assertNull(replicationConfig);
  }

  /**
   * Tests validateAndGetClientReplicationConfig with user pass null values but
   * config has only type configured.
   */
  @Test
  public void testValidateAndGetRepConfWhenNullReplicationConfigValues() {
    OzoneConfiguration clientSideConfig = new OzoneConfiguration();
    clientSideConfig.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE, "EC");
    //By default config values are null. Let's don't set replication to keep it
    // as null.
    ReplicationConfig replicationConfig = OzoneClientUtils
        .validateAndGetClientReplicationConfig(null, null, clientSideConfig);
    assertNull(replicationConfig);
  }

}
