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

package org.apache.hadoop.ozone.protocolPB;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ChecksumTypeProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateManagedS3AccessKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateManagedS3AccessKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MD5MD5Crc32FileChecksumProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ManagedS3AccessKeyMetadataProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetrieveManagedS3AccessKeySecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetrieveManagedS3AccessKeySecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RotateManagedS3AccessKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ManagedAccessKeyInfoProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateCreateManagedS3AccessKeyRequest;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.junit.jupiter.api.Test;

/**
 * Test {@link OMPBHelper}.
 */
public final class TestOMPBHelper {
  /**
   * This is to test backward compatibility for a bug fixed by HDDS-12954
   * for {@link OMPBHelper#convertMD5MD5FileChecksum(MD5MD5Crc32FileChecksumProto)}.
   * Previously, the proto md5 was created using a 20-byte buffer with the last 4 bytes unused.
   * This test verifies the new code can handle the previous (buggy) case.
   */
  @Test
  void testConvertMD5MD5FileChecksum() throws Exception {
    runTestConvertMD5MD5FileChecksum(MD5Hash.MD5_LEN);
    // for testing backward compatibility
    runTestConvertMD5MD5FileChecksum(20);
  }

  void runTestConvertMD5MD5FileChecksum(int n) throws Exception {
    System.out.println("n=" + n);
    // random bytesPerCrc and crcPerBlock
    final Random random = ThreadLocalRandom.current();
    final int bytesPerCrc = random.nextInt(1 << 20) + 1;
    final int crcPerBlock = random.nextInt(1 << 20) + 1;

    // random md5
    final byte[] md5bytes = new byte[n];
    random.nextBytes(md5bytes);
    Arrays.fill(md5bytes, MD5Hash.MD5_LEN, n, (byte) 0); // set extra bytes to zeros.
    final ByteString md5 = ByteString.copyFrom(md5bytes);
    System.out.println("md5     : " + StringUtils.bytes2Hex(md5.asReadOnlyByteBuffer()));
    assertEquals(n, md5.size());

    // build proto
    final MD5MD5Crc32FileChecksumProto proto = MD5MD5Crc32FileChecksumProto.newBuilder()
        .setChecksumType(ChecksumTypeProto.CHECKSUM_CRC32)
        .setBytesPerCRC(bytesPerCrc)
        .setCrcPerBlock(crcPerBlock)
        .setMd5(md5)
        .build();

    // covert proto
    final MD5MD5CRC32FileChecksum checksum = OMPBHelper.convertMD5MD5FileChecksum(proto);
    assertEquals(bytesPerCrc, checksum.getChecksumOpt().getBytesPerChecksum());

    // get bytes
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    checksum.write(new DataOutputStream(byteArrayOutputStream));
    final byte[] bytes = byteArrayOutputStream.toByteArray();
    assertEquals(checksum.getLength(), bytes.length);

    // assert bytes
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    assertEquals(bytesPerCrc, buffer.getInt());
    assertEquals(crcPerBlock, buffer.getLong());
    final ByteString computed = ByteString.copyFrom(buffer);
    System.out.println("computed: " + StringUtils.bytes2Hex(computed.asReadOnlyByteBuffer()));
    assertEquals(MD5Hash.MD5_LEN, computed.size());
    assertEquals(md5.substring(0, MD5Hash.MD5_LEN), computed);
  }

  @Test
  void processForDebugRedactsManagedS3AccessKeyCreateRequest() {
    String debug = OMPBHelper.processForDebug(OMRequest.newBuilder()
        .setCmdType(Type.CreateManagedS3AccessKey)
        .setClientId("client")
        .setCreateManagedS3AccessKeyRequest(
            CreateManagedS3AccessKeyRequest.newBuilder()
                .setEffectiveUser("user")
                .setCustomSecret("custom-secret-sentinel")
                .setPolicyDocument("policy-sentinel"))
        .build());

    assertFalse(debug.contains("custom-secret-sentinel"));
    assertFalse(debug.contains("policy-sentinel"));
  }

  @Test
  void processForDebugRedactsManagedS3AccessKeyUpdateRequest() {
    String debug = OMPBHelper.processForDebug(OMRequest.newBuilder()
        .setCmdType(Type.CreateManagedS3AccessKey)
        .setClientId("client")
        .setUpdateCreateManagedS3AccessKeyRequest(
            UpdateCreateManagedS3AccessKeyRequest.newBuilder()
                .setInfo(S3ManagedAccessKeyInfoProto.newBuilder()
                    .setAccessKeyId("access-key")
                    .setEncryptedSecretKey(
                        ByteString.copyFromUtf8("envelope-sentinel"))
                    .setPolicyDocument("policy-sentinel")))
        .build());

    assertFalse(debug.contains("envelope-sentinel"));
    assertFalse(debug.contains("policy-sentinel"));
  }

  @Test
  void processForDebugRedactsManagedS3AccessKeyRotateRequest() {
    String debug = OMPBHelper.processForDebug(OMRequest.newBuilder()
        .setCmdType(Type.RotateManagedS3AccessKey)
        .setClientId("client")
        .setRotateManagedS3AccessKeyRequest(
            RotateManagedS3AccessKeyRequest.newBuilder()
                .setAccessKeyId("access-key")
                .setCustomSecret("rotate-secret-sentinel"))
        .build());

    assertFalse(debug.contains("rotate-secret-sentinel"));
  }

  @Test
  void processForDebugRedactsManagedS3AccessKeyRetrieveRequest() {
    String debug = OMPBHelper.processForDebug(OMRequest.newBuilder()
        .setCmdType(Type.RetrieveManagedS3AccessKeySecret)
        .setClientId("client")
        .setRetrieveManagedS3AccessKeySecretRequest(
            RetrieveManagedS3AccessKeySecretRequest.newBuilder()
                .setAccessKeyId("access-key")
                .setRetrievalHandle("handle-sentinel"))
        .build());

    assertFalse(debug.contains("handle-sentinel"));
  }

  @Test
  void processForDebugRedactsManagedS3AccessKeyResponses() {
    String createDebug = OMPBHelper.processForDebug(OMResponse.newBuilder()
        .setCmdType(Type.CreateManagedS3AccessKey)
        .setStatus(Status.OK)
        .setCreateManagedS3AccessKeyResponse(
            CreateManagedS3AccessKeyResponse.newBuilder()
                .setRetrievalHandle("handle-sentinel")
                .setMetadata(ManagedS3AccessKeyMetadataProto.newBuilder()
                    .setAccessKeyId("access-key")))
        .build());

    String retrieveDebug = OMPBHelper.processForDebug(OMResponse.newBuilder()
        .setCmdType(Type.RetrieveManagedS3AccessKeySecret)
        .setStatus(Status.OK)
        .setRetrieveManagedS3AccessKeySecretResponse(
            RetrieveManagedS3AccessKeySecretResponse.newBuilder()
                .setAccessKeyId("access-key")
                .setPlaintextSecret("plaintext-sentinel"))
        .build());

    assertFalse(createDebug.contains("handle-sentinel"));
    assertFalse(retrieveDebug.contains("plaintext-sentinel"));
  }

  @Test
  void smProtoToStringRedactsManagedS3AccessKeyUpdateRequest() {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.CreateManagedS3AccessKey)
        .setClientId("client")
        .setUpdateCreateManagedS3AccessKeyRequest(
            UpdateCreateManagedS3AccessKeyRequest.newBuilder()
                .setInfo(S3ManagedAccessKeyInfoProto.newBuilder()
                    .setAccessKeyId("access-key")
                    .setEncryptedSecretKey(
                        ByteString.copyFromUtf8("envelope-sentinel"))
                    .setPolicyDocument("policy-sentinel")))
        .build();
    StateMachineLogEntryProto entry = StateMachineLogEntryProto.newBuilder()
        .setLogData(OMRatisHelper.convertRequestToByteString(request))
        .build();

    String debug = OMRatisHelper.smProtoToString(entry);

    assertFalse(debug.contains("envelope-sentinel"));
    assertFalse(debug.contains("policy-sentinel"));
  }
}
