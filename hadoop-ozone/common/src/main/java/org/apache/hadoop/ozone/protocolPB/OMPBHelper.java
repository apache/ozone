/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.ByteString;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketEncryptionInfoProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ChecksumTypeProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CipherSuiteProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CompositeCrcFileChecksumProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CryptoProtocolVersionProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FileChecksumProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FileChecksumTypeProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FileEncryptionInfoProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MD5MD5Crc32FileChecksumProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Utilities for converting protobuf classes.
 */
public final class OMPBHelper {
  private static final Logger LOG = LoggerFactory.getLogger(OMPBHelper.class);
  public static final ByteString REDACTED =
      ByteString.copyFromUtf8("<redacted>");

  private OMPBHelper() {
    /** Hidden constructor */
  }

  /**
   * Converts Ozone delegation token to @{@link TokenProto}.
   * @return tokenProto
   */
  public static TokenProto convertToTokenProto(Token<?> tok) {
    if (tok == null) {
      throw new IllegalArgumentException("Invalid argument: token is null");
    }

    return TokenProto.newBuilder().
        setIdentifier(getByteString(tok.getIdentifier())).
        setPassword(getByteString(tok.getPassword())).
        setKind(tok.getKind().toString()).
        setService(tok.getService().toString()).build();
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  /**
   * Converts @{@link TokenProto} to Ozone delegation token.
   *
   * @return Ozone
   */
  public static Token<OzoneTokenIdentifier> convertToDelegationToken(
      TokenProto tokenProto) {
    return new Token<>(tokenProto.getIdentifier()
        .toByteArray(), tokenProto.getPassword().toByteArray(), new Text(
        tokenProto.getKind()), new Text(tokenProto.getService()));
  }

  public static BucketEncryptionKeyInfo convert(
      BucketEncryptionInfoProto beInfo) {
    if (beInfo == null) {
      throw new IllegalArgumentException("Invalid argument: bucket encryption" +
          " info is null");
    }

    return new BucketEncryptionKeyInfo(
        beInfo.hasCryptoProtocolVersion() ?
            convert(beInfo.getCryptoProtocolVersion()) : null,
        beInfo.hasSuite() ? convert(beInfo.getSuite()) : null,
        beInfo.getKeyName());
  }


  public static BucketEncryptionInfoProto convert(
      BucketEncryptionKeyInfo beInfo) {
    if (beInfo == null || beInfo.getKeyName() == null) {
      throw new IllegalArgumentException("Invalid argument: bucket encryption" +
          " info is null");
    }

    BucketEncryptionInfoProto.Builder bb = BucketEncryptionInfoProto
        .newBuilder().setKeyName(beInfo.getKeyName());

    if (beInfo.getSuite() != null) {
      bb.setSuite(convert(beInfo.getSuite()));
    }
    if (beInfo.getVersion() != null) {
      bb.setCryptoProtocolVersion(convert(beInfo.getVersion()));
    }
    return bb.build();
  }

  public static FileEncryptionInfoProto convert(
      FileEncryptionInfo info) {
    if (info == null) {
      return null;
    }
    return OzoneManagerProtocolProtos.FileEncryptionInfoProto.newBuilder()
        .setSuite(convert(info.getCipherSuite()))
        .setCryptoProtocolVersion(convert(info.getCryptoProtocolVersion()))
        .setKey(getByteString(info.getEncryptedDataEncryptionKey()))
        .setIv(getByteString(info.getIV()))
        .setEzKeyVersionName(info.getEzKeyVersionName())
        .setKeyName(info.getKeyName())
        .build();
  }

  public static FileEncryptionInfo convert(FileEncryptionInfoProto proto) {
    if (proto == null) {
      return null;
    }
    CipherSuite suite = convert(proto.getSuite());
    CryptoProtocolVersion version = convert(proto.getCryptoProtocolVersion());
    byte[] key = proto.getKey().toByteArray();
    byte[] iv = proto.getIv().toByteArray();
    String ezKeyVersionName = proto.getEzKeyVersionName();
    String keyName = proto.getKeyName();
    return new FileEncryptionInfo(suite, version, key, iv, keyName,
        ezKeyVersionName);
  }

  public static DefaultReplicationConfig convert(
      HddsProtos.DefaultReplicationConfig defaultReplicationConfig) {
    if (defaultReplicationConfig == null) {
      throw new IllegalArgumentException(
          "Invalid argument: default replication config" + " is null");
    }

    final ReplicationType type =
        ReplicationType.fromProto(defaultReplicationConfig.getType());
    DefaultReplicationConfig defaultReplicationConfigObj = null;
    switch (type) {
    case EC:
      defaultReplicationConfigObj = new DefaultReplicationConfig(type,
          new ECReplicationConfig(
              defaultReplicationConfig.getEcReplicationConfig()));
      break;
    default:
      final ReplicationFactor factor =
          ReplicationFactor.fromProto(defaultReplicationConfig.getFactor());
      defaultReplicationConfigObj = new DefaultReplicationConfig(type, factor);
    }
    return defaultReplicationConfigObj;
  }

  public static HddsProtos.DefaultReplicationConfig convert(
      DefaultReplicationConfig defaultReplicationConfig) {
    if (defaultReplicationConfig == null) {
      throw new IllegalArgumentException(
          "Invalid argument: default replication config" + " is null");
    }

    final HddsProtos.DefaultReplicationConfig.Builder builder =
        HddsProtos.DefaultReplicationConfig.newBuilder();
    builder.setType(ReplicationType.toProto(
        defaultReplicationConfig.getType()));

    if (defaultReplicationConfig.getFactor() != null) {
      builder.setFactor(ReplicationFactor.toProto(
          defaultReplicationConfig.getFactor()));
    }

    if (defaultReplicationConfig.getEcReplicationConfig() != null) {
      builder.setEcReplicationConfig(
          defaultReplicationConfig.getEcReplicationConfig().toProto());
    }

    return builder.build();
  }

  public static FileChecksum convert(FileChecksumProto proto)
      throws IOException {
    if (proto == null) {
      return null;
    }

    switch (proto.getChecksumType()) {
    case MD5CRC:
      if (proto.hasMd5Crc()) {
        return convertMD5MD5FileChecksum(proto.getMd5Crc());
      }
      throw new IOException("The field md5Crc is not set.");
    case COMPOSITE_CRC:
      if (proto.hasCompositeCrc()) {
        return convertCompositeCrcChecksum(proto.getCompositeCrc());
      }
      throw new IOException("The field CompositeCrc is not set.");
    default:
      throw new IOException("Unexpected checksum type" +
          proto.getChecksumType());
    }
  }

  public static MD5MD5CRC32FileChecksum convertMD5MD5FileChecksum(
      MD5MD5Crc32FileChecksumProto proto) throws IOException {
    ChecksumTypeProto checksumTypeProto = proto.getChecksumType();
    int bytesPerCRC = proto.getBytesPerCRC();
    long crcPerBlock = proto.getCrcPerBlock();
    ByteString md5 = proto.getMd5();
    DataInputStream inputStream = new DataInputStream(
        new ByteArrayInputStream(md5.toByteArray()));
    MD5Hash md5Hash = MD5Hash.read(inputStream);
    switch (checksumTypeProto) {
    case CHECKSUM_CRC32:
      return new MD5MD5CRC32GzipFileChecksum(bytesPerCRC, crcPerBlock, md5Hash);
    case CHECKSUM_CRC32C:
      return new MD5MD5CRC32CastagnoliFileChecksum(bytesPerCRC, crcPerBlock,
          md5Hash);
    default:
      throw new IOException("Unexpected checksum type " + checksumTypeProto);
    }
  }

  public static CompositeCrcFileChecksum convertCompositeCrcChecksum(
      CompositeCrcFileChecksumProto proto) throws IOException {
    ChecksumTypeProto checksumTypeProto = proto.getChecksumType();
    int bytesPerCRC = proto.getBytesPerCrc();
    int crc = proto.getCrc();
    switch (checksumTypeProto) {
    case CHECKSUM_CRC32:
      return new CompositeCrcFileChecksum(
          crc, DataChecksum.Type.CRC32, bytesPerCRC);
    case CHECKSUM_CRC32C:
      return new CompositeCrcFileChecksum(
          crc, DataChecksum.Type.CRC32C, bytesPerCRC);
    default:
      throw new IOException("Unexpected checksum type " + checksumTypeProto);
    }
  }

  public static MD5MD5Crc32FileChecksumProto convert(
      MD5MD5CRC32FileChecksum checksum)
      throws IOException {
    ChecksumTypeProto type;
    switch (checksum.getCrcType()) {
    case CRC32:
      type = ChecksumTypeProto.CHECKSUM_CRC32;
      break;
    case CRC32C:
      type = ChecksumTypeProto.CHECKSUM_CRC32C;
      break;
    default:
      type = ChecksumTypeProto.CHECKSUM_NULL;
    }

    DataOutputBuffer buf = new DataOutputBuffer();
    checksum.write(buf);
    byte[] bytes = buf.getData();
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(bytes, 0, bytes.length);
    int bytesPerCRC = buffer.readInt();
    long crcPerBlock = buffer.readLong();
    buffer.close();

    int offset = Integer.BYTES + Long.BYTES;
    ByteString byteString = ByteString.copyFrom(
        bytes, offset, bytes.length - offset);

    return MD5MD5Crc32FileChecksumProto.newBuilder()
        .setChecksumType(type)
        .setBytesPerCRC(bytesPerCRC)
        .setCrcPerBlock(crcPerBlock)
        .setMd5(byteString)
        .build();
  }

  public static CompositeCrcFileChecksumProto convert(
      CompositeCrcFileChecksum checksum)
      throws IOException {
    ChecksumTypeProto type;
    Options.ChecksumOpt opt = checksum.getChecksumOpt();
    switch (opt.getChecksumType()) {
    case CRC32:
      type = ChecksumTypeProto.CHECKSUM_CRC32;
      break;
    case CRC32C:
      type = ChecksumTypeProto.CHECKSUM_CRC32C;
      break;
    default:
      type = ChecksumTypeProto.CHECKSUM_NULL;
    }
    int crc = CrcUtil.readInt(checksum.getBytes(), 0);
    return CompositeCrcFileChecksumProto.newBuilder()
        .setChecksumType(type)
        .setBytesPerCrc(opt.getBytesPerChecksum())
        .setCrc(crc)
        .build();
  }

  public static FileChecksumProto convert(FileChecksum checksum) {
    if (checksum == null) {
      return null;
    }

    try {
      if (checksum instanceof MD5MD5CRC32FileChecksum) {
        MD5MD5Crc32FileChecksumProto c1 =
            convert((MD5MD5CRC32FileChecksum) checksum);

        return FileChecksumProto.newBuilder()
            .setChecksumType(FileChecksumTypeProto.MD5CRC)
            .setMd5Crc(c1)
            .build();
      } else if (checksum instanceof CompositeCrcFileChecksum) {
        CompositeCrcFileChecksumProto c2 =
            convert((CompositeCrcFileChecksum) checksum);

        return FileChecksumProto.newBuilder()
            .setChecksumType(FileChecksumTypeProto.COMPOSITE_CRC)
            .setCompositeCrc(c2)
            .build();
      } else {
        LOG.warn("Unsupported file checksum runtime type " +
            checksum.getClass().getName());
      }
    } catch (IOException ioe) {
      LOG.warn(
          "Failed to convert a FileChecksum {} to its protobuf representation",
          checksum, ioe);
    }
    return null;
  }

  public static CipherSuite convert(CipherSuiteProto proto) {
    switch (proto) {
    case AES_CTR_NOPADDING:
      return CipherSuite.AES_CTR_NOPADDING;
    default:
      // Set to UNKNOWN and stash the unknown enum value
      CipherSuite suite = CipherSuite.UNKNOWN;
      suite.setUnknownValue(proto.getNumber());
      return suite;
    }
  }

  public static CipherSuiteProto convert(CipherSuite suite) {
    switch (suite) {
    case UNKNOWN:
      return CipherSuiteProto.UNKNOWN;
    case AES_CTR_NOPADDING:
      return CipherSuiteProto.AES_CTR_NOPADDING;
    default:
      return null;
    }
  }

  public static CryptoProtocolVersionProto convert(
      CryptoProtocolVersion version) {
    switch (version) {
    case UNKNOWN:
      return OzoneManagerProtocolProtos.CryptoProtocolVersionProto
          .UNKNOWN_PROTOCOL_VERSION;
    case ENCRYPTION_ZONES:
      return OzoneManagerProtocolProtos.CryptoProtocolVersionProto
          .ENCRYPTION_ZONES;
    default:
      return null;
    }
  }

  public static CryptoProtocolVersion convert(
      CryptoProtocolVersionProto proto) {
    switch (proto) {
    case ENCRYPTION_ZONES:
      return CryptoProtocolVersion.ENCRYPTION_ZONES;
    default:
      // Set to UNKNOWN and stash the unknown enum value
      CryptoProtocolVersion version = CryptoProtocolVersion.UNKNOWN;
      version.setUnknownValue(proto.getNumber());
      return version;
    }
  }


  public static OMRequest processForDebug(OMRequest msg) {
    return msg;
  }

  public static OMResponse processForDebug(OMResponse msg) {
    if (msg == null) {
      return null;
    }

    if (msg.hasDbUpdatesResponse()) {
      OMResponse.Builder builder = msg.toBuilder();

      builder.getDbUpdatesResponseBuilder()
          .clearData().addData(REDACTED);

      return builder.build();
    }

    return msg;
  }
}
