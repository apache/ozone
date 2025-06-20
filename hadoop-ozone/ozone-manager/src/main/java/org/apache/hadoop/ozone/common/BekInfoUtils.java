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

package org.apache.hadoop.ozone.common;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CryptoProtocolVersionProto.ENCRYPTION_ZONES;

import java.io.IOException;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketEncryptionInfoProto;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;

/**
 * Utility class for common bucket encryption key operations.
 */
public final class BekInfoUtils {

  private BekInfoUtils() {
  }

  public static BucketEncryptionInfoProto getBekInfo(
      KeyProviderCryptoExtension kmsProvider, BucketEncryptionInfoProto bek)
      throws IOException {
    BucketEncryptionInfoProto.Builder bekb = null;
    if (kmsProvider == null) {
      throw new OMException("Invalid KMS provider, check configuration " +
          CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
          OMException.ResultCodes.INVALID_KMS_PROVIDER);
    }
    if (bek.getKeyName() == null) {
      throw new OMException("Bucket encryption key needed.", OMException
          .ResultCodes.BUCKET_ENCRYPTION_KEY_NOT_FOUND);
    }
    // Talk to KMS to retrieve the bucket encryption key info.
    KeyProvider.Metadata metadata = kmsProvider.getMetadata(
        bek.getKeyName());
    if (metadata == null) {
      throw new OMException("Bucket encryption key " + bek.getKeyName()
          + " doesn't exist.",
          OMException.ResultCodes.BUCKET_ENCRYPTION_KEY_NOT_FOUND);
    }
    // If the provider supports pool for EDEKs, this will fill in the pool
    kmsProvider.warmUpEncryptedKeys(bek.getKeyName());
    bekb = BucketEncryptionInfoProto.newBuilder()
        .setKeyName(bek.getKeyName())
        .setCryptoProtocolVersion(ENCRYPTION_ZONES)
        .setSuite(OMPBHelper.convert(
            CipherSuite.convert(metadata.getCipher())));
    return bekb.build();
  }
}
