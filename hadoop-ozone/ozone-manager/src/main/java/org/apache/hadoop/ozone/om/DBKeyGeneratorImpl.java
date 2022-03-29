/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.eclipse.jetty.util.StringUtil;

import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * DBKeyPathGenerator implementation for Object Store and Legacy buckets.
 */
public class DBKeyGeneratorImpl implements DBKeyGenerator {

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOzoneDBKey(OmKeyArgs keyArgs, OzoneManager ozoneManager,
                              String errMsg) {
    return getOzoneDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOzoneDBKey(OzoneManagerProtocolProtos.KeyArgs keyArgs,
                              OzoneManager ozoneManager, String errMsg)
      throws IOException {
    return getOzoneDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOzoneOpenDBKey(OmKeyArgs keyArgs,
                                  OzoneManager ozoneManager,
                                  long clientId, String errMsg) {

    return getOzoneOpenDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), clientId);
  }

  @Override
  public String getOzoneOpenDBKey(OzoneManagerProtocolProtos.KeyArgs keyArgs,
                                  OzoneManager ozoneManager, long clientId,
                                  String errMsg) throws IOException {
    return getOzoneOpenDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), clientId);
  }

  /**
   * Generate the DB key to query keyTable.
   * Key format: /volumeName/bucketName/keyName
   *
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param keyName    key name
   * @return DB key
   */
  private String getOzoneDBKey(String volumeName, String bucketName,
                               String keyName) {
    StringBuilder builder = new StringBuilder()
        .append(OM_KEY_PREFIX)
        .append(volumeName);

    // TODO : Throw if the Bucket is null?
    builder.append(OM_KEY_PREFIX).append(bucketName);

    if (StringUtil.isNotBlank(keyName)) {
      builder.append(OM_KEY_PREFIX);
      if (!keyName.equals(OM_KEY_PREFIX)) {
        builder.append(keyName);
      }
    }
    return builder.toString();
  }

  /**
   * Generate the DB key to query OpenKeyTable.
   * Key format: /volumeName/bucketName/keyName/clientId
   *
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param keyName    key name
   * @param clientId   request client id
   * @return DB key
   */
  private String getOzoneOpenDBKey(String volumeName, String bucketName,
                                   String keyName, long clientId) {
    String openKey = OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName +
        OM_KEY_PREFIX + keyName + OM_KEY_PREFIX + clientId;

    return openKey;
  }
}
