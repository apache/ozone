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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

/**
 * Protocol for OmMetadataReader's.
 */
public interface IOmMetadataReader {
  /**
   * Look up for the container of an existing key.
   *
   * @param args the args of the key.
   * @return OmKeyInfo instance that client uses to talk to container.
   */
  OmKeyInfo lookupKey(OmKeyArgs args) throws IOException;

  KeyInfoWithVolumeContext getKeyInfo(OmKeyArgs args,
                                      boolean assumeS3Context)
      throws IOException;

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param args    Key args
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @param allowPartialPrefixes if partial prefixes should be allowed,
   *                             this is needed in context of ListKeys
   * @return list of file status
   */
  List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
                                   String startKey, long numEntries,
                                   boolean allowPartialPrefixes)
      throws IOException;

  /**
   * Lightweight listStatus API.
   *
   * @param args    Key args
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @param allowPartialPrefixes if partial prefixes should be allowed,
   *                             this is needed in context of ListKeys
   * @return list of file status
   */
  List<OzoneFileStatusLight> listStatusLight(OmKeyArgs args, boolean recursive,
                                   String startKey, long numEntries,
                                   boolean allowPartialPrefixes)
      throws IOException;

  default List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries)
      throws IOException {
    return listStatus(args, recursive, startKey, numEntries, false);
  }

  /**
   * OzoneFS api to get file status for an entry.
   *
   * @param keyArgs Key args
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OzoneFileStatus getFileStatus(OmKeyArgs keyArgs) throws IOException;

  /**
   * OzoneFS api to lookup for a file.
   *
   * @param args Key args
   * @throws OMException if given key is not found, or it is not a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OmKeyInfo lookupFile(OmKeyArgs args) throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo}
   * in the given bucket. Argument volumeName, bucketName is required,
   * others are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKey
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   */
  ListKeysResult listKeys(String volumeName, String bucketName,
                          String startKey, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Lightweight listKeys implementation.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKey
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  ListKeysLightResult listKeysLight(String volumeName, String bucketName,
                                     String startKey, String keyPrefix,
                                     int maxKeys)
      throws IOException;

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  List<OzoneAcl> getAcl(OzoneObj obj) throws IOException;

  /**
   * Gets the tags for the specified key.
   * @param args Key args
   * @return Tags associated with the key.
   */
  Map<String, String> getObjectTagging(OmKeyArgs args) throws IOException;
}
