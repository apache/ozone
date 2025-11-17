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

package org.apache.hadoop.ozone.om.fs;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.IOzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;

/**
 * Ozone Manager FileSystem interface.
 */
public interface OzoneManagerFS extends IOzoneAcl {

  /**
   * Get file status for a file or a directory.
   *
   * @param args          the args of the key provided by client.
   * @return file status.
   * @throws IOException if file or bucket or volume does not exist
   */
  OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException;

  /**
   * Get file status for a file or a directory.
   *
   * @param args          the args of the key provided by client.
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return file status.
   * @throws IOException if file or bucket or volume does not exist
   */
  OzoneFileStatus getFileStatus(OmKeyArgs args, String clientAddress)
          throws IOException;

  /**
   * Look up a file. Return the info of the file to client side.
   *
   * @param args the args of the key provided by client.
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return a OmKeyInfo instance client uses to talk to container.
   * @throws IOException
   */
  OmKeyInfo lookupFile(OmKeyArgs args, String clientAddress) throws IOException;

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param keyArgs       the args of the key provided by client.
   * @param recursive     For a directory if true all the descendants of a
   *                      particular directory are listed
   * @param startKey      Key from which listing needs to start. If startKey
   *                      exists its status is included in the final list.
   * @param numEntries    Number of entries to list from the start key
   * @return list of file status
   * @throws IOException if file or bucket or volume does not exist
   */
  List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive,
                                   String startKey, long numEntries)
          throws IOException;

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param keyArgs       the args of the key provided by client.
   * @param recursive     For a directory if true all the descendants of a
   *                      particular directory are listed
   * @param startKey      Key from which listing needs to start. If startKey
   *                      exists its status is included in the final list.
   * @param numEntries    Number of entries to list from the start key
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return list of file status
   * @throws IOException if file or bucket or volume does not exist
   */
  List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive,
      String startKey, long numEntries, String clientAddress)
          throws IOException;

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param keyArgs       the args of the key provided by client.
   * @param recursive     For a directory if true all the descendants of a
   *                      particular directory are listed
   * @param startKey      Key from which listing needs to start. If startKey
   *                      exists its status is included in the final list.
   * @param numEntries    Number of entries to list from the start key
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @param allowPartialPrefixes if partial prefixes should be allowed,
   *                             this is needed in context of ListKeys
   * @return list of file status
   * @throws IOException if file or bucket or volume does not exist
   */
  List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive,
      String startKey, long numEntries, String clientAddress,
      boolean allowPartialPrefixes)
          throws IOException;
}
