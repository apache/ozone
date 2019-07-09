/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.fs;

import org.apache.hadoop.ozone.om.IOzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;

import java.io.IOException;
import java.util.List;

/**
 * Ozone Manager FileSystem interface.
 */
public interface OzoneManagerFS extends IOzoneAcl {
  OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException;

  void createDirectory(OmKeyArgs args) throws IOException;

  OpenKeySession createFile(OmKeyArgs args, boolean isOverWrite,
      boolean isRecursive) throws IOException;

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

  List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive,
      String startKey, long numEntries) throws IOException;
}
