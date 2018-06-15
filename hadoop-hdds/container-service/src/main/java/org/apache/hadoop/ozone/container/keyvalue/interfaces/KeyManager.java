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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.interfaces;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import java.io.IOException;
import java.util.List;

/**
 * KeyManager is for performing key related operations on the container.
 */
public interface KeyManager {

  /**
   * Puts or overwrites a key.
   *
   * @param container - Container for which key need to be added.
   * @param data     - Key Data.
   * @throws IOException
   */
  void putKey(Container container, KeyData data) throws IOException;

  /**
   * Gets an existing key.
   *
   * @param container - Container from which key need to be get.
   * @param data - Key Data.
   * @return Key Data.
   * @throws IOException
   */
  KeyData getKey(Container container, KeyData data) throws IOException;

  /**
   * Deletes an existing Key.
   *
   * @param container - Container from which key need to be deleted.
   * @param blockID - ID of the block.
   * @throws StorageContainerException
   */
  void deleteKey(Container container, BlockID blockID) throws IOException;

  /**
   * List keys in a container.
   *
   * @param container - Container from which keys need to be listed.
   * @param startLocalID  - Key to start from, 0 to begin.
   * @param count    - Number of keys to return.
   * @return List of Keys that match the criteria.
   */
  List<KeyData> listKey(Container container, long startLocalID, int count) throws
      IOException;

  /**
   * Shutdown ContainerManager.
   */
  void shutdown();
}
