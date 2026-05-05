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

package org.apache.hadoop.ozone.security.acl;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;

/**
 * Interface used to lists immediate children(sub-paths) for a given keyPrefix.
 */
public interface OzonePrefixPath {

  /**
   * Returns file status for the given key path.
   *
   * @return OzoneFileStatus for the given path.
   */
  OzoneFileStatus getOzoneFileStatus();

  /**
   * Lists immediate children(files or a directories) of the given keyPrefix.
   * It won't do recursive traversal. The given keyPrefix parameter should be a
   * directory type.
   *
   * Assume following is the Ozone FS tree structure.
   *
   *                  buck-1
   *                    |
   *                    a
   *                    |
   *      -----------------------------------
   *     |           |                       |
   *     b1          b2                      b3
   *   -----       --------               ----------
   *   |    |      |    |   |             |    |     |
   *  c1   c2     d1   d2  d3             e1   e2   e3
   *                   |                  |
   *               --------               |
   *              |        |              |
   *           d21.txt   d22.txt        e11.txt
   *
   * Say, KeyPrefix = "a" will return immediate children [a/b1, a/b2, a/b3].
   * Say, KeyPrefix = "a/b2" will return children [a/b2/d1, a/b2/d2, a/b2/d3].
   *
   * @param keyPrefix  keyPrefix name
   * @return list of immediate files or directories under the given keyPrefix.
   * @throws IOException
   */
  Iterator<? extends OzoneFileStatus> getChildren(String keyPrefix)
      throws IOException;
}
