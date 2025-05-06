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

package org.apache.hadoop.hdds.scm.node.states;

import org.apache.hadoop.hdds.protocol.DatanodeID;

/**
 * This exception represents that the node that is being accessed does not
 * exist in NodeStateMap.
 */
public class NodeNotFoundException extends NodeException {

  /**
   * Constructs an {@code NodeNotFoundException} with {@code null}
   * as its error detail message.
   */
  public NodeNotFoundException() {
    super();
  }

  /**
   * Constructs an {@code NodeNotFoundException} with the given {@link DatanodeID}.
   */
  public NodeNotFoundException(DatanodeID datanodeID) {
    super("Datanode " + datanodeID + " not found");
  }

}
