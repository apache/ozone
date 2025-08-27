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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;

/**
 * Interface that encapsulates lifecycle rule actions.
 * This class serves as a foundation for various action types in lifecycle
 * configuration, such as Expiration and (in the future) Transition.
 */
public interface OmLCAction {

  /**
   * Creates LifecycleAction protobuf from OmLCAction.
   */
  LifecycleAction getProtobuf();

  /**
   * Validates the action configuration.
   * Each concrete action implementation must define its own validation logic.
   *
   * @param creationTime The creation time of the lifecycle configuration in milliseconds since epoch
   * @throws OMException if the validation fails
   */
  void valid(long creationTime) throws OMException;

  /**
   * Returns the action type.
   *
   * @return the type of this action
   */
  ActionType getActionType();

  /**
   * Enum defining supported action types.
   */
  enum ActionType {
    EXPIRATION,
    // Future action types can be added here (e.g., TRANSITION)
  }
}
