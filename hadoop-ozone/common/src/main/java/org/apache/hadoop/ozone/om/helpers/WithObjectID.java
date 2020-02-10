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
package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;

/**
 * Mixin class to handle ObjectID and UpdateID.
 */
public class WithObjectID extends WithMetadata {

  /**
   * ObjectIDs are unique and immutable identifier for each object in the
   * System.
   */
  @SuppressWarnings("visibilitymodifier")
  protected long objectID;
  /**
   * UpdateIDs are monotonically increasing values which are updated
   * each time there is an update.
   */
  @SuppressWarnings("visibilitymodifier")
  protected long updateID;

  /**
   * Returns objectID.
   * @return long
   */
  public long getObjectID() {
    return objectID;
  }

  /**
   * Returns updateID.
   * @return long
   */
  public long getUpdateID() {
    return updateID;
  }

  /**
   * Set the Object ID. If this value is already set then this function throws.
   * There is a reason why we cannot use the final here. The object
   * ({@link OmVolumeArgs}/ {@link OmBucketInfo}/ {@link OmKeyInfo}) is
   * deserialized from the protobuf in many places in code. We need to set
   * this object ID, after it is deserialized.
   *
   * @param obId - long
   */
  public void setObjectID(long obId) {
    if(this.objectID != 0) {
      throw new UnsupportedOperationException("Attempt to modify object ID " +
          "which is not zero. Current Object ID is " + this.objectID);
    }
    this.objectID = obId;
  }

  /**
   * Sets the update ID. For each modification of this object, we will set
   * this to a value greater than the current value.
   * @param updateId  long
   */
  public void setUpdateID(long updateId) {
    Preconditions.checkArgument(updateId >= this.updateID, String.format(
        "Trying to set updateID to %d which is not greater than the current " +
            "value of %d for %s", updateId, this.updateID, getObjectInfo()));
    this.updateID = updateId;
  }

  public boolean isUpdateIDset() {
    return this.updateID > 0;
  }

  public String getObjectInfo() {
    return this.toString();
  }
}
