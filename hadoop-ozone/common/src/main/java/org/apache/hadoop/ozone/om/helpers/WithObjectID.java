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

import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;

import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Mixin class to handle ObjectID and UpdateID.
 */
@Immutable
public abstract class WithObjectID extends WithMetadata {

  private final long objectID;
  private final long updateID;

  protected WithObjectID() {
    super();
    objectID = 0;
    updateID = 0;
  }

  protected WithObjectID(Builder<?> b) {
    super(b);
    objectID = b.objectID;
    updateID = b.updateID;
  }

  protected WithObjectID(WithObjectID other) {
    super(other);
    objectID = other.objectID;
    updateID = other.updateID;
  }

  /**
   * ObjectIDs are unique and immutable identifier for each object in the
   * System.
   */
  public final long getObjectID() {
    return objectID;
  }

  /**
   * UpdateIDs are monotonically increasing values which are updated
   * each time there is an update.
   */
  public final long getUpdateID() {
    return updateID;
  }

  /** Hook method, customized in subclasses. */
  public String getObjectInfo() {
    return this.toString();
  }

  /** Builder for {@link WithObjectID}. */
  public abstract static class Builder<T extends WithObjectID> extends WithMetadata.Builder {
    private final long initialObjectID;
    private final long initialUpdateID;
    private long objectID;
    private long updateID;

    protected Builder() {
      super();
      initialObjectID = 0;
      initialUpdateID = OzoneConsts.DEFAULT_OM_UPDATE_ID;
    }

    protected Builder(WithObjectID obj) {
      super(obj);
      initialObjectID = obj.getObjectID();
      initialUpdateID = obj.getUpdateID();
      objectID = initialObjectID;
      updateID = initialUpdateID;
    }

    /**
     * Sets the Object ID for this Object.
     * Object ID are unique and immutable identifier for each object in the
     * System.
     */
    public Builder<T> setObjectID(long obId) {
      this.objectID = obId;
      return this;
    }

    /**
     * Sets the update ID for this Object. Update IDs are monotonically
     * increasing values which are updated each time there is an update.
     */
    public Builder<T> setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    public long getObjectID() {
      return objectID;
    }

    public long getUpdateID() {
      return updateID;
    }

    protected void validate() {
      if (initialObjectID != objectID && initialObjectID != 0 && objectID != OBJECT_ID_RECLAIM_BLOCKS) {
        throw new UnsupportedOperationException("Attempt to modify object ID " +
            "which is not zero. Current Object ID is " + initialObjectID);
      }

      if (updateID < initialUpdateID) {
        throw new IllegalArgumentException(String.format(
            "Trying to set updateID to %d which is not greater than the " +
                "current value of %d for %s", updateID, initialUpdateID,
            buildObject().getObjectInfo()));
      }
    }

    protected abstract T buildObject();

    public final T build() {
      validate();
      return buildObject();
    }
  }
}
