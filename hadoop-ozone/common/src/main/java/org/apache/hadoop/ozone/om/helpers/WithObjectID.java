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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;

import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Mixin class to handle ObjectID and UpdateID.
 */
@Immutable
public abstract class WithObjectID extends WithMetadata {

  private /*final*/ long objectID;
  private /*final*/ long updateID;

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

  private long multiRaftTerm;

  /**
   * Returns objectID.
   * @return long
   */
  public long getObjectID() {
    return objectID;
  }

  /**
   * UpdateIDs are monotonically increasing values which are updated
   * each time there is an update.
   */
  public final long getUpdateID() {
    return updateID;
  }

  /**
   * Set the Object ID.
   * There is a reason why we cannot use the final here. The object
   * ({@link OmVolumeArgs}/ {@link OmBucketInfo}/ {@link OmKeyInfo}) is
   * deserialized from the protobuf in many places in code. We need to set
   * this object ID, after it is deserialized.
   *
   * @param obId - long
   */
  public void setObjectID(long obId) {
    if (this.objectID != 0 && obId != OBJECT_ID_RECLAIM_BLOCKS) {
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
  public void setUpdateID(
      long updateId, boolean isMultiraftEnabled, long currentMultiraftTerm
  ) {

    // Because in non-HA, we have multiple rpc handler threads and
    // transactionID is generated in OzoneManagerServerSideTranslatorPB.

    // Lets take T1 -> Set Bucket Property
    // T2 -> Set Bucket Acl

    // Now T2 got lock first, so updateID will be set to 2. Now when T1 gets
    // executed we will hit the precondition exception. So for OM non-HA with
    // out ratis we should not have this check.

    // Same can happen after OM restart also.

    // OM Start
    // T1 -> Create Bucket
    // T2 -> Set Bucket Property

    // OM restart
    // T1 -> Set Bucket Acl

    // So when T1 is executing, Bucket will have updateID 2 which is set by T2
    // execution before restart.

    // Main reason, in non-HA transaction Index after restart starts from 0.
    // And also because of this same reason we don't do replay checks in non-HA.
    if ((!isMultiraftEnabled || currentMultiraftTerm == multiRaftTerm)
         && updateId < this.updateID
    ) {
      throw new IllegalArgumentException(String.format(
          "Trying to set updateID to %d which is not greater than the " +
          "current value of %d for %s. Multiraft term: %s", updateId, this.updateID,
          getObjectInfo(), multiRaftTerm));
    }

    if (isMultiraftEnabled && currentMultiraftTerm != multiRaftTerm) {
      this.multiRaftTerm = currentMultiraftTerm;
    }

    this.updateID = updateId;
  }

  public boolean isUpdateIDset() {
    return this.updateID > 0;
  }

  public String getObjectInfo() {
    return this.toString();
  }

  /** Builder for {@link WithObjectID}. */
  public abstract static class Builder<T extends WithObjectID> extends WithMetadata.Builder {
    private final long initialObjectID;
    private final long initialUpdateID;
    private long objectID;
    private long updateID;
    private boolean multiRaftEnabled;
    private long multiRaftTerm;

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

    public Builder<T> setMultiRaftEnabled(boolean multiRaftEnabled) {
      this.multiRaftEnabled = multiRaftEnabled;
      return this;
    }

    public Builder<T> setMultiRaftTerm(long multiRaftTerm) {
      this.multiRaftTerm = multiRaftTerm;
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
      // TODO: move the check above (line 134) here
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
