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

import net.jcip.annotations.Immutable;

/**
 * Object ID with additional parent ID field.
 */
@Immutable
public abstract class WithParentObjectId extends WithObjectID {
  private final long parentObjectID;

  public WithParentObjectId(Builder<?> builder) {
    super(builder);
    parentObjectID = builder.getParentObjectID();
  }

  /**
   * Object ID with additional parent ID field.
   *
   * A pointer to parent directory used for path traversal. ParentID will be
   * used only when the key is created into a FileSystemOptimized(FSO) bucket.
   * <p>
   * For example, if a key "a/b/key1" created into a FSOBucket then each
   * path component will be assigned an ObjectId and linked to its parent path
   * component using parent's objectID.
   * <p>
   * Say, Bucket's ObjectID = 512, which is the parent for its immediate child
   * element.
   * <p>
   * ------------------------------------------|
   * PathComponent |   ObjectID   |   ParentID |
   * ------------------------------------------|
   *      a        |     1024     |     512    |
   * ------------------------------------------|
   *      b        |     1025     |     1024   |
   * ------------------------------------------|
   *     key1      |     1026     |     1025   |
   * ------------------------------------------|
   */
  public final long getParentObjectID() {
    return parentObjectID;
  }

  /** Builder for {@link WithParentObjectId}. */
  public abstract static class Builder<T extends WithParentObjectId> extends WithObjectID.Builder<T> {
    private long parentObjectID;

    protected Builder() {
      super();
    }

    protected Builder(WithParentObjectId obj) {
      super(obj);
      parentObjectID = obj.getParentObjectID();
    }

    public Builder<T> setParentObjectID(long parentObjectId) {
      this.parentObjectID = parentObjectId;
      return this;
    }

    protected long getParentObjectID() {
      return parentObjectID;
    }
  }
}
