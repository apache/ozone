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

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.ozone.OzoneAcl;

/** Helps incrementally build a list of ACLs. */
public final class AclListBuilder {

  /** The original list being built from, used if no changes are made, to reduce copying. */
  private final ImmutableList<OzoneAcl> originalList;
  /** The updated list being built, created lazily on the first modification. */
  private List<OzoneAcl> updatedList;
  /** Whether any changes were made. */
  private boolean changed;

  public static AclListBuilder empty() {
    return new AclListBuilder(ImmutableList.of());
  }

  public static AclListBuilder of(ImmutableList<OzoneAcl> list) {
    return new AclListBuilder(list);
  }

  public static AclListBuilder copyOf(List<OzoneAcl> list) {
    return new AclListBuilder(list == null ? ImmutableList.of() : ImmutableList.copyOf(list));
  }

  private AclListBuilder(ImmutableList<OzoneAcl> list) {
    originalList = list;
  }

  public boolean isChanged() {
    return changed;
  }

  public ImmutableList<OzoneAcl> build() {
    return changed ? ImmutableList.copyOf(updatedList) : originalList;
  }

  public boolean add(@Nonnull OzoneAcl acl) {
    Objects.requireNonNull(acl, "acl == null");
    ensureInitialized();
    boolean added = OzoneAclUtil.addAcl(updatedList, acl);
    changed |= added;
    return added;
  }

  public boolean addAll(@Nullable List<OzoneAcl> newAcls) {
    if (newAcls == null || newAcls.isEmpty()) {
      return false;
    }
    ensureInitialized();
    boolean added = OzoneAclUtil.addAllAcl(updatedList, newAcls);
    changed |= added;
    return added;
  }

  /** Set the list being built to {@code acls}.  For further mutations to work, it must be modifiable. */
  public boolean set(@Nonnull List<OzoneAcl> acls) {
    Objects.requireNonNull(acls, "acls == null");
    boolean set = !acls.equals(updatedList != null ? updatedList : originalList);
    changed |= set;
    updatedList = acls;
    return set;
  }

  public boolean remove(@Nonnull OzoneAcl acl) {
    Objects.requireNonNull(acl, "acl == null");
    ensureInitialized();
    boolean removed = OzoneAclUtil.removeAcl(updatedList, acl);
    changed |= removed;
    return removed;
  }

  private void ensureInitialized() {
    if (updatedList == null) {
      updatedList = new ArrayList<>(originalList);
    }
  }

  @Override
  public String toString() {
    return build().toString();
  }
}
