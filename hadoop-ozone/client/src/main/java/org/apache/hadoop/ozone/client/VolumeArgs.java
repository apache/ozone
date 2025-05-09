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

package org.apache.hadoop.ozone.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class encapsulates the arguments that are
 * required for creating a volume.
 */
@Immutable
public final class VolumeArgs {

  private final String admin;
  private final String owner;
  private final long quotaInBytes;
  private final long quotaInNamespace;
  private final ImmutableList<OzoneAcl> acls;
  private final ImmutableMap<String, String> metadata;

  /**
   * Private constructor, constructed via builder.
   * @param admin Administrator's name.
   * @param owner Volume owner's name
   * @param quotaInBytes Volume quota in bytes.
   * @param quotaInNamespace Volume quota in counts.
   * @param acls User to access rights map.
   * @param metadata Metadata of volume.
   */
  private VolumeArgs(String admin,
      String owner,
      long quotaInBytes,
      long quotaInNamespace,
      List<OzoneAcl> acls,
      Map<String, String> metadata) {
    this.admin = admin;
    this.owner = owner;
    this.quotaInBytes = quotaInBytes;
    this.quotaInNamespace = quotaInNamespace;
    this.acls = acls == null ? ImmutableList.of() : ImmutableList.copyOf(acls);
    this.metadata = metadata == null ? ImmutableMap.of() : ImmutableMap.copyOf(metadata);
  }

  /**
   * Returns the Admin Name.
   * @return String.
   */
  public String getAdmin() {
    return admin;
  }

  /**
   * Returns the owner Name.
   * @return String
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns Volume Quota in bytes.
   * @return quotaInBytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns Volume Quota in bucket counts.
   * @return quotaInNamespace.
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  /**
   * Return custom key value map.
   *
   * @return metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
  }

  public static VolumeArgs.Builder newBuilder() {
    return new VolumeArgs.Builder();
  }

  /**
   * Builder for VolumeArgs.
   */
  public static class Builder {
    private String adminName;
    private String ownerName;
    private long quotaInBytes = OzoneConsts.QUOTA_RESET;
    private long quotaInNamespace = OzoneConsts.QUOTA_RESET;
    private List<OzoneAcl> acls;
    private Map<String, String> metadata;

    public VolumeArgs.Builder setAdmin(String admin) {
      this.adminName = admin;
      return this;
    }

    public VolumeArgs.Builder setOwner(String owner) {
      this.ownerName = owner;
      return this;
    }

    public VolumeArgs.Builder setQuotaInBytes(long quota) {
      this.quotaInBytes = quota;
      return this;
    }

    public VolumeArgs.Builder setQuotaInNamespace(long quota) {
      this.quotaInNamespace = quota;
      return this;
    }

    public VolumeArgs.Builder addMetadata(String key, String value) {
      if (metadata == null) {
        metadata = new HashMap<>();
      }
      metadata.put(key, value);
      return this;
    }

    public VolumeArgs.Builder addAcl(OzoneAcl acl)
        throws IOException {
      if (acls == null) {
        acls = new ArrayList<>();
      }
      acls.add(acl);
      return this;
    }

    /**
     * Constructs a CreateVolumeArgument.
     * @return CreateVolumeArgs.
     */
    public VolumeArgs build() {
      return new VolumeArgs(adminName, ownerName, quotaInBytes,
          quotaInNamespace, acls, metadata);
    }
  }

}
