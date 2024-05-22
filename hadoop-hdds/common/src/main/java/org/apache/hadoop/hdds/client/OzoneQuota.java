/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.client;

import com.google.common.base.Strings;


/**
 * represents an OzoneQuota Object that can be applied to
 * a storage volume.
 */
public final class OzoneQuota {

  public static final String OZONE_QUOTA_B = "B";
  public static final String OZONE_QUOTA_KB = "KB";
  public static final String OZONE_QUOTA_MB = "MB";
  public static final String OZONE_QUOTA_GB = "GB";
  public static final String OZONE_QUOTA_TB = "TB";

  /** Quota Units.*/
  public enum Units { B, KB, MB, GB, TB }

  // Quota to decide how many buckets can be created.
  private long quotaInNamespace;
  // Quota to decide how many storage space will be used in bytes.
  private final long quotaInBytes;
  private final RawQuotaInBytes rawQuotaInBytes;
  // Data class of Quota.
  private static final QuotaList QUOTA_LIST = new QuotaList();

  /**
   * Used to convert user input values into bytes such as: 1MB-> 1048576.
   */
  private static class RawQuotaInBytes {
    private final Units unit;
    private final long size;

    RawQuotaInBytes(Units unit, long size) {
      this.unit = unit;
      this.size = size;
    }

    public Units getUnit() {
      return unit;
    }

    public long getSize() {
      return size;
    }

    /**
     * Returns size in Bytes or negative num if there is no Quota.
     */
    public long sizeInBytes() {
      long sQuota = -1L;
      for (Units quota : QUOTA_LIST.getUnitQuotaArray()) {
        if (quota == this.unit) {
          sQuota = QUOTA_LIST.getQuotaSize(quota);
          break;
        }
      }
      return this.getSize() * sQuota;
    }

    @Override
    public String toString() {
      return size + " " + unit;
    }

  }

  /**
   * Returns size.
   *
   * @return long
   */
  public long getRawSize() {
    return this.rawQuotaInBytes.getSize();
  }

  /**
   * Returns Units.
   *
   * @return Unit in MB, GB or TB
   */
  public Units getUnit() {
    return this.rawQuotaInBytes.getUnit();
  }

  /**
   * Constructor for Ozone Space Quota.
   *
   * @param rawQuotaInBytes RawQuotaInBytes value
   */
  private OzoneQuota(RawQuotaInBytes rawQuotaInBytes) {
    this.rawQuotaInBytes = rawQuotaInBytes;
    this.quotaInBytes = rawQuotaInBytes.sizeInBytes();
  }

  /**
   * Constructor for Ozone Quota.
   *
   * @param quotaInNamespace ozone quota in counts
   * @param rawQuotaInBytes RawQuotaInBytes value
   */
  private OzoneQuota(long quotaInNamespace, RawQuotaInBytes rawQuotaInBytes) {
    this.quotaInNamespace = quotaInNamespace;
    this.rawQuotaInBytes = rawQuotaInBytes;
    this.quotaInBytes = rawQuotaInBytes.sizeInBytes();
  }

  /**
   * Parses a user provided string space quota and returns the
   * Quota Object.
   *
   * @param quotaInBytes ozone quota in bytes
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota parseSpaceQuota(String quotaInBytes) {

    if (Strings.isNullOrEmpty(quotaInBytes)) {
      throw new IllegalArgumentException(
          "Quota string cannot be null or empty.");
    }

    String uppercase = quotaInBytes.toUpperCase()
        .replaceAll("\\s+", "");
    String size = "";
    long nSize = 0;
    Units currUnit = Units.B;

    try {
      for (String quota : QUOTA_LIST.getOzoneQuotaArray()) {
        if (uppercase.endsWith((quota))) {
          size = uppercase
              .substring(0, uppercase.length() - quota.length());
          currUnit = QUOTA_LIST.getUnits(quota);
          break;
        }
      }
      // there might be no unit specified.
      if (size.equals("")) {
        size = uppercase;
      }
      nSize = Long.parseLong(size);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(quotaInBytes + " is invalid. " +
          "The quota value should be a positive integer " +
          "with byte numeration(B, KB, MB, GB and TB)");
    }

    if (nSize <= 0) {
      throw new IllegalArgumentException("Invalid value for space quota: "
          + nSize);
    }

    return new OzoneQuota(new RawQuotaInBytes(currUnit, nSize));
  }

  /**
   * Parses a user provided string Namespace quota and returns the
   * Quota Object.
   *
   * @param quotaInNamespace ozone quota in counts
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota parseNameSpaceQuota(String quotaInNamespace) {
    if (Strings.isNullOrEmpty(quotaInNamespace)) {
      throw new IllegalArgumentException(
          "Quota string cannot be null or empty.");
    }
    try {
      long nameSpaceQuota = Long.parseLong(quotaInNamespace);
      if (nameSpaceQuota <= 0) {
        throw new IllegalArgumentException(
            "Invalid value for namespace quota: " + nameSpaceQuota);
      }
      return new OzoneQuota(nameSpaceQuota, new RawQuotaInBytes(Units.B, -1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(quotaInNamespace + " is invalid. " +
          "The quota value should be a positive integer");
    }
  }

  /**
   * Parses a user provided string and returns the
   * Quota Object.
   *
   * @param quotaInBytes ozone quota in bytes
   * @param quotaInNamespace ozone quota in counts
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota parseQuota(String quotaInBytes,
      String quotaInNamespace) {
    return new OzoneQuota(parseNameSpaceQuota(quotaInNamespace)
        .quotaInNamespace, parseSpaceQuota(quotaInBytes).rawQuotaInBytes);
  }

  /**
   * Returns OzoneQuota corresponding to size in bytes.
   *
   * @param quotaInBytes in bytes to be converted
   * @param quotaInNamespace in counts to be converted
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota getOzoneQuota(long quotaInBytes,
      long quotaInNamespace) {
    long size = 1L;
    Units unit = Units.B;
    for (Long quota : QUOTA_LIST.getSizeQuotaArray()) {
      if (quotaInBytes % quota == 0) {
        size = quotaInBytes / quota;
        unit = QUOTA_LIST.getQuotaUnit(quota);
      }
    }
    return new OzoneQuota(quotaInNamespace, new RawQuotaInBytes(unit, size));
  }

  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  @Override
  public String toString() {
    return "Space Bytes Quota: " + rawQuotaInBytes.toString() + "\n" +
        "Counts Quota: " + quotaInNamespace;
  }
}
