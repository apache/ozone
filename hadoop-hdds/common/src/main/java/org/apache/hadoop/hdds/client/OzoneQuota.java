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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.KB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.apache.hadoop.ozone.OzoneConsts.TB;


/**
 * represents an OzoneQuota Object that can be applied to
 * a storage volume.
 */
public final class OzoneQuota {
  public static final Logger LOG =
      LoggerFactory.getLogger(OzoneQuota.class);

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
  private long quotaInBytes;
  private RawQuotaInBytes rawQuotaInBytes;
  // Data class of Quota.
  private static QuotaList quotaList;

  /** Setting QuotaList parameters from large to small. */
  static {
    quotaList = new QuotaList();
    quotaList.addQuotaList(OZONE_QUOTA_TB, Units.TB, TB);
    quotaList.addQuotaList(OZONE_QUOTA_GB, Units.GB, GB);
    quotaList.addQuotaList(OZONE_QUOTA_MB, Units.MB, MB);
    quotaList.addQuotaList(OZONE_QUOTA_KB, Units.KB, KB);
    quotaList.addQuotaList(OZONE_QUOTA_B, Units.B, 1L);
  }

  /**
   * Used to convert user input values into bytes such as: 1MB-> 1048576.
   */
  private static class RawQuotaInBytes {
    private Units unit;
    private long size;

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
      for (Units quota : quotaList.getUnitQuotaArray()) {
        if (quota == this.unit) {
          sQuota = quotaList.getQuotaSize(quota);
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
   * Formats a quota as a string.
   *
   * @param quota the quota to format
   * @return string representation of quota
   */
  public static String formatQuota(OzoneQuota quota) {
    return String.valueOf(quota.getRawSize()) + quota.getUnit();
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
      for (String quota : quotaList.getOzoneQuotaArray()) {
        if (uppercase.endsWith((quota))) {
          size = uppercase
              .substring(0, uppercase.length() - quota.length());
          currUnit = quotaList.getUnits(quota);
          break;
        }
      }
      // there might be no unit specified.
      if (size.equals("")) {
        size = uppercase;
      }
      nSize = Long.parseLong(size);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid values for quota, to ensure" +
          " that the Quota format is legal(supported values are B," +
          " KB, MB, GB and TB). And the quota value cannot be greater than " +
          "Long.MAX_VALUE BYTES");
    }

    if (nSize <= 0) {
      throw new IllegalArgumentException("Invalid values for space quota: "
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
    long nameSpaceQuota = Long.parseLong(quotaInNamespace);
    if (nameSpaceQuota <= 0) {
      throw new IllegalArgumentException(
          "Invalid values for namespace quota: " + nameSpaceQuota);
    }
    return new OzoneQuota(nameSpaceQuota, new RawQuotaInBytes(Units.B, -1));
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
    for (Long quota : quotaList.getSizeQuotaArray()) {
      if (quotaInBytes % quota == 0) {
        size = quotaInBytes / quota;
        unit = quotaList.getQuotaUnit(quota);
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
