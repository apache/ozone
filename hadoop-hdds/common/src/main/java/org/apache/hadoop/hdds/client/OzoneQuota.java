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

  public static final String OZONE_QUOTA_BYTES = "BYTES";
  public static final String OZONE_QUOTA_KB = "KB";
  public static final String OZONE_QUOTA_MB = "MB";
  public static final String OZONE_QUOTA_GB = "GB";
  public static final String OZONE_QUOTA_TB = "TB";

  /** Quota Units.*/
  public enum Units {BYTES, KB, MB, GB, TB}

  // Quota to decide how many buckets can be created.
  private long quotaInCounts;
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
    quotaList.addQuotaList(OZONE_QUOTA_BYTES, Units.BYTES, 1L);
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
   * Constructor for Ozone Quota.
   *
   * @param quotaInCounts Volume quota in counts
   * @param rawQuotaInBytes RawQuotaInBytes value
   */
  private OzoneQuota(long quotaInCounts, RawQuotaInBytes rawQuotaInBytes) {
    this.quotaInCounts = quotaInCounts;
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
   * Parses a user provided string and returns the
   * Quota Object.
   *
   * @param quotaInBytes Volume quota in bytes
   * @param quotaInCounts Volume quota in counts
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota parseQuota(String quotaInBytes,
      long quotaInCounts) {

    if (Strings.isNullOrEmpty(quotaInBytes)) {
      throw new IllegalArgumentException(
          "Quota string cannot be null or empty.");
    }

    String uppercase = quotaInBytes.toUpperCase()
        .replaceAll("\\s+", "");
    String size = "";
    long nSize = 0;
    Units currUnit = Units.MB;
    long quotaMultiplyExact = 0;

    try {
      for (String quota : quotaList.getOzoneQuotaArray()) {
        if (uppercase.endsWith((quota))) {
          size = uppercase
              .substring(0, uppercase.length() - quota.length());
          currUnit = quotaList.getUnits(quota);
          quotaMultiplyExact = Math.multiplyExact(Long.parseLong(size),
              quotaList.getQuotaSize(currUnit));
          break;
        }
      }
      nSize = Long.parseLong(size);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid values for quota, to ensure" +
          " that the Quota format is legal(supported values are BYTES, " +
          " KB, MB, GB and TB).");
    } catch (ArithmeticException e) {
      LOG.debug("long overflow:\n{}", quotaMultiplyExact);
      throw new IllegalArgumentException("Invalid values for quota, the quota" +
          " value cannot be greater than Long.MAX_VALUE BYTES");
    }

    if (nSize < 0) {
      throw new IllegalArgumentException("Quota cannot be negative.");
    }

    return new OzoneQuota(quotaInCounts,
        new RawQuotaInBytes(currUnit, nSize));
  }


  /**
   * Returns OzoneQuota corresponding to size in bytes.
   *
   * @param quotaInBytes in bytes to be converted
   * @param quotaInCounts in counts to be converted
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota getOzoneQuota(long quotaInBytes,
      long quotaInCounts) {
    long size = 1L;
    Units unit = Units.BYTES;
    for (Long quota : quotaList.getSizeQuotaArray()) {
      if (quotaInBytes % quota == 0) {
        size = quotaInBytes / quota;
        unit = quotaList.getQuotaUnit(quota);
      }
    }
    return new OzoneQuota(quotaInCounts, new RawQuotaInBytes(unit, size));
  }

  public long getQuotaInCounts() {
    return quotaInCounts;
  }

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  @Override
  public String toString() {
    return "Space Bytes Quota: " + rawQuotaInBytes.toString() + "\n" +
        "Counts Quota: " + quotaInCounts;
  }
}
