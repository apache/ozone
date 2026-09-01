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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_VERSIONED_KEY_SEPARATOR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLCNoncurrentVersionExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;

/**
 * Decides which of a bucket's noncurrent versions a set of
 * NoncurrentVersionExpiration rules expires.
 *
 * <p>The versionedKeyTable holds a key's versions adjacent and newest first,
 * so one pass gives the position a NewerNoncurrentVersions rule counts:
 * position N is the Nth newest noncurrent version. The moment a version
 * stopped being current is on the record itself, stamped when it was moved
 * out of the keyTable.
 *
 * <p>Separate from the scan that drives it so that what gets selected can be
 * checked without running a lifecycle service.
 */
public class NoncurrentVersionSelector {

  private final OMMetadataManager metadataManager;

  public NoncurrentVersionSelector(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  /**
   * A record the rules expired, named by its dbKey and by the write that
   * produced it.
   *
   * <p>The dbKey alone does not identify it. Scan and reclaim are separated by
   * replication, and a dbKey names a position rather than a record: the key
   * can be rewritten in between, leaving a different record where the scan
   * looked. The updateId is carried so the reclaim can tell the two apart.
   */
  public static final class ExpiredRecord {
    private final String dbKey;
    private final long updateId;

    public ExpiredRecord(String dbKey, long updateId) {
      this.dbKey = dbKey;
      this.updateId = updateId;
    }

    public String getDbKey() {
      return dbKey;
    }

    public long getUpdateId() {
      return updateId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExpiredRecord other = (ExpiredRecord) o;
      return updateId == other.updateId && dbKey.equals(other.dbKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dbKey, updateId);
    }

    @Override
    public String toString() {
      return dbKey + "@" + updateId;
    }
  }

  /**
   * What one pass selected, and where it stopped.
   */
  public static final class Selection {
    private final List<ExpiredRecord> expiredVersions;
    private final String resumeFrom;
    private final long versionsScanned;

    private Selection(List<ExpiredRecord> expiredVersions, String resumeFrom,
        long versionsScanned) {
      this.expiredVersions = expiredVersions;
      this.resumeFrom = resumeFrom;
      this.versionsScanned = versionsScanned;
    }

    /**
     * @return how many versions the pass walked, expired or not. The scan
     *     reports this as the work it did: a pass over versions that all
     *     survive is as much work as one that expires every version it sees.
     */
    public long getVersionsScanned() {
      return versionsScanned;
    }

    /** The versions the rules expire, in the order the pass met them. */
    public List<ExpiredRecord> getExpiredVersions() {
      return expiredVersions;
    }

    /**
     * @return the dbKey to resume the next pass from, always the first version
     *     of a key, or null when the bucket was walked to the end. Never points
     *     into the middle of a key: resuming there would restart the per-key
     *     count and keep more versions than the rule allows.
     */
    public String getResumeFrom() {
      return resumeFrom;
    }

    public boolean isFinished() {
      return resumeFrom == null;
    }
  }

  /**
   * Walks the bucket's noncurrent versions and selects the expired ones.
   *
   * @param bucket the bucket to walk
   * @param rules the rules carrying a NoncurrentVersionExpiration action
   * @param resumeFrom where a previous pass stopped, or null to start over
   * @param limit how many versions to walk before stopping at the next key
   *     boundary; the pass can exceed it by the remainder of the key being
   *     walked, so that a key is never left half-processed. It bounds the
   *     versions read rather than the ones selected, so a bucket whose
   *     versions all survive still yields the scan between passes instead of
   *     being walked to the end in one.
   */
  public Selection select(OmBucketInfo bucket, List<OmLCRule> rules,
      String resumeFrom, int limit) throws IOException {

    // Trailing separator included: without it the prefix of a bucket is also
    // a prefix of every bucket whose name extends it, so scanning one would
    // expire another's versions under this bucket's rules.
    final String bucketPrefix = metadataManager.getBucketKey(
        bucket.getVolumeName(), bucket.getBucketName()) + OM_KEY_PREFIX;
    final List<ExpiredRecord> expired = new ArrayList<>();

    String currentKeyName = null;
    int seenInKey = 0;
    long scanned = 0;

    try (Table.KeyValueIterator<String, OmKeyInfo> versions =
             metadataManager.getVersionedKeyTable().iterator(bucketPrefix)) {
      if (resumeFrom != null) {
        versions.seek(resumeFrom);
      }

      while (versions.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = versions.next();
        final String dbKey = entry.getKey();
        // The versionId suffix is fixed-width lower-case hex, so the last
        // separator is the one that starts it. Searching forward would cut
        // the name short on a key that contains the separator itself.
        final int separator = dbKey.lastIndexOf(OM_VERSIONED_KEY_SEPARATOR);
        if (separator < 0) {
          continue;
        }
        final String keyName =
            dbKey.substring(bucketPrefix.length(), separator);

        if (!keyName.equals(currentKeyName)) {
          // A key boundary, the only place a pass may stop.
          if (scanned >= limit) {
            return new Selection(expired, dbKey, scanned);
          }
          currentKeyName = keyName;
          seenInKey = 0;
        }

        final OmKeyInfo version = entry.getValue();
        scanned++;
        if (isExpired(rules, version, ++seenInKey)) {
          expired.add(new ExpiredRecord(dbKey, version.getUpdateID()));
        }
      }
    }

    // Walked to the end, so the next pass starts over rather than resuming.
    return new Selection(expired, null, scanned);
  }

  /**
   * @return whether a rule that matches this version expires it, either
   *     because enough newer noncurrent versions of the key survive it or
   *     because it has been noncurrent long enough.
   */
  private boolean isExpired(List<OmLCRule> rules, OmKeyInfo version,
      int position) {
    final long becameNoncurrentAt = version.getNoncurrentTime();
    for (OmLCRule rule : rules) {
      // Scope only: whether the version is expired is this action's own
      // decision, not the Expiration action's modification-time check.
      if (!rule.matchesScope(version)) {
        continue;
      }
      final OmLCNoncurrentVersionExpiration expiration =
          rule.getNoncurrentVersionExpiration();
      final Integer keep = expiration.getNewerNoncurrentVersions();
      if (keep != null && position > keep) {
        return true;
      }
      // Every record in the versionedKeyTable is stamped as it is moved
      // there, so an unstamped one means that invariant is broken. Expiring
      // on a broken invariant would destroy data, so it is left alone.
      if (becameNoncurrentAt > 0
          && expiration.isExpired(becameNoncurrentAt)) {
        return true;
      }
    }
    return false;
  }
}
