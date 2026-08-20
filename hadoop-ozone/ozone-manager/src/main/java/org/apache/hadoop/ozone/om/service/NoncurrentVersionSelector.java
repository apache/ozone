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

import static org.apache.hadoop.ozone.OzoneConsts.OM_VERSIONED_KEY_SEPARATOR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLCNoncurrentVersionExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;

/**
 * Decides which of a bucket's noncurrent versions a set of
 * NoncurrentVersionExpiration rules expires.
 *
 * <p>The versionedKeyTable holds a key's versions adjacent and newest first,
 * so one pass gives both things the rules need: position N is the Nth newest
 * noncurrent version, and the moment a version stopped being current is the
 * moment the version above it was written. For the newest noncurrent version
 * that is the key's current version, which the keyTable holds.
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
   * What one pass selected, and where it stopped.
   */
  public static final class Selection {
    private final List<String> expiredVersionKeys;
    private final String resumeFrom;
    private final long versionsScanned;

    private Selection(List<String> expiredVersionKeys, String resumeFrom,
        long versionsScanned) {
      this.expiredVersionKeys = expiredVersionKeys;
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

    /** versionedKeyTable dbKeys of the versions the rules expire. */
    public List<String> getExpiredVersionKeys() {
      return expiredVersionKeys;
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

    final String bucketPrefix = metadataManager.getBucketKey(
        bucket.getVolumeName(), bucket.getBucketName());
    final List<String> expired = new ArrayList<>();

    String currentKeyName = null;
    int seenInKey = 0;
    long scanned = 0;
    long becameNoncurrentAt = 0L;

    try (Table.KeyValueIterator<String, OmKeyInfo> versions =
             metadataManager.getVersionedKeyTable().iterator(bucketPrefix)) {
      if (resumeFrom != null) {
        versions.seek(resumeFrom);
      }

      while (versions.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = versions.next();
        final String dbKey = entry.getKey();
        final int separator = dbKey.indexOf(OM_VERSIONED_KEY_SEPARATOR);
        if (separator < 0) {
          continue;
        }
        final String keyName =
            dbKey.substring(bucketPrefix.length() + 1, separator);

        if (!keyName.equals(currentKeyName)) {
          // A key boundary, the only place a pass may stop.
          if (scanned >= limit) {
            return new Selection(expired, dbKey, scanned);
          }
          currentKeyName = keyName;
          seenInKey = 0;
          becameNoncurrentAt = supersedingTime(bucket, keyName);
        }

        final OmKeyInfo version = entry.getValue();
        scanned++;
        if (isExpired(rules, version, ++seenInKey, becameNoncurrentAt)) {
          expired.add(dbKey);
        }

        // Walking a key newest first, the version just visited is the one that
        // superseded the next one.
        becameNoncurrentAt = version.getModificationTime();
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
      int position, long becameNoncurrentAt) {
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
      // A version with no superseding time is one whose key has no current
      // version, which breaks the keyTable invariant. Expiring on a broken
      // invariant would destroy data, so it is left alone.
      if (becameNoncurrentAt > 0
          && expiration.isExpired(becameNoncurrentAt)) {
        return true;
      }
    }
    return false;
  }

  /**
   * When the key's current version was committed, which is the moment the
   * newest noncurrent version stopped being current. Returns 0 when the key
   * has no current version at all.
   */
  private long supersedingTime(OmBucketInfo bucket, String keyName)
      throws IOException {
    final OmKeyInfo current = metadataManager
        .getKeyTable(BucketLayout.OBJECT_STORE)
        .get(metadataManager.getOzoneKey(bucket.getVolumeName(),
            bucket.getBucketName(), keyName));
    return current == null ? 0L : current.getModificationTime();
  }
}
