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

import com.google.common.base.Preconditions;

/**
 * Pins the first version of every key to {@link #FIRST_VERSION_ID} and uses the
 * committing transaction's index for every later version, exactly like
 * {@link TransactionIndexVersionIdGenerator}. Clusters configured with this
 * generator can reference the first version of a key without listing it first.
 *
 * <p>Holds no allocator state either: a key is on its first version exactly
 * when keyTable holds no current version for it, which the write path looks up
 * anyway.
 *
 * <p>The sentinel is smaller than any transaction index, so the first version
 * sorts at the old end of the key's version sequence, as the versionedKeyTable
 * layout requires.
 *
 * <p>Known trade-off: once every version of a key is permanently deleted, a
 * recreated key takes the sentinel again, so an external reference to the first
 * version resolves to the new content. Later versions are transaction indices
 * and are never reused.
 */
public class PinnedFirstVersionIdGenerator implements VersionIdGenerator {

  @Override
  public long generateVersionId(long transactionLogIndex, boolean hasCurrentVersion) {
    if (!hasCurrentVersion) {
      return FIRST_VERSION_ID;
    }
    Preconditions.checkArgument(transactionLogIndex > FIRST_VERSION_ID,
        "Transaction index " + transactionLogIndex
            + " is a reserved versionId, expected greater than " + FIRST_VERSION_ID);
    return transactionLogIndex;
  }
}
