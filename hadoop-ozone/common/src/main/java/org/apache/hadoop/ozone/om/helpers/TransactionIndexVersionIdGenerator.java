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
 * Uses the index of the committing transaction as the versionId, the default
 * generator. Holds no allocator state, so a version costs no read or write
 * beyond the commit itself.
 */
public class TransactionIndexVersionIdGenerator implements VersionIdGenerator {

  @Override
  public long generateVersionId(long transactionLogIndex, boolean hasCurrentVersion) {
    Preconditions.checkArgument(transactionLogIndex > FIRST_VERSION_ID,
        "Transaction index " + transactionLogIndex
            + " is a reserved versionId, expected greater than " + FIRST_VERSION_ID);
    return transactionLogIndex;
  }
}
