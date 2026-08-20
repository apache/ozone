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

/**
 * Numbers versions like {@link UniqueIdVersionIdGenerator}, except that a key's
 * first version takes {@link #FIRST_VERSION_ID}, so it can be referenced
 * without listing the key's versions first.
 *
 * <p>Known trade-off: once every version of a key has been permanently deleted,
 * a recreated key takes the sentinel again, so an external reference to the
 * first version resolves to the new content.
 */
public class PinnedFirstVersionIdGenerator extends UniqueIdVersionIdGenerator {

  /**
   * Id of a pinned first version. Smaller than any proposed id, so such a
   * version sorts at the old end of the key's versions.
   */
  public static final long FIRST_VERSION_ID = 1L;

  @Override
  public long versionIdFor(long proposedVersionId, boolean hasCurrentVersion) {
    return hasCurrentVersion ? proposedVersionId : FIRST_VERSION_ID;
  }
}
