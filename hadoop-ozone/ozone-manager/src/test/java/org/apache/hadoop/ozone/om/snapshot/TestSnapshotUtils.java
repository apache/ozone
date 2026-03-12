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

package org.apache.hadoop.ozone.om.snapshot;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.Test;

/**
 * Unit test for SnapshotUtils.
 */
public class TestSnapshotUtils {
  private static final long CONTAINER_ID = 1L;

  private OmKeyLocationInfo createLocation(long blockId, long length) {
    BlockID blockID = new BlockID(CONTAINER_ID, blockId);
    // Only blockId is used in .hasSameBlockAs for the corner cases we're testing.
    // You can expand this as needed for extra fields.
    return new OmKeyLocationInfo.Builder()
        .setBlockID(blockID)
        .setLength(length)
        .build();
  }

  private OmKeyLocationInfoGroup createLocationGroup(OmKeyLocationInfo... locs) {
    OmKeyLocationInfoGroup group = new OmKeyLocationInfoGroup(0, Arrays.asList(locs));
    return group;
  }

  private OmKeyInfo createOmKeyInfo(boolean hsync, OmKeyLocationInfoGroup group) {
    return createOmKeyInfo(hsync, group, 0);
  }

  private OmKeyInfo createOmKeyInfo(boolean hsync, OmKeyLocationInfoGroup group, long objectId) {
    return createOmKeyInfo(hsync, Collections.singletonList(group), objectId);
  }

  private OmKeyInfo createOmKeyInfo(boolean hsync, List<OmKeyLocationInfoGroup> group, long objectId) {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("key")
        .setOmKeyLocationInfos(group)
        .setObjectID(objectId);
    if (hsync) {
      builder.addMetadata(OzoneConsts.HSYNC_CLIENT_ID, "clientid");
    }
    return builder.build();
  }

  @Test
  public void testBothNull() {
    assertTrue(SnapshotUtils.isBlockLocationInfoSame(null, null));
  }

  @Test
  public void testOneNull() {
    OmKeyInfo keyInfo = createOmKeyInfo(false, createLocationGroup(createLocation(1, 100)));
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(keyInfo, null));
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(null, keyInfo));
  }

  @Test
  public void testBothHsync() {
    OmKeyLocationInfoGroup group = createLocationGroup(createLocation(1, 100));
    OmKeyInfo prev = createOmKeyInfo(true, group);
    OmKeyLocationInfoGroup group2 = createLocationGroup(createLocation(1, 200));
    OmKeyInfo del = createOmKeyInfo(true, group2);
    assertTrue(SnapshotUtils.isBlockLocationInfoSame(prev, del));

    OmKeyInfo del2 = createOmKeyInfo(true, group2, 1);
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(del, del2));
  }

  @Test
  public void testDifferentLocationVersionsSize() {
    OmKeyLocationInfoGroup group1 = createLocationGroup(createLocation(1, 100));
    OmKeyLocationInfoGroup group2 = createLocationGroup(createLocation(2, 200));
    OmKeyInfo prev = createOmKeyInfo(false, group1);
    OmKeyInfo del = createOmKeyInfo(false, Arrays.asList(group1, group2), 0);
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(prev, del));
  }

  @Test
  public void testNullLatestVersionLocations() {
    OmKeyInfo prev = createOmKeyInfo(false, null);
    OmKeyInfo del = createOmKeyInfo(false, createLocationGroup(createLocation(1, 100)));
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(prev, del));

    OmKeyInfo prev2 = createOmKeyInfo(false, createLocationGroup(createLocation(1, 100)));
    OmKeyInfo del2 = createOmKeyInfo(false, null);
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(prev2, del2));
  }

  @Test
  public void testLocationListSizeMismatch() {
    OmKeyLocationInfoGroup prevGroup = createLocationGroup(createLocation(1, 100));
    OmKeyLocationInfoGroup delGroup = createLocationGroup(
        createLocation(1, 100),
        createLocation(2, 200)
    );
    OmKeyInfo prev = createOmKeyInfo(false, prevGroup);
    OmKeyInfo del = createOmKeyInfo(false, delGroup);
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(prev, del));
  }

  @Test
  public void testLocationListBlockIdMismatch() {
    OmKeyLocationInfoGroup prevGroup = createLocationGroup(createLocation(1, 100));
    OmKeyLocationInfoGroup delGroup = createLocationGroup(createLocation(2, 100));
    OmKeyInfo prev = createOmKeyInfo(false, prevGroup);
    OmKeyInfo del = createOmKeyInfo(false, delGroup);
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(prev, del));
  }

  @Test
  public void testLocationListBlockIdMatch() {
    OmKeyLocationInfoGroup prevGroup = createLocationGroup(
        createLocation(1, 100),
        createLocation(2, 200)
    );
    OmKeyLocationInfoGroup delGroup = createLocationGroup(
        createLocation(1, 100),
        createLocation(2, 200)
    );
    OmKeyInfo prev = createOmKeyInfo(false, prevGroup);
    OmKeyInfo del = createOmKeyInfo(false, delGroup);
    assertTrue(SnapshotUtils.isBlockLocationInfoSame(prev, del));
  }

  @Test
  public void testMultipleBlocksWithPartialMatch() {
    OmKeyLocationInfoGroup prevGroup = createLocationGroup(
        createLocation(1, 100),
        createLocation(2, 200),
        createLocation(3, 300)
    );
    OmKeyLocationInfoGroup delGroup = createLocationGroup(
        createLocation(1, 100),
        createLocation(999, 200),
        createLocation(3, 300)
    );
    OmKeyInfo prev = createOmKeyInfo(false, prevGroup);
    OmKeyInfo del = createOmKeyInfo(false, delGroup);
    assertFalse(SnapshotUtils.isBlockLocationInfoSame(prev, del));
  }
}
