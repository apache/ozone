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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.TransactionIndexVersionIdGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link VersionIdAllocator}: which versionId a commit gets, and the
 * rejection of ids that are already taken on the key.
 */
public class TestVersionIdAllocator {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";

  private OMMetadataManager metadataManager;
  private Set<String> versionedKeys;
  private int lookups;

  @BeforeEach
  void setUp() throws Exception {
    versionedKeys = new HashSet<>();
    lookups = 0;

    Table<String, OmKeyInfo> versionedKeyTable = mock(Table.class);
    when(versionedKeyTable.isExist(anyString())).thenAnswer(invocation -> {
      lookups++;
      return versionedKeys.contains(invocation.getArgument(0));
    });

    metadataManager = mock(OMMetadataManager.class);
    when(metadataManager.getVersionedKeyTable()).thenReturn(versionedKeyTable);
    when(metadataManager.getVersionedOzoneKey(eq(VOLUME), eq(BUCKET), eq(KEY), anyLong()))
        .thenAnswer(invocation -> dbKey(invocation.getArgument(3)));
  }

  private static String dbKey(long versionId) {
    return "/" + VOLUME + "/" + BUCKET + "/" + KEY + "/" + versionId;
  }

  private VersionIdAllocator allocator() {
    return new VersionIdAllocator(new TransactionIndexVersionIdGenerator());
  }

  private static OmKeyInfo keyWithVersionId(Long versionId) {
    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .setVersionId(versionId)
        .build();
  }

  @Test
  void allocatesTheTransactionIndexForTheFirstVersion() throws Exception {
    assertEquals(7, allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 7, null));
  }

  @Test
  void allocatesTheTransactionIndexForALaterVersion() throws Exception {
    assertEquals(9, allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 9,
        keyWithVersionId(7L)));
  }

  @Test
  void rejectsAnIdEqualToTheCurrentVersion() {
    OMException e = assertThrows(OMException.class,
        () -> allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 7,
            keyWithVersionId(7L)));

    assertEquals(OMException.ResultCodes.INVALID_REQUEST, e.getResult());
  }

  @Test
  void rejectsAnIdOlderThanTheCurrentVersion() {
    // Refused even though no version holds this id: writing it would sort the
    // new version before versions that predate it.
    OMException e = assertThrows(OMException.class,
        () -> allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 5,
            keyWithVersionId(9L)));

    assertEquals(OMException.ResultCodes.INVALID_REQUEST, e.getResult());
  }

  @Test
  void skipsTheLookupWhenTheKeyHasNoCurrentVersion() throws Exception {
    assertEquals(7, allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 7, null));
    assertEquals(0, lookups);
  }

  @Test
  void skipsTheLookupWhenTheGeneratedIdIsNewerThanTheCurrentVersion() throws Exception {
    // The steady-state path: the current version holds the key's largest id, so
    // an id above it cannot be taken and costs no read.
    assertEquals(9, allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 9,
        keyWithVersionId(7L)));
    assertEquals(0, lookups);
  }

  @Test
  void skipsTheLookupWhenTheIdIsRefusedForGoingBackwards() {
    assertThrows(OMException.class,
        () -> allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 5,
            keyWithVersionId(9L)));

    assertEquals(0, lookups);
  }

  @Test
  void looksUpTheTableForACurrentVersionPredatingVersioning() throws Exception {
    // Keys written before versioning was enabled carry no versionId, so there
    // is nothing to order against and the id has to be looked up.
    assertEquals(7, allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 7,
        keyWithVersionId(null)));

    assertEquals(1, lookups);
  }

  @Test
  void rejectsATakenIdForACurrentVersionPredatingVersioning() {
    versionedKeys.add(dbKey(7));

    OMException e = assertThrows(OMException.class,
        () -> allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 7,
            keyWithVersionId(null)));

    assertEquals(OMException.ResultCodes.KEY_ALREADY_EXISTS, e.getResult());
  }

  @Test
  void allowsAnIdHeldByAnotherKeysVersion() throws Exception {
    // Ids are only unique within a key, so another key holding it is fine.
    versionedKeys.add("/" + VOLUME + "/" + BUCKET + "/otherKey/7");

    assertEquals(7, allocator().allocate(metadataManager, VOLUME, BUCKET, KEY, 7,
        keyWithVersionId(null)));
  }

  @Test
  void usesTheGeneratorConfiguredForTheCluster() {
    assertInstanceOf(TransactionIndexVersionIdGenerator.class,
        new VersionIdAllocator(new OzoneConfiguration()).getGenerator());
  }

}
