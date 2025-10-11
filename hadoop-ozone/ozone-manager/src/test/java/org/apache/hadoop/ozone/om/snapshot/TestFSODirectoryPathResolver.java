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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.junit.jupiter.api.Test;

/**
 * Test class for FSODirectoryPathResolver.
 */
public class TestFSODirectoryPathResolver {

  private Table<String, OmDirectoryInfo> getMockedDirectoryInfoTable(
      String prefix, Map<Integer, List<Integer>> dirMap) throws IOException {
    Table<String, OmDirectoryInfo> dirInfos = mock(Table.class);

    when(dirInfos.iterator(anyString()))
        .thenAnswer(i -> {
          int dirId = Integer.parseInt(((String)i.getArgument(0))
               .split(OM_KEY_PREFIX)[3]);
          Iterator<? extends Table.KeyValue<String, OmDirectoryInfo>> iterator =
              dirMap
              .getOrDefault(dirId, Collections.emptyList()).stream()
              .map(children -> Table.newKeyValue(prefix + children + OM_KEY_PREFIX + "dir" + children,
                  OmDirectoryInfo.newBuilder().setName("dir" + children).setObjectID(children).build()))
              .iterator();
          return new Table.KeyValueIterator<String, OmDirectoryInfo>() {

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public Table.KeyValue<String, OmDirectoryInfo> next() {
              return iterator.next();
            }

            @Override
            public void close() {
            }

            @Override
            public void seekToFirst() {
            }

            @Override
            public void seekToLast() {
            }

            @Override
            public Table.KeyValue<String, OmDirectoryInfo> seek(String s) {
              return null;
            }

            @Override
            public void removeFromDB() {

            }
          };

        });

    return dirInfos;
  }

  @Test
  public void testGetAbsolutePathForValidObjectIDs() throws IOException {
    Map<Integer, List<Integer>> dirMap = ImmutableMap.of(
        1, Lists.newArrayList(2, 3, 4, 5, 6),
        2, Lists.newArrayList(7, 8, 9, 10, 11),
        3, Lists.newArrayList(12, 13, 14, 15),
        9, Lists.newArrayList(16),
        14, Lists.newArrayList(17),
        18, Lists.newArrayList(19, 20)
        );
    String prefix = "/vol/buck/";
    FSODirectoryPathResolver fsoDirectoryPathResolver =
        new FSODirectoryPathResolver(prefix, 1,
            getMockedDirectoryInfoTable(prefix, dirMap));
    Set<Long> objIds = Sets.newHashSet(17L, 9L, 10L, 15L, 4L, 3L, 1L);
    Set<Long> invalidObjIds = Sets.newHashSet(17L, 9L, 10L, 15L, 4L, 3L, 1L,
        19L);
    Map<Long, Path> absolutePathMap = fsoDirectoryPathResolver
        .getAbsolutePathForObjectIDs(Optional.of(objIds));

    Map<Long, String> pathMapping = ImmutableMap.<Long, String>builder()
        .put(17L, "/dir3/dir14/dir17")
        .put(9L, "/dir2/dir9")
        .put(10L, "/dir2/dir10")
        .put(15L, "/dir3/dir15")
        .put(4L, "/dir4")
        .put(3L, "/dir3")
        .put(1L, "/")
        .build();

    Map<Long, Path> expectedPaths = pathMapping.entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(
            Map.Entry::getKey,
            e -> Paths.get(e.getValue())
        ));

    assertEquals(expectedPaths, absolutePathMap);
    assertEquals(objIds.size(), absolutePathMap.size());
    // Invalid Obj Id 19 with dirInfo dir19 which is not present in the bucket.
    assertThrows(IllegalArgumentException.class,
        () -> fsoDirectoryPathResolver.getAbsolutePathForObjectIDs(
            Optional.of(invalidObjIds)),
        "Dir object Ids required but not found in bucket: [19]");
  }

}
