/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Test class for FSODirectoryPathResolver.
 */
public class TestFSODirectoryPathResolver {

  private Table<String, OmDirectoryInfo> getMockedDirectoryInfoTable(
      String prefix, Map<Integer, List<Integer>> dirMap) throws IOException {
    Table<String, OmDirectoryInfo> dirInfos = Mockito.mock(Table.class);

    Mockito.when(dirInfos.iterator(Mockito.anyString()))
        .thenAnswer(i -> {
          int dirId = Integer.parseInt(((String)i.getArgument(0))
               .split(OM_KEY_PREFIX)[3]);
          Iterator<? extends Table.KeyValue<String, OmDirectoryInfo>> iterator =
              dirMap
              .getOrDefault(dirId, Collections.emptyList()).stream()
              .map(children -> new Table.KeyValue<String, OmDirectoryInfo>() {
                  @Override
                  public String getKey() {
                    return prefix + children + OM_KEY_PREFIX + "dir" + children;
                  }

                  @Override
                  public OmDirectoryInfo getValue() {
                    return OmDirectoryInfo.newBuilder()
                        .setName("dir" + children).setObjectID(children)
                        .build();
                  }
              })
              .iterator();
          return new TableIterator<String,
              Table.KeyValue<String, OmDirectoryInfo>>() {

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

    Assertions.assertEquals(ImmutableMap.of(
        17L, Paths.get("/dir3/dir14/dir17"),
        9L, Paths.get("/dir2/dir9"),
        10L, Paths.get("/dir2/dir10"),
        15L, Paths.get("/dir3/dir15"),
        4L, Paths.get("/dir4"),
        3L, Paths.get("/dir3"),
        1L, Paths.get("/")
    ), absolutePathMap);
    Assertions.assertEquals(objIds.size(), absolutePathMap.size());
    // Invalid Obj Id 19 with dirInfo dir19 which is not present in the bucket.
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> fsoDirectoryPathResolver.getAbsolutePathForObjectIDs(
            Optional.of(invalidObjIds)),
        "Dir object Ids required but not found in bucket: [19]");
  }

}
