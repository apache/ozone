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
import static org.apache.hadoop.ozone.OzoneConsts.ROOT_PATH;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;

/**
 * Class to resolve absolute paths for FSO DirectoryInfo Objects.
 */
public class FSODirectoryPathResolver implements ObjectPathResolver {

  private final String prefix;
  private final long bucketId;
  private final Table<String, OmDirectoryInfo> dirInfoTable;

  public FSODirectoryPathResolver(String prefix, long bucketId,
      Table<String, OmDirectoryInfo> dirInfoTable) {
    this.prefix = prefix;
    this.dirInfoTable = dirInfoTable;
    this.bucketId = bucketId;
  }

  private void addToPathMap(Pair<Long, Path> objectIDPath,
                            Set<Long> dirObjIds, Map<Long, Path> pathMap) {
    if (dirObjIds.contains(objectIDPath.getKey())) {
      pathMap.put(objectIDPath.getKey(), objectIDPath.getValue());
      dirObjIds.remove(objectIDPath.getKey());
    }
  }

  /**
   * Assuming all dirObjIds belong to a bucket this function resolves absolute
   * path for a given FSO bucket.
   * @param dirObjIds Object Ids corresponding to which absolute path is needed.
   * @param skipUnresolvedObjs boolean value to skipUnresolved objects when
   *                           false exception will be thrown.
   * @return Map of Path corresponding to provided directory object IDs
   */
  @Override
  public Map<Long, Path> getAbsolutePathForObjectIDs(
      Optional<Set<Long>> dirObjIds, boolean skipUnresolvedObjs)
      throws IOException {
    // Root of a bucket would always have the
    // key as /volumeId/bucketId/bucketId/
    if (!dirObjIds.isPresent() || dirObjIds.get().isEmpty()) {
      return Collections.emptyMap();
    }
    Set<Long> objIds = Sets.newHashSet(dirObjIds.get());
    Map<Long, Path> objectIdPathMap = new HashMap<>();
    Queue<Pair<Long, Path>> objectIdPathVals = new LinkedList<>();
    Pair<Long, Path> root = Pair.of(bucketId, ROOT_PATH);
    objectIdPathVals.add(root);
    addToPathMap(root, objIds, objectIdPathMap);

    while (!objectIdPathVals.isEmpty() && !objIds.isEmpty()) {
      Pair<Long, Path> parent = objectIdPathVals.poll();
      try (TableIterator<String,
              ? extends Table.KeyValue<String, OmDirectoryInfo>>
              subDirIter = dirInfoTable.iterator(
                  prefix + parent.getKey() + OM_KEY_PREFIX)) {
        while (!objIds.isEmpty() && subDirIter.hasNext()) {
          OmDirectoryInfo childDir = subDirIter.next().getValue();
          Pair<Long, Path> pathVal = Pair.of(childDir.getObjectID(),
              parent.getValue().resolve(childDir.getName()));
          addToPathMap(pathVal, objIds, objectIdPathMap);
          objectIdPathVals.add(pathVal);
        }
      }
    }
    // Invalid directory objectId which does not exist in the given bucket.
    if (!objIds.isEmpty() && !skipUnresolvedObjs) {
      throw new IllegalArgumentException(
          "Dir object Ids required but not found in bucket: " + objIds);
    }
    return objectIdPathMap;
  }
}
