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

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Class to resolve absolute paths for FSO DirectoryInfo Objects.
 */
public class FSODirectoryPathResolver implements ObjectPathResolver {

  private String prefix;
  private long bucketId;
  private Table<String, OmDirectoryInfo> dirInfoTable;

  public FSODirectoryPathResolver(String prefix, long bucketId,
      Table<String, OmDirectoryInfo> dirInfoTable) {
    this.prefix = prefix;
    this.dirInfoTable = dirInfoTable;
    this.bucketId = bucketId;
  }

  private void addToPathMap(ObjectIDPathVal objectIDPathVal,
                            Set<Long> dirObjIds, Map<Long, Path> pathMap) {
    if (dirObjIds.contains(objectIDPathVal.getObjectId())) {
      pathMap.put(objectIDPathVal.getObjectId(), objectIDPathVal.getPath());
      dirObjIds.remove(objectIDPathVal.getObjectId());
    }
  }

  /**
   * Assuming all dirObjIds belong to a bucket this function resolves absolute
   * path for a given FSO bucket.
   * @param dirObjIds
   * @return Map of Path corresponding to provided directory object IDs
   */
  @Override
  public Map<Long, Path> getAbsolutePathForObjectIDs(Set<Long> dirObjIds)
      throws IOException {
    // Root of a bucket would always have the
    // key as /volumeId/bucketId/bucketId/
    if (dirObjIds.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<Long, Path> pathMap = new HashMap<>();
    Queue<ObjectIDPathVal> objectIdPathVals = new LinkedList<>();
    ObjectIDPathVal root = new ObjectIDPathVal(bucketId, Paths.get("/"));
    objectIdPathVals.add(root);
    addToPathMap(root, dirObjIds, pathMap);

    while (!objectIdPathVals.isEmpty() && dirObjIds.size() > 0) {
      ObjectIDPathVal parent = objectIdPathVals.poll();
      try (TableIterator<String,
              ? extends Table.KeyValue<String, OmDirectoryInfo>>
              subDirIter = dirInfoTable.iterator(
                  prefix + parent.getObjectId())) {
        while (dirObjIds.size() > 0 && subDirIter.hasNext()) {
          OmDirectoryInfo dir = subDirIter.next().getValue();
          ObjectIDPathVal pathVal = new ObjectIDPathVal(dir.getObjectID(),
              parent.getPath().resolve(dir.getName()));
          addToPathMap(pathVal, dirObjIds, pathMap);
          objectIdPathVals.add(pathVal);
        }
      }
    }

    if (dirObjIds.size() > 0) {
      throw new IllegalArgumentException(
          "Dir object Ids required but not found in bucket: " + dirObjIds);
    }
    return pathMap;
  }

  private static class ObjectIDPathVal {
    private long objectId;
    private Path path;

    ObjectIDPathVal(long objectId, Path path) {
      this.objectId = objectId;
      this.path = path;
    }

    public long getObjectId() {
      return objectId;
    }

    public Path getPath() {
      return path;
    }
  }
}
