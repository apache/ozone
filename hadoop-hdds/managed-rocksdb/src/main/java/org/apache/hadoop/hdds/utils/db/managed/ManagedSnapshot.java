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

package org.apache.hadoop.hdds.utils.db.managed;

import java.util.function.Consumer;
import org.rocksdb.Snapshot;

public class ManagedSnapshot extends ManagedObject<Snapshot> {

  private Consumer<ManagedSnapshot> snapshotCloseHandler;

  private ManagedSnapshot(Snapshot snapshot, Consumer<ManagedSnapshot> snapshotCloseHandler) {
    super(snapshot);
    this.snapshotCloseHandler = snapshotCloseHandler;
  }

  public static ManagedSnapshot newManagedSnapshots(Snapshot snapshot, Consumer<ManagedSnapshot> snapshotCloseHandler) {
    return new ManagedSnapshot(snapshot, snapshotCloseHandler);
  }
  @Override
  public void close() {
    this.snapshotCloseHandler.accept(this);
    super.close();
  }

}
