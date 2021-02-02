/**
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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This is the background service to delete hanging open keys.
 * Scan the metadata of om periodically to get
 * the keys with prefix "#open#" and ask scm to
 * delete metadata accordingly, if scm returns
 * success for keys, then clean up those keys.
 */
public class OpenKeyCleanupService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpenKeyCleanupService.class);

  private static final int OPEN_KEY_DELETING_CORE_POOL_SIZE = 2;

  private final KeyManager keyManager;
  private final ScmBlockLocationProtocol scmClient;

  public OpenKeyCleanupService(ScmBlockLocationProtocol scmClient,
      KeyManager keyManager, int serviceInterval,
      long serviceTimeout) {
    super("OpenKeyCleanupService", serviceInterval, TimeUnit.SECONDS,
        OPEN_KEY_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.keyManager = keyManager;
    this.scmClient = scmClient;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new OpenKeyDeletingTask());
    return queue;
  }

  private class OpenKeyDeletingTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      // This method is currently never used. It will be implemented in
      // HDDS-4122, and integrated into the rest of the code base in HDDS-4123.
      try {
        // The new API for deleting expired open keys in OM HA will differ
        // significantly from the old implementation.
        // The old implementation has been removed so the code compiles.
        keyManager.getExpiredOpenKeys(0);
      } catch (IOException e) {
        LOG.error("Unable to get hanging open keys, retry in"
            + " next interval", e);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }
}
