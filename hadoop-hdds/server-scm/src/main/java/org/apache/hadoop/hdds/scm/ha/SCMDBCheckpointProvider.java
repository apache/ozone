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

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: define a generic interface for this
/**
 * Checkpoint write stream and exception handling.
 */
public class SCMDBCheckpointProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDBCheckpointProvider.class);
  private transient DBStore scmDbStore;

  public SCMDBCheckpointProvider(DBStore scmDbStore) {
    this.scmDbStore = scmDbStore;
  }

  public void writeDBCheckPointToSream(OutputStream stream, boolean flush)
      throws IOException {
    LOG.info("Received request to obtain SCM DB checkpoint snapshot");
    if (scmDbStore == null) {
      LOG.error("Unable to process checkpointing request. DB Store is null");
      return;
    }

    DBCheckpoint checkpoint = null;
    try {

      checkpoint = scmDbStore.getCheckpoint(flush);
      if (checkpoint == null || checkpoint.getCheckpointLocation() == null) {
        throw new IOException("Unable to process metadata snapshot request. "
            + "Checkpoint request returned null.");
      }

      Path file = checkpoint.getCheckpointLocation().getFileName();
      if (file == null) {
        return;
      }

      Instant start = Instant.now();
      HddsServerUtil.writeDBCheckpointToStream(checkpoint, stream, new HashSet<>());
      Instant end = Instant.now();

      long duration = Duration.between(start, end).toMillis();
      LOG.info("Time taken to write the checkpoint to response output " +
          "stream: {} milliseconds", duration);

    } catch (IOException ioe) {
      LOG.error("Unable to process metadata snapshot request. ", ioe);
      throw ioe;
    } finally {
      if (checkpoint != null) {
        try {
          checkpoint.cleanupCheckpoint();
        } catch (IOException e) {
          LOG.error("Error trying to clean checkpoint at {} .",
              checkpoint.getCheckpointLocation().toString());
        }
      }
    }
  }
}
