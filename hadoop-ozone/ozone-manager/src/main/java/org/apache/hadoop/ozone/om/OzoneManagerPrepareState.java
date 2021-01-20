/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Controls the prepare state of the {@link OzoneManager} containing the
 * instance. When prepared, an ozone manager should have no Ratis logs
 * remaining, disallow all write requests except prepare and cancel prepare,
 * and have a marker file present on disk that will cause it to remain prepared
 * on restart.
 */
public final class OzoneManagerPrepareState {
  public static final long NO_PREPARE_INDEX = -1;

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerPrepareState.class);

  private boolean prepareGateEnabled;
  private long prepareIndex;
  private PrepareStatus status;
  private final ConfigurationSource conf;

  public OzoneManagerPrepareState(ConfigurationSource conf) {
    prepareGateEnabled = false;
    prepareIndex = NO_PREPARE_INDEX;
    status = PrepareStatus.PREPARE_NOT_STARTED;
    this.conf = conf;
  }

  /**
   * Turns on the prepare gate flag, clears the prepare index, and moves the
   * prepare status to {@link PrepareStatus#PREPARE_IN_PROGRESS}.
   *
   * Turning on the prepare gate flag will enable a gate in the
   * {@link OzoneManagerStateMachine#preAppendTransaction} (called on leader
   * OM only) and {@link OzoneManagerRatisServer#submitRequest}
   * (called on all OMs) that block write requests from reaching the OM and
   * fail them with error responses to the client.
   */
  public synchronized void enablePrepareGate() {
    prepareGateEnabled = true;
    prepareIndex = NO_PREPARE_INDEX;
    status = PrepareStatus.PREPARE_IN_PROGRESS;
  }

  /**
   * Removes the prepare marker file, clears the prepare index, turns off
   * the prepare gate, and moves the prepare status to
   * {@link PrepareStatus#PREPARE_NOT_STARTED}.
   * This can be called from any state to clear the current prepare state.
   *
   * @throws IOException If the prepare marker file exists but cannot be
   * deleted.
   */
  public synchronized void cancelPrepare()
      throws IOException {
    deletePrepareMarkerFile();
    prepareIndex = NO_PREPARE_INDEX;
    prepareGateEnabled = false;
    status = PrepareStatus.PREPARE_NOT_STARTED;
  }

  /**
   * Enables the prepare gate, writes the prepare marker file, sets the in
   * memory prepare index, and
   * moves the prepare status to {@link PrepareStatus#PREPARE_COMPLETED}.
   * This can be called from any state to move the OM into prepare mode.
   *
   * @param index The log index to prepare the OM on.
   * @throws IOException If the marker file cannot be written.
   */
  public synchronized void finishPrepare(long index) throws IOException {
    finishPrepare(index, true);
  }

  private void finishPrepare(long index, boolean writeFile) throws IOException {
    // Enabling the prepare gate is idempotent, and may have already been
    // performed if we are the leader. If we are a follower, we must ensure this
    // is run now case we become the leader.
    enablePrepareGate();

    if (writeFile) {
      writePrepareMarkerFile(index);
    }
    prepareIndex = index;
    status = PrepareStatus.PREPARE_COMPLETED;
  }

  /**
   * Uses the on disk marker file to determine the OM's prepare state.
   * If the marker file exists and contains an index matching {@code
   * expectedPrepareIndex}, the necessary steps will be taken to finish
   * preparation and the state will be moved to
   * {@link PrepareStatus#PREPARE_COMPLETED}.
   * Else, the status will be moved to
   * {@link PrepareStatus#PREPARE_NOT_STARTED} and any preparation steps will
   * be cancelled.
   *
   * @return The status the OM is in after this method call.
   * @throws IOException If the marker file cannot be read, and it cannot be
   * deleted as part of moving to the
   * {@link PrepareStatus#PREPARE_NOT_STARTED} state.
   */
  public synchronized PrepareStatus restorePrepare(long expectedPrepareIndex)
      throws IOException {
    boolean prepareIndexRead = true;
    long prepareMarkerIndex = NO_PREPARE_INDEX;

    File prepareMarkerFile = getPrepareMarkerFile();
    if (prepareMarkerFile.exists()) {
      byte[] data = new byte[(int) prepareMarkerFile.length()];
      try(FileInputStream stream = new FileInputStream(prepareMarkerFile)) {
        stream.read(data);
      } catch (IOException e) {
        LOG.error("Failed to read prepare marker file {} while restoring OM.",
            prepareMarkerFile.getAbsolutePath());
        prepareIndexRead = false;
      }

      try {
        prepareMarkerIndex = Long.parseLong(
            new String(data, StandardCharsets.UTF_8));
      } catch (NumberFormatException e) {
        LOG.error("Failed to parse log index from prepare marker file {} " +
            "while restoring OM.", prepareMarkerFile.getAbsolutePath());
        prepareIndexRead = false;
      }
    } else {
      // No marker file found.
      prepareIndexRead = false;
    }

    boolean prepareRestored = false;
    if (prepareIndexRead) {
      if (prepareMarkerIndex != expectedPrepareIndex) {
        LOG.error("Failed to restore OM prepare state, because the expected " +
            "prepare index {} does not match the index {} written to the " +
            "marker file.", expectedPrepareIndex, prepareMarkerIndex);
      } else {
        // Prepare state can only be restored if we read the expected index
        // from the marker file.
        prepareRestored = true;
      }
    }

    if (prepareRestored) {
      // Do not rewrite the marker file, since we verified it already exists.
      finishPrepare(prepareMarkerIndex, false);
    } else {
      // If the potentially faulty marker file cannot be deleted,
      // propagate the IOException.
      // If there is no marker file, this call sets the in memory state only.
      cancelPrepare();
    }

    return status;
  }

  /**
   * If the prepare gate is enabled, always returns true.
   * If the prepare gate is disabled, returns true only if {@code
   * requestType} is {@code Prepare} or {@code CancelPrepare}. Returns false
   * otherwise.
   */
  public synchronized boolean requestAllowed(Type requestType) {
    boolean requestAllowed = true;

    if (prepareGateEnabled) {
      requestAllowed =
          (requestType == Type.Prepare) || (requestType == Type.CancelPrepare);
    }

    return requestAllowed;
  }

  /**
   * @return the current log index and status of preparation.
   * Both fields are returned together to provide a consistent view of the
   * state, which would not be guaranteed if they had to be retrieved through
   * separate getters.
   */
  public synchronized State getState() {
    return new State(prepareIndex, status);
  }

  /**
   * Creates a prepare marker file inside the OM metadata directory which
   * contains the log index {@code index}.
   * If a marker file already exists, it will be overwritten.
   */
  private void writePrepareMarkerFile(long index) throws IOException {
    File markerFile = getPrepareMarkerFile();
    File parentDir = markerFile.getParentFile();
    Files.createDirectories(parentDir.toPath());

    try(FileOutputStream stream = new FileOutputStream(markerFile)) {
      stream.write(Long.toString(index).getBytes(StandardCharsets.UTF_8));
    }

    LOG.info("Prepare marker file written with log index {} to file {}", index,
        markerFile.getAbsolutePath());
  }

  private void deletePrepareMarkerFile()
      throws IOException {
    File markerFile = getPrepareMarkerFile();
    if (markerFile.exists()) {
      Files.delete(markerFile.toPath());
      LOG.info("Deleted prepare marker file: {}", markerFile.getAbsolutePath());
    } else {
      LOG.info("Request to delete prepare marker file that does not exist: {}",
          markerFile.getAbsolutePath());
    }
  }

  /**
   * Returns a {@link File} object representing the prepare marker file,
   * which may or may not actually exist on disk.
   * This method should be used for testing only.
   */
  @VisibleForTesting
  public File getPrepareMarkerFile() {
    File markerFileDir = new File(ServerUtils.getOzoneMetaDirPath(conf),
        OMStorage.STORAGE_DIR_CURRENT);
    return new File(markerFileDir, OzoneConsts.PREPARE_MARKER);
  }

  /**
   * The current state of preparation is defined by the status and the
   * prepare index that are currently set.
   */
  public static class State {
    private final long index;
    private final PrepareStatus status;

    public State(long index, PrepareStatus status) {
      this.index = index;
      this.status = status;
    }

    public long getIndex() {
      return index;
    }

    public PrepareStatus getStatus() {
      return status;
    }
  }
}
