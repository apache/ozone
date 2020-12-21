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
import org.apache.hadoop.ozone.om.exceptions.OMException;
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
import java.nio.file.Files;

/**
 * Controls the prepare state of the {@link OzoneManager}.
 * When prepared, an ozone manager should have no Ratis logs remaining,
 * disallow all write requests except prepare and cancel prepare, and have a
 * marker file present on disk that will cause it to remain prepared on restart.
 */
public final class OzoneManagerPrepareState {
  public static final long NO_PREPARE_INDEX = -1;

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerPrepareState.class);

  private static boolean prepareGateEnabled = false;
  private static long prepareIndex = NO_PREPARE_INDEX;
  private static PrepareStatus status = PrepareStatus.PREPARE_NOT_STARTED;

  private OzoneManagerPrepareState() { }

  /**
   * Turns on the prepare gate flag, clears the prepare index, and moves the
   * prepare status to {@link PrepareStatus#PREPARE_IN_PROGRESS}.
   * This can be called from any state to start or restart preparing.
   *
   * Turning on the prepare gate flag will enable a gate in the
   * {@link OzoneManagerStateMachine#preAppendTransaction}
   * and {@link OzoneManagerRatisServer#submitRequest} methods that block
   * write requests from reaching the OM and fail them with error responses
   * to the client.
   */
  public static synchronized void startPrepare() {
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
  public static synchronized void cancelPrepare(ConfigurationSource conf)
      throws IOException {
    deletePrepareMarkerFile(conf);
    prepareIndex = NO_PREPARE_INDEX;
    prepareGateEnabled = false;
    status = PrepareStatus.PREPARE_NOT_STARTED;
  }

  /**
   * Writes the prepare marker file, sets the in memory prepare index, and
   * moves the prepare status to {@link PrepareStatus#PREPARE_COMPLETED}.
   * This can only be called when the prepare status is
   * {@link PrepareStatus#PREPARE_IN_PROGRESS}. If this is not the case, an
   * {@link OMException} is thrown.
   * @param conf
   * @param index The log index to prepare the OM on.
   * @throws IOException If the marker file cannot be written.
   */
  public static synchronized void finishPrepare(ConfigurationSource conf,
      long index) throws IOException {
    finishPrepare(conf, index, true);
  }

  private static void finishPrepare(ConfigurationSource conf, long index,
      boolean writeFile) throws IOException {
    // Finish prepare call should only happen after prepare has been started,
    // otherwise it is a bug.
    if (status != PrepareStatus.PREPARE_IN_PROGRESS) {
      throw new OMException(String.format("Cannot move OM prepare status from" +
          " %s to %s", status.name(), PrepareStatus.PREPARE_COMPLETED.name()),
      OMException.ResultCodes.INTERNAL_ERROR);
    }

    if (writeFile) {
      writePrepareMarkerFile(conf, index);
    }
    prepareIndex = index;
    status = PrepareStatus.PREPARE_COMPLETED;
  }

  /**
   * Uses the on disk marker file to determine the OM's prepare state.
   * If the marker file exists and contains an index matching {@code
   * expectedPrepareIndex}, the status will be moved to
   * {@link PrepareStatus#PREPARE_COMPLETED}.
   * Else, the status will be moved to
   * {@link PrepareStatus#PREPARE_NOT_STARTED}.
   *
   * @return The status the OM is in after this method call.
   * @throws IOException If the marker file cannot be read, and it cannot be
   * deleted as part of moving to the
   * {@link PrepareStatus#PREPARE_NOT_STARTED} state.
   */
  public static synchronized PrepareStatus restorePrepare(ConfigurationSource conf,
      long expectedPrepareIndex) throws IOException {
    boolean prepareIndexRead = true;
    long prepareMarkerIndex = NO_PREPARE_INDEX;

    File prepareMarkerFile = getPrepareMarkerFile(conf);
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
        prepareMarkerIndex = Long.parseLong(new String(data));
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
      startPrepare();
      // Do not rewrite the marker file, since we verified it already exists.
      finishPrepare(conf, prepareMarkerIndex, false);
    } else {
      // If the potentially faulty marker file cannot be deleted,
      // propagate the IOException.
      // If there is no marker file, this call sets the in memory state only.
      cancelPrepare(conf);
    }

    return status;
  }

  public static synchronized boolean isPrepareGateEnabled() {
    return prepareGateEnabled;
  }

  public static synchronized long getPrepareIndex() {
    return prepareIndex;
  }

  /**
   * If the prepare gate is enabled, always returns true.
   * If the prepare gate is disabled, returns true only if {@code
   * requestType} is {@code Prepare} or {@code CancelPrepare}. Returns false
   * otherwise.
   */
  public static synchronized boolean requestAllowed(Type requestType) {
    boolean requestAllowed = true;

    if (prepareGateEnabled) {
      // TODO: Also return true for cancel prepare when it is implemented.
      requestAllowed = (requestType == Type.Prepare);
    }

    return requestAllowed;
  }

  public static synchronized PrepareStatus getStatus() {
    return status;
  }

  /**
   * Creates a prepare marker file inside {@code metadataDir} which contains
   * the log index {@code index}. If a marker file already exists, it will be
   * overwritten.
   */
  private static void writePrepareMarkerFile(
      ConfigurationSource conf, long index) throws IOException {
    File markerFile = getPrepareMarkerFile(conf);
    markerFile.getParentFile().mkdirs();
    try(FileOutputStream stream =
            new FileOutputStream(markerFile)) {
      stream.write(Long.toString(index).getBytes());
    }
  }

  private static void deletePrepareMarkerFile(ConfigurationSource conf)
      throws IOException {
    File markerFile = getPrepareMarkerFile(conf);
    if (markerFile.exists()) {
      Files.delete(markerFile.toPath());
    }
  }

  /**
   * Returns a {@link File} object representing the prepare marker file,
   * which may or may not actually exist on disk.
   * This method should be used for testing only.
   */
  @VisibleForTesting
  public static File getPrepareMarkerFile(ConfigurationSource conf) {
    File markerFileDir = new File(ServerUtils.getOzoneMetaDirPath(conf),
        OMStorage.STORAGE_DIR_CURRENT);
    return new File(markerFileDir, OzoneConsts.PREPARE_MARKER);
  }
}
