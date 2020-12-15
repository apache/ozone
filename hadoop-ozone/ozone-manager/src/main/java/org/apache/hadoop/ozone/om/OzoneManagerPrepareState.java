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
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Controls the prepare state of the {@link OzoneManager}.
 * When prepared, an ozone manager should have no Ratis logs remaining,
 * disallow all write requests except prepare and cancel prepare, and have a
 * marker file present on disk that will cause it to remain prepared on restart.
 */
public final class OzoneManagerPrepareState {
  private static boolean prepareGateEnabled = false;

  private OzoneManagerPrepareState() { }

  public static synchronized boolean isPrepareGateEnabled() {
    return prepareGateEnabled;
  }

  /**
   * Turning on this flag will enable a gate in the
   * {@link OzoneManagerStateMachine#preAppendTransaction}
   * and {@link OzoneManagerRatisServer#submitRequest} methods that block
   * write requests from reaching the OM and fail them with error responses
   * to the client.
   *
   * @param value true if the prepare gate for this Ozone Manager should be
   * enabled, false if the gate should be disabled.
   */
  public static synchronized void setPrepareGateEnabled(boolean value) {
    prepareGateEnabled = value;
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

  /**
   * Creates a prepare marker file inside {@code metadataDir} which contains
   * the log index {@code index}. If a marker file already exists, it will be
   * overwritten. This method does not change the state of the prepare gate.
   */
  public static synchronized void writePrepareMarkerFile(
      ConfigurationSource conf, long index) throws IOException {
    File markerFile = getPrepareMarkerFile(conf);
    markerFile.getParentFile().mkdirs();
    try(FileOutputStream stream =
            new FileOutputStream(markerFile)) {
      stream.write(Long.toString(index).getBytes());
    }
  }

  /**
   * If a prepare marker file exists and contains a
   * log index matching {@code index}, the prepare gate will be enabled.
   * Otherwise, the prepare gate will be disabled.
   */
  public static synchronized void checkPrepareMarkerFile(
      ConfigurationSource conf, long index) {
    File prepareMarkerFile = getPrepareMarkerFile(conf);
    if (prepareMarkerFile.exists()) {
      byte[] data = new byte[(int) prepareMarkerFile.length()];
      try(FileInputStream stream = new FileInputStream(prepareMarkerFile)) {
        stream.read(data);
      } catch (IOException e) {
        setPrepareGateEnabled(false);
      }

      try {
        long prepareMarkerIndex = Long.parseLong(new String(data));
        setPrepareGateEnabled(index == prepareMarkerIndex);
      } catch (NumberFormatException e) {
        setPrepareGateEnabled(false);
      }
    } else {
      // No marker file found.
      setPrepareGateEnabled(false);
    }
  }

  /**
   * Returns a {@link File} object representing the prepare marker file,
   * which may or may not correspond to an actual file on disk.
   * This method should be used for testing only, and all other interactions
   * with the prepare marker file should be done through
   * {@link OzoneManagerPrepareState#writePrepareMarkerFile} and
   * {@link OzoneManagerPrepareState#checkPrepareMarkerFile}
   */
  @VisibleForTesting
  public static File getPrepareMarkerFile(ConfigurationSource conf) {
    File markerFileDir = new File(ServerUtils.getOzoneMetaDirPath(conf),
        OMStorage.STORAGE_DIR_CURRENT);
    return new File(markerFileDir, OzoneConsts.PREPARE_MARKER);
  }
}
