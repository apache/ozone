package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ratis.OMTransactionInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public final class OzoneManagerPrepareState {
  private static boolean isPrepared;

  public static synchronized boolean isPrepared() {
    return isPrepared;
  }

  /**
   * Mark this Ozone Manager as being in or out of prepare mode.
   * In prepare mode, an Ozone Manager has applied all transactions from
   * its Ratis log and cleared out the log. It will not allow any write requests
   * through until it is taken out of prepare mode.
   *
   * Blocking write requests while the OM is in prepare mode is enforced by
   * {@link OzoneManagerStateMachine#preAppendTransaction}
   * and {@link OzoneManagerRatisServer#submitRequest}
   *
   * @param value true if this Ozone Manager should be put in prepare mode,
   * false if this Ozone Manager should be taken out of prepare mode.
   */
  public static synchronized void setPrepared(boolean value) {
    isPrepared = value;
  }

  /**
   * If this Ozone Manager is not in prepare mode, returns true.
   * If this Ozone Manager is in prepare mode, returns true only if {@code
   * requestType} is{@code Prepare} or {@code CancelPrepare}. Returns false
   * otherwise.
   */
  public static synchronized boolean requestAllowed(Type requestType) {
    boolean requestAllowed = true;

    if (isPrepared) {
      // TODO: Also return true for cancel prepare when it is implemented.
      requestAllowed = (requestType == Type.Prepare);
    }

    return requestAllowed;
  }

  /**
   * Creates a prepare marker file inside {@code metadataDir} which contains
   * the log index {@code index}. If a marker file already exists, it will be
   * overwritten.
   */
  public static void writePrepareMarkerFile(ConfigurationSource conf,
      long index) throws IOException {
    File markerFile = getPrepareMarkerFile(conf);
    markerFile.getParentFile().mkdirs();
    try(FileOutputStream stream =
            new FileOutputStream(markerFile)) {
      stream.write(Long.toString(index).getBytes());
    }
  }

  /**
   * If a prepare marker file exists in {@code metadataDir} and contains a
   * log index matching {@code index}, the prepare state flag will be be
   * turned on. Otherwise, the prepare state flag will be turned off.
   */
  public static void checkPrepareMarkerFile(ConfigurationSource conf,
      long index) {
    File prepareMarkerFile = getPrepareMarkerFile(conf);
    if (prepareMarkerFile.exists()) {
      byte[] data = new byte[(int) prepareMarkerFile.length()];
      try(FileInputStream stream = new FileInputStream(prepareMarkerFile)) {
        stream.read(data);
      } catch (IOException e) {
        setPrepared(false);
      }

      try {
        long prepareMarkerIndex = Long.parseLong(new String(data));
        setPrepared(index == prepareMarkerIndex);
      } catch (NumberFormatException e) {
        setPrepared(false);
      }
    } else {
      // No marker file found.
      OzoneManagerPrepareState.setPrepared(false);
    }
  }

  private static File getPrepareMarkerFile(ConfigurationSource conf) {
    File markerFileDir = new File(ServerUtils.getOzoneMetaDirPath(conf),
        OMStorage.STORAGE_DIR_CURRENT);
    return new File(markerFileDir, OzoneConsts.PREPARE_MARKER);
  }
}
