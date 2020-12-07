package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

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
}
