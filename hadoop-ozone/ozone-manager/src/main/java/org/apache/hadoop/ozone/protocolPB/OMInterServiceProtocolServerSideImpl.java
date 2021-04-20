package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMInterServiceProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.BootstrapErrorCode;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.BootstrapOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.BootstrapOMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OMInterServiceProtocolServerSideImpl implements
    OMInterServiceProtocolPB {

  private static final Logger LOG = LoggerFactory.getLogger(
      OMInterServiceProtocolServerSideImpl.class);

  private final OzoneManagerRatisServer omRatisServer;
  private final boolean isRatisEnabled;

  public OMInterServiceProtocolServerSideImpl(
      OzoneManagerRatisServer ratisServer, boolean enableRatis) {
    this.omRatisServer = ratisServer;
    this.isRatisEnabled = enableRatis;
  }

  @Override
  public BootstrapOMResponse bootstrap(RpcController controller,
      BootstrapOMRequest request) throws ServiceException {
    if (request == null) {
      return null;
    }
    if (!isRatisEnabled) {
      return BootstrapOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorCode(BootstrapErrorCode.RATIS_NOT_ENABLED)
          .setErrorMsg("New OM node cannot be bootstrapped as Ratis " +
              "is not enabled on existing OM")
          .build();
    }

    checkLeaderStatus();

    OMNodeDetails newOmNode = new OMNodeDetails.Builder()
        .setOMNodeId(request.getNodeId())
        .setHostAddress(request.getHostAddress())
        .setRatisPort(request.getRatisPort())
        .build();

    try {
      omRatisServer.addOMToRatisRing(newOmNode);
    } catch (IOException ex) {
      return BootstrapOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorCode(BootstrapErrorCode.RATIS_BOOTSTRAP_ERROR)
          .setErrorMsg(ex.getMessage())
          .build();
    }

    return BootstrapOMResponse.newBuilder()
        .setSuccess(true)
        .build();
  }

  private void checkLeaderStatus() throws ServiceException {
    OzoneManagerRatisUtils.checkLeaderStatus(omRatisServer.checkLeaderStatus(),
        omRatisServer.getRaftPeerId());
  }
}
