package org.apache.hadoop.ozone.om;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc.OzoneManagerServiceImplBase;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Grpc Service for handling S3 gateway OzoneManagerProtocol client requests.
 */
public class OzoneManagerServiceGrpc extends OzoneManagerServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerServiceGrpc.class);
  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private OzoneManagerProtocolServerSideTranslatorPB omTranslator;

  OzoneManagerServiceGrpc(
      OzoneManagerProtocolServerSideTranslatorPB omTranslator) {
    this.omTranslator = omTranslator;
  }

  @Override
  public void submitRequest(org.apache.hadoop.ozone.protocol.proto.
                                OzoneManagerProtocolProtos.
                                OMRequest request,
                            io.grpc.stub.StreamObserver<org.apache.
                                hadoop.ozone.protocol.proto.
                                OzoneManagerProtocolProtos.OMResponse>
                                responseObserver) {
    LOG.debug("GrpcOzoneManagerServer: OzoneManagerServiceImplBase " +
        "processing s3g client submit request");
    AtomicInteger callCount = new AtomicInteger(0);
    try {
      // need to look into handling the error path, trapping exception
      // will be generating OmResponse with status fail & OmException shortly
      org.apache.hadoop.ipc.Server.getCurCall().set(new Server.Call(1,
          callCount.incrementAndGet(),
          null,
          null,
          RPC.RpcKind.RPC_PROTOCOL_BUFFER,
          ClientId.getClientId()));

      OMResponse omResponse = this.omTranslator.
          submitRequest(NULL_RPC_CONTROLLER, request);
      responseObserver.onNext(omResponse);
    } catch (ServiceException e) {}
    responseObserver.onCompleted();
  }
}

