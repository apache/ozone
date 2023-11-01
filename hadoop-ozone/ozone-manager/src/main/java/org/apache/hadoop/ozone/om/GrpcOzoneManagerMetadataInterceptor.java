package org.apache.hadoop.ozone.om;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Optional;

public class GrpcOzoneManagerMetadataInterceptor implements ServerInterceptor {
  public static final Context.Key<String> CLIENT_ADDRESS_KEY =
      Context.key("clientAddressKey");

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall, Metadata metadata,
      ServerCallHandler<ReqT, RespT> next) {
    String clientAddress = Optional.ofNullable(
            (InetSocketAddress) serverCall.getAttributes()
                .get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR))
        .map(InetSocketAddress::getAddress).map(InetAddress::getHostAddress)
        .orElse(null);
    Context context =
        Context.current().withValue(CLIENT_ADDRESS_KEY, clientAddress);
    return Contexts.interceptCall(context, serverCall, metadata, next);
  }
}
