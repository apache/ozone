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

package org.apache.hadoop.ozone.container.common.transport.server;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.getSendMethod;

/**
 * Grpc Service for handling Container Commands on datanode.
 */
public class GrpcXceiverService extends
    XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase {
  public static final Logger
      LOG = LoggerFactory.getLogger(GrpcXceiverService.class);

  private final ContainerDispatcher dispatcher;
  private final boolean zeroCopyEnabled;
  private final ZeroCopyMessageMarshaller<ContainerCommandRequestProto>
      zeroCopyMessageMarshaller = new ZeroCopyMessageMarshaller<>(
          ContainerCommandRequestProto.getDefaultInstance());

  public GrpcXceiverService(ContainerDispatcher dispatcher,
      boolean zeroCopyEnabled) {
    this.dispatcher = dispatcher;
    this.zeroCopyEnabled = zeroCopyEnabled;
  }

  /**
   * Bind service with zerocopy marshaller equipped for the `send` API if
   * zerocopy is enabled.
   * @return  service definition.
   */
  public ServerServiceDefinition bindServiceWithZeroCopy() {
    ServerServiceDefinition orig = super.bindService();
    if (!zeroCopyEnabled) {
      LOG.info("Zerocopy is not enabled.");
      return orig;
    }

    ServerServiceDefinition.Builder builder =
        ServerServiceDefinition.builder(orig.getServiceDescriptor().getName());
    // Add `send` method with zerocopy marshaller.
    addZeroCopyMethod(orig, builder, getSendMethod(),
        zeroCopyMessageMarshaller);
    // Add other methods as is.
    orig.getMethods().stream().filter(
        x -> !x.getMethodDescriptor().getFullMethodName().equals(
            getSendMethod().getFullMethodName())
    ).forEach(
        builder::addMethod
    );

    return builder.build();
  }

  private static <Req extends MessageLite, Resp> void addZeroCopyMethod(
      ServerServiceDefinition orig,
      ServerServiceDefinition.Builder newServiceBuilder,
      MethodDescriptor<Req, Resp> origMethod,
      ZeroCopyMessageMarshaller<Req> zeroCopyMarshaller) {
    MethodDescriptor<Req, Resp> newMethod = origMethod.toBuilder()
        .setRequestMarshaller(zeroCopyMarshaller)
        .build();
    @SuppressWarnings("unchecked")
    ServerCallHandler<Req, Resp> serverCallHandler =
        (ServerCallHandler<Req, Resp>) orig.getMethod(
            newMethod.getFullMethodName()).getServerCallHandler();
    newServiceBuilder.addMethod(newMethod, serverCallHandler);
  }

  @Override
  public StreamObserver<ContainerCommandRequestProto> send(
      StreamObserver<ContainerCommandResponseProto> responseObserver) {
    return new StreamObserver<ContainerCommandRequestProto>() {
      private final AtomicBoolean isClosed = new AtomicBoolean(false);

      @Override
      public void onNext(ContainerCommandRequestProto request) {
        try {
          ContainerCommandResponseProto resp =
              dispatcher.dispatch(request, null);
          responseObserver.onNext(resp);
        } catch (Throwable e) {
          LOG.error("Got exception when processing"
                    + " ContainerCommandRequestProto {}", request, e);
          isClosed.set(true);
          responseObserver.onError(e);
        } finally {
          InputStream popStream = zeroCopyMessageMarshaller.popStream(request);
          if (popStream != null) {
            IOUtils.close(LOG, popStream);
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        // for now we just log a msg
        LOG.error("ContainerCommand send on error. Exception: ", t);
      }

      @Override
      public void onCompleted() {
        if (isClosed.compareAndSet(false, true)) {
          LOG.debug("ContainerCommand send completed");
          responseObserver.onCompleted();
        }
      }
    };
  }
}
