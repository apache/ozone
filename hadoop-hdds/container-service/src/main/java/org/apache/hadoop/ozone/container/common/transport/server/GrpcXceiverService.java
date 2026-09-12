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

package org.apache.hadoop.ozone.container.common.transport.server;

import static org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.getSendMethod;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.utils.io.RandomAccessFileChannel;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grpc Service for handling Container Commands on datanode.
 */
public class GrpcXceiverService extends
    XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase {
  public static final Logger
      LOG = LoggerFactory.getLogger(GrpcXceiverService.class);

  private final ContainerDispatcher dispatcher;
  // A streaming ReadBlock call has no deadline, so an idle stream would pin its block file indefinitely.
  // This timer closes the file of any stream idle for longer than the timeout; the next request reopens it.
  private final long streamReadFileIdleTimeoutNanos;
  private final ScheduledExecutorService idleFileCloser;
  private final ZeroCopyMessageMarshaller<ContainerCommandRequestProto>
      zeroCopyMessageMarshaller = new ZeroCopyMessageMarshaller<>(
          ContainerCommandRequestProto.getDefaultInstance());

  public GrpcXceiverService(ContainerDispatcher dispatcher, Duration streamReadFileIdleTimeout,
      String threadNamePrefix) {
    this.dispatcher = dispatcher;
    this.streamReadFileIdleTimeoutNanos = streamReadFileIdleTimeout.toNanos();
    this.idleFileCloser = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadNamePrefix + "ReadBlockIdleFileCloser").build());
  }

  public void shutdown() {
    idleFileCloser.shutdownNow();
  }

  /**
   * Bind service with zerocopy marshaller equipped for the `send` API.
   * @return  service definition.
   */
  public ServerServiceDefinition bindServiceWithZeroCopy() {
    ServerServiceDefinition orig = super.bindService();

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
      private final RandomAccessFileChannel blockFile = new RandomAccessFileChannel();
      // Held while a request is served, so the idle closer never closes the file under an in-flight read.
      private final ReentrantLock requestLock = new ReentrantLock();
      private volatile long lastRequestNanos;
      private volatile ScheduledFuture<?> idleFileCheck;

      boolean close() {
        if (isClosed.compareAndSet(false, true)) {
          final ScheduledFuture<?> check = idleFileCheck;
          if (check != null) {
            check.cancel(false);
          }
          blockFile.close();
          return true;
        }
        return false;
      }

      private void scheduleIdleFileCheck(long delayNanos) {
        try {
          idleFileCheck = idleFileCloser.schedule(this::closeFileIfIdle, delayNanos, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
          // Server is shutting down; the file is closed when the stream ends.
          LOG.debug("Idle file closer is shut down, not scheduling check", e);
        }
      }

      private void closeFileIfIdle() {
        if (isClosed.get()) {
          return;
        }
        final long idleNanos = System.nanoTime() - lastRequestNanos;
        if (idleNanos < streamReadFileIdleTimeoutNanos || !requestLock.tryLock()) {
          // Not idle for long enough, or a request is in flight: check again later.
          scheduleIdleFileCheck(idleNanos < streamReadFileIdleTimeoutNanos
              ? streamReadFileIdleTimeoutNanos - idleNanos : streamReadFileIdleTimeoutNanos);
          return;
        }
        try {
          if (blockFile.isOpen()) {
            LOG.debug("Closing block file of streaming read idle for {}ms", TimeUnit.NANOSECONDS.toMillis(idleNanos));
            blockFile.close();
          }
          // The next request reopens the file and schedules a new check.
          idleFileCheck = null;
        } finally {
          requestLock.unlock();
        }
      }

      @Override
      public void onNext(ContainerCommandRequestProto request) {
        final DispatcherContext context = request.getCmdType() != Type.ReadChunk ? null
            : DispatcherContext.newBuilder(DispatcherContext.Op.HANDLE_READ_CHUNK)
            .setReleaseSupported(true)
            .build();

        requestLock.lock();
        try {
          if (request.getCmdType() == Type.ReadBlock) {
            dispatcher.streamDataReadOnly(request, responseObserver, blockFile, context);
          } else {
            final ContainerCommandResponseProto resp = dispatcher.dispatch(request, context);
            responseObserver.onNext(resp);
          }
        } catch (Throwable e) {
          LOG.error("Got exception when processing"
                    + " ContainerCommandRequestProto {}", request, e);
          close();
          responseObserver.onError(e);
        } finally {
          zeroCopyMessageMarshaller.release(request);
          if (context != null) {
            context.release();
          }
          lastRequestNanos = System.nanoTime();
          if (!isClosed.get() && blockFile.isOpen() && idleFileCheck == null) {
            scheduleIdleFileCheck(streamReadFileIdleTimeoutNanos);
          }
          requestLock.unlock();
        }
      }

      @Override
      public void onError(Throwable t) {
        close();
        if (t instanceof StatusRuntimeException) {
          if (((StatusRuntimeException) t).getStatus().getCode() == Status.Code.CANCELLED) {
            return;
          }
        }

        // for now we just log a msg
        LOG.error("ContainerCommand send on error. Exception: ", t);
      }

      @Override
      public void onCompleted() {
        if (close()) {
          LOG.debug("ContainerCommand send completed");
          responseObserver.onCompleted();
        }
      }
    };
  }
}
