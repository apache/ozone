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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetrics;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetricsServerRequestInterceptor;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark for HDDS-16742: the cost of setting the {@code LatestRequestType} tag in
 * {@link GrpcMetricsServerRequestInterceptor#interceptCall}'s {@code onMessage}.
 *
 * <p>Two arms run in the same JVM, interleaved, on the same {@code OMRequest} objects:
 * <ul>
 *   <li>{@code text}: the removed implementation, copied below in {@link TextFormatRequestInterceptor}. It renders
 *       the whole request with {@code toString()} and parses the first line.</li>
 *   <li>{@code descriptor}: the current {@link GrpcMetricsServerRequestInterceptor}, which reads {@code cmdType}
 *       through the message descriptor.</li>
 * </ul>
 * Everything else the listener does (serialized size, received-bytes counter, forwarding to a no-op delegate) is
 * identical in both arms, so the difference is the request-type extraction.
 *
 * <p>Reported per request size: single-thread and {@link #THREADS}-thread throughput (median of
 * {@link #ROUNDS} rounds), per-call latency percentiles (single thread, over all rounds; each sample
 * includes the cost of the {@code System.nanoTime} calls around it), bytes allocated per call (from the per-thread
 * allocation counter of the JVM), and young/old GC count and time during the multi-thread measurement.
 *
 * <p>Run with:
 * <pre>
 *   mvn -pl :ozone-manager test -Dtest=TestGrpcMetricsInterceptorBenchmark -Dexcluded-test-groups= \
 *     -DskipShade -DskipRecon
 * </pre>
 */
@Tag("benchmark")
public class TestGrpcMetricsInterceptorBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TestGrpcMetricsInterceptorBenchmark.class);

  private static final long WARMUP_MS = 2_000;
  private static final long MEASURE_MS = 4_000;
  private static final int ROUNDS = 3;
  private static final int THREADS = 8;
  private static final int LATENCY_SAMPLES = 50_000;
  private static final int ALLOCATION_CALLS = 20_000;

  private static final com.sun.management.ThreadMXBean THREAD_MX =
      (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

  @Test
  // The 5 minute default timeout of the build applies unless a method-level @Timeout overrides it.
  @Timeout(value = 30, unit = TimeUnit.MINUTES)
  void benchmarkRequestTypeExtraction() throws Exception {
    assertTrue(THREAD_MX.isThreadAllocatedMemorySupported(), "needs per-thread allocation counters");
    THREAD_MX.setThreadAllocatedMemoryEnabled(true);

    List<OMRequest> requests = Arrays.asList(lookupKey(), createKey(), deleteKeys(1000));
    StringBuilder report = new StringBuilder(String.format("%n"
        + "BENCHRESULT gRPC metrics request interceptor, LatestRequestType extraction (HDDS-16742)%n"
        + "  rounds=%d (warm-up %d ms, measure %d ms each), multi-thread=%d threads; text = toString() parsing,"
        + " descriptor = field read%n", ROUNDS, WARMUP_MS, MEASURE_MS, THREADS));

    for (OMRequest request : requests) {
      // both arms must report the same tag value
      assertEquals(request.getCmdType().name(), extractOnce(Arm.TEXT, request));
      assertEquals(request.getCmdType().name(), extractOnce(Arm.DESCRIPTOR, request));

      Result text = new Result();
      Result descriptor = new Result();
      for (int round = 0; round < ROUNDS; round++) {
        // alternate which arm goes first in each round
        Arm first = (round & 1) == 0 ? Arm.TEXT : Arm.DESCRIPTOR;
        Arm second = first == Arm.TEXT ? Arm.DESCRIPTOR : Arm.TEXT;
        measure(first, request, first == Arm.TEXT ? text : descriptor);
        measure(second, request, second == Arm.TEXT ? text : descriptor);
      }
      report.append(format(request, text, descriptor));
    }
    LOG.info(report.toString());
  }

  private enum Arm { TEXT, DESCRIPTOR }

  private void measure(Arm arm, OMRequest request, Result result) throws Exception {
    result.singleThreadOps.add(throughput(arm, request, 1));
    result.allocatedBytesPerOp.add(allocatedBytesPerOp(arm, request));
    result.latencies.add(latencies(arm, request));
    GcSnapshot gcBefore = GcSnapshot.now();
    result.multiThreadOps.add(throughput(arm, request, THREADS));
    GcSnapshot gcAfter = GcSnapshot.now();
    result.gcCount.add((double) (gcAfter.count - gcBefore.count));
    result.gcMillis.add((double) (gcAfter.millis - gcBefore.millis));
  }

  /** Runs {@code threads} threads, each with its own listener, and returns the total calls per second. */
  private double throughput(Arm arm, OMRequest request, int threads) throws Exception {
    GrpcMetrics metrics = newMetrics();
    long[] counts = new long[threads];
    CountDownLatch done = new CountDownLatch(threads);
    CountDownLatch start = new CountDownLatch(1);
    for (int t = 0; t < threads; t++) {
      final int index = t;
      Thread worker = new Thread(() -> {
        ServerCall.Listener<Object> listener = listener(arm, metrics);
        try {
          start.await();
          long warmupEnd = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WARMUP_MS);
          while (System.nanoTime() < warmupEnd) {
            listener.onMessage(request);
          }
          long measureEnd = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MEASURE_MS);
          long n = 0;
          // check the clock every 256 calls to keep its cost out of the loop
          while (true) {
            for (int i = 0; i < 256; i++) {
              listener.onMessage(request);
            }
            n += 256;
            if (System.nanoTime() >= measureEnd) {
              break;
            }
          }
          counts[index] = n;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
      worker.start();
    }
    start.countDown();
    done.await();
    long total = 0;
    for (long c : counts) {
      total += c;
    }
    return total / (MEASURE_MS / 1000.0);
  }

  /** Bytes allocated by the calling thread per {@code onMessage} call, after a warm-up. */
  private double allocatedBytesPerOp(Arm arm, OMRequest request) {
    ServerCall.Listener<Object> listener = listener(arm, newMetrics());
    for (int i = 0; i < ALLOCATION_CALLS; i++) {
      listener.onMessage(request);
    }
    int calls = ALLOCATION_CALLS;
    long tid = Thread.currentThread().getId();
    long before = THREAD_MX.getThreadAllocatedBytes(tid);
    for (int i = 0; i < calls; i++) {
      listener.onMessage(request);
    }
    long after = THREAD_MX.getThreadAllocatedBytes(tid);
    return (after - before) / (double) calls;
  }

  /** Per-call latencies in nanoseconds, single thread, after a warm-up. */
  private long[] latencies(Arm arm, OMRequest request) {
    ServerCall.Listener<Object> listener = listener(arm, newMetrics());
    for (int i = 0; i < LATENCY_SAMPLES; i++) {
      listener.onMessage(request);
    }
    long[] samples = new long[LATENCY_SAMPLES];
    for (int i = 0; i < LATENCY_SAMPLES; i++) {
      long t0 = System.nanoTime();
      listener.onMessage(request);
      samples[i] = System.nanoTime() - t0;
    }
    return samples;
  }

  private static String extractOnce(Arm arm, OMRequest request) {
    GrpcMetrics metrics = newMetrics();
    listener(arm, metrics).onMessage(request);
    return metrics.getRequestType();
  }

  @SuppressWarnings("unchecked")
  private static ServerCall.Listener<Object> listener(Arm arm, GrpcMetrics metrics) {
    ServerInterceptor interceptor = arm == Arm.TEXT
        ? new TextFormatRequestInterceptor(metrics)
        : new GrpcMetricsServerRequestInterceptor(metrics);
    ServerCallHandler<Object, Object> handler = (call, headers) -> new ServerCall.Listener<Object>() { };
    return interceptor.interceptCall(mock(ServerCall.class), new Metadata(), handler);
  }

  private static GrpcMetrics newMetrics() {
    GrpcMetrics metrics = new GrpcMetrics(new OzoneConfiguration());
    // populates the @Metric counters, as DefaultMetricsSystem.register() does
    MetricsAnnotations.makeSource(metrics);
    return metrics;
  }

  /**
   * The {@code onMessage} logic removed by HDDS-16742, copied verbatim, so both arms can run in one JVM. The
   * timing fields and {@code onComplete} are not used by this benchmark and are omitted.
   */
  private static final class TextFormatRequestInterceptor implements ServerInterceptor {
    private final GrpcMetrics grpcMetrics;

    TextFormatRequestInterceptor(GrpcMetrics grpcMetrics) {
      this.grpcMetrics = grpcMetrics;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall,
        Metadata headers, ServerCallHandler<ReqT, RespT> serverCallHandler) {
      return new SimpleForwardingServerCallListener<ReqT>(serverCallHandler.startCall(serverCall, headers)) {
        @Override
        public void onMessage(ReqT message) {
          long messageSize = 0;
          if (message instanceof AbstractMessage) {
            AbstractMessage parsedMessage = (AbstractMessage) message;
            messageSize += parsedMessage.getSerializedSize();
          } else {
            grpcMetrics.incrUnknownMessagesReceived();
          }

          grpcMetrics.incrReceivedBytes(messageSize);

          String[] messageFields = message.toString()
              .split(System.lineSeparator());
          // messageFields[0] should be in the format
          // cmdType: "type"
          String[] cmdTypeLine = messageFields[0].split(":");

          // Get only the type and remove any leading spaces
          grpcMetrics.setRequestType(cmdTypeLine[1].trim());

          super.onMessage(message);
        }
      };
    }
  }

  private static OMRequest.Builder baseRequest(Type type) {
    return OMRequest.newBuilder()
        .setCmdType(type)
        .setClientId("client-0f3a6c2e-5d5b-4b0e-9c62-7a3c1e0b8f41")
        .setTraceID("4f1c2d3e4a5b6c7d:8e9f0a1b2c3d4e5f:0:1")
        .setUserInfo(UserInfo.newBuilder().setUserName("hadoop").setRemoteAddress("10.0.0.17").build());
  }

  private static KeyArgs.Builder keyArgs(String keyName) {
    return KeyArgs.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName(keyName);
  }

  /** A small read request. */
  private static OMRequest lookupKey() {
    return baseRequest(Type.LookupKey)
        .setLookupKeyRequest(LookupKeyRequest.newBuilder()
            .setKeyArgs(keyArgs("dir1/dir2/part-00000.parquet").setSortDatanodes(true).setLatestVersionLocation(true)))
        .build();
  }

  /** A typical write: key arguments with ACLs and user metadata. */
  private static OMRequest createKey() {
    KeyArgs.Builder args = keyArgs("warehouse/sales/date=2026-10-13/part-00042-c000.snappy.parquet")
        .setDataSize(134_217_728L)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE)
        .setModificationTime(1_791_900_000_000L);
    for (int i = 0; i < 4; i++) {
      args.addAcls(OzoneAclInfo.newBuilder()
          .setType(i == 0 ? OzoneAclInfo.OzoneAclType.USER : OzoneAclInfo.OzoneAclType.GROUP)
          .setName(i == 0 ? "hadoop" : "group" + i)
          .setRights(ByteString.copyFrom(new byte[] {(byte) 0x7f, 0x01}))
          .setAclScope(OzoneAclInfo.OzoneAclScope.ACCESS));
    }
    for (int i = 0; i < 4; i++) {
      args.addMetadata(HddsProtos.KeyValue.newBuilder().setKey("x-amz-meta-attr" + i).setValue("value-" + i));
    }
    return baseRequest(Type.CreateKey)
        .setCreateKeyRequest(CreateKeyRequest.newBuilder().setKeyArgs(args).setClientID(123_456_789L))
        .build();
  }

  /** A large request: one delete of {@code n} keys, the shape of an S3 multi-object delete. */
  private static OMRequest deleteKeys(int n) {
    DeleteKeyArgs.Builder args = DeleteKeyArgs.newBuilder().setVolumeName("vol1").setBucketName("bucket1");
    for (int i = 0; i < n; i++) {
      args.addKeys(String.format("logs/2026/10/13/app-%05d.log.gz", i));
    }
    return baseRequest(Type.DeleteKeys)
        .setDeleteKeysRequest(DeleteKeysRequest.newBuilder().setDeleteKeys(args))
        .build();
  }

  private static String format(OMRequest request, Result text, Result descriptor) {
    long[] t = Result.merged(text.latencies);
    long[] d = Result.merged(descriptor.latencies);
    return String.format("%n  %s (%,d bytes serialized)%n"
            + "    %-28s %14s %14s %9s%n"
            + "    %-28s %14.0f %14.0f %8.1fx%n"
            + "    %-28s %14.0f %14.0f %8.1fx%n"
            + "    %-28s %14.0f %14.0f %8.1fx%n"
            + "    %-28s %14d %14d%n"
            + "    %-28s %14d %14d%n"
            + "    %-28s %14d %14d%n"
            + "    %-28s %14.0f %14.0f%n"
            + "    %-28s %14.0f %14.0f%n",
        request.getCmdType(), request.getSerializedSize(),
        "", "text", "descriptor", "change",
        "ops/s, 1 thread", median(text.singleThreadOps), median(descriptor.singleThreadOps),
        median(descriptor.singleThreadOps) / median(text.singleThreadOps),
        "ops/s, " + THREADS + " threads", median(text.multiThreadOps), median(descriptor.multiThreadOps),
        median(descriptor.multiThreadOps) / median(text.multiThreadOps),
        "bytes allocated / call", median(text.allocatedBytesPerOp), median(descriptor.allocatedBytesPerOp),
        median(text.allocatedBytesPerOp) / Math.max(1.0, median(descriptor.allocatedBytesPerOp)),
        "latency p50 ns", percentile(t, 0.50), percentile(d, 0.50),
        "latency p99 ns", percentile(t, 0.99), percentile(d, 0.99),
        "latency p99.9 ns", percentile(t, 0.999), percentile(d, 0.999),
        "GCs, " + THREADS + "-thread runs", median(text.gcCount), median(descriptor.gcCount),
        "GC ms, " + THREADS + "-thread runs", median(text.gcMillis), median(descriptor.gcMillis));
  }

  private static double median(List<Double> values) {
    List<Double> sorted = new ArrayList<>(values);
    sorted.sort(null);
    return sorted.get(sorted.size() / 2);
  }

  private static long percentile(long[] sorted, double p) {
    int index = (int) Math.ceil(p * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
  }

  /** Measurements of one arm for one request, collected over all rounds. */
  private static final class Result {
    private final List<Double> singleThreadOps = new ArrayList<>();
    private final List<Double> multiThreadOps = new ArrayList<>();
    private final List<Double> allocatedBytesPerOp = new ArrayList<>();
    private final List<Double> gcCount = new ArrayList<>();
    private final List<Double> gcMillis = new ArrayList<>();
    private final List<long[]> latencies = new ArrayList<>();

    static long[] merged(List<long[]> samples) {
      int size = 0;
      for (long[] s : samples) {
        size += s.length;
      }
      long[] all = new long[size];
      int offset = 0;
      for (long[] s : samples) {
        System.arraycopy(s, 0, all, offset, s.length);
        offset += s.length;
      }
      Arrays.sort(all);
      return all;
    }
  }

  /** Total collection count and time over all garbage collectors. */
  private static final class GcSnapshot {
    private final long count;
    private final long millis;

    private GcSnapshot(long count, long millis) {
      this.count = count;
      this.millis = millis;
    }

    static GcSnapshot now() {
      long count = 0;
      long millis = 0;
      for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
        count += Math.max(0, gc.getCollectionCount());
        millis += Math.max(0, gc.getCollectionTime());
      }
      return new GcSnapshot(count, millis);
    }
  }
}
