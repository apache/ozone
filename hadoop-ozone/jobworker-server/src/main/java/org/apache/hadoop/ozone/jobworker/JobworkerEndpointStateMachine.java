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

package org.apache.hadoop.ozone.jobworker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerEndpointStateMachine is used as a holder class that keeps state
 * around the gRPC endpoint to OzoneManager.
 */
public class JobworkerEndpointStateMachine implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerEndpointStateMachine.class);

  private final JobworkerProtocol endPoint;
  private final AtomicLong missedCount;
  private final InetSocketAddress address;
  private final Lock lock;
  private final ConfigurationSource conf;
  private final ExecutorService executorService;
  private final String configuredOmServiceId;
  private String omServiceId;
  private EndpointStates state;
  private GetOMVersionResponse version;
  private ZonedDateTime lastSuccessfulHeartbeat;
  private volatile long term = 0;

  /**
   * Constructs a JobworkerEndpointStateMachine.
   *
   * @param address          - Address of the OM endpoint
   * @param endPoint         - The RPC endpoint
   * @param conf             - Configuration
   * @param threadNamePrefix - Prefix for thread names
   * @param configuredOmServiceId  - OM service ID in the current jobworker configuration
   */
  public JobworkerEndpointStateMachine(InetSocketAddress address,
                                       JobworkerProtocol endPoint, ConfigurationSource conf,
                                       String threadNamePrefix, String configuredOmServiceId) {
    this.endPoint = endPoint;
    this.missedCount = new AtomicLong(0);
    this.address = address;
    this.state = EndpointStates.getInitState();
    this.lock = new ReentrantLock();
    this.conf = conf;
    this.configuredOmServiceId = configuredOmServiceId;
    executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat(threadNamePrefix + "JobworkerEndpointStateMachineTaskThread-"
            + this.address + "-%d ")
        .build());
  }

  /**
   * Takes a lock on this endpoint so that other threads don't use this while we
   * are trying to communicate via this endpoint.
   */
  public void lock() {
    lock.lock();
  }

  /**
   * Unlocks this endpoint.
   */
  public void unlock() {
    lock.unlock();
  }

  /**
   * Returns the version that we read from the OM.
   *
   * @return - Version Response.
   */
  public GetOMVersionResponse getVersion() {
    return version;
  }

  /**
   * Sets the Version response we received from the OM.
   *
   * @param version Version response
   */
  public void setVersion(GetOMVersionResponse version) {
    this.version = version;
  }

  /**
   * Returns the current State this end point is in.
   *
   * @return current state
   */
  public EndpointStates getState() {
    return state;
  }

  /**
   * Sets the endpoint state.
   *
   * @param epState - end point state.
   * @return The updated state
   */
  public EndpointStates setState(EndpointStates epState) {
    this.state = epState;
    return this.state;
  }

  /**
   * Returns the endpoint specific ExecutorService.
   *
   * @return ExecutorService
   */
  public ExecutorService getExecutorService() {
    return executorService;
  }

  /**
   * Closes the connection.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (endPoint != null) {
      endPoint.close();
    }
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    state = EndpointStates.SHUTDOWN;
  }

  /**
   * We maintain a count of how many times we missed communicating with a
   * specific OM. This is not made atomic since the access to this is always
   * guarded by the read or write lock. That is, it is serialized.
   */
  public void incMissed() {
    this.missedCount.incrementAndGet();
  }

  /**
   * Returns the value of the missed count.
   *
   * @return missed count
   */
  public long getMissedCount() {
    return this.missedCount.get();
  }

  /**
   * Resets the missed count to zero.
   */
  public void zeroMissedCount() {
    this.missedCount.set(0);
  }

  /**
   * Returns the InetAddress of the endPoint.
   *
   * @return InetSocketAddress
   */
  public InetSocketAddress getAddress() {
    return this.address;
  }

  /**
   * Returns real RPC endPoint.
   *
   * @return JobworkerProtocol
   */
  public JobworkerProtocol getEndPoint() {
    return endPoint;
  }

  /**
   * Get the OM service ID for this endpoint.
   *
   * @return OM service ID
   */
  public String getOMServiceId() {
    return omServiceId;
  }

  /**
   * Get the OM service ID in the current jobworker configuration for this endpoint.
   *
   * @return OM service ID
   */
  public String getConfiguredOmServiceId() {
    return configuredOmServiceId;
  }

  /**
   * Set the OM service ID for this endpoint.
   *
   * @param serviceId the OM service ID
   */
  public void setOmServiceId(String serviceId) {
    this.omServiceId = serviceId;
  }

  /**
   * Get the term (leadership epoch) for this OM endpoint.
   *
   * @return leadership term
   */
  public long getTerm() {
    return term;
  }

  /**
   * Set the term (leadership epoch) for this OM endpoint.
   *
   * @param term leadership term
   */
  public void setTerm(long term) {
    this.term = term;
  }

  /**
   * Logs exception if needed based on configuration settings.
   *
   * @param ex Exception
   */
  public void logIfNeeded(Exception ex) {
    JobworkerConfiguration jwConf = conf.getObject(JobworkerConfiguration.class);
    double missCounter = this.getMissedCount() % jwConf.getHeartbeatLogWarnInterval();

    if (missCounter == 0) {
      LOG.warn(
          "Unable to communicate to OM server at {} for past {}.",
          getAddress().getHostString() + ":" + getAddress().getPort(),
          jwConf.getHeartbeatInterval().multipliedBy(this.getMissedCount()), ex);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Incrementing the Missed count.", ex);
    }
    this.incMissed();
  }

  /**
   * Set the timestamp of the last successful heartbeat.
   *
   * @param lastSuccessfulHeartbeat timestamp
   */
  public void setLastSuccessfulHeartbeat(
      ZonedDateTime lastSuccessfulHeartbeat) {
    this.lastSuccessfulHeartbeat = lastSuccessfulHeartbeat;
  }

  @Override
  public String toString() {
    return "JobworkerEndpointStateMachine{" +
        "lastSuccessfulHeartbeat=" + lastSuccessfulHeartbeat +
        ", term=" + term +
        ", version=" + version +
        ", state=" + state +
        ", omServiceId='" + omServiceId + '\'' +
        ", configuredOmServiceId='" + configuredOmServiceId + '\'' +
        ", address=" + address +
        '}';
  }

  /**
   * States that an Endpoint can be in.
   * <p>
   * This is a sorted list of states that EndPoint will traverse.
   * <p>
   * GetNextState will move this enum from getInitState to getLastState.
   */
  public enum EndpointStates {
    GETVERSION(1),
    REGISTER(2),
    HEARTBEAT(3),
    SHUTDOWN(4);

    private final int value;

    /**
     * Constructs endPointStates.
     *
     * @param value state value
     */
    EndpointStates(int value) {
      this.value = value;
    }

    /**
     * Returns the first State.
     *
     * @return First State.
     */
    public static EndpointStates getInitState() {
      return GETVERSION;
    }

    /**
     * The last state of endpoint states.
     *
     * @return last state.
     */
    public static EndpointStates getLastState() {
      return SHUTDOWN;
    }

    /**
     * returns the numeric value associated with the endPoint.
     *
     * @return int.
     */
    public int getValue() {
      return value;
    }

    /**
     * Returns the next logical state that endPoint should move to.
     * The next state is computed by adding 1 to the current state.
     *
     * @return NextState.
     */
    public EndpointStates getNextState() {
      if (this.getValue() < getLastState().getValue()) {
        int stateValue = this.getValue() + 1;
        for (EndpointStates iter : values()) {
          if (stateValue == iter.getValue()) {
            return iter;
          }
        }
      }
      return getLastState();
    }
  }
}
