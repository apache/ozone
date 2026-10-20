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

package org.apache.hadoop.ozone.jobworker.commands;

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;

/**
 * A class that acts as the base class to convert between Java and OM jobworker
 * commands in protobuf format.
 * @param <T>
 */
public abstract class OMJobworkerCommand<T extends Message> implements
    IdentifiableEventPayload {
  private final long id;
  private final boolean explicitId;
  private long expirationTimestampMs = 0;

  public OMJobworkerCommand() {
    // Transient control commands may use JVM-local IDs. Durable jobs require an explicit stable workflow ID.
    this.id = HddsIdFactory.getLongId();
    this.explicitId = false;
  }

  public OMJobworkerCommand(long id) {
    this.id = id;
    this.explicitId = true;
  }

  public boolean hasExplicitId() {
    return explicitId;
  }

  /** Reconstruct a previously committed command without allocating a new identity. */
  public static OMJobworkerCommand<OMJobworkerCommandProto> fromEnvelope(OMJobworkerCommandProto envelope) {
    OMJobworkerCommand<OMJobworkerCommandProto> command = new OMJobworkerCommand<OMJobworkerCommandProto>(
        envelope.getCmdId()) {
      @Override
      public OMJobworkerCommandProto.Type getType() {
        return envelope.getCommandType();
      }

      @Override
      public OMJobworkerCommandProto getProto() {
        return envelope;
      }
    };
    command.setExpirationTimestampMs(envelope.getExpirationTimestampMs());
    return command;
  }

  /**
   * Returns the type of this command.
   * @return Type
   */
  public abstract OMJobworkerCommandProto.Type getType();

  /**
   * Gets the protobuf message of this object.
   * @return A protobuf message.
   */
  public abstract T getProto();

  /**
   * Gets the commandId of this object.
   * @return uuid.
   */
  @Override
  public long getId() {
    return id;
  }

  /**
   * Allows setting a command expiration time as the milliseconds since the epoch.
   * Tasks that exceed this timestamp should be abandoned.
   * @param expirationTimestampMs The ms since epoch when the command must have completed by.
   */
  public void setExpirationTimestampMs(long expirationTimestampMs) {
    this.expirationTimestampMs = expirationTimestampMs;
  }

  /**
   * If the expiry timestamp has been set to a non-zero value, check if the current command has expired.
   * @param currentEpochMs current time in milliseconds since the epoch.
   * @return False if the command has not expired, or no expiration time has been set, otherwise, it is true.
   */
  public boolean hasExpired(long currentEpochMs) {
    return expirationTimestampMs > 0 &&
        currentEpochMs > expirationTimestampMs;
  }

  /**
   * @return The expiration timestamp set for this command, or zero if no command has been
   *         set.
   */
  public long getExpirationTimestampMs() {
    return expirationTimestampMs;
  }

}
