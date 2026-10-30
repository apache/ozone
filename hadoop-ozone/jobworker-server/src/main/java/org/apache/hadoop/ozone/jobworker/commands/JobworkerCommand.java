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
import jakarta.annotation.Nonnull;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;

/**
 * Base class for all jobworker commands.
 *
 * @param <T> Type of the command specific proto inside the wrapper command
 */
public abstract class JobworkerCommand<T extends Message> {
  private final long id;
  private long term;
  private final String omServiceId;
  private long expirationTimestampMs;

  /**
   * Wraps a command received from the OM leader of the given service in the given term.
   */
  public static JobworkerCommand<OMJobworkerCommandProto> fromEnvelope(
      OMJobworkerCommandProto envelope, String service, long term) {
    return new JobworkerCommand<OMJobworkerCommandProto>(
        envelope.getCmdId(), service, term, envelope.getExpirationTimestampMs()) {
      @Override
      public OMJobworkerCommandProto.Type getType() {
        return envelope.getCommandType();
      }

      @Override
      public OMJobworkerCommandProto getProto() {
        return envelope;
      }
    };
  }

  /**
   * Constructor with command ID.
   *
   * @param id Command ID
   */
  public JobworkerCommand(long id, String omServiceId) {
    Objects.requireNonNull(omServiceId, "omServiceId cannot be null");
    this.id = id;
    this.omServiceId = omServiceId;
  }

  /**
   * Constructor with command ID.
   *
   * @param id Command ID
   */
  public JobworkerCommand(long id, String omServiceId, long term, long expirationTimestampMs) {
    Objects.requireNonNull(omServiceId, "omServiceId cannot be null");
    this.id = id;
    this.omServiceId = omServiceId;
    this.term = term;
    this.expirationTimestampMs = expirationTimestampMs;
  }

  /**
   * Returns the command ID.
   *
   * @return ID
   */
  public long getId() {
    return id;
  }

  /**
   * Returns the command type.
   *
   * @return command type
   */
  public abstract OMJobworkerCommandProto.Type getType();

  /**
   * Returns the wrapped proto message.
   *
   * @return proto message
   */
  public abstract T getProto();

  /**
   * Returns the term of the OM leader that generated this command.
   *
   * @return term
   */
  public long getTerm() {
    return term;
  }

  /**
   * Returns the OM service ID this command was sent from.
   *
   * @return OM service ID
   */
  @Nonnull
  public String getOmServiceId() {
    return omServiceId;
  }

  /**
   * Returns the deadline for this command.
   *
   * @return deadline timestamp
   */
  public final long getExpirationTimestampMs() {
    return expirationTimestampMs;
  }

  /**
   * Check if the command has expired.
   *
   * @param currentTimeMillis current time in milliseconds
   * @return true if expired, false otherwise
   */
  public boolean hasExpired(long currentTimeMillis) {
    return getExpirationTimestampMs() > 0 && currentTimeMillis > getExpirationTimestampMs();
  }
}
