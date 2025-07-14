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

package org.apache.hadoop.ozone.protocol.commands;

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;

/**
 * A class that acts as the base class to convert between Java and SCM
 * commands in protobuf format.
 * @param <T>
 */
public abstract class SCMCommand<T extends Message> implements
    IdentifiableEventPayload {
  private final long id;

  // If running upon Ratis, holds term of underlying RaftServer iff current
  // SCM is a leader. If running without Ratis, holds SCMContext.INVALID_TERM.
  private long term;

  private String encodedToken = "";

  private long deadlineMsSinceEpoch = 0;

  SCMCommand() {
    this.id = HddsIdFactory.getLongId();
  }

  SCMCommand(long id) {
    this.id = id;
  }

  /**
   * Returns the type of this command.
   * @return Type
   */
  public  abstract SCMCommandProto.Type getType();

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
   * Get term of this command.
   * @return term
   */
  public long getTerm() {
    return term;
  }

  /**
   * Set term of this command.
   */
  public void setTerm(long term) {
    this.term = term;
  }

  public String getEncodedToken() {
    return encodedToken;
  }

  public void setEncodedToken(String encodedToken) {
    this.encodedToken = encodedToken;
  }

  /**
   * Allows a deadline to be set on the command. The deadline is set as the
   * milliseconds since the epoch when the command must have been completed by.
   * It is up to the code processing the command to enforce the deadline by
   * calling the hasExpired() method, and the code sending the command to set
   * the deadline. The default deadline is zero, which means no deadline.
   * @param deadlineMs The ms since epoch when the command must have completed
   *                   by.
   */
  public void setDeadline(long deadlineMs) {
    this.deadlineMsSinceEpoch = deadlineMs;
  }

  /**
   * @return The deadline set for this command, or zero if no command has been
   *         set.
   */
  public long getDeadline() {
    return deadlineMsSinceEpoch;
  }

  /**
   * If a deadline has been set to a non zero value, test if the current time
   * passed is beyond the deadline or not.
   * @param currentEpochMs current time in milliseconds since the epoch.
   * @return false if there is no deadline, or it has not expired. True if the
   *         set deadline has expired.
   */
  public boolean hasExpired(long currentEpochMs) {
    return deadlineMsSinceEpoch > 0 &&
        currentEpochMs > deadlineMsSinceEpoch;
  }

  public boolean contributesToQueueSize() {
    return true;
  }

  @Override
  public String toString() {
    return getType() + "(id=" + id + ", term=" + term + ')';
  }
}
