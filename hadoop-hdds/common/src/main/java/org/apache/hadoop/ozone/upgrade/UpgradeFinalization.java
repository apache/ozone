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

package org.apache.hadoop.ozone.upgrade;

import java.util.Collection;
import java.util.Collections;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/** Client-side interface of upgrade finalization. */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class UpgradeFinalization {

  /**
   * Default message can be used to indicate the starting of finalization.
   */
  public static final StatusAndMessages STARTING_MSG = new StatusAndMessages(
      Status.STARTING_FINALIZATION,
      Collections.singletonList("Starting Finalization")
  );

  public static final StatusAndMessages FINALIZATION_IN_PROGRESS_MSG = new StatusAndMessages(
      Status.FINALIZATION_IN_PROGRESS,
      Collections.singletonList("Finalization in progress")
  );

  public static final StatusAndMessages FINALIZATION_REQUIRED_MSG = new StatusAndMessages(
      Status.FINALIZATION_REQUIRED,
      Collections.singletonList("Finalization required")
  );

  /**
   * Default message to provide when the service is in ALREADY_FINALIZED state.
   */
  public static final StatusAndMessages FINALIZED_MSG = new StatusAndMessages(
      Status.ALREADY_FINALIZED, Collections.emptyList()
  );

  /**
   * Represents the current state in which the service is with regards to
   * finalization after an upgrade.
   * The state transitions are the following:
   * {@code ALREADY_FINALIZED} - no entry no exit from this status without restart.
   * After an upgrade:
   * {@code FINALIZATION_REQUIRED -(finalize)-> STARTING_FINALIZATION
   * -> FINALIZATION_IN_PROGRESS -> FINALIZATION_DONE} from finalization done
   * there is no more move possible, after a restart the service can end up in:
   * {@code FINALIZATION_REQUIRED}, if the finalization failed and have not reached
   * {@code FINALIZATION_DONE},
   * - or it can be {@code ALREADY_FINALIZED} if the finalization was successfully done.
   */
  public enum Status {
    ALREADY_FINALIZED,
    STARTING_FINALIZATION,
    FINALIZATION_IN_PROGRESS,
    FINALIZATION_DONE,
    FINALIZATION_REQUIRED,
  }

  /**
   * A class that holds the current service status, and if the finalization is
   * ongoing, the messages that should be passed to the initiating client of
   * finalization.
   * This translates to a counterpart in the RPC layer.
   */
  public static final class StatusAndMessages {
    private final Status status;
    private final Collection<String> msgs;

    /**
     * Constructs a StatusAndMessages tuple from the given params.
     * @param status the finalization status of the service
     * @param msgs the messages to be transferred to the client
     */
    public StatusAndMessages(Status status, Collection<String> msgs) {
      this.status = status;
      this.msgs = msgs;
    }

    /**
     * Provides the status.
     * @return the upgrade finalization status.
     */
    public Status status() {
      return status;
    }

    /**
     * Provides the messages, or an empty list if there are no messages.
     * @return a list with possibly multiple messages.
     */
    public Collection<String> msgs() {
      return msgs;
    }
  }

  private UpgradeFinalization() {
    // no instances
  }

}
