/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class to get/set the seek position used by the
 * OMEventListenerLedgerPoller.
 *
 * XXX: the seek position should be persisted (and ideally distrbuted to
 * all OMs) but at the moment it only lives in memory
 */
public class OMEventListenerLedgerPollerSeekPosition {
  public static final Logger LOG = LoggerFactory.getLogger(OMEventListenerLedgerPollerSeekPosition.class);

  private final AtomicReference<String> seekPosition;

  public OMEventListenerLedgerPollerSeekPosition() {
    this.seekPosition = new AtomicReference(initSeekPosition());
  }

  // TODO: load this from persistent storage
  public String initSeekPosition() {
    return null;
  }

  public String get() {
    return seekPosition.get();
  }

  public void set(String val) {
    LOG.debug("Setting seek position {}", val);
    // NOTE: this in-memory view of the seek position needs to be kept
    // up to date because the OMEventListenerLedgerPoller has a
    // reference to it
    seekPosition.set(val);
  }
}
