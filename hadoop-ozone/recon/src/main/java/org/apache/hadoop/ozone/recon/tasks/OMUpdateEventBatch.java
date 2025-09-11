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

package org.apache.hadoop.ozone.recon.tasks;

import java.util.Iterator;
import java.util.List;

/**
 * Wrapper class to hold multiple OM DB update events.
 */
public class OMUpdateEventBatch implements ReconEvent {

  private final List<OMDBUpdateEvent> events;
  private final long batchSequenceNumber;

  public OMUpdateEventBatch(List<OMDBUpdateEvent> e, long batchSequenceNumber) {
    events = e;
    this.batchSequenceNumber = batchSequenceNumber;
  }

  /**
   * Get Sequence Number and timestamp of last event in this batch.
   * @return Event Info instance.
   */
  long getLastSequenceNumber() {
    return this.batchSequenceNumber;
  }

  /**
   * Return iterator to Event batch.
   * @return iterator
   */
  public Iterator<OMDBUpdateEvent> getIterator() {
    return events.iterator();
  }

  /**
   * Return if empty.
   * @return true if empty, else false.
   */
  public boolean isEmpty() {
    return !getIterator().hasNext();
  }

  public List<OMDBUpdateEvent> getEvents() {
    return events;
  }
  
  @Override
  public EventType getEventType() {
    return EventType.OM_UPDATE_BATCH;
  }
  
  @Override
  public int getEventCount() {
    return events.size();
  }
}
