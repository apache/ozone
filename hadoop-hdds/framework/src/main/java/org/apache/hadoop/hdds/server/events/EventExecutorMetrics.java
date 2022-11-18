/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server.events;

/**
 * Metrics for Event Executors.
 */
public interface EventExecutorMetrics {
  /**
   * get name.
   */
  String getName();
  
  /**
   * stop metrics.
   */
  void stop();
  
  /**
   * Increment the number of the failed events.
   */
  void incrFailedEvents(long delta);


  /**
   * Increment the number of the processed events.
   */
  void incrSuccessfulEvents(long delta);

  /**
   * Increment the number of the not-yet processed events.
   */
  void incrQueuedEvents(long delta);

  /**
   * Increment the number of events scheduled to be processed.
   */
  void incrScheduledEvents(long delta);

  /**
   * Increment the number of dropped events to be processed.
   */
  void incrDroppedEvents(long delta);

  /**
   * Increment the number of events having long wait in queue 
   * crossing threshold.
   */
  void incrLongWaitInQueueEvents(long delta);

  /**
   * Increment the number of events having long execution crossing threshold.
   */
  void incrLongTimeExecutionEvents(long delta);

  /**
   * Return the number of the failed events.
   */
  long failedEvents();


  /**
   * Return the number of the processed events.
   */
  long successfulEvents();

  /**
   * Return the number of the not-yet processed events.
   */
  long queuedEvents();

  /**
   * Return the number of events scheduled to be processed.
   */
  long scheduledEvents();

  /**
   * Return the number of dropped events to be processed.
   */
  long droppedEvents();

  /**
   * Return the number of events having long wait in queue crossing threshold.
   */
  long longWaitInQueueEvents();

  /**
   * Return the number of events having long execution crossing threshold.
   */
  long longTimeExecutionEvents();
}
