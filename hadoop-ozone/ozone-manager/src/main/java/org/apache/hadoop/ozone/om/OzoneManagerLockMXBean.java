/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * This is the JMX management interface for OzoneManagerLock information.
 */
@InterfaceAudience.Private
public interface OzoneManagerLockMXBean {

  /**
   * Returns readLockWaitingTimeMsStat. Provides information on the total number
   * of samples, minimum value, maximum value, arithmetic mean, standard
   * deviation of all the samples added.
   *
   * @return String representation of readLockWaitingTimeMsStat
   */
  String getReadLockWaitingTimeMsStat();

  /**
   * Returns readLockHeldTimeMsStat. Provides information on the total number of
   * samples, minimum value, maximum value, arithmetic mean, standard deviation
   * of all the samples added.
   *
   * @return String representation of readLockHeldTimeMsStat
   */
  String getReadLockHeldTimeMsStat();

  /**
   * Returns writeLockWaitingTimeMsStat. Provides information on the total
   * number of samples, minimum value, maximum value, arithmetic mean, standard
   * deviation of all the samples added.
   *
   * @return String representation of writeLockWaitingTimeMsStat
   */
  String getWriteLockWaitingTimeMsStat();

  /**
   * Returns writeLockHeldTimeMsStat. Provides information on the total number
   * of samples, minimum value, maximum value, arithmetic mean, standard
   * deviation of all the samples added.
   *
   * @return String representation of writeLockHeldTimeMsStat
   */
  String getWriteLockHeldTimeMsStat();

}
