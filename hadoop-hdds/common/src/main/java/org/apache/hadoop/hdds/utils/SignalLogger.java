/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.slf4j.Logger;

/**
 * This class logs a message whenever we're about to exit on a UNIX signal.
 * This is helpful for determining the root cause of a process' exit.
 * For example, if the process exited because the system administrator 
 * ran a standard "kill," you would see 'EXITING ON SIGNAL SIGTERM' in the log.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum SignalLogger {

  INSTANCE;

  private static final String[] SIGNALS = new String[] {"TERM", "HUP", "INT"};

  private boolean registered = false;

  /**
   * Our signal handler.
   */
  private static class Handler implements sun.misc.SignalHandler {
    private final Logger log;
    private final sun.misc.SignalHandler prevHandler;

    Handler(String name, Logger log) {
      this.log = log;
      prevHandler = sun.misc.Signal.handle(new sun.misc.Signal(name), this);
    }

    /**
     * Handle an incoming signal.
     *
     * @param signal The incoming signal
     */
    @Override
    public void handle(sun.misc.Signal signal) {
      log.error("RECEIVED SIGNAL {}: SIG{}",
          signal.getNumber(), signal.getName());
      prevHandler.handle(signal);
    }
  }

  /**
   * Register some signal handlers.
   *
   * @param log The logger to use in the signal handlers.
   */
  public void register(final Logger log) {
    if (registered) {
      throw new IllegalStateException("Can't re-install the signal handlers.");
    }
    registered = true;
    StringBuilder bld = new StringBuilder();
    bld.append("registered UNIX signal handlers for [");
    String separator = "";
    for (String signalName : SIGNALS) {
      try {
        new Handler(signalName, log);
        bld.append(separator).append(signalName);
        separator = ", ";
      } catch (Exception e) {
        log.debug("", e);
      }
    }
    bld.append("]");
    if (log.isInfoEnabled()) {
      log.info(bld.toString());
    }
  }
}
