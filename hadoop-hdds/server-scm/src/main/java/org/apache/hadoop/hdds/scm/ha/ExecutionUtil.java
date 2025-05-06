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

package org.apache.hadoop.hdds.scm.ha;

import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Utility class is to rethrow the original exception after
 * executing the clean-up code.
 *
 * @param <E> Exception to throw on failure
 */
public final class ExecutionUtil<E extends Throwable> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ExecutionUtil.class);

  private final CheckedRunnable<E> fn;

  private CheckedRunnable<E> onException;

  private volatile boolean completed;

  private ExecutionUtil(final CheckedRunnable<E> fn) {
    this.fn = fn;
    this.completed = false;
  }

  public static <E extends Exception> ExecutionUtil<E> create(
      CheckedRunnable<E> tryBlock) {
    return new ExecutionUtil<>(tryBlock);
  }

  public ExecutionUtil<E> onException(CheckedRunnable<E>  catchBlock) {
    onException = catchBlock;
    return this;
  }

  public void execute() throws E {
    if (!completed) {
      completed = true;
      try {
        fn.run();
      } catch (Exception ex) {
        try {
          onException.run();
        } catch (Exception error) {
          LOG.warn("Got error while doing clean-up.", error);
        }
        throw ex;
      }
    }
  }
}
