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

package org.apache.hadoop.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.assertj.core.api.Fail;
import org.junit.jupiter.api.Assertions;

/**
 * A general FaultInjector implementation.
 */
public class FaultInjectorImpl extends FaultInjector {
  private CountDownLatch ready;
  private CountDownLatch wait;
  private Throwable ex;
  private ContainerProtos.Type type = null;

  public FaultInjectorImpl() {
    init();
  }

  @Override
  public void init() {
    this.ready = new CountDownLatch(1);
    this.wait = new CountDownLatch(1);
  }

  @Override
  public void pause() throws IOException {
    ready.countDown();
    try {
      wait.await();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void resume() throws IOException {
    // Make sure injector pauses before resuming.
    try {
      ready.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assertions.assertTrue(Fail.fail("resume interrupted"));
    }
    wait.countDown();
  }

  @Override
  public void reset() throws IOException {
    init();
  }

  @Override
  @VisibleForTesting
  public void setException(Throwable e) {
    ex = e;
  }

  @Override
  @VisibleForTesting
  public Throwable getException() {
    return ex;
  }

  @Override
  @VisibleForTesting
  public void setType(ContainerProtos.Type type) {
    this.type = type;
  }

  @Override
  @VisibleForTesting
  public ContainerProtos.Type getType() {
    return type;
  }
}

