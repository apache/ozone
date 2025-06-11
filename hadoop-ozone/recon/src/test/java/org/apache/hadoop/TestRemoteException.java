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

package org.apache.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

/** Testing {@link RemoteException#unwrapRemoteException()}. */
public class TestRemoteException {
  private static final Reflections REFLECTIONS = new Reflections(TestRemoteException.class.getPackage().getName());

  /** An exception without a constructor with a single {@link String} parameter. */
  static class SomeException extends SCMException {
    SomeException(ResultCodes result) {
      super(result);
    }
  }

  @Test
  public void testSCMException() {
    REFLECTIONS.getSubTypesOf(SCMException.class)
        .forEach(TestRemoteException::runUnwrappingRemoteException);
  }

  static void runUnwrappingRemoteException(Class<? extends Exception> clazz) {
    final RemoteException remoteException = new RemoteException(clazz.getName(), "message");
    System.out.println("Test " + remoteException);
    final IOException unwrapped = remoteException.unwrapRemoteException();
    final Class<?> expected = clazz == SomeException.class ? RemoteException.class : clazz;
    assertEquals(expected, unwrapped.getClass());
  }
}
