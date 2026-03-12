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

package org.apache.hadoop.hdds;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ipc_.RemoteException;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

/** Testing {@link RemoteException#unwrapRemoteException()}. */
public class TestRemoteEx {
  private static final Reflections REFLECTIONS = new Reflections(TestRemoteEx.class.getPackage().getName());

  /** An exception without a single {@link String} parameter constructor. */
  static class SomeException extends SCMException {
    SomeException(ResultCodes result) {
      super(result);
    }
  }

  @Test
  public void testSCMException() {
    REFLECTIONS.getSubTypesOf(SCMException.class)
        .forEach(TestRemoteEx::runUnwrappingRemoteException);
  }

  static void runUnwrappingRemoteException(Class<? extends Exception> clazz) {
    final String message = clazz.getSimpleName() + "-message";
    final RemoteException remoteException = new RemoteException(clazz.getName(), message);
    System.out.println("Test      " + remoteException);
    final IOException unwrapped = remoteException.unwrapRemoteException();
    System.out.println("unwrapped " + unwrapped);
    final Class<?> expected = clazz == SomeException.class ? RemoteException.class : clazz;
    assertEquals(expected, unwrapped.getClass());
    assertEquals(message, unwrapped.getMessage());
  }
}
