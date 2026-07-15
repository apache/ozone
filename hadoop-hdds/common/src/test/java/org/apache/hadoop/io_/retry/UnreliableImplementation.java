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

package org.apache.hadoop.io_.retry;

import java.io.IOException;
import javax.security.sasl.SaslException;
import org.apache.hadoop.security.AccessControlException;

/**
 * For the usage and purpose of this class see {@link UnreliableInterface}
 * which this class implements.
 *
 * @see UnreliableInterface
 */
class UnreliableImplementation implements UnreliableInterface {

  private int failsOnceInvocationCount;
  private int failsTenTimesInvocationCount;
  private int failsWithSASLExceptionTenTimesInvocationCount;
  private int failsWithAccessControlExceptionInvocationCount;

  @Override
  public void alwaysSucceeds() {
    // do nothing
  }
  
  @Override
  public void alwaysFailsWithFatalException() throws FatalException {
    throw new FatalException();
  }
  
  @Override
  public void failsOnceThenSucceeds() throws UnreliableException {
    if (failsOnceInvocationCount++ == 0) {
      throw new UnreliableException();
    }
  }

  @Override
  public void failsTenTimesThenSucceeds() throws UnreliableException {
    if (failsTenTimesInvocationCount++ < 10) {
      throw new UnreliableException();
    }
  }

  @Override
  public void failsWithSASLExceptionTenTimes() throws SaslException {
    if (failsWithSASLExceptionTenTimesInvocationCount++ < 10) {
      throw new SaslException();
    }
  }

  @Override
  public void failsWithAccessControlExceptionEightTimes()
      throws AccessControlException {
    if (failsWithAccessControlExceptionInvocationCount++ < 8) {
      throw new AccessControlException();
    }
  }

  @Override
  public void failsWithWrappedAccessControlException()
      throws IOException {
    AccessControlException ace = new AccessControlException();
    IOException ioe = new IOException(ace);
    throw new IOException(ioe);
  }
}
