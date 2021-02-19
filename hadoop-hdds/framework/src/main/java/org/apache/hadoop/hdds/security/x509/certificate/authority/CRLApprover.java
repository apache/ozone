/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.operator.OperatorCreationException;

import java.security.cert.CRLException;
import java.security.cert.X509CRL;

/**
 * CRL Approver interface is used to sign CRLs.
 */
public interface CRLApprover {

  /**
   * Signs a CRL.
   * @param builder - CRL builder instance with CRL info to be signed.
   * @return Signed CRL.
   * @throws CRLException - On Error
   * @throws OperatorCreationException - on Error.
   */
  X509CRL sign(X509v2CRLBuilder builder)
      throws CRLException, OperatorCreationException;

}
