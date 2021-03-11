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

import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CRLCodec;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.X509CRL;

/**
 * Default CRL Approver used by the DefaultCA.
 */
public class DefaultCRLApprover implements CRLApprover {

  private SecurityConfig config;
  private PrivateKey caPrivate;

  public DefaultCRLApprover(SecurityConfig config, PrivateKey caPrivate) {
    this.config = config;
    this.caPrivate = caPrivate;
  }

  @Override
  public X509CRL sign(X509v2CRLBuilder builder)
      throws CRLException, OperatorCreationException {
    JcaContentSignerBuilder contentSignerBuilder =
        new JcaContentSignerBuilder(config.getSignatureAlgo());

    contentSignerBuilder.setProvider(config.getProvider());
    X509CRLHolder crlHolder =
        builder.build(contentSignerBuilder.build(caPrivate));

    return CRLCodec.getX509CRL(crlHolder);
  }
}
