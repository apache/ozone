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

package org.apache.hadoop.hdds.security.x509.certificate.authority.profile;

import java.util.Map;
import java.util.function.BiPredicate;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;

/**
 * CA Profile, this is needed when SCM does HA.
 * A placeholder class indicating what we need to do when we support issuing
 * CA certificates to other SCMs in HA mode.
 */
public class DefaultCAProfile extends DefaultProfile {
  private static final BiPredicate<Extension, PKIProfile>
      VALIDATE_BASIC_CONSTRAINTS = DefaultCAProfile::validateBasicExtensions;

  private static boolean validateBasicExtensions(Extension ext,
      PKIProfile pkiProfile) {
    BasicConstraints constraints =
        BasicConstraints.getInstance(ext.getParsedValue());
    return constraints.isCA() && pkiProfile.isCA();
  }

  @Override
  public boolean isCA() {
    return true;
  }

  @Override
  public Map<ASN1ObjectIdentifier,
      BiPredicate<Extension, PKIProfile>> getExtensionsMap() {
    // Add basic constraint.
    EXTENSIONS_MAP.putIfAbsent(Extension.basicConstraints,
        VALIDATE_BASIC_CONSTRAINTS);
    return EXTENSIONS_MAP;
  }

  @Override
  public KeyUsage getKeyUsage() {
    return new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment
        | KeyUsage.dataEncipherment | KeyUsage.keyAgreement | KeyUsage.cRLSign
        | KeyUsage.keyCertSign);
  }
}
