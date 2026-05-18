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

package org.apache.hadoop.hdds.scm.ha.invoker;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link CertificateStore}.  Do not modify. */
public class CertificateStoreInvoker extends ScmInvoker<CertificateStore> {
  enum ReplicateMethod implements NameAndParameterTypes {
    removeAllExpiredCertificates(new Class<?>[][] {
        new Class<?>[] {}
    }),
    storeValidCertificate(new Class<?>[][] {
        null,
        null,
        null,
        new Class<?>[] {BigInteger.class, X509Certificate.class, NodeType.class}
    });

    private final Class<?>[][] parameterTypes;

    ReplicateMethod(Class<?>[][] parameterTypes) {
      this.parameterTypes = parameterTypes;
    }

    @Override
    public Class<?>[] getParameterTypes(int numArgs) {
      return parameterTypes[numArgs];
    }
  }

  public CertificateStoreInvoker(CertificateStore impl, SCMRatisServer ratis) {
    super(impl, CertificateStoreInvoker::newProxy, ratis);
  }

  @Override
  public Class<CertificateStore> getApi() {
    return CertificateStore.class;
  }

  static CertificateStore newProxy(ScmInvoker<CertificateStore> invoker) {
    return new CertificateStore() {

      @Override
      public void checkValidCertID(BigInteger arg0) throws IOException {
        invoker.getImpl().checkValidCertID(arg0);
      }

      @Override
      public X509Certificate getCertificateByID(BigInteger arg0) throws IOException {
        return invoker.getImpl().getCertificateByID(arg0);
      }

      @Override
      public List<X509Certificate> listCertificate(NodeType arg0, BigInteger arg1, int arg2) throws IOException {
        return invoker.getImpl().listCertificate(arg0, arg1, arg2);
      }

      @Override
      public void reinitialize(SCMMetadataStore arg0) {
        invoker.getImpl().reinitialize(arg0);
      }

      @Override
      public List<X509Certificate> removeAllExpiredCertificates() throws IOException {
        final Object[] args = {};
        return (List<X509Certificate>) invoker.invokeReplicateDirect(ReplicateMethod.removeAllExpiredCertificates,
            args);
      }

      @Override
      public void storeValidCertificate(BigInteger arg0, X509Certificate arg1, NodeType arg2) throws IOException {
        final Object[] args = {arg0, arg1, arg2};
        invoker.invokeReplicateClient(ReplicateMethod.storeValidCertificate, args);
      }

      @Override
      public void storeValidScmCertificate(BigInteger arg0, X509Certificate arg1) throws IOException {
        invoker.getImpl().storeValidScmCertificate(arg0, arg1);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "checkValidCertID":
      final BigInteger arg0 = p.length > 0 ? (BigInteger) p[0] : null;
      getImpl().checkValidCertID(arg0);
      return Message.EMPTY;

    case "getCertificateByID":
      final BigInteger arg1 = p.length > 0 ? (BigInteger) p[0] : null;
      returnType = X509Certificate.class;
      returnValue = getImpl().getCertificateByID(arg1);
      break;

    case "listCertificate":
      final NodeType arg2 = p.length > 0 ? (NodeType) p[0] : null;
      final BigInteger arg3 = p.length > 1 ? (BigInteger) p[1] : null;
      final int arg4 = p.length > 2 ? (int) p[2] : 0;
      returnType = List.class;
      returnValue = getImpl().listCertificate(arg2, arg3, arg4);
      break;

    case "reinitialize":
      final SCMMetadataStore arg5 = p.length > 0 ? (SCMMetadataStore) p[0] : null;
      getImpl().reinitialize(arg5);
      return Message.EMPTY;

    case "removeAllExpiredCertificates":
      returnType = List.class;
      returnValue = getImpl().removeAllExpiredCertificates();
      break;

    case "storeValidCertificate":
      final BigInteger arg6 = p.length > 0 ? (BigInteger) p[0] : null;
      final X509Certificate arg7 = p.length > 1 ? (X509Certificate) p[1] : null;
      final NodeType arg8 = p.length > 2 ? (NodeType) p[2] : null;
      getImpl().storeValidCertificate(arg6, arg7, arg8);
      return Message.EMPTY;

    case "storeValidScmCertificate":
      final BigInteger arg9 = p.length > 0 ? (BigInteger) p[0] : null;
      final X509Certificate arg10 = p.length > 1 ? (X509Certificate) p[1] : null;
      getImpl().storeValidScmCertificate(arg9, arg10);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in CertificateStore");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
