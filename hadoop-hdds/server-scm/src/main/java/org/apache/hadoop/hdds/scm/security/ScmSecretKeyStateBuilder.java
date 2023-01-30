package org.apache.hadoop.hdds.scm.security;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyStateImpl;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyStore;

import java.lang.reflect.Proxy;

/**
 * Builder for {@link SecretKeyState} with a proper proxy to make @Replicate
 * happen.
 */
public class ScmSecretKeyStateBuilder {
  private SecretKeyStore secretKeyStore;
  private SCMRatisServer scmRatisServer;

  public ScmSecretKeyStateBuilder setSecretKeyStore(
      SecretKeyStore secretKeyStore) {
    this.secretKeyStore = secretKeyStore;
    return this;
  }

  public ScmSecretKeyStateBuilder setRatisServer(
      final SCMRatisServer ratisServer) {
    scmRatisServer = ratisServer;
    return this;
  }

  public SecretKeyState build() {
    final SecretKeyState impl = new SecretKeyStateImpl(secretKeyStore);

    final SCMHAInvocationHandler scmhaInvocationHandler =
        new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.SECRET_KEY,
            impl, scmRatisServer);

    return (SecretKeyState) Proxy.newProxyInstance(
        SCMHAInvocationHandler.class.getClassLoader(),
        new Class<?>[]{SecretKeyState.class}, scmhaInvocationHandler);
  }
}
