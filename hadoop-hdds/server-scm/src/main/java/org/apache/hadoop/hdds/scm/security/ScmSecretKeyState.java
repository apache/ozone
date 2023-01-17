/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.security;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

public class ScmSecretKeyState implements SecretKeyState {
  private static final Logger LOG =
      LoggerFactory.getLogger(ScmSecretKeyState.class);

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private ManagedSecretKey currentKey;
  private final Map<UUID, ManagedSecretKey> allKeys = new HashMap<>();

  private final SecretKeyStore keyStore;


  private ScmSecretKeyState(SecretKeyStore keyStore) {
    this.keyStore = requireNonNull(keyStore);
  }

  @Override
  @Nonnull
  public ManagedSecretKey getCurrentKey() {
    lock.readLock().lock();
    try {
      return currentKey;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<ManagedSecretKey> getAllKeys() {
    lock.readLock().lock();
    try {
      return new HashSet<>(allKeys.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void updateKeys(ManagedSecretKey newCurrentKey,
                         List<ManagedSecretKey> newAllKeys) {
    LOG.info("Updating keys with currentKey={}, all keys={}", newCurrentKey,
        newAllKeys);
    lock.writeLock().lock();
    try {
      currentKey = newCurrentKey;
      allKeys.clear();
      for (ManagedSecretKey secretKey : newAllKeys) {
        allKeys.put(secretKey.getId(), secretKey);
      }
      keyStore.save(allKeys.values());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static class Builder {

    private SecretKeyStore secretKeyStore;
    private SCMRatisServer scmRatisServer;


    public Builder setSecretKeyStore(SecretKeyStore secretKeyStore) {
      this.secretKeyStore = secretKeyStore;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public SecretKeyState build() {
      final ScmSecretKeyState impl =
          new ScmSecretKeyState(secretKeyStore);

      final SCMHAInvocationHandler scmhaInvocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.SECRET_KEY,
              impl, scmRatisServer);

      return (SecretKeyState) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{SecretKeyState.class}, scmhaInvocationHandler);

    }
  }

}
