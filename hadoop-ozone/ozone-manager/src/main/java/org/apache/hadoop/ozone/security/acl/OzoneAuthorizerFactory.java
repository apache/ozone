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

package org.apache.hadoop.ozone.security.acl;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR_DEFAULT;
import static org.apache.hadoop.util.ReflectionUtils.newInstance;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link IAccessAuthorizer} instances based on configuration.
 */
public final class OzoneAuthorizerFactory {
  static final Logger LOG = LoggerFactory.getLogger(OzoneAuthorizerFactory.class);

  private OzoneAuthorizerFactory() {
    // no instances
  }

  /**
   * @return authorizer instance for {@link OzoneManager}
   */
  public static IAccessAuthorizer forOM(OzoneManager om) {
    return create(om, om.getKeyManager(), om.getPrefixManager(), "OM");
  }

  /**
   * @return authorizer instance for {@link OmSnapshot}, may be new instance,
   * or existing one, depending on configuration
   */
  public static IAccessAuthorizer forSnapshot(
      OzoneManager om, KeyManager keyManager, PrefixManager prefixManager
  ) {
    return om.getAccessAuthorizer().isNative()
        ? create(om, keyManager, prefixManager, "Snapshot")
        : om.getAccessAuthorizer();
  }

  /**
   * Creates new instance (except for {@link OzoneAccessAuthorizer},
   * which is a no-op authorizer.
   */
  private static IAccessAuthorizer create(OzoneManager om, KeyManager km, PrefixManager pm, String name) {
    final IAccessAuthorizer authorizer = createImpl(om, km, pm);
    LOG.info("{}: Authorizer for {} is {}", om.getOMNodeId(), name, authorizer.getClass());
    return authorizer;
  }

  private static IAccessAuthorizer createImpl(OzoneManager om, KeyManager km, PrefixManager pm) {
    if (!om.getAclsEnabled()) {
      return OzoneAccessAuthorizer.get();
    }

    final OzoneConfiguration conf = om.getConfiguration();
    final Class<? extends IAccessAuthorizer> clazz = authorizerClass(conf);

    if (OzoneAccessAuthorizer.class == clazz) {
      return OzoneAccessAuthorizer.get();
    }

    if (OzoneNativeAuthorizer.class == clazz) {
      return new OzoneNativeAuthorizer().configure(om, km, pm);
    }

    final IAccessAuthorizer authorizer = newInstance(clazz, conf);

    if (authorizer instanceof OzoneManagerAuthorizer) {
      return ((OzoneManagerAuthorizer) authorizer).configure(om, km, pm);
    }

    // If authorizer isn't native and shareable tmp dir is enabled,
    // then return the shared tmp hybrid authorizer.
    if (conf.getBoolean(OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR,
        OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR_DEFAULT)) {
      return new SharedTmpDirAuthorizer(
          new OzoneNativeAuthorizer().configure(om, km, pm),
          authorizer);
    }

    return authorizer;
  }

  private static Class<? extends IAccessAuthorizer> authorizerClass(
      ConfigurationSource conf) {
    return conf.getClass(OZONE_ACL_AUTHORIZER_CLASS,
        OzoneAccessAuthorizer.class,
        IAccessAuthorizer.class);
  }

}
