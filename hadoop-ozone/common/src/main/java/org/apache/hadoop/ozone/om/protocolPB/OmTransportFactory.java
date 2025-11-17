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

package org.apache.hadoop.ozone.om.protocolPB;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS_DEFAULT;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory pattern to create object for RPC communication with OM.
 */
public interface OmTransportFactory {
  Logger LOG = LoggerFactory.getLogger(OmTransportFactory.class);

  OmTransport createOmTransport(ConfigurationSource source,
      UserGroupInformation ugi, String omServiceId) throws IOException;

  static OmTransport create(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    OmTransportFactory factory = createFactory(conf);

    return factory.createOmTransport(conf, ugi, omServiceId);
  }

  static OmTransportFactory createFactory(ConfigurationSource conf)
      throws IOException {
    try {
      // if a transport implementation is found via ServiceLoader, use it.
      ServiceLoader<OmTransportFactory> transportFactoryServiceLoader = ServiceLoader.load(OmTransportFactory.class);
      Iterator<OmTransportFactory> iterator = transportFactoryServiceLoader.iterator();
      if (iterator.hasNext()) {
        OmTransportFactory next = iterator.next();
        LOG.debug("Found OM transport implementation {} from service loader.", next.getClass().getName());
        return next;
      }

      // Otherwise, load the transport implementation specified by configuration.
      String transportClassName = conf.get(OZONE_OM_TRANSPORT_CLASS, OZONE_OM_TRANSPORT_CLASS_DEFAULT);
      LOG.debug("Loading OM transport implementation {} as specified by configuration.", transportClassName);
      return OmTransportFactory.class.getClassLoader()
          .loadClass(transportClassName)
          .asSubclass(OmTransportFactory.class)
          .newInstance();
    } catch (Exception ex) {
      throw new IOException("Can't create the default OmTransport implementation", ex);
    }
  }

}
