/**
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
package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Factory pattern to create object for RPC communication with OM.
 */
public interface OmTransportFactory {

  OmTransport createOmTransport(ConfigurationSource source,
      UserGroupInformation ugi, String omServiceId) throws IOException;

  static OmTransport create(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    OmTransportFactory factory = createFactory();

    return factory.createOmTransport(conf, ugi, omServiceId);
  }

  static OmTransportFactory createFactory() throws IOException {
    ServiceLoader<OmTransportFactory> transportFactoryServiceLoader =
        ServiceLoader.load(OmTransportFactory.class);
    Iterator<OmTransportFactory> iterator =
        transportFactoryServiceLoader.iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    }
    try {
      return OmTransportFactory.class.getClassLoader()
          .loadClass(
              "org.apache.hadoop.ozone.om.protocolPB"
                  + ".Hadoop3OmTransportFactory")
          .asSubclass(OmTransportFactory.class)
          .newInstance();
    } catch (Exception ex) {
      throw new IOException(
          "Can't create the default OmTransport implementation", ex);
    }
  }

}
