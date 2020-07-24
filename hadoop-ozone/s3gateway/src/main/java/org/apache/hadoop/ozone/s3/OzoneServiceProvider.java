/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.s3.util.OzoneS3Util;
import org.apache.hadoop.security.SecurityUtil;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import java.io.IOException;
import java.util.Collection;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;

/**
 * This class creates the OM service .
 */
@ApplicationScoped
public class OzoneServiceProvider {

  private Text omServiceAddr;

  private String omserviceID;

  @Inject
  private OzoneConfiguration conf;

  @PostConstruct
  public void init() {
    try {
      omserviceID = OmUtils.getOzoneManagerServiceId(conf);
    } catch (IOException ex) {
      throw new ConfigurationException(ex.getMessage(), ex);
    }

    // Non-HA cluster.
    if (omserviceID == null) {
      omServiceAddr = SecurityUtil.buildTokenService(OmUtils.
          getOmAddressForClients(conf));
    } else {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(conf, omserviceID);
      if (omNodeIds.size() == 0) {
        throw new ConfigurationException(OZONE_OM_NODES_KEY
            + "." + omserviceID + " is not defined");
      }
      omServiceAddr = new Text(OzoneS3Util.buildServiceNameForToken(conf,
          omserviceID, omNodeIds));
    }
  }


  @Produces
  public Text getService() {
    return omServiceAddr;
  }

  @Produces
  public String getOmServiceID() {
    return omserviceID;
  }

}
