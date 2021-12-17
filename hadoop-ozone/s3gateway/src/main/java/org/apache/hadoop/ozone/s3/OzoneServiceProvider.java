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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * This class creates the OM service .
 */
@ApplicationScoped
public class OzoneServiceProvider {

  private String omServiceID;

  @Inject
  private OzoneConfiguration conf;

  @PostConstruct
  public void init() {
    Collection<String> serviceIdList =
        conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
    if (!serviceIdList.isEmpty()) {
      // HA cluster.
      //For now if multiple service id's are configured we throw exception.
      // As if multiple service id's are configured, S3Gateway will not be
      // knowing which one to talk to. In future, if OM federation is supported
      // we can resolve this by having another property like
      // ozone.om.internal.service.id.
      // TODO: Revisit this later.
      if (serviceIdList.size() > 1) {
        throw new IllegalArgumentException("Multiple serviceIds are " +
            "configured. " + Arrays.toString(serviceIdList.toArray()));
      } else {
        String serviceId = serviceIdList.iterator().next();
        Collection<String> omNodeIds = OmUtils.getActiveOMNodeIds(conf,
            serviceId);
        if (omNodeIds.size() == 0) {
          throw new IllegalArgumentException(OZONE_OM_NODES_KEY
              + "." + serviceId + " is not defined");
        }
        omServiceID = serviceId;
      }
    }
  }

  @Produces
  public String getOmServiceID() {
    return omServiceID;
  }

}
