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

package org.apache.hadoop.ozone.om.multitenant;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;

/**
 * AccountNameSpace interface.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public interface AccountNameSpace {
  /**
   * A Tenant will typically have his own AccountNameSpace to isolate the
   * the users of this Tenancy from that of the others.
   * An AccountNameSpace can have different attributes and
   * restrictions/additional-privileges that could apply to the users of this
   * AccountNameSpace.
   * Example of AccountNameSpace attribute can include
   *  - Space usage information across all the users of this account NameSpace
   *  - Quota restrictions across all the users of this namespace
   *  - Collective Bandwidth(Network usage information) usage across users of
   *  this NameSpace over a time window.
   *  - Any QOS restrictions.
   *
   *  Note that users can be explicitly given access to another Tenant's
   *  BucketNameSpace. It is the AccountNameSpace QOS restriction of
   *  user's Tenancy that will come into play when it comes to QOS enforcement.
   *  Similarly Quotas can be enforce both/either for AccountNameSpace and/or
   *  BucketNameSpace.
   *
   * All the users of this Tenant can be identified with an accessID
   * AccountNameSpaceID$Username.
   * @return AccountNameSpaceID
   */
  String getAccountNameSpaceID();

  /**
   * Get Space Usage Information for this AccountNameSpace. This can be
   * used for billing purpose. Such Aggregation can also be done lazily
   * by a Recon job. Implementations can decide.
   * @return SpaceUsage
   */
  SpaceUsageSource getSpaceUsage();

  /**
   * Sets quota for this AccountNameSpace. Quota enforcement can also be done
   * Lazily by a Recon job but that would be a soft quota enforcement. Choice
   * of quota enforcement style is left to Implementation.
   * @param quota
   */
  void setQuota(OzoneQuota quota);

  /**
   * Get Quota Information for this AccountNameSpace.
   * @return OzoneQuota
   */
  OzoneQuota getQuota();
}
