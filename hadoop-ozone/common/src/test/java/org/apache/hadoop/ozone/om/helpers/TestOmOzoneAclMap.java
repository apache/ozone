/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.OzoneAcl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test {@link OmOzoneAclMap}.
 */
public class TestOmOzoneAclMap {

  @Test
  public void testAddAcl() throws Exception {
    OmOzoneAclMap map = new OmOzoneAclMap();
    map.addAcl(OzoneAcl.parseAcl("user:masstter:rx[DEFAULT]"));
    map.addAcl(OzoneAcl.parseAcl("user:masstter:rw[DEFAULT]"));

    //[user:masstter:rwx[DEFAULT]]
    Assert.assertEquals(1, map.getAcl().size());
    Assert.assertEquals(1, map.getDefaultAclList().size());

    map = new OmOzoneAclMap();
    map.addAcl(OzoneAcl.parseAcl("user:masstter:rx"));
    map.addAcl(OzoneAcl.parseAcl("user:masstter:rw[ACCESS]"));

    //[user:masstter:rwx[ACCESS]]
    Assert.assertEquals(1, map.getAcl().size());
    Assert.assertEquals(0, map.getDefaultAclList().size());

    map = new OmOzoneAclMap();
    map.addAcl(OzoneAcl.parseAcl("user:masstter:rwx[DEFAULT]"));
    map.addAcl(OzoneAcl.parseAcl("user:masstter:rwx[ACCESS]"));

    //[user:masstter:rwx[ACCESS], user:masstter:rwx[DEFAULT]]
    Assert.assertEquals(2, map.getAcl().size());
    Assert.assertEquals(1, map.getDefaultAclList().size());

  }
}
