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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test OmKeyLocationInfoGroup.
 */
public class TestOmKeyLocationInfoGroup {

  @Test
  public void testCreatingAndGetLatestVersionLocations() {
    OmKeyLocationInfoGroup testInstance = createTestInstance();
    List<OmKeyLocationInfo> latestList =
        testInstance.getBlocksLatestVersionOnly();
    Assert.assertEquals(1, latestList.size());
    Assert.assertEquals(2, latestList.get(0).getCreateVersion());
  }

  @Test
  public void testGettingPreviousVersions() {
    OmKeyLocationInfoGroup testInstance = createTestInstance();
    List<OmKeyLocationInfo> list = testInstance.getLocationList(1L);
    Assert.assertEquals(2, list.size());
  }

  private OmKeyLocationInfoGroup createTestInstance() {
    OmKeyLocationInfo info1 = new OmKeyLocationInfo.Builder().build();
    info1.setCreateVersion(1);
    OmKeyLocationInfo info2 = new OmKeyLocationInfo.Builder().build();
    info2.setCreateVersion(1);
    OmKeyLocationInfo info3 = new OmKeyLocationInfo.Builder().build();
    info3.setCreateVersion(2);
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    locationInfoList.add(info1);
    locationInfoList.add(info2);
    locationInfoList.add(info3);
    return new OmKeyLocationInfoGroup(2, locationInfoList);
  }
}
