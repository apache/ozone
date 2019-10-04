/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web.ozShell.volume;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.Objects;

import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

/**
 * Add acl handler for volume.
 */
@Command(name = "addacl",
    description = "Add a new Acl.")
public class AddAclVolumeHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_BUCKET_URI_DESCRIPTION)
  private String uri;

  @CommandLine.Option(names = {"--acl", "-a"},
      required = true,
      description = "Add acl." +
          "r = READ," +
          "w = WRITE," +
          "c = CREATE," +
          "d = DELETE," +
          "l = LIST," +
          "a = ALL," +
          "n = NONE," +
          "x = READ_AC," +
          "y = WRITE_AC" +
          "Ex user:user1:rw or group:hadoop:rw")
  private String acl;

  @CommandLine.Option(names = {"--store", "-s"},
      required = false,
      description = "store type. i.e OZONE or S3")
  private String storeType;

  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {
    Objects.requireNonNull(acl, "New acl to be added not specified.");
    OzoneAddress address = new OzoneAddress(uri);
    address.ensureVolumeAddress();
    OzoneClient client = address.createClient(createOzoneConfiguration());

    String volumeName = address.getVolumeName();

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
    }

    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(storeType == null ? OZONE :
            OzoneObj.StoreType.valueOf(storeType))
        .build();

    boolean result = client.getObjectStore().addAcl(obj,
        OzoneAcl.parseAcl(acl));

    System.out.printf("%s%n", "Acl added successfully: " + result);

    client.close();
    return null;
  }

}
