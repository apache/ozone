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

import java.net.URI;

import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Executes volume Info calls.
 */
@Command(name = "-infoVolume",
    description = "returns information about a specific volume")
public class InfoVolumeHandler extends Handler{

  @Parameters(arity = "1..1", description = Shell.OZONE_VOLUME_URI_DESCRIPTION)
  private String uri;

  /**
   * Executes volume Info.
   */
  @Override
  public Void call() throws Exception {

    URI ozoneURI = verifyURI(uri);
    if (ozoneURI.getPath().isEmpty()) {
      throw new OzoneClientException(
          "Volume name is required to get info of a volume");
    }

    // we need to skip the slash in the URI path
    String volumeName = ozoneURI.getPath().substring(1);

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    System.out.printf("%s%n", JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(OzoneClientUtils.asVolumeInfo(vol))));
    return null;
  }

}
