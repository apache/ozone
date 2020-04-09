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

package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * Ozone user interface commands.
 *
 * This class uses dispatch method to make calls
 * to appropriate handlers that execute the ozone functions.
 */
public abstract class Shell extends GenericCli {

  public static final String OZONE_URI_DESCRIPTION = "Ozone URI could start "
      + "with o3:// or without prefix. URI may contain the host/serviceId "
      + " and port of the OM server. Both are optional. "
      + "If they are not specified it will be identified from "
      + "the config files.";


  @Override
  protected void printError(Throwable errorArg) {
    if (errorArg instanceof OMException) {
      if (isVerbose()) {
        errorArg.printStackTrace(System.err);
      } else {
        OMException omException = (OMException) errorArg;
        System.err.println(String
            .format("%s %s", omException.getResult().name(),
                omException.getMessage()));
      }
    } else {
      super.printError(errorArg);
    }
  }
}

