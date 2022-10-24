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

package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.ozone.container.common.interfaces.Container;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * The collector of orphan files(garbage) in the container chunk directory.
 * It should be executed after container full scan, then the garbage would be
 * deleted here.
 */
public abstract class AbstractContainerGarbageCollector {

  /**
   * Collect the garbage files with the container info.
   *
   * @param container container where garbage is found
   * @param files     garbage files to be deleted
   * @throws IOException
   */
  public abstract void collectGarbage(Container container, Set<File> files)
      throws IOException;

  /**
   * Check container state before delete.
   *
   * @param container container where garbage is found
   * @return to delete or not
   */
  public abstract boolean isDeletionAllowed(Container container);

}
