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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.LinkedList;

/**
 * This class defines a partial listing of a directory to support.
 * iterative directory listing.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FileStatusListing {
  private Collection<FileStatus> partialListing = new LinkedList<>();;

  /**
   *
   * @param partialListing a partial listing of a directory.
   */
  public FileStatusListing(LinkedList<FileStatus> partialListing) {
    this.partialListing.addAll(partialListing);
  }

  /**
   * Get the partial listing of file status.
   *
   * @return the partial listing of file status
   */
  public Collection<FileStatus> getPartialListing() {
    return partialListing;
  }

  /**
   * Get the last name in this list.
   *
   * @return the last name in the list if it is not empty otherwise return null.
   */
  public Path getLastName() {
    if (partialListing.size() == 0) {
      return null;
    }
    return partialListing.stream().skip(partialListing.size() - 1)
            .reduce((first, second) -> second)
            .orElse(null).getPath();
  }
}
