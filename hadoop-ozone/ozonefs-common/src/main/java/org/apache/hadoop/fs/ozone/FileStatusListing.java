/**
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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * This class defines a partial listing of a directory to support
 * iterative directory listing.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FileStatusListing {
  private FileStatus[] partialListing;

  /**
   *
   * @param partialListing a partial listing of a directory
   */
  public FileStatusListing(FileStatus[] partialListing) {
    if (partialListing == null) {
      throw new IllegalArgumentException("partial listing should not be null");
    }
    this.partialListing = partialListing;
  }

  /**
   *
   * Get the partial listing of file status
   * @return the partial listing of file status
   */
  public FileStatus[] getPartialListing() {
    return partialListing;
  }

  /**
   *
   * Get the last name in this list
   * @return the last name in the list if it is not empty; otherwise return null
   */
  public Path getLastName() {
    if (partialListing.length == 0) {
      return null;
    }
    return partialListing[partialListing.length - 1].getPath();
  }
}
