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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.resource;

/**
 * Interface for a leakable resource. This interface should be implemented by the resource to monitor itself.
 *
 * Creating proxy objects, e.g. using lambda, like below does not work because we expect the resource object
 * itself is monitored, not the immediate proxy object.
 * <pre> {@code
 * LEAK_DETECTOR.watch(() -> { assertClose(resource) });
 * }</pre>
 *
 * @see LeakDetector
 */
public interface Leakable {
  /**
   * Perform assertions to verify proper resource closure. This is invoked after the resource object is GCed.
   */
  void check();
}
