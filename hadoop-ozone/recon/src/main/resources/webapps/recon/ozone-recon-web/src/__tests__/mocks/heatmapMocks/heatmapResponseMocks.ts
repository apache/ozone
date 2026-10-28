/*
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

/** Root key heatmap (entityType=key, path=/). */
export const RootKeyHeatmap = {
  label: 'root',
  path: '/',
  size: 512,
  minAccessCount: 10,
  maxAccessCount: 100,
  children: [
    {
      label: 'vol1',
      path: '/vol1',
      size: 512,
      minAccessCount: 10,
      maxAccessCount: 100,
      children: [
        {
          label: 'bucket1',
          path: '/vol1/bucket1',
          size: 256,
          minAccessCount: 10,
          maxAccessCount: 100,
          children: [
            {
              label: 'demo-key',
              size: 256,
              accessCount: 84,
              color: 1
            }
          ]
        }
      ]
    }
  ]
};

/** Key heatmap scoped to /vol1 (used when the user searches that path). */
export const Vol1KeyHeatmap = {
  label: 'vol1',
  path: '/vol1',
  size: 512,
  minAccessCount: 20,
  maxAccessCount: 200,
  children: [
    {
      label: 'bucket1',
      path: '/vol1/bucket1',
      size: 256,
      minAccessCount: 20,
      maxAccessCount: 200,
      children: [
        {
          label: 'scoped-key',
          size: 128,
          accessCount: 42,
          color: 0.75
        }
      ]
    }
  ]
};

/** Response that drives the "No Data available" warning state. */
export const EmptyHeatmap = {
  label: null,
  path: null,
  size: 0,
  minAccessCount: 0,
  maxAccessCount: 0,
  children: []
};
