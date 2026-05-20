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
import { lazy } from 'react';

const Overview = lazy(() => import('@/v2/pages/overview/overview'));
const Volumes = lazy(() => import('@/v2/pages/volumes/volumes'))
const Buckets = lazy(() => import('@/v2/pages/buckets/buckets'));
const Datanodes = lazy(() => import('@/v2/pages/datanodes/datanodes'));
const Pipelines = lazy(() => import('@/v2/pages/pipelines/pipelines'));
const NamespaceUsage = lazy(() => import('@/v2/pages/namespaceUsage/namespaceUsage'));
const Containers = lazy(() => import('@/v2/pages/containers/containers'));
const Insights = lazy(() => import('@/v2/pages/insights/insights'));
const OMDBInsights = lazy(() => import('@/v2/pages/insights/omInsights'));
const Capacity = lazy(() => import('@/v2/pages/capacity/capacity'));
const Heatmap = lazy(() => import('@/v2/pages/heatmap/heatmap'));


export const routesV2 = [
  {
    path: '/Overview',
    component: Overview
  },
  {
    path: '/Volumes',
    component: Volumes
  },
  {
    path: '/Buckets',
    component: Buckets
  },
  {
    path: '/Datanodes',
    component: Datanodes
  },
  {
    path: '/Pipelines',
    component: Pipelines
  },
  {
    path: '/NamespaceUsage',
    component: NamespaceUsage
  },
  {
    path: '/Containers',
    component: Containers
  },
  {
    path: '/Insights',
    component: Insights
  },
  {
    path: '/Om',
    component: OMDBInsights
  },
  {
    path: '/Capacity',
    component: Capacity
  },
  {
    path: '/Heatmap',
    component: Heatmap
  }
];
