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

import { Overview } from './views/overview/overview';
import { Datanodes } from './views/datanodes/datanodes';
import { Pipelines } from './views/pipelines/pipelines';
import { NotFound } from './views/notFound/notFound';
import { IRoute } from './types/routes.types';
import { MissingContainers } from './views/missingContainers/missingContainers';
import { Insights } from './views/insights/insights';
import { Om } from './views/insights/om/om';

import { DiskUsage } from './views/diskUsage/diskUsage';
import { Heatmap } from './views/heatMap/heatmap';
import { Volumes } from './views/volumes/volumes';
import { Buckets } from './views/buckets/buckets';

export const routes: IRoute[] = [
  {
    path: '/Overview',
    component: Overview
  },
  {
    path: '/Datanodes',
    component: Datanodes
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
    path: '/Pipelines',
    component: Pipelines
  },
  {
    path: '/Insights',
    component: Insights
  },
  {
    path: '/Om',
    component: Om
  },
  {
    path: '/MissingContainers',
    component: MissingContainers
  },
  {
    path: '/NamespaceUsage',
    component: DiskUsage
  },
  {
    path: '/Containers',
    component: MissingContainers,
  },
  {
    path: '/Heatmap',
    component: Heatmap
  },
  {
    path: '/:NotFound',
    component: NotFound
  }
];
