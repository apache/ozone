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

import { setupServer } from 'msw/node';
import { http, HttpResponse } from 'msw';

import { ClusterState } from '@tests/mocks/overviewMocks/overviewResponseMocks';
import * as mockResponses from './containerResponseMocks';

const handlers = [
  http.get('/api/v1/clusterState', () => HttpResponse.json(ClusterState)),
  http.get('/api/v1/containers/unhealthy/MISSING', () =>
    HttpResponse.json(mockResponses.MissingContainersResponse)),
  http.get('/api/v1/containers/unhealthy/UNDER_REPLICATED', () =>
    HttpResponse.json(mockResponses.UnderReplicatedContainersResponse)),
  http.get('/api/v1/containers/unhealthy/OVER_REPLICATED', () =>
    HttpResponse.json(mockResponses.OverReplicatedContainersResponse)),
  http.get('/api/v1/containers/quasiClosed', () =>
    HttpResponse.json(mockResponses.QuasiClosedResponse)),
  http.get('/api/v1/containers/unhealthy/export', () =>
    HttpResponse.json(mockResponses.ExportJobsResponse)),
];

export const containersServer = setupServer(...handlers);
