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

import * as mockResponses from './heatmapResponseMocks';

export type HeatmapRequest = {
  path: string;
  entityType: string | null;
  startDate: string | null;
};

/** Query params of each intercepted heatmap request (for tests). */
export const heatmapRequests: HeatmapRequest[] = [];

export function resetHeatmapRequestTracking(): void {
  heatmapRequests.length = 0;
}

const heatmapHandler = http.get('/api/v1/heatmap/readaccess', ({ request }) => {
  const params = new URL(request.url).searchParams;
  const path = params.get('path') ?? '/';
  heatmapRequests.push({ path, entityType: params.get('entityType'), startDate: params.get('startDate') });

  if (path === '/empty') {
    return HttpResponse.json(mockResponses.EmptyHeatmap);
  }
  if (path === '/vol1') {
    return HttpResponse.json(mockResponses.Vol1KeyHeatmap);
  }
  return HttpResponse.json(mockResponses.RootKeyHeatmap);
});

export const heatmapUnavailableHandler = http.get('/api/v1/heatmap/readaccess', () => {
  return new HttpResponse(null, { status: 404 });
});

export const heatmapServerErrorHandler = http.get('/api/v1/heatmap/readaccess', () => {
  return HttpResponse.json({ message: 'Internal error' }, { status: 500 });
});

export const heatmapServer = setupServer(heatmapHandler);
