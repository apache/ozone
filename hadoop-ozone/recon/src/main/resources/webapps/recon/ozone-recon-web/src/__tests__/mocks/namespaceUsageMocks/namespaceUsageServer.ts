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

import * as mockResponses from './namespaceUsageResponseMocks';

/** Paths from `path` query param on each intercepted usage request (for tests). */
export const usagePathsRequested: string[] = [];

export function resetNamespaceUsageRequestTracking(): void {
  usagePathsRequested.length = 0;
}

const usageHandler = http.get('/api/v1/namespace/usage', ({ request }) => {
  const url = new URL(request.url);
  const path = url.searchParams.get('path') ?? '/';
  usagePathsRequested.push(path);

  if (path === '/missing') {
    return HttpResponse.json(mockResponses.PathNotFoundUsage);
  }
  if (path === '/vol1/bucket1/key1') {
    return HttpResponse.json(mockResponses.Key1NamespaceUsage);
  }
  if (path === '/vol1/bucket1') {
    return HttpResponse.json(mockResponses.Bucket1NamespaceUsage);
  }
  if (path === '/vol1') {
    return HttpResponse.json(mockResponses.Vol1NamespaceUsage);
  }
  if (path === '/') {
    return HttpResponse.json(mockResponses.RootNamespaceUsage);
  }
  return HttpResponse.json(mockResponses.pathNotFoundUsage(path));
});

const summaryHandler = http.get('/api/v1/namespace/summary', ({ request }) => {
  const url = new URL(request.url);
  const path = url.searchParams.get('path') ?? '/';
  if (path === '/missing') {
    return HttpResponse.json(mockResponses.PathNotFoundSummaryResponse);
  }
  if (path === '/vol1/bucket1/key1') {
    return HttpResponse.json(mockResponses.Key1SummaryResponse);
  }
  if (path === '/vol1/bucket1') {
    return HttpResponse.json(mockResponses.Bucket1SummaryResponse);
  }
  if (path === '/vol1') {
    return HttpResponse.json(mockResponses.Vol1SummaryResponse);
  }
  if (path === '/') {
    return HttpResponse.json(mockResponses.RootSummaryResponse);
  }
  return HttpResponse.json(mockResponses.pathNotFoundSummary(path));
});

const quotaHandler = http.get('/api/v1/namespace/quota', ({ request }) => {
  const url = new URL(request.url);
  const path = url.searchParams.get('path') ?? '/';
  if (path === '/missing') {
    return HttpResponse.json(mockResponses.PathNotFoundQuotaResponse);
  }
  if (path === '/vol1/bucket1/key1') {
    return HttpResponse.json(mockResponses.Key1QuotaResponse);
  }
  if (path === '/vol1/bucket1') {
    return HttpResponse.json(mockResponses.Bucket1QuotaResponse);
  }
  if (path === '/vol1') {
    return HttpResponse.json(mockResponses.Vol1QuotaResponse);
  }
  if (path === '/') {
    return HttpResponse.json(mockResponses.RootQuotaResponse);
  }
  return HttpResponse.json(mockResponses.PathNotFoundQuotaResponse);
});

export const namespaceUsageServer = setupServer(
  usageHandler,
  summaryHandler,
  quotaHandler
);
