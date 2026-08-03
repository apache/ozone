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

import { QueryClient, type QueryClientConfig } from '@tanstack/react-query';

/**
 * Default query behaviour shared by every Ozone service UI. Tuned for
 * monitoring dashboards: data is considered fresh briefly, window-focus
 * refetches are off (they surprise operators watching a screen), and failed
 * requests retry once. Per-query auto-polling is opt-in via each query's
 * `refetchInterval`; this is the single place to later enable it globally.
 */
export const defaultQueryClientConfig: QueryClientConfig = {
  defaultOptions: {
    queries: {
      staleTime: 15_000,
      gcTime: 5 * 60_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
};

/** Create a `QueryClient` pre-configured with the Ozone UI defaults. */
export function createQueryClient(config: QueryClientConfig = defaultQueryClientConfig): QueryClient {
  return new QueryClient(config);
}

export default createQueryClient;
