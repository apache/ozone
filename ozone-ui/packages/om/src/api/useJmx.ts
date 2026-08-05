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

import { useQuery, useSuspenseQuery } from '@tanstack/react-query';
import { useRefetchInterval } from '@ozone-ui/shared';
import { queryJmx } from './jmx';

export interface JmxBeanState<T> {
  data?: T;
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
  /** The query succeeded but no MBean matched (`{ beans: [] }`). */
  isEmpty: boolean;
}

export interface SuspenseJmxBeanState<T> {
  /** The first matching MBean, or `undefined` when the query returned none. */
  data?: T;
  /** The query succeeded but no MBean matched (`{ beans: [] }`). */
  isEmpty: boolean;
}

export interface UseJmxBeanOptions {
  /**
   * Auto-refresh interval in milliseconds. Omit or pass `false` to disable
   * polling (the default). This is the hook-level hook for a future
   * auto-polling toggle.
   */
  refetchInterval?: number | false;
  /** Disable the query until a dependency is ready. Defaults to `true`. */
  enabled?: boolean;
}

/** The shared cache key for a JMX query, so callers can invalidate by prefix. */
export const JMX_QUERY_KEY = 'jmx';

/**
 * TanStack Query options for a JMX query. Shared by the plain and suspense hooks
 * (and usable with `useSuspenseQueries`) so every JMX read dedupes on the same
 * `['jmx', qry]` key.
 */
export function jmxQueryOptions<T>(qry: string) {
  return {
    queryKey: [JMX_QUERY_KEY, qry] as const,
    queryFn: () => queryJmx<T>(qry),
  };
}

/**
 * Fetch a single JMX MBean (the first bean) for a section via TanStack Query.
 * Requests are de-duplicated by query key, so multiple sections depending on the
 * same MBean share one network call. Refresh by invalidating the `['jmx']` key.
 */
export function useJmxBean<T>(qry: string, options: UseJmxBeanOptions = {}): JmxBeanState<T> {
  const { refetchInterval = false, enabled = true } = options;

  const query = useQuery({
    ...jmxQueryOptions<T>(qry),
    refetchInterval,
    enabled,
  });

  return {
    data: query.data?.[0],
    isLoading: query.isLoading,
    isError: query.isError,
    error: (query.error as Error | null) ?? null,
    isEmpty: query.isSuccess && (query.data?.length ?? 0) === 0,
  };
}

/**
 * Suspense variant: suspends while loading and throws to the nearest error
 * boundary on failure, so the caller renders assuming data is settled. Returns
 * the first matching MBean (or `undefined` when the endpoint returned no beans).
 *
 * Automatically picks up the auto-refresh interval from the nearest
 * `SyncConfigProvider` — no manual `setInterval` needed.
 */
export function useSuspenseJmxBean<T>(qry: string): SuspenseJmxBeanState<T> {
  const refetchInterval = useRefetchInterval();
  const { data } = useSuspenseQuery({ ...jmxQueryOptions<T>(qry), refetchInterval });
  return { data: data[0], isEmpty: data.length === 0 };
}
