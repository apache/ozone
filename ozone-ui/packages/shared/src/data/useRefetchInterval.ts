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

import { useSyncConfig } from './SyncConfigContext';

/**
 * Returns the `refetchInterval` value to pass to TanStack Query: the configured
 * interval in milliseconds when auto-refresh is enabled, or `false` to disable
 * polling. Reads from the nearest `SyncConfigProvider`.
 *
 * @example
 * ```ts
 * const refetchInterval = useRefetchInterval();
 * const { data } = useSuspenseQuery({ ...queryOptions, refetchInterval });
 * ```
 */
export function useRefetchInterval(): number | false {
  const { enabled, refetchIntervalMs } = useSyncConfig();
  return enabled ? refetchIntervalMs : false;
}
