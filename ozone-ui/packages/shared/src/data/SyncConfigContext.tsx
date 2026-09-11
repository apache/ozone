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

import React, { createContext, useContext, useState } from 'react';

/** Default polling interval: 30 seconds. */
export const DEFAULT_REFRESH_INTERVAL_MS = 30_000;

export interface SyncConfig {
  /** Whether automatic polling is currently active. */
  enabled: boolean;
  /** Polling interval when enabled (ms). Defaults to 30 000. */
  refetchIntervalMs: number;
  /** Toggle auto-refresh on/off. */
  setEnabled: (enabled: boolean) => void;
}

const SyncConfigContext = createContext<SyncConfig>({
  enabled: true,
  refetchIntervalMs: DEFAULT_REFRESH_INTERVAL_MS,
  setEnabled: () => undefined,
});

export interface SyncConfigProviderProps {
  children: React.ReactNode;
  /** Polling interval in ms. Defaults to {@link DEFAULT_REFRESH_INTERVAL_MS}. */
  refetchIntervalMs?: number;
  /** Initial enabled state. Defaults to `true`. */
  defaultEnabled?: boolean;
}

/**
 * Provides the global auto-refresh toggle to all `SyncChip` instances and data
 * hooks in the subtree. Every Ozone service UI mounts this once near the root
 * (alongside `QueryProvider`). Data hooks read `useRefetchInterval()` to wire
 * TanStack Query's native `refetchInterval` — no manual `setInterval` is needed.
 */
export const SyncConfigProvider: React.FC<SyncConfigProviderProps> = ({
  children,
  refetchIntervalMs = DEFAULT_REFRESH_INTERVAL_MS,
  defaultEnabled = true,
}) => {
  const [enabled, setEnabled] = useState(defaultEnabled);

  return (
    <SyncConfigContext.Provider value={{ enabled, refetchIntervalMs, setEnabled }}>
      {children}
    </SyncConfigContext.Provider>
  );
};

/** Read the current sync configuration. Must be used inside `SyncConfigProvider`. */
export function useSyncConfig(): SyncConfig {
  return useContext(SyncConfigContext);
}
