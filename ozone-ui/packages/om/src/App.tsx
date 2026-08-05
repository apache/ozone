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

import { useEffect, useState } from 'react';
import { Routes, Route, useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import {
  AppLayout,
  Chip,
  NotFoundState,
  Sidebar,
  SyncConfigProvider,
  UtilityBar,
} from '@ozone-ui/shared';
import { navItems, SIDEBAR_WIDTH } from './navigation';
import { JMX_QUERY_KEY } from './api/useJmx';
import OverviewPage from './pages/Overview/OverviewPage';
import Placeholder from './pages/Placeholder';

/** 404 page for unknown routes; the action returns to the Overview. */
const NotFoundRoute = () => {
  const navigate = useNavigate();
  return <NotFoundState onAction={() => navigate('/')} />;
};

/** Product branding: the app name plus a chip showing the current host. */
const BrandTitle = () => {
  const host = window.location.hostname;
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
      Ozone Manager
      {host && (
        <Chip color="neutral" size="small">
          {host}
        </Chip>
      )}
    </span>
  );
};

/**
 * Inner app shell. Must be rendered inside `SyncConfigProvider` and
 * `QueryProvider` so `useSyncConfig` / `useQueryClient` are available.
 * Tracks the last-refreshed timestamp by subscribing to the query cache, so the
 * SyncChip always shows the correct time without any manual `setInterval`.
 */
function AppShell() {
  const queryClient = useQueryClient();
  const [lastRefreshedAt, setLastRefreshedAt] = useState<Date>(() => new Date());

  // Update the timestamp whenever any JMX query settles successfully — TanStack
  // fires this on both auto-refetch and manual invalidation.
  useEffect(() => {
    const cache = queryClient.getQueryCache();
    const unsubscribe = cache.subscribe((event) => {
      if (
        event.type === 'updated' &&
        event.action.type === 'success' &&
        Array.isArray(event.query.queryKey) &&
        event.query.queryKey[0] === JMX_QUERY_KEY
      ) {
        setLastRefreshedAt(new Date());
      }
    });
    return unsubscribe;
  }, [queryClient]);

  return (
    <AppLayout
      utilityBar={<UtilityBar branding={<BrandTitle />} lastRefreshedAt={lastRefreshedAt} />}
      sider={<Sidebar items={navItems} width={SIDEBAR_WIDTH} />}
    >
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/configuration" element={<Placeholder title="Configuration" />} />
        <Route path="/rpc" element={<Placeholder title="Remote Procedure Call" />} />
        <Route path="/ozone-manager" element={<Placeholder title="Ozone Manager" />} />
        <Route path="/jmx-info" element={<Placeholder title="JMX" />} />
        <Route path="/stacks" element={<Placeholder title="Stacks" />} />
        <Route path="/documentation" element={<Placeholder title="Documentation" />} />
        <Route path="/log-levels" element={<Placeholder title="Log levels" />} />
        <Route path="*" element={<NotFoundRoute />} />
      </Routes>
    </AppLayout>
  );
}

/**
 * Application root. Wraps the shell in `SyncConfigProvider` so the auto-refresh
 * toggle is available app-wide. `QueryProvider` and `ThemeProvider` are mounted
 * above this in `main.tsx`.
 */
function App() {
  return (
    <SyncConfigProvider>
      <AppShell />
    </SyncConfigProvider>
  );
}

export default App;
