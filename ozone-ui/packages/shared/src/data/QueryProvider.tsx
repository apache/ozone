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

import React, { useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createQueryClient } from './queryClient';

export interface QueryProviderProps {
  children: React.ReactNode;
  /** Provide a pre-built client (e.g. in tests); otherwise one is created once. */
  client?: QueryClient;
}

/**
 * Provides a TanStack Query client to an application subtree. Every Ozone app
 * should mount this once near its root (alongside `ThemeProvider`) so data
 * hooks share one cache. The client is created lazily and kept stable across
 * re-renders.
 */
export const QueryProvider: React.FC<QueryProviderProps> = ({ children, client }) => {
  const [queryClient] = useState(() => client ?? createQueryClient());
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
};

export default QueryProvider;
