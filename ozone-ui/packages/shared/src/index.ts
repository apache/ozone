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

// Theme (design tokens + Ant Design theme + provider)
export * from './theme';

// Components
export { default as Sidebar } from './components/Sidebar/Sidebar';
export type { SidebarProps } from './components/Sidebar/Sidebar';
export { default as AppLayout } from './components/AppLayout/AppLayout';
export type { AppLayoutProps } from './components/AppLayout/AppLayout';
export { default as UtilityBar } from './components/UtilityBar/UtilityBar';
export type { UtilityBarProps } from './components/UtilityBar/UtilityBar';
export { default as PageHeader } from './components/PageHeader/PageHeader';
export type { PageHeaderProps } from './components/PageHeader/PageHeader';
export { default as Card } from './components/Card/Card';
export type { CardProps } from './components/Card/Card';
export { default as Section } from './components/Section/Section';
export type { SectionProps } from './components/Section/Section';
export { default as KeyValuePair } from './components/KeyValuePair/KeyValuePair';
export type { KeyValuePairProps } from './components/KeyValuePair/KeyValuePair';
export { default as SearchInput } from './components/SearchInput/SearchInput';
export type { SearchInputProps } from './components/SearchInput/SearchInput';
export { default as DataTable } from './components/DataTable/DataTable';
export type { DataTableProps } from './components/DataTable/DataTable';
export { default as TablePagination } from './components/DataTable/TablePagination';
export type { TablePaginationProps } from './components/DataTable/TablePagination';
export { default as Chip } from './components/Chip/Chip';
export type { ChipProps, ChipColor, ChipVariant, ChipSize } from './components/Chip/Chip';
export { default as Alert } from './components/Alert/Alert';
export type { AlertProps } from './components/Alert/Alert';
export {
  default as ErrorState,
  NetworkErrorState,
  NotFoundState,
  ServerErrorState,
} from './components/ErrorState/ErrorState';
export type { ErrorStateProps } from './components/ErrorState/ErrorState';
export { ErrorBoundary } from './components/ErrorBoundary/ErrorBoundary';
export type { ErrorBoundaryProps } from './components/ErrorBoundary/ErrorBoundary';
export { QueryErrorBoundary } from './components/ErrorBoundary/QueryErrorBoundary';
export type {
  QueryErrorBoundaryProps,
  QueryErrorFallbackProps,
} from './components/ErrorBoundary/QueryErrorBoundary';
export { default as TextLink } from './components/TextLink/TextLink';
export type { TextLinkProps } from './components/TextLink/TextLink';
export { default as IconButton } from './components/IconButton/IconButton';
export type { IconButtonProps } from './components/IconButton/IconButton';
export { default as Icon } from './components/Icon/Icon';
export type { IconProps, IconName } from './components/Icon/Icon';
export { default as SyncChip } from './components/SyncChip/SyncChip';
export type { SyncChipProps, DbSyncConfig } from './components/SyncChip/SyncChip';

// Data fetching (TanStack Query foundation)
export { fetchJson, HttpError, NetworkError } from './data/fetchJson';
export type { FetchJsonOptions, QueryParams } from './data/fetchJson';
export { createQueryClient, defaultQueryClientConfig } from './data/queryClient';
export { QueryProvider } from './data/QueryProvider';
export type { QueryProviderProps } from './data/QueryProvider';
export {
  SyncConfigProvider,
  useSyncConfig,
  DEFAULT_REFRESH_INTERVAL_MS,
} from './data/SyncConfigContext';
export type { SyncConfig, SyncConfigProviderProps } from './data/SyncConfigContext';
export { useRefetchInterval } from './data/useRefetchInterval';

// Utils
export * from './utils/menuUtils';
