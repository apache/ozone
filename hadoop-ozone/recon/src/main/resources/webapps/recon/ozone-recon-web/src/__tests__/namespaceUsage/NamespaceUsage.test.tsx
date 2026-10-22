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

import React from 'react';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { http, HttpResponse } from 'msw';
import { vi } from 'vitest';

import NamespaceUsage from '@/v2/pages/namespaceUsage/namespaceUsage';
import * as commonUtils from '@/utils/common';
import {
  namespaceUsageServer,
  resetNamespaceUsageRequestTracking,
  usagePathsRequested
} from '@tests/mocks/namespaceUsageMocks/namespaceUsageServer';
import * as mockResponses from '@tests/mocks/namespaceUsageMocks/namespaceUsageResponseMocks';

vi.spyOn(commonUtils, 'showDataFetchError');
vi.spyOn(commonUtils, 'showInfoNotification');

vi.mock('@/v2/components/eChart/eChart', () => ({
  default: () => <div data-testid="nu-echart" />
}));

vi.mock('@/v2/hooks/useAPIData.hook', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/v2/hooks/useAPIData.hook')>();
  return {
    ...actual,
    useApiData: <T,>(url: string, defaultValue: T, options = {}) =>
      actual.useApiData(url, defaultValue, {
        ...options,
        retryAttempts: 0,
        retryDelay: 0
      })
  };
});

describe('Namespace Usage page (Disk Usage)', () => {
  beforeAll(() => namespaceUsageServer.listen());

  beforeEach(() => {
    resetNamespaceUsageRequestTracking();
    vi.mocked(commonUtils.showDataFetchError).mockClear();
    vi.mocked(commonUtils.showInfoNotification).mockClear();
  });

  afterEach(() => namespaceUsageServer.resetHandlers());
  afterAll(() => namespaceUsageServer.close());

  const rootUsageRequestCount = () =>
    usagePathsRequested.filter((path) => path === '/').length;

  // Give any request fired on render/click a chance to reach the mock server before asserting none was sent.
  const flushPendingRequests = () => new Promise((resolve) => setTimeout(resolve, 50));

  const confirmRootLoad = async () => {
    fireEvent.click(screen.getByRole('button', { name: 'Load usage' }));
    expect(screen.getByText('Confirm Root Path Usage Load')).toBeInTheDocument();
    const modal = document.querySelector('.ant-modal') as HTMLElement;
    await userEvent.click(within(modal).getByRole('button', { name: /^ok$/i }));
  };

  const openBreadcrumbMenu = async () => {
    const trigger = document.querySelector('.ant-breadcrumb-overlay-link') as HTMLElement;
    await userEvent.click(trigger);
  };

  const waitForMetadataEntityType = async (entityType: string) => {
    const metadataTable = screen.getByRole('table');
    await waitFor(() => {
      const entityTypeRow = within(metadataTable).getByText('Entity Type').closest('tr');
      expect(entityTypeRow).toHaveTextContent(entityType);
    });
    return metadataTable;
  };

  const metadataValueCell = (table: HTMLElement, propertyLabel: string) => {
    const row = within(table).getByText(propertyLabel).closest('tr');
    expect(row).not.toBeNull();
    return row!.querySelectorAll('td')[1];
  };

  const clickBreadcrumbSubmenuItem = async (label: string) => {
    await openBreadcrumbMenu();
    const menu = await screen.findByRole('menu');
    await userEvent.click(within(menu).getByRole('menuitem', { name: label }));
  };

  const searchPathFromBreadcrumb = async (pathSegment: string) => {
    await openBreadcrumbMenu();
    const menu = await screen.findByRole('menu');
    const searchInput = within(menu).getByPlaceholderText('Enter Path');
    await userEvent.type(searchInput, pathSegment);
    const searchButton = searchInput
      .closest('.ant-input-search')
      ?.querySelector('button.ant-input-search-button') as HTMLButtonElement;
    await userEvent.click(searchButton);
  };

  test('does not fetch root usage until the user confirms load', async () => {
    render(<NamespaceUsage />);

    expect(screen.getByText('Namespace Usage')).toBeInTheDocument();
    expect(screen.getByText(/Root path usage is not loaded by default/i)).toBeInTheDocument();

    await flushPendingRequests();
    expect(usagePathsRequested).toEqual([]);

    await confirmRootLoad();

    await waitFor(() => {
      expect(rootUsageRequestCount()).toBe(1);
    });
    expect(usagePathsRequested).toContain('/');
    await waitFor(() => {
      expect(screen.queryByText(/Root path usage is not loaded by default/i)).not.toBeInTheDocument();
    });

    const metadataTable = await waitForMetadataEntityType('ROOT');
    expect(metadataValueCell(metadataTable, 'Volumes')).toHaveTextContent(/^2$/);
  });

  test('navigates to a child path via breadcrumb submenu', async () => {
    render(<NamespaceUsage />);
    await confirmRootLoad();
    await waitForMetadataEntityType('ROOT');

    await clickBreadcrumbSubmenuItem('vol1');

    await waitFor(() =>
      expect(screen.queryByText(/Root path usage can be expensive/i)).not.toBeInTheDocument()
    );
    const metadataTable = await waitForMetadataEntityType('VOLUME');
    const nameRow = within(metadataTable).getByText('Name').closest('tr');
    expect(nameRow).toHaveTextContent('vol1');
    const volumeNameRow = within(metadataTable).getByText('Volume Name').closest('tr');
    expect(volumeNameRow).toHaveTextContent('vol1');
    expect(metadataValueCell(metadataTable, 'Buckets')).toHaveTextContent(/^1$/);
  });

  test('navigates to a key path via breadcrumb search', async () => {
    render(<NamespaceUsage />);
    await confirmRootLoad();
    await waitForMetadataEntityType('ROOT');

    await clickBreadcrumbSubmenuItem('vol1');
    await waitForMetadataEntityType('VOLUME');

    await clickBreadcrumbSubmenuItem('bucket1');
    await waitForMetadataEntityType('BUCKET');

    await searchPathFromBreadcrumb('key1');

    const keyMetadataTable = await waitForMetadataEntityType('KEY');
    const fileSizeRow = within(keyMetadataTable).getByText('File Size').closest('tr');
    expect(fileSizeRow).toHaveTextContent('2.93 KB');
  });

  test('prompts before reloading root from home and cancel does not refetch root usage', async () => {
    render(<NamespaceUsage />);
    await confirmRootLoad();
    await clickBreadcrumbSubmenuItem('vol1');
    await waitForMetadataEntityType('VOLUME');

    const usagePathsBeforeHome = [...usagePathsRequested];
    const rootUsageBeforeHome = rootUsageRequestCount();

    await userEvent.click(
      document.querySelector('#breadcrumb-container .anticon-home') as HTMLElement
    );
    expect(screen.getByText('Confirm Root Path Usage Load')).toBeInTheDocument();

    const modal = document.querySelector('.ant-modal') as HTMLElement;
    await userEvent.click(within(modal).getByRole('button', { name: /cancel/i }));

    await flushPendingRequests();
    expect(rootUsageRequestCount()).toBe(rootUsageBeforeHome);
    expect(usagePathsRequested).toEqual(usagePathsBeforeHome);
  });

  test('shows error and clears metadata when path is not found', async () => {
    render(<NamespaceUsage />);
    await confirmRootLoad();
    const metadataTable = await waitForMetadataEntityType('ROOT');
    expect(within(metadataTable).getByText('Volumes')).toBeInTheDocument();

    await searchPathFromBreadcrumb('missing');

    await waitFor(() =>
      expect(commonUtils.showDataFetchError).toHaveBeenCalledWith('Invalid Path: /missing')
    );
    await waitFor(() => {
      const table = screen.getByRole('table');
      expect(within(table).queryByText('Entity Type')).not.toBeInTheDocument();
      expect(within(table).queryByText('Volumes')).not.toBeInTheDocument();
    });
  });

  test('notifies when namespace summary is still initializing', async () => {
    namespaceUsageServer.use(
      http.get('/api/v1/namespace/usage', () =>
        HttpResponse.json(mockResponses.InitializingUsage)),
      http.get('/api/v1/namespace/summary', () =>
        HttpResponse.json(mockResponses.InitializingSummaryResponse)),
      http.get('/api/v1/namespace/quota', () =>
        HttpResponse.json(mockResponses.InitializingQuotaResponse))
    );

    render(<NamespaceUsage />);
    await confirmRootLoad();

    await waitFor(() =>
      expect(commonUtils.showInfoNotification).toHaveBeenCalledWith(
        'Information being initialized',
        expect.stringContaining('Namespace Summary is being initialized')
      )
    );
  });
});
