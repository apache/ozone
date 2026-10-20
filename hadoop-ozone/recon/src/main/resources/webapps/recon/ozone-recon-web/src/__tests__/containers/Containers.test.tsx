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
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { http, HttpResponse } from 'msw';
import { vi } from 'vitest';

import Containers from '@/v2/pages/containers/containers';
import * as apiData from '@/v2/hooks/useAPIData.hook';
import * as commonUtils from '@/utils/common';
import { ClusterState } from '@tests/mocks/overviewMocks/overviewResponseMocks';
import { containersServer } from '@tests/mocks/containerMocks/containersServer';
import * as containerMocks from '@tests/mocks/containerMocks/containerResponseMocks';
import { containerLocators, searchInputLocator } from '@tests/locators/locators';
import { waitForContainerRows } from '@tests/utils/containers.utils';

vi.spyOn(commonUtils, 'showDataFetchError');

vi.mock('@/components/autoReloadPanel/autoReloadPanel', () => ({
  default: () => <div data-testid="auto-reload-panel" />,
}));
vi.mock('@/v2/components/select/multiSelect.tsx', () => ({
  default: ({ onChange }: { onChange: Function }) => (
    <select data-testid="multi-select" onChange={(e) => onChange(e.target.value)}>
      <option value="containerID">Container ID</option>
      <option value="pipelineID">Pipeline ID</option>
    </select>
  ),
}));

vi.mock('@/v2/hooks/useAPIData.hook', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/v2/hooks/useAPIData.hook')>();
  return {
    ...actual,
    useApiData: <T,>(url: string, defaultValue: T, options = {}) => {
      // Containers fetches clusterState with initialFetch: true and again via useAutoReload on mount;
      // the second call aborts the first, which surfaces as unhandled CanceledError in tests.
      if (url === '/api/v1/clusterState') {
        return {
          data: ClusterState as T,
          loading: false,
          error: null,
          lastUpdated: Date.now(),
          success: true,
          execute: vi.fn(),
          refetch: vi.fn(() => Promise.resolve({ data: ClusterState })),
          clearError: vi.fn(),
          reset: vi.fn(),
        };
      }
      return actual.useApiData(url, defaultValue, {
        ...options,
        retryAttempts: 0,
        retryDelay: 0,
      });
    },
  };
});

function getEnabledSearchInput() {
  const inputs = screen.getAllByTestId(searchInputLocator);
  return inputs.find((el) => !(el as HTMLInputElement).disabled) ?? inputs[0];
}

function highlightValue(label: string) {
  return screen.getByText(label, { selector: '.highlight-content' })
    .querySelector('.highlight-content-value');
}

async function waitForLoadingToFinish(container: HTMLElement) {
  await waitFor(() => {
    expect(container.querySelector('.ant-spin-spinning')).not.toBeInTheDocument();
  });
}

describe('Containers Component', () => {
  beforeEach(() => {
    vi.mocked(commonUtils.showDataFetchError).mockClear();
  });
  beforeAll(() => containersServer.listen());
  afterEach(() => containersServer.resetHandlers());
  afterAll(() => containersServer.close());

  test('renders component correctly', async () => {
    render(<Containers />);

    expect(screen.getByText('Containers')).toBeInTheDocument();
    expect(screen.getByTestId('auto-reload-panel')).toBeInTheDocument();
    expect(screen.getByTestId('multi-select')).toBeInTheDocument();
    expect(screen.getByText('Highlights')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Missing' })).toBeInTheDocument();

    await waitForContainerRows();
    expect(getEnabledSearchInput()).toBeInTheDocument();
  });

  test('Loads data on mount', async () => {
    render(<Containers />);

    const rows = await waitForContainerRows();
    expect(rows[0]).toHaveTextContent('101');
    expect(screen.getByTestId(containerLocators.containerTableRow(102))).toBeInTheDocument();
  });

  test('loads missing containers on mount and updates highlights', async () => {
    render(<Containers />);

    await waitForContainerRows();

    await waitFor(() =>
      expect(highlightValue('Total Containers')).toHaveTextContent(new RegExp(`^${ClusterState.containers}$`)));
    expect(highlightValue('Missing')).toHaveTextContent(/^25$/);
    await waitFor(() => expect(highlightValue('Quasi Closed')).toHaveTextContent(/^2$/));
  });

  test('Renders table with correct number of rows', async () => {
    render(<Containers />);

    const rows = await waitForContainerRows();
    expect(rows).toHaveLength(3);
  });

  test('Displays no data message if the missing containers API returns an empty array', async () => {
    containersServer.use(
      http.get('/api/v1/containers/unhealthy/MISSING', () =>
        HttpResponse.json(containerMocks.EmptyUnhealthyResponse))
    );

    const { container } = render(<Containers />);

    await waitForLoadingToFinish(container);
    await waitFor(() => {
      expect(screen.queryAllByTestId(containerLocators.containerRowRegex)).toHaveLength(0);
      expect(screen.getByText('No Data')).toBeInTheDocument();
    });
  });

  test('loads the next page of missing containers when Next is clicked', async () => {
    containersServer.use(
      http.get('/api/v1/containers/unhealthy/MISSING', ({ request }) => {
        const url = new URL(request.url);
        const minContainerId = Number(url.searchParams.get('minContainerId') ?? 0);
        const limit = Number(url.searchParams.get('limit') ?? 11);
        return HttpResponse.json(
          containerMocks.paginatedMissingResponse(minContainerId, limit));
      })
    );

    render(<Containers />);

    await waitFor(() =>
      expect(screen.getAllByTestId(containerLocators.containerRowRegex)).toHaveLength(10));
    expect(screen.getByTestId(containerLocators.containerTableRow(101))).toBeInTheDocument();
    expect(screen.getByTestId(containerLocators.containerTableRow(110))).toBeInTheDocument();
    expect(screen.queryByTestId(containerLocators.containerTableRow(111))).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: /Next/i }));

    await waitFor(() =>
      expect(screen.getByTestId(containerLocators.containerTableRow(111))).toBeInTheDocument());
    expect(screen.queryByTestId(containerLocators.containerTableRow(101))).not.toBeInTheDocument();
  });

  test.each([
    { tab: 'Under-Replicated', endpoint: '/api/v1/containers/unhealthy/UNDER_REPLICATED', id: 201 },
    { tab: 'Over-Replicated', endpoint: '/api/v1/containers/unhealthy/OVER_REPLICATED', id: 301 },
    { tab: 'Quasi Closed', endpoint: '/api/v1/containers/quasiClosed', id: 401 },
  ])('lazy-loads $tab once on first activation', async ({ tab, endpoint, id }) => {
    const requests = vi.spyOn(apiData, 'fetchData');
    const tableRequests = () => requests.mock.calls.filter(
      ([url, , params]) => url === endpoint && params?.limit !== 0
    );

    try {
      render(<Containers />);
      await waitForContainerRows();
      expect(tableRequests()).toHaveLength(0);

      await userEvent.click(screen.getByRole('tab', { name: tab }));
      await waitFor(() =>
        expect(screen.getByTestId(containerLocators.containerTableRow(id))).toBeInTheDocument());
      expect(tableRequests()).toHaveLength(1);

      await userEvent.click(screen.getByRole('tab', { name: 'Missing' }));
      await userEvent.click(screen.getByRole('tab', { name: tab }));
      expect(tableRequests()).toHaveLength(1);
    } finally {
      requests.mockRestore();
    }
  });

  test('Handles search input change', async () => {
    render(<Containers />);
    await waitForContainerRows();

    fireEvent.change(getEnabledSearchInput(), { target: { value: '101' } });

    await waitFor(() =>
      expect(screen.getAllByTestId(containerLocators.containerRowRegex)).toHaveLength(1));
  });

  test('Displays a message when no results match the search term', async () => {
    render(<Containers />);
    await waitForContainerRows();

    fireEvent.change(getEnabledSearchInput(), { target: { value: '999999' } });
    await waitFor(() => expect(screen.getByText('No Data')).toBeInTheDocument());
  });

  test('Handles API errors gracefully by showing error message', async () => {
    containersServer.use(
      http.get('/api/v1/containers/unhealthy/MISSING', () =>
        HttpResponse.json({ error: 'Internal Server Error' }, { status: 500 }))
    );

    render(<Containers />);

    await waitFor(() =>
      expect(commonUtils.showDataFetchError).toHaveBeenCalledWith(
        expect.objectContaining({ message: 'Request failed with status code 500' })
      )
    );
  });

  test('Handles API errors when loading under-replicated tab', async () => {
    containersServer.use(
      http.get('/api/v1/containers/unhealthy/UNDER_REPLICATED', () =>
        HttpResponse.json({ error: 'Internal Server Error' }, { status: 500 }))
    );

    render(<Containers />);
    await waitForContainerRows();

    await userEvent.click(screen.getByRole('tab', { name: 'Under-Replicated' }));

    await waitFor(() =>
      expect(commonUtils.showDataFetchError).toHaveBeenCalledWith(
        expect.objectContaining({ message: 'Request failed with status code 500' })
      )
    );
  });
});
