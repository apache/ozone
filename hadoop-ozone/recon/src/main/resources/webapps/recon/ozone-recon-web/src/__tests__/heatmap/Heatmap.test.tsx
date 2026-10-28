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
import { vi } from 'vitest';

import Heatmap from '@/v2/pages/heatmap/heatmap';
import * as commonUtils from '@/utils/common';
import {
  heatmapRequests,
  heatmapServer,
  heatmapServerErrorHandler,
  heatmapUnavailableHandler,
  resetHeatmapRequestTracking
} from '@tests/mocks/heatmapMocks/heatmapServer';

vi.spyOn(commonUtils, 'showDataFetchError');

const { collectDrillDownTiles } = vi.hoisted(() => {
  type MockHeatmapNode = {
    label?: string | null;
    path?: string;
    color?: number;
    children?: MockHeatmapNode[];
  };

  /** Non-leaf treemap tiles (no color, has path) call onClick — same rule as HeatmapPlot nodeClick. */
  function collectDrillDownTiles(node: MockHeatmapNode, onClick: (path: string) => void): React.ReactNode[] {
    const tiles: React.ReactNode[] = [];
    node.children?.forEach((child) => {
      if (child.path && child.color === undefined) {
        tiles.push(
          <div
            key={child.path}
            role="button"
            tabIndex={0}
            data-testid={`heatmap-tile-${child.label}`}
            onClick={() => onClick(child.path!)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                onClick(child.path!);
              }
            }}
          >
            {child.label}
          </div>
        );
      }
      tiles.push(...collectDrillDownTiles(child, onClick));
    });
    return tiles;
  }

  return { collectDrillDownTiles };
});

vi.mock('@/v2/components/plots/heatmapPlot', () => ({
  default: ({ data, onClick, entityType }: {
    data: { label?: string | null; path?: string; color?: number; children?: unknown[] };
    onClick: (path: string) => void;
    entityType: string;
  }) => (
    <div data-testid="heatmap-plot" data-label={data.label ?? ''} data-entity-type={entityType}>
      {collectDrillDownTiles(data, onClick)}
    </div>
  ),
}));

vi.mock('@/v2/hooks/useAPIData.hook', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/v2/hooks/useAPIData.hook')>();
  return {
    ...actual,
    useApiData: <T,>(url: string, defaultValue: T, options = {}) => {
      // The hook's own initial GET does not handle a rejected request, so a 404/500 on mount surfaces as an
      // unhandled rejection in tests. Skip it and issue the initial request with a handled promise instead.
      const api = actual.useApiData(url, defaultValue, {
        ...options,
        initialFetch: false,
        retryAttempts: 0,
        retryDelay: 0,
      });
      React.useEffect(() => {
        if (url) {
          api.execute().catch(() => undefined);
        }
      }, []); // eslint-disable-line react-hooks/exhaustive-deps
      return api;
    },
  };
});

const waitForPlot = (label: string) => waitFor(() => {
  expect(screen.getByTestId('heatmap-plot')).toHaveAttribute('data-label', label);
});

const searchPath = async (path: string) => {
  const pathInput = screen.getByPlaceholderText('/');
  await userEvent.clear(pathInput);
  if (path) {
    await userEvent.type(pathInput, path);
  }
  fireEvent.click(screen.getByRole('button', { name: /search/i }));
};

// antd Dropdown opens its overlay on hover after a short delay.
const selectFromDropdown = async (trigger: RegExp, item: string) => {
  fireEvent.mouseOver(screen.getByRole('button', { name: trigger }));
  fireEvent.click(await screen.findByText(item));
};

// Give any request fired on click a chance to reach the mock server before asserting none was sent.
const flushPendingRequests = () => new Promise((resolve) => setTimeout(resolve, 50));

describe('Heatmap page', () => {
  beforeAll(() => heatmapServer.listen());

  beforeEach(() => {
    resetHeatmapRequestTracking();
    vi.mocked(commonUtils.showDataFetchError).mockClear();
  });

  afterEach(() => heatmapServer.resetHandlers());
  afterAll(() => heatmapServer.close());

  test('renders header, filters, legend, and heatmap plot with data', async () => {
    render(<Heatmap />);

    expect(screen.getByText('Heatmap')).toBeInTheDocument();
    expect(screen.getByText('Path')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('/')).toHaveValue('/');
    expect(screen.getByText(/Entity Type:\s*key/)).toBeInTheDocument();
    expect(screen.getByText(/Last\s+24H/)).toBeInTheDocument();
    expect(screen.getByText('Less Accessed')).toBeInTheDocument();
    expect(screen.getByText('Moderate Accessed')).toBeInTheDocument();
    expect(screen.getByText('Most Accessed')).toBeInTheDocument();

    await waitForPlot('root');
    expect(screen.getByTestId('heatmap-plot')).toHaveAttribute('data-entity-type', 'key');
    expect(heatmapRequests).toEqual([{ path: '/', entityType: 'key', startDate: '24H' }]);
  });

  test('shows validation error for invalid path characters', async () => {
    render(<Heatmap />);
    await waitForPlot('root');

    const pathInput = screen.getByPlaceholderText('/');
    await userEvent.clear(pathInput);
    await userEvent.type(pathInput, 'bad@path');

    expect(screen.getByText('Please enter valid path')).toBeInTheDocument();
  });

  test('fetches heatmap for a new path when the user searches', async () => {
    render(<Heatmap />);
    await waitForPlot('root');

    await searchPath('/vol1');

    await waitForPlot('vol1');
    expect(heatmapRequests.map((request) => request.path)).toEqual(['/', '/vol1']);
  });

  test('does not fetch when searching with an empty path', async () => {
    render(<Heatmap />);
    await waitForPlot('root');

    await searchPath('');
    await flushPendingRequests();

    expect(heatmapRequests).toHaveLength(1);
    expect(screen.getByTestId('heatmap-plot')).toHaveAttribute('data-label', 'root');
  });

  test('drills down into the clicked entity and updates the path input', async () => {
    render(<Heatmap />);
    await waitForPlot('root');

    fireEvent.click(screen.getByTestId('heatmap-tile-vol1'));

    await waitForPlot('vol1');
    expect(screen.getByPlaceholderText('/')).toHaveValue('/vol1');
    expect(heatmapRequests.map((request) => request.path)).toContain('/vol1');
  });

  test('refetches heatmap when the entity type is changed', async () => {
    render(<Heatmap />);
    await waitForPlot('root');

    await selectFromDropdown(/Entity Type/, 'Bucket');

    await waitFor(() => {
      expect(heatmapRequests).toContainEqual({ path: '/', entityType: 'bucket', startDate: '24H' });
    });
    expect(screen.getByText(/Entity Type:\s*bucket/)).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByTestId('heatmap-plot')).toHaveAttribute('data-entity-type', 'bucket');
    });
  });

  test('refetches heatmap when the time period is changed', async () => {
    render(<Heatmap />);
    await waitForPlot('root');

    await selectFromDropdown(/^Last/, '7 Days');

    await waitFor(() => {
      expect(heatmapRequests).toContainEqual({ path: '/', entityType: 'key', startDate: '7D' });
    });
    expect(screen.getByText(/Last\s+7D/)).toBeInTheDocument();
  });

  test('shows no data warning when the API returns empty labels', async () => {
    render(<Heatmap />);
    await waitForPlot('root');

    await searchPath('/empty');

    await waitFor(() => {
      expect(screen.getByText('No Data available')).toBeInTheDocument();
    });
    expect(screen.queryByTestId('heatmap-plot')).not.toBeInTheDocument();
  });

  test('shows disabled state when heatmap API returns 404', async () => {
    heatmapServer.use(heatmapUnavailableHandler);
    render(<Heatmap />);

    await waitFor(() => {
      expect(screen.getByText('Heatmap Not Available')).toBeInTheDocument();
    });
    expect(
      screen.getByText(/Please ensure Heatmap is enabled in the configs and you have sufficient permissions/i)
    ).toBeInTheDocument();
    expect(screen.queryByTestId('heatmap-plot')).not.toBeInTheDocument();
    expect(screen.queryByPlaceholderText('/')).not.toBeInTheDocument();
    expect(commonUtils.showDataFetchError).not.toHaveBeenCalled();
  });

  test('shows fetch error state when heatmap API returns 500', async () => {
    heatmapServer.use(heatmapServerErrorHandler);
    render(<Heatmap />);

    await waitFor(() => {
      expect(screen.getByText('Failed to fetch Heatmap')).toBeInTheDocument();
    });
    expect(screen.getByText('Check for any failed requests for more information')).toBeInTheDocument();
    expect(commonUtils.showDataFetchError).toHaveBeenCalledWith('Request failed with status code 500');
    expect(screen.queryByTestId('heatmap-plot')).not.toBeInTheDocument();
  });
});
