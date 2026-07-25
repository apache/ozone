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
import { rest } from 'msw';

import Capacity from '@/v2/pages/capacity/capacity';
import { capacityServer } from '@tests/mocks/capacityMocks/capacityServer';
import * as mockResponses from '@tests/mocks/capacityMocks/capacityResponseMocks';

vi.mock('@/components/autoReloadPanel/autoReloadPanel', () => ({
  default: (props: { onReload?: () => void }) => (
    <div data-testid="auto-reload-panel">
      <button data-testid="manual-reload" onClick={() => props.onReload?.()}>
        reload
      </button>
    </div>
  ),
}));
vi.mock('@/components/eChart/eChart', () => ({
  EChart: () => <div data-testid="echart" />,
}));

describe('Capacity Page', () => {
  beforeAll(() => capacityServer.listen());
  afterEach(() => capacityServer.resetHandlers());
  afterAll(() => capacityServer.close());

  test('renders cluster and service breakdown with data', async () => {
    render(<Capacity />);

    expect(screen.getByText('Cluster Capacity')).toBeInTheDocument();
    expect(screen.getByTestId('auto-reload-panel')).toBeInTheDocument();

    const ozoneCapacityTitle = await screen.findByText('Ozone Capacity');
    const ozoneCapacityCard = ozoneCapacityTitle.closest('.ant-card');
    expect(ozoneCapacityCard).not.toBeNull();
    if (!ozoneCapacityCard) {
      return;
    }
    await waitFor(() =>
      expect(ozoneCapacityCard).toHaveTextContent(/TOTAL CAPACITY\s*10\s*KB/i)
    );
    expect(ozoneCapacityCard).toHaveTextContent(/USED SPACE\s*4\s*KB/i);
    expect(ozoneCapacityCard).toHaveTextContent(/OTHER USED SPACE\s*2\s*KB/i);
    expect(ozoneCapacityCard).toHaveTextContent(/CONTAINER PRE-ALLOCATED\s*1\s*KB/i);
    expect(ozoneCapacityCard).toHaveTextContent(/REMAINING SPACE\s*4\s*KB/i);

    const ozoneUsedSpaceTitle = screen.getByText('Ozone Used Space');
    const ozoneUsedSpaceCard = ozoneUsedSpaceTitle.closest('.ant-card');
    expect(ozoneUsedSpaceCard).not.toBeNull();
    if (!ozoneUsedSpaceCard) {
      return;
    }
    await waitFor(() =>
      expect(ozoneUsedSpaceCard).toHaveTextContent(/PENDING DELETION\s*7\s*KB/i)
    );
  });

  test('shows pending deletion and datanode detail values', async () => {
    render(<Capacity />);

    const pendingDeletionTitle = await screen.findByText('Pending Deletion');
    const pendingDeletionCard = pendingDeletionTitle.closest('.ant-card');
    expect(pendingDeletionCard).not.toBeNull();
    if (!pendingDeletionCard) {
      return;
    }
    await waitFor(() =>
      expect(pendingDeletionCard).toHaveTextContent(/OZONE MANAGER\s*2\s*KB/i)
    );
    expect(pendingDeletionCard)
      .toHaveTextContent(/STORAGE CONTAINER MANAGER\s*2\s*KB/i);
    expect(pendingDeletionCard).toHaveTextContent(/DATANODES\s*3\s*KB/i);

    const downloadLink = await screen.findByText('Download Insights');
    const datanodeCard = downloadLink.closest('.ant-card');
    expect(datanodeCard).not.toBeNull();
    if (!datanodeCard) {
      return;
    }
    await waitFor(() =>
      expect(datanodeCard).toHaveTextContent(/USED SPACE\s*5\s*KB/i)
    );
    expect(datanodeCard).toHaveTextContent(/FREE SPACE\s*3\s*KB/i);
  });

  test('defaults to the first available datanode when the first datanode reports -1 (offline/unreachable)', async () => {
    capacityServer.use(
      rest.get('api/v1/pendingDeletion', (req, res, ctx) => {
        const component = req.url.searchParams.get('component');
        if (component === 'dn') {
          return res(
            ctx.status(200),
            ctx.json({
              ...mockResponses.DnPendingDeletion,
              pendingDeletionPerDataNode: [
                { hostName: 'dn-1', datanodeUuid: 'uuid-1', pendingBlockSize: -1 },
                { hostName: 'dn-2', datanodeUuid: 'uuid-2', pendingBlockSize: 2048 }
              ]
            })
          );
        }
        const map: Record<string, object> = {
          scm: mockResponses.ScmPendingDeletion,
          om: mockResponses.OmPendingDeletion
        };
        const body = component ? map[component] : undefined;
        return body
          ? res(ctx.status(200), ctx.json(body))
          : res(ctx.status(400), ctx.json({ message: 'Unsupported pending deletion component.' }));
      })
    );

    render(<Capacity />);

    const downloadLink = await screen.findByText('Download Insights');
    const datanodeCard = downloadLink.closest('.ant-card');
    expect(datanodeCard).not.toBeNull();
    if (!datanodeCard) {
      return;
    }
    // dn-1 is unavailable (pendingBlockSize -1), but dn-2 is healthy, so the page should
    // default to dn-2 and show its capacity instead of landing on the error card.
    // USED SPACE = used (2048) + pendingBlockSize (2048) = 4 KB, FREE SPACE = remaining
    // (2048) + committed (1024) = 3 KB.
    await waitFor(() =>
      expect(datanodeCard).toHaveTextContent(/USED SPACE\s*4\s*KB/i)
    );
    expect(datanodeCard).toHaveTextContent(/FREE SPACE\s*3\s*KB/i);
    expect(screen.queryByTestId('dn-used-space-error')).not.toBeInTheDocument();
    expect(screen.queryByTestId('dn-free-space-error')).not.toBeInTheDocument();
    // The dropdown label must reflect the actually-selected DN (dn-2), not the first
    // (unavailable) option.
    const selectionItem = datanodeCard.querySelector('.ant-select-selection-item');
    expect(selectionItem).toHaveTextContent('dn-2');
    expect(selectionItem).not.toHaveTextContent('dn-1');
  });

  test('shows error card instead of outdated data when every datanode reports -1 (offline/unreachable)', async () => {
    capacityServer.use(
      rest.get('api/v1/pendingDeletion', (req, res, ctx) => {
        const component = req.url.searchParams.get('component');
        if (component === 'dn') {
          return res(
            ctx.status(200),
            ctx.json({
              ...mockResponses.DnPendingDeletion,
              pendingDeletionPerDataNode: [
                { hostName: 'dn-1', datanodeUuid: 'uuid-1', pendingBlockSize: -1 },
                { hostName: 'dn-2', datanodeUuid: 'uuid-2', pendingBlockSize: -1 }
              ]
            })
          );
        }
        const map: Record<string, object> = {
          scm: mockResponses.ScmPendingDeletion,
          om: mockResponses.OmPendingDeletion
        };
        const body = component ? map[component] : undefined;
        return body
          ? res(ctx.status(200), ctx.json(body))
          : res(ctx.status(400), ctx.json({ message: 'Unsupported pending deletion component.' }));
      })
    );

    render(<Capacity />);

    const downloadLink = await screen.findByText('Download Insights');
    const datanodeCard = downloadLink.closest('.ant-card');
    expect(datanodeCard).not.toBeNull();
    if (!datanodeCard) {
      return;
    }
    // No DN is available, so we fall back to dn-1; its pendingBlockSize is -1 (offline
    // sentinel), so the datanode is unavailable and its capacity data may be outdated.
    // Show an error card for USED SPACE and FREE SPACE instead of the stale
    // storage-report values.
    expect(await screen.findByTestId('dn-used-space-error')).toBeInTheDocument();
    expect(await screen.findByTestId('dn-free-space-error')).toBeInTheDocument();
    await waitFor(() =>
      expect(datanodeCard).toHaveTextContent(/USED SPACE\s*N\/A/i)
    );
    expect(datanodeCard).toHaveTextContent(/FREE SPACE\s*N\/A/i);
    // With every DN unavailable, the dropdown falls back to the first DN (dn-1).
    expect(datanodeCard.querySelector('.ant-select-selection-item')).toHaveTextContent('dn-1');
  });

  test('shows scm-only error state when SCM pending deletion returns sentinel failure values', async () => {
    capacityServer.use(
      rest.get('api/v1/pendingDeletion', (req, res, ctx) => {
        const component = req.url.searchParams.get('component');
        switch (component) {
        case 'scm':
          return res(
            ctx.status(200),
            ctx.json({
              totalBlocksize: -1,
              totalReplicatedBlockSize: -1,
              totalBlocksCount: -1
            })
          );
        case 'om':
          return res(
            ctx.status(200),
            ctx.json(mockResponses.OmPendingDeletion)
          );
        case 'dn':
          return res(
            ctx.status(200),
            ctx.json(mockResponses.DnPendingDeletion)
          );
        default:
          return res(
            ctx.status(400),
            ctx.json({ message: 'Unsupported pending deletion component.' })
          );
        }
      })
    );

    render(<Capacity />);

    const pendingDeletionTitle = await screen.findByText('Pending Deletion');
    const pendingDeletionCard = pendingDeletionTitle.closest('.ant-card');
    expect(pendingDeletionCard).not.toBeNull();
    if (!pendingDeletionCard) {
      return;
    }

    await waitFor(() =>
      expect(pendingDeletionCard).toHaveTextContent(/OZONE MANAGER\s*2\s*KB/i)
    );
    expect(pendingDeletionCard).toHaveTextContent(/DATANODES\s*3\s*KB/i);
    expect(pendingDeletionCard).toHaveTextContent(/STORAGE CONTAINER MANAGER\s*N\/A/i);
    expect(await screen.findByTestId('pending-deletion-scm-error')).toBeInTheDocument();
    await waitFor(() => expect(screen.getAllByTestId('echart')).toHaveLength(4));
  });

  test('node selector dropdown only lists datanodes from pending deletion API when cluster has more than 15 DNs', async () => {
    const totalDatanodes = 17;
    const pendingDeletionLimit = 15;

    const allDataNodeUsage = Array.from({ length: totalDatanodes }, (_, index) => {
      const datanodeNumber = index + 1;
      return {
        datanodeUuid: `uuid-${datanodeNumber}`,
        hostName: `dn-${datanodeNumber}`,
        capacity: 8192,
        used: 2048,
        remaining: 2048,
        committed: 1024,
        minimumFreeSpace: 256,
        reserved: 128
      };
    });

    const pendingDeletionPerDataNode = Array.from({ length: pendingDeletionLimit }, (_, index) => {
      const datanodeNumber = index + 1;
      return {
        hostName: `dn-${datanodeNumber}`,
        datanodeUuid: `uuid-${datanodeNumber}`,
        pendingBlockSize: 1024 * datanodeNumber
      };
    });
    let pendingDeletionLimitParam: string | null = null;

    capacityServer.use(
      rest.get('api/v1/storageDistribution', (req, res, ctx) => {
        return res(
          ctx.status(200),
          ctx.json({
            ...mockResponses.StorageDistribution,
            dataNodeUsage: allDataNodeUsage
          })
        );
      }),
      rest.get('api/v1/pendingDeletion', (req, res, ctx) => {
        const component = req.url.searchParams.get('component');
        switch (component) {
        case 'scm':
          return res(
            ctx.status(200),
            ctx.json(mockResponses.ScmPendingDeletion)
          );
        case 'om':
          return res(
            ctx.status(200),
            ctx.json(mockResponses.OmPendingDeletion)
          );
        case 'dn':
          pendingDeletionLimitParam = req.url.searchParams.get('limit');
          return res(
            ctx.status(200),
            ctx.json({
              status: 'FINISHED',
              totalPendingDeletionSize: pendingDeletionPerDataNode.reduce(
                (total, datanode) => total + datanode.pendingBlockSize,
                0
              ),
              pendingDeletionPerDataNode,
              totalNodesQueried: totalDatanodes,
              totalNodeQueriesFailed: 0
            })
          );
        default:
          return res(
            ctx.status(400),
            ctx.json({ message: 'Unsupported pending deletion component.' })
          );
        }
      })
    );

    render(<Capacity />);

    await waitFor(() => expect(pendingDeletionLimitParam).toBe('15'));

    const downloadLink = await screen.findByText('Download Insights');
    const datanodeCard = downloadLink.closest('.ant-card');
    expect(datanodeCard).not.toBeNull();
    if (!datanodeCard) {
      return;
    }

    await waitFor(() =>
      expect(datanodeCard).toHaveTextContent(/PENDING DELETION\s*1\s*KB/i)
    );
    expect(datanodeCard).toHaveTextContent(/OZONE USED\s*2\s*KB/i);
    expect(datanodeCard).toHaveTextContent(/USED SPACE\s*3\s*KB/i);

    const nodeSelector = within(datanodeCard as HTMLElement).getByRole('combobox');
    fireEvent.mouseDown(nodeSelector);

    await waitFor(() => {
      expect(document.querySelector('.ant-select-dropdown')).toBeInTheDocument();
    });

    const visibleDropdownHostNames = Array.from(
      document.querySelectorAll('.ant-select-item-option-content span:first-child')
    ).map(option => option.textContent);
    expect(visibleDropdownHostNames.length).toBeGreaterThan(0);
    expect(visibleDropdownHostNames.length).toBeLessThan(totalDatanodes);
    expect(visibleDropdownHostNames).toContain('dn-1');
    expect(visibleDropdownHostNames).not.toContain('dn-16');
    expect(visibleDropdownHostNames).not.toContain('dn-17');

    fireEvent.change(nodeSelector, { target: { value: 'dn-15' } });
    expect(await screen.findByRole('option', { name: 'dn-15' })).toBeInTheDocument();

    fireEvent.change(nodeSelector, { target: { value: 'dn-16' } });
    await waitFor(() =>
      expect(screen.queryByRole('option', { name: 'dn-16' })).not.toBeInTheDocument()
    );
    expect(screen.queryByRole('option', { name: 'dn-17' })).not.toBeInTheDocument();
  });

  // Interval constants mirrored from capacity.tsx / autoReload.constants.
  const PENDING_POLL_INTERVAL = 5 * 1000;
  const AUTO_RELOAD_INTERVAL = 60 * 1000;

  type EndpointCounts = { storage: number; scm: number; om: number; dn: number };

  // Installs handlers that count how often each endpoint is hit and serves the
  // supplied DN scan statuses in order (clamped to the last entry afterwards).
  const setupCountingHandlers = (dnStatuses: string[]) => {
    const counts: EndpointCounts = { storage: 0, scm: 0, om: 0, dn: 0 };
    capacityServer.use(
      rest.get('api/v1/storageDistribution', (_req, res, ctx) => {
        counts.storage++;
        return res(ctx.status(200), ctx.json(mockResponses.StorageDistribution));
      }),
      rest.get('api/v1/pendingDeletion', (req, res, ctx) => {
        const component = req.url.searchParams.get('component');
        if (component === 'dn') {
          const status = dnStatuses[Math.min(counts.dn, dnStatuses.length - 1)];
          counts.dn++;
          return res(
            ctx.status(200),
            ctx.json({ ...mockResponses.DnPendingDeletion, status })
          );
        }
        if (component === 'scm') {
          counts.scm++;
          return res(ctx.status(200), ctx.json(mockResponses.ScmPendingDeletion));
        }
        if (component === 'om') {
          counts.om++;
          return res(ctx.status(200), ctx.json(mockResponses.OmPendingDeletion));
        }
        return res(ctx.status(400), ctx.json({ message: 'Unsupported pending deletion component.' }));
      })
    );
    return counts;
  };

  test('Auto Refresh off: a manual refresh drives the DN scan, polling only the DN endpoint until it finishes, then syncs the other endpoints', async () => {
    // Auto Refresh disabled before mount, so useAutoReload never starts its timer.
    sessionStorage.setItem('autoReloadEnabled', 'false');

    // Mount read is FINISHED (no scan running). The manual refresh kicks off a
    // scan: the next reads are IN_PROGRESS, IN_PROGRESS, then FINISHED.
    const counts = setupCountingHandlers([
      'FINISHED',      // mount
      'IN_PROGRESS',   // manual refresh
      'IN_PROGRESS',   // poll #1
      'FINISHED'       // poll #2
    ]);

    vi.useFakeTimers();
    try {
      render(<Capacity />);

      // Flush the initial mount fetch: everything fetched exactly once.
      await vi.advanceTimersByTimeAsync(50);
      expect(counts).toEqual({ storage: 1, scm: 1, om: 1, dn: 1 });

      // Scan is FINISHED, so nothing is polled while idle.
      await vi.advanceTimersByTimeAsync(AUTO_RELOAD_INTERVAL * 2);
      expect(counts).toEqual({ storage: 1, scm: 1, om: 1, dn: 1 });

      // Manual reload -> full refresh of all four endpoints; DN comes back
      // IN_PROGRESS, which starts the DN-only poll.
      fireEvent.click(screen.getByTestId('manual-reload'));
      await vi.advanceTimersByTimeAsync(50);
      expect(counts).toEqual({ storage: 2, scm: 2, om: 2, dn: 2 });

      // While IN_PROGRESS only the DN endpoint is polled; the others stay put.
      await vi.advanceTimersByTimeAsync(PENDING_POLL_INTERVAL);
      expect(counts).toEqual({ storage: 2, scm: 2, om: 2, dn: 3 });

      // Next poll returns FINISHED -> DN polling stops and the other three
      // endpoints are synced exactly once.
      await vi.advanceTimersByTimeAsync(PENDING_POLL_INTERVAL);
      expect(counts).toEqual({ storage: 3, scm: 3, om: 3, dn: 4 });

      // No further polling once finished, and no periodic refresh while off.
      await vi.advanceTimersByTimeAsync(AUTO_RELOAD_INTERVAL * 2);
      expect(counts).toEqual({ storage: 3, scm: 3, om: 3, dn: 4 });
    } finally {
      vi.useRealTimers();
      sessionStorage.removeItem('autoReloadEnabled');
    }
  });

  test('Auto Refresh on: refreshes all endpoints on the interval, polls only the DN endpoint while a scan runs, and syncs the others when it finishes', async () => {
    // Auto Refresh enabled (default). useAutoReload runs a full refresh every 60s.
    sessionStorage.setItem('autoReloadEnabled', 'true');

    // A scan is already running at mount: IN_PROGRESS, IN_PROGRESS, then FINISHED.
    const counts = setupCountingHandlers([
      'IN_PROGRESS',   // mount
      'IN_PROGRESS',   // poll #1
      'FINISHED'       // poll #2
    ]);

    vi.useFakeTimers();
    try {
      render(<Capacity />);

      // Flush the initial mount fetch: everything fetched once, scan IN_PROGRESS.
      await vi.advanceTimersByTimeAsync(50);
      expect(counts).toEqual({ storage: 1, scm: 1, om: 1, dn: 1 });

      // While IN_PROGRESS only the DN endpoint is polled.
      await vi.advanceTimersByTimeAsync(PENDING_POLL_INTERVAL);
      expect(counts).toEqual({ storage: 1, scm: 1, om: 1, dn: 2 });

      // Next poll returns FINISHED -> DN polling stops and the others sync once.
      await vi.advanceTimersByTimeAsync(PENDING_POLL_INTERVAL);
      expect(counts).toEqual({ storage: 2, scm: 2, om: 2, dn: 3 });

      // No DN-only polling once finished.
      await vi.advanceTimersByTimeAsync(PENDING_POLL_INTERVAL * 4);
      expect(counts).toEqual({ storage: 2, scm: 2, om: 2, dn: 3 });

      // At the auto-refresh interval, all four endpoints are refreshed together.
      await vi.advanceTimersByTimeAsync(AUTO_RELOAD_INTERVAL);
      expect(counts).toEqual({ storage: 3, scm: 3, om: 3, dn: 4 });
    } finally {
      vi.useRealTimers();
      sessionStorage.removeItem('autoReloadEnabled');
    }
  });
});
