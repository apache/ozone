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
import { cleanup, render, waitFor } from '@testing-library/react';
import Heatmap from '@/v2/pages/heatmap/heatmap';
import HeatmapPlot from '@/v2/components/plots/heatmapPlot';
import { Heatmap as LegacyHeatmap } from '@/views/heatMap/heatmap';
import HeatMapConfiguration from '@/views/heatMap/heatMapConfiguration';
import { useApiData } from '@/v2/hooks/useAPIData.hook';

const { chart } = vi.hoisted(() => ({ chart: vi.fn() }));
vi.mock('ag-charts-react', () => ({
  AgChartsReact: (props: any) => { chart(props.options); return null; }
}));
vi.mock('@/v2/hooks/useAPIData.hook', () => ({ useApiData: vi.fn() }));

const leaf = (color?: number) => ({
  label: 'cold-key', path: '/vol/bucket/cold-key', size: 256, accessCount: 1,
  ...(color === undefined ? {} : { color })
});
const tree = (child: any) => ({
  label: 'bucket', path: '/vol/bucket', size: 512,
  minAccessCount: 1, maxAccessCount: 5000,
  children: [child, { ...leaf(1), label: 'hot-key', accessCount: 5000 }]
});

afterEach(() => { cleanup(); vi.clearAllMocks(); });

describe.each([undefined, 0, 0.002])('heatmap leaf with color %s', color => {
  test('v2 gives the leaf a visible area without sizing the group', async () => {
    const data = tree(leaf(color));
    vi.mocked(useApiData).mockReturnValue({ data } as any);
    render(<Heatmap />);
    await waitFor(() => expect((data.children[0] as any).normalizedSize).toBeGreaterThan(0));
    expect(data.children[0].color).toBe(color ?? 0);
    expect(data.children[1].color).toBe(1);
    expect((data as any).normalizedSize).toBeUndefined();
  });

  test('legacy gives the leaf a visible area and includes it in size bounds', () => {
    const page = new LegacyHeatmap({ location: {} } as any);
    const data = tree(leaf(color));
    data.children[0].size = 128;
    expect(page.minmax(data)).toEqual([128, 256]);
    page.updateSize(data);
    expect((data.children[0] as any).normalizedSize).toBeGreaterThan(0);
    expect(data.children[0].color).toBe(color ?? 0);
    expect(data.children[1].color).toBe(1);
    expect((data as any).normalizedSize).toBeUndefined();
  });

  test.each(['v2', 'legacy'])('%s only drills into groups', version => {
    const onClick = vi.fn();
    const data = tree(leaf(color));
    render(version === 'v2'
      ? <HeatmapPlot data={data as any} onClick={onClick} colorScheme={[]} entityType='key' />
      : <HeatMapConfiguration data={[data] as any} onClick={onClick} colorScheme={[]} />);
    const click = chart.mock.calls[chart.mock.calls.length - 1][0].series[0].listeners.nodeClick;
    click({ datum: data.children[0] });
    expect(onClick).not.toHaveBeenCalled();
    click({ datum: data });
    expect(onClick).toHaveBeenCalledWith('/vol/bucket');
  });
});

test.each(['v2', 'legacy'])('%s preserves zero-size handling for leaves with empty children', async version => {
  const hidden = { ...leaf(), size: 0, accessCount: 0, children: [] };
  const visible = { ...leaf(), size: 0, children: [] };
  const data = { ...tree(hidden), children: [hidden, visible] };
  if (version === 'v2') {
    vi.mocked(useApiData).mockReturnValue({ data } as any);
    render(<Heatmap />);
    await waitFor(() => expect((hidden as any).normalizedSize).toBe(0));
  } else {
    new LegacyHeatmap({ location: {} } as any).updateSize(data);
  }
  expect((hidden as any).normalizedSize).toBe(0);
  expect((visible as any).normalizedSize).toBe(1);
});
