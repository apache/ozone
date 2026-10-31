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
import { render } from '@testing-library/react';
import { vi } from 'vitest';

import HeatmapPlot from '@/v2/components/plots/heatmapPlot';
import * as CONSTANTS from '@/v2/constants/heatmap.constants';
import { HeatmapResponse } from '@/v2/types/heatmap.types';

type AgChartOptions = {
  title?: { text?: string };
  series?: Array<{
    tooltip?: {
      renderer: (params: { datum: Record<string, unknown> }) => { content: string };
    };
  }>;
};

let lastChartOptions: AgChartOptions | undefined;

vi.mock('ag-charts-react', () => ({
  AgChartsReact: ({ options }: { options: AgChartOptions }) => {
    lastChartOptions = options;
    return <div data-testid="ag-charts-mock" />;
  },
}));

const rootHeatmapData: HeatmapResponse = {
  label: 'root',
  path: '/',
  size: 512,
  minAccessCount: 10,
  maxAccessCount: 100,
  children: [],
};

function renderTooltipContent(datum: Record<string, unknown>): string {
  if (!lastChartOptions?.series?.[0]?.tooltip?.renderer) {
    throw new Error('HeatmapPlot did not configure a treemap tooltip renderer');
  }
  return lastChartOptions.series[0].tooltip.renderer({ datum }).content;
}

describe('HeatmapPlot tooltips', () => {
  beforeEach(() => {
    lastChartOptions = undefined;
  });

  test('shows size, access count, and entity name for a leaf tile (hover tooltip content)', () => {
    render(
      <HeatmapPlot
        data={rootHeatmapData}
        onClick={vi.fn()}
        colorScheme={CONSTANTS.colourScheme.amberAlert}
        entityType="bucket"
      />
    );

    const content = renderTooltipContent({
      label: 'clusterbuck33',
      size: 256,
      accessCount: 3832,
      color: 1,
    });

    expect(content).toContain('<strong>Size</strong>:');
    expect(content).toContain('256 Bytes');
    expect(content).toContain('<strong>Access count</strong>:');
    expect(content).toContain('3832');
    expect(content).toContain('<strong>Entity Name</strong>:');
    expect(content).toContain('clusterbuck33');
  });

  test('shows max access count for a non-leaf group tile (hover tooltip content)', () => {
    render(
      <HeatmapPlot
        data={rootHeatmapData}
        onClick={vi.fn()}
        colorScheme={CONSTANTS.colourScheme.amberAlert}
        entityType="bucket"
      />
    );

    const content = renderTooltipContent({
      label: 'vol1',
      path: '/vol1',
      size: 512,
      maxAccessCount: 1220,
    });

    expect(content).not.toContain('<strong>Access count</strong>:');
    expect(content).toContain('<strong>Size</strong>:');
    expect(content).toContain('512 Bytes');
    expect(content).toContain('<strong>Max Access Count</strong>:');
    expect(content).toContain('1220');
    expect(content).toContain('<strong>Entity Name</strong>:');
    expect(content).toContain('vol1');
  });

  test('uses entity type in chart title', () => {
    render(
      <HeatmapPlot
        data={rootHeatmapData}
        onClick={vi.fn()}
        colorScheme={CONSTANTS.colourScheme.amberAlert}
        entityType="bucket"
      />
    );

    expect(lastChartOptions?.title?.text).toBe('Bucket Heatmap');
  });
});
