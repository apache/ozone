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
import { AgChartsReact } from 'ag-charts-react';
import { byteToSize } from '@/utils/common';
import { HeatmapResponse } from '@/v2/types/heatmap.types';

type HeatmapPlotProps = {
  data: HeatmapResponse;
  onClick: (arg0: string) => void;
  colorScheme: string[];
  entityType: string;
};

const capitalize = <T extends string>(str: T) => {
  return str.charAt(0).toUpperCase() + str.slice(1) as Capitalize<typeof str>;
}

const HeatmapPlot: React.FC<HeatmapPlotProps> = ({
  data,
  onClick,
  colorScheme,
  entityType = ''
}) => {

  const tooltipContent = (params: any) => {
    let tooltipContent = `<span>
      <strong>Size</strong>:
      ${byteToSize(params.datum.size, 1)}
      `;
    if (params.datum.accessCount !== undefined) {
      tooltipContent += `<br/>
        <strong>Access count</strong>:
      ${params.datum.accessCount }
    `;
      }
    else{
        tooltipContent += `<br/>
        <strong>Max Access Count</strong>:
      ${params.datum.maxAccessCount}
    `;}
    if (params.datum.label !== '') {
      tooltipContent += `<br/>
        <strong>Entity Name</strong>:
        ${params.datum.label ? params.datum.label.split('/').slice(-1) : ''}
      `;
    }
    tooltipContent += '</span>';
    return tooltipContent;
  };

  const heatmapConfig = {
    type: 'treemap',
    labelKey: 'label',// the name of the key to fetch the label value from
    sizeKey: 'normalizedSize',// the name of the key to fetch the value that will determine tile size
    colorKey: 'color',
    title: { color: '#424242', fontSize: 14, fontFamily: 'Roboto', fontWeight: '600' },
    subtitle: { color: '#424242', fontSize: 12, fontFamily: 'Roboto', fontWeight: '400' },
    tooltip: {
      renderer: (params) => {
        return {
          content: tooltipContent(params)
        };
      }
    },
    formatter: ({ highlighted }: { highlighted: boolean }) => {
      const stroke = highlighted ? '#CED4D9' : '#FFFFFF';
      return { stroke };
    },
    labels: {
      color: '#FFFFFF',
      fontWeight: 'bold',
      fontSize: 12
    },
    tileStroke: '#FFFFFF',
    tileStrokeWidth: 1.4,
    colorDomain: [
      0.000,
      0.050,
      0.100,
      0.150,
      0.200,
      0.250,
      0.300,
      0.350,
      0.400,
      0.450,
      0.500,
      0.550,
      0.600,
      0.650,
      0.700,
      0.750,
      0.800,
      0.850,
      0.900,
      0.950,
      1.000
    ],
    colorRange: [...colorScheme],
    groupFill: '#E6E6E6',
    groupStroke: '#E1E2E6',
    nodePadding: 3,
    labelShadow: { enabled: false }, //labels shadow
    gradient: false,
    highlightStyle: {
      text: {
        color: '#424242',
      },
      item: {
        fill: 'rgba(0, 0 ,0, 0.0)',
      },
    },
    listeners: {
        nodeClick: (event) => {
        var data = event.datum;
        // Leaf level box should not call API
        if (!data.color)
          if (data.path) {
            onClick(data.path);
          }
        },
      },
    }
  
  const options = {
    data,
    series: [heatmapConfig],
    title: { text: `${capitalize(entityType)} Heatmap`}
  };

  return <AgChartsReact options={options} />
}

export default HeatmapPlot;