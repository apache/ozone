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
import filesize from 'filesize';
import { EChartsOption } from 'echarts';

import EChart from '@/v2/components/eChart/eChart';
import { ContainerCountResponse, ContainerPlotData } from '@/v2/types/insights.types';

type ContainerSizeDistributionProps = {
  containerCountResponse: ContainerCountResponse[];
  containerSizeError: string | undefined;
}

const size = filesize.partial({ standard: 'iec', round: 0 });

const ContainerSizeDistribution: React.FC<ContainerSizeDistributionProps> = ({
  containerCountResponse,
  containerSizeError
}) => {

  const [containerPlotData, setContainerPlotData] = React.useState<ContainerPlotData>({
    containerCountValues: [],
    containerCountMap: new Map<number, number>()
  });

  function updatePlotData() {
    const containerCountMap: Map<number, number> = containerCountResponse.reduce(
      (map: Map<number, number>, current) => {
        const containerSize = current.containerSize;
        const oldCount = map.get(containerSize) ?? 0;
        map.set(containerSize, oldCount + current.count);
        return map;
      },
      new Map<number, number>()
    );

    const containerCountValues = Array.from(containerCountMap.keys()).map(value => {
      const upperbound = size(value);
      const upperboundPwr = Math.log2(value);

      const lowerbound = upperboundPwr > 10 ? size(2 ** (upperboundPwr - 1)) : size(0);
      return `${lowerbound} - ${upperbound}`;
    });

    setContainerPlotData({
      containerCountValues: containerCountValues,
      containerCountMap: containerCountMap
    });
  }

  React.useEffect(() => {
    updatePlotData();
  }, [containerCountResponse]);

  const { containerCountMap, containerCountValues } = containerPlotData;

  const containerPlotOptions: EChartsOption = {
    tooltip: {
      trigger: 'item',
      formatter: ({ data }) => {
        return `Size Range: <strong>${data.name}</strong><br>Count: <strong>${data.value}</strong>`
      }
    },
    legend: {
      orient: 'vertical',
      left: 'right'
    },
    series: {
      type: 'pie',
      radius: '50%',
      data: Array.from(containerCountMap?.values() ?? []).map((value, idx) => {
        return {
          value: value,
          name: containerCountValues[idx] ?? ''
        }
      }),
    },
    graphic: (containerSizeError) ? {
      type: 'group',
      left: 'center',
      top: 'middle',
      z: 100,
      children: [
        {
          type: 'rect',
          left: 'center',
          top: 'middle',
          z: 100,
          shape: {
            width: 500,
            height: 500
          },
          style: {
            fill: 'rgba(256, 256, 256, 0.5)'
          }
        },
        {
          type: 'rect',
          left: 'center',
          top: 'middle',
          z: 100,
          shape: {
            width: 500,
            height: 40
          },
          style: {
            fill: '#FC909B'
          }
        },
        {
          type: 'text',
          left: 'center',
          top: 'middle',
          z: 100,
          style: {
            text: `No data available. ${containerSizeError}`,
            font: '20px sans-serif'
          }
        }
      ]
    } : undefined
  }
  
  return (<>
    <EChart option={containerPlotOptions} style={{
        width: '30vw',
        height: '65vh'
      }} />
  </>)
}

export default ContainerSizeDistribution;