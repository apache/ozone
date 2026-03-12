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

import EChart from '@/v2/components/eChart/eChart';
import { byteToSize } from '@/utils/common';
import { NUSubpath } from '@/v2/types/namespaceUsage.types';

//-------Types--------//
type PieChartProps = {
  path?: string | null;
  limit: number;
  size: number;
  subPaths: NUSubpath[];
  subPathCount: number;
  sizeWithReplica: number;
  loading: boolean;
}

//-------Constants---------//
const OTHER_PATH_NAME = 'Other Objects';
const MIN_BLOCK_SIZE = 0.05;


//----------Component---------//
const NUPieChart: React.FC<PieChartProps> = ({
  path,
  limit,
  size,
  subPaths,
  subPathCount,
  sizeWithReplica,
  loading
}) => {
  const [subpathSize, setSubpathSize]  = React.useState<number>(0);

  function getSubpathSize(subpaths: NUSubpath[]): number {
    const subpathSize = subpaths
      .map((subpath) => subpath.size)
      .reduce((acc, curr) => acc + curr, 0);
    // If there is no subpaths, then the size will be total size of path
    return (subPaths.length === 0) ? size : subpathSize;
  }

  function updatePieData() {
    /**
     * We need to calculate the size of "Other objects" in two cases:
     * 
     *  1) If we have more subpaths listed, than the limit.
     *  2) If the limit is set to the maximum limit (30) and we have any number of subpaths.
     *     In this case we won't necessarily have "Other objects", but we check if the
     *     other objects's size is more than zero (we will have other objects if there are more than 30 subpaths,
     *     but we can't check on that, as the response will always have
     *     30 subpaths, but from the total size and the subpaths size we can calculate it).
     */
    let subpaths: NUSubpath[] = subPaths;

    let pathLabels: string[] = [];
    let percentage: string[] = [];
    let sizeStr: string[];
    let valuesWithMinBlockSize: number[] = [];

    if (subPathCount > limit) {
      // If the subpath count is greater than the provided limit
      // Slice the subpath to the limit
      subpaths = subpaths.slice(0, limit);
      // Add the size of the subpath
      const limitedSize = getSubpathSize(subpaths);
      const remainingSize = size - limitedSize;
      subpaths.push({
        path: OTHER_PATH_NAME,
        size: remainingSize,
        sizeWithReplica: (sizeWithReplica === -1)
          ? -1
          : sizeWithReplica - remainingSize,
        isKey: false
      })
    }

    if (subPathCount === 0 || subpaths.length === 0) {
      // No more subpaths available
      pathLabels = [(path ?? '/').split('/').pop() ?? ''];
      valuesWithMinBlockSize = [0.1];
      percentage = ['100.00'];
      sizeStr = [byteToSize(size, 1)];
    } else {
      pathLabels = subpaths.map(subpath => {
        const subpathName = subpath.path.split('/').pop() ?? '';
        // Diferentiate keys by removing trailing slash
        return (subpath.isKey || subpathName === OTHER_PATH_NAME)
          ? subpathName
          : subpathName + '/';
      });

      let values: number[] = [0];
      if (size > 0) {
        values = subpaths.map(
          subpath => (subpath.size / size)
        );
      }
      const valueClone = structuredClone(values);
      valuesWithMinBlockSize = valueClone?.map(
        (val: number) => (val > 0)
          ? val + MIN_BLOCK_SIZE
          : val
      );
      percentage = values.map(value => (value * 100).toFixed(2));
      sizeStr = subpaths.map((subpath) => byteToSize(subpath.size, 1));
    }

    return valuesWithMinBlockSize.map((key, idx) => {
      return {
        value: key,
        name: pathLabels[idx],
        size: sizeStr[idx],
        percentage: percentage[idx]
      }
    });
  }

  React.useEffect(() => {
    setSubpathSize(getSubpathSize(subPaths));
  }, [subPaths, limit]);

  const pieData = React.useMemo(() => updatePieData(), [path, subPaths, limit]);

  const eChartsOptions = {
    title: {
      text: `${byteToSize(subpathSize, 1)} /  ${byteToSize(size, 1)}`,
      left: 'center',
      top: '95%'
    },
    tooltip: {
      trigger: 'item',
      formatter: ({ dataIndex, name, color }) => {
        const nameEl = `<strong style='color: ${color}'>${name}</strong><br>`;
        const dataEl = `Total Data Size: ${pieData[dataIndex]['size']}<br>`
        const percentageEl = `Percentage: ${pieData[dataIndex]['percentage']} %`
        return `${nameEl}${dataEl}${percentageEl}`
      }
    },
    legend: {
      show: false
    },
    series: [
      {
        type: 'pie',
        radius: '70%',
        data: pieData.map((value) => {
          return {
            value: value.value,
            name: value.name
          }
        }),
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        }
      }
    ]
  };

  const handleLegendChange = ({selected}: {selected: Record<string, boolean>}) => {
    const filteredPath = subPaths.filter((value) => {
      // In case of any leading '/' remove them and add a / at end
      // to make it similar to legend
      const splitPath = value.path?.split('/');
      const pathName = splitPath[splitPath.length - 1] ?? '' + ((value.isKey) ? '' : '/');
      return selected[pathName];
    })
    const newSize = getSubpathSize(filteredPath);
    setSubpathSize(newSize);
  }

  return (
    <EChart
      loading={loading}
      option={eChartsOptions}
      style={{ flex: '1 3 80%', height: '50vh' }}
      eventHandler={{name: 'legendselectchanged', handler: handleLegendChange}}/>
  );
}

export default NUPieChart;