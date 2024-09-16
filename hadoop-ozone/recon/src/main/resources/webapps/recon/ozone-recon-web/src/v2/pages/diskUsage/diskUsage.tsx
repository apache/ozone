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

import React, { useRef, useState } from 'react';
import { AxiosError } from 'axios';
import {
  Alert, Layout,
} from 'antd';
import {
  InfoCircleFilled
} from '@ant-design/icons';
import { ValueType } from 'react-select';

import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import { byteToSize, showDataFetchError } from '@/utils/common';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';

import { DUResponse, DUState, DUSubpath, PlotData } from '@/v2/types/diskUsage.types';

import './diskUsage.less';
import DUMetadata from '@/v2/components/duMetadata/duMetadata';

const OTHER_PATH_NAME = 'Other Objects';
const MIN_BLOCK_SIZE = 0.05;

const LIMIT_OPTIONS: Option[] = [
  { label: '5', value: '5' },
  { label: '10', value: '10' },
  { label: '15', value: '15' },
  { label: '20', value: '20' },
  { label: '30', value: '30' }
]

const DiskUsage: React.FC<{}> = () => {
  const [loading, setLoading] = useState<boolean>(false);
  const [limit, setLimit] = useState<Option>(LIMIT_OPTIONS[1]);
  const [state, setState] = useState<DUState>({
    duResponse: {},
    plotData: []
  });
  const [selectedPath, setSelectedPath] = useState<string>('/');

  const cancelPieSignal = useRef<AbortController>();

  function updatePieChart(path: string) {
    setLoading(true);
    const { request, controller } = AxiosGetHelper(
      `/api/v1/namespace/du?path=${path}&files=true&sortSubPaths=true`,
      cancelPieSignal.current
    );
    cancelPieSignal.current = controller;

    request.then(response => {
      let pathLabels: string[] = [];
      let percentage: string[] = [];
      let sizeStr: string[];
      let valuesWithMinBlockSize: number[] = [];

      const duResponse: DUResponse = response.data;
      const status = duResponse.status;
      if (status === 'PATH_NOT_FOUND') {
        setLoading(false);
        showDataFetchError(`Invalid Path: ${path}`);
        return;
      }

      const dataSize = duResponse.size;
      let subpaths: DUSubpath[] = duResponse.subPaths;

      // We need to calculate the size of "Other objects" in two cases:
      // 1) If we have more subpaths listed, than the limit.
      // 2) If the limit is set to the maximum limit (30) and we have any number of subpaths.
      //    In this case we won't necessarily have "Other objects", but we check if the
      //    other objects's size is more than zero (we will have other objects if there are more than 30 subpaths,
      //    but we can't check on that, as the response will always have
      //    30 subpaths, but from the total size and the subpaths size we can calculate it).

      if (duResponse.subPathCount > Number.parseInt(limit.value)) {
        // If the subpath count is greater than the provided limit
        // Slice the subpath to the limit
        subpaths = subpaths.slice(0, Number.parseInt(limit.value));
        // Add the size of the subpath
        const limitedSize = subpaths
          .map((subpath) => subpath.size)
          .reduce((acc, curr) => acc + curr, 0);
        const remainingSize = dataSize - limitedSize;
        subpaths.push({
          path: OTHER_PATH_NAME,
          size: remainingSize,
          sizeWithReplica: (duResponse.sizeWithReplica === -1)
            ? -1
            : duResponse.sizeWithReplica - remainingSize,
          isKey: false
        })
      }

      if (duResponse.subPathCount === 0 || subpaths.length === 0) {
        // No more subpaths available
        pathLabels = [duResponse?.path.split('/').pop() ?? ''];
        valuesWithMinBlockSize = [0.1];
        percentage = ['100.00'];
        sizeStr = [byteToSize(dataSize, 1)];
      } else {
        pathLabels = subpaths.map(subpath => {
          const subpathName = subpath.path.split('/').pop() ?? '';
          // Diferentiate keys by removing trailing slash
          return (subpath.isKey || subpathName === OTHER_PATH_NAME)
            ? subpathName
            : subpathName + '/';
        });

        let values: number[] = [0];
        if (dataSize > 0) {
          values = subpaths.map(
            subpath => (subpath.size / dataSize)
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
      setLoading(false);
      setSelectedPath(duResponse.path);
      setState({
        duResponse: duResponse,
        plotData: valuesWithMinBlockSize.map((key, idx) => {
          return {
            value: key,
            name: pathLabels[idx],
            size: sizeStr[idx],
            percentage: percentage[idx]
          } as PlotData
        })
      });
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    });
  }

  function handleMenuClick(e) {
    cancelRequests([
      cancelPieSignal.current!,
    ]);

    updatePieChart(selectedPath);
  }

  function handleLimitChange(selected: ValueType<Option, false>) {
    setLimit(selected as Option);
  }

  React.useEffect(() => {
    //Load root path by default
    updatePieChart('/');

    return (() => {
      cancelPieSignal.current && cancelPieSignal.current.abort();
    });
  }, [limit]);

  const { plotData } = state;

  const eChartsOptions = {
    tooltip: {
      trigger: 'item',
      formatter: ({ dataIndex, name, color }) => {
        const nameEl = `<strong style='color: ${color}'>${name}</strong><br>`;
        const dataEl = `Total Data Size: ${plotData[dataIndex]['size']}<br>`
        const percentageEl = `Percentage: ${plotData[dataIndex]['percentage']} %`
        return `${nameEl}${dataEl}${percentageEl}`
      }
    },
    legend: {
      top: '10%',
      orient: 'vertical',
      left: 'left'
    },
    series: [
      {
        type: 'pie',
        radius: '50%',
        data: plotData.map((value) => {
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

  return (
    <>
      <div className='page-header-v2'>
        Disk Usage
      </div>
      <div style={{ padding: '24px' }}>
        <Alert
          className='du-alert-message'
          message="Additional block size is added to small entities, for better visibility.
            Please refer to pie-chart details for exact size information."
          type="info"
          icon={<InfoCircleFilled />}
          showIcon={true}
          closable={false} />
        <div className='content-div'>
          <div className='table-header-section'>
            <div className='table-filter-section'>
              <SingleSelect
                options={LIMIT_OPTIONS}
                defaultValue={limit}
                placeholder='Limit'
                onChange={handleLimitChange} />
            </div>
          </div>
          <DUMetadata path={state.duResponse.path} />
        </div>
      </div>
    </>
  );
}

export default DiskUsage;