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
import { Alert, Button, Tooltip } from 'antd';
import { InfoCircleFilled, ReloadOutlined } from '@ant-design/icons';
import { ValueType } from 'react-select';

import DUMetadata from '@/v2/components/duMetadata/duMetadata';
import DUPieChart from '@/v2/components/plots/duPieChart';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';

import DUBreadcrumbNav from '@/v2/components/duBreadcrumbNav/duBreadcrumbNav';
import { showDataFetchError } from '@/utils/common';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';

import { DUResponse } from '@/v2/types/diskUsage.types';

import './diskUsage.less';

const LIMIT_OPTIONS: Option[] = [
  { label: '5', value: '5' },
  { label: '10', value: '10' },
  { label: '15', value: '15' },
  { label: '20', value: '20' },
  { label: '30', value: '30' },
];

const DiskUsage: React.FC = () => {
  const [loading, setLoading] = useState<boolean>(false);
  const [limit, setLimit] = useState<Option>(LIMIT_OPTIONS[1]);
  const [duResponse, setDUResponse] = useState<DUResponse>({
    status: '',
    path: '/',
    subPathCount: 0,
    size: 0,
    sizeWithReplica: 0,
    subPaths: [],
    sizeDirectKey: 0,
  });

  const cancelPieSignal = useRef<AbortController>();

  function loadData(path: string) {
    setLoading(true);
    const { request, controller } = AxiosGetHelper(
      `/api/v1/namespace/du?path=${path}&files=true&sortSubPaths=true`,
      cancelPieSignal.current
    );
    cancelPieSignal.current = controller;

    request
      .then((response) => {
        const duResponse: DUResponse = response.data;
        const status = duResponse.status;
        if (status === 'PATH_NOT_FOUND') {
          setLoading(false);
          showDataFetchError(`Invalid Path: ${path}`);
          return;
        }

        setDUResponse(duResponse);
        setLoading(false);
      })
      .catch((error) => {
        setLoading(false);
        showDataFetchError((error as AxiosError).toString());
      });
  }

  function handleLimitChange(selected: ValueType<Option, false>) {
    setLimit(selected as Option);
  }

  React.useEffect(() => {
    //On mount load default data
    loadData(duResponse.path);

    return () => {
      cancelRequests([cancelPieSignal.current!]);
    };
  }, []);

  return (
    <>
      <div className='page-header-v2'>Disk Usage</div>
      <div className='data-container'>
        <Alert
          className='du-alert-message'
          message='Additional block size is added to small entities, for better visibility.
            Please refer to pie-chart details for exact size information.'
          type='info'
          icon={<InfoCircleFilled />}
          showIcon={true}
          closable={false}
        />
        <div className='content-div'>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
            }}
          >
            <DUBreadcrumbNav
              path={duResponse.path}
              subPaths={duResponse.subPaths}
              updateHandler={loadData}
            />
            <Tooltip title='Click to reload Disk Usage data'>
              <Button
                type='primary'
                icon={<ReloadOutlined />}
                onClick={() => loadData(duResponse.path)}
              />
            </Tooltip>
          </div>
          <div className='du-table-header-section'>
            <SingleSelect
              options={LIMIT_OPTIONS}
              defaultValue={limit}
              placeholder='Limit'
              onChange={handleLimitChange}
            />
          </div>
          <div className='du-content'>
            <DUPieChart
              loading={loading}
              limit={Number.parseInt(limit.value)}
              path={duResponse.path}
              subPathCount={duResponse.subPathCount}
              subPaths={duResponse.subPaths}
              sizeWithReplica={duResponse.sizeWithReplica}
              size={duResponse.size}
              onDrillDown={loadData}
            />
            <DUMetadata path={duResponse.path} />
          </div>
        </div>
      </div>
    </>
  );
};

export default DiskUsage;
