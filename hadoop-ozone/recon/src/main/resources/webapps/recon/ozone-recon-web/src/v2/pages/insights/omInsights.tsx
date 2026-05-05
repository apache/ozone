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
import { ValueType } from 'react-select';
import { Tabs, Tooltip } from 'antd';
import { TablePaginationConfig } from 'antd/es/table';
import { InfoCircleOutlined } from '@ant-design/icons';

import { Option } from '@/v2/components/select/singleSelect';
import ContainerMismatchTable from '@/v2/components/tables/insights/containerMismatchTable';
import DeletedContainerKeysTable from '@/v2/components/tables/insights/deletedContainerKeysTable';
import DeletePendingDirTable from '@/v2/components/tables/insights/deletePendingDirsTable';
import DeletePendingKeysTable from '@/v2/components/tables/insights/deletePendingKeysTable';
import ExpandedKeyTable from '@/v2/components/tables/insights/expandedKeyTable';
import OpenKeysTable from '@/v2/components/tables/insights/openKeysTable';
import { showDataFetchError } from '@/utils/common';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';

import {
  Container,
  ExpandedRow,
  ExpandedRowState,
  MismatchKeysResponse
} from '@/v2/types/insights.types';

import './insights.less';
import { useLocation } from 'react-router-dom';


const OMDBInsights: React.FC<{}> = () => {

  const [loading, setLoading] = React.useState<boolean>(false);
  const [expandedRowData, setExpandedRowData] = React.useState<ExpandedRow>({});
  const [selectedLimit, setSelectedLimit] = React.useState<Option>({
    label: '1000',
    value: '1000'
  });

  const rowExpandSignal = React.useRef<AbortController>();
  const {state: { activeTab } = {}} = useLocation<Record<string, any>>();

  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total}`
  };

  function onRowExpandClick(expanded: boolean, record: Container) {
    if (expanded) {
      setLoading(true);

      const { request, controller } = AxiosGetHelper(
        `/api/v1/containers/${record.containerId}/keys`,
        rowExpandSignal.current
      );
      rowExpandSignal.current = controller;

      request.then(response => {
        const containerKeysResponse: MismatchKeysResponse = response?.data;
        const expandedRowState: ExpandedRowState = Object.assign(
          {},
          expandedRowData[record.containerId],
          {
            dataSource: containerKeysResponse?.keys ?? [],
            totalCount: containerKeysResponse?.totalCount ?? 0
          }
        );
        setExpandedRowData(Object.assign(
          {},
          expandedRowData,
          { [record.containerId]: expandedRowState }
        ));
        setLoading(false);
      }).catch(error => {
        showDataFetchError(error);
      });
    }
  }

  function expandedRowRender(record: Container) {
    const containerId = record.containerId;
    if (expandedRowData[containerId]) {
      const containerKeys: ExpandedRowState = expandedRowData[containerId];
      const data = containerKeys?.dataSource?.map(record => (
        { ...record, uid: `${record.Volume}/${record.Bucket}/${record.Key}` }
      ));

      return (
        <ExpandedKeyTable
          loading={loading}
          data={data} paginationConfig={paginationConfig} />
      )
    }
    return <div>Loading...</div>
  }

  function handleLimitChange(selected: ValueType<Option, false>) {
    setSelectedLimit(selected as Option);
  }

  return (<>
    <div className='page-header-v2'>
      OM DB Insights
    </div>
    <div style={{ padding: '24px' }}>
      <div className='content-div'>
        <Tabs defaultActiveKey={activeTab ?? '1'}>
          <Tabs.TabPane key='1' tab={
            <label>
              Container Mismatch Info
              <Tooltip
                placement='right'
                title='Containers which are present in OM and missing in SCM or vice-versa'>
                <InfoCircleOutlined style={{ marginLeft: '10px' }} />
              </Tooltip>
            </label>
          }>
            <ContainerMismatchTable
              limit={selectedLimit}
              paginationConfig={paginationConfig}
              expandedRowRender={expandedRowRender}
              onRowExpand={onRowExpandClick}
              handleLimitChange={handleLimitChange} />
          </Tabs.TabPane>
          <Tabs.TabPane key='2' tab={
            <label>
              Open Keys
              <Tooltip
                placement='right'
                title='Keys which are not yet committed but currently being written'>
                <InfoCircleOutlined style={{ marginLeft: '10px' }} />
              </Tooltip>
            </label>
          }>
            <OpenKeysTable
              limit={selectedLimit}
              paginationConfig={paginationConfig}
              handleLimitChange={handleLimitChange} />
          </Tabs.TabPane>
          <Tabs.TabPane key='3' tab={
            <label>
              Keys Pending for Deletion
              <Tooltip
                placement='right'
                title='Keys that are pending for deletion'>
                <InfoCircleOutlined style={{ marginLeft: '10px' }} />
              </Tooltip>
            </label>
          }>
            <DeletePendingKeysTable
              limit={selectedLimit}
              paginationConfig={paginationConfig}
              handleLimitChange={handleLimitChange} />
          </Tabs.TabPane>
          <Tabs.TabPane key='4' tab={
            <label>
              Deleted Container Keys
              <Tooltip
                placement='right'
                title='Keys mapped to Containers in DELETED state SCM.'>
                <InfoCircleOutlined style={{ marginLeft: '10px' }} />
              </Tooltip>
            </label>
          }>
            <DeletedContainerKeysTable
              limit={selectedLimit}
              paginationConfig={paginationConfig}
              handleLimitChange={handleLimitChange}
              onRowExpand={onRowExpandClick}
              expandedRowRender={expandedRowRender} />
          </Tabs.TabPane>
          <Tabs.TabPane key='5' tab={
            <label>
              Directories Pending for Deletion
              <Tooltip
                placement='right'
                title='Directories that are pending for deletion.'>
                <InfoCircleOutlined style={{ marginLeft: '10px' }} />
              </Tooltip>
            </label>
          }>
            <DeletePendingDirTable
              limit={selectedLimit}
              paginationConfig={paginationConfig}
              handleLimitChange={handleLimitChange} />
          </Tabs.TabPane>
        </Tabs>
      </div>
    </div>
  </>)
}

export default OMDBInsights;