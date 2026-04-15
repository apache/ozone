
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

import React, { useState, useCallback } from "react";
import moment from "moment";
import { Button, Card, Row, Tabs, Tooltip, message, Select } from "antd";
import { DownloadOutlined } from "@ant-design/icons";
import { ValueType } from "react-select/src/types";

import Search from "@/v2/components/search/search";
import MultiSelect, { Option } from "@/v2/components/select/multiSelect";
import ContainerTable, { COLUMNS } from "@/v2/components/tables/containersTable";
import AutoReloadPanel from "@/components/autoReloadPanel/autoReloadPanel";
import { showDataFetchError } from "@/utils/common";
import { useDebounce } from "@/v2/hooks/useDebounce";
import { useApiData } from "@/v2/hooks/useAPIData.hook";
import { useAutoReload } from "@/v2/hooks/useAutoReload.hook";
import * as CONSTANTS from '@/v2/constants/overview.constants';

import {
  Container,
  ContainerState,
  ExpandedRow
} from "@/v2/types/container.types";
import { ClusterStateResponse } from "@/v2/types/overview.types";

import './containers.less';

const SearchableColumnOpts = [{
  label: 'Container ID',
  value: 'containerID'
}, {
  label: 'Pipeline ID',
  value: 'pipelineID'
}]

const defaultColumns = COLUMNS.map(column => ({
  label: column.title as string,
  value: column.key as string
}));

const DEFAULT_CONTAINERS_RESPONSE = {
  containers: []
};

const Containers: React.FC<{}> = () => {
  const [state, setState] = useState<ContainerState>({
    lastUpdated: 0,
    totalContainers: 0,
    columnOptions: defaultColumns,
    missingContainerData: [],
    underReplicatedContainerData: [],
    overReplicatedContainerData: [],
    misReplicatedContainerData: [],
    mismatchedReplicaContainerData: []
  });
  const [expandedRow, setExpandedRow] = useState<ExpandedRow>({});
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [selectedTab, setSelectedTab] = useState<string>('1');
  const [searchColumn, setSearchColumn] = useState<'containerID' | 'pipelineID'>('containerID');
  const [exportLimit, setExportLimit] = useState<number>(10000);

  const debouncedSearch = useDebounce(searchTerm, 300);

  // Use the modern hooks pattern
  const containersData = useApiData<{ containers: Container[] }>(
    '/api/v1/containers/unhealthy',
    DEFAULT_CONTAINERS_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  const clusterState = useApiData<ClusterStateResponse>(
    '/api/v1/clusterState',
    CONSTANTS.DEFAULT_CLUSTER_STATE,
    {
      retryAttempts: 2,
      initialFetch: true,
      onError: (error) => showDataFetchError(error)
    }
  );

  // Process containers data when it changes
  React.useEffect(() => {
    if (containersData.data && containersData.data.containers && clusterState.data) {
      const containers: Container[] = containersData.data.containers;

      const missingContainerData: Container[] = containers?.filter(
        container => container.containerState === 'MISSING'
      ) ?? [];
      const underReplicatedContainerData: Container[] = containers?.filter(
        container => container.containerState === 'UNDER_REPLICATED'
      ) ?? [];
      const overReplicatedContainerData: Container[] = containers?.filter(
        container => container.containerState === 'OVER_REPLICATED'
      ) ?? [];
      const misReplicatedContainerData: Container[] = containers?.filter(
        container => container.containerState === 'MIS_REPLICATED'
      ) ?? [];
      const mismatchedReplicaContainerData: Container[] = containers?.filter(
        container => container.containerState === 'MISMATCHED_REPLICA'
      ) ?? [];

      const totalContainers = clusterState.data.containers;

      setState({
        ...state,
        totalContainers,
        missingContainerData,
        underReplicatedContainerData,
        overReplicatedContainerData,
        misReplicatedContainerData,
        mismatchedReplicaContainerData,
        lastUpdated: Number(moment())
      });
    }
  }, [containersData.data, clusterState.data]);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleTagClose(label: string) {
    setSelectedColumns(
      selectedColumns.filter((column) => column.label !== label)
    );
  }

  function handleTabChange(key: string) {
    setSelectedTab(key);
  }

  // Create refresh function for auto-reload
  const loadContainersData = () => {
    containersData.refetch();
    clusterState.refetch();
  };

  const autoReload = useAutoReload(loadContainersData);

  const {
    lastUpdated,
    totalContainers,
    columnOptions,
    missingContainerData,
    underReplicatedContainerData,
    overReplicatedContainerData,
    misReplicatedContainerData,
    mismatchedReplicaContainerData
  } = state;

  // Mapping the data to the Tab keys for enabling/disabling search
  const dataToTabKeyMap: Record<string, Container[]> = {
    1: missingContainerData,
    2: underReplicatedContainerData,
    3: overReplicatedContainerData,
    4: misReplicatedContainerData,
    5: mismatchedReplicaContainerData
  }

  // Mapping tab keys to the backend state parameter for CSV export
  const tabToExportState: Record<string, string> = {
    '1': 'MISSING',
    '2': 'UNDER_REPLICATED',
    '3': 'OVER_REPLICATED',
    '4': 'MIS_REPLICATED',
    '5': 'REPLICA_MISMATCH'
  };

  // Human-readable labels for the export tooltip
  const tabToLabel: Record<string, string> = {
    '1': 'Missing',
    '2': 'Under-Replicated',
    '3': 'Over-Replicated',
    '4': 'Mis-Replicated',
    '5': 'Mismatched Replicas'
  };

  const handleExportCsv = useCallback(() => {
    const state = tabToExportState[selectedTab];
    const exportUrl = `/api/v1/containers/unhealthy/export?state=${state}&limit=${exportLimit}`;
    window.open(exportUrl, '_blank');
    message.success(`Exporting ${tabToLabel[selectedTab]} containers as CSV (Limit: ${exportLimit})`);
  }, [selectedTab, exportLimit]);

  const highlightData = (
    <div style={{
        display: 'flex',
        width: '90%',
        justifyContent: 'space-between'
      }}>
      <div className='highlight-content'>
        Total Containers <br/>
        <span className='highlight-content-value'>{totalContainers ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Missing <br/>
        <span className='highlight-content-value'>{missingContainerData?.length ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Under-Replicated <br/>
        <span className='highlight-content-value'>{underReplicatedContainerData?.length ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Over-Replicated <br/>
        <span className='highlight-content-value'>{overReplicatedContainerData?.length ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Mis-Replicated <br/>
        <span className='highlight-content-value'>{misReplicatedContainerData?.length ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Mismatched Replicas <br/>
        <span className='highlight-content-value'>{mismatchedReplicaContainerData?.length ?? 'N/A'}</span>
      </div>
    </div>
  )

  const getCurrentTabData = () => {
    switch (selectedTab) {
      case '1':
        return missingContainerData;
      case '2':
        return underReplicatedContainerData;
      case '3':
        return overReplicatedContainerData;
      case '4':
        return misReplicatedContainerData;
      case '5':
        return mismatchedReplicaContainerData;
      default:
        return missingContainerData;
    }
  };

  return (
    <>
      <div className='page-header-v2'>
        Containers
        <AutoReloadPanel
          isLoading={containersData.loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReload.handleAutoReloadToggle}
          onReload={loadContainersData}
        />
      </div>
      <div style={{ padding: '24px' }}>
        <div style={{ marginBottom: '12px' }}>
          <Card
            title='Highlights'
            loading={containersData.loading}>
              <Row
                align='middle'>
                  {highlightData}
                </Row>
          </Card>
        </div>
        <div className='content-div'>
          <div className='table-header-section'>
            <div className='table-filter-section'>
              <MultiSelect
                options={columnOptions}
                defaultValue={selectedColumns}
                selected={selectedColumns}
                placeholder='Columns'
                onChange={handleColumnChange}
                fixedColumn='containerID'
                onTagClose={() => { }}
                columnLength={columnOptions.length} />
            </div>
            <div className='table-actions-section'>
              <Search
                disabled={dataToTabKeyMap[selectedTab]?.length < 1}
                searchOptions={SearchableColumnOpts}
                searchInput={searchTerm}
                searchColumn={searchColumn}
                onSearchChange={
                  (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
                }
                onChange={(value) => {
                  setSearchTerm('');
                  setSearchColumn(value as 'containerID' | 'pipelineID');
                }} />
              <Select
                value={exportLimit}
                onChange={(value: number) => setExportLimit(value)}
                style={{ width: 130 }}
                options={[
                  { value: 10000, label: '10K Records' },
                  { value: 100000, label: '100K Records' },
                  { value: 1000000, label: '1M Records' },
                  { value: 0, label: 'Complete' }
                ]}
              />
              <Tooltip title={`Export ${tabToLabel[selectedTab]} containers as CSV`}>
                <Button
                  type='primary'
                  icon={<DownloadOutlined />}
                  onClick={handleExportCsv}
                  className='export-csv-btn'>
                  Export CSV
                </Button>
              </Tooltip>
            </div>
          </div>
          <Tabs defaultActiveKey='1'
            onChange={(activeKey: string) => setSelectedTab(activeKey)}>
            <Tabs.TabPane
              key='1'
              tab='Missing'>
              <ContainerTable
                data={missingContainerData}
                loading={containersData.loading}
                searchColumn={searchColumn}
                searchTerm={debouncedSearch}
                selectedColumns={selectedColumns}
                expandedRow={expandedRow}
                expandedRowSetter={setExpandedRow}
              />
            </Tabs.TabPane>
            <Tabs.TabPane
              key='2'
              tab='Under-Replicated'>
              <ContainerTable
                data={underReplicatedContainerData}
                loading={containersData.loading}
                searchColumn={searchColumn}
                searchTerm={debouncedSearch}
                selectedColumns={selectedColumns}
                expandedRow={expandedRow}
                expandedRowSetter={setExpandedRow}
              />
            </Tabs.TabPane>
            <Tabs.TabPane
              key='3'
              tab='Over-Replicated'>
              <ContainerTable
                data={overReplicatedContainerData}
                loading={containersData.loading}
                searchColumn={searchColumn}
                searchTerm={debouncedSearch}
                selectedColumns={selectedColumns}
                expandedRow={expandedRow}
                expandedRowSetter={setExpandedRow}
              />
            </Tabs.TabPane>
            <Tabs.TabPane
              key='4'
              tab='Mis-Replicated'>
              <ContainerTable
                data={misReplicatedContainerData}
                loading={containersData.loading}
                searchColumn={searchColumn}
                searchTerm={debouncedSearch}
                selectedColumns={selectedColumns}
                expandedRow={expandedRow}
                expandedRowSetter={setExpandedRow}
              />
            </Tabs.TabPane>
            <Tabs.TabPane
              key='5'
              tab='Mismatched Replicas'>
              <ContainerTable
                data={mismatchedReplicaContainerData}
                loading={containersData.loading}
                searchColumn={searchColumn}
                searchTerm={debouncedSearch}
                selectedColumns={selectedColumns}
                expandedRow={expandedRow}
                expandedRowSetter={setExpandedRow}
              />
            </Tabs.TabPane>
          </Tabs>
        </div>
      </div>
    </>
  );
}

export default Containers;
