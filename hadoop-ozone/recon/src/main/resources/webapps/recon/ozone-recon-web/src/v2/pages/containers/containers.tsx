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
import { Card, Row, Tabs } from "antd";
import { ValueType } from "react-select/src/types";

import Search from "@/v2/components/search/search";
import MultiSelect, { Option } from "@/v2/components/select/multiSelect";
import ContainerTable, { COLUMNS } from "@/v2/components/tables/containersTable";
import AutoReloadPanel from "@/components/autoReloadPanel/autoReloadPanel";
import { showDataFetchError } from "@/utils/common";
import { useDebounce } from "@/v2/hooks/useDebounce";
import { useApiData } from "@/v2/hooks/useAPIData.hook";
import { useAutoReload } from "@/v2/hooks/useAutoReload.hook";

import {
  Container,
  ContainerState,
  ExpandedRow
} from "@/v2/types/container.types";

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

  // Process containers data when it changes
  React.useEffect(() => {
    if (containersData.data && containersData.data.containers) {
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

      setState({
        ...state,
        missingContainerData,
        underReplicatedContainerData,
        overReplicatedContainerData,
        misReplicatedContainerData,
        mismatchedReplicaContainerData,
        lastUpdated: Number(moment())
      });
    }
  }, [containersData.data]);

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
  };

  const autoReload = useAutoReload(loadContainersData);

  const {
    lastUpdated,
    columnOptions,
    missingContainerData,
    underReplicatedContainerData,
    overReplicatedContainerData,
    misReplicatedContainerData,
    mismatchedReplicaContainerData
  } = state;

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
      <div className='data-container'>
        <div className='content-div'>
          <div className='table-header-section'>
            <div className='table-filter-section'>
              <MultiSelect
                options={columnOptions}
                defaultValue={selectedColumns}
                selected={selectedColumns}
                placeholder='Columns'
                onChange={handleColumnChange}
                onTagClose={handleTagClose}
                fixedColumn='containerID'
                columnLength={COLUMNS.length} />
            </div>
            <Search
              disabled={getCurrentTabData()?.length < 1}
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
          </div>
          <Card>
            <Tabs activeKey={selectedTab} onChange={handleTabChange}>
              <Tabs.TabPane tab={`Missing (${missingContainerData.length})`} key="1">
                <ContainerTable
                  loading={containersData.loading}
                  data={missingContainerData}
                  searchColumn={searchColumn}
                  searchTerm={debouncedSearch}
                  selectedColumns={selectedColumns}
                  expandedRow={expandedRow}
                  expandedRowSetter={setExpandedRow} />
              </Tabs.TabPane>
              <Tabs.TabPane tab={`Under Replicated (${underReplicatedContainerData.length})`} key="2">
                <ContainerTable
                  loading={containersData.loading}
                  data={underReplicatedContainerData}
                  searchColumn={searchColumn}
                  searchTerm={debouncedSearch}
                  selectedColumns={selectedColumns}
                  expandedRow={expandedRow}
                  expandedRowSetter={setExpandedRow} />
              </Tabs.TabPane>
              <Tabs.TabPane tab={`Over Replicated (${overReplicatedContainerData.length})`} key="3">
                <ContainerTable
                  loading={containersData.loading}
                  data={overReplicatedContainerData}
                  searchColumn={searchColumn}
                  searchTerm={debouncedSearch}
                  selectedColumns={selectedColumns}
                  expandedRow={expandedRow}
                  expandedRowSetter={setExpandedRow} />
              </Tabs.TabPane>
              <Tabs.TabPane tab={`Mis Replicated (${misReplicatedContainerData.length})`} key="4">
                <ContainerTable
                  loading={containersData.loading}
                  data={misReplicatedContainerData}
                  searchColumn={searchColumn}
                  searchTerm={debouncedSearch}
                  selectedColumns={selectedColumns}
                  expandedRow={expandedRow}
                  expandedRowSetter={setExpandedRow} />
              </Tabs.TabPane>
              <Tabs.TabPane tab={`Mismatched Replica (${mismatchedReplicaContainerData.length})`} key="5">
                <ContainerTable
                  loading={containersData.loading}
                  data={mismatchedReplicaContainerData}
                  searchColumn={searchColumn}
                  searchTerm={debouncedSearch}
                  selectedColumns={selectedColumns}
                  expandedRow={expandedRow}
                  expandedRowSetter={setExpandedRow} />
              </Tabs.TabPane>
            </Tabs>
          </Card>
        </div>
      </div>
    </>
  );
}

export default Containers;
