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

import React, { useState, useEffect } from "react";
import moment from "moment";
import { Card, Row, Tabs } from "antd";
import { ValueType } from "react-select/src/types";

import Search from "@/v2/components/search/search";
import MultiSelect, { Option } from "@/v2/components/select/multiSelect";
import ContainerTable, { COLUMNS } from "@/v2/components/tables/containersTable";
import AutoReloadPanel from "@/components/autoReloadPanel/autoReloadPanel";
import { showDataFetchError } from "@/utils/common";
import { useDebounce } from "@/v2/hooks/useDebounce";
import { fetchData, useApiData } from "@/v2/hooks/useAPIData.hook";
import { useAutoReload } from "@/v2/hooks/useAutoReload.hook";
import * as CONSTANTS from '@/v2/constants/overview.constants';

import {
  ContainersPaginationResponse,
  ContainerState,
  ExpandedRow,
  TabPaginationState,
} from "@/v2/types/container.types";
import { ClusterStateResponse } from "@/v2/types/overview.types";

import './containers.less';

const DEFAULT_PAGE_SIZE = 10;
export const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

const TAB_STATE_MAP: Record<string, string> = {
  '1': 'MISSING',
  '2': 'UNDER_REPLICATED',
  '3': 'OVER_REPLICATED',
  '4': 'MIS_REPLICATED',
  '5': 'REPLICA_MISMATCH',
};

const SearchableColumnOpts = [{
  label: 'Container ID',
  value: 'containerID'
}, {
  label: 'Pipeline ID',
  value: 'pipelineID'
}];

const defaultColumns = COLUMNS.map(column => ({
  label: column.title as string,
  value: column.key as string
}));

const DEFAULT_TAB_STATE: TabPaginationState = {
  data: [],
  loading: false,
  firstKey: 0,
  lastKey: 0,
  currentMinContainerId: 0,
  pageHistory: [],
  hasNextPage: false,
};

const Containers: React.FC<{}> = () => {
  const [state, setState] = useState<ContainerState>({
    lastUpdated: 0,
    totalContainers: 0,
    columnOptions: defaultColumns,
    missingCount: 0,
    underReplicatedCount: 0,
    overReplicatedCount: 0,
    misReplicatedCount: 0,
    replicaMismatchCount: 0,
  });
  const [pageSize, setPageSize] = useState<number>(DEFAULT_PAGE_SIZE);
  const [tabStates, setTabStates] = useState<Record<string, TabPaginationState>>({
    '1': { ...DEFAULT_TAB_STATE },
    '2': { ...DEFAULT_TAB_STATE },
    '3': { ...DEFAULT_TAB_STATE },
    '4': { ...DEFAULT_TAB_STATE },
    '5': { ...DEFAULT_TAB_STATE },
  });
  const [expandedRow, setExpandedRow] = useState<ExpandedRow>({});
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [selectedTab, setSelectedTab] = useState<string>('1');
  const [searchColumn, setSearchColumn] = useState<'containerID' | 'pipelineID'>('containerID');

  const debouncedSearch = useDebounce(searchTerm, 300);

  const clusterState = useApiData<ClusterStateResponse>(
    '/api/v1/clusterState',
    CONSTANTS.DEFAULT_CLUSTER_STATE,
    {
      retryAttempts: 2,
      initialFetch: true,
      onError: (error) => showDataFetchError(error)
    }
  );

  useEffect(() => {
    if (clusterState.data) {
      setState(prev => ({
        ...prev,
        totalContainers: clusterState.data.containers,
      }));
    }
  }, [clusterState.data]);

  // Fetch a single page for a tab using cursor-based pagination.
  // minContainerId=0 means "start from the beginning".
  // currentPageSize is passed explicitly so callers (e.g. size-change handler) can
  // provide the new value before React state has updated.
  const fetchTabData = async (
    tabKey: string,
    minContainerId: number,
    currentPageSize: number
  ) => {
    const containerStateName = TAB_STATE_MAP[tabKey];
    // Fetch one extra item so we can detect a next page without a separate count request.
    const fetchSize = currentPageSize + 1;

    setTabStates(prev => ({
      ...prev,
      [tabKey]: { ...prev[tabKey], loading: true },
    }));

    try {
      const response = await fetchData<ContainersPaginationResponse>(
        `/api/v1/containers/unhealthy/${containerStateName}`,
        'GET',
        { limit: fetchSize, minContainerId }
      );

      const allContainers = response.containers ?? [];
      // If we received more than currentPageSize items, a next page exists.
      const hasNextPage = allContainers.length > currentPageSize;
      // Always display at most currentPageSize rows.
      const containers = allContainers.slice(0, currentPageSize);
      // Derive cursor keys from the visible slice, not the full response,
      // so the next-page request starts exactly after the last displayed row.
      const lastKey = containers.length > 0
        ? Math.max(...containers.map(c => c.containerID))
        : 0;
      const firstKey = containers.length > 0
        ? Math.min(...containers.map(c => c.containerID))
        : 0;

      setTabStates(prev => ({
        ...prev,
        [tabKey]: {
          ...prev[tabKey],
          data: containers,
          loading: false,
          firstKey,
          lastKey,
          currentMinContainerId: minContainerId,
          hasNextPage,
        },
      }));

      // Summary counts are returned by every tab endpoint.
      setState(prev => ({
        ...prev,
        missingCount: response.missingCount ?? 0,
        underReplicatedCount: response.underReplicatedCount ?? 0,
        overReplicatedCount: response.overReplicatedCount ?? 0,
        misReplicatedCount: response.misReplicatedCount ?? 0,
        replicaMismatchCount: response.replicaMismatchCount ?? 0,
        lastUpdated: Number(moment()),
      }));
    } catch (error) {
      setTabStates(prev => ({
        ...prev,
        [tabKey]: { ...prev[tabKey], loading: false },
      }));
      showDataFetchError(error);
    }
  };

  // Initial fetch on mount.
  useEffect(() => {
    fetchTabData('1', 0, DEFAULT_PAGE_SIZE);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleTabChange(key: string) {
    setSelectedTab(key);
    // Lazy-load: fetch first page only if the tab has never been loaded.
    if (tabStates[key].data.length === 0 && !tabStates[key].loading) {
      fetchTabData(key, 0, pageSize);
    }
  }

  function handleNextPage(tabKey: string) {
    const tab = tabStates[tabKey];
    if (tab.loading || !tab.hasNextPage) return;

    // Push the current minContainerId so we can navigate back.
    setTabStates(prev => ({
      ...prev,
      [tabKey]: {
        ...prev[tabKey],
        pageHistory: [...prev[tabKey].pageHistory, tab.currentMinContainerId],
      },
    }));
    fetchTabData(tabKey, tab.lastKey, pageSize);
  }

  function handlePrevPage(tabKey: string) {
    const tab = tabStates[tabKey];
    if (tab.loading || tab.pageHistory.length === 0) return;

    const history = [...tab.pageHistory];
    const prevMinContainerId = history.pop() ?? 0;

    setTabStates(prev => ({
      ...prev,
      [tabKey]: { ...prev[tabKey], pageHistory: history },
    }));
    fetchTabData(tabKey, prevMinContainerId, pageSize);
  }

  // Changing page size resets all tabs and re-fetches the active tab from page 1.
  function handlePageSizeChange(newSize: number) {
    setPageSize(newSize);
    const reset = {
      '1': { ...DEFAULT_TAB_STATE },
      '2': { ...DEFAULT_TAB_STATE },
      '3': { ...DEFAULT_TAB_STATE },
      '4': { ...DEFAULT_TAB_STATE },
      '5': { ...DEFAULT_TAB_STATE },
    };
    setTabStates(reset);
    fetchTabData(selectedTab, 0, newSize);
  }

  // Full refresh: reset all tab states and re-fetch the active tab from page 1.
  const loadContainersData = () => {
    setTabStates({
      '1': { ...DEFAULT_TAB_STATE },
      '2': { ...DEFAULT_TAB_STATE },
      '3': { ...DEFAULT_TAB_STATE },
      '4': { ...DEFAULT_TAB_STATE },
      '5': { ...DEFAULT_TAB_STATE },
    });
    fetchTabData(selectedTab, 0, pageSize);
    clusterState.refetch();
  };

  const autoReload = useAutoReload(loadContainersData);

  const {
    lastUpdated,
    totalContainers,
    columnOptions,
    missingCount,
    underReplicatedCount,
    overReplicatedCount,
    misReplicatedCount,
    replicaMismatchCount,
  } = state;

  const currentTabState = tabStates[selectedTab];

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
        <span className='highlight-content-value'>{missingCount ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Under-Replicated <br/>
        <span className='highlight-content-value'>{underReplicatedCount ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Over-Replicated <br/>
        <span className='highlight-content-value'>{overReplicatedCount ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Mis-Replicated <br/>
        <span className='highlight-content-value'>{misReplicatedCount ?? 'N/A'}</span>
      </div>
      <div className='highlight-content'>
        Mismatched Replicas <br/>
        <span className='highlight-content-value'>{replicaMismatchCount ?? 'N/A'}</span>
      </div>
    </div>
  );

  return (
    <>
      <div className='page-header-v2'>
        Containers
        <AutoReloadPanel
          isLoading={currentTabState.loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReload.handleAutoReloadToggle}
          onReload={loadContainersData}
        />
      </div>
      <div style={{ padding: '24px' }}>
        <div style={{ marginBottom: '12px' }}>
          <Card
            title='Highlights'
            loading={currentTabState.loading && missingCount === 0}>
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
            <Search
              disabled={currentTabState.data.length === 0}
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
          <Tabs defaultActiveKey='1'
            onChange={(activeKey: string) => handleTabChange(activeKey)}>
            {(['1','2','3','4','5'] as const).map((key) => (
              <Tabs.TabPane
                key={key}
                tab={['Missing','Under-Replicated','Over-Replicated','Mis-Replicated','Mismatched Replicas'][Number(key)-1]}>
                <ContainerTable
                  data={tabStates[key].data}
                  loading={tabStates[key].loading}
                  searchColumn={searchColumn}
                  searchTerm={debouncedSearch}
                  selectedColumns={selectedColumns}
                  expandedRow={expandedRow}
                  expandedRowSetter={setExpandedRow}
                  onNextPage={() => handleNextPage(key)}
                  onPrevPage={() => handlePrevPage(key)}
                  hasNextPage={tabStates[key].hasNextPage}
                  hasPrevPage={tabStates[key].pageHistory.length > 0}
                  pageSize={pageSize}
                  onPageSizeChange={handlePageSizeChange}
                />
              </Tabs.TabPane>
            ))}
          </Tabs>
        </div>
      </div>
    </>
  );
}

export default Containers;
