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

import React, { useRef, useState } from "react";
import moment from "moment";
import { AxiosError } from "axios";
import { Card, Row, Tabs } from "antd";
import { ValueType } from "react-select/src/types";

import Search from "@/v2/components/search/search";
import MultiSelect, { Option } from "@/v2/components/select/multiSelect";
import ContainerTable, { COLUMNS } from "@/v2/components/tables/containersTable";
import AutoReloadPanel from "@/components/autoReloadPanel/autoReloadPanel";
import { showDataFetchError } from "@/utils/common";
import { AutoReloadHelper } from "@/utils/autoReloadHelper";
import { AxiosGetHelper, cancelRequests } from "@/utils/axiosRequestHelper";
import { useDebounce } from "@/v2/hooks/debounce.hook";

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

const Containers: React.FC<{}> = () => {

  const cancelSignal = useRef<AbortController>();

  const [state, setState] = useState<ContainerState>({
    lastUpdated: 0,
    columnOptions: defaultColumns,
    missingContainerData: [],
    underReplicatedContainerData: [],
    overReplicatedContainerData: [],
    misReplicatedContainerData: [],
  });
  const [expandedRow, setExpandedRow] = useState<ExpandedRow>({});

  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [selectedTab, setSelectedTab] = useState<string>('1');
  const [searchColumn, setSearchColumn] = useState<'containerID' | 'pipelineID'>('containerID');

  const debouncedSearch = useDebounce(searchTerm, 300);

  function loadData() {
    setLoading(true);

    const { request, controller } = AxiosGetHelper(
      '/api/v1/containers/unhealthy',
      cancelSignal.current
    );

    cancelSignal.current = controller;

    request.then(response => {
      const containers: Container[] = response.data.containers;

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

      setState({
        ...state,
        missingContainerData: missingContainerData,
        underReplicatedContainerData: underReplicatedContainerData,
        overReplicatedContainerData: overReplicatedContainerData,
        misReplicatedContainerData: misReplicatedContainerData,
        lastUpdated: Number(moment())
      });
      setLoading(false)
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    });
  }

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  const autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  React.useEffect(() => {
    autoReloadHelper.startPolling();
    loadData();

    return (() => {
      autoReloadHelper.stopPolling();
      cancelRequests([cancelSignal.current!])
    })
  }, []);

  const {
    lastUpdated, columnOptions,
    missingContainerData, underReplicatedContainerData,
    overReplicatedContainerData, misReplicatedContainerData
  } = state;

  // Mapping the data to the Tab keys for enabling/disabling search
  const dataToTabKeyMap: Record<string, Container[]> = {
    1: missingContainerData,
    2: underReplicatedContainerData,
    3: overReplicatedContainerData,
    4: misReplicatedContainerData
  }

  const highlightData = (
    <div style={{
        display: 'flex',
        width: '90%',
        justifyContent: 'space-between'
      }}>
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
    </div>
  )

  return (
    <>
      <div className='page-header-v2'>
        Containers
        <AutoReloadPanel
          isLoading={loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReloadHelper.handleAutoReloadToggle}
          onReload={loadData}
        />
      </div>
      <div style={{ padding: '24px' }}>
        <div style={{ marginBottom: '12px' }}>
          <Card
            title='Highlights'
            loading={loading}>
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
          </div>
          <Tabs defaultActiveKey='1'
            onChange={(activeKey: string) => setSelectedTab(activeKey)}>
            <Tabs.TabPane
              key='1'
              tab='Missing'>
              <ContainerTable
                data={missingContainerData}
                loading={loading}
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
                loading={loading}
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
                loading={loading}
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
                loading={loading}
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