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

import React, { useEffect, useState, useCallback } from 'react';
import moment from 'moment';
import { ValueType } from 'react-select';

import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import Search from '@/v2/components/search/search';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import PipelinesTable, { COLUMNS } from '@/v2/components/tables/pipelinesTable';
import { showDataFetchError } from '@/utils/common';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useAutoReload } from '@/v2/hooks/useAutoReload.hook';

import {
  Pipeline,
  PipelinesResponse,
  PipelinesState
} from '@/v2/types/pipelines.types';

import './pipelines.less';

const defaultColumns = COLUMNS.map(column => ({
  label: (typeof column.title === 'string')
    ? column.title
    : (column.title as Function)().props.children[0],
  value: column.key as string,
}));

const DEFAULT_PIPELINES_RESPONSE: PipelinesResponse = {
  totalCount: 0,
  pipelines: []
};

const Pipelines: React.FC<{}> = () => {
  const [state, setState] = useState<PipelinesState>({
    activeDataSource: [],
    columnOptions: defaultColumns,
    lastUpdated: 0,
  });
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [searchTerm, setSearchTerm] = useState<string>('');

  const debouncedSearch = useDebounce(searchTerm, 300);

  // Use the modern hooks pattern
  const pipelinesData = useApiData<PipelinesResponse>(
    '/api/v1/pipelines',
    DEFAULT_PIPELINES_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  // Process pipelines data when it changes
  useEffect(() => {
    if (pipelinesData.data && pipelinesData.data.pipelines) {
      const pipelines: Pipeline[] = pipelinesData.data.pipelines;
      setState({
        ...state,
        activeDataSource: pipelines,
        lastUpdated: Number(moment())
      });
    }
  }, [pipelinesData.data]);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleTagClose(label: string) {
    setSelectedColumns(
      selectedColumns.filter((column) => column.label !== label)
    );
  }

  // Create refresh function for auto-reload
  const loadPipelinesData = () => {
    pipelinesData.refetch();
  };

  const autoReload = useAutoReload(loadPipelinesData);

  const { activeDataSource, lastUpdated, columnOptions } = state;

  return (
    <>
      <div className='page-header-v2'>
        Pipelines
        <AutoReloadPanel
          isLoading={pipelinesData.loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReload.handleAutoReloadToggle}
          onReload={loadPipelinesData}
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
                fixedColumn='pipelineId'
                columnLength={COLUMNS.length} />
            </div>
            <Search
              disabled={activeDataSource?.length < 1}
              searchOptions={[{
                label: 'Pipeline ID',
                value: 'pipelineId'
              }]}
              searchInput={searchTerm}
              searchColumn='pipelineId'
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={() => setSearchTerm('')} />
          </div>
          <PipelinesTable
            loading={pipelinesData.loading}
            data={activeDataSource}
            selectedColumns={selectedColumns}
            searchTerm={debouncedSearch} />
        </div>
      </div>
    </>
  );
}

export default Pipelines;
