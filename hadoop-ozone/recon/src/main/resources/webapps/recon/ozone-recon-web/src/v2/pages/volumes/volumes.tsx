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
import { ValueType } from 'react-select/src/types';

import AclPanel from '@/v2/components/aclDrawer/aclDrawer';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import SingleSelect from '@/v2/components/select/singleSelect';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import VolumesTable, { COLUMNS } from '@/v2/components/tables/volumesTable';
import Search from '@/v2/components/search/search';

import { showDataFetchError } from '@/utils/common';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useAutoReload } from '@/v2/hooks/useAutoReload.hook';

import {
  Volume,
  VolumesState,
  VolumesResponse
} from '@/v2/types/volume.types';

import './volumes.less';

const SearchableColumnOpts = [
  {
    label: 'Volume',
    value: 'volume'
  },
  {
    label: 'Owner',
    value: 'owner'
  },
  {
    label: 'Admin',
    value: 'admin'
  }
]

const DEFAULT_VOLUMES_RESPONSE: VolumesResponse = {
  totalCount: 0,
  volumes: []
};

const Volumes: React.FC<{}> = () => {
  const defaultColumns = COLUMNS.map(column => ({
    label: column.title as string,
    value: column.key as string,
  }));

  const [state, setState] = useState<VolumesState>({
    data: [],
    lastUpdated: 0,
    columnOptions: defaultColumns
  });
  const [currentRow, setCurrentRow] = useState<Volume | Record<string, never>>({});
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedLimit, setSelectedLimit] = useState<Option>(LIMIT_OPTIONS[0]);
  const [searchColumn, setSearchColumn] = useState<'volume' | 'owner' | 'admin'>('volume');
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [showPanel, setShowPanel] = useState<boolean>(false);

  const debouncedSearch = useDebounce(searchTerm, 300);

  // Use the modern hooks pattern
  const volumesData = useApiData<VolumesResponse>(
    `/api/v1/volumes?limit=${selectedLimit.value}`,
    DEFAULT_VOLUMES_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  // Process volumes data when it changes
  useEffect(() => {
    if (volumesData.data && volumesData.data.volumes) {
      const volumes: Volume[] = volumesData.data.volumes.map(volume => ({
        volume: volume.volume,
        owner: volume.owner,
        admin: volume.admin,
        creationTime: volume.creationTime,
        modificationTime: volume.modificationTime,
        quotaInBytes: volume.quotaInBytes,
        quotaInNamespace: volume.quotaInNamespace,
        usedNamespace: volume.usedNamespace,
        acls: volume.acls
      }));

      setState({
        ...state,
        data: volumes,
        lastUpdated: Number(moment()),
      });
    }
  }, [volumesData.data]);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleLimitChange(selected: ValueType<Option, false>) {
    setSelectedLimit(selected as Option);
  }

  function handleTagClose(label: string) {
    setSelectedColumns(
      selectedColumns.filter((column) => column.label !== label)
    )
  }

  function handleAclLinkClick(volume: Volume) {
    setCurrentRow(volume);
    setShowPanel(true);
  }

  // Create refresh function for auto-reload
  const loadVolumesData = () => {
    volumesData.refetch();
  };

  const autoReload = useAutoReload(loadVolumesData);

  const {
    data, lastUpdated,
    columnOptions
  } = state;

  return (
    <>
      <div className='page-header-v2'>
        Volumes
        <AutoReloadPanel
          isLoading={volumesData.loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReload.handleAutoReloadToggle}
          onReload={loadVolumesData}
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
                fixedColumn='volume'
                columnLength={COLUMNS.length} />
              <SingleSelect
                options={LIMIT_OPTIONS}
                defaultValue={selectedLimit}
                placeholder='Limit'
                onChange={handleLimitChange} />
            </div>
            <Search
              disabled={data?.length < 1}
              searchOptions={SearchableColumnOpts}
              searchInput={searchTerm}
              searchColumn={searchColumn}
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={(value) => {
                setSearchTerm('');
                setSearchColumn(value as 'volume' | 'owner' | 'admin');
              }} />
          </div>
          <VolumesTable
            loading={volumesData.loading}
            data={data}
            handleAclClick={handleAclLinkClick}
            selectedColumns={selectedColumns}
            searchColumn={searchColumn}
            searchTerm={debouncedSearch} />
        </div>
        <AclPanel
          visible={showPanel}
          acls={currentRow.acls}
          entityName={currentRow.volume}
          entityType='Volume'
          onClose={() => setShowPanel(false)}/>
      </div>
    </>
  );
}

export default Volumes;
