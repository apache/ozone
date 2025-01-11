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

import React, { useEffect, useRef, useState } from 'react';
import moment from 'moment';
import { ValueType } from 'react-select';
import { useLocation } from 'react-router-dom';

import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import AclPanel from '@/v2/components/aclDrawer/aclDrawer';
import Search from '@/v2/components/search/search';
import MultiSelect from '@/v2/components/select/multiSelect';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import BucketsTable, { COLUMNS } from '@/v2/components/tables/bucketsTable';

import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { AxiosGetHelper, cancelRequests } from "@/utils/axiosRequestHelper";
import { showDataFetchError } from '@/utils/common';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';
import { useDebounce } from '@/v2/hooks/debounce.hook';

import {
  Bucket,
  BucketResponse,
  BucketsState,
} from '@/v2/types/bucket.types';

import './buckets.less';

const SearchableColumnOpts = [{
  label: 'Bucket',
  value: 'name'
}, {
  label: 'Volume',
  value: 'volumeName'
}]

const defaultColumns = COLUMNS.map(column => ({
  label: column.title as string,
  value: column.key as string
}));

function getVolumeBucketMap(data: Bucket[]) {
  const volumeBucketMap = data.reduce((
    map: Map<string, Set<Bucket>>,
    currentBucket
  ) => {
    const volume = currentBucket.volumeName;
    if (map.has(volume)) {
      const buckets = Array.from(map.get(volume)!);
      map.set(volume, new Set([...buckets, currentBucket]));
    } else {
      map.set(volume, new Set<Bucket>().add(currentBucket));
    }
    return map;
  }, new Map<string, Set<Bucket>>());
  return volumeBucketMap;
}

function getFilteredBuckets(
  selectedVolumes: Option[],
  bucketsMap: Map<string, Set<Bucket>>
) {
  let selectedBuckets: Bucket[] = [];
  selectedVolumes.forEach(selectedVolume => {
    if (bucketsMap.has(selectedVolume.value)
      && bucketsMap.get(selectedVolume.value)) {
      selectedBuckets = [
        ...selectedBuckets,
        ...Array.from(bucketsMap.get(selectedVolume.value)!)
      ];
    }
  });

  return selectedBuckets;
}

const Buckets: React.FC<{}> = () => {

  const cancelSignal = useRef<AbortController>();

  const [state, setState] = useState<BucketsState>({
    totalCount: 0,
    lastUpdated: 0,
    columnOptions: defaultColumns,
    volumeBucketMap: new Map<string, Set<Bucket>>(),
    bucketsUnderVolume: [],
    volumeOptions: [],
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedVolumes, setSelectedVolumes] = useState<Option[]>([]);
  const [selectedLimit, setSelectedLimit] = useState<Option>(LIMIT_OPTIONS[0]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [showPanel, setShowPanel] = useState<boolean>(false);
  const [searchColumn, setSearchColumn] = useState<'name' | 'volumeName'>('name');
  const [currentRow, setCurrentRow] = useState<Bucket | Record<string, never>>({})

  const debouncedSearch = useDebounce(searchTerm, 300);
  const { search } = useLocation();

  function getVolumeSearchParam() {
    return new URLSearchParams(search).get('volume');
  };

  function handleVolumeChange(selected: ValueType<Option, true>) {
    const { volumeBucketMap } = state;
    const volumeSelections = (selected as Option[]);
    let selectedBuckets: Bucket[] = [];

    if (volumeSelections?.length > 0) {
      selectedBuckets = getFilteredBuckets(volumeSelections, volumeBucketMap)
    }

    setSelectedVolumes(volumeSelections);
    setState({
      ...state,
      bucketsUnderVolume: selectedBuckets
    });
  };

  function handleAclLinkClick(bucket: Bucket) {
    setCurrentRow(bucket);
    setShowPanel(true);
  }

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleLimitChange(selected: ValueType<Option, false>) {
    setSelectedLimit(selected as Option);
  }

  const loadData = () => {
    setLoading(true);
    const { request, controller } = AxiosGetHelper(
      '/api/v1/buckets',
      cancelSignal.current,
      '',
      { limit: selectedLimit.value }
    );
    cancelSignal.current = controller;
    request.then(response => {
      const bucketsResponse: BucketResponse = response.data;
      const totalCount = bucketsResponse.totalCount;
      const buckets: Bucket[] = bucketsResponse.buckets;

      const dataSource: Bucket[] = buckets?.map(bucket => {
        return {
          volumeName: bucket.volumeName,
          name: bucket.name,
          versioning: bucket.versioning,
          storageType: bucket.storageType,
          bucketLayout: bucket.bucketLayout,
          creationTime: bucket.creationTime,
          modificationTime: bucket.modificationTime,
          sourceVolume: bucket.sourceVolume,
          sourceBucket: bucket.sourceBucket,
          usedBytes: bucket.usedBytes,
          usedNamespace: bucket.usedNamespace,
          quotaInBytes: bucket.quotaInBytes,
          quotaInNamespace: bucket.quotaInNamespace,
          owner: bucket.owner,
          acls: bucket.acls
        };
      }) ?? [];

      const volumeBucketMap: Map<string, Set<Bucket>> = getVolumeBucketMap(dataSource);

      // Set options for volume selection dropdown
      const volumeOptions: Option[] = Array.from(
        volumeBucketMap.keys()
      ).map(k => ({
        label: k,
        value: k
      }));

      setLoading(false);

      setSelectedVolumes((prevState) => {
        if (prevState.length === 0) return volumeOptions;
        return prevState;
      });

      setState({
        ...state,
        totalCount: totalCount,
        volumeBucketMap: volumeBucketMap,
        volumeOptions: volumeOptions,
        lastUpdated: Number(moment())
      });
    }).catch(error => {
      setLoading(false);
      showDataFetchError(error.toString());
    });
  }

  const autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  useEffect(() => {
    autoReloadHelper.startPolling();
    const initialVolume = getVolumeSearchParam();
    if (initialVolume) {
      setSelectedVolumes([{
        label: initialVolume,
        value: initialVolume
      }]);
    }
    loadData();

    return (() => {
      autoReloadHelper.stopPolling();
      cancelRequests([cancelSignal.current!]);
    })
  }, []);

  useEffect(() => {
    // If the data is fetched, we need to regenerate the columns
    // To make sure the filters are properly applied
    setState({
      ...state,
      bucketsUnderVolume: getFilteredBuckets(
        selectedVolumes,
        state.volumeBucketMap
      )
    });
  }, [state.volumeBucketMap])

  // If limit changes, load new data
  useEffect(() => {
    loadData();
  }, [selectedLimit.value]);

  const {
    lastUpdated, columnOptions,
    volumeOptions, bucketsUnderVolume
  } = state;

  return (
    <>
      <div className='page-header-v2'>
        Buckets
        <AutoReloadPanel
          isLoading={loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReloadHelper.handleAutoReloadToggle}
          onReload={loadData}
        />
      </div>
      <div className='data-container'>
        <div className='content-div'>
          <div className='table-header-section'>
            <div className='table-filter-section'>
              <MultiSelect
                options={volumeOptions}
                defaultValue={selectedVolumes}
                selected={selectedVolumes}
                placeholder='Volumes'
                onChange={handleVolumeChange}
                fixedColumn=''
                onTagClose={() => { }}
                columnLength={volumeOptions.length} />
              <MultiSelect
                options={columnOptions}
                defaultValue={selectedColumns}
                selected={selectedColumns}
                placeholder='Columns'
                onChange={handleColumnChange}
                onTagClose={() => { }}
                fixedColumn='name'
                columnLength={COLUMNS.length} />
              <SingleSelect
                options={LIMIT_OPTIONS}
                defaultValue={selectedLimit}
                placeholder='Limit'
                onChange={handleLimitChange} />
            </div>
            <Search
              disabled={bucketsUnderVolume?.length < 1}
              searchOptions={SearchableColumnOpts}
              searchInput={searchTerm}
              searchColumn={searchColumn}
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={(value) => {
                setSearchTerm('');
                setSearchColumn(value as 'name' | 'volumeName');
              }} />
          </div>
          <BucketsTable
            loading={loading}
            data={bucketsUnderVolume}
            handleAclClick={handleAclLinkClick}
            selectedColumns={selectedColumns}
            searchColumn={searchColumn}
            searchTerm={debouncedSearch} />
        </div>
        <AclPanel
          visible={showPanel}
          acls={currentRow.acls}
          entityName={currentRow.name}
          entityType='Bucket'
          onClose={() => setShowPanel(false)} />
      </div>
    </>
  )
}

export default Buckets;