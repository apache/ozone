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
import { useLocation } from 'react-router-dom';

import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import AclPanel from '@/v2/components/aclDrawer/aclDrawer';
import Search from '@/v2/components/search/search';
import MultiSelect from '@/v2/components/select/multiSelect';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import BucketsTable, { COLUMNS } from '@/v2/components/tables/bucketsTable';

import { showDataFetchError } from '@/utils/common';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useAutoReload } from '@/v2/hooks/useAutoReload.hook';

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

const DEFAULT_BUCKET_RESPONSE: BucketResponse = {
  totalCount: 0,
  buckets: []
};

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
  const [state, setState] = useState<BucketsState>({
    totalCount: 0,
    lastUpdated: 0,
    columnOptions: defaultColumns,
    volumeBucketMap: new Map<string, Set<Bucket>>(),
    bucketsUnderVolume: [],
    volumeOptions: [],
  });
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedVolumes, setSelectedVolumes] = useState<Option[]>([]);
  const [selectedLimit, setSelectedLimit] = useState<Option>(LIMIT_OPTIONS[0]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [showPanel, setShowPanel] = useState<boolean>(false);
  const [searchColumn, setSearchColumn] = useState<'name' | 'volumeName'>('name');
  const [currentRow, setCurrentRow] = useState<Bucket | Record<string, never>>({})

  const debouncedSearch = useDebounce(searchTerm, 300);
  const { search } = useLocation();

  // Use the modern hooks pattern
  const bucketsData = useApiData<BucketResponse>(
    `/api/v1/buckets?limit=${selectedLimit.value}`,
    DEFAULT_BUCKET_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  function getVolumeSearchParam() {
    return new URLSearchParams(search).get('volume');
  }

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
  }

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

  // Process buckets data when it changes
  useEffect(() => {
    if (bucketsData.data && bucketsData.data.buckets) {
      const buckets: Bucket[] = bucketsData.data.buckets.map(bucket => ({
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
      }));

      const volumeBucketMap: Map<string, Set<Bucket>> = getVolumeBucketMap(buckets);

      // Set options for volume selection dropdown
      const volumeOptions: Option[] = Array.from(
        volumeBucketMap.keys()
      ).map(k => ({
        label: k,
        value: k
      }));

      setState(prevState => ({
        ...prevState,
        totalCount: bucketsData.data.totalCount,
        volumeBucketMap: volumeBucketMap,
        volumeOptions: volumeOptions,
        lastUpdated: Number(moment())
      }));

      // Set default volumes if none selected
      setSelectedVolumes((prevState) => {
        if (prevState.length === 0) return volumeOptions;
        return prevState;
      });
    }
  }, [bucketsData.data]);

  // Update buckets under volume when volume selection or data changes
  useEffect(() => {
    setState(prevState => ({
      ...prevState,
      bucketsUnderVolume: getFilteredBuckets(
        selectedVolumes,
        prevState.volumeBucketMap
      )
    }));
  }, [selectedVolumes, state.volumeBucketMap]);

  useEffect(() => {
    const initialVolume = getVolumeSearchParam();
    if (initialVolume) {
      setSelectedVolumes([{
        label: initialVolume,
        value: initialVolume
      }]);
    }
  }, []);

  // Create refresh function for auto-reload
  const loadBucketsData = () =>{
    bucketsData.refetch();
  };

  const autoReload = useAutoReload(loadBucketsData);

  const {
    lastUpdated, columnOptions,
    volumeOptions, bucketsUnderVolume
  } = state;

  return (
    <>
      <div className='page-header-v2'>
        Buckets
        <AutoReloadPanel
          isLoading={bucketsData.loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReload.handleAutoReloadToggle}
          onReload={loadBucketsData}
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
            loading={bucketsData.loading}
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
