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

import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { Button, Modal, Tag, Tooltip, message } from 'antd';
import Table, {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import {
  ExclamationCircleOutlined,
  LoadingOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import { byteToSize, showDataFetchError } from '@/utils/common';
import { getFormattedTime } from '@/v2/utils/momentUtils';
import { fetchData, useApiData } from '@/v2/hooks/useAPIData.hook';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import {
  BucketPendingDeletion,
  SnapshotTrappedBytesJob
} from '@/v2/types/insights.types';

//-----Types------
type BucketPendingDeletionTableProps = {
  paginationConfig: TablePaginationConfig
  limit: Option;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
}

// Latest snapshot trapped bytes job of each bucket, keyed by volume/bucket
type JobsByBucket = Record<string, SnapshotTrappedBytesJob>;

type SnapshotTrappedBytesResult = NonNullable<SnapshotTrappedBytesJob['result']>;

//-----Constants------
const SNAPSHOT_TRAPPED_BYTES_API = '/api/v1/snapshotTrappedBytes';
const POLL_INTERVAL_MS = 3000;

//-----Helpers------
function bucketKey(record: { volumeName: string, bucketName: string }): string {
  return `${record.volumeName}/${record.bucketName}`;
}

function isActive(job?: SnapshotTrappedBytesJob): boolean {
  return job?.status === 'QUEUED' || job?.status === 'RUNNING';
}

function getErrorMessage(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const data: any = error.response?.data;
    return data?.message ?? data?.error ?? error.message;
  }
  return String(error);
}

//-----Components------
const BucketPendingDeletionTable: React.FC<BucketPendingDeletionTableProps> = ({
  limit,
  paginationConfig,
  handleLimitChange
}) => {
  const [data, setData] = useState<BucketPendingDeletion[]>([]);
  const [jobs, setJobs] = useState<JobsByBucket>({});
  const [searchTerm, setSearchTerm] = useState<string>('');

  const jobsRef = useRef<JobsByBucket>(jobs);
  jobsRef.current = jobs;
  const mountedRef = useRef<boolean>(true);

  const debouncedSearch = useDebounce(searchTerm, 300);

  const bucketPendingDeletionData = useApiData<BucketPendingDeletion[]>(
    `/api/v1/keys/deletePending/buckets?limit=${limit.value}`,
    [],
    {
      retryAttempts: 2,
      onError: (error) => showDataFetchError(error)
    }
  );

  useEffect(() => {
    if (bucketPendingDeletionData.data) {
      setData(bucketPendingDeletionData.data);
    }
  }, [bucketPendingDeletionData.data]);

  // Restore results of recent calculations, so they are not lost on page reload
  useEffect(() => {
    mountedRef.current = true;
    loadRecentJobs();
    return () => {
      mountedRef.current = false;
    };
  }, []);

  // Poll the queued and running jobs until they finish
  const hasActiveJobs = Object.values(jobs).some(isActive);
  useEffect(() => {
    if (!hasActiveJobs) return;
    const timer = setInterval(pollActiveJobs, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [hasActiveJobs]);

  async function loadRecentJobs() {
    let recentJobs: SnapshotTrappedBytesJob[];
    try {
      recentJobs = await fetchData<SnapshotTrappedBytesJob[]>(SNAPSHOT_TRAPPED_BYTES_API);
    } catch {
      // Not fatal: the user can still request the calculation
      return;
    }
    if (!mountedRef.current) return;
    const latest: JobsByBucket = {};
    (recentJobs ?? []).forEach(job => {
      const key = bucketKey(job);
      if (!latest[key] || latest[key].submittedAt < job.submittedAt) {
        latest[key] = job;
      }
    });
    // A job submitted while this request was in flight takes precedence
    setJobs(prev => ({
      ...latest,
      ...prev
    }));
  }

  async function pollJob(job: SnapshotTrappedBytesJob): Promise<SnapshotTrappedBytesJob> {
    try {
      return await fetchData<SnapshotTrappedBytesJob>(`${SNAPSHOT_TRAPPED_BYTES_API}/${job.jobId}`);
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        // Recon restarted or the job expired
        return {
          ...job,
          status: 'FAILED',
          errorMessage: 'The calculation is no longer tracked by Recon'
        };
      }
      // Transient error, keep polling
      return job;
    }
  }

  async function pollActiveJobs() {
    const activeJobs = Object.values(jobsRef.current).filter(isActive);
    const polledJobs = await Promise.all(activeJobs.map(pollJob));
    if (!mountedRef.current) return;
    setJobs(prev => {
      const next = { ...prev };
      polledJobs.forEach(job => {
        const key = bucketKey(job);
        // Ignore the response if a newer job was submitted for the bucket meanwhile
        if (next[key]?.jobId === job.jobId) {
          next[key] = job;
        }
      });
      return next;
    });
  }

  async function submitJob(record: BucketPendingDeletion) {
    const params = new URLSearchParams({
      volume: record.volumeName,
      bucket: record.bucketName
    });
    try {
      const job = await fetchData<SnapshotTrappedBytesJob>(`${SNAPSHOT_TRAPPED_BYTES_API}?${params}`, 'POST');
      if (mountedRef.current) {
        setJobs(prev => ({
          ...prev,
          [bucketKey(job)]: job
        }));
      }
    } catch (error) {
      message.error({
        content: `Failed to start snapshot trapped bytes calculation for ${bucketKey(record)}: ${getErrorMessage(error)}`,
        duration: 6
      });
    }
  }

  function confirmCalculation(record: BucketPendingDeletion) {
    Modal.confirm({
      title: `Calculate snapshot trapped bytes for ${bucketKey(record)}?`,
      icon: <ExclamationCircleOutlined />,
      content: (
        <>
          <p>
            This is an expensive operation. OM scans the deleted keys and directories of this bucket
            in the active OM DB and in every snapshot of the bucket, which adds load on OM and may take a long time
            for buckets with many deleted entries or snapshots.
          </p>
          <p>The calculation runs in the background, the result is shown here once it is ready.</p>
        </>
      ),
      okText: 'Calculate',
      onOk: () => { submitJob(record); }
    });
  }

  function renderSnapshotTrappedBytes(record: BucketPendingDeletion) {
    const job = jobs[bucketKey(record)];
    const calculateButton = (label: string) => (
      <Button size='small' onClick={() => confirmCalculation(record)}>{label}</Button>
    );

    if (!job) {
      return calculateButton('Calculate');
    }
    if (isActive(job)) {
      return (
        <Tag icon={<LoadingOutlined />} color='processing'>
          {job.status === 'QUEUED' ? 'Queued' : 'Calculating'}
        </Tag>
      );
    }
    if (job.status === 'FAILED' || !job.result) {
      return (
        <>
          <Tooltip title={job.errorMessage ?? 'Unknown error'}>
            <Tag color='error'>Failed</Tag>
          </Tooltip>
          {calculateButton('Retry')}
        </>
      );
    }
    return (
      <>
        <Tooltip title={`Calculated by OM at ${getFormattedTime(job.completedAt, 'll LTS')}`}>
          <span data-testid={`snapshot-trapped-bytes-${bucketKey(record)}`}>
            {byteToSize(job.result.snapshotTrappedBytes, 1)}
          </span>
        </Tooltip>
        <Tooltip title='Recalculate'>
          <Button
            type='link'
            size='small'
            icon={<ReloadOutlined />}
            aria-label='Recalculate'
            onClick={() => confirmCalculation(record)} />
        </Tooltip>
      </>
    );
  }

  const columns: ColumnsType<BucketPendingDeletion> = [{
    title: 'Volume Name',
    dataIndex: 'volumeName',
    key: 'volumeName',
    sorter: (a: BucketPendingDeletion, b: BucketPendingDeletion) => a.volumeName.localeCompare(b.volumeName)
  },
  {
    title: 'Bucket Name',
    dataIndex: 'bucketName',
    key: 'bucketName',
    sorter: (a: BucketPendingDeletion, b: BucketPendingDeletion) => a.bucketName.localeCompare(b.bucketName)
  },
  {
    title: (
      <Tooltip title='Replicated size of the keys and directories pending deletion in the active OM DB'>
        Total Pending Deletion
      </Tooltip>
    ),
    dataIndex: 'totalPendingDeletionBytes',
    key: 'totalPendingDeletionBytes',
    defaultSortOrder: 'descend',
    sorter: (a: BucketPendingDeletion, b: BucketPendingDeletion) =>
      a.totalPendingDeletionBytes - b.totalPendingDeletionBytes,
    render: (totalPendingDeletionBytes: number, record: BucketPendingDeletion) => (
      <Tooltip title={
        <>
          <div>Keys: {record.pendingDeletionKeys} ({byteToSize(record.pendingDeletionKeyBytes, 1)})</div>
          <div>Directories: {record.pendingDeletionDirs} ({byteToSize(record.pendingDeletionDirBytes, 1)})</div>
        </>
      }>
        {byteToSize(totalPendingDeletionBytes, 1)}
      </Tooltip>
    )
  },
  {
    title: (
      <Tooltip title={'Replicated size of deleted data that cannot be reclaimed because snapshots still reference it.'
        + ' Calculated on demand by OM, including the deleted data held in the snapshots of the bucket.'}>
        Snapshot Trapped Bytes
      </Tooltip>
    ),
    key: 'snapshotTrappedBytes',
    render: (_: unknown, record: BucketPendingDeletion) => renderSnapshotTrappedBytes(record)
  },
  calculatedColumn('Snapshot Trapped Keys', 'snapshotTrappedKeys',
    'Deleted keys that cannot be reclaimed because snapshots still reference them'),
  calculatedColumn('Snapshot Trapped Directories', 'snapshotTrappedDirs',
    'Deleted directories that cannot be reclaimed because snapshots still reference them'),
  calculatedColumn('Purgeable Bytes', 'purgeableBytes',
    'Replicated size of deleted data that is not referenced by any snapshot and can be reclaimed', true),
  calculatedColumn('Purgeable Keys', 'purgeableKeys',
    'Deleted keys that are not referenced by any snapshot and can be reclaimed'),
  calculatedColumn('Purgeable Directories', 'purgeableDirs',
    'Deleted directories that are not referenced by any snapshot and can be reclaimed')];

  // Column for a value of the snapshot trapped bytes calculation, shown once the calculation has completed
  function calculatedColumn(title: string, field: keyof SnapshotTrappedBytesResult, description: string,
    isBytes = false) {
    return {
      title: <Tooltip title={description}>{title}</Tooltip>,
      key: field,
      render: (_: unknown, record: BucketPendingDeletion) => {
        const job = jobs[bucketKey(record)];
        if (job?.status !== 'COMPLETED' || !job.result) {
          return '-';
        }
        const value = job.result[field];
        return (
          <span data-testid={`${field}-${bucketKey(record)}`}>
            {isBytes ? byteToSize(value, 1) : value}
          </span>
        );
      }
    };
  }

  function filterData(data: BucketPendingDeletion[] | undefined) {
    return data?.filter(
      (data: BucketPendingDeletion) => bucketKey(data).includes(debouncedSearch)
    );
  }

  return (<>
    <div className='table-header-section'>
      <div className='table-filter-section'>
        <SingleSelect
          options={LIMIT_OPTIONS}
          defaultValue={limit}
          placeholder='Limit'
          onChange={handleLimitChange} />
      </div>
      <Search
        disabled={(data?.length ?? 0) < 1}
        searchInput={searchTerm}
        onSearchChange={
          (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
        }
        onChange={() => undefined} />
    </div>
    <Table
      loading={bucketPendingDeletionData.loading}
      dataSource={filterData(data)}
      columns={columns}
      pagination={paginationConfig}
      rowKey={bucketKey}
      locale={{ filterTitle: '' }}
      scroll={{ x: 'max-content' }} />
  </>)
}

export default BucketPendingDeletionTable;
