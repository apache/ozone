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

import React, { useState, useEffect, useRef } from "react";
import moment from "moment";
import {
  Button,
  Card,
  message,
  Progress,
  Row,
  Select,
  Table,
  Tag,
  Tabs,
  Tooltip,
} from "antd";
import { DeleteOutlined, DownloadOutlined, ExportOutlined } from "@ant-design/icons";
import { ValueType } from "react-select/src/types";
import { ColumnsType } from "antd/es/table";

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
  Container,
  ContainersPaginationResponse,
  ContainerState,
  ExpandedRow,
  ExportJob,
  QuasiClosedContainer,
  QuasiClosedContainersResponse,
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
  // '6' (Quasi Closed) intentionally absent — it uses /quasiClosed, not /unhealthy/:state
  // '7' (Export) intentionally absent — it has no container data to fetch
};

const EXPORT_STATE_OPTIONS = [
  { label: 'Missing', value: 'MISSING' },
  { label: 'Under-Replicated', value: 'UNDER_REPLICATED' },
  { label: 'Over-Replicated', value: 'OVER_REPLICATED' },
  { label: 'Mis-Replicated', value: 'MIS_REPLICATED' },
  { label: 'Replica Mismatch', value: 'REPLICA_MISMATCH' },
];

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

const POLL_INTERVAL_MS = 3000;

/**
 * Maps a QuasiClosedContainer (from the /quasiClosed API) to the shared
 * Container type so it can be displayed in the existing ContainerTable.
 * Explicit field mapping ensures TypeScript catches any upstream renames.
 */
function toContainer(qc: QuasiClosedContainer): Container {
  return {
    containerID: qc.containerID,
    pipelineID: qc.pipelineID,
    keys: qc.keys,
    containerState: 'QUASI_CLOSED',
    unhealthySince: qc.stateEnterTime,
    expectedReplicaCount: qc.expectedReplicaCount,
    actualReplicaCount: qc.actualReplicaCount,
    replicaDeltaCount: qc.actualReplicaCount - qc.expectedReplicaCount,
    reason: '',
    replicas: qc.replicas,
  };
}

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
    quasiClosedCount: 0,
  });
  const [pageSize, setPageSize] = useState<number>(DEFAULT_PAGE_SIZE);
  const [tabStates, setTabStates] = useState<Record<string, TabPaginationState>>({
    '1': { ...DEFAULT_TAB_STATE },
    '2': { ...DEFAULT_TAB_STATE },
    '3': { ...DEFAULT_TAB_STATE },
    '4': { ...DEFAULT_TAB_STATE },
    '5': { ...DEFAULT_TAB_STATE },
    '6': { ...DEFAULT_TAB_STATE },
  });
  const [expandedRow, setExpandedRow] = useState<ExpandedRow>({});
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [selectedTab, setSelectedTab] = useState<string>('1');
  const [searchColumn, setSearchColumn] = useState<'containerID' | 'pipelineID'>('containerID');

  // Export tab state
  const [exportJobs, setExportJobs] = useState<ExportJob[]>([]);
  const [selectedExportState, setSelectedExportState] = useState<string>('MISSING');
  const [exportSubmitting, setExportSubmitting] = useState<boolean>(false);
  const pollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

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

  // ── Polling ──────────────────────────────────────────────────────────────
  const fetchExportJobs = async () => {
    try {
      const jobs = await fetchData<ExportJob[]>(
        '/api/v1/containers/unhealthy/export'
      );
      setExportJobs(jobs ?? []);
      // Stop polling when no active jobs remain
      const hasActive = (jobs ?? []).some(
        j => j.status === 'QUEUED' || j.status === 'RUNNING'
      );
      if (!hasActive && pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    } catch (err) {
      // Silent — polling errors shouldn't break the UI
    }
  };

  const startPolling = () => {
    if (pollTimerRef.current) return; // already polling
    fetchExportJobs(); // immediate fetch
    pollTimerRef.current = setInterval(fetchExportJobs, POLL_INTERVAL_MS);
  };

  // Start polling when Export tab is active; stop when leaving if no active jobs.
  useEffect(() => {
    if (selectedTab === '7') {
      startPolling();
    } else {
      const hasActive = exportJobs.some(
        j => j.status === 'QUEUED' || j.status === 'RUNNING'
      );
      if (!hasActive && pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    }
    return () => {
      // Do NOT clear on unmount if active jobs exist; React StrictMode
      // can remount, so we guard with hasActive inside the interval callback.
    };
  }, [selectedTab]); // eslint-disable-line react-hooks/exhaustive-deps

  // Clear on component unmount
  useEffect(() => {
    return () => {
      if (pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
      }
    };
  }, []);

  // ── Export submit ─────────────────────────────────────────────────────────
  const handleSubmitExport = async () => {
    // Guard against race condition where exportJobs state may be stale
    if (exportJobs.some(
      j => j.state === selectedExportState
        && (j.status === 'QUEUED' || j.status === 'RUNNING' || j.status === 'COMPLETED')
    )) {
      message.warning(
        `A ${selectedExportState} export already exists. Delete it from the Completed Exports table to start a new one.`,
      );
      return;
    }
    setExportSubmitting(true);
    try {
      const response = await fetch(
        `/api/v1/containers/unhealthy/export?state=${selectedExportState}`,
        { method: 'POST' }
      );
      if (!response.ok) {
        let errorMsg = `Failed to start export (HTTP ${response.status})`;
        try {
          const body = await response.json();
          errorMsg = body.message || body.error || errorMsg;
        } catch {
          const text = await response.text();
          if (text && !text.includes('<html>')) errorMsg = text;
        }
        // Use a longer duration for queue-full errors so the user has time to read it
        const duration = response.status === 429 ? 6 : 4;
        message.error({ content: errorMsg, duration });
        return;
      }
      await fetchExportJobs();
      startPolling();
      message.success({ content: 'Export job submitted. Track progress in the table below.', duration: 3 });
    } catch (err: any) {
      message.error({ content: `Export failed: ${err.message || err}`, duration: 4 });
    } finally {
      setExportSubmitting(false);
    }
  };

  // ── Download helper ───────────────────────────────────────────────────────
  // Uses a hidden <a> so the browser streams the TAR directly to disk
  // (no in-memory buffering — important for multi-GB exports).
  const downloadFile = (jobId: string) => {
    const link = document.createElement('a');
    link.href = `/api/v1/containers/unhealthy/export/${jobId}/download`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    // Backend has already incremented downloadCount; refresh so the UI reflects
    // the new downloadsRemaining value without waiting for the next poll tick.
    setTimeout(() => fetchExportJobs(), 500);
  };

  // ── Delete job helper ─────────────────────────────────────────────────────
  const deleteJob = async (jobId: string) => {
    try {
      await fetch(`/api/v1/containers/unhealthy/export/${jobId}`, { method: 'DELETE' });
      fetchExportJobs();
    } catch (err: any) {
      message.error({ content: `Delete failed: ${err.message || err}`, duration: 4 });
    }
  };

  // ── Container data fetching ───────────────────────────────────────────────

  // Fetches the quasi-closed count independently to populate Highlights on page load.
  const fetchQuasiClosedCount = async () => {
    try {
      const response = await fetchData<QuasiClosedContainersResponse>(
        '/api/v1/containers/quasiClosed',
        'GET',
        { limit: 0, minContainerId: 0 }
      );
      setState(prev => ({
        ...prev,
        quasiClosedCount: response.quasiClosedCount ?? prev.quasiClosedCount,
      }));
    } catch (_) {
      // Non-critical: count stays 0 until the tab is opened.
    }
  };

  const fetchTabData = async (
    tabKey: string,
    minContainerId: number,
    currentPageSize: number
  ) => {
    const fetchSize = currentPageSize + 1;

    if (tabKey === '6') {
      // Quasi-closed uses its own dedicated in-memory endpoint, not /unhealthy/:state.
      setTabStates(prev => ({
        ...prev,
        [tabKey]: { ...prev[tabKey], loading: true },
      }));
      try {
        const response = await fetchData<QuasiClosedContainersResponse>(
          '/api/v1/containers/quasiClosed',
          'GET',
          { limit: fetchSize, minContainerId }
        );
        const allContainers = response.containers ?? [];
        const hasNextPage = allContainers.length > currentPageSize;
        const pageContainers = allContainers.slice(0, currentPageSize);
        const mapped: Container[] = pageContainers.map(toContainer);
        const lastKey = mapped.length > 0 ? Math.max(...mapped.map(c => c.containerID)) : 0;
        const firstKey = mapped.length > 0 ? Math.min(...mapped.map(c => c.containerID)) : 0;
        setTabStates(prev => ({
          ...prev,
          [tabKey]: {
            ...prev[tabKey],
            data: mapped,
            loading: false,
            firstKey,
            lastKey,
            currentMinContainerId: minContainerId,
            hasNextPage,
          },
        }));
        setState(prev => ({
          ...prev,
          quasiClosedCount: response.quasiClosedCount ?? prev.quasiClosedCount,
          lastUpdated: Number(moment()),
        }));
      } catch (error) {
        setTabStates(prev => ({
          ...prev,
          [tabKey]: { ...prev[tabKey], loading: false },
        }));
        showDataFetchError(error);
      }
      return;
    }

    const containerStateName = TAB_STATE_MAP[tabKey];
    if (!containerStateName) return; // skips tab '7' (Export) and any unknown keys

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
      const hasNextPage = allContainers.length > currentPageSize;
      const containers = allContainers.slice(0, currentPageSize);
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

      setState(prev => ({
        ...prev,
        missingCount: response.missingCount ?? prev.missingCount,
        underReplicatedCount: response.underReplicatedCount ?? prev.underReplicatedCount,
        overReplicatedCount: response.overReplicatedCount ?? prev.overReplicatedCount,
        misReplicatedCount: response.misReplicatedCount ?? prev.misReplicatedCount,
        replicaMismatchCount: response.replicaMismatchCount ?? prev.replicaMismatchCount,
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

  useEffect(() => {
    fetchTabData('1', 0, DEFAULT_PAGE_SIZE);
    fetchQuasiClosedCount();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleTabChange(key: string) {
    setSelectedTab(key);
    if (key !== '7' && tabStates[key]?.data.length === 0 && !tabStates[key]?.loading) {
      fetchTabData(key, 0, pageSize);
    }
  }

  function handleNextPage(tabKey: string) {
    const tab = tabStates[tabKey];
    if (tab.loading || !tab.hasNextPage) return;
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

  function handlePageSizeChange(newSize: number) {
    setPageSize(newSize);
    const reset = {
      '1': { ...DEFAULT_TAB_STATE },
      '2': { ...DEFAULT_TAB_STATE },
      '3': { ...DEFAULT_TAB_STATE },
      '4': { ...DEFAULT_TAB_STATE },
      '5': { ...DEFAULT_TAB_STATE },
      '6': { ...DEFAULT_TAB_STATE },
    };
    setTabStates(reset);
    fetchTabData(selectedTab, 0, newSize);
  }

  const loadContainersData = () => {
    setTabStates({
      '1': { ...DEFAULT_TAB_STATE },
      '2': { ...DEFAULT_TAB_STATE },
      '3': { ...DEFAULT_TAB_STATE },
      '4': { ...DEFAULT_TAB_STATE },
      '5': { ...DEFAULT_TAB_STATE },
      '6': { ...DEFAULT_TAB_STATE },
    });
    fetchTabData(selectedTab, 0, pageSize);
    fetchQuasiClosedCount();
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
    quasiClosedCount,
  } = state;

  const currentTabState = tabStates[selectedTab] ?? DEFAULT_TAB_STATE;

  // ── Export jobs table helpers ─────────────────────────────────────────────
  const activeJobs = exportJobs.filter(j => j.status === 'RUNNING' || j.status === 'QUEUED');
  const completedJobs = exportJobs.filter(j => j.status === 'COMPLETED' || j.status === 'FAILED');
  const isStateAlreadyActive = exportJobs.some(
    j => j.state === selectedExportState
      && (j.status === 'QUEUED' || j.status === 'RUNNING' || j.status === 'COMPLETED')
  );

  const statusColor: Record<string, string> = {
    QUEUED: 'blue',
    RUNNING: 'processing',
    COMPLETED: 'green',
    FAILED: 'red',
  };

  const jobIdColumn: ColumnsType<ExportJob>[0] = {
    title: 'Job ID',
    dataIndex: 'jobId',
    key: 'jobId',
    width: 110,
    render: (id: string) => (
      <Tooltip title={id}>
        <code>{id.substring(0, 8)}</code>
      </Tooltip>
    ),
  };

  const stateColumn: ColumnsType<ExportJob>[0] = {
    title: 'State',
    dataIndex: 'state',
    key: 'state',
    render: (s: string) => s.replace(/_/g, ' '),
  };

  const statusColumn: ColumnsType<ExportJob>[0] = {
    title: 'Status',
    dataIndex: 'status',
    key: 'status',
    width: 120,
    render: (status: string) => (
      <Tag color={statusColor[status] ?? 'default'}>{status}</Tag>
    ),
  };

  const submittedColumn: ColumnsType<ExportJob>[0] = {
    title: 'Submitted',
    dataIndex: 'submittedAt',
    key: 'submittedAt',
    render: (ts: number) => ts ? moment(ts).format('MMM D, HH:mm:ss') : '—',
  };

  const startedColumn: ColumnsType<ExportJob>[0] = {
    title: 'Started',
    dataIndex: 'startedAt',
    key: 'startedAt',
    render: (ts: number) => ts ? moment(ts).format('MMM D, HH:mm:ss') : '—',
  };

  // ── Active exports columns (RUNNING / QUEUED) ─────────────────────────────
  const activeExportColumns: ColumnsType<ExportJob> = [
    jobIdColumn,
    stateColumn,
    statusColumn,
    {
      title: 'Queue Position',
      dataIndex: 'queuePosition',
      key: 'queuePosition',
      width: 130,
      render: (_: number, record: ExportJob) =>
        record.status === 'QUEUED' && record.queuePosition > 0
          ? `#${record.queuePosition}`
          : '—',
    },
    submittedColumn,
    startedColumn,
    {
      title: 'Progress',
      key: 'progress',
      render: (_: unknown, record: ExportJob) => {
        if (record.status === 'RUNNING') {
          const pct = record.progressPercent || 0;
          const processed = record.totalRecords?.toLocaleString() ?? '0';
          const total = record.estimatedTotal > 0
            ? record.estimatedTotal.toLocaleString()
            : '?';
          return (
            <div style={{ minWidth: 160 }}>
              <Progress percent={pct} size='small' />
              <div style={{ fontSize: 11, color: '#888', marginTop: 2 }}>
                {processed} / {total} records
              </div>
            </div>
          );
        }
        return '—';
      },
    },
  ];

  // ── Completed exports columns (COMPLETED / FAILED) ────────────────────────
  const completedExportColumns: ColumnsType<ExportJob> = [
    jobIdColumn,
    stateColumn,
    statusColumn,
    {
      title: 'Records',
      dataIndex: 'totalRecords',
      key: 'totalRecords',
      render: (n: number, record: ExportJob) =>
        record.status === 'COMPLETED' ? (n?.toLocaleString() ?? '—') : '—',
    },
    submittedColumn,
    startedColumn,
    {
      title: 'Completed',
      dataIndex: 'completedAt',
      key: 'completedAt',
      render: (ts: number) => ts ? moment(ts).format('MMM D, HH:mm:ss') : '—',
    },
    {
      title: 'Action',
      key: 'action',
      render: (_: unknown, record: ExportJob) => {
        const deleteBtn = (
          <Button
            danger
            size='small'
            icon={<DeleteOutlined />}
            onClick={() => deleteJob(record.jobId)}>
            Delete
          </Button>
        );
        if (record.status === 'COMPLETED') {
          const limitReached = record.downloadsRemaining === 0;
          return (
            <div style={{ display: 'flex', gap: 8 }}>
              <Button
                type='primary'
                size='small'
                icon={<DownloadOutlined />}
                disabled={limitReached}
                onClick={() => downloadFile(record.jobId)}>
                {limitReached ? 'Limit reached' : `Download (${record.downloadsRemaining} left)`}
              </Button>
              {deleteBtn}
            </div>
          );
        }
        if (record.status === 'FAILED') {
          return (
            <div style={{ display: 'flex', gap: 8 }}>
              <Tooltip title={record.errorMessage ?? 'Unknown error'}>
                <span style={{ color: '#ff4d4f', fontSize: 12, alignSelf: 'center' }}>
                  {record.errorMessage ?? 'Failed'}
                </span>
              </Tooltip>
              {deleteBtn}
            </div>
          );
        }
        return null;
      },
    },
  ];

  // ── Highlights ────────────────────────────────────────────────────────────
  const highlightData = (
    <div style={{ display: 'flex', width: '90%', justifyContent: 'space-between' }}>
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
      <div className='highlight-content'>
        Quasi Closed <br/>
        <span className='highlight-content-value'>{quasiClosedCount ?? 'N/A'}</span>
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
            <Row align='middle'>{highlightData}</Row>
          </Card>
        </div>
        <div className='content-div'>
          <Tabs
            defaultActiveKey='1'
            onChange={(activeKey: string) => handleTabChange(activeKey)}>

            {/* ── Container data tabs ───────────────────────────────────── */}
            {(['1','2','3','4','5'] as const).map((key) => (
              <Tabs.TabPane
                key={key}
                tab={['Missing','Under-Replicated','Over-Replicated','Mis-Replicated','Mismatched Replicas'][Number(key)-1]}>
                <div className='table-header-section'>
                  <div className='table-filter-section'>
                    <MultiSelect
                      options={columnOptions}
                      defaultValue={selectedColumns}
                      selected={selectedColumns}
                      placeholder='Columns'
                      onChange={handleColumnChange}
                      fixedColumn='containerID'
                      onTagClose={() => {}}
                      columnLength={columnOptions.length} />
                  </div>
                  <Search
                    disabled={tabStates[key].data.length === 0}
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

            {/* ── Quasi-Closed tab ──────────────────────────────────────── */}
            <Tabs.TabPane key='6' tab='Quasi Closed'>
              <div className='table-header-section'>
                <div className='table-filter-section'>
                  <MultiSelect
                    options={columnOptions}
                    defaultValue={selectedColumns}
                    selected={selectedColumns}
                    placeholder='Columns'
                    onChange={handleColumnChange}
                    fixedColumn='containerID'
                    onTagClose={() => {}}
                    columnLength={columnOptions.length} />
                </div>
                <Search
                  disabled={tabStates['6'].data.length === 0}
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
              <ContainerTable
                data={tabStates['6'].data}
                loading={tabStates['6'].loading}
                searchColumn={searchColumn}
                searchTerm={debouncedSearch}
                selectedColumns={selectedColumns}
                expandedRow={expandedRow}
                expandedRowSetter={setExpandedRow}
                onNextPage={() => handleNextPage('6')}
                onPrevPage={() => handlePrevPage('6')}
                hasNextPage={tabStates['6'].hasNextPage}
                hasPrevPage={tabStates['6'].pageHistory.length > 0}
                pageSize={pageSize}
                onPageSizeChange={handlePageSizeChange}
                sinceColumnTitle='State Enter Time'
              />
            </Tabs.TabPane>

            {/* ── Export tab ────────────────────────────────────────────── */}
            <Tabs.TabPane
              key='7'
              tab={
                <span>
                  <ExportOutlined />
                  Export
                </span>
              }>
              <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 12 }}>
                <span style={{ fontWeight: 500 }}>Container State:</span>
                <Select
                  value={selectedExportState}
                  onChange={(v: string) => setSelectedExportState(v)}
                  options={EXPORT_STATE_OPTIONS}
                  style={{ width: 200 }} />
                <Tooltip title={isStateAlreadyActive
                  ? `A ${selectedExportState} export already exists. Delete it to start a new one.`
                  : ''}>
                  <Button
                    type='primary'
                    icon={<ExportOutlined />}
                    loading={exportSubmitting}
                    disabled={isStateAlreadyActive}
                    onClick={handleSubmitExport}>
                    Export CSV
                  </Button>
                </Tooltip>
              </div>

              {/* Active Exports */}
              {activeJobs.length > 0 && (
                <div style={{ marginBottom: 24 }}>
                  <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 8 }}>
                    Active Exports
                  </div>
                  <Table<ExportJob>
                    rowKey='jobId'
                    dataSource={activeJobs}
                    columns={activeExportColumns}
                    pagination={false}
                    size='middle'
                    locale={{ filterTitle: '' }}
                  />
                </div>
              )}

              {/* Completed Exports */}
              <div>
                <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 8 }}>
                  Completed Exports
                </div>
                <Table<ExportJob>
                  rowKey='jobId'
                  dataSource={completedJobs}
                  columns={completedExportColumns}
                  pagination={{ pageSize: 10, showSizeChanger: false }}
                  size='middle'
                  locale={{
                    emptyText: activeJobs.length === 0
                      ? 'No export jobs yet. Select a state and click Export CSV.'
                      : 'No completed exports yet.',
                    filterTitle: '',
                  }}
                />
              </div>
            </Tabs.TabPane>
          </Tabs>
        </div>
      </div>
    </>
  );
};

export default Containers;
