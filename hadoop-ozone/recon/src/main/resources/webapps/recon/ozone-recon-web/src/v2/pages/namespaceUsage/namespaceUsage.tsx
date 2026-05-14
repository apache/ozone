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

import React, { useState } from 'react';
import { Alert, Button, Modal, Tooltip } from 'antd';
import { InfoCircleFilled, ReloadOutlined, } from '@ant-design/icons';
import { ValueType } from 'react-select';

import NUMetadata from '@/v2/components/nuMetadata/nuMetadata';
import NUPieChart from '@/v2/components/plots/nuPieChart';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import DUBreadcrumbNav from '@/v2/components/duBreadcrumbNav/duBreadcrumbNav';
import { showDataFetchError, showInfoNotification } from '@/utils/common';
import { useApiData } from '@/v2/hooks/useAPIData.hook';

import { NUResponse } from '@/v2/types/namespaceUsage.types';

import './namespaceUsage.less';

const LIMIT_OPTIONS: Option[] = [
  { label: '5', value: '5' },
  { label: '10', value: '10' },
  { label: '15', value: '15' },
  { label: '20', value: '20' },
  { label: '30', value: '30' }
]

const DEFAULT_NU_RESPONSE: NUResponse = {
  status: '',
  path: '/',
  subPathCount: 0,
  size: 0,
  sizeWithReplica: 0,
  subPaths: [],
  sizeDirectKey: 0
};

const getSafeNumber = (value: unknown, defaultValue = 0): number => (
  typeof value === 'number' && Number.isFinite(value)
    ? value
    : defaultValue
);

const normalizeNamespacePath = (path: string): string => {
  const trimmedPath = path.trim();
  if (!trimmedPath) {
    return '';
  }
  return trimmedPath.startsWith('/')
    ? trimmedPath
    : `/${trimmedPath}`;
};

const getSafeNamespaceUsageResponse = (
  data: NUResponse | null | undefined,
  fallbackPath: string
): NUResponse => {
  const safeSubPaths = Array.isArray(data?.subPaths)
    ? data.subPaths.filter((subPath): subPath is NUResponse['subPaths'][number] => (
      Boolean(subPath && typeof subPath.path === 'string')
    ))
    : [];

  return {
    status: data?.status ?? '',
    path: normalizeNamespacePath(data?.path ?? fallbackPath) || '/',
    subPathCount: getSafeNumber(data?.subPathCount, safeSubPaths.length),
    size: getSafeNumber(data?.size),
    sizeWithReplica: getSafeNumber(data?.sizeWithReplica),
    subPaths: safeSubPaths,
    sizeDirectKey: getSafeNumber(data?.sizeDirectKey)
  };
};

const NamespaceUsage: React.FC<{}> = () => {
  const [limit, setLimit] = useState<Option>(LIMIT_OPTIONS[1]);
  const [currentPath, setCurrentPath] = useState<string>('/');
  const [showRootPathModal, setShowRootPathModal] = useState<boolean>(false);
  const [isRootPathUsageEnabled, setIsRootPathUsageEnabled] = useState<boolean>(false);
  const normalizedCurrentPath = normalizeNamespacePath(currentPath) || '/';
  const shouldFetchNamespaceUsage = normalizedCurrentPath !== '/' || isRootPathUsageEnabled;
  const showRootPathLoadPrompt = normalizedCurrentPath === '/' && !isRootPathUsageEnabled;

  // Use the modern hooks pattern
  const namespaceUsageData = useApiData<NUResponse>(
    shouldFetchNamespaceUsage
      ? `/api/v1/namespace/usage?path=${normalizedCurrentPath}&files=true&sortSubPaths=true`
      : '',
    DEFAULT_NU_RESPONSE,
    {
      initialFetch: shouldFetchNamespaceUsage,
      retryAttempts: 2,
      onError: (error) => showDataFetchError(error)
    }
  );

  const duResponse = React.useMemo(() => {
    if (!shouldFetchNamespaceUsage) {
      return {
        ...DEFAULT_NU_RESPONSE,
        path: normalizedCurrentPath,
        subPaths: []
      };
    }
    return getSafeNamespaceUsageResponse(namespaceUsageData.data, normalizedCurrentPath);
  }, [namespaceUsageData.data, normalizedCurrentPath, shouldFetchNamespaceUsage]);

  // Process data when it changes
  React.useEffect(() => {
    if (!shouldFetchNamespaceUsage || namespaceUsageData.loading) {
      return;
    }

    if (duResponse.status === 'PATH_NOT_FOUND') {
      showDataFetchError(`Invalid Path: ${normalizedCurrentPath}`);
      return;
    }

    if (duResponse.status === 'INITIALIZING') {
      showInfoNotification("Information being initialized", "Namespace Summary is being initialized, please wait.");
    }
  }, [duResponse.status, normalizedCurrentPath, namespaceUsageData.loading, shouldFetchNamespaceUsage]);

  const handleApiRequestFailure = (error: unknown) => {
    showDataFetchError(error);
  };

  const requestRootPathLoad = () => {
    if (normalizedCurrentPath !== '/') {
      setIsRootPathUsageEnabled(false);
    }
    setCurrentPath('/');
    setShowRootPathModal(true);
  };

  const handleReload = () => {
    if (normalizedCurrentPath === '/') {
      requestRootPathLoad();
      return;
    }
    void namespaceUsageData.refetch().catch(handleApiRequestFailure);
  };

  const handleRootPathLoadConfirm = () => {
    setShowRootPathModal(false);
    if (!isRootPathUsageEnabled) {
      setIsRootPathUsageEnabled(true);
      return;
    }
    void namespaceUsageData.refetch().catch(handleApiRequestFailure);
  };

  const loadData = (path: string) => {
    const normalizedPath = normalizeNamespacePath(path);
    if (!normalizedPath) {
      showDataFetchError('Invalid Path: Path cannot be empty');
      return;
    }

    if (normalizedPath === '/') {
      requestRootPathLoad();
      return;
    }
    setShowRootPathModal(false);
    setIsRootPathUsageEnabled(false);
    setCurrentPath(normalizedPath);
  };

  function handleLimitChange(selected: ValueType<Option, false>) {
    setLimit(selected as Option);
  }

  const loading = shouldFetchNamespaceUsage && namespaceUsageData.loading;

  return (
    <>
      <div className='page-header-v2'>
        Namespace Usage
      </div>
      <Modal
        centered={true}
        title='Confirm Root Path Usage Load'
        visible={showRootPathModal}
        onOk={handleRootPathLoadConfirm}
        onCancel={() => setShowRootPathModal(false)}>
        <p>Root path usage calculation is an expensive operation in large deployments.</p>
        <p><strong>Are you sure you want to continue?</strong></p>
      </Modal>
      <div className='data-container'>
        <Alert
          className='du-alert-message'
          message="Additional block size is added to small entities, for better visibility.
            Please refer to pie-chart details for exact size information."
          type="info"
          icon={<InfoCircleFilled />}
          showIcon={true}
          closable={false} />
        {
          currentPath == '/' && (
            <Alert
              className='du-alert-message'
              message='Root path usage can be expensive in large deployments. Enter an exact path to load usage directly.'
              type='warning'
              showIcon={true}
              closable={false} />
          )
        }
        <div className='content-div'>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
            }}>
            <DUBreadcrumbNav
              path={duResponse.path ?? '/'}
              subPaths={duResponse.subPaths}
              updateHandler={loadData} />
            <Tooltip
              title="Click to reload Namespace Usage data">
              <Button
                type='primary'
                icon={<ReloadOutlined />}
                onClick={handleReload} />
            </Tooltip>
          </div>
          <div className='du-table-header-section'>
            <SingleSelect
              options={LIMIT_OPTIONS}
              defaultValue={limit}
              placeholder='Limit'
              onChange={handleLimitChange} />
          </div>
          <div className='du-content'>
            {showRootPathLoadPrompt ? (
              <div className='du-root-path-load-container'>
                <p>Root path usage is not loaded by default.</p>
                <Button
                  type='primary'
                  onClick={requestRootPathLoad}>
                  Load usage
                </Button>
              </div>
            ) : (
              <>
                <NUPieChart
                  loading={loading}
                  limit={Number.parseInt(limit.value)}
                  path={duResponse.path}
                  subPathCount={duResponse.subPathCount}
                  subPaths={duResponse.subPaths}
                  sizeWithReplica={duResponse.sizeWithReplica}
                  size={duResponse.size} />
                <NUMetadata path={normalizedCurrentPath} />
              </>
            )}
          </div>
        </div>
      </div>
    </>
  );
}

export default NamespaceUsage;
