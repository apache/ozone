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

import React, { useRef, useState } from 'react';
import moment from 'moment';
import { AxiosError } from 'axios';
import { Table } from 'antd';

import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';
import { byteToSize, showDataFetchError } from '@/utils/common';

import { Acl } from '@/v2/types/acl.types';


// ------------- Types -------------- //
type CountStats = {
  numBucket: number;
  numDir: number;
  numKey: number;
  numVolume: number;
};

type LocationInfo = {
  blockID: {
    containerBlockID: {
      containerID: number;
      localID: number;
    };
    blockCommitSequenceId: number;
    containerID: number;
    localID: number;
  };
  length: number;
  offset: number;
  token: null;
  createVersion: number;
  pipeline: null;
  partNumber: number;
  containerID: number;
  localID: number;
  blockCommitSequenceId: number;
};

type ObjectInfo = {
  bucketName: string;
  bucketLayout: string;
  encInfo: null;
  fileName: string;
  keyName: string;
  name: string;
  owner: string;
  volume: string;
  volumeName: string;
  sourceVolume: string | null;
  sourceBucket: string | null;
  usedBytes: number | null;
  usedNamespace: number;
  storageType: string;
  creationTime: number;
  dataSize: number;
  modificationTime: number;
  quotaInBytes: number;
  quotaInNamespace: number;
}

type ReplicationConfig = {
  replicationFactor: string;
  requiredNodes: number;
  replicationType: string;
}

type ObjectInfoResponse = ObjectInfo & {
  acls: Acl[];
  versioningEnabled: boolean;
  metadata: Record<string, any>;
  file: boolean;
  keyLocationVersions: {
    version: number;
    locationList: LocationInfo[];
    multipartKey: boolean;
    blocksLatestVersionOnly: LocationInfo[];
    locationLists: LocationInfo[][];
    locationListCount: number;
  }[];
  versioning: boolean;
  encryptionInfo: null;
  replicationConfig: ReplicationConfig;
};

type SummaryResponse = {
  countStats: CountStats;
  objectInfo: ObjectInfoResponse;
  path: string;
  status: string;
  type: string;
}

type MetadataProps = {
  path: string;
};

type MetadataState = {
  keys: string[];
  values: (string | number | boolean | null)[];
};


// ------------- Component -------------- //
const DUMetadata: React.FC<MetadataProps> = ({
  path = '/'
}) => {
  const [loading, setLoading] = useState<boolean>(false);
  const [state, setState] = useState<MetadataState>({
    keys: [],
    values: []
  });
  const cancelSummarySignal = useRef<AbortController>();
  const keyMetadataSummarySignal = useRef<AbortController>();
  const cancelQuotaSignal = useRef<AbortController>();

  const getObjectInfoMapping = React.useCallback((summaryResponse) => {

    const keys: string[] = [];
    const values: (string | number | boolean | null)[] = [];
    /**
     * We are creating a specific set of keys under Object Info response
     * which do not require us to modify anything
     */
    const selectedInfoKeys = [
      'bucketName', 'bucketLayout', 'encInfo', 'fileName', 'keyName',
      'name', 'owner', 'sourceBucket', 'sourceVolume', 'storageType',
      'usedNamespace', 'volumeName', 'volume'
    ] as const;
    const objectInfo: ObjectInfo = summaryResponse.objectInfo ?? {};

    selectedInfoKeys.forEach((key) => {
      if (objectInfo[key as keyof ObjectInfo] !== undefined && objectInfo[key as keyof ObjectInfo] !== -1) {
        // We will use regex to convert the Object key from camel case to space separated title
        // The following regex will match abcDef and produce Abc Def
        let keyName = key.replace(/([a-z0-9])([A-Z])/g, '$1 $2');
        keyName = keyName.charAt(0).toUpperCase() + keyName.slice(1);
        keys.push(keyName);
        values.push(objectInfo[key as keyof ObjectInfo]);
      }
    });

    if (objectInfo?.creationTime !== undefined && objectInfo?.creationTime !== -1) {
      keys.push('Creation Time');
      values.push(moment(objectInfo.creationTime).format('ll LTS'));
    }

    if (objectInfo?.usedBytes !== undefined && objectInfo?.usedBytes !== -1 && objectInfo!.usedBytes !== null) {
      keys.push('Used Bytes');
      values.push(byteToSize(objectInfo.usedBytes, 3));
    }

    if (objectInfo?.dataSize !== undefined && objectInfo?.dataSize !== -1) {
      keys.push('Data Size');
      values.push(byteToSize(objectInfo.dataSize, 3));
    }

    if (objectInfo?.modificationTime !== undefined && objectInfo?.modificationTime !== -1) {
      keys.push('Modification Time');
      values.push(moment(objectInfo.modificationTime).format('ll LTS'));
    }

    if (objectInfo?.quotaInBytes !== undefined && objectInfo?.quotaInBytes !== -1) {
      keys.push('Quota In Bytes');
      values.push(byteToSize(objectInfo.quotaInBytes, 3));
    }

    if (objectInfo?.quotaInNamespace !== undefined && objectInfo?.quotaInNamespace !== -1) {
      keys.push('Quota In Namespace');
      values.push(byteToSize(objectInfo.quotaInNamespace, 3));
    }

    if (summaryResponse.objectInfo?.replicationConfig?.replicationFactor !== undefined) {
      keys.push('Replication Factor');
      values.push(summaryResponse.objectInfo.replicationConfig.replicationFactor);
    }

    if (summaryResponse.objectInfo?.replicationConfig?.replicationType !== undefined) {
      keys.push('Replication Type');
      values.push(summaryResponse.objectInfo.replicationConfig.replicationType);
    }

    if (summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== undefined
      && summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== -1) {
      keys.push('Replication Required Nodes');
      values.push(summaryResponse.objectInfo.replicationConfig.requiredNodes);
    }

    return { keys, values }
  }, [path]);

  function loadMetadataSummary(path: string) {
    cancelRequests([
      cancelSummarySignal.current!,
      keyMetadataSummarySignal.current!
    ]);
    const keys: string[] = [];
    const values: (string | number | boolean | null)[] = [];

    const { request, controller } = AxiosGetHelper(
      `/api/v1/namespace/summary?path=${path}`,
      cancelSummarySignal.current
    );
    cancelSummarySignal.current = controller;

    request.then(response => {
      const summaryResponse: SummaryResponse = response.data;
      keys.push('Entity Type');
      values.push(summaryResponse.type);

      if (summaryResponse.status === 'INITIALIZING') {
        showDataFetchError(`The metadata is currently initializing. Please wait a moment and try again later`);
        return;
      }

      if (summaryResponse.status === 'PATH_NOT_FOUND') {
        showDataFetchError(`Invalid Path: ${path}`);
        return;
      }

      // If the entity is a Key then fetch the Key metadata only
      if (summaryResponse.type === 'KEY') {
        const { request: metadataRequest, controller: metadataNewController } = AxiosGetHelper(
          `/api/v1/namespace/du?path=${path}&replica=true`,
          keyMetadataSummarySignal.current
        );
        keyMetadataSummarySignal.current = metadataNewController;
        metadataRequest.then(response => {
          keys.push('File Size');
          values.push(byteToSize(response.data.size, 3));
          keys.push('File Size With Replication');
          values.push(byteToSize(response.data.sizeWithReplica, 3));
          keys.push("Creation Time");
          values.push(moment(summaryResponse.objectInfo.creationTime).format('ll LTS'));
          keys.push("Modification Time");
          values.push(moment(summaryResponse.objectInfo.modificationTime).format('ll LTS'));

          setState({
            keys: keys,
            values: values
          });
        }).catch(error => {
          showDataFetchError(error.toString());
        });
        return;
      }

      /** 
       * Will iterate over the keys of the countStats to avoid multiple if blocks
       * and check from the map for the respective key name / title to insert
      */
      const countStats: CountStats = summaryResponse.countStats ?? {};
      const keyToNameMap: Record<string, string> = {
        numVolume: 'Volumes',
        numBucket: 'Buckets',
        numDir: 'Total Directories',
        numKey: 'Total Keys'
      }
      Object.keys(countStats).forEach((key: string) => {
        if (countStats[key as keyof CountStats] !== undefined
          && countStats[key as keyof CountStats] !== -1) {
          keys.push(keyToNameMap[key]);
          values.push(countStats[key as keyof CountStats]);
        }
      })

      const {
        keys: objectInfoKeys,
        values: objectInfoValues
      } = getObjectInfoMapping(summaryResponse);

      keys.push(...objectInfoKeys);
      values.push(...objectInfoValues);

      setState({
        keys: keys,
        values: values
      });
    }).catch(error => {
      showDataFetchError((error as AxiosError).toString());
    });
  }

  function loadQuotaSummary(path: string) {
    cancelRequests([
      cancelQuotaSignal.current!
    ]);

    const { request, controller } = AxiosGetHelper(
      `/api/v1/namespace/quota?path=${path}`,
      cancelQuotaSignal.current
    );
    cancelQuotaSignal.current = controller;

    request.then(response => {
      const quotaResponse = response.data;

      if (quotaResponse.status === 'INITIALIZING') {
        return;
      }
      if (quotaResponse.status === 'TYPE_NOT_APPLICABLE') {
        return;
      }
      if (quotaResponse.status === 'PATH_NOT_FOUND') {
        showDataFetchError(`Invalid Path: ${path}`);
        return;
      }

      const keys: string[] = [];
      const values: (string | number | boolean | null)[] = [];
      // Append quota information
      // In case the object's quota isn't set
      if (quotaResponse.allowed !== undefined && quotaResponse.allowed !== -1) {
        keys.push('Quota Allowed');
        values.push(byteToSize(quotaResponse.allowed, 3));
      }

      if (quotaResponse.used !== undefined && quotaResponse.used !== -1) {
        keys.push('Quota Used');
        values.push(byteToSize(quotaResponse.used, 3));
      }
      setState((prevState) => ({
        keys: [...prevState.keys, ...keys],
        values: [...prevState.values, ...values]
      }));
    }).catch(error => {
      showDataFetchError(error.toString());
    });
  }

  React.useEffect(() => {
    setLoading(true);
    loadMetadataSummary(path);
    loadQuotaSummary(path);
    setLoading(false);

    return (() => {
      cancelRequests([
        cancelSummarySignal.current!,
        keyMetadataSummarySignal.current!,
        cancelQuotaSignal.current!
      ]);
    })
  }, [path]);

  const content = [];
  for (const [i, v] of state.keys.entries()) {
    content.push({
      key: v,
      value: state.values[i]
    });
  }

  return (
    <Table
      size='small'
      loading={loading}
      dataSource={content}
      bordered={true}
      style={{
        flex: '0 1 45%',
        margin: '10px auto' }}
      locale={{ filterTitle: '' }}>
      <Table.Column title='Property' dataIndex='key' />
      <Table.Column title='Value' dataIndex='value' />
    </Table>
  );
}

export default DUMetadata;