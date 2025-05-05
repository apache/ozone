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
import axios, { AxiosError } from 'axios';
import { Table } from 'antd';

import { AxiosGetHelper, cancelRequests, PromiseAllSettledGetHelper } from '@/utils/axiosRequestHelper';
import { byteToSize, checkResponseError, removeDuplicatesAndMerge, showDataFetchError } from '@/utils/common';

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
  key: string,
  value: string | number | boolean | null
}[];


// ------------- Component -------------- //
const DUMetadata: React.FC<MetadataProps> = ({
  path = '/'
}) => {
  const [loading, setLoading] = useState<boolean>(false);
  const [state, setState] = useState<MetadataState>([]);
  const keyMetadataSummarySignal = useRef<AbortController>();
  const cancelMetadataSignal = useRef<AbortController>();

  const getObjectInfoMapping = React.useCallback((summaryResponse) => {
    const data: MetadataState = [];
    /**
     * We are creating a specific set of keys under Object Info response
     * which do not require us to modify anything
     */
    const selectedInfoKeys = [
      'bucketName', 'bucketLayout', 'encInfo', 'fileName', 'keyName',
      'name', 'owner', 'storageType', 'usedNamespace', 'volumeName', 'volume'
    ] as const;
    const objectInfo: ObjectInfo = summaryResponse.objectInfo ?? {};

    selectedInfoKeys.forEach((key) => {
      if (objectInfo[key as keyof ObjectInfo] !== undefined && objectInfo[key as keyof ObjectInfo] !== -1) {
        // We will use regex to convert the Object key from camel case to space separated title
        // The following regex will match abcDef and produce Abc Def
        let keyName = key.replace(/([a-z0-9])([A-Z])/g, '$1 $2');
        keyName = keyName.charAt(0).toUpperCase() + keyName.slice(1);
        data.push({
          key: keyName as string,
          value: objectInfo[key as keyof ObjectInfo]
        });
      }
    });

    // Source Volume and Source Bucket are present for linked buckets and volumes.
    // If it is not linked it will be null and should not be shown
    if (objectInfo?.sourceBucket !== undefined && objectInfo?.sourceBucket !== null) {
      data.push({
        key: 'Source Bucket',
        value: objectInfo.sourceBucket
      });
    }

    if(objectInfo?.sourceVolume !== undefined && objectInfo?.sourceVolume !== null) {
      data.push({
        key: 'Source Volume',
        value: objectInfo.sourceVolume
      });
    }

    if (objectInfo?.creationTime !== undefined && objectInfo?.creationTime !== -1) {
      data.push({
        key: 'Creation Time',
        value: moment(objectInfo.creationTime).format('ll LTS')
      });
    }

    if (objectInfo?.usedBytes !== undefined && objectInfo?.usedBytes !== -1 && objectInfo!.usedBytes !== null) {
      data.push({
        key: 'Used Bytes',
        value: byteToSize(objectInfo.usedBytes, 3)
      });
    }

    if (objectInfo?.dataSize !== undefined && objectInfo?.dataSize !== -1) {
      data.push({
        key: 'Data Size',
        value: byteToSize(objectInfo.dataSize, 3)
      });
    }

    if (objectInfo?.modificationTime !== undefined && objectInfo?.modificationTime !== -1) {
      data.push({
        key: 'Modification Time',
        value: moment(objectInfo.modificationTime).format('ll LTS')
      });
    }

    if (objectInfo?.quotaInNamespace !== undefined && objectInfo?.quotaInNamespace !== -1) {
      data.push({
        key: 'Quota In Namespace',
        value: objectInfo.quotaInNamespace
      });
    }

    if (summaryResponse.objectInfo?.replicationConfig?.replicationFactor !== undefined) {
      data.push({
        key: 'Replication Factor',
        value: summaryResponse.objectInfo.replicationConfig.replicationFactor
      });
    }

    if (summaryResponse.objectInfo?.replicationConfig?.replicationType !== undefined) {
      data.push({
        key: 'Replication Type',
        value: summaryResponse.objectInfo.replicationConfig.replicationType
      });
    }

    if (summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== undefined
      && summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== -1) {
      data.push({
        key: 'Replication Required Nodes',
        value: summaryResponse.objectInfo.replicationConfig.requiredNodes
      });
    }

    return data;
  }, [path]);

  function loadData(path: string) {
    const { requests, controller } = PromiseAllSettledGetHelper([
      `/api/v1/namespace/summary?path=${path}`,
      `/api/v1/namespace/quota?path=${path}`
    ], cancelMetadataSignal.current);
    cancelMetadataSignal.current = controller;

    requests.then(axios.spread((
      nsSummaryResponse: Awaited<Promise<any>>,
      quotaApiResponse: Awaited<Promise<any>>,
    ) => {
      checkResponseError([nsSummaryResponse, quotaApiResponse]);
      const summaryResponse: SummaryResponse = nsSummaryResponse.value?.data ?? {};
      const quotaResponse = quotaApiResponse.value?.data ?? {};
      let data: MetadataState = [];
      let summaryResponsePresent = true;
      let quotaResponsePresent = true;

      // Error checks
      if (summaryResponse.status === 'INITIALIZING') {
        summaryResponsePresent = false;
        showDataFetchError(`The metadata is currently initializing. Please wait a moment and try again later`);
      }

      if (summaryResponse.status === 'PATH_NOT_FOUND' || quotaResponse.status === 'PATH_NOT_FOUND') {
        summaryResponsePresent = false;
        quotaResponsePresent = false;
        showDataFetchError(`Invalid Path: ${path}`);
      }

      if (summaryResponsePresent) {
        // Summary Response data section
        data.push({
          key: 'Entity Type',
          value: summaryResponse.type
        });

        // If the entity is a Key then fetch the Key metadata only
        if (summaryResponse.type === 'KEY') {
          const { request: metadataRequest, controller: metadataNewController } = AxiosGetHelper(
            `/api/v1/namespace/du?path=${path}&replica=true`,
            keyMetadataSummarySignal.current
          );
          keyMetadataSummarySignal.current = metadataNewController;
          metadataRequest.then(response => {
            data.push(...[{
              key: 'File Size',
              value: byteToSize(response.data.size, 3)
            }, {
              key: 'File Size With Replication',
              value: byteToSize(response.data.sizeWithReplica, 3)
            }, {
              key: 'Creation Time',
              value: moment(summaryResponse.objectInfo.creationTime).format('ll LTS')
            }, {
              key: 'Modification Time',
              value: moment(summaryResponse.objectInfo.modificationTime).format('ll LTS')
            }])
            setState(data);
          }).catch(error => {
            showDataFetchError(error.toString());
          });
          return;
        }

        data = removeDuplicatesAndMerge(data, getObjectInfoMapping(summaryResponse), 'key');

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
            data.push({
              key: keyToNameMap[key],
              value: countStats[key as keyof CountStats]
            });
          }
        })
      }

      if (quotaResponse.state === 'INITIALIZING') {
        quotaResponsePresent = false;
        showDataFetchError(`The quota is currently initializing. Please wait a moment and try again later`);
      }

      if (quotaResponse.status === 'TYPE_NOT_APPLICABLE') {
        quotaResponsePresent = false;
      }

      if (quotaResponsePresent) {
        // Quota Response section
        // In case the object's quota isn't set, we should not populate the values
        if (quotaResponse.allowed !== undefined && quotaResponse.allowed !== -1) {
          data.push({
            key: 'Quota Allowed',
            value: byteToSize(quotaResponse.allowed, 3)
          });
        }

        if (quotaResponse.used !== undefined && quotaResponse.used !== -1) {
          data.push({
            key: 'Quota Used',
            value: byteToSize(quotaResponse.used, 3)
          })
        }
      }
      setState(data);
    })).catch(error => {
      showDataFetchError((error as AxiosError).toString());
    });
  }

  React.useEffect(() => {
    setLoading(true);
    loadData(path);
    setLoading(false);

    return (() => {
      cancelRequests([
        cancelMetadataSignal.current!,
      ]);
    })
  }, [path]);

  return (
    <Table
      size='small'
      loading={loading}
      dataSource={state}
      bordered={true}
      style={{
        flex: '0 1 45%',
        margin: '10px auto'
      }}
      locale={{ filterTitle: '' }}>
      <Table.Column title='Property' dataIndex='key' />
      <Table.Column title='Value' dataIndex='value' />
    </Table>
  );
}

export default DUMetadata;
