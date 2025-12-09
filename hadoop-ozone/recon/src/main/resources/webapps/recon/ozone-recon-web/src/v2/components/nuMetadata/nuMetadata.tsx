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

import React, {useState, useEffect, useCallback} from 'react';
import moment from 'moment';
import {Table} from 'antd';

import {byteToSize, removeDuplicatesAndMerge, showDataFetchError} from '@/utils/common';
import {useApiData, fetchData} from '@/v2/hooks/useAPIData.hook';

import {Acl} from '@/v2/types/acl.types';
import { QuotaAllowed, QuotaInNamespace, QuotaUsed } from '@/v2/constants/description.constants';


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
  key: string | React.ReactNode,
  value: string | number | boolean | null,
  rowKey: string
}[];


// ------------- Component -------------- //
const NUMetadata: React.FC<MetadataProps> = ({
  path = '/'
}) => {
  const [state, setState] = useState<MetadataState>([]);
  const [isProcessingData, setIsProcessingData] = useState<boolean>(false);
  const [pgNumber, setPgNumber] = useState<number>(1);
  // Individual API calls that resolve together
  const summaryAPI = useApiData<SummaryResponse>(
    `/api/v1/namespace/summary?path=${path}`,
    {} as SummaryResponse,
    {
      retryAttempts: 2,
      onError: (error) => showDataFetchError(error)
    }
  );
  
  const quotaAPI = useApiData<any>(
    `/api/v1/namespace/quota?path=${path}`,
    {},
    {
      retryAttempts: 2,
      onError: (error) => showDataFetchError(error)
    }
  );
  
  const loading = summaryAPI.loading || quotaAPI.loading || isProcessingData;

  const getObjectInfoMapping = useCallback((summaryResponse) => {
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
          value: objectInfo[key as keyof ObjectInfo],
          rowKey: keyName as string
        });
      }
    });

    // Source Volume and Source Bucket are present for linked buckets and volumes.
    // If it is not linked it will be null and should not be shown
    if (objectInfo?.sourceBucket !== undefined && objectInfo?.sourceBucket !== null) {
      data.push({
        key: 'Source Bucket',
        value: objectInfo.sourceBucket,
        rowKey: 'Source Bucket'
      });
    }

    if(objectInfo?.sourceVolume !== undefined && objectInfo?.sourceVolume !== null) {
      data.push({
        key: 'Source Volume',
        value: objectInfo.sourceVolume,
        rowKey: 'Source Volume'
      });
    }

    if (objectInfo?.creationTime !== undefined && objectInfo?.creationTime !== -1) {
      data.push({
        key: 'Creation Time',
        value: moment(objectInfo.creationTime).format('ll LTS'),
        rowKey: 'Creation Time'
      });
    }

    if (objectInfo?.usedBytes !== undefined && objectInfo?.usedBytes !== -1 && objectInfo!.usedBytes !== null) {
      data.push({
        key: 'Used Bytes',
        value: byteToSize(objectInfo.usedBytes, 3),
        rowKey: 'Used Bytes'
      });
    }

    if (objectInfo?.dataSize !== undefined && objectInfo?.dataSize !== -1) {
      data.push({
        key: 'Data Size',
        value: byteToSize(objectInfo.dataSize, 3),
        rowKey: 'Data Size'
      });
    }

    if (objectInfo?.modificationTime !== undefined && objectInfo?.modificationTime !== -1) {
      data.push({
        key: 'Modification Time',
        value: moment(objectInfo.modificationTime).format('ll LTS'),
        rowKey: 'Modification Time'
      });
    }

    if (objectInfo?.quotaInNamespace !== undefined && objectInfo?.quotaInNamespace !== -1) {
      data.push({
        key: QuotaInNamespace,
        value: objectInfo.quotaInNamespace,
        rowKey: 'Quota In Namespace'
      });
    }

    if (summaryResponse.objectInfo?.replicationConfig?.replicationFactor !== undefined) {
      data.push({
        key: 'Replication Factor',
        value: summaryResponse.objectInfo.replicationConfig.replicationFactor,
        rowKey: 'Replication Factor'
      });
    }

    if (summaryResponse.objectInfo?.replicationConfig?.replicationType !== undefined) {
      data.push({
        key: 'Replication Type',
        value: summaryResponse.objectInfo.replicationConfig.replicationType,
        rowKey: 'Replication Type'
      });
    }

    if (summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== undefined
      && summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== -1) {
      data.push({
        key: 'Replication Required Nodes',
        value: summaryResponse.objectInfo.replicationConfig.requiredNodes,
        rowKey: 'Replication Required Nodes'
      });
    }

    return data;
  }, [path]);

  // Process data when both APIs complete
  const processMetadata = useCallback(async (summaryResponse: SummaryResponse, quotaResponse: any) => {
    setIsProcessingData(true);
    try {
      let data: MetadataState = [];
      let summaryResponsePresent = true;
      let quotaResponsePresent = true;

      // Error checks for summary response
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
          value: summaryResponse.type,
          rowKey: 'Entity Type'
        });

        // If the entity is a Key then fetch the Key metadata only
        if (summaryResponse.type === 'KEY') {
          try {
            const usageResponse: any = await fetchData(`/api/v1/namespace/usage?path=${path}&replica=true`);
            data.push(...[{
              key: 'File Size',
              value: byteToSize(usageResponse.size, 3),
              rowKey: 'File Size'
            }, {
              key: 'File Size With Replication',
              value: byteToSize(usageResponse.sizeWithReplica, 3),
              rowKey: 'File Size With Replication'
            }, {
              key: 'Creation Time',
              value: moment(summaryResponse.objectInfo.creationTime).format('ll LTS'),
              rowKey: 'Creation Time'
            }, {
              key: 'Modification Time',
              value: moment(summaryResponse.objectInfo.modificationTime).format('ll LTS'),
              rowKey: 'Modification Time'
            }]);
            setState(data);
            return;
          } catch (error) {
            showDataFetchError(error);
            return;
          }
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
        };
        Object.keys(countStats).forEach((key: string) => {
          if (countStats[key as keyof CountStats] !== undefined
            && countStats[key as keyof CountStats] !== -1) {
            data.push({
              key: keyToNameMap[key],
              value: countStats[key as keyof CountStats],
              rowKey: keyToNameMap[key]
            });
          }
        });
      }

      // Error checks for quota response
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
            key: QuotaAllowed,
            value: byteToSize(quotaResponse.allowed, 3),
            rowKey: 'Quota Allowed'
          });
        }

        if (quotaResponse.used !== undefined && quotaResponse.used !== -1) {
          data.push({
            key: QuotaUsed,
            value: byteToSize(quotaResponse.used, 3),
            rowKey: 'Quota Used'
          });
        }
      }
      
      setState(data);
    } catch (error) {
      showDataFetchError(error);
    } finally {
      setIsProcessingData(false);
    }
  }, [path, getObjectInfoMapping]);

  // Reset pagination when path changes
  useEffect(() => {
    setPgNumber(1);
  }, [path]);

  // Coordinate API calls - process data when both calls complete
  useEffect(() => {
    if (!summaryAPI.loading && !quotaAPI.loading && 
        summaryAPI.data && quotaAPI.data &&
        summaryAPI.lastUpdated && quotaAPI.lastUpdated) {
      processMetadata(summaryAPI.data, quotaAPI.data);
    }
  }, [summaryAPI.loading, quotaAPI.loading, summaryAPI.data, quotaAPI.data, 
      summaryAPI.lastUpdated, quotaAPI.lastUpdated, processMetadata]);

  const handleTableChange = (newPagination: any) => {
    setPgNumber(newPagination.current);
  };

  return (
    <Table
      size='small'
      loading={loading}
      dataSource={state}
      rowKey='rowKey'
      bordered={true}
      pagination={{
        current: pgNumber
      }}
      onChange={handleTableChange}
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

export default NUMetadata;
