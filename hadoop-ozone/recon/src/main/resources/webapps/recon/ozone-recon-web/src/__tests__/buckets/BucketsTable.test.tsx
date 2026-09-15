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

import { vi } from 'vitest';
import { render, screen } from '@testing-library/react';

import BucketsTable from '@/v2/components/tables/bucketsTable';
import { Bucket, BucketsTableProps } from '@/v2/types/bucket.types';

function getBucketWith(
  name: string,
  replicationConfigInfo: Bucket['replicationConfigInfo']
): Bucket {
  return {
    volumeName: 'vol1',
    name,
    versioning: false,
    storageType: 'DISK',
    creationTime: 1728280581608,
    modificationTime: 1728280581608,
    usedBytes: 0,
    usedNamespace: 0,
    quotaInBytes: -1,
    quotaInNamespace: -1,
    owner: 'om',
    acls: [],
    bucketLayout: 'FILE_SYSTEM_OPTIMIZED',
    replicationConfigInfo
  };
}

const defaultProps: BucketsTableProps = {
  loading: false,
  data: [],
  handleAclClick: vi.fn(),
  searchColumn: 'name',
  searchTerm: '',
  selectedColumns: [
    { label: 'Bucket',
      value: 'name' },
    { label: 'Volume',
      value: 'volumeName' },
    { label: 'Replication Type',
      value: 'replicationType' }
  ]
};

describe('BucketsTable Replication Type column', () => {
  test('renders the Ratis variant for a Ratis bucket', () => {
    render(
      <BucketsTable
        {...defaultProps}
        data={[getBucketWith('ratis-bucket', {
          type: 'RATIS',
          replicationConfig: {
            replicationType: 'RATIS',
            replicationFactor: 'THREE',
            requiredNodes: 3,
            minimumNodes: 1
          }
        })]}
      />
    );

    expect(screen.getByText('Ratis-3')).toBeInTheDocument();
  });

  test.each([
    ['RS', 6, 3, 1048576, 'rs-6-3-1024k'],
    ['RS', 3, 2, 1048576, 'rs-3-2-1024k'],
    ['RS', 10, 4, 1048576, 'rs-10-4-1024k'],
    ['XOR', 10, 4, 2097152, 'xor-10-4-2048k']
  ] as const)(
    'renders an EC bucket with codec %s, %i data and %i parity and a %i byte chunk as %s',
    (codec, data, parity, ecChunkSize, expected) => {
      render(
        <BucketsTable
          {...defaultProps}
          data={[getBucketWith('ec-bucket', {
            type: 'EC',
            replicationConfig: {
              replicationType: 'EC',
              codec,
              data,
              parity,
              ecChunkSize,
              requiredNodes: data + parity,
              minimumNodes: data
            }
          })]}
        />
      );

      expect(screen.getByText(expected)).toBeInTheDocument();
    });

  test('renders the Standalone variant for a single replica bucket', () => {
    render(
      <BucketsTable
        {...defaultProps}
        data={[getBucketWith('standalone-bucket', {
          type: 'STAND_ALONE',
          replicationConfig: {
            replicationType: 'STANDALONE',
            replicationFactor: 'ONE',
            requiredNodes: 1,
            minimumNodes: 1
          }
        })]}
      />
    );

    expect(screen.getByText('Standalone-1')).toBeInTheDocument();
  });

  test('renders the Standalone variant for the STAND_ALONE enum spelling', () => {
    render(
      <BucketsTable
        {...defaultProps}
        data={[getBucketWith('standalone-enum-bucket', {
          type: 'STAND_ALONE',
          replicationConfig: {
            replicationType: 'STAND_ALONE',
            replicationFactor: 'ONE',
            requiredNodes: 1,
            minimumNodes: 1
          }
        })]}
      />
    );

    expect(screen.getByText('Standalone-1')).toBeInTheDocument();
  });

  test('falls back to the replication type when the nested config is absent', () => {
    render(
      <BucketsTable
        {...defaultProps}
        data={[getBucketWith('type-only-bucket', { type: 'RATIS' })]}
      />
    );

    expect(screen.getByText('RATIS')).toBeInTheDocument();
  });

  test('falls back to NA when replicationConfigInfo is missing', () => {
    render(
      <BucketsTable
        {...defaultProps}
        data={[getBucketWith('legacy-bucket', undefined)]}
      />
    );

    expect(screen.getByText('NA')).toBeInTheDocument();
  });

  test('falls back to NA when replicationConfigInfo is null', () => {
    render(
      <BucketsTable
        {...defaultProps}
        data={[getBucketWith('null-bucket', null)]}
      />
    );

    expect(screen.getByText('NA')).toBeInTheDocument();
  });
});
