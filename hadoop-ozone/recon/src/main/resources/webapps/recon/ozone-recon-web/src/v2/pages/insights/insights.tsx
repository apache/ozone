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
import axios, {
  CanceledError,
  AxiosError
} from 'axios';
import { Row, Col, Card, Result } from 'antd';

import { showDataFetchError } from '@/utils/common';
import { PromiseAllSettledGetHelper } from '@/utils/axiosRequestHelper';

import { Option } from '@/v2/components/select/multiSelect';
import FileSizeDistribution from '@/v2/components/plots/insightsFilePlot';
import ContainerSizeDistribution from '@/v2/components/plots/insightsContainerPlot';

import {
  FileCountResponse,
  InsightsState,
  PlotResponse,
} from '@/v2/types/insights.types';

const Insights: React.FC<{}> = () => {

  const [loading, setLoading] = useState<boolean>(false);
  const [state, setState] = useState<InsightsState>({
    volumeBucketMap: new Map<string, Set<string>>(),
    volumeOptions: [],
    fileCountError: undefined,
    containerSizeError: undefined
  });
  const [plotResponse, setPlotResponse] = useState<PlotResponse>({
    fileCountResponse: [{
      volume: '',
      bucket: '',
      fileSize: 0,
      count: 0
    }],
    containerCountResponse: [{
      containerSize: 0,
      count: 0
    }]
  });

  const cancelInsightSignal = React.useRef<AbortController>();

  function loadData() {
    setLoading(true);
    const { requests, controller } = PromiseAllSettledGetHelper([
      '/api/v1/utilization/fileCount',
      '/api/v1/utilization/containerCount'
    ], cancelInsightSignal.current);

    cancelInsightSignal.current = controller;
    requests.then(axios.spread((
      fileCountResponse: Awaited<Promise<any>>,
      containerCountResponse: Awaited<Promise<any>>
    ) => {
      let fileAPIError;
      let containerAPIError;
      let responseError = [
        fileCountResponse,
        containerCountResponse
      ].filter((resp) => resp.status === 'rejected');

      if (responseError.length !== 0) {
        responseError.forEach((err) => {
          if (err.reason.toString().includes('CancelledError')) {
            throw new CanceledError('canceled', 'ERR_CANCELED');
          } else {
            if (err.reason.config.url.includes("fileCount")) {
              fileAPIError = err.reason.toString();
            } else {
              containerAPIError = err.reason.toString();
            }
          }
        });
      }

      // Construct volume -> bucket[] map for populating filters
      // Ex: vol1 -> [bucket1, bucket2], vol2 -> [bucket1]
      const volumeBucketMap: Map<string, Set<string>> = fileCountResponse.value?.data?.reduce(
        (map: Map<string, Set<string>>, current: FileCountResponse) => {
          const volume = current.volume;
          const bucket = current.bucket;
          if (map.has(volume)) {
            const buckets = Array.from(map.get(volume)!);
            map.set(volume, new Set<string>([...buckets, bucket]));
          } else {
            map.set(volume, new Set<string>().add(bucket));
          }
          return map;
        },
        new Map<string, Set<string>>()
      );
      const volumeOptions: Option[] = Array.from(volumeBucketMap.keys()).map(k => ({
        label: k,
        value: k
      }));

      setState({
        ...state,
        volumeBucketMap: volumeBucketMap,
        volumeOptions: volumeOptions,
        fileCountError: fileAPIError,
        containerSizeError: containerAPIError
      });
      setPlotResponse({
        fileCountResponse: fileCountResponse.value?.data ?? [{
          volume: '',
          bucket: '',
          fileSize: 0,
          count: 0
        }],
        containerCountResponse: containerCountResponse.value?.data ?? [{
          containerSize: 0,
          count: 0
        }]
      });
      setLoading(false);
    })).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    })
  }

  React.useEffect(() => {
    loadData();

    return (() => {
      cancelInsightSignal.current && cancelInsightSignal.current.abort();
    })
  }, []);

  return (
    <>
      <div className='page-header-v2'>
        Insights
      </div>
      <div style={{ padding: '24px' }}>
        {
          loading
            ? <Result title='Charts are being loaded' />
            : <>
              <Row gutter={20}>
                <Col xs={24} xl={12}>
                  <Card title='File Size Distribution' size='small'>
                    {plotResponse.fileCountResponse?.length > 0
                      ? <FileSizeDistribution
                          volumeOptions={state.volumeOptions}
                          volumeBucketMap={state.volumeBucketMap}
                          fileCountError={state.fileCountError}
                          fileCountResponse={plotResponse.fileCountResponse} />
                      : <Result
                        title='No Data'
                        subTitle='Add files to Ozone to see a visualization on file size distribution.' />}

                  </Card>
                </Col>
                <Col xs={24} xl={12}>
                  <Card title='Container Size Distribution' size='small'>
                    {plotResponse.containerCountResponse?.length > 0
                      ? <ContainerSizeDistribution
                          containerCountResponse={plotResponse.containerCountResponse}
                          containerSizeError={state.containerSizeError} />
                      : <Result
                        title='No Data'
                        subTitle='Add files to Ozone to see a visualization on container size distribution.' />}
                  </Card>
                </Col>
              </Row>
            </>
        }
      </div>
    </>
  )

}

export default Insights;