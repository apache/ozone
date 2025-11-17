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

import React from 'react';
import axios, { CanceledError } from 'axios';
import filesize from 'filesize';
import { Row, Col, Tabs, message } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import { ActionMeta, ValueType } from 'react-select';
import { graphic, type EChartsOption } from 'echarts';

import { EChart } from '@/components/eChart/eChart';
import { MultiSelect, IOption } from '@/components/multiSelect/multiSelect';
import { showDataFetchError } from '@/utils/common';
import { PromiseAllSettledGetHelper } from '@/utils/axiosRequestHelper';

import './insights.less';


const size = filesize.partial({ standard: 'iec', round: 0 });

interface IFileCountResponse {
  volume: string;
  bucket: string;
  fileSize: number;
  count: number;
}

interface IContainerCountResponse {
  containerSize: number;
  count: number;
}

interface IInsightsState {
  isLoading: boolean;
  fileCountsResponse: IFileCountResponse[];
  containerCountResponse: IContainerCountResponse[];
  fileCountData: EChartsOption;
  containerCountData: EChartsOption;
  volumeBucketMap: Map<string, Set<string>>;
  selectedVolumes: IOption[];
  selectedBuckets: IOption[];
  bucketOptions: IOption[];
  volumeOptions: IOption[];
  isBucketSelectionDisabled: boolean;
  fileCountError: string | undefined;
  containerSizeError: string | undefined;
}

const allVolumesOption: IOption = {
  label: 'All Volumes',
  value: '*'
};

const allBucketsOption: IOption = {
  label: 'All Buckets',
  value: '*'
};

let cancelInsightSignal: AbortController;

export class Insights extends React.Component<Record<string, object>, IInsightsState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      isLoading: false,
      fileCountsResponse: [],
      containerCountResponse: [],
      fileCountData: {},
      containerCountData: {},
      volumeBucketMap: new Map<string, Set<string>>(),
      selectedBuckets: [],
      selectedVolumes: [],
      bucketOptions: [],
      volumeOptions: [],
      isBucketSelectionDisabled: false,
      fileCountError: undefined,
      containerSizeError: undefined
    };
  }

  handleVolumeChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const { volumeBucketMap } = this.state;
    const selectedVolumes = (selected as IOption[]);

    // Disable bucket selection dropdown if more than one volume is selected
    // If there is only one volume, bucket selection dropdown should not be disabled.
    const isBucketSelectionDisabled = !selectedVolumes ||
      (selectedVolumes?.length > 1 && volumeBucketMap.size !== 1);
    let bucketOptions: IOption[] = [];
    // When volume is changed and more than one volume is selected,
    // selected buckets value should be reset to all buckets
    let selectedBuckets = [allBucketsOption];
    // Update bucket options only if one volume is selected
    if (selectedVolumes && ((selectedVolumes.length === 2 && selectedVolumes[0].value === '*') || (selectedVolumes.length === 1))) {
      let selectedVolume;
      if (selectedVolumes.length === 1) {
        selectedVolume = selectedVolumes[0].value;
      }
      else {
        selectedVolume = selectedVolumes[1].value;
      }
      if (volumeBucketMap.has(selectedVolume) && volumeBucketMap.get(selectedVolume)) {
        bucketOptions = Array.from(volumeBucketMap.get(selectedVolume)!).map(bucket => ({
          label: bucket,
          value: bucket
        }));
        selectedBuckets = [...selectedBuckets, ...bucketOptions];
      }
    }

    this.setState({
      selectedVolumes,
      selectedBuckets,
      bucketOptions,
      isBucketSelectionDisabled
    }, this.updatePlotData);
  };

  handleBucketChange = (selected: ValueType<IOption>, _event: ActionMeta<IOption>) => {
    const selectedBuckets = (selected as IOption[]);
    this.setState({
      selectedBuckets
    }, this.updatePlotData);
  };

  updatePlotData = () => {
    const { fileCountsResponse, selectedVolumes, selectedBuckets, containerCountResponse } = this.state;
    // Aggregate counts across volumes & buckets
    if (selectedVolumes && selectedBuckets) {
      let filteredData = fileCountsResponse;
      const selectedVolumeValues = new Set(selectedVolumes.map(option => option.value));
      const selectedBucketValues = new Set(selectedBuckets.map(option => option.value));
      if (selectedVolumes.length > 0 && !selectedVolumeValues.has(allVolumesOption.value)) {
        // If not all volumes are selected, filter volumes based on the selection
        filteredData = filteredData.filter(item => selectedVolumeValues.has(item.volume));
      }

      if (selectedBuckets.length > 0 && !selectedBucketValues.has(allBucketsOption.value)) {
        // If not all buckets are selected, filter buckets based on the selection
        filteredData = filteredData.filter(item => selectedBucketValues.has(item.bucket));
      }

      const xyFileCountMap: Map<number, number> = filteredData.reduce(
        (map: Map<number, number>, current) => {
          const fileSize = current.fileSize;
          const oldCount = map.has(fileSize) ? map.get(fileSize)! : 0;
          map.set(fileSize, oldCount + current.count);
          return map;
        }, new Map<number, number>());
      // Calculate the previous power of 2 to find the lower bound of the range
      // Ex: for 2048, the lower bound is 1024
      const xFileCountValues = Array.from(xyFileCountMap.keys()).map(value => {
        const upperbound = size(value);
        const upperboundPower = Math.log2(value);
        // For 1024 which is 2^10, the lowerbound is 0, since we start binning
        // after 2^10
        const lowerbound = upperboundPower > 10 ? size(2 ** (upperboundPower - 1)) : size(0);
        return `${lowerbound} - ${upperbound}`;
      });

      const xyContainerCountMap: Map<number, number> = containerCountResponse.reduce(
        (map: Map<number, number>, current) => {
          const containerSize = current.containerSize;
          const oldCount = map.has(containerSize) ? map.get(containerSize)! : 0;
          map.set(containerSize, oldCount + current.count);
          return map;
        }, new Map<number, number>());
      // Calculate the previous power of 2 to find the lower bound of the range
      // Ex: for 2048, the lower bound is 1024
      const xContainerCountValues = Array.from(xyContainerCountMap.keys()).map(value => {
        const upperbound = size(value);
        const upperboundPower = Math.log2(value);
        // For 1024 which is 2^10, the lowerbound is 0, since we start binning
        // after 2^10
        const lowerbound = upperboundPower > 10 ? size(2 ** (upperboundPower - 1)) : size(0);
        return `${lowerbound} - ${upperbound}`;
      });

      let keysize = [];
      keysize = Array.from(xyContainerCountMap.keys()).map(value => {
        return (size(value));
      });

      let renderFileCountError = (this.state.fileCountError) ? {
        type: 'group',
        left: 'center',
        top: 'middle',
        z: 100,
        children: [
          {
            type: 'rect',
            left: 'center',
            top: 'middle',
            z: 100,
            shape: {
              width: 500,
              height: 40
            },
            style: {
              fill: '#FC909B'
            }
          },
          {
            type: 'text',
            left: 'center',
            top: 'middle',
            z: 100,
            style: {
              text: `No data available. ${this.state.fileCountError}`,
              font: '20px sans-serif'
            }
          }
        ]
      } : undefined
      let renderContainerSizeError = (this.state.containerSizeError) ? {
        type: 'group',
        left: 'center',
        top: 'middle',
        z: 100,
        children: [
          {
            type: 'rect',
            left: 'center',
            top: 'middle',
            z: 100,
            shape: {
              width: 500,
              height: 500
            },
            style: {
              fill: 'rgba(256, 256, 256, 0.5)'
            }
          },
          {
            type: 'rect',
            left: 'center',
            top: 'middle',
            z: 100,
            shape: {
              width: 500,
              height: 40
            },
            style: {
              fill: '#FC909B'
            }
          },
          {
            type: 'text',
            left: 'center',
            top: 'middle',
            z: 100,
            style: {
              text: `No data available. ${this.state.containerSizeError}`,
              font: '20px sans-serif'
            }
          }
        ]
      } : undefined

      this.setState({
        fileCountData: {
          title: {
            text: 'File Size Distribution',
            left: 'center'
          },
          xAxis: {
            type: 'category',
            data: xFileCountValues
          },
          yAxis: {
            type: 'value'
          },
          tooltip: {
            trigger: 'item',
            formatter: ({ name, value }) => {
              return `Size Range: ${name}<br>Count: <strong>${value}</strong>`
            }
          },
          series: {
            itemStyle: {
              color: '#04AD78'
            },
            data: Array.from(xyFileCountMap.values()),
            type: 'bar'
          },
          graphic: renderFileCountError
        },
        containerCountData: {
          title: {
            text: 'Container Size Distribution',
            left: 'center'
          },
          tooltip: {
            trigger: 'item',
            formatter: ({ data }) => {
              return `Size Range: <strong>${data.name}</strong><br>Count: <strong>${data.value}</strong>`
            }
          },
          legend: {
            orient: 'vertical',
            left: 'right'
          },
          series: {
            type: 'pie',
            radius: '50%',
            data: Array.from(xyContainerCountMap.values()).map((value, idx) => {
              return {
                value: value,
                name: xContainerCountValues[idx]
              }
            }),
          },
          graphic: renderContainerSizeError
        }
      });
    }
  };

  componentDidMount(): void {
    // Fetch file size counts on component mount
    this.setState({
      isLoading: true
    });
    const { requests, controller } = PromiseAllSettledGetHelper([
      '/api/v1/utilization/fileCount',
      '/api/v1/utilization/containerCount'
    ], cancelInsightSignal);

    cancelInsightSignal = controller;
    requests.then(axios.spread((
      fileCountresponse: Awaited<Promise<any>>,
      containerCountresponse: Awaited<Promise<any>>
    ) => {
      let fileAPIError = undefined;
      let containerAPIError = undefined;
      let responseError = [
        fileCountresponse,
        containerCountresponse
      ].filter((resp) => resp.status === 'rejected');

      if (responseError.length !== 0) {
        responseError.forEach((err) => {
          if (err.reason.toString().includes("CanceledError")) {
            throw new CanceledError('canceled', "ERR_CANCELED");
          }
          else {
            if (err.reason.config.url.includes("fileCount")) {
              fileAPIError = err.reason.toString();
            }
            else {
              containerAPIError = err.reason.toString();
            }
          }
        })
      }

      const fileCountsResponse: IFileCountResponse[] = fileCountresponse.value?.data ?? [{
        volume: '0',
        bucket: '0',
        fileSize: '0',
        count: 0
      }];
      const containerCountResponse: IContainerCountResponse[] = containerCountresponse.value?.data ?? [{
        containerSize: 0,
        count: 0
      }];

      // Construct volume -> bucket[] map for populating filters
      // Ex: vol1 -> [bucket1, bucket2], vol2 -> [bucket1]
      const volumeBucketMap: Map<string, Set<string>> = fileCountsResponse.reduce(
        (map: Map<string, Set<string>>, current) => {
          const volume = current.volume;
          const bucket = current.bucket;
          if (map.has(volume)) {
            const buckets = Array.from(map.get(volume)!);
            map.set(volume, new Set([...buckets, bucket]));
          } else {
            map.set(volume, new Set<string>().add(bucket));
          }

          return map;
        }, new Map<string, Set<string>>());

      // Set options for volume selection dropdown
      const volumeOptions: IOption[] = Array.from(volumeBucketMap.keys()).map(k => ({
        label: k,
        value: k
      }));

      this.setState({
        isLoading: false,
        volumeBucketMap,
        fileCountsResponse,
        containerCountResponse,
        volumeOptions,
        fileCountError: fileAPIError,
        containerSizeError: containerAPIError
      }, () => {
        this.updatePlotData();
        // Select all volumes by default
        this.handleVolumeChange([allVolumesOption, ...volumeOptions], { action: 'select-option' });
      });
    })).catch(error => {
      this.setState({
        isLoading: false,
      });
      showDataFetchError(error);
    });
  }

  componentWillUnmount(): void {
    cancelInsightSignal && cancelInsightSignal.abort();
  }

  render() {
    const {
      fileCountData,
      isLoading,
      selectedBuckets,
      volumeOptions,
      selectedVolumes,
      fileCountsResponse,
      bucketOptions,
      isBucketSelectionDisabled,
      containerCountResponse,
      containerCountData
    } = this.state;

    return (
      <div className='insights-container'>
        <div className='page-header'>
          Insights
        </div>

        <div className='content-div'>
          <Tabs defaultActiveKey='1'>
            <Tabs.TabPane tab='File Size' key='1'>
              <div className='content-div'>
                {isLoading ? <span><LoadingOutlined /> Loading...</span> :
                  ((fileCountsResponse?.length > 0) ?
                    <div>
                      <Row>
                        <Col xs={24} xl={18}>
                          <Row>
                            <Col>
                              <div className='filter-block'>
                                <h4>Volumes</h4>
                                <MultiSelect
                                  allowSelectAll
                                  isMulti
                                  className='multi-select-container'
                                  options={volumeOptions}
                                  closeMenuOnSelect={false}
                                  hideSelectedOptions={false}
                                  value={selectedVolumes}
                                  allOption={allVolumesOption}
                                  onChange={this.handleVolumeChange}
                                />
                              </div>
                              <div className='filter-block'>
                                <h4>Buckets</h4>
                                <MultiSelect
                                  allowSelectAll
                                  isMulti
                                  className='multi-select-container'
                                  options={bucketOptions}
                                  closeMenuOnSelect={false}
                                  hideSelectedOptions={false}
                                  value={selectedBuckets}
                                  allOption={allBucketsOption}
                                  isDisabled={isBucketSelectionDisabled}
                                  onChange={this.handleBucketChange}
                                />
                              </div>
                            </Col>
                          </Row>
                        </Col>
                      </Row>
                      <Row>
                        <Col style={{ margin: 'auto', marginTop: '2%' }}>
                          <EChart option={fileCountData} />
                        </Col>
                      </Row>
                    </div> :
                    <div>No data to visualize file size distribution. Add files to Ozone to see a visualization on file size distribution.</div>)}
              </div>
            </Tabs.TabPane>
            <Tabs.TabPane tab='Container Size' key='2'>
              <div className='content-div'>
                {isLoading ? <span><LoadingOutlined /> Loading...</span> :
                  ((containerCountResponse?.length > 0) ?
                    <div>
                      <Row>
                        <Col style={{ margin: 'auto', marginTop: '2%' }}>
                          <EChart option={containerCountData} />
                        </Col>
                      </Row>
                    </div> :
                    <div>No data available for container size distribution visualization. Add files to Ozone</div>)}
              </div>
            </Tabs.TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}
