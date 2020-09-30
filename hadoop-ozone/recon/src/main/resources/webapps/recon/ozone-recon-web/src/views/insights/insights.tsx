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
import axios from 'axios';
import {Icon, Row, Col} from 'antd';
import filesize from 'filesize';
import {showDataFetchError} from 'utils/common';
import Plot from 'react-plotly.js';
import * as Plotly from 'plotly.js';
import {MultiSelect, IOption} from 'components/multiSelect/multiSelect';
import {ActionMeta, ValueType} from 'react-select';
import './insights.less';

const size = filesize.partial({standard: 'iec'});

interface IFileCountResponse {
  volume: string;
  bucket: string;
  fileSize: number;
  count: number;
}

interface IInsightsState {
  isLoading: boolean;
  fileCountsResponse: IFileCountResponse[];
  plotData: Plotly.Data[];
  volumeBucketMap: Map<string, Set<string>>;
  selectedVolumes: IOption[];
  selectedBuckets: IOption[];
  bucketOptions: IOption[];
  volumeOptions: IOption[];
  isBucketSelectionDisabled: boolean;
}

const allVolumesOption: IOption = {
  label: 'All Volumes',
  value: '*'
};

const allBucketsOption: IOption = {
  label: 'All Buckets',
  value: '*'
};

export class Insights extends React.Component<Record<string, object>, IInsightsState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      isLoading: false,
      fileCountsResponse: [],
      plotData: [],
      volumeBucketMap: new Map<string, Set<string>>(),
      selectedBuckets: [],
      selectedVolumes: [],
      bucketOptions: [],
      volumeOptions: [],
      isBucketSelectionDisabled: false
    };
  }

  handleVolumeChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const {volumeBucketMap} = this.state;
    const selectedVolumes = (selected as IOption[]);

    // Disable bucket selection dropdown if more than one volume is selected
    // If there is only one volume, bucket selection dropdown should not be disabled.
    const isBucketSelectionDisabled = !selectedVolumes ||
        (selectedVolumes &&
            (selectedVolumes.length > 2 &&
                (volumeBucketMap.size !== 1)));
    let bucketOptions: IOption[] = [];
    // When volume is changed and more than one volume is selected,
    // selected buckets value should be reset to all buckets
    let selectedBuckets = [allBucketsOption];
    // Update bucket options only if one volume is selected
    if (selectedVolumes && selectedVolumes.length === 1) {
      const selectedVolume = selectedVolumes[0].value;
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
    const {fileCountsResponse, selectedVolumes, selectedBuckets} = this.state;
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

      const xyMap: Map<number, number> = filteredData.reduce(
        (map: Map<number, number>, current) => {
          const fileSize = current.fileSize;
          const oldCount = map.has(fileSize) ? map.get(fileSize)! : 0;
          map.set(fileSize, oldCount + current.count);
          return map;
        }, new Map<number, number>());
      // Calculate the previous power of 2 to find the lower bound of the range
      // Ex: for 2048, the lower bound is 1024
      const xValues = Array.from(xyMap.keys()).map(value => {
        const upperbound = size(value);
        const upperboundPower = Math.log2(value);
        // For 1024 which is 2^10, the lowerbound is 0, since we start binning
        // after 2^10
        const lowerbound = upperboundPower > 10 ? size(2 ** (upperboundPower - 1)) : size(0);
        return `${lowerbound} - ${upperbound}`;
      });
      this.setState({
        plotData: [{
          type: 'bar',
          x: xValues,
          y: Array.from(xyMap.values()),
          name: 'file count'
        }]
      });
    }
  };

  componentDidMount(): void {
    // Fetch file size counts on component mount
    this.setState({
      isLoading: true
    });
    axios.get('/api/v1/utilization/fileCount').then(response => {
      const fileCountsResponse: IFileCountResponse[] = response.data;
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
            map.set(volume, new Set().add(bucket));
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
        volumeOptions
      }, () => {
        this.updatePlotData();
        // Select all volumes by default
        this.handleVolumeChange([allVolumesOption, ...volumeOptions], {action: 'select-option'});
      });
    }).catch(error => {
      this.setState({
        isLoading: false
      });
      showDataFetchError(error.toString());
    });
  }

  render() {
    const {plotData, isLoading, selectedBuckets, volumeOptions,
      selectedVolumes, fileCountsResponse, bucketOptions, isBucketSelectionDisabled} = this.state;
    return (
      <div className='insights-container'>
        <div className='page-header'>
          Insights
        </div>
        <div className='content-div'>
          {isLoading ? <span><Icon type='loading'/> Loading...</span> :
            ((fileCountsResponse && fileCountsResponse.length > 0) ?
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
                  <Col>
                    <Plot
                      data={plotData}
                      layout={
                        {
                          width: 800,
                          height: 600,
                          title: 'File Size Distribution',
                          showlegend: true
                        }
                      }/>
                  </Col>
                </Row>
              </div> :
              <div>No data to visualize file size distribution. Add files to Ozone to see a visualization on file size distribution.</div>)}
        </div>
      </div>
    );
  }
}
