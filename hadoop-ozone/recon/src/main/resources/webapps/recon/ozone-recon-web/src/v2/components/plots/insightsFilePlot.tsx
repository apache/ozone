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
import filesize from 'filesize';
import { EChartsOption } from 'echarts';
import { ValueType } from 'react-select';

import EChart from '@/v2/components/eChart/eChart';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import { FileCountResponse, FilePlotData } from '@/v2/types/insights.types';


//-----Types------
type FileSizeDistributionProps = {
  volumeOptions: Option[];
  volumeBucketMap: Map<string, Set<string>>;
  fileCountResponse: FileCountResponse[];
  fileCountError: string | undefined;
}

const size = filesize.partial({ standard: 'iec', round: 0 });

const dropdownStyles: React.CSSProperties = {
  display: 'flex',
  justifyContent: 'space-between'
}

const FileSizeDistribution: React.FC<FileSizeDistributionProps> = ({
  volumeOptions = [],
  volumeBucketMap,
  fileCountResponse,
  fileCountError
}) => {

  const [bucketOptions, setBucketOptions] = React.useState<Option[]>([]);
  const [selectedBuckets, setSelectedBuckets] = React.useState<Option[]>([]);
  const [selectedVolumes, setSelectedVolumes] = React.useState<Option[]>([]);
  const [isBucketSelectionEnabled, setBucketSelectionEnabled] = React.useState<boolean>(false);

  const [filePlotData, setFilePlotData] = React.useState<FilePlotData>({
    fileCountValues: [],
    fileCountMap: new Map<number, number>()
  });

  function handleVolumeChange(selectedVolumes: ValueType<Option, true>) {

    // Disable bucket selection options if more than one volume is selected or no volumes present
    // If there is only one volume then the bucket selection is enabled
    const bucketSelectionDisabled = ((selectedVolumes as Option[])?.length > 1
      && volumeBucketMap.size !== 1);

    let bucketOptions: Option[] = [];

    // Update buckets if only one volume is selected
    if (selectedVolumes?.length === 1) {
      const selectedVolume = selectedVolumes[0].value;
      if (volumeBucketMap.has(selectedVolume)) {
        bucketOptions = Array.from(
          volumeBucketMap.get(selectedVolume)!
        ).map(bucket => ({
          label: bucket,
          value: bucket
        }));
      }
    }
    setBucketOptions([...bucketOptions]);
    setSelectedVolumes(selectedVolumes as Option[]);
    setSelectedBuckets([...bucketOptions]);
    setBucketSelectionEnabled(!bucketSelectionDisabled);
  }

  function handleBucketChange(selectedBuckets: ValueType<Option, true>) {
    setSelectedBuckets(selectedBuckets as Option[]);
  }

  function updatePlotData() {
    // Aggregate count across volumes and buckets for use in plot
    let filteredData = fileCountResponse;
    const selectedVolumeValues = new Set(selectedVolumes.map(option => option.value));
    const selectedBucketValues = new Set(selectedBuckets.map(option => option.value));
    if (selectedVolumes.length >= 0) {
      // Not all volumes are selected, need to filter based on the selected values
      filteredData = filteredData.filter(data => selectedVolumeValues.has(data.volume));

      // We have selected a volume but all the buckets are deselected
      if (selectedVolumes.length === 1 && selectedBuckets.length === 0) {
        // Since no buckets are selected there is no data
        filteredData = [];
      }
    }
    if (selectedBuckets.length > 0) {
      // Not all buckcets are selected, filter based on the selected values
      filteredData = filteredData.filter(data => selectedBucketValues.has(data.bucket));
    }

    // This is a map of 'size : count of the size'
    const fileCountMap: Map<number, number> = filteredData.reduce(
      (map: Map<number, number>, current) => {
        const fileSize = current.fileSize;
        const oldCount = map.get(fileSize) ?? 0;
        map.set(fileSize, oldCount + current.count);
        return map;
      },
      new Map<number, number>
    );

    // Calculate the previous power of 2 to find the lower bound of the range
    // Ex: for 2048, the lower bound is 1024
    const fileCountValues = Array.from(fileCountMap.keys()).map(value => {
      const upperbound = size(value);
      const upperboundPwr = Math.log2(value);
      // For 1024 i.e 2^10, the lower bound is 0, so we start binning after 2^10
      const lowerbound = upperboundPwr > 10 ? size(2 ** (upperboundPwr - 1)) : size(0);
      return `${lowerbound} - ${upperbound}`;
    });

    setFilePlotData({
      fileCountValues: fileCountValues,
      // set the sorted value by size for the map
      fileCountMap: new Map([...fileCountMap.entries()].sort((a, b) => a[0] - b[0]))
    });
  }

  // If the response is updated or the volume-bucket data is updated, update plot
  React.useEffect(() => {
    updatePlotData();
    handleVolumeChange(volumeOptions);
  }, [
    fileCountResponse, volumeBucketMap
  ]);

  // If the selected volumes and buckets change, update plot
  React.useEffect(() => {
    updatePlotData();
  }, [selectedVolumes, selectedBuckets])

  const { fileCountValues, fileCountMap } = filePlotData;

  const filePlotOptions: EChartsOption = {
    xAxis: {
      type: 'category',
      data: [...fileCountValues] ?? []
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
      data: Array.from(fileCountMap?.values() ?? []),
      type: 'bar'
    },
    graphic: (fileCountError) ? {
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
            text: `No data available. ${fileCountError}`,
            font: '20px sans-serif'
          }
        }
      ]
    } : undefined
  }

  return (<>
    <div style={dropdownStyles}>
      <MultiSelect
        options={volumeOptions}
        defaultValue={selectedVolumes}
        selected={selectedVolumes}
        placeholder='Volumes'
        onChange={handleVolumeChange}
        onTagClose={() => { }}
        fixedColumn=''
        columnLength={volumeOptions.length}
        style={{
          control: (baseStyles, state) => ({
            ...baseStyles,
            minWidth: 345
          })
        }} />
      <MultiSelect
        options={bucketOptions}
        defaultValue={selectedBuckets}
        selected={selectedBuckets}
        placeholder='Buckets'
        onChange={handleBucketChange}
        onTagClose={() => { }}
        fixedColumn=''
        columnLength={bucketOptions.length}
        isDisabled={!isBucketSelectionEnabled}
        style={{
          control: (baseStyles, state) => ({
            ...baseStyles,
            minWidth: 345
          })
        }} />
    </div>
    <EChart option={filePlotOptions} style={{
      width: '30vw',
      height: '55.5vh',
      marginTop: '5vh'
    }} />
  </>)
}

export default FileSizeDistribution;