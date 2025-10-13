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
import React, { ChangeEvent, useState, useEffect, useCallback } from 'react';
import moment, { Moment } from 'moment';
import { Button, Menu, Input, Dropdown, DatePicker, Form, Result, Spin } from 'antd';
import { MenuProps } from 'antd/es/menu';
import { DownOutlined } from '@ant-design/icons';

import { showDataFetchError } from '@/utils/common';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import * as CONSTANTS from '@/v2/constants/heatmap.constants';
import { HeatmapChild, HeatmapResponse, HeatmapState, InputPathState, InputPathValidTypes, IResponseError } from '@/v2/types/heatmap.types';
import HeatmapPlot from '@/v2/components/plots/heatmapPlot';

import './heatmap.less';
import { useLocation } from 'react-router-dom';

let minSize = Infinity;
let maxSize = 0;

const DEFAULT_HEATMAP_RESPONSE: HeatmapResponse = {
  label: '',
  path: '',
  children: [],
  size: 0,
  maxAccessCount: 0,
  minAccessCount: 0
};

const DEFAULT_DISABLED_FEATURES_RESPONSE = {
  data: []
};

const Heatmap: React.FC<{}> = () => {
  const [state, setState] = useState<HeatmapState>({
    heatmapResponse: DEFAULT_HEATMAP_RESPONSE,
    entityType: CONSTANTS.ENTITY_TYPES[0],
    date: CONSTANTS.TIME_PERIODS[0]
  });

  const [inputPathState, setInputPathState] = useState<InputPathState>({
    inputPath: CONSTANTS.ROOT_PATH,
    isInputPathValid: undefined,
    helpMessage: ''
  });

  const [searchPath, setSearchPath] = useState<string>(CONSTANTS.ROOT_PATH);
  const [treeEndpointFailed, setTreeEndpointFailed] = useState<boolean>(false);

  const location = useLocation();
  const [isHeatmapEnabled, setIsHeatmapEnabled] = useState<boolean>((location?.state as any)?.isHeatmapEnabled);

  // Use the modern hooks pattern for heatmap data - only trigger on searchPath change
  const heatmapData = useApiData<HeatmapResponse>(
    isHeatmapEnabled && state.date && searchPath && state.entityType
      ? `/api/v1/heatmap/readaccess?startDate=${state.date}&path=${searchPath}&entityType=${state.entityType}`
      : '',
    DEFAULT_HEATMAP_RESPONSE,
    {
      retryAttempts: 2,
      onError: (error: any) => {
        if (error.response?.status !== 404) {
          showDataFetchError(error.message.toString());
        }
        setTreeEndpointFailed(true);
        setInputPathState(prevState => ({
          ...prevState,
          inputPath: CONSTANTS.ROOT_PATH
        }));
        setSearchPath(CONSTANTS.ROOT_PATH);
      }
    }
  );

  // Use the modern hooks pattern for disabled features
  const disabledFeaturesData = useApiData<{ data: string[] }>(
    '/api/v1/features/disabledFeatures',
    DEFAULT_DISABLED_FEATURES_RESPONSE,
    {
      retryAttempts: 2,
      onError: (error: any) => showDataFetchError(error)
    }
  );

  // Process heatmap data when it changes
  useEffect(() => {
    if (heatmapData.data && heatmapData.data.label !== '') {
      minSize = heatmapData.data.minAccessCount;
      maxSize = heatmapData.data.maxAccessCount;
      const heatmapResponse: HeatmapResponse = updateSize(heatmapData.data);
      setState(prevState => ({
        ...prevState,
        heatmapResponse: heatmapResponse
      }));
      setTreeEndpointFailed(false);
    }
  }, [heatmapData.data]);

  // Process disabled features data when it changes
  useEffect(() => {
    if (disabledFeaturesData.data && disabledFeaturesData.data.data) {
      setIsHeatmapEnabled(!disabledFeaturesData.data.data.includes('HEATMAP'));
    }
  }, [disabledFeaturesData.data]);

  function handleChange(e: ChangeEvent<HTMLInputElement>) {
    const value = e.target.value;
    // Only allow letters, numbers,underscores and forward slashes and hyphen
    const regex = /^[a-zA-Z0-9_/-]*$/;

    let inputValid = undefined;
    let helpMessage = '';
    if (!regex.test(value)) {
      helpMessage = 'Please enter valid path';
      inputValid = 'error';
    }
    setInputPathState({
      inputPath: value,
      isInputPathValid: inputValid as InputPathValidTypes,
      helpMessage: helpMessage
    });
  }

  function handleSubmit() {
    if (isHeatmapEnabled && state.date && inputPathState.inputPath && state.entityType) {
      setSearchPath(inputPathState.inputPath);
    }
  }

  const normalize = (min: number, max: number, size: number) => {
    // Since there can be a huge difference between the largest entity size
    // and the smallest entity size, it might cause some blocks to render smaller
    // we are normalizing the size to ensure all entities are visible
    //Normaized Size using Deviation and mid Point
    const mean = (max + min) / 2;
    const highMean = (max + mean) / 2;
    const lowMean1 = (min + mean) / 2;
    const lowMean2 = (lowMean1 + min) / 2;

    if (size > highMean) {
      const newsize = highMean + (size * 0.1);
      return (newsize);
    }
    if (size < lowMean2) {
      const diff = (lowMean2 - size) / 2;
      const newSize = lowMean2 - diff;
      return (newSize);
    }

    return size;
  };

  const updateSize = (obj: HeatmapResponse | HeatmapChild) => {
    //Normalize Size so other blocks also get visualized if size is large in bytes minimize and if size is too small make it big
    // it will only apply on leaf level as checking color property
    if (obj.hasOwnProperty('size') && obj.hasOwnProperty('color')) {

      // hide block at key,volume,bucket level if size accessCount and maxAccessCount are zero apply normalized size only for leaf level
      if ((obj as HeatmapChild)?.size === 0 && (obj as HeatmapChild)?.accessCount === 0) {
        (obj as any)['normalizedSize'] = 0;
      } else if ((obj as HeatmapResponse)?.size === 0 && (obj as HeatmapResponse)?.maxAccessCount === 0) {
        (obj as any)['normalizedSize'] = 0;
      }
      else if (obj?.size === 0 && ((obj as HeatmapChild)?.accessCount >= 0 || (obj as HeatmapResponse).maxAccessCount >= 0)) {
        (obj as any)['normalizedSize'] = 1;
        obj.size = 0;
      }
      else {
        const newSize = normalize(minSize, maxSize, obj.size);
        (obj as any)['normalizedSize'] = newSize;
      }
    }

    if (obj.hasOwnProperty('children')) {
      (obj as HeatmapResponse)?.children.forEach(child => updateSize(child));
    }
    return obj as HeatmapResponse;
  };

  const updateHeatmapParent = (path: string) => {
    setInputPathState(prevState => ({
      ...prevState,
      inputPath: path
    }));
    setSearchPath(path);
  }

  function isDateDisabled(current: Moment) {
    return current > moment() || current < moment().subtract(90, 'day');
  }

  const handleDatePickerChange = (date: moment.MomentInput) => {
    setState(prevState => ({
      ...prevState,
      date: moment(date).unix()
    }));
  };

  const handleMenuChange: MenuProps["onClick"] = (e) => {
    if (CONSTANTS.ENTITY_TYPES.includes(e.key as string)) {
      minSize = Infinity;
      maxSize = 0;
      setState(prevState => ({
        ...prevState,
        entityType: e.key as string,
      }));
    }
  };

  const handleCalendarChange: MenuProps["onClick"] = (e) => {
    if (CONSTANTS.TIME_PERIODS.includes(e.key as string)) {
      setState(prevState => ({
        ...prevState,
        date: e.key
      }));
    }
  };

  const { date, entityType, heatmapResponse } = state;
  const { inputPath, helpMessage, isInputPathValid } = inputPathState;
  const loading = heatmapData.loading || disabledFeaturesData.loading;

  const menuCalendar = (
    <Menu
      defaultSelectedKeys={[date as string]}
      onClick={handleCalendarChange}
      selectable={true}>
      <Menu.Item key={CONSTANTS.TIME_PERIODS[0]}>
        24 Hour
      </Menu.Item>
      <Menu.Item key={CONSTANTS.TIME_PERIODS[1]}>
        7 Days
      </Menu.Item>
      <Menu.Item key={CONSTANTS.TIME_PERIODS[2]}>
        90 Days
      </Menu.Item>
      <Menu.SubMenu title='Custom Select Last 90 Days'>
        <div>
          <DatePicker
            format="YYYY-MM-DD"
            onChange={handleDatePickerChange}
            onClick={(e) => { e.stopPropagation() }}
            disabledDate={isDateDisabled} />
        </div>
      </Menu.SubMenu>
    </Menu>
  );

  const entityTypeMenu = (
    <Menu
      defaultSelectedKeys={[entityType]}
      onClick={handleMenuChange}
      selectable={true}>
      <Menu.Item key={CONSTANTS.ENTITY_TYPES[2]}>
        Volume
      </Menu.Item>
      <Menu.Item key={CONSTANTS.ENTITY_TYPES[1]}>
        Bucket
      </Menu.Item>
      <Menu.Item key={CONSTANTS.ENTITY_TYPES[0]}>
        Key
      </Menu.Item>
    </Menu>
  );

  function getErrorContent() {
    if (!isHeatmapEnabled) {
      return <Result
        status='error'
        title='Heatmap Not Available'
        subTitle='Please ensure Heatmap is enabled in the configs and you have sufficient permissions' />
    }

    if (treeEndpointFailed) {
      return <Result
        status='error'
        title='Failed to fetch Heatmap'
        subTitle='Check for any failed requests for more information' />
    }
  }

  return (
    <>
      <div className='page-header-v2'>
        Heatmap
      </div>
      <div className='data-container'>
        {
          (!isHeatmapEnabled || treeEndpointFailed)
            ? getErrorContent()
            : <div className='content-div'>
              <div className='heatmap-header-section'>
                <div className='heatmap-filter-section'>
                  <div className='path-input-container'>
                    <h4 style={{ paddingTop: '2%' }}>Path</h4>
                    <Form.Item className='path-input-element' validateStatus={isInputPathValid} help={helpMessage}>
                      <Input.Search
                        allowClear
                        placeholder={CONSTANTS.ROOT_PATH}
                        name="inputPath"
                        value={inputPath}
                        onChange={handleChange}
                        onSearch={handleSubmit} />
                    </Form.Item>
                  </div>
                  <div className='entity-dropdown-button'>
                    <Dropdown
                      overlay={entityTypeMenu}
                      placement='bottomCenter'>
                      <Button>Entity Type:&nbsp;{entityType}<DownOutlined /></Button>
                    </Dropdown>
                  </div>
                  <div className='date-dropdown-button'>
                    <Dropdown
                      overlay={menuCalendar}
                      placement='bottomLeft'>
                      <Button>Last &nbsp;{(date as number) > 100 ? new Date((date as number) * 1000).toLocaleString() : date}<DownOutlined /></Button>
                    </Dropdown>
                  </div>
                </div>
                <div className='heatmap-legend-container'>
                  <div className='heatmap-legend-item'>
                    <div style={{ width: "13px", height: "13px", backgroundColor: `${CONSTANTS.colourScheme["amberAlert"][0]}`, marginRight: "5px" }}> </div>
                    <span>Less Accessed</span>
                  </div>
                  <div className='heatmap-legend-item'>
                    <div style={{ width: "13px", height: "13px", backgroundColor: `${CONSTANTS.colourScheme["amberAlert"][8]}`, marginRight: "5px" }}> </div>
                    <span>Moderate Accessed</span>
                  </div>
                  <div className='heatmap-legend-item'>
                    <div style={{ width: "13px", height: "13px", backgroundColor: `${CONSTANTS.colourScheme["amberAlert"][20]}`, marginRight: "5px" }}> </div>
                    <span>Most Accessed</span>
                  </div>
                </div>
              </div>
              {loading
                ? <Spin size='large' />
                : (Object.keys(heatmapResponse).length > 0 && (heatmapResponse.label !== null || heatmapResponse.path !== null))
                  ? <div id="heatmap-plot-container">
                    <HeatmapPlot
                      data={heatmapResponse}
                      onClick={updateHeatmapParent}
                      colorScheme={CONSTANTS.colourScheme['amberAlert']}
                      entityType={entityType} />
                  </div>
                  : <Result
                    status='warning'
                    title='No Data available' />
              }
            </div>
        }
      </div>
    </>
  );
}

export default Heatmap;
