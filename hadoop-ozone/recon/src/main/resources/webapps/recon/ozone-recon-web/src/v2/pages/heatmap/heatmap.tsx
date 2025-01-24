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
import React, { ChangeEvent, useRef, useState } from 'react';
import moment, { Moment } from 'moment';
import { Row, Button, Menu, Input, Dropdown, DatePicker, Form, Result, message, Spin } from 'antd';
import { MenuProps } from 'antd/es/menu';
import { DownOutlined, LoadingOutlined, UndoOutlined } from '@ant-design/icons';


import { showDataFetchError } from '@/utils/common';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
import * as CONSTANTS from '@/v2/constants/heatmap.constants';
import { HeatmapChild, HeatmapResponse, HeatmapState, InputPathState, InputPathValidTypes, IResponseError } from '@/v2/types/heatmap.types';
import HeatmapPlot from '@/v2/components/plots/heatmapPlot';

import './heatmap.less';
import { useLocation } from 'react-router-dom';
import { AxiosResponse } from 'axios';

let minSize = Infinity;
let maxSize = 0;

const Heatmap: React.FC<{}> = () => {

  const [state, setState] = useState<HeatmapState>({
    heatmapResponse: {
      label: '',
      path: '',
      children: [],
      size: 0,
      maxAccessCount: 0,
      minAccessCount: 0
    },
    entityType: CONSTANTS.ENTITY_TYPES[0],
    date: CONSTANTS.TIME_PERIODS[0]
  });

  const [inputPathState, setInputPathState] = useState<InputPathState>({
    inputPath: CONSTANTS.ROOT_PATH,
    isInputPathValid: undefined,
    helpMessage: ''
  });

  const [isLoading, setLoading] = useState<boolean>(false);
  const [treeEndpointFailed, setTreeEndpointFailed] = useState<boolean>(false);

  const location = useLocation();
  const cancelSignal = useRef<AbortController>();
  const cancelDisabledFeatureSignal = useRef<AbortController>();

  const [isHeatmapEnabled, setIsHeatmapEnabled] = useState<boolean>(location?.state?.isHeatmapEnabled);


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
    updateHeatmap(inputPathState.inputPath, state.entityType, state.date);
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
        obj['normalizedSize'] = 0;
      } else if ((obj as HeatmapResponse)?.size === 0 && (obj as HeatmapResponse)?.maxAccessCount === 0) {
        obj['normalizedSize'] = 0;
      }
      else if (obj?.size === 0 && ((obj as HeatmapChild)?.accessCount >= 0 || (obj as HeatmapResponse).maxAccessCount >= 0)) {
        obj['normalizedSize'] = 1;
        obj.size = 0;
      }
      else {
        const newSize = normalize(minSize, maxSize, obj.size);
        obj['normalizedSize'] = newSize;
      }
    }

    if (obj.hasOwnProperty('children')) {
      (obj as HeatmapResponse)?.children.forEach(child => updateSize(child));
    }
    return obj as HeatmapResponse;
  };

  const updateHeatmap = (path: string, entityType: string, date: string | number) => {
    // Only perform requests if the heatmap is enabled
    if (isHeatmapEnabled) {
      setLoading(true);
      // We want to ensure these are not empty as they will be passed as path params
      if (date && path && entityType) {
        const { request, controller } = AxiosGetHelper(
          `/api/v1/heatmap/readaccess?startDate=${date}&path=${path}&entityType=${entityType}`,
          cancelSignal.current
        );
        cancelSignal.current = controller;

        request.then(response => {
          if (response?.status === 200) {
            minSize = response.data.minAccessCount;
            maxSize = response.data.maxAccessCount;
            const heatmapResponse: HeatmapResponse = updateSize(response.data);
            setLoading(false);
            setState(prevState => ({
              ...prevState,
              heatmapResponse: heatmapResponse
            }));
          } else {
            const error = new Error((response.status).toString()) as IResponseError;
            error.status = response.status;
            error.message = `Failed to fetch Heatmap Response with status ${error.status}`
            throw error;
          }
        }).catch(error => {
          setLoading(false);
          setInputPathState(prevState => ({
            ...prevState,
            inputPath: CONSTANTS.ROOT_PATH
          }));
          setTreeEndpointFailed(true);
          if (error.response.status !== 404) {
            showDataFetchError(error.message.toString());
          }
        });
      } else {
        setLoading(false);
      }

    }
  }

  const updateHeatmapParent = (path: string) => {
    setInputPathState(prevState => ({
      ...prevState,
      inputPath: path
    }));
  }

  function isDateDisabled(current: Moment) {
    return current > moment() || current < moment().subtract(90, 'day');
  }

  function getIsHeatmapEnabled() {
    const disabledfeaturesEndpoint = `/api/v1/features/disabledFeatures`;
    const { request, controller } = AxiosGetHelper(
      disabledfeaturesEndpoint,
      cancelDisabledFeatureSignal.current
    )
    cancelDisabledFeatureSignal.current = controller;
    request.then(response => {
      setIsHeatmapEnabled(!response?.data?.includes('HEATMAP'));
    }).catch(error => {
      showDataFetchError((error as Error).toString());
    });
  }

  React.useEffect(() => {
    // We do not know if heatmap is enabled or not, so set it
    if (isHeatmapEnabled === undefined) {
      getIsHeatmapEnabled();
    }
    updateHeatmap(inputPathState.inputPath, state.entityType, state.date);

    return (() => {
      cancelSignal.current && cancelSignal.current.abort();
    })
  }, [isHeatmapEnabled, state.entityType, state.date]);

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
        <Menu.Item key='heatmapDatePicker'>
          <DatePicker
            format="YYYY-MM-DD"
            onChange={handleDatePickerChange}
            onClick={(e) => { e.stopPropagation() }}
            disabledDate={isDateDisabled} />
        </Menu.Item>
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
              {isLoading
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