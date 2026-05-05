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
import { Button } from 'antd';
import { ArrowLeftOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router-dom';

// Declare SVG constant instead of importing a file to help loading
const notFoundIcon = (
  <svg width="100%" height="100%" viewBox="0 0 1080 1080" style={{ fillRule: 'evenodd', clipRule: 'evenodd', strokeLinecap: 'round', strokeLinejoin: 'round', strokeMiterlimit: 1.5 }}>
    <g transform="matrix(1.44095,0,0,1.44095,-185.216,-251.373)">
      <g transform="matrix(0.944978,0,0,0.914657,24.9329,68.941)">
        <path d="M247.428,443.796C248.822,443.952 391.105,376.828 247.428,443.796C103.751,510.764 115.658,684.797 231.483,694.498C347.308,704.199 384.351,672.783 454.345,635.793C524.34,598.803 525.667,599.868 561.552,593.322C597.437,586.777 583.99,603.712 574.32,632.611C564.65,661.511 493.253,787.738 741.087,710.394C988.922,633.051 882.616,522.813 771.238,531.547C659.86,540.281 579.512,617.269 590.123,579.339C600.734,541.408 714.633,402.262 579.57,382.851C444.507,363.439 246.035,443.641 247.428,443.796Z" style={{ fill: 'rgb(9,209,118)', fillOpacity: 0.6, stroke: 'black', strokeOpacity: 0, strokeWidth: '8.96px' }} />
      </g>
      <g transform="matrix(0.644866,-1.18562e-16,-8.57454e-17,-0.27393,51.8343,793.841)">
        <rect x="215.614" y="745.673" width="711.164" height="5.027" style={{ fill: 'rgb(4,15,66)', stroke: 'black', strokeOpacity: 0, strokeWidth: '16.81px' }} />
      </g>
      <g transform="matrix(0.0449181,2.51391e-16,-3.87546e-16,-0.298306,128.725,812.065)">
        <rect x="215.614" y="745.673" width="711.164" height="5.027" style={{ fill: 'rgb(4,15,66)', stroke: 'black', strokeOpacity: 0, strokeWidth: '39.04px' }} />
      </g>
      <g transform="matrix(0.0536855,1.21321e-16,-4.8904e-18,0.178017,501.717,463.557)">
        <rect x="215.614" y="745.673" width="711.164" height="5.027" style={{ fill: 'rgb(4,15,66)', stroke: 'black', strokeOpacity: 0, strokeWidth: '63.34px' }} />
      </g>
      <g transform="matrix(14.9468,0,0,14.9468,-2160.92,-6553.61)">
        <g transform="matrix(24,0,0,24,170.176,478.583)">
        </g>
        <text x="156.406px" y="478.583px" style={{ fontFamily: 'Roboto-Bold, Roboto', fontWeight: 700, fontSize: '24px', fill: 'rgb(11,36,0)' }}>4</text>
      </g>
      <g transform="matrix(1.74862,0,0,1.74862,-357.721,-43.1954)">
        <g transform="matrix(192,0,0,192,526.902,357.026)">
        </g>
        <text x="416.746px" y="357.026px" style={{ fontFamily: 'Roboto-Bold, Roboto', fontWeight: 700, fontSize: '192px', fill: 'rgb(11,36,0)' }}>0</text>
      </g>
      <g transform="matrix(21.2052,0,0,21.2052,-2759.51,-9389.18)">
        <g transform="matrix(24,0,0,24,170.176,478.583)">
        </g>
        <text x="156.406px" y="478.583px" style={{ fontFamily: 'Roboto-Bold, Roboto', fontWeight: 700, fontSize: '24px', fill: 'rgb(11,36,0)' }}>4</text>
      </g>
      <g transform="matrix(1,0,0,1,-27,0)">
        <g transform="matrix(1,0,0,0.589632,0,242.476)">
          <path d="M617.69,423.839L617.69,589.68C617.69,590.339 617.375,590.874 616.986,590.874L615.578,590.874C615.19,590.874 614.874,590.339 614.874,589.68L614.874,423.839C614.874,423.18 615.19,422.645 615.578,422.645L616.986,422.645C617.375,422.645 617.69,423.18 617.69,423.839Z" style={{ fill: 'rgb(23,51,38)', stroke: 'rgb(43,43,43)', strokeOpacity: 0, strokeWidth: '0.85px' }} />
        </g>
        <g transform="matrix(0.851104,0.0593827,-0.0701034,1.00476,128.082,-24.3374)">
          <path d="M601.027,497.013L640.442,497.013L648.218,502.656L641.04,509.834L600.77,509.834L601.027,497.013Z" style={{ fill: 'rgb(9,172,98)', stroke: 'rgb(43,43,43)', strokeOpacity: 0, strokeWidth: '0.74px' }} />
        </g>
        <g transform="matrix(-0.716372,0,0,1,1058.43,-0.0408301)">
          <path d="M601.027,497.013L640.442,497.013L650.733,504.094L641.04,509.834L600.77,509.834L601.027,497.013Z" style={{ fill: 'rgb(0,166,91)', stroke: 'rgb(43,43,43)', strokeOpacity: 0, strokeWidth: '0.8px' }} />
        </g>
      </g>
      <g transform="matrix(0.100667,0,0,0.100667,514.784,673.301)">
        <g transform="matrix(22,0,0,22,1001.53,645.714)">
        </g>
        <text x="846.825px" y="645.714px" style={{ fontFamily: 'Roboto-Bold, Roboto', fontWeight: 700, fontSize: '22px', fill: 'rgb(108,217,156)' }}>de<tspan x="870.974px 881.931px " y="645.714px 645.714px ">va</tspan>bhishekpal</text>
      </g>
    </g>
  </svg>
)

const contentStyles: React.CSSProperties = {
  height: '80vh',
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'center',
  alignItems: 'center',
  gap: '20px',
  fontSize: '18px',
  color: '#8a8a8a',
  fontWeight: 400
}

const NotFound: React.FC<{}> = () => {
  const history = useHistory();

  return (
    <div style={contentStyles}>
      <div style={{ width: '100%', height: '70%' }}> {notFoundIcon} </div>
      The page you visited does not exist
      <Button type='primary'
        onClick={() => { history.goBack() }}>
        <ArrowLeftOutlined />
        Go Back
      </Button>
    </div>
  )
}

export default NotFound;