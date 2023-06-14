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
import { AgChartsReact } from 'ag-charts-react';
import { byteToSize } from 'utils/common';

interface ITreeResponse {
  label: string;
  children: IChildren[];
  size: number;
  path: string;
  minAccessCount: number;
  maxAccessCount: number;
}

interface IChildren {
  label: string;
  size: number;
  accessCount: number;
  color: number;
}

interface IHeatmapConfigurationProps {
  data: ITreeResponse[];
  onClick: Function;
}

const colourScheme = {
  pastel_greens: [
    '#CCFFD9', //light green start (least accessed)
    '#B9FBD5',
    '#A7F7D1',
    '#94F2CD',
    '#82EEC9',
    '#6FEAC5',
    '#5DE6C2',
    '#4AE2BE',
    '#38DEBA',
    '#25D9B6',
    '#13D5B2',
    '#00D1AE', //dark green ends (light to moderate accces)
    '#FFD28F', //light orange (moderate access)
    '#FFC58A',
    '#FFB984',
    '#FEAC7F',
    '#FE9F7A',
    '#FE9274',
    '#FE866F',
    '#FD7969',
    '#FD6C64' //red (most accessed)
  ]
};

export default class HeatMapConfiguration extends React.Component<IHeatmapConfigurationProps> {
  constructor(props: IHeatmapConfigurationProps) {
    super(props);
    const { data } = this.props;
    this.state = {
      // Tree Map Options Start
      options: {
        data,
        series: [{
          type: 'treemap',
          labelKey: 'label',// the name of the key to fetch the label value from
          sizeKey: 'normalizedSize',// the name of the key to fetch the value that will determine tile size
          colorKey: 'color',
          title: { color: '#424242', fontSize: 16, fontFamily: 'Roboto', fontWeight: '600' },
          subtitle: { color: '#424242', fontSize: 12, fontFamily: 'Roboto', fontWeight: '400' },
          tooltip: {
            renderer: (params) => {
              return {
                content: this.tooltipContent(params)
              };
            }
          },
          formatter: ({highlighted}) => {
            const stroke = highlighted ? '#CED4D9' : '#FFFFFF';
            return { stroke };
          },
          labels: {
            color: 'white',
            fontWeight: 'bold',
            fontSize: 12
          },
          tileStroke: 'white',
          tileStrokeWidth: 1.5,
          colorDomain: [0.000, 0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.350, 0.400, 0.450, 0.500, 0.550, 0.600, 0.650, 0.700, 0.750, 0.800, 0.850, 0.900, 0.950, 1.000],
          colorRange: [...colourScheme["pastel_greens"]],
          groupFill: '#E6E6E6',
          groupStroke: "#E1E2E6",
          nodePadding: 3.5,
          labelShadow: { enabled: false }, //labels shadow
          gradient: false,
          highlightStyle: {
            text: {
              color: '#424242',
            },
            item: {
              fill: undefined,
            },
          },
          listeners: {
            nodeClick: (event) => {
              const data = event.datum;
              if (data.path) {
                this.props.onClick(data.path);
              }
            },
          },
        }],
        title: { text: 'Volumes and Buckets' },
      }
      // Tree Map Options End
    };
  };


  tooltipContent = (params: any) => {
    let tooltipContent = `<span>
      Size:
      ${byteToSize(params.datum.size, 1)}
      `;
    if (params.datum.accessCount !== undefined) {
      tooltipContent += `<br/>
        Access count:
      ${params.datum.accessCount}
    `;
    }
    if (params.datum.label !== "") {
      tooltipContent += `<br/>
          File Name:
          ${params.datum.label ? params.datum.label.split("/").slice(-1) : "None"}
        `;
    }
    tooltipContent += '</span>';
    return tooltipContent;
  };

  render() {
    const { options } = this.state;
    return <AgChartsReact options={options} />;
  }
}
