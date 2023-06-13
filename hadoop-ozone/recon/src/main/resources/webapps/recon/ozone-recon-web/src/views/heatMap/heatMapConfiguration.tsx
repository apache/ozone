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

import React, { Component } from 'react';
import { AgChartsReact } from 'ag-charts-react';
import {byteToSize} from 'utils/common';

export default class HeatMapConfiguration extends Component {
  constructor(props: {} | Readonly<{}>) {
    super(props);
    const { data } = this.props;

    const colorRange1 = [
      '#ffff99',  //yellow start 80%
      '#ffff80',  //75%
      '#ffff66',  //70%
      '#ffff4d',  //yellow 65%
      '#ffd24d',  //dark Mustered yellow start 65%
      '#ffbf00',   //dark Mustard yellow end 50%
      '#b38600',  //Dark Mustard yellow 35%
      '#ffb366',  //orange start 70%
      '#ff9933',  //orange 60%
      '#ff8c1a',  //55%
      '#e67300',  //45%
      '#994d00',  //orange end 30%
      '#ff6633',  //Red start 60%
      '#ff4000',   // Red 50%
      '#cc3300',   //40%
      '#992600',   //30%
      '#802000',   //25%
      '#661a00',   //20%
      '#4d1300',   // 15%
      '#330000',    //dark Maroon
      '#330d00',   //10 % Last Red
    ];

    this.state = {
      // Tree Map Options Start
      options: {
        data,
        series: [
          {
          type: 'treemap',
          labelKey: 'label',// the name of the key to fetch the label value from
          sizeKey: 'normalizedSize',// the name of the key to fetch the value that will determine tile size
          colorkey: 'color',
          fontSize: 35,
          title: { color: 'white', fontSize: 18, fontFamily:'Courier New' },
          subtitle: { color: 'white', fontSize: 15, fontFamily:'Courier New' },
          tooltip: {
            renderer: (params) => {
              return {
                content: this.tooltipContent(params)
              };
            }
          },
          formatter: ({ highlighted}) => {
            const stroke = highlighted ? 'yellow' : 'white';
            return { stroke };
          },
          labels: {
            color: 'white',
            fontWeight: 'bold',
            fontSize: 12
          },
          tileStroke: 'white',
          tileStrokeWidth: 1,
          colorDomain: [0.000, 0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.350, 0.400, 0.450, 0.500, 0.550, 0.600, 0.650, 0.700, 0.750, 0.800, 0.850, 0.900, 0.950, 1.000],
          colorRange: [...colorRange1],
          groupFill: 'black',
          nodePadding: 1, //Disatnce between two nodes
          labelShadow: { enabled: false }, //labels shadow
          highlightStyle: {
            text: {
              color: 'yellow',
            },
            item:{
              fill: undefined,
            },
          },
          listeners: {
              nodeClick: (event) => {
              var data = event.datum;
              if (data.path) {
                this.props.onClick(data.path);
              }
              },
            },
        }],
        title: { text: 'Volumes and Buckets'},
        }
    // Tree Map Options End
    }
  };

    
  tooltipContent = (params:any) => {
    let tooltipContent = `<span>
      Size:
      ${byteToSize(params.datum.size, 1)}
      `;
      if(params.datum.accessCount!==undefined ){
        tooltipContent += `<br/>
        Access count:
      ${params.datum.accessCount }
    `;}
    if (params.datum.label !== "") {
        tooltipContent += `<br/>
          File Name:
          ${params.datum.label ? params.datum.label.split("/").slice(-1) : "no"}
        `;
      }
    tooltipContent += '</span>';
    return tooltipContent;
  };

  render() {
    const { options } = this.state;
    return (
      <AgChartsReact options={options} />
    );
  }

}

