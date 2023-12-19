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
import Plot from 'react-plotly.js';
import * as Plotly from 'plotly.js';


interface ChartState {
    windowWidth: number;
    windowHeight: number; 
}

interface ChartProps{
    plotData: Plotly.Data[];
    plotOnClickHandler?: ((event: Readonly<Plotly.PlotMouseEvent>) => void) | undefined;
    title: string;
}

export class PieChartComponent extends React.Component<ChartProps, ChartState> {
    constructor(props = {
        plotData: [],
        title: ""
    }) {
        super(props);
        this.state = {
            windowWidth: window.innerWidth,
            windowHeight: window.innerHeight
        };
    }

    updateWindowSize = () => {
        this.setState({
            ...this.state,
            windowHeight: window.innerHeight,
            windowWidth: window.innerWidth
        });
    }

    parseLabels = (label: string, charlen: number) => {
        let res = "";
        while(label.length > 0){
            res += label.substring(0, charlen) + "<br>";
            label = label.substring(charlen)
        }
        return res;
    }

    componentDidMount(): void {
        window.addEventListener('resize', this.updateWindowSize);
        // this.props.plotData.forEach((data, idx) => {
        //     if (data && Object.keys(data).length !== 0){
        //         let wrappedLabels = data["labels"].map((label: string) => {
        //             if (label.length < 50)
        //                 return label
        //             else
        //                 return this.parseLabels(label, 50)
        //         })
        //         this.props.plotData[idx]["labels"] = wrappedLabels;
        //     }
        // })
    }

    componentWillUnmount(): void {
        window.removeEventListener('resize', this.updateWindowSize);
    }

    render() {
        const { windowHeight, windowWidth } = this.state;
        let layoutProps: Partial<Plotly.Legend> = 
        {
            width: windowWidth * 0.8,
            height: windowHeight - 200,
            font: {
                family: 'Roboto, sans-serif',
                size: windowWidth * 0.009
            },
            showlegend: true,
            legend: {
                font: {
                    size: windowWidth * 0.008
                }
            },
            title: {
                text: this.props.title,
                font: {
                    size: 20
                }
            },
            margin: {
                l: windowWidth * 0.3
            }
        }
        if (windowWidth < 1200) {
            // We are now almost at tablet/small size screen
            layoutProps["width"] = windowWidth * 0.7
            layoutProps["height"] = windowHeight * 0.9
            layoutProps["legend"]["orientation"] = "h"
            // layoutProps["width"] = windowWidth * 
            layoutProps["legend"]["x"] = 0.4;
            layoutProps["legend"]["xanchor"] = "center";
            layoutProps["legend"]["y"] = (- windowHeight);
            layoutProps["legend"]["yanchor"] = "bottom";
            layoutProps["legend"]["font"]["size"] = windowWidth * 0.009;
            layoutProps["margin"]["b"] = windowHeight * 0.01
        }

        return (
            <Plot
                data={this.props.plotData}
                layout={
                    layoutProps
                }
                onClick={this.props.plotOnClickHandler}/>
        );
    }
}
