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

import React, { useRef, useEffect } from "react";
import { init, getInstanceByDom } from 'echarts';
import type { CSSProperties } from "react";
import type { EChartsOption, ECharts, SetOptionOpts } from 'echarts';

export interface EChartProps {
    option: EChartsOption;
    style?: CSSProperties;
    settings?: SetOptionOpts;
    loading?: boolean;
    theme?: 'light';
    onClick?: () => any | void;
}

export function EChart({
    option,
    style,
    settings,
    loading,
    theme,
    onClick
}: EChartProps): JSX.Element {
    const chartRef = useRef<HTMLDivElement>(null);
    useEffect(() => {
        // Initialize chart
        let chart: ECharts | undefined;
        if (chartRef.current !== null) {
            chart = init(chartRef.current, theme);
            if (onClick) {
                chart.on('click', onClick);
            }
        }

        // Add chart resize listener
        // ResizeObserver is leading to a bit janky UX
        function resizeChart() {
            chart?.resize();
        }
        window.addEventListener("resize", resizeChart);

        // Return cleanup function
        return () => {
            chart?.dispose();
            window.removeEventListener("resize", resizeChart);
        };
    }, [theme]);

    useEffect(() => {
        // Update chart
        if (chartRef.current !== null) {
            const chart = getInstanceByDom(chartRef.current);
            chart.setOption(option, settings);
            if (onClick) {
                chart.on('click', onClick);
            }
        }
    }, [option, settings, theme]); // Whenever theme changes we need to add option and setting due to it being deleted in cleanup function

    useEffect(() => {
        // Update chart
        if (chartRef.current !== null) {
            const chart = getInstanceByDom(chartRef.current);
            // eslint-disable-next-line @typescript-eslint/no-unused-expressions
            loading === true ? chart.showLoading() : chart.hideLoading();
        }
    }, [loading, theme]);

    return <div ref={chartRef} style={{ width: "100em", height: "50em", margin: 'auto', ...style }}/>;
}
