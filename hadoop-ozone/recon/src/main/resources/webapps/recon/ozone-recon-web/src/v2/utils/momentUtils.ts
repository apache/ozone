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

import moment from "moment";

moment.updateLocale('en', {
  relativeTime: {
    past: '%s ago',
    s: '%ds',
    m: '1min',
    mm: '%dmins',
    h: '1hr',
    hh: '%dhrs',
    d: '1d',
    dd: '%dd',
    M: '1m',
    MM: '%dm',
    y: '1y',
    yy: '%dy'
  }
});

export function getTimeDiffFromTimestamp(timestamp: number): string {
  const timestampDate = new Date(timestamp);
  return moment(timestampDate).fromNow();
}

export function getDurationFromTimestamp(timestamp: number): string {
  const duration: moment.Duration = moment.duration(timestamp, 'milliseconds');
  // return nothing when the duration is falsy or not correctly parsed (P0D)
  if(!duration || duration.toISOString() === "P0D") return '';

  let elapsedTime = [];
  const durationBreakdowns: Record<string, number> = {
    'y': Math.floor(duration.years()),
    'm': Math.floor(duration.months()),
    'd': Math.floor(duration.days()),
    'h': Math.floor(duration.hours()),
    'min': Math.floor(duration.minutes()),
    's': Math.floor(duration.seconds())
  }

  for (const [key, value] of Object.entries(durationBreakdowns)) {
    value > 0 && elapsedTime.push(value + key);
  }

  return (elapsedTime.length === 0) ? 'Just now' : elapsedTime.join(' ');
}

export function getFormattedTime(time: number | string, format: string) {
  if (typeof time === 'string') return moment(time).format(format);
  return (time > 0) ? moment(time).format(format) : 'N/A';
}
