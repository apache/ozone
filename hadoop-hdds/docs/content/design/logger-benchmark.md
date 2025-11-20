---
title: Audit Logger Benchmarks 
summary: Benchmarking the async and sync audit loggers using log4j2
date: 2025-11-02
jira: HDDS-13894
status: implemented
author: Rishabh Patel
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
## Introduction & Goal
   
This document presents the performance benchmark results comparing our current Asynchronous (Async) Logger with 
a new Synchronous (Sync) Logger configuration in Log4j2.
The primary goal of this testing is to validate the performance impact of transitioning our audit logging framework
to a Sync Logger. 
This transition is critical to guarantee the integrity and completeness of our audit logs, as the current Async Logger
can drop log events under high stress.


## Benchmark Methodology 
To evaluate the impact, we ran a series of tests using the ozone freon tool to simulate the OM being under pressure.

- We used a bare-metal cluster with a few configuration tweaks to perform these tests. 

- The OM log directory was moved from an SSD to an HDD to expose any issues when the disk is slow.

- Both of the OM `-Xms` and `-Xmx` java heap values were set to 32GB to avoid any allocation or GC related discrepancies.

- Key deletion service was disabled for the entire duration of the experiment.

- The differences between the Async and Sync OM audit logging configuration can be seen in 
[HDDS-13794](https://github.com/apache/ozone/pull/9229) 

  
### Workload Type:
The ockrw tests were perfomed to find the time that was taken to generate a certain number of keys.
The ommg tests were performed to find the number of keys that could be created in a limited amount of time.
Write-heavy (ockrw 0% Read), mixed read/write (ockrw 50% Read), and metadata-heavy (ommg CREATE_KEY) 
workloads were covered.

### Concurrency: 
Both high-concurrency (50 threads) and low-concurrency (1 thread) scenarios were covered.



## Results & Analysis
We compared the performance (in calls/second) of the existing Async Logger against the proposed Sync Logger.
- No significant difference was observed between the two logging configurations when the number of threads was 50.
- When using synchronized logging configuration, a decrease of ~10 calls/sec was observed for single-threaded tests when
compared against the async configuration.

<table class="waffle" cellspacing="0" cellpadding="0"><thead><tr><th class="row-header freezebar-vertical-handle"></th><th id="0C0" style="width:398px;" class="column-headers-background">A</th><th id="0C1" style="width:97px;" class="column-headers-background">B</th><th id="0C2" style="width:114px;" class="column-headers-background">C</th><th id="0C3" style="width:71px;" class="column-headers-background">D</th><th id="0C4" style="width:113px;" class="column-headers-background">E</th><th id="0C5" style="width:154px;" class="column-headers-background">F</th><th id="0C6" style="width:100px;" class="column-headers-background">G</th><th id="0C7" style="width:100px;" class="column-headers-background">H</th><th id="0C8" style="width:100px;" class="column-headers-background">I</th><th id="0C9" style="width:100px;" class="column-headers-background">J</th><th id="0C10" style="width:100px;" class="column-headers-background">K</th><th id="0C11" style="width:100px;" class="column-headers-background">L</th><th id="0C12" style="width:100px;" class="column-headers-background">M</th><th id="0C13" style="width:100px;" class="column-headers-background">N</th></tr></thead><tbody><tr style="height: 20px"><th id="0R0" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">1</div></th><td class="s0" dir="ltr">TEST</td><td class="s0" dir="ltr">OBJECT_SIZE</td><td class="s0" dir="ltr">OBJECT_COUNT</td><td class="s0" dir="ltr">THREADS</td><td class="s0" dir="ltr">TEST DURATION</td><td class="s0" dir="ltr">READ WORKLOAD</td><td class="s0" dir="ltr"></td><td class="s0" dir="ltr" colspan="3">ASYNC LOGGER (calls/second)</td><td class="s0" dir="ltr"></td><td class="s0" dir="ltr" colspan="3">SYNC LOGGER (calls/second)</td></tr><tr style="height: 20px"><th id="0R1" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">2</div></th><td class="s0" dir="ltr"></td><td class="s0" dir="ltr"></td><td class="s0" dir="ltr"></td><td class="s0" dir="ltr"></td><td class="s0"></td><td class="s0" dir="ltr"></td><td class="s0" dir="ltr"></td><td class="s0" dir="ltr">RUN 1</td><td class="s0" dir="ltr">RUN 2</td><td class="s0" dir="ltr">RUN 3</td><td class="s0" dir="ltr"></td><td class="s0" dir="ltr">RUN 1 </td><td class="s0" dir="ltr">RUN 2</td><td class="s0" dir="ltr">RUN 3</td></tr><tr><th style="height:3px;" class="freezebar-cell freezebar-horizontal-handle"></th><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td><td class="freezebar-cell"></td></tr><tr style="height: 20px"><th id="0R2" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">3</div></th><td class="s1" dir="ltr">ozone freon ockrw \<br>        --linear \<br>        --contiguous \<br>        --prefix rmp \<br>        --percentage-list=0 \<br>        --percentage-read=0 \<br>        --threads 50 \<br>        --start-index 1 \<br>        --range $OBJECT_COUNT \<br>        --size $OBJECT_SIZE \<br>        --number-of-tests $OBJECT_COUNT</td><td class="s2" dir="ltr">1044</td><td class="s2" dir="ltr">500,000</td><td class="s2" dir="ltr">50</td><td></td><td class="s2" dir="ltr">0</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">1254</td><td class="s2" dir="ltr">1337</td><td class="s2" dir="ltr">1374</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">1188</td><td class="s2" dir="ltr">1312</td><td class="s2" dir="ltr">1370</td></tr><tr style="height: 20px"><th id="0R3" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">4</div></th><td class="s1" dir="ltr">ozone freon ockrw \<br>        --linear \<br>        --contiguous \<br>        --prefix rmp \<br>        --percentage-list=0 \<br>        --percentage-read=50 \<br>        --threads 50 \<br>        --start-index 1 \<br>        --range $OBJECT_COUNT \<br>        --size $OBJECT_SIZE \<br>        --number-of-tests $OBJECT_COUNT</td><td class="s2" dir="ltr">1044</td><td class="s2" dir="ltr">500,000</td><td class="s2" dir="ltr">50</td><td></td><td class="s2" dir="ltr">50</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">1910</td><td class="s2" dir="ltr">1986</td><td class="s2" dir="ltr">2148</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">2639</td><td class="s2" dir="ltr">2059</td><td class="s2" dir="ltr">2076</td></tr><tr style="height: 20px"><th id="0R4" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">5</div></th><td class="s1" dir="ltr">ozone freon ockrw \<br>        --linear \<br>        --contiguous \<br>        --prefix rmp \<br>        --percentage-list=0 \<br>        --percentage-read=0 \<br>        --threads 1 \<br>        --start-index 1 \<br>        --range $OBJECT_COUNT \<br>        --size $OBJECT_SIZE \<br>        --number-of-tests $OBJECT_COUNT</td><td class="s2" dir="ltr">1044</td><td class="s2" dir="ltr">50,000</td><td class="s2" dir="ltr">1</td><td></td><td class="s2" dir="ltr">0</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">102</td><td class="s2" dir="ltr">106</td><td></td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">97</td><td class="s2" dir="ltr">98</td><td></td></tr><tr style="height: 20px"><th id="0R5" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">6</div></th><td class="s1" dir="ltr">ozone freon ockrw \<br>        --linear \<br>        --contiguous \<br>        --prefix rmp \<br>        --percentage-list=0 \<br>        --percentage-read=50 \<br>        --threads 1 \<br>        --start-index 1 \<br>        --range $OBJECT_COUNT \<br>        --size $OBJECT_SIZE \<br>        --number-of-tests $OBJECT_COUNT</td><td class="s2" dir="ltr">1044</td><td class="s2" dir="ltr">50,000</td><td class="s2" dir="ltr">1</td><td></td><td class="s2" dir="ltr">50</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">164</td><td class="s2" dir="ltr">162</td><td></td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">153</td><td class="s2" dir="ltr">155</td><td></td></tr><tr style="height: 20px"><th id="0R6" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">7</div></th><td class="s1" dir="ltr">ozone freon ommg --operation CREATE_KEY -t 50 --duration 10m</td><td class="s2" dir="ltr">0</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">50</td><td class="s1" dir="ltr">10m</td><td class="s2" dir="ltr">0</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">2336</td><td class="s2" dir="ltr">2306</td><td></td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">2335</td><td class="s2" dir="ltr">2331</td><td></td></tr><tr style="height: 20px"><th id="0R7" style="height: 20px;" class="row-headers-background"><div class="row-header-wrapper" style="line-height: 20px">8</div></th><td class="s1" dir="ltr">ozone freon ommg --operation CREATE_KEY -t 1 --duration 10m</td><td class="s2" dir="ltr">0</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">1</td><td class="s1" dir="ltr">10m</td><td class="s2" dir="ltr">0</td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">203</td><td class="s2" dir="ltr">201</td><td></td><td class="s1" dir="ltr"></td><td class="s2" dir="ltr">199</td><td class="s2" dir="ltr">196</td><td></td></tr></tbody></table>
