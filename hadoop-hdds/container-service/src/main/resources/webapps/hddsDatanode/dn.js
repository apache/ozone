/**
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
(function () {
    "use strict";
    angular.module('dn', ['ozone', 'nvd3']);

    angular.module('dn').component('dnOverview', {
        templateUrl: 'dn-overview.html',
        require: {
            overview: "^overview"
        },
        controller: function ($http) {
            var ctrl = this;
            $http.get("jmx?qry=Hadoop:service=HddsDatanode,name=VolumeInfoMetrics*")
                .then(function (result) {
                    ctrl.dnmetrics = result.data.beans;
                    ctrl.dnmetrics.forEach(volume => {
                 volume.Used = transform(volume.Used);
                 volume.Available = transform(volume.Available);
                 volume.Reserved = transform(volume.Reserved);
                 volume.TotalCapacity = transform(volume.TotalCapacity);
                })
                });
        }
    });
        function transform(v) {
          var UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'ZB'];
          var prev = 0, i = 0;
          while (Math.floor(v) > 0 && i < UNITS.length) {
            prev = v;
            v /= 1024;
            i += 1;
          }
          if (i > 0 && i < UNITS.length) {
            v = prev;
            i -= 1;
          }
          return Math.round(v * 100) / 100 + ' ' + UNITS[i];
        }
})();
