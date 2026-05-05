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
                 volume.OzoneCapacity = transform(volume.OzoneCapacity);
                 volume.OzoneUsed = transform(volume.OzoneUsed);
                 volume.OzoneAvailable = transform(volume.OzoneAvailable);
                 volume.Reserved = transform(volume.Reserved);
                 volume.TotalCapacity = transform(volume.TotalCapacity);
                 volume.FilesystemCapacity = transform(volume.FilesystemCapacity);
                 volume.FilesystemAvailable = transform(volume.FilesystemAvailable);
                 volume.FilesystemUsed = transform(volume.FilesystemUsed);
                })
                });

            $http.get("jmx?qry=Hadoop:service=HddsDatanode,name=SCMConnectionManager")
                .then(function (result) {
                    ctrl.heartbeatmetrics = result.data.beans;
                    ctrl.heartbeatmetrics.forEach(scm => {
                        var scmServers = scm.SCMServers;
                        scmServers.forEach(scmServer => {
                            scmServer.lastSuccessfulHeartbeat = convertTimestampToDate(scmServer.lastSuccessfulHeartbeat)
                        })
                    })
                });
        }
    });

    // Register ioStatus Controller
    angular.module('ozone').config(function ($routeProvider) {
        $routeProvider.when('/iostatus', {
            templateUrl: 'iostatus.html',
            controller: 'IOStatusController as ioStatusCtrl',
        });
    });

    angular.module('ozone')
        .controller('IOStatusController', function ($http) {
            var ctrl = this;
            $http.get("jmx?qry=Hadoop:service=HddsDatanode,name=VolumeIOStats*")
                .then(function (result) {
                    ctrl.dniostatus = result.data.beans;
                });
        });

    // Register Scanner Controller
    angular.module('ozone').config(function ($routeProvider) {
        $routeProvider.when('/dn-scanner', {
            templateUrl: 'dn-scanner.html',
            controller: 'DNScannerController as scannerStatusCtrl',
        });
    });

    angular.module('ozone')
        .controller('DNScannerController', function ($http) {
            var ctrl = this;
            $http.get("jmx?qry=Hadoop:service=HddsDatanode,name=ContainerDataScannerMetrics*")
                .then(function (result) {
                    ctrl.dnscanner = result.data.beans;
                });
        });

    angular.module('ozone')
        .filter('millisecondsToMinutes', function() {
            return function(milliseconds) {
                if (isNaN(milliseconds)) {
                    return 'Invalid input';
                }
                var minutes = Math.floor(milliseconds / 60000); // 1 minute = 60000 milliseconds
                var seconds = Math.floor((milliseconds % 60000) / 1000);
                return minutes + ' mins ' + seconds + ' secs';
            };
        });

    angular.module('ozone')
        .filter('twoDecimalPlaces', function() {
            return function(input) {
                if (isNaN(input)) {
                    return 'Invalid input';
                }
                return parseFloat(input).toFixed(2);
            };
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

    function convertTimestampToDate(timestamp) {
        if (!timestamp) return '';
        var milliseconds = timestamp * 1000;

        var date = new Date(milliseconds);

        var year = date.getFullYear();
        var month = date.getMonth() + 1;
        var day = date.getDate();
        var hours = date.getHours();
        var minutes = date.getMinutes();
        var seconds = date.getSeconds();

        return `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')} ${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
    }
})();
