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
    angular.module('scm', ['ozone', 'nvd3']);

    angular.module('scm').component('scmOverview', {
        templateUrl: 'scm-overview.html',
        require: {
            overview: "^overview"
        },
        controller: function ($http,$scope) {
            var ctrl = this;
            $scope.reverse = false;
            $scope.columnName = "hostname";
            let nodeStatusCopy = [];
            $scope.RecordsToDisplay = "10";
            $scope.currentPage = 1;
            $scope.lastIndex = 0;

            $http.get("jmx?qry=Hadoop:service=SCMNodeManager,name=SCMNodeManagerInfo")
                .then(function (result) {
                    ctrl.nodemanagermetrics = result.data.beans[0];
                    $scope.nodeStatus = ctrl.nodemanagermetrics && ctrl.nodemanagermetrics.NodeStatusInfo &&
                    ctrl.nodemanagermetrics.NodeStatusInfo.map(({ key, value }) => {
                                return {
                                    hostname: key,
                                    opstate: value[0],
                                    comstate: value[1]
                                }});
                    nodeStatusCopy = [...$scope.nodeStatus];
                    $scope.lastIndex = Math.ceil(nodeStatusCopy.length / $scope.RecordsToDisplay);
                    $scope.nodeStatus = nodeStatusCopy.slice(0, $scope.RecordsToDisplay);
                });
            /*if option is 'All' display all records else display specified record on page*/
            $scope.UpdateRecordsToShow = () => {
                 if($scope.RecordsToDisplay == 'All') {
                     $scope.lastIndex = 1;
                     $scope.nodeStatus = nodeStatusCopy;
                 } else {
                     $scope.lastIndex = Math.ceil(nodeStatusCopy.length / $scope.RecordsToDisplay);
                     $scope.nodeStatus = nodeStatusCopy.slice(0, $scope.RecordsToDisplay);
                 }
                 $scope.currentPage = 1;
            }
            /* Page Slicing  logic */
            $scope.handlePagination = (pageIndex, isDisabled) => {
                if(!isDisabled) {
                    let startIndex = 0, endIndex = 0;
                    $scope.currentPage = pageIndex;
                    startIndex = (pageIndex * $scope.RecordsToDisplay) - $scope.RecordsToDisplay;
                    endIndex = startIndex + parseInt($scope.RecordsToDisplay);
                    $scope.nodeStatus = nodeStatusCopy.slice(startIndex, endIndex);
                }
            }
             /*column sort logic*/
            $scope.columnSort = (colName) => {
                $scope.columnName = colName;
                $scope.reverse = !$scope.reverse;
            }

            const nodeOpStateSortOrder = {
                "IN_SERVICE": "a",
                "DECOMMISSIONING": "b",
                "DECOMMISSIONED": "c",
                "ENTERING_MAINTENANCE": "d",
                "IN_MAINTENANCE": "e"
            };
            ctrl.nodeOpStateOrder = function (v1, v2) {
                //status with non defined sort order will be "undefined"
                return ("" + nodeOpStateSortOrder[v1.value])
                    .localeCompare("" + nodeOpStateSortOrder[v2.value])
            }

            const nodeStateSortOrder = {
                "HEALTHY": "a",
                "STALE": "b",
                "DEAD": "c"
            };
            ctrl.nodeStateOrder = function (v1, v2) {
                //status with non defined sort order will be "undefined"
                return ("" + nodeStateSortOrder[v1.value])
                    .localeCompare("" + nodeStateSortOrder[v2.value])
            }

        }
    });

})();
