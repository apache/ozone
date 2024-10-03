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
        controller: function ($http,$scope,$sce) {
            var ctrl = this;
            $scope.reverse = false;
            $scope.columnName = "hostname";
            let nodeStatusCopy = [];
            $scope.RecordsToDisplay = "10";
            $scope.currentPage = 1;
            $scope.lastIndex = 0;
            $scope.statistics = {
                nodes : {
                    usages : {
                        min : "N/A",
                        max : "N/A",
                        median : "N/A",
                        stdev : "N/A"
                    },
                    state : {
                        healthy : "N/A",
                        dead : "N/A",
                        decommissioning : "N/A",
                        enteringmaintenance : "N/A",
                        volumefailures : "N/A"
                    },
                    space : {
                        capacity : "N/A",
                        scmused : "N/A",
                        remaining : "N/A",
                        nonscmused : "N/A"
                    }
                },
                pipelines : {
                    closed : "N/A",
                    allocated : "N/A",
                    open : "N/A",
                    dormant : "N/A"
                },
                containers : {
                    lifecycle : {
                        open : "N/A",
                        closing : "N/A",
                        quasi_closed : "N/A",
                        closed : "N/A",
                        deleting : "N/A",
                        deleted : "N/A",
                        recovering : "N/A"
                    },
                    health : {
                        under_replicated : "N/A",
                        mis_replicated : "N/A",
                        over_replicated : "N/A",
                        missing : "N/A",
                        unhealthy : "N/A",
                        empty : "N/A",
                        open_unhealthy : "N/A",
                        quasi_closed_stuck : "N/A",
                        open_without_pipeline : "N/A"
                    }
                }
            }

            $http.get("jmx?qry=Ratis:service=RaftServer,group=*,id=*")
                .then(function (result) {
                    ctrl.role = result.data.beans[0];
            });

            function get_protocol(URLScheme, value, baseProto, fallbackProto) {
                let protocol = "unknown"
                let port = -1;
                if (URLScheme.toLowerCase() === baseProto) {
                    let portSpec = value && value.find((element) => element.key.toLowerCase() === baseProto)
                    if (portSpec) {
                        port = portSpec.value
                        protocol = baseProto
                        return { proto : protocol, port: port }
                    }
                    portSpec = value && value.find((element) => element.key.toLowerCase() === fallbackProto);
                    if (portSpec) {
                        port = portSpec.value
                        protocol = fallbackProto
                    }
                }
                return { proto : protocol, port: port }
            }

            $http.get("jmx?qry=Hadoop:service=SCMNodeManager,name=SCMNodeManagerInfo")
                .then(function (result) {
                    const URLScheme = location.protocol.replace(":" , "");
                    ctrl.nodemanagermetrics = result.data.beans[0];

                    $scope.nodeStatus = ctrl.nodemanagermetrics
                        && ctrl.nodemanagermetrics.NodeStatusInfo
                        && ctrl.nodemanagermetrics.NodeStatusInfo
                            .map(({ key, value }) => {
                                let portSpec = get_protocol(URLScheme, value, "https", "http")
                                if (portSpec.port === -1) {
                                    portSpec = get_protocol(URLScheme, value, "http", "https")
                                }
                                return {
                                    hostname: key,
                                    opstate: value && value.find((element) => element.key === "OPSTATE").value,
                                    usedspacepercent: value && value.find((element) => element.key === "USEDSPACEPERCENT").value,
                                    capacity: value && value.find((element) => element.key === "CAPACITY").value,
                                    comstate: value && value.find((element) => element.key === "COMSTATE").value,
                                    lastheartbeat: value && value.find((element) => element.key === "LASTHEARTBEAT").value,
                                    uuid: value && value.find((element) => element.key === "UUID").value,
                                    version: value && value.find((element) => element.key === "VERSION").value,
                                    port: portSpec.port,
                                    protocol: portSpec.proto
                                }
                            });

                    nodeStatusCopy = [...$scope.nodeStatus];
                    $scope.totalItems = nodeStatusCopy.length;
                    $scope.lastIndex = Math.ceil(nodeStatusCopy.length / $scope.RecordsToDisplay);
                    $scope.nodeStatus = nodeStatusCopy.slice(0, $scope.RecordsToDisplay);

                    $scope.formatValue = function(value) {
                        if (value && value.includes(';')) {
                            return $sce.trustAsHtml(value.replace('/;/g', '<br>'));
                        } else {
                            return $sce.trustAsHtml(value);
                        }
                    };

                    ctrl.nodemanagermetrics.NodeStatistics.forEach(({key, value}) => {
                        if(key == "Min") {
                            $scope.statistics.nodes.usages.min = value;
                        } else if(key == "Max") {
                            $scope.statistics.nodes.usages.max = value;
                        } else if(key == "Median") {
                            $scope.statistics.nodes.usages.median = value;
                        } else if(key == "Stdev") {
                            $scope.statistics.nodes.usages.stdev = value;
                        } else if(key == "Healthy") {
                            $scope.statistics.nodes.state.healthy = value;
                        } else if(key == "Dead") {
                            $scope.statistics.nodes.state.dead = value;
                        } else if(key == "Decommissioning") {
                            $scope.statistics.nodes.state.decommissioning = value;
                        } else if(key == "EnteringMaintenance") {
                            $scope.statistics.nodes.state.enteringmaintenance = value;
                        } else if(key == "VolumeFailures") {
                            $scope.statistics.nodes.state.volumefailures = value;
                        } else if(key == "Capacity") {
                            $scope.statistics.nodes.space.capacity = value;
                        } else if(key == "Scmused") {
                            $scope.statistics.nodes.space.scmused = value;
                        } else if(key == "Remaining") {
                            $scope.statistics.nodes.space.remaining = value;
                        } else if(key == "NonScmused") {
                            $scope.statistics.nodes.space.nonscmused = value;
                        }
                    });
                });

            $http.get("jmx?qry=Hadoop:service=SCMPipelineManager,name=SCMPipelineManagerInfo")
                .then(function (result) {
                    const URLScheme = location.protocol.replace(":" , "");
                    ctrl.scmpipelinemanager = result.data.beans[0];
                    ctrl.scmpipelinemanager.PipelineInfo.forEach(({key, value}) => {
                        if(key == "CLOSED") {
                            $scope.statistics.pipelines.closed = value;
                        } else if(key == "ALLOCATED") {
                            $scope.statistics.pipelines.allocated = value;
                        } else if(key == "OPEN") {
                            $scope.statistics.pipelines.open = value;
                        } else if(key == "DORMANT") {
                            $scope.statistics.pipelines.dormant = value;
                        }
                    });
                });

            $http.get("jmx?qry=Hadoop:service=StorageContainerManager,name=ReplicationManagerMetrics")
                .then(function (result) {
                    const URLScheme = location.protocol.replace(":" , "");
                    ctrl.scmcontainermanager = result.data.beans[0];
                    $scope.statistics.containers.lifecycle.open = ctrl.scmcontainermanager.OpenContainers;
                    $scope.statistics.containers.lifecycle.closing = ctrl.scmcontainermanager.ClosingContainers;
                    $scope.statistics.containers.lifecycle.quasi_closed = ctrl.scmcontainermanager.QuasiClosedContainers;
                    $scope.statistics.containers.lifecycle.closed = ctrl.scmcontainermanager.ClosedContainers;
                    $scope.statistics.containers.lifecycle.deleting = ctrl.scmcontainermanager.DeletingContainers;
                    $scope.statistics.containers.lifecycle.deleted = ctrl.scmcontainermanager.DeletedContainers;
                    $scope.statistics.containers.lifecycle.recovering = ctrl.scmcontainermanager.RecoveringContainers;
                    $scope.statistics.containers.health.under_replicated = ctrl.scmcontainermanager.UnderReplicatedContainers;
                    $scope.statistics.containers.health.mis_replicated = ctrl.scmcontainermanager.MisReplicatedContainers;
                    $scope.statistics.containers.health.over_replicated = ctrl.scmcontainermanager.OverReplicatedContainers;
                    $scope.statistics.containers.health.missing = ctrl.scmcontainermanager.MissingContainers;
                    $scope.statistics.containers.health.unhealthy = ctrl.scmcontainermanager.UnhealthyContainers;
                    $scope.statistics.containers.health.empty = ctrl.scmcontainermanager.EmptyContainers;
                    $scope.statistics.containers.health.open_unhealthy = ctrl.scmcontainermanager.OpenUnhealthyContainers;
                    $scope.statistics.containers.health.quasi_closed_stuck = ctrl.scmcontainermanager.StuckQuasiClosedContainers;
                    $scope.statistics.containers.health.open_without_pipeline = ctrl.scmcontainermanager.OpenContainersWithoutPipeline;
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
                if(!isDisabled && $scope.RecordsToDisplay != 'All') {
                    pageIndex = parseInt(pageIndex);
                    let startIndex = 0, endIndex = 0;
                    $scope.currentPage = pageIndex;
                    startIndex = ($scope.currentPage - 1) * parseInt($scope.RecordsToDisplay);
                    endIndex = startIndex + parseInt($scope.RecordsToDisplay);
                    $scope.nodeStatus = nodeStatusCopy.slice(startIndex, endIndex);
                }
            }
            /*column sort logic*/
            $scope.columnSort = (colName) => {
                $scope.columnName = colName;
                $scope.reverse = !$scope.reverse;
            }
            /*show page*/
            $scope.getPagesArray = function () {
                return Array.from({ length: $scope.lastIndex }, (_, index) => index + 1);
            };
            /*show last item index*/
            $scope.getCurrentPageLastItemIndex = ()  => {
                if ($scope.RecordsToDisplay == 'All') {
                    return $scope.totalItems;
                }

                let endIndex = $scope.currentPage * parseInt($scope.RecordsToDisplay);
                return Math.min(endIndex, $scope.totalItems);
            }
            /*show first item index*/
            $scope.getCurrentPageFirstItemIndex = () => {
                if ($scope.RecordsToDisplay == 'All') {
                    return 1;
                }
                return ($scope.currentPage - 1) * $scope.RecordsToDisplay + 1;
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
