# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ubuntu:24.04 AS builder

ARG GANESHA_COMMIT=3113f970511289ae8928078cf392d17c0df81b38

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    bison build-essential ca-certificates cmake flex git libacl1-dev libblkid-dev libcap-dev \
    libdbus-1-dev libgrpc++-dev libkrb5-dev libnfsidmap-dev libnsl-dev libprotobuf-dev libpcre2-dev libssl-dev \
    libtirpc-dev libunwind-dev liburcu-dev pkg-config protobuf-compiler protobuf-compiler-grpc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/ozone/hadoop-ozone

COPY nfs-gateway /opt/ozone/hadoop-ozone/nfs-gateway
COPY interface-client/src/main/proto /opt/ozone/hadoop-ozone/interface-client/src/main/proto
COPY common/src/test/resources/nfs /opt/ozone/hadoop-ozone/common/src/test/resources/nfs

RUN git clone https://github.com/nfs-ganesha/nfs-ganesha.git /opt/ganesha && \
    git -C /opt/ganesha checkout --detach "${GANESHA_COMMIT}" && \
    git -C /opt/ganesha submodule update --init --depth 1 && \
    git -C /opt/ganesha apply /opt/ozone/hadoop-ozone/nfs-gateway/src/main/ganesha/patches/0001-expose-rpcsec-gss-client-context.patch && \
    git -C /opt/ganesha apply /opt/ozone/hadoop-ozone/nfs-gateway/src/main/ganesha/patches/0003-capture-rpcsec-gss-context-per-nfs4-compound.patch && \
    git -C /opt/ganesha apply /opt/ozone/hadoop-ozone/nfs-gateway/src/main/ganesha/patches/0002-add-ozone-fsal-build-target.patch && \
    git -C /opt/ganesha apply /opt/ozone/hadoop-ozone/nfs-gateway/src/main/ganesha/patches/0004-include-assert-header-in-ntirpc.patch && \
    cp -R /opt/ozone/hadoop-ozone/nfs-gateway/src/main/ganesha/FSAL_OZONE \
      /opt/ganesha/src/FSAL/FSAL_OZONE

RUN cmake -S /opt/ganesha/src -B /tmp/ganesha-build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DOZONE_NFS_GATEWAY_SOURCE=/opt/ozone/hadoop-ozone/nfs-gateway \
      -DUSE_FSAL_OZONE=ON -DUSE_FSAL_MEM=ON -DUSE_GSS=ON \
      -DUSE_FSAL_PROXY_V4=OFF -DUSE_FSAL_PROXY_V3=OFF -DUSE_FSAL_VFS=OFF \
      -DUSE_FSAL_LUSTRE=OFF -DUSE_FSAL_KVSFS=OFF -DUSE_FSAL_CEPH=OFF -DUSE_FSAL_GPFS=OFF \
      -DUSE_FSAL_XFS=OFF -DUSE_FSAL_GLUSTER=OFF -DUSE_FSAL_NULL=OFF -DUSE_FSAL_RGW=OFF \
      -DUSE_RADOS_RECOV=OFF -DRADOS_URLS=OFF -DUSE_MONITORING=OFF \
      -DUSE_9P=OFF -DUSE_DBUS=OFF -DUSE_GUI_ADMIN_TOOLS=OFF -D_MSPAC_SUPPORT=OFF && \
    cmake --build /tmp/ganesha-build --target ganesha.nfsd fsalozone fsalmem -j2 && \
    cmake --install /tmp/ganesha-build

FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libacl1 libblkid1 libcap2 libgrpc++1.51t64 libkrb5-3 libnfsidmap1 libnsl2 \
    libpcre2-8-0 libprotobuf32t64 libssl3t64 libtirpc3t64 libunwind8 liburcu8t64 rpcbind && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local /usr/local
COPY nfs-gateway/src/main/ganesha/ganesha.conf.example /etc/ganesha/ganesha.conf.example

RUN ldconfig && mkdir -p /etc/ganesha /etc/ozone /var/lib/nfs/ganesha && \
    ganesha.nfsd -v && test -f /usr/local/lib/ganesha/libfsalozone.so && \
    ldd /usr/local/lib/ganesha/libfsalozone.so > /tmp/fsal-dependencies.txt && \
    ! grep -q "not found" /tmp/fsal-dependencies.txt && rm /tmp/fsal-dependencies.txt

EXPOSE 2049/tcp
VOLUME ["/etc/ganesha", "/etc/ozone", "/var/lib/nfs/ganesha"]
CMD ["ganesha.nfsd", "-F", "-L", "STDOUT", "-f", "/etc/ganesha/ganesha.conf"]
