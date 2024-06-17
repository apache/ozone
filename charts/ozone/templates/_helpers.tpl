{{/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/}}

{{/* Common labels */}}
{{- define "ozone.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Selector labels */}}
{{- define "ozone.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* List of comma separated SCM pod names */}}
{{- define "ozone.scm.pods" -}}
  {{- $pods := list }}
  {{- $replicas := .Values.scm.replicas | int }}
  {{- range $i := until $replicas }}
    {{- $pods = append $pods (printf "%s-scm-%d.%s-scm-headless" $.Release.Name $i $.Release.Name) }}
  {{- end }}
  {{- $pods | join "," }}
{{- end }}

{{/* List of comma separated OM pod names */}}
{{- define "ozone.om.pods" -}}
  {{- $pods := list }}
  {{- $replicas := .Values.om.replicas | int }}
  {{- range $i := until $replicas }}
    {{- $pods = append $pods (printf "%s-om-%d.%s-om-headless" $.Release.Name $i $.Release.Name) }}
  {{- end }}
  {{- $pods | join "," }}
{{- end }}

{{/* Common configuration environment variables */}}
{{- define "ozone.configuration.env" -}}
- name: OZONE-SITE.XML_hdds.datanode.dir
  value: /data/storage
- name: OZONE-SITE.XML_ozone.scm.datanode.id.dir
  value: /data
- name: OZONE-SITE.XML_ozone.metadata.dirs
  value: /data/metadata
- name: OZONE-SITE.XML_ozone.scm.block.client.address
  value: {{ include "ozone.scm.pods" . }}
- name: OZONE-SITE.XML_ozone.scm.client.address
  value: {{ include "ozone.scm.pods" . }}
- name: OZONE-SITE.XML_ozone.scm.names
  value: {{ include "ozone.scm.pods" . }}
- name: OZONE-SITE.XML_ozone.om.address
  value: {{ include "ozone.om.pods" . }}
- name: OZONE-SITE.XML_hdds.scm.safemode.min.datanode
  value: "3"
- name: OZONE-SITE.XML_ozone.datanode.pipeline.limit
  value: "1"
- name: OZONE-SITE.XML_dfs.datanode.use.datanode.hostname
  value: "true"
{{- end }}
