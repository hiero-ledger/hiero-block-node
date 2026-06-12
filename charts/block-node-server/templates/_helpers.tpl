{{/*
Expand the name of the chart.
*/}}
{{- define "hiero-block-node.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hiero-block-node.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hiero-block-node.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hiero-block-node.labels" -}}
helm.sh/chart: {{ include "hiero-block-node.chart" . }}
{{ include "hiero-block-node.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hiero-block-node.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hiero-block-node.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "hiero-block-node.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hiero-block-node.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
This function returns the image tag from the values.yaml file if provided.
If the tag is not provided, it defaults to the AppVersion specified in the Chart.yaml file.
Usage: {{ include "image.AppVersion" . }}
*/}}
{{- define "hiero-block-node.image.tag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{/*
This function returns the image tag from the values.yaml file if provided.
If the tag is not provided, it defaults to the AppVersion specified in the Chart.yaml file.
Usage: {{ include "hiero-block-node.app.version" . }}
*/}}
{{- define "hiero-block-node.appVersion" -}}
{{- default .Chart.AppVersion .Values.blockNode.version -}}
{{- end -}}

{{/*
Build JAVA_TOOL_OPTIONS combining logging config and metrics system properties.
*/}}
{{- define "hiero-block-node.javaToolOptions" -}}
-Djava.util.logging.config.file=/opt/hiero/block-node/logs/config/logging.properties
{{- with .Values.blockNode.metrics }}
{{- if .hostname }} -Dmetrics.exporter.openmetrics.http.hostname={{ .hostname }}{{ end }}
{{- if .port }} -Dmetrics.exporter.openmetrics.http.port={{ .port }}{{ end }}
{{- if .path }} -Dmetrics.exporter.openmetrics.http.path={{ .path }}{{ end }}
{{- if .decimalFormat }} -Dmetrics.exporter.openmetrics.http.decimalFormat={{ .decimalFormat }}{{ end }}
{{- end }}
{{- end }}

{{/*
Emit ConfigMap data lines (KEY: "value") for each non-null plugin port in blockNode.ports.
These are emitted before the blockNode.config range so that explicit config values win on conflict.
Usage: include "hiero-block-node.pluginPortEnvVars" .
*/}}
{{- define "hiero-block-node.pluginPortEnvVars" -}}
{{- with .Values.blockNode.ports -}}
{{- if .health }}
  HEALTH_PORT: {{ .health | quote }}
{{- end -}}
{{- if .publisher }}
  PRODUCER_PORT: {{ .publisher | quote }}
{{- end -}}
{{- if .subscriber }}
  SUBSCRIBER_PORT: {{ .subscriber | quote }}
{{- end -}}
{{- if .blockAccess }}
  BLOCK_ACCESS_PORT: {{ .blockAccess | quote }}
{{- end -}}
{{- if .serverStatus }}
  SERVER_STATUS_PORT: {{ .serverStatus | quote }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Emit Service port entries for each non-null plugin port in blockNode.ports.
Usage: include "hiero-block-node.pluginServicePorts" . | nindent 4
*/}}
{{- define "hiero-block-node.pluginServicePorts" -}}
{{- with .Values.blockNode.ports -}}
{{- if .publisher }}
- name: publisher
  port: {{ .publisher }}
  targetPort: publisher
  protocol: TCP
{{- end -}}
{{- if .subscriber }}
- name: subscriber
  port: {{ .subscriber }}
  targetPort: subscriber
  protocol: TCP
{{- end -}}
{{- if .blockAccess }}
- name: block-access
  port: {{ .blockAccess }}
  targetPort: block-access
  protocol: TCP
{{- end -}}
{{- if .health }}
- name: health
  port: {{ .health }}
  targetPort: health
  protocol: TCP
{{- end -}}
{{- if .serverStatus }}
- name: server-status
  port: {{ .serverStatus }}
  targetPort: server-status
  protocol: TCP
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Emit container port entries for each non-null plugin port in blockNode.ports.
Accepts a dict context: (dict "Values" .Values "hostPorts" $hostPorts)
Usage: include "hiero-block-node.pluginContainerPorts" (dict "Values" .Values "hostPorts" $hostPorts) | nindent 10
*/}}
{{- define "hiero-block-node.pluginContainerPorts" -}}
{{- $hp := .hostPorts -}}
{{- with .Values.blockNode.ports -}}
{{- if .publisher }}
- name: publisher
  containerPort: {{ .publisher }}
  {{- if hasKey $hp "publisher" }}
  hostPort: {{ index $hp "publisher" }}
  {{- end }}
  protocol: TCP
{{- end -}}
{{- if .subscriber }}
- name: subscriber
  containerPort: {{ .subscriber }}
  {{- if hasKey $hp "subscriber" }}
  hostPort: {{ index $hp "subscriber" }}
  {{- end }}
  protocol: TCP
{{- end -}}
{{- if .blockAccess }}
- name: block-access
  containerPort: {{ .blockAccess }}
  {{- if hasKey $hp "block-access" }}
  hostPort: {{ index $hp "block-access" }}
  {{- end }}
  protocol: TCP
{{- end -}}
{{- if .health }}
- name: health
  containerPort: {{ .health }}
  {{- if hasKey $hp "health" }}
  hostPort: {{ index $hp "health" }}
  {{- end }}
  protocol: TCP
{{- end -}}
{{- if .serverStatus }}
- name: server-status
  containerPort: {{ .serverStatus }}
  {{- if hasKey $hp "server-status" }}
  hostPort: {{ index $hp "server-status" }}
  {{- end }}
  protocol: TCP
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The service name to connect to Loki. Defaults to the same logic as "loki.fullname"
*/}}
{{- define "loki.serviceName" -}}
{{- if .Values.loki.serviceName -}}
{{- .Values.loki.serviceName -}}
{{- else if .Values.loki.fullnameOverride -}}
{{- .Values.loki.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "loki" .Values.loki.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}
