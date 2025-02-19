{{/*
Expand the name of the chart.
*/}}
{{- define "hedera-block-node.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hedera-block-node.fullname" -}}
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
{{- define "hedera-block-node.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hedera-block-node.labels" -}}
helm.sh/chart: {{ include "hedera-block-node.chart" . }}
{{ include "hedera-block-node.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hedera-block-node.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hedera-block-node.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "hedera-block-node.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hedera-block-node.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
This function returns the image tag from the values.yaml file if provided.
If the tag is not provided, it defaults to the AppVersion specified in the Chart.yaml file.
Usage: {{ include "image.AppVersion" . }}
*/}}
{{- define "hedera-block-node.image.tag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{/*
This function returns the image tag from the values.yaml file if provided.
If the tag is not provided, it defaults to the AppVersion specified in the Chart.yaml file.
Usage: {{ include "hedera-block-node.app.version" . }}
*/}}
{{- define "hedera-block-node.appVersion" -}}
{{- default .Chart.AppVersion .Values.blockNode.version -}}
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
