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
Resolve the names string of a plugin profile by reading its values-overrides file.
Returns "" when no such profile file exists (used to detect unknown profiles).
Usage: include "hiero-block-node.pluginProfileNames" (dict "root" $ "profile" "rfh")
*/}}
{{- define "hiero-block-node.pluginProfileNames" -}}
{{- $raw := .root.Files.Get (printf "values-overrides/plugin-profile-%s.yaml" .profile) -}}
{{- if $raw -}}
{{- dig "plugins" "names" "" (fromYaml $raw) -}}
{{- end -}}
{{- end -}}

{{/*
Resolve the effective plugin names string from plugins.names and plugins.profile.

This is the single place that turns the plugin selector into a concrete comma-separated
list; every consumer (resolve-plugins init container, BN_PLUGINS env, plugins PVC) must
route through it so the effective set is consistent everywhere.

The values-overrides/plugin-profile-<name>.yaml files are the single source of truth for each
plugin set: a non-empty plugins.profile is expanded by reading the matching file at render time,
so adding a new profile file makes it selectable with no change here or in values.yaml.

Precedence (first match wins):
  1. an explicit non-empty plugins.names is used as-is. An explicit empty ("") plugins.names
     also wins and keeps its meaning (plugins baked into the image); a profile never overrides
     an intentional empty. "Explicit" is distinguished from the chart default by comparing
     against plugin-profile-lfh.yaml, which the chart default (plugins.names) mirrors.
  2. otherwise a non-empty plugins.profile expands to the names from plugin-profile-<profile>.yaml;
  3. otherwise the plugins.names chart default applies.

An unrecognized plugins.profile stops templating via fail, listing the available profiles
discovered from the values-overrides/plugin-profile-*.yaml files -- it never resolves to an
empty or partial set.
*/}}
{{- define "hiero-block-node.pluginNames" -}}
{{- $names := default "" .Values.plugins.names -}}
{{- $profile := default "" .Values.plugins.profile -}}
{{- if $profile -}}
{{- $profilePluginNames := include "hiero-block-node.pluginProfileNames" (dict "root" . "profile" $profile) -}}
{{- if not $profilePluginNames -}}
{{- $available := list -}}
{{- range $path, $_ := .Files.Glob "values-overrides/plugin-profile-*.yaml" -}}
{{- $available = append $available (base $path | trimPrefix "plugin-profile-" | trimSuffix ".yaml") -}}
{{- end -}}
{{- fail (printf "plugins.profile %q is not recognized; valid profiles are: %s (or \"\" to use plugins.names)" $profile ($available | sortAlpha | join ", ")) -}}
{{- end -}}
{{- $chartDefault := include "hiero-block-node.pluginProfileNames" (dict "root" . "profile" "lfh") -}}
{{- if eq $names $chartDefault -}}
{{- /* plugins.names is at the chart default -> the profile selection wins (rule 2) */ -}}
{{- $profilePluginNames -}}
{{- else -}}
{{- /* plugins.names was set explicitly (incl. an explicit "") -> it wins (rule 1) */ -}}
{{- $names -}}
{{- end -}}
{{- else -}}
{{- $names -}}
{{- end -}}
{{- end -}}

{{/*
Validated plugins.source: "maven" (default) or "image".
*/}}
{{- define "hiero-block-node.pluginsSource" -}}
{{- $source := default "maven" .Values.plugins.source -}}
{{- if not (has $source (list "maven" "image")) -}}
{{- fail (printf "plugins.source must be \"maven\" or \"image\", got %q" $source) -}}
{{- end -}}
{{- if eq $source "image" -}}
{{- range splitList "," (include "hiero-block-node.pluginNames" .) -}}
{{- if contains ":" . -}}
{{- fail (printf "plugins.names entry %q pins a version, which plugins.source=image does not support; use plugins.source=maven" (trim .)) -}}
{{- end -}}
{{- end -}}
{{- if .Values.plugins.thirdParty -}}
{{- fail "plugins.thirdParty requires plugins.source=maven; with plugins.source=image mount third-party jars via blockNode.persistence.plugins.existingClaim" -}}
{{- end -}}
{{- end -}}
{{- $source -}}
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
The health port is intentionally excluded from both the ClusterIP and the
LoadBalancer Service: probes go directly to the pod port by number and do not
need a Service entry, so advertising 40983 in any Service serves no functional
purpose and would expose the /healthz endpoint on the public LB when
includePluginPorts is true. Operators who want health in a Service for any
reason can add it explicitly via loadBalancer.extraPorts.
Multiple plugins that share the same port number emit only one Service port entry
(Kubernetes rejects duplicate port numbers within a Service).
Usage: include "hiero-block-node.pluginServicePorts" . | nindent 4
*/}}
{{- define "hiero-block-node.pluginServicePorts" -}}
{{- $seen := dict -}}
{{- with .Values.blockNode.ports -}}
{{- $entries := list (list "publisher" .publisher) (list "subscriber" .subscriber) (list "block-access" .blockAccess) (list "server-status" .serverStatus) -}}
{{- range $entries -}}
{{- $name := index . 0 -}}
{{- $port := index . 1 -}}
{{- if $port -}}
{{- $key := toString $port -}}
{{- if not (hasKey $seen $key) -}}
{{- $_ := set $seen $key true }}
- name: {{ $name }}
  port: {{ $port }}
  targetPort: {{ $port }}
  protocol: TCP
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Emit container port entries for each non-null plugin port in blockNode.ports.
Multiple plugins that share the same port number emit only one container port entry
(Kubernetes rejects duplicate containerPort numbers within a container).
Accepts a dict context: (dict "Values" .Values "hostPorts" $hostPorts)
Usage: include "hiero-block-node.pluginContainerPorts" (dict "Values" .Values "hostPorts" $hostPorts) | nindent 10
*/}}
{{- define "hiero-block-node.pluginContainerPorts" -}}
{{- $seen := dict -}}
{{- $hp := .hostPorts -}}
{{- with .Values.blockNode.ports -}}
{{- $entries := list (list "publisher" .publisher) (list "subscriber" .subscriber) (list "block-access" .blockAccess) (list "health" .health) (list "server-status" .serverStatus) -}}
{{- range $entries -}}
{{- $name := index . 0 -}}
{{- $port := index . 1 -}}
{{- if $port -}}
{{- $key := toString $port -}}
{{- if not (hasKey $seen $key) -}}
{{- $_ := set $seen $key true }}
- name: {{ $name }}
  containerPort: {{ $port }}
  {{- if hasKey $hp $name }}
  hostPort: {{ index $hp $name }}
  {{- end }}
  protocol: TCP
{{- end -}}
{{- end -}}
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

{{/*
Persistence volume helpers. All take (dict "root" $ "key" <volume>) where volume is one of
archive, live, logging, applicationState, plugins. A volume is chart-managed when pv.path is set.
*/}}
{{- define "hiero-block-node.persistence.volumeName" -}}
{{- $defaults := dict "archive" "archive-storage" "live" "live-storage" "logging" "logging-storage" "applicationState" "application-state-storage" "plugins" "plugins-storage" -}}
{{- default (index $defaults .key) (index .root.Values.blockNode.persistence .key).volumeName -}}
{{- end -}}

{{- define "hiero-block-node.persistence.managed" -}}
{{- if dig "pv" "path" "" (index .root.Values.blockNode.persistence .key) -}}true{{- end -}}
{{- end -}}

{{- define "hiero-block-node.persistence.pvcName" -}}
{{- $default := printf "%s-%s" (include "hiero-block-node.fullname" .root) (include "hiero-block-node.persistence.volumeName" .) -}}
{{- dig "pv" "claimName" $default (index .root.Values.blockNode.persistence .key) | default $default -}}
{{- end -}}

{{/*
Chart-managed or existing PVC name; empty when the StatefulSet should use a volumeClaimTemplate.
*/}}
{{- define "hiero-block-node.persistence.claimName" -}}
{{- if include "hiero-block-node.persistence.managed" . -}}
{{- include "hiero-block-node.persistence.pvcName" . -}}
{{- else -}}
{{- default "" (index .root.Values.blockNode.persistence .key).existingClaim -}}
{{- end -}}
{{- end -}}

{{- define "hiero-block-node.persistence.validate" -}}
{{- if include "hiero-block-node.persistence.managed" . -}}
{{- $cfg := index .root.Values.blockNode.persistence .key -}}
{{- $path := printf "blockNode.persistence.%s" .key -}}
{{- $type := default "hostPath" $cfg.pv.type -}}
{{- if $cfg.existingClaim -}}
{{- fail (printf "%s.pv.path and %s.existingClaim are mutually exclusive" $path $path) -}}
{{- end -}}
{{- if not (has $type (list "hostPath" "local")) -}}
{{- fail (printf "%s.pv.type must be one of hostPath, local, got %q" $path $type) -}}
{{- end -}}
{{- if and (eq $type "local") (not $cfg.pv.nodeAffinity) -}}
{{- fail (printf "%s.pv.nodeAffinity is required when pv.type is local" $path) -}}
{{- end -}}
{{- $modes := default (list "ReadWriteOnce") $cfg.pv.accessModes -}}
{{- $replicas := int (default 1 .root.Values.replicaCount) -}}
{{- if and (gt $replicas 1) (or (has "ReadWriteOnce" $modes) (has "ReadWriteOncePod" $modes)) -}}
{{- fail (printf "%s.pv.accessModes %v cannot be shared by %d replicas" $path $modes $replicas) -}}
{{- end -}}
{{- end -}}
{{- end -}}
