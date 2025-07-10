{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "fullname" -}}
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


{{- define "chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "labels" -}}
helm.sh/chart: {{ include "chart" . }}
{{ include "selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "selectorLabels" -}}
app.kubernetes.io/name: {{ include "name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- $name := include "name" . }}
{{- default $name .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "environmentName" -}}}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}

{{- define "volumes" -}}
{{- if hasKey .Values "extraVolumes" }}
{{- $lenExtraVolumes := len .Values.extraVolumes }}
{{- if gt $lenExtraVolumes 0 }}
volumes:
{{ toYaml .Values.extraVolumes }}
{{- end }}
{{- end }}
{{- end }}

{{- define "volumeMounts" -}}
{{- if hasKey .Values "extraVolumeMounts" }}
{{- if hasKey .Values.extraVolumeMounts .ContainerName }}
{{- $extraVolumeMounts := index .Values.extraVolumeMounts .ContainerName }}
{{- $lenExtraVolumeMounts := len $extraVolumeMounts }}
{{- if gt $lenExtraVolumeMounts 0 }}
volumeMounts:
{{ toYaml $extraVolumeMounts }}
{{- end }}
{{- else if hasKey .Values.extraVolumeMounts "default" }}
{{- $extraVolumeMounts := index .Values.extraVolumeMounts "default" }}
{{- $lenExtraVolumeMounts := len $extraVolumeMounts }}
{{- if gt $lenExtraVolumeMounts 0 }}
volumeMounts:
{{ toYaml $extraVolumeMounts }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{- define "envVars" -}}
{{- if hasKey .Values.env .ContainerName }}
{{- $extraEnvs := index .Values.env .ContainerName }}
{{- $lenExtraEnvs := len $extraEnvs }}
{{- if gt $lenExtraEnvs 0 }}
env:
- name: ONEIRO_ENVIRONMENT
  value: {{ include "environmentName" . }}
{{ toYaml $extraEnvs }}
{{- end }}
{{- else if hasKey .Values.env "default" }}
{{- $extraEnvs := index .Values.env "default" }}
{{- $lenExtraEnvs := len $extraEnvs }}
{{- if gt $lenExtraEnvs 0 }}
env:
- name: ONEIRO_ENVIRONMENT
  value: {{ include "environmentName" . }}
{{ toYaml $extraEnvs }}
{{- end }}
{{- end }}
{{- end }}

{{- define "resources" -}}
{{- if hasKey .Values.resources .ContainerName }}
resources:
{{ toYaml (index .Values.resources .ContainerName) }}
{{- else if hasKey .Values.resources "default" }}
resources:
{{ toYaml .Values.resources.default | trim | indent 2 }}
{{- else }}
resources:
  limits:
    cpu: 1000m
    memory: 512Mi
  requests:
    cpu: 100m
    memory: 128Mi
{{- end }}
{{- end }}



