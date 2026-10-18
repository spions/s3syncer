{{- define "s3syncer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "s3syncer.fullname" -}}
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

{{- define "s3syncer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "s3syncer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "s3syncer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "s3syncer.labels" -}}
helm.sh/chart: {{ include "s3syncer.chart" . }}
{{ include "s3syncer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "s3syncer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "s3syncer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "s3syncer.configSecretName" -}}
{{- if .Values.existingConfigSecret.name }}{{ .Values.existingConfigSecret.name }}{{ else }}{{ include "s3syncer.fullname" . }}{{ end }}
{{- end }}

{{- define "s3syncer.configSecretKey" -}}
{{- if .Values.existingConfigSecret.name }}{{ .Values.existingConfigSecret.key }}{{ else }}config.yaml{{ end }}
{{- end }}

{{- define "s3syncer.apiSecretName" -}}
{{- if .Values.api.existingSecret }}{{ .Values.api.existingSecret }}{{ else }}{{ include "s3syncer.fullname" . }}-api{{ end }}
{{- end }}
