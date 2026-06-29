{{/*
Common labels applied to all Nèrvi-owned resources (the NATS subchart manages
its own labels).
*/}}
{{- define "nervi.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: occitan
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Selector labels for a given component.
Usage: include "nervi.selectorLabels" (dict "component" "nervi-mcp" "context" .)
*/}}
{{- define "nervi.selectorLabels" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .context.Release.Name }}
{{- end }}

{{/*
NATS client URL the MCP server connects to. Honors an explicit override,
otherwise derives it from the release namespace.
*/}}
{{- define "nervi.natsUrl" -}}
{{- if .Values.mcp.natsUrl }}
{{- .Values.mcp.natsUrl }}
{{- else }}
{{- printf "nats://nervi-nats.%s.svc.cluster.local:4222" .Release.Namespace }}
{{- end }}
{{- end }}
