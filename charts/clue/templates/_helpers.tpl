{{- define "clue.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "clue.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if eq .Release.Name (include "clue.name" .) -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "clue.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "clue.labels" -}}
app.kubernetes.io/name: {{ include "clue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "clue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "clue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "clue.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{- define "clue.consoleImage" -}}
{{- printf "%s:%s" .Values.console.image.repository (.Values.console.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{- define "clue.accessMode" -}}
{{- $requested := .Values.access.mode -}}
{{- if ne $requested "auto" -}}
{{- $requested -}}
{{- else if eq .Values.service.type "LoadBalancer" -}}
loadbalancer
{{- else if eq .Values.service.type "NodePort" -}}
nodeport
{{- else -}}
{{- $ingressClasses := (lookup "networking.k8s.io/v1" "IngressClass" "" "") | default dict -}}
{{- $ingressItems := (get $ingressClasses "items") | default list -}}
{{- if and .Values.access.host (gt (len $ingressItems) 0) -}}
ingress
{{- else -}}
{{- $nodes := (lookup "v1" "Node" "" "") | default dict -}}
{{- $nodeItems := (get $nodes "items") | default list -}}
{{- $cloud := false -}}
{{- range $node := $nodeItems -}}
{{- $providerID := dig "spec" "providerID" "" $node -}}
{{- if regexMatch "^(aws|gce|azure)://" $providerID -}}
{{- $cloud = true -}}
{{- end -}}
{{- end -}}
{{- if $cloud -}}loadbalancer{{- else -}}portforward{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "clue.serviceType" -}}
{{- $mode := include "clue.accessMode" . | trim -}}
{{- if eq $mode "loadbalancer" -}}LoadBalancer
{{- else if eq $mode "nodeport" -}}NodePort
{{- else -}}ClusterIP
{{- end -}}
{{- end -}}

{{- define "clue.externalUrl" -}}
{{- $configured := .Values.access.externalUrl | trim | trimSuffix "/" -}}
{{- if $configured -}}
{{- $configured -}}
{{- else if and (eq (include "clue.accessMode" . | trim) "ingress") .Values.access.host -}}
{{- if .Values.access.ingress.tls.enabled -}}https{{- else -}}http{{- end -}}://{{ .Values.access.host }}
{{- end -}}
{{- end -}}

{{- define "clue.internalUrl" -}}
http://{{ include "clue.fullname" . }}.{{ .Release.Namespace }}.svc
{{- end -}}

{{- define "clue.managementBaseUrl" -}}
{{- include "clue.externalUrl" . | trim | default (include "clue.internalUrl" . | trim) -}}
{{- end -}}

{{- define "clue.cookieSecure" -}}
{{- if hasPrefix "https://" (include "clue.externalUrl" . | trim) -}}1{{- else -}}0{{- end -}}
{{- end -}}

{{- define "clue.validateAccess" -}}
{{- $mode := include "clue.accessMode" . | trim -}}
{{- $externalUrl := include "clue.externalUrl" . | trim -}}
{{- if and (eq $mode "loadbalancer") (hasPrefix "https://" $externalUrl) (ne .Values.access.loadBalancer.tlsTermination "external") -}}
{{- fail "HTTPS load-balancer access requires access.loadBalancer.tlsTermination=external to acknowledge external TLS termination" -}}
{{- end -}}
{{- end -}}
