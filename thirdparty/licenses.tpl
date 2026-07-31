{{- range . }}{{ printf "\x1e" }}
-------------------------------------------------------------------------------
Dependency: {{ .Name }}
Version: {{ if .Version }}{{ .Version }}{{ else }}unknown{{ end }}
License: {{ .LicenseName }}
Source: {{ .LicenseURL }}
-------------------------------------------------------------------------------
{{ .LicenseText }}
{{ end -}}
