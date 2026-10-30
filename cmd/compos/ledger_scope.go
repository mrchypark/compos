package main

import (
	"fmt"
	"net/url"
	"strings"
)

func canonicalLedgerServer(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("invalid server URL: %w", err)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("server URL must use http or https")
	}
	if u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("server URL must be an origin with an optional path prefix")
	}
	u.Host = strings.ToLower(u.Host)
	escapedPath := strings.TrimRight(u.EscapedPath(), "/")
	path, err := url.PathUnescape(escapedPath)
	if err != nil {
		return "", fmt.Errorf("invalid server path: %w", err)
	}
	u.Path = path
	u.RawPath = escapedPath
	return u.String(), nil
}
