package dashboard

import "embed"

//go:embed VERSION index.html css/* js/*.js js/components/* vendor/*
var Assets embed.FS
