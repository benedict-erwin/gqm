package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
)

// generateAPIKey creates a random API key with the gqm_ak_ prefix.
func generateAPIKey() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate api key: %w", err)
	}
	return "gqm_ak_" + hex.EncodeToString(b), nil
}

func runGenerateAPIKey(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		fmt.Fprintln(stderr, `Usage: gqm generate-api-key

Generate a random API key with the gqm_ak_ prefix.
Use the output in your GQM config file for api.api_keys[].key.`)
		return 1
	}

	key, err := generateAPIKey()
	if err != nil {
		fmt.Fprintf(stderr, "gqm: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout, key)
	return 0
}
