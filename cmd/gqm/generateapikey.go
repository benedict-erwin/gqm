package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
)

func runGenerateAPIKey(args []string) {
	if len(args) > 0 {
		fmt.Fprintln(os.Stderr, `Usage: gqm generate-api-key

Generate a random API key with the gqm_ak_ prefix.
Use the output in your GQM config file for api.api_keys[].key.`)
		os.Exit(1)
	}

	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		fmt.Fprintf(os.Stderr, "gqm: generate api key: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("gqm_ak_%s\n", hex.EncodeToString(b))
}
