package main

import (
	"flag"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

func runRevokeAPIKey(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("revoke-api-key", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "Path to GQM config file (required)")
	name := fs.String("name", "", "Name of the API key to revoke (required)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, `Usage: gqm revoke-api-key --config <file> --name <name>

Remove an API key from the GQM config file.

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *configPath == "" || *name == "" {
		fs.Usage()
		return 1
	}

	if err := removeAPIKey(*configPath, *name); err != nil {
		fmt.Fprintf(stderr, "gqm: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "API key %q revoked from %s\n", *name, *configPath)
	fmt.Fprint(stdout, restartNotice)
	return 0
}

func removeAPIKey(configPath, name string) error {
	doc, err := loadConfigNode(configPath)
	if err != nil {
		return err
	}

	root := doc.Content[0]

	monitoring := mapGet(root, "monitoring")
	if monitoring == nil {
		return fmt.Errorf("API key %q not found (no monitoring section)", name)
	}

	api := mapGet(monitoring, "api")
	if api == nil {
		return fmt.Errorf("API key %q not found (no api section)", name)
	}

	apiKeys := mapGet(api, "api_keys")
	if apiKeys == nil || apiKeys.Kind != yaml.SequenceNode {
		return fmt.Errorf("API key %q not found (no api_keys section)", name)
	}

	_, idx := seqFindMapping(apiKeys, "name", name)
	if idx < 0 {
		return fmt.Errorf("API key %q not found", name)
	}

	// Remove entry at idx.
	apiKeys.Content = append(apiKeys.Content[:idx], apiKeys.Content[idx+1:]...)

	return saveConfigNode(configPath, doc)
}
