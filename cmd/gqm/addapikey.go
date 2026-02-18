package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func runAddAPIKey(args []string) {
	fs := flag.NewFlagSet("add-api-key", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to GQM config file (required)")
	name := fs.String("name", "", "Name for the API key (required)")
	role := fs.String("role", "admin", "Role: admin or viewer")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: gqm add-api-key --config <file> --name <name> [--role admin|viewer]

Generate a random API key and add it to the GQM config file.
The generated key is printed to stdout.

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *configPath == "" || *name == "" {
		fs.Usage()
		os.Exit(1)
	}

	if *role != "admin" && *role != "viewer" {
		fmt.Fprintf(os.Stderr, "gqm: invalid role %q (must be admin or viewer)\n", *role)
		os.Exit(1)
	}

	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		fmt.Fprintf(os.Stderr, "gqm: generating key: %v\n", err)
		os.Exit(1)
	}
	key := "gqm_ak_" + hex.EncodeToString(b)

	if err := injectAPIKey(*configPath, *name, key, *role); err != nil {
		fmt.Fprintf(os.Stderr, "gqm: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("API key added for %q in %s\n", *name, *configPath)
	fmt.Printf("Key: %s\n", key)
	fmt.Print(restartNotice)
}

func injectAPIKey(configPath, name, key, role string) error {
	doc, err := loadConfigNode(configPath)
	if err != nil {
		return err
	}

	root := doc.Content[0]

	monitoring := mapGetOrCreate(root, "monitoring", yaml.MappingNode)
	api := mapGetOrCreate(monitoring, "api", yaml.MappingNode)
	apiKeys := mapGetOrCreate(api, "api_keys", yaml.SequenceNode)

	// Check for duplicate name.
	if _, idx := seqFindMapping(apiKeys, "name", name); idx >= 0 {
		return fmt.Errorf("API key with name %q already exists", name)
	}

	apiKeys.Content = append(apiKeys.Content, newMappingFromPairs(
		"name", name,
		"key", key,
		"role", role,
	))

	return saveConfigNode(configPath, doc)
}
