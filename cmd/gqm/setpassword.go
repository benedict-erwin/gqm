package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"syscall"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"

	"golang.org/x/term"
)

func runSetPassword(args []string) {
	fs := flag.NewFlagSet("set-password", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to GQM config file (required)")
	username := fs.String("user", "", "Username to set password for (required)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: gqm set-password --config <file> --user <username>

Set or update a user's password in the GQM config file.
The password is read from an interactive prompt (not passed as an argument).

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *configPath == "" || *username == "" {
		fs.Usage()
		os.Exit(1)
	}

	password, err := promptPassword()
	if err != nil {
		fmt.Fprintf(os.Stderr, "gqm: reading password: %v\n", err)
		os.Exit(1)
	}

	if strings.TrimSpace(password) == "" {
		fmt.Fprintln(os.Stderr, "gqm: password must not be empty")
		os.Exit(1)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gqm: hashing password: %v\n", err)
		os.Exit(1)
	}

	if err := injectPassword(*configPath, *username, string(hash)); err != nil {
		fmt.Fprintf(os.Stderr, "gqm: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Password updated for user %q in %s\n", *username, *configPath)
	fmt.Print(restartNotice)
}

func promptPassword() (string, error) {
	fmt.Fprint(os.Stderr, "Enter password: ")
	p1, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}

	fmt.Fprint(os.Stderr, "Confirm password: ")
	p2, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}

	if string(p1) != string(p2) {
		return "", fmt.Errorf("passwords do not match")
	}

	return string(p1), nil
}

func injectPassword(configPath, username, hash string) error {
	doc, err := loadConfigNode(configPath)
	if err != nil {
		return err
	}

	root := doc.Content[0] // The top-level mapping.

	// Navigate: monitoring -> auth -> users (create path if missing).
	monitoring := mapGetOrCreate(root, "monitoring", yaml.MappingNode)
	auth := mapGetOrCreate(monitoring, "auth", yaml.MappingNode)

	// Ensure auth.enabled is true.
	if v := mapGet(auth, "enabled"); v == nil || v.Value != "true" {
		mapSet(auth, "enabled", "true")
	}

	users := mapGetOrCreate(auth, "users", yaml.SequenceNode)

	// Find existing user or append new one.
	if entry, _ := seqFindMapping(users, "username", username); entry != nil {
		mapSet(entry, "password_hash", hash)
	} else {
		users.Content = append(users.Content, newMappingFromPairs(
			"username", username,
			"password_hash", hash,
			"role", "admin",
		))
	}

	return saveConfigNode(configPath, doc)
}
