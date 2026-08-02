package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"

	"golang.org/x/term"
)

// passwordReader can be replaced in tests for non-interactive password input.
var passwordReader = promptPassword

func runSetPassword(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("set-password", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "Path to GQM config file (required)")
	username := fs.String("user", "", "Username to set password for (required)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, `Usage: gqm set-password --config <file> --user <username>

Set or update a user's password in the GQM config file.
The password is read from an interactive prompt (not passed as an argument).

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 1
	}

	if *configPath == "" || strings.TrimSpace(*username) == "" {
		fs.Usage()
		return 1
	}

	password, err := passwordReader()
	if err != nil {
		errLine(stderr, "reading password: %v", err)
		return 1
	}

	if strings.TrimSpace(password) == "" {
		errLine(stderr, "password must not be empty")
		return 1
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		errLine(stderr, "hashing password: %v", err)
		return 1
	}

	if err := injectPassword(*configPath, *username, string(hash)); err != nil {
		errLine(stderr, "%v", err)
		return 1
	}

	okLine(stdout, "Password updated for user %q in %s", *username, *configPath)
	restartWarning(stdout)
	return 0
}

func promptPassword() (string, error) {
	q := paint(os.Stderr, ansiCyan+ansiBold, "?")
	fmt.Fprintf(os.Stderr, "%s Enter password: ", q)
	p1, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}

	fmt.Fprintf(os.Stderr, "%s Confirm password: ", q)
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
