package main

import (
	"fmt"
	"io"

	"golang.org/x/crypto/bcrypt"
)

// hashPassword generates a bcrypt hash for the given password.
func hashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

func runHashPassword(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, `Usage: gqm hash-password <password>

Generate a bcrypt hash for the given password.
Use the output in your GQM config file for auth.users[].password_hash.`)
		return 1
	}

	hash, err := hashPassword(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "gqm: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout, hash)
	return 0
}
