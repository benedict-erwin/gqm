package main

import (
	"fmt"
	"os"

	"golang.org/x/crypto/bcrypt"
)

func runHashPassword(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, `Usage: gqm hash-password <password>

Generate a bcrypt hash for the given password.
Use the output in your GQM config file for auth.users[].password_hash.`)
		os.Exit(1)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(args[0]), bcrypt.DefaultCost)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gqm: hash password: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(hash))
}
