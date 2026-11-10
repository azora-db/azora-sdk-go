// Package demo holds what the examples share: reading the connection
// settings that `go run ./cmd/azora-dev` wrote to examples/.env.
package demo

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Env returns the value of a setting: the environment variable, else the
// line of examples/.env. It exits with a hint when neither has it.
func Env(name string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	if v := fromFile(name); v != "" {
		return v
	}
	fmt.Fprintf(os.Stderr, "%s is not set and examples/.env does not have it.\nStart the dev engine first:  go run ./cmd/azora-dev\n", name)
	os.Exit(1)
	return ""
}

// fromFile looks name up in examples/.env, searching upwards from the
// working directory so examples run from the module root or their own
// folder.
func fromFile(name string) string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		for _, p := range []string{filepath.Join(dir, "examples", ".env"), filepath.Join(dir, ".env")} {
			if v := lookup(p, name); v != "" {
				return v
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func lookup(path, name string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if k, v, ok := strings.Cut(strings.TrimSpace(sc.Text()), "="); ok && k == name {
			return v
		}
	}
	return ""
}

// Check exits with the error when it is not nil.
func Check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
