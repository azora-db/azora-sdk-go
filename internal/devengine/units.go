package devengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	azora "github.com/azora-db/azora-go"
)

// Features returns the optional features the running server lists in
// its discovery document (/.well-known/azora.json), such as "units".
func (e *Engine) Features() ([]string, error) {
	c := http.Client{Timeout: 10 * time.Second}
	resp, err := c.Get(e.HTTPURL() + "/.well-known/azora.json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("devengine: discovery answered %s", resp.Status)
	}
	var doc struct {
		Features []string `json:"features"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("devengine: discovery: %w", err)
	}
	return doc.Features, nil
}

// Supports reports whether the running server lists feature in its
// discovery document.
func (e *Engine) Supports(feature string) (bool, error) {
	f, err := e.Features()
	if err != nil {
		return false, err
	}
	return slices.Contains(f, feature), nil
}

// DeployUnits publishes a units bundle to a database of the running
// server, with an API key that has the ddl scope. Unlike the other setup
// it goes through the SDK, so it needs the server up (after Serve).
func (e *Engine) DeployUnits(ctx context.Context, db, key string, req azora.DeployRequest) (*azora.Deploy, error) {
	c, err := azora.Connect(ctx, e.ConnectURL(db), azora.WithToken(key))
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.Units().Deploy(ctx, req)
}
