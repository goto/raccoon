package client

import (
	"context"
	"fmt"
	"github.com/gojek/courier-go"
	"github.com/goto/raccoon/config"
)

// credentialFetcher implements courier.CredentialFetcher interface
// and provides MQTT authentication credentials.
type credentialFetcher struct {
	username string
	password string
}

// newCredentialFetcher initializes a new credentialFetcher
// using configuration values from ServerMQTT.AuthConfig.
func newCredentialFetcher() (*credentialFetcher, error) {
	authCfg := config.ServerMQTT.AuthConfig

	if authCfg.Username == "" {
		return nil, fmt.Errorf("missing MQTT username in config")
	}
	if authCfg.Password == "" {
		return nil, fmt.Errorf("missing MQTT password in config")
	}

	return &credentialFetcher{
		username: authCfg.Username,
		password: authCfg.Password,
	}, nil
}

// Credentials returns the MQTT username and password as a courier.Credential.
func (cf *credentialFetcher) Credentials(_ context.Context) (*courier.Credential, error) {
	return &courier.Credential{
		Username: cf.username,
		Password: cf.password,
	}, nil
}
