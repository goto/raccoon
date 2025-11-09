package client

import (
	"context"
	"testing"

	"github.com/gojek/courier-go"
	"github.com/goto/raccoon/config"
	"github.com/stretchr/testify/assert"
)

func TestNewCredentialFetcher(t *testing.T) {
	tests := []struct {
		name        string
		username    string
		password    string
		expectError bool
	}{
		{
			name:        "success - valid username and password",
			username:    "testUser",
			password:    "testPass",
			expectError: false,
		},
		{
			name:        "error - missing username",
			username:    "",
			password:    "somePass",
			expectError: true,
		},
		{
			name:        "error - missing password",
			username:    "someUser",
			password:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// mock config
			config.ServerMQTT.AuthConfig.Username = tt.username
			config.ServerMQTT.AuthConfig.Password = tt.password

			fetcher, err := newCredentialFetcher()

			if tt.expectError {
				assert.Nil(t, fetcher)
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, fetcher)
				assert.Equal(t, tt.username, fetcher.username)
				assert.Equal(t, tt.password, fetcher.password)
			}
		})
	}
}

func TestCredentials(t *testing.T) {
	fetcher := &credentialFetcher{
		username: "myUser",
		password: "myPass",
	}

	cred, err := fetcher.Credentials(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, cred)
	assert.Equal(t, &courier.Credential{
		Username: "myUser",
		Password: "myPass",
	}, cred)
}
