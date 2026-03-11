package policy_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writePolicyFile(t *testing.T, policies []policy.PolicyConfig) string {
	t.Helper()
	b, err := json.Marshal(policies)
	require.NoError(t, err)
	f, err := os.CreateTemp(t.TempDir(), "policy-*.json")
	require.NoError(t, err)
	_, err = f.Write(b)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestLoad_ValidFile(t *testing.T) {
	policies := []policy.PolicyConfig{
		{
			Resource: policy.ResourceEvent,
			Details:  policy.Details{Name: "test-event", Product: "test-product", Publisher: "pub-a"},
			Action: policy.ActionConfig{
				Type: policy.ActionDrop,
				EventTimestampThreshold: policy.EventTimestampThreshold{
					Past:   policy.Duration{},
					Future: policy.Duration{},
				},
			},
		},
	}
	path := writePolicyFile(t, policies)
	got, err := policy.Load(path)
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, policy.ResourceEvent, got[0].Resource)
	assert.Equal(t, "test-event", got[0].Details.Name)
}

func TestLoad_EmptyPath(t *testing.T) {
	got, err := policy.Load("")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := policy.Load(filepath.Join(t.TempDir(), "nonexistent.json"))
	assert.Error(t, err)
}

func TestLoad_MalformedJSON(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "bad-*.json")
	_, _ = f.WriteString("{not valid json")
	_ = f.Close()
	_, err := policy.Load(f.Name())
	assert.Error(t, err)
}
