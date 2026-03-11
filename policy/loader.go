package policy

import (
	"encoding/json"
	"fmt"
	"os"
)

// Load reads the policy config from the file path specified by POLICY_CONFIG_FILE
// (injected via the config package). It returns a slice of PolicyConfig parsed
// from the JSON array in that file.
func Load(filePath string) ([]PolicyConfig, error) {
	if filePath == "" {
		return nil, nil
	}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("policy: cannot open config file %q: %w", filePath, err)
	}
	defer f.Close()

	var policies []PolicyConfig
	if err := json.NewDecoder(f).Decode(&policies); err != nil {
		return nil, fmt.Errorf("policy: cannot decode config file %q: %w", filePath, err)
	}
	return policies, nil
}
