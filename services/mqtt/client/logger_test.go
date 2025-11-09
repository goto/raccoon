package client

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/raccoon/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// testHook captures log entries emitted by logrus.
type testHook struct {
	entries []logrus.Entry
}

func (h *testHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *testHook) Fire(e *logrus.Entry) error {
	h.entries = append(h.entries, *e)
	return nil
}

func TestLogger_InfoAndError(t *testing.T) {
	// Arrange
	baseLogger := logrus.New()
	hook := &testHook{}
	baseLogger.AddHook(hook)

	l := Logger{log: baseLogger}
	ctx := context.Background()

	tests := []struct {
		name        string
		callFunc    func()
		expected    logrus.Level
		expectedMsg string
	}{
		{
			name: "Info logs message with attrs",
			callFunc: func() {
				l.Info(ctx, "info message", map[string]any{"key": "value"})
			},
			expected:    logrus.InfoLevel,
			expectedMsg: "info message",
		},
		{
			name: "Error logs error with attrs",
			callFunc: func() {
				l.Error(ctx, errors.New("some error"), map[string]any{"code": 500})
			},
			expected:    logrus.ErrorLevel,
			expectedMsg: "some error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hook.entries = nil // reset before each run
			tt.callFunc()

			assert.NotEmpty(t, hook.entries)
			lastEntry := hook.entries[len(hook.entries)-1]

			assert.Equal(t, tt.expected, lastEntry.Level)
			assert.Contains(t, lastEntry.Message, tt.expectedMsg)
		})
	}
}

func TestNewLogger(t *testing.T) {
	l := NewLogger()
	assert.NotNil(t, l)
	assert.NotNil(t, l.log)
	assert.Equal(t, logger.GetLogger(), l.log)
}
