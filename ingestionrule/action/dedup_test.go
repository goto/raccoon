package action_test

import (
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/mocks"
	"github.com/stretchr/testify/assert"
)

func TestDedup_Apply_WithService(t *testing.T) {
	events := []*pb.Event{
		{
			Type:       "click-event-type",
			EventBytes: []byte("event-payload"),
		},
	}
	connGroup := "group-whitelisted"

	t.Run("ApplyDelegatesToService", func(t *testing.T) {
		mockSvc := mocks.NewDedupService(t)
		expectedResult := []*pb.Event{events[0]}

		mockSvc.EXPECT().Apply(events, connGroup).Return(expectedResult)

		d := action.NewDedup(mockSvc)

		res := d.Apply(events, connGroup)
		assert.Equal(t, expectedResult, res)
	})
}
