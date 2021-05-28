package pkg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pursuit/event-go/pkg"
	"github.com/pursuit/event-go/pkg/mock"

	"github.com/golang/mock/gomock"
)

func TestStoreEvent(t *testing.T) {
	data := pkg.EventData{
		Topic:   "this is a topic name",
		Payload: []byte("this is the payload"),
	}

	for _, testcase := range []struct {
		tName     string
		connErr   error
		outputErr error
	}{
		{
			tName:     "failed to insert to db",
			connErr:   errors.New("failed insert"),
			outputErr: errors.New("event: failed insert"),
		},
		{
			tName: "success",
		},
	} {
		mocker := gomock.NewController(t)
		defer mocker.Finish()
		db := mock_pkg.NewMockDB(mocker)

		db.EXPECT().ExecContext(gomock.Any(), "INSERT INTO events (topic,payload) VALUES($1,$2)", data.Topic, data.Payload).Return(nil, testcase.connErr)

		err := pkg.StoreEvent(context.Background(), db, data)
		if (testcase.outputErr == nil && err != nil) ||
			(testcase.outputErr != nil && err == nil) ||
			(err != nil && testcase.outputErr.Error() != err.Error()) {
			t.Errorf("Test %s, err is %v, should be %v", testcase.tName, err, testcase.outputErr)
		}
	}
}
