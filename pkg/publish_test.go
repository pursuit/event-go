package pkg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pursuit/event-go/mock/shopify/sarama"
	"github.com/pursuit/event-go/pkg"
	"github.com/pursuit/event-go/pkg/mock"

	"github.com/golang/mock/gomock"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestKafkaConsumer(t *testing.T) {
	id := 2
	topic := "topic1"
	payload := []byte(`{"foo":"bar"}`)

	for _, testcase := range []struct {
		tName     string
		txErr     error
		queryErr  error
		deleteErr error
		kafkaErr  error
		commitErr error
		outputErr error
	}{
		{
			tName:     "fail start transaction",
			txErr:     errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail query",
			queryErr:  errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail delete",
			deleteErr: errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail kafka",
			kafkaErr:  errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail commit",
			commitErr: errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName: "success",
		},
		{
			tName: "batch failed to query",
		},
	} {
		t.Run(testcase.tName, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			kafka := mock_sarama.NewMockSyncProducer(ctrl)

			db, mock, err := sqlmock.New()
			if err != nil {
				panic(err)
			}
			defer db.Close()

			if testcase.txErr != nil {
				mock.ExpectBegin().WillReturnError(testcase.txErr)
			} else {
				mock.ExpectBegin()
				if testcase.queryErr != nil {
					mock.ExpectQuery("SELECT id, topic, payload FROM events LIMIT 1 FOR UPDATE SKIP LOCKED").WillReturnError(testcase.queryErr)
					mock.ExpectRollback()
				} else {
					mock.ExpectQuery("SELECT id, topic, payload FROM events LIMIT 1 FOR UPDATE SKIP LOCKED").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
					if testcase.deleteErr != nil {
						mock.ExpectExec("DELETE FROM kafka_events where id = ?").WillReturnError(testcase.deleteErr)
						mock.ExpectRollback()
					} else {
						mock.ExpectExec("DELETE FROM kafka_events where id = ?").WillReturnResult(sqlmock.NewResult(1, 1))
						if testcase.kafkaErr != nil {
							kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), testcase.kafkaErr)
							mock.ExpectRollback()
						} else {
							kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), nil)
							if testcase.commitErr != nil {
								mock.ExpectCommit().WillReturnError(testcase.commitErr)
							} else {
								mock.ExpectCommit()
							}
						}
					}
				}
			}

			consumer := pkg.KafkaConsumer{
				DB:        db,
				Kafka:     kafka,
				Batch:     1,
				WorkerNum: 1,
			}

			err = consumer.Process()
			if (testcase.outputErr == nil && err != nil) ||
				(testcase.outputErr != nil && err == nil) ||
				(err != nil && testcase.outputErr.Error() != err.Error()) {
				t.Errorf("Test %s, err is %v, should be %v", testcase.tName, err, testcase.outputErr)
			}

			consumer.Shutdown()
			consumer.Run()

			if err := mock.ExpectationsWereMet(); err != nil {
				panic(err)
			}
		})
	}
}

func TestKafkaConsumerBatch(t *testing.T) {
	id := 2
	topic := "topic1"
	payload := []byte(`{"foo":"bar"}`)

	for _, testcase := range []struct {
		tName     string
		txErr     error
		queryErr  error
		deleteErr error
		kafkaErr  error
		commitErr error
		outputErr error
	}{
		{
			tName:     "fail start transaction",
			txErr:     errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail query",
			queryErr:  errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail scan",
			outputErr: errors.New(`sql: Scan error on column index 0, name "id": converting driver.Value type string ("invalid id") to a int: invalid syntax`),
		},
		{
			tName:     "fail delete",
			deleteErr: errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail kafka",
			kafkaErr:  errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName:     "fail commit",
			commitErr: errors.New("fail"),
			outputErr: errors.New("fail"),
		},
		{
			tName: "success",
		},
	} {
		t.Run(testcase.tName, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			kafka := mock_sarama.NewMockSyncProducer(ctrl)

			db, mock, err := sqlmock.New()
			if err != nil {
				panic(err)
			}
			defer db.Close()

			if testcase.txErr != nil {
				mock.ExpectBegin().WillReturnError(testcase.txErr)
			} else {
				mock.ExpectBegin()
				if testcase.queryErr != nil {
					mock.ExpectQuery("SELECT id, topic, payload FROM events LIMIT 2 FOR UPDATE SKIP LOCKED").WillReturnError(testcase.queryErr)
					mock.ExpectRollback()
				} else {
					if testcase.tName == "fail scan" {
						mock.ExpectQuery("SELECT id, topic, payload FROM events LIMIT 2 FOR UPDATE SKIP LOCKED").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow("invalid id", topic, payload))
					} else {
						mock.ExpectQuery("SELECT id, topic, payload FROM events LIMIT 2 FOR UPDATE SKIP LOCKED").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
						if testcase.deleteErr != nil {
							mock.ExpectExec("DELETE FROM kafka_events where id IN \\('2'\\)").WillReturnError(testcase.deleteErr)
							mock.ExpectRollback()
						} else {
							mock.ExpectExec("DELETE FROM kafka_events where id IN \\('2'\\)").WillReturnResult(sqlmock.NewResult(1, 1))
							if testcase.kafkaErr != nil {
								kafka.EXPECT().SendMessages(gomock.Any()).Return(testcase.kafkaErr)
								mock.ExpectRollback()
							} else {
								kafka.EXPECT().SendMessages(gomock.Any()).Return(nil)
								if testcase.commitErr != nil {
									mock.ExpectCommit().WillReturnError(testcase.commitErr)
								} else {
									mock.ExpectCommit()
								}
							}
						}
					}
				}
			}

			consumer := pkg.KafkaConsumer{
				DB:        db,
				Kafka:     kafka,
				Batch:     2,
				WorkerNum: 1,
			}

			err = consumer.Process()
			if (testcase.outputErr == nil && err != nil) ||
				(testcase.outputErr != nil && err == nil) ||
				(err != nil && testcase.outputErr.Error() != err.Error()) {
				t.Errorf("Test %s, err is %v, should be %v", testcase.tName, err, testcase.outputErr)
			}

			consumer.Shutdown()
			consumer.Run()

			if err := mock.ExpectationsWereMet(); err != nil {
				panic(err)
			}
		})
	}
}
