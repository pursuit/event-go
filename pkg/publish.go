package pkg

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

//go:generate mockgen -source=publish.go -destination=mock/publish.go

type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type EventData struct {
	Topic   string
	Payload []byte
}

func StoreEvent(ctx context.Context, db DB, data EventData) error {
	_, err := db.ExecContext(ctx, "INSERT INTO events (topic,payload) VALUES($1,$2)", data.Topic, data.Payload)
	if err != nil {
		return fmt.Errorf("event: %w", err)
	}

	return nil
}

type KafkaPublishFromSQL struct {
	Batch     uint
	DB        *sql.DB
	Kafka     sarama.SyncProducer
	QInterval time.Duration
	WorkerNum uint

	stopChan chan struct{}
}

func (this *KafkaPublishFromSQL) Run() {
	this.stopChan = make(chan struct{})
	for i := uint(1); i <= this.WorkerNum; i++ {
		go func() {
			for {
				select {
				case <-this.stopChan:
					return
				default:
					if err := this.Process(); err != nil {
						sleepInterval := this.QInterval
						if err != sql.ErrNoRows {
							sleepInterval += 10 * time.Second
						}
						time.Sleep(sleepInterval)
					}
				}
			}
		}()
	}
}

func (this *KafkaPublishFromSQL) Shutdown() {
	for i := uint(1); i <= this.WorkerNum; i++ {
		this.stopChan <- struct{}{}
	}
}

func (this *KafkaPublishFromSQL) Process() error {
	tx, err := this.DB.Begin()
	if err != nil {
		return err
	}

	if this.Batch < 2 {
		row := tx.QueryRow("SELECT id, topic, payload FROM events LIMIT 1 FOR UPDATE SKIP LOCKED")

		var id int
		var topic string
		var payload []byte
		if err := row.Scan(&id, &topic, &payload); err != nil {
			tx.Rollback()
			return err
		}

		_, err = tx.ExecContext(context.Background(), "DELETE FROM events where id = ?", id)
		if err != nil {
			tx.Rollback()
			return err
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(payload),
		}
		_, _, err = this.Kafka.SendMessage(msg)
		if err != nil {
			tx.Rollback()
			return err
		}
	} else {
		rows, err := tx.Query(fmt.Sprintf("SELECT id, topic, payload FROM events LIMIT %d FOR UPDATE SKIP LOCKED", this.Batch))
		if err != nil {
			tx.Rollback()
			return err
		}

		idAry := make([]string, 0)
		messages := make([]*sarama.ProducerMessage, 0)

		for rows.Next() {
			var id int
			var topic string
			var payload []byte
			if err := rows.Scan(&id, &topic, &payload); err != nil {
				tx.Rollback()
				return err
			}

			idAry = append(idAry, strconv.Itoa(id))
			messages = append(messages, &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(payload),
			})
		}
		if err := rows.Err(); err != nil {
			tx.Rollback()
			return err
		}

		if len(idAry) == 0 {
			tx.Rollback()
			return sql.ErrNoRows
		}

		ids := strings.Join(idAry, "','")
		_, err = tx.ExecContext(context.Background(), fmt.Sprintf(`DELETE FROM events where id IN ('%s')`, ids))
		if err != nil {
			tx.Rollback()
			return err
		}

		if err = this.Kafka.SendMessages(messages); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
