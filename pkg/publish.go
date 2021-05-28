package pkg

import (
	"context"
	"database/sql"
	"fmt"
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
