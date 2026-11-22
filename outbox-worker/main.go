// The projection worker fails fast on a write error. Kubernetes restarts it;
// the undelivered row remains in Postgres for replay.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"ece573-prj07/shared/history"
	"ece573-prj07/shared/marketplace"
	"ece573-prj07/shared/model"
)

func main() {
	store, err := marketplace.OpenStore(marketplace.DatabaseConfig{
		Host: os.Getenv("POSTGRES_HOST"), Port: os.Getenv("POSTGRES_PORT"),
		Database: os.Getenv("POSTGRES_DB"), User: os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
	})
	if err != nil {
		log.Fatal("FATAL open Postgres: ", err)
	}
	defer store.Close()
	replicas, err := strconv.Atoi(envDefault("CASSANDRA_REPLICATION_FACTOR", "1"))
	if err != nil {
		log.Fatal("FATAL CASSANDRA_REPLICATION_FACTOR: ", err)
	}
	h, err := history.OpenHistoryOnce(history.DatabaseConfig{
		ContactPoints:     os.Getenv("CASSANDRA_CONTACT_POINTS"),
		ReplicationFactor: replicas,
		ReadConsistency:   envDefault("READ_CONSISTENCY", "QUORUM"),
		WriteConsistency:  envDefault("WRITE_CONSISTENCY", "QUORUM"),
	})
	if err != nil {
		log.Fatal("FATAL open Cassandra: ", err)
	}
	defer h.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for ctx.Err() == nil {
		projection, err := store.NextProjection(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatal("FATAL read projection outbox: ", err)
		}
		if err := apply(h, projection); err != nil {
			log.Fatalf("FATAL projection_failed id=%d job_id=%s: %v", projection.ID, projection.JobID, err)
		}
		if err := store.MarkProjectionDelivered(ctx, projection.ID); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatalf("FATAL mark projection delivered id=%d job_id=%s: %v", projection.ID, projection.JobID, err)
		}
		log.Printf("INFO event=projection_delivered id=%d job_id=%s", projection.ID, projection.JobID)
	}
}

func apply(h *history.History, projection marketplace.Projection) error {
	switch projection.Kind {
	case "listing":
		var listing model.Listing
		if err := json.Unmarshal(projection.Payload, &listing); err != nil {
			return err
		}
		return h.WriteListing(listing)
	case "order":
		var order model.Order
		if err := json.Unmarshal(projection.Payload, &order); err != nil {
			return err
		}
		return h.WriteOrder(order)
	default:
		return errors.New("unknown projection kind: " + projection.Kind)
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
