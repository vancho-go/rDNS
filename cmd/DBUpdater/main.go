package main

import (
	"context"
	"github.com/go-chi/chi/v5"
	"github.com/vancho-go/rDNS/internal/app/handlers"
	"github.com/vancho-go/rDNS/internal/app/storage"
	"log"
	"net/http"
	"time"
)

// only for testing purposes
const (
	databaseURI    = "host=localhost port=5432 user=vancho password=vancho_pswd dbname=vancho_db sslmode=disable"
	updateInterval = 5 * time.Second
)

func periodicUpdateExecutor(ctx context.Context, interval time.Duration, task func(context.Context)) {
	for {
		task(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func main() {

	dbInstance, err := storage.Initialize(databaseURI)
	if err != nil {
		log.Fatalf("error initialising database: %s", err.Error())
	}

	ctx := context.Background()

	go periodicUpdateExecutor(ctx, updateInterval, dbInstance.UpdateDNSRecords)

	r := chi.NewRouter()

	r.Route("/api", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Post("/fqdn", handlers.UploadFQDNList(dbInstance))
		})
	})

	err = http.ListenAndServe(":8080", r)
	if err != nil {
		log.Fatalf("error starting server: %s", err.Error())
	}
}
