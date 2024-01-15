package main

import (
	"context"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/vancho-go/rDNS/internal/app/handlers"
	"github.com/vancho-go/rDNS/internal/app/storage"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

// only for testing purposes
const (
	databaseURI    = "host=localhost port=5432 user=vancho password=vancho_pswd dbname=vancho_db sslmode=disable"
	updateInterval = 10 * time.Millisecond
)

func periodicUpdateExecutor(ctx context.Context, interval time.Duration, task func(context.Context, *int64), fqdnProcessed *int64) {
	for {
		task(ctx, fqdnProcessed)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fqdnProcessed int64

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	go periodicUpdateExecutor(ctx, updateInterval, dbInstance.UpdateDNSRecords, &fqdnProcessed)

	timer := time.After(5 * time.Minute)
	<-timer
	cancel()
	fmt.Printf("FQDNs processed in 5 min: %d\n\n", fqdnProcessed)

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
