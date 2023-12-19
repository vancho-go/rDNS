package main

import (
	"github.com/go-chi/chi/v5"
	"github.com/vancho-go/rDNS/internal/app/handlers"
	"github.com/vancho-go/rDNS/internal/app/storage"
	"log"
	"net/http"
)

// only for testing purposes
const (
	databaseURI = "host=localhost port=5432 user=vancho password=vancho_pswd dbname=vancho_db sslmode=disable"
)

func main() {

	dbInstance, err := storage.Initialize(databaseURI)
	if err != nil {
		log.Fatalf("error initialising database: %s", err.Error())
	}

	r := chi.NewRouter()

	r.Route("/api", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Get("/ip", handlers.GetFQNDs(dbInstance))
		})
	})

	err = http.ListenAndServe(":8090", r)
	if err != nil {
		log.Fatalf("error starting server: %s", err.Error())
	}
}
