package main

import (
	"github.com/go-chi/chi/v5"
	"github.com/vancho-go/rDNS/internal/app/storage"
	"log"
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
	_ = dbInstance

	r := chi.NewRouter()

	r.Route("/api", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Get("/ip", nil)
		})
	})
}
