package handlers

import (
	"context"
	"encoding/json"
	"github.com/vancho-go/rDNS/internal/app/models"
	"log/slog"
	"net/http"
)

type FQDNUploader interface {
	UploadFQDN(context.Context, []models.APIUploadFQDNRequest) error
}

func UploadFQDNList(uploader FQDNUploader) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		var FQDNs []models.APIUploadFQDNRequest

		decoder := json.NewDecoder(req.Body)
		if err := decoder.Decode(&FQDNs); err != nil {
			slog.ErrorContext(req.Context(), err.Error())
			http.Error(res, "Invalid request format", http.StatusBadRequest)
			return
		}

		res.WriteHeader(http.StatusOK)
		err := uploader.UploadFQDN(req.Context(), FQDNs)
		if err != nil {
			slog.ErrorContext(req.Context(), err.Error())
			return
		}
	}
}
