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

type FQDNGetter interface {
	GetFQDNs(context.Context, models.APIGetFQDNsRequest) (models.APIGetFQDNsResponse, error)
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

		err := uploader.UploadFQDN(req.Context(), FQDNs)
		if err != nil {
			slog.ErrorContext(req.Context(), err.Error())
			return
		}
	}
}

func GetFQNDs(fg FQDNGetter) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		var ipAddresses models.APIGetFQDNsRequest
		decoder := json.NewDecoder(req.Body)
		if err := decoder.Decode(&ipAddresses.IPAddresses); err != nil {
			slog.ErrorContext(req.Context(), err.Error())
			http.Error(res, "Invalid request format", http.StatusBadRequest)
			return
		}

		fqdns, err := fg.GetFQDNs(req.Context(), ipAddresses)
		if err != nil {
			slog.ErrorContext(req.Context(), err.Error())
			http.Error(res, "Something went wrong", http.StatusInternalServerError)
			return
		}

		res.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(res)
		if err := encoder.Encode(fqdns); err != nil {
			slog.ErrorContext(req.Context(), err.Error())
			http.Error(res, "Something went wrong", http.StatusInternalServerError)
			return
		}
	}
}
