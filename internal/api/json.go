package api

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
)

// decodeJSON reads exactly one JSON object from the (already size-limited)
// body. It is strict on purpose: unknown fields are rejected so a typo such
// as "idempotency_ky" fails loudly instead of silently disabling
// deduplication, and trailing data is rejected so requests are unambiguous.
func decodeJSON(r *http.Request, dst any) error {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		return newErr(http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		switch {
		case errors.As(err, &tooBig):
			return err
		case errors.Is(err, io.EOF):
			return newErr(http.StatusBadRequest, "invalid_json", "request body is empty")
		default:
			return newErr(http.StatusBadRequest, "invalid_json", "%s", err.Error())
		}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return err
		}
		return newErr(http.StatusBadRequest, "invalid_json", "unexpected data after the JSON object")
	}
	return nil
}
