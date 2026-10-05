# One image holds every binary; compose selects the entrypoint per service.
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot
