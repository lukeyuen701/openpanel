# The Go event receiver. Build context is this directory:
#   docker build -t openpanel-ingest apps/ingest
#
# The builder may be newer than go.mod's go directive: the directive is the
# minimum language version, the builder decides which stdlib (and its
# security fixes) ships in the binary.
ARG GO_VERSION=1.27.4

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingest ./cmd/ingest

# The GeoLite databases, from the same sources packages/core's codegen uses:
# the public redistribution, or MaxMind directly when a license key is passed
# as a build secret (never an ARG, which lands in the image history):
#   docker build --secret id=maxmind_license_key,env=MAXMIND_LICENSE_KEY ...
FROM alpine:3.22 AS geo
RUN apk add --no-cache curl tar
WORKDIR /geo
RUN --mount=type=secret,id=maxmind_license_key,required=false \
    MAXMIND_LICENSE_KEY="$(cat /run/secrets/maxmind_license_key 2>/dev/null)"; \
    for db in GeoLite2-City GeoLite2-ASN; do \
      if [ -n "$MAXMIND_LICENSE_KEY" ]; then \
        url="https://download.maxmind.com/app/geoip_download?edition_id=$db&license_key=$MAXMIND_LICENSE_KEY&suffix=tar.gz"; \
      else \
        url="https://raw.githubusercontent.com/GitSquared/node-geolite2-redist/master/redist/$db.tar.gz"; \
      fi; \
      curl -fsSL "$url" | tar -xz --strip-components=1 --wildcards '*.mmdb'; \
    done && test -s GeoLite2-City.mmdb && test -s GeoLite2-ASN.mmdb

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingest /app/ingest
COPY --from=geo /geo/*.mmdb /app/geo/
ENV INGEST_GEO_DIR=/app/geo \
    INGEST_PORT=3000 \
    INGEST_ADMIN_PORT=3001
EXPOSE 3000 3001
HEALTHCHECK --interval=10s --timeout=5s --retries=5 CMD ["/app/ingest", "healthcheck"]
ENTRYPOINT ["/app/ingest"]
