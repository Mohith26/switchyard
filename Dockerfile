# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/switchyard ./cmd/switchyard

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl && adduser -D -u 10001 switchyard
COPY --from=build /out/switchyard /usr/local/bin/switchyard
USER switchyard
ENTRYPOINT ["switchyard"]
