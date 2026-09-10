FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api

FROM alpine:3.20
RUN addgroup -S app && adduser -S app -G app
USER app
COPY --from=build /out/api /usr/local/bin/api
ENTRYPOINT ["/usr/local/bin/api"]
