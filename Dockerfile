# Stage 1 - Build binary
FROM golang:1.9.1 AS builder

WORKDIR /go/src/github.com/alanctgardner/gogen-avro
ADD . .

ENV CGO_ENABLED=0

RUN go build -o /out/gogen-avro -v -a -tags netgo -ldflags="-s -w" .

# Stage 2 - Package binary
FROM scratch

WORKDIR /app/

COPY --from=builder /out/gogen-avro .
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

ENTRYPOINT ["./gogen-avro"]
