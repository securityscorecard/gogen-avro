# Stage 1 - Build binary
FROM golang:1.9.1 AS builder

WORKDIR /go/src/github.com/alanctgardner/gogen-avro
ADD . .

ENV CGO_ENABLED=0

RUN go build -o /out/gogen-avro -v -a -tags netgo -ldflags="-s -w" .

# Stage 2 - Package binary
#
# Note that we cannot use 'FROM scratch' here because this container needs to be
# runnable in Jenkins, which requires 'cat' to exist for 'docker.inside()' to
# work.
FROM alpine:3.6

WORKDIR /app/

COPY --from=builder /out/gogen-avro .
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

ENTRYPOINT ["./gogen-avro"]
