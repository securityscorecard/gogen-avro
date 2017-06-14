FROM registry.daymax.xyz/tm-go-deployer:latest

ENV binary gogen-avro

WORKDIR /app

RUN update-ca-certificates --fresh

ADD $binary /app/

ENTRYPOINT ["./$binary"]
