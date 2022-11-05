FROM golang:1.19-alpine3.15 AS build-env
RUN apk --no-cache add build-base git

WORKDIR /binance.d
COPY . .

RUN \
    cd cmd/binance && \
    go build -o /binance

FROM alpine:3.15
WORKDIR /app
RUN apk add --no-cache libstdc++ libgcc
COPY --from=build-env /binance /app/binance

ENTRYPOINT [ "./binance" ]
