FROM golang:1.26-alpine AS build
WORKDIR /src
ENV GOEXPERIMENT=jsonv2
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /login-service ./login-service && \
    CGO_ENABLED=0 go build -o /marketplace-service ./marketplace-service && \
    CGO_ENABLED=0 go build -o /marketplace-worker ./marketplace-worker && \
    CGO_ENABLED=0 go build -o /outbox-worker ./outbox-worker

FROM alpine:3.21 AS login
COPY --from=build /login-service /login-service
EXPOSE 8081
ENTRYPOINT ["/login-service"]

FROM alpine:3.21 AS marketplace
COPY --from=build /marketplace-service /marketplace-service
EXPOSE 8080
ENTRYPOINT ["/marketplace-service"]

FROM alpine:3.21 AS worker
COPY --from=build /marketplace-worker /marketplace-worker
ENTRYPOINT ["/marketplace-worker"]

FROM alpine:3.21 AS outbox
COPY --from=build /outbox-worker /outbox-worker
ENTRYPOINT ["/outbox-worker"]
