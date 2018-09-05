FROM golang:1.10.1

WORKDIR /go/src/github.com/whynowy/function-samples/
COPY . .
WORKDIR funcs/lists3buckets/

RUN CGO_ENABLED=0 GOOS=linux go build -v -o app

FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=0 /go/src/github.com/whynowy/function-samples/funcs/lists3buckets/app .
CMD ["/app"]
