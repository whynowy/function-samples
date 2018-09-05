FROM golang:1.10.1

WORKDIR /go/src/github.com/whynowy/function-samples/
COPY . .
WORKDIR funcs/simple/

RUN CGO_ENABLED=0 GOOS=linux go build -v -o app

FROM scratch
COPY --from=0 /go/src/github.com/whynowy/function-samples/funcs/simple/app .
ENTRYPOINT ["/app"]
