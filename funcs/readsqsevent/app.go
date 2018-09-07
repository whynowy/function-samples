package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/knative/eventing/pkg/event"
)

func handler(ctx context.Context, m *sqs.Message) {
	metadata := event.FromContext(ctx)
	log.Printf("[%s] %s : %s", metadata.EventTime.Format(time.RFC3339), metadata.Source, *m.Body)
}

func main() {
	log.Print("Ready and listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", event.Handler(handler)))
}
