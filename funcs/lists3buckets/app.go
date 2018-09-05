package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func handler(w http.ResponseWriter, r *http.Request) {
	log.Print("ListS3Buckets app running...")

	roleArn := os.Getenv("ROLE_ARN")
	if roleArn == "" {
		fmt.Fprintf(w, "FATAL: ROLE_ARN is not defined in ENV\n")
		return
	}

	region := os.Getenv("REGION")
	if region == "" {
		region = "us-west-2"
	}

	sess := session.Must(session.NewSession())
	creds := stscreds.NewCredentials(sess, roleArn)

	svc := s3.New(sess, &aws.Config{Credentials: creds,
		Region: aws.String(region)})

	result, err := svc.ListBuckets(nil)
	if err != nil {
		log.Printf("Unable to list buckets, %v\n", err)
		fmt.Fprintf(w, "Error: Unable to list buckets, %v\n", err)
		return
	}
	fmt.Fprintf(w, "Buckets:\n")
	for _, b := range result.Buckets {
		fmt.Fprintf(w, "* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}

func main() {
	flag.Parse()
	log.Print("ListS3Buckets app server started...")
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}
