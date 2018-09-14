package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/knative/eventing/pkg/event"
)

type s3Event struct {
	Records []struct {
		EventVersion string    `json:"eventVersion"`
		EventSource  string    `json:"eventSource"`
		AwsRegion    string    `json:"awsRegion"`
		EventTime    time.Time `json:"eventTime"`
		EventName    string    `json:"eventName"`
		UserIdentity struct {
			PrincipalID string `json:"principalId"`
		} `json:"userIdentity"`
		RequestParameters struct {
			SourceIPAddress string `json:"sourceIPAddress"`
		} `json:"requestParameters"`
		ResponseElements struct {
			XAmzRequestID string `json:"x-amz-request-id"`
			XAmzID2       string `json:"x-amz-id-2"`
		} `json:"responseElements"`
		S3 struct {
			S3SchemaVersion string `json:"s3SchemaVersion"`
			ConfigurationID string `json:"configurationId"`
			Bucket          struct {
				Name          string `json:"name"`
				OwnerIdentity struct {
					PrincipalID string `json:"principalId"`
				} `json:"ownerIdentity"`
				Arn string `json:"arn"`
			} `json:"bucket"`
			Object struct {
				Key       string `json:"key"`
				Size      int    `json:"size"`
				ETag      string `json:"eTag"`
				Sequencer string `json:"sequencer"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func handler(ctx context.Context, m *sqs.Message) {
	metadata := event.FromContext(ctx)
	log.Printf("[%s] %s : %s\n", metadata.EventTime.Format(time.RFC3339), metadata.Source, *m.Body)
	var event s3Event
	err := json.Unmarshal([]byte(*m.Body), &event)
	if err != nil {
		log.Fatalf("Failed to unmarshal message: %v\n", err)
		return
	}
	sourceRoleArn := os.Getenv("SOURCE_ROLE_ARN")
	if sourceRoleArn == "" {
		log.Fatalf("SOURCE_ROLE_ARN is not configured in ENV.\n")
		return
	}
	sourceBucket := os.Getenv("SOURCE_BUCKET")
	if sourceBucket == "" {
		log.Fatalf("SOURCE_BUCKET is not configured in ENV.\n")
		return
	}
	sourceBucketRegion := os.Getenv("SOURCE_BUCKET_REGION")
	if sourceBucketRegion == "" {
		log.Fatalf("SOURCE_BUCKET_REGION is not configured in ENV.\n")
		return
	}
	targetRoleArn := os.Getenv("TARGET_ROLE_ARN")
	if targetRoleArn == "" {
		log.Fatalf("TARGET_ROLE_ARN is not configured in ENV.\n")
		return
	}
	targetBucket := os.Getenv("TARGET_BUCKET")
	if targetBucket == "" {
		log.Fatalf("TARGET_BUCKET is not configured in ENV.\n")
		return
	}
	targetBucketRegion := os.Getenv("TARGET_BUCKET_REGION")
	if targetBucketRegion == "" {
		log.Fatalf("TARGET_BUCKET_REGION is not configured in ENV.\n")
		return
	}
	for _, v := range event.Records {
		if v.EventSource != "aws:s3" {
			log.Println("Not S3 event, skip.")
			continue
		}
		fileName := v.S3.Object.Key
		bucketName := v.S3.Bucket.Name
		if bucketName != sourceBucket {
			log.Printf("Not interested in bucket %s\n", bucketName)
			continue
		}
		if v.EventName == "ObjectCreated:Put" {
			log.Printf("Copying file %s from %s to %s ...", fileName, bucketName, targetBucket)
			err := copyFile(bucketName, fileName, sourceRoleArn, sourceBucketRegion, targetRoleArn, targetBucket, targetBucketRegion)
			if err != nil {
				log.Printf("Copy file error: s3://%s -> s3://%s, %s, %v\n", bucketName, targetBucket, fileName, err)
				return
			}
			log.Printf("Finished copying file %s from %s to %s", fileName, bucketName, targetBucket)
		} else if v.EventName == "ObjectRemoved:Delete" {
			log.Printf("Deleting copy file of %s from %s", fileName, targetBucket)
			err := deleteFile(fileName, targetRoleArn, targetBucket, targetBucketRegion)
			if err != nil {
				log.Printf("Delete file error: s3://%s, %s, %v\n", targetBucket, fileName, err)
				return
			}
			log.Printf("Finished deleting copy file from %s", targetBucket)
		} else {
			log.Printf("Warning: Unrecognized event.")
		}
	}
}

func copyFile(bucketName string, fileName string, sourceRoleArn string, sourceRgion string, targetRoleArn string, targetBucket string, targetRgion string) error {
	newFile, err := downloadFile(bucketName, fileName, sourceRoleArn, sourceRgion)
	if err != nil {
		return fmt.Errorf("Downdload file error: %v", err)
	}
	err = uploadFile(targetBucket, newFile, targetRoleArn, targetRgion)
	if err != nil {
		return fmt.Errorf("Upload file error: %v", err)
	}
	err = os.Remove(newFile)
	if err != nil {
		return fmt.Errorf("Unable to delete temp file: %v", err)
	}
	return nil
}

func uploadFile(bucketName string, fileName string, roleArn string, region string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("Unable to open file %s\n", fileName)
	}
	defer file.Close()
	sess := session.Must(session.NewSession())
	creds := stscreds.NewCredentials(sess, roleArn)
	sess = session.Must(session.NewSession(&aws.Config{Credentials: creds,
		Region: aws.String(region)}))
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("Unable to upload %q to %q, %v", fileName, bucketName, err)
	}
	return nil
}

func downloadFile(bucketName string, fileName string, roleArn string, region string) (string, error) {
	newFileName := "copy_" + fileName
	file, err := os.Create(newFileName)
	if err != nil {
		return "", fmt.Errorf("Unable to open file %s\n", newFileName)
	}
	defer file.Close()
	sess := session.Must(session.NewSession())
	creds := stscreds.NewCredentials(sess, roleArn)
	sess = session.Must(session.NewSession(&aws.Config{Credentials: creds,
		Region: aws.String(region)}))
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fileName),
		})
	if err != nil {
		return "", fmt.Errorf("Unable to download item %q, %v", fileName, err)
	}
	log.Println("Downloaded", file.Name(), numBytes, "bytes")
	return newFileName, nil
}

func deleteFile(fileName string, roleArn, bucketName string, region string) error {
	newFileName := "copy_" + fileName
	sess := session.Must(session.NewSession())
	creds := stscreds.NewCredentials(sess, roleArn)

	svc := s3.New(sess, &aws.Config{Credentials: creds,
		Region: aws.String(region)})
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(newFileName)})
	if err != nil {
		return fmt.Errorf("Unable to delete object %q from bucket %q, %v", newFileName, bucketName, err)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(newFileName),
	})
	if err != nil {
		return fmt.Errorf("Error occurred while waiting for object %q to be deleted, %v", newFileName, err)
	}
	log.Printf("Object %q successfully deleted\n", newFileName)
	return nil
}

func main() {
	log.Print("Ready and listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", event.Handler(handler)))
}
