package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
)

var (
	s3Client          *s3.S3
	AWS_CLIENT_ID     string
	AWS_CLIENT_SECRET string
	AWS_S3_BUCKET     string
	AWS_REGION        string
	wg                sync.WaitGroup
)

func init() {
	setConfig()
	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String("us-east-1"),
			Credentials: credentials.NewStaticCredentials(
				AWS_CLIENT_ID,
				AWS_CLIENT_SECRET,
				"",
			),
		},
	)
	if err != nil {
		panic(err)
	}

	s3Client = s3.New(sess)
}

func setConfig() {
	err := godotenv.Load("../../.env")
	if err != nil {
		panic(err)
	}

	AWS_CLIENT_ID = os.Getenv("AWS_CLIENT_ID")
	AWS_CLIENT_SECRET = os.Getenv("AWS_CLIENT_SECRET")
	AWS_REGION = os.Getenv("AWS_REGION")
	AWS_S3_BUCKET = os.Getenv("AWS_S3_BUCKET")
}

func main() {

	dir, err := os.Open("../../tmp")
	if err != nil {
		panic(err)
	}
	defer dir.Close()

	uploadControl := make(chan struct{}, 100)
	errorFileUpload := make(chan string, 3)

	go func() {
		for {
			select {
			case filename := <-errorFileUpload:
				uploadControl <- struct{}{}
				wg.Add(1)
				go uploadFile(filename, uploadControl, errorFileUpload)
			}
		}

	}()

	for {
		files, err := dir.ReadDir(1)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading directory: %s \n", err)
			continue
		}
		wg.Add(1)
		uploadControl <- struct{}{}
		go uploadFile(files[0].Name(), uploadControl, errorFileUpload)
	}
	wg.Wait()
}

func uploadFile(filename string, uploadControl <-chan struct{}, errorList chan string) {
	defer wg.Done()
	completeFileName := fmt.Sprintf("../../tmp/%s", filename)

	fmt.Printf("Uploading file %s in bucket %s \n", completeFileName, AWS_S3_BUCKET)

	f, err := os.Open(completeFileName)
	if err != nil {
		fmt.Printf("Error opening file %s \n", completeFileName)
		<-uploadControl // esvazia o canal
		errorList <- filename
		return
	}
	defer f.Close()

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(AWS_S3_BUCKET),
		Key:    aws.String(filename),
		Body:   f,
	})
	if err != nil {
		fmt.Printf("Error uploading file %s \n", completeFileName)
		<-uploadControl // esvazia o canal
		errorList <- filename
		return
	}

	fmt.Printf("File %s uploaded successfully \n", completeFileName)
	<-uploadControl // esvazia o canal
}
