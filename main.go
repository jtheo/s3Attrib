package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	var bucket string
	var list string
	var workers int
	flag.StringVar(&bucket, "b", "", "bucket to read")
	flag.StringVar(&list, "l", "", "file list")
	flag.IntVar(&workers, "w", 20, "number of workers")
	flag.Parse()

	if list == "" || bucket == "" {
		flag.Usage()
		os.Exit(1)
	}

	fd, err := os.Open(list)
	if err != nil {
		log.Fatalln(err)
	}

	defer fd.Close()

	sc := bufio.NewScanner(fd)

	lines := []string{}

	for sc.Scan() {
		lines = append(lines, sc.Text())
	}

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}

	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return
	}

	jobs := make(chan string, workers)

	wg := sync.WaitGroup{}
	for w := 1; w <= workers; w++ {
		wg.Add(1)
		go worker(bucket, sdkConfig, jobs, &wg)
	}

	for _, key := range lines {
		if key == "" {
			log.Fatalf("Empty Key!!!")
		}
		jobs <- key
	}
	close(jobs)
	wg.Wait()
}

func worker(bucket string, sdkConfig aws.Config, jobs chan string, wg *sync.WaitGroup) {
	s3Client := s3.NewFromConfig(sdkConfig)
	for key := range jobs {
		res, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		res.Body.Close()
		if err != nil {
			log.Println(err)
			time.Sleep(5 * time.Second)
			jobs <- key
		}
		fmt.Printf("Res: %s: %+v\n", key, res.ServerSideEncryption)
	}
	wg.Done()
}
