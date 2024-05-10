package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	c := run()

	jobs := make(chan string, c.workers)
	results := make(chan Result, len(*c.keys))

	go func() {
		for w := 1; w <= c.workers; w++ {
			go worker(c.bucket, c.sdkConfig, jobs, results, c)
		}
		for _, key := range *c.keys {
			jobs <- key
		}
		close(jobs)
	}()

	for r := 0; r < len(*c.keys); r++ {
		res := <-results
		fmt.Printf("Key: %s - Size: %d\n", res.key, *res.size)
	}
}

type Result struct {
	size *int64
	key  string
}

func worker(bucket string, sdkConfig aws.Config, jobs chan string, results chan Result, c Config) {
	s3Client := s3.NewFromConfig(sdkConfig)

	for key := range jobs {
		// fmt.Printf("Key: %s, len(jobs): %d\n", key, len(jobs))
		res, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			c.logger.Printf("Key: %s in error: %v\n", key, err)
			continue
		}
		results <- Result{
			key:  key,
			size: res.ContentLength,
		}

		if res.Body != nil {
			res.Body.Close()
		}
	}
}

type Config struct {
	logger    *log.Logger
	keys      *[]string
	bucket    string
	list      string
	sdkConfig aws.Config
	workers   int
}

func run() Config {
	c := Config{}
	var err error
	buf := bytes.Buffer{}
	c.logger = log.New(&buf, "logger: ", log.Lshortfile)

	flag.StringVar(&c.bucket, "b", "", "bucket to read")
	flag.StringVar(&c.list, "l", "", "file list")
	flag.IntVar(&c.workers, "w", 20, "number of workers")
	flag.Parse()

	if c.list == "" || c.bucket == "" {
		flag.Usage()
		os.Exit(1)
	}

	c.sdkConfig, err = config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		c.logger.Fatalln(err)
	}
	keys := load(c)
	c.keys = &keys

	if len(keys) < c.workers {
		c.workers = len(keys)
	}

	return c
}

func load(c Config) []string {
	fd, err := os.Open(c.list)
	if err != nil {
		c.logger.Fatalln(err)
	}
	defer fd.Close()

	keys := []string{}

	sc := bufio.NewScanner(fd)
	for sc.Scan() {
		keys = append(keys, sc.Text())
	}

	if err := sc.Err(); err != nil {
		c.logger.Fatal(err)
	}
	return keys
}
