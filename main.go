package main

import (
	"bufio"
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
			go worker(c.bucket, c.sdkConfig, jobs, results)
		}
		for _, key := range *c.keys {
			jobs <- key
		}
		close(jobs)
	}()

	for r := 0; r < len(*c.keys); r++ {
		res := <-results
		fmt.Printf("Enc: %s - Key: %s\n", res.enc, res.key)
	}
}

type Result struct {
	key string
	enc string
}

func worker(bucket string, sdkConfig aws.Config, jobs chan string, results chan Result) {
	s3Client := s3.NewFromConfig(sdkConfig)

	for key := range jobs {
		// fmt.Printf("Key: %s, len(jobs): %d\n", key, len(jobs))
		res, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			log.Printf("Key: %s in error: %v\n", key, err)
			continue
		}
		if res.Body != nil {
			res.Body.Close()
		}

		results <- Result{
			key: key,
			enc: string(res.ServerSideEncryption),
		}
	}
}

type Config struct {
	bucket    string
	list      string
	sdkConfig aws.Config
	workers   int
	keys      *[]string
}

func run() Config {
	c := Config{}
	var err error

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
		log.Fatalln(err)
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
		log.Fatalln(err)
	}
	defer fd.Close()

	keys := []string{}

	sc := bufio.NewScanner(fd)
	for sc.Scan() {
		keys = append(keys, sc.Text())
	}

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
	return keys
}
