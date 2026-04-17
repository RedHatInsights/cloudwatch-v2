package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/RedHatInsights/cloudwatch"
	"github.com/pborman/uuid"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	g := cloudwatch.NewGroup("test", cloudwatchlogs.NewFromConfig(cfg))

	stream := uuid.New()

	w, err := g.Create(stream)
	if err != nil {
		log.Fatal(err)
	}

	r, err := g.Open(stream)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		var i int
		for {
			i++
			<-time.After(time.Second / 30)
			_, err := fmt.Fprintf(w, "Line %d\n", i)
			if err != nil {
				log.Println(err)
			}
		}
	}()

	io.Copy(os.Stdout, r)
}
