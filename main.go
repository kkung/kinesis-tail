package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func listStreams(svc *kinesis.Kinesis) error {
	rsp, err := svc.ListStreams(&kinesis.ListStreamsInput{})
	if err != nil {
		return err
	}

	for _, s := range rsp.StreamNames {
		fmt.Println(*s)
	}

	return nil
}

func followShard(svc *kinesis.Kinesis, streamName string, shardId string, stream chan string) error {
	rsp, err := svc.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardId),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(streamName),
	})

	if err != nil {
		return err
	}

	shardIterator := rsp.ShardIterator

	for {
		rrsp, err := svc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: aws.String(*shardIterator),
		})

		if err != nil {
			return err
		}

		for _, record := range rrsp.Records {
			stream <- string(record.Data[:])
		}

		shardIterator = rrsp.NextShardIterator

		if len(rrsp.Records) == 0 {
			time.Sleep(1000 * time.Millisecond)
		}
	}

}

func followStream(svc *kinesis.Kinesis, streamName *string, shardId *string) error {
	rsp, err := svc.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(*streamName),
	})

	if err != nil {
		return err
	}

	shardIds := make([]string, 0)
	stream := make(chan string, 5)

	for _, s := range rsp.StreamDescription.Shards {
		if shardId != nil {
			if *s.ShardId == *shardId {
				shardIds = append(shardIds, *s.ShardId)
			}
		} else {
			shardIds = append(shardIds, *s.ShardId)
		}
	}

	if len(shardIds) == 0 {
		return fmt.Errorf("Shards empty or shard id %v not found", *shardId)
	}

	for _, sid := range shardIds {
		go followShard(svc, *streamName, sid, stream)
	}

	for {
		fmt.Println(<-stream)
	}

	return nil
}

func main() {
	svc := kinesis.New(session.New())

	if len(os.Args) < 2 {
		err := listStreams(svc)
		if err != nil {
			fmt.Println(os.Stderr, err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

	var shardId *string
	streamName := os.Args[1]
	if len(os.Args) > 2 {
		shardId = &os.Args[2]
	} else {
		shardId = nil
	}

	err := followStream(svc, &streamName, shardId)
	if err != nil {
		fmt.Println(os.Stderr, err.Error())
		os.Exit(1)
	}
}
