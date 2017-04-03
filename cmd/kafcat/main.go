package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/euforia/kafka-websocket"
)

var (
	peerList = flag.String("peers", os.Getenv("KAFKA_PEERS"), "Kafka peers [ comma-separated host:port ]")

	startPos   = flag.String("s", "oldest", "Starting position [ 'oldest' | 'newest' ]")
	offset     = flag.Uint64("o", 0, "Offset from the starting position")
	topic      = flag.String("t", "", "Topic to consume")
	partitions = flag.String("p", "all", "Partitions to consume [ 'all' | comma-separated numbers ]")
	bufferSize = flag.Int("buffer-size", 256, "Message channel buffer size")

	listTopics = flag.Bool("l", false, "List topics")

	verbose = flag.Bool("verbose", false, "Turn on underlying (sarama) logging")
	logger  = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
)

func init() {
	flag.Parse()

	if *peerList == "" {
		printUsageErrorExit("-peers or KAFKA_PEERS variable required")
	}

	if !*listTopics && *topic == "" {
		printUsageErrorExit("-topic is required")
	}

	if *verbose {
		sarama.Logger = logger
	}
}

func main() {
	peers := kafkatoolkit.ParsePeers(*peerList)

	c, err := sarama.NewConsumer(peers, nil)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	tpcs, er := c.Topics()
	if er != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	if *listTopics {
		for _, t := range tpcs {
			fmt.Println(t)
		}
		c.Close()
		os.Exit(0)
	} else {
		found := false
		for _, t := range tpcs {
			if t == *topic {
				found = true
				break
			}
		}
		if !found {
			printErrorAndExit(70, "topic not found")
		}
	}

	initialOffset, err := kafkatoolkit.OffsetFromPosition(*startPos, *offset)
	if err != nil {
		printUsageErrorExit("-s must be [ oldest | newest ]")
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)
	// Start signal handler
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	// Start consuming all partitions in in go routines
	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(*topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messages {
			fmt.Printf("[ %d/%d/%s ] %s\n", msg.Partition, msg.Offset, msg.Key, msg.Value)
		}
	}()

	wg.Wait()
	logger.Println("Done consuming:", *topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
