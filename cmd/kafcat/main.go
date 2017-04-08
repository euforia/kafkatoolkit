package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/euforia/kafkatoolkit"
)

var (
	peerList = flag.String("peers", os.Getenv("KAFKA_PEERS"), "Kafka peers [ comma-separated host:port ]")

	//startPos   = flag.String("s", "oldest", "Starting position [ 'oldest' | 'newest' ]")
	//offset     = flag.Uint64("o", 0, "Offset from the starting position")
	head = flag.Bool("head", false, "")
	tail = flag.Bool("tail", false, "")

	topic = flag.String("t", "", "Topic to consume")
	//partitions = flag.String("p", "all", "Partitions to consume [ 'all' | comma-separated numbers ]")

	bufferSize = flag.Int("buffer-size", 256, "Message channel buffer size")

	listTopics = flag.Bool("l", false, "List topics")

	filterString = flag.String("filter", "", "Filter")

	printHeader = flag.Bool("header", false, "Print header")

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

	if *head && *tail {
		printUsageErrorExit("-head | -tail cannot specify both")
	}

	if *verbose {
		sarama.Logger = logger
	}
}

func cliListTopics(peers []string) {
	c, err := sarama.NewConsumer(peers, nil)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	tpcs, er := c.Topics()
	if er != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}
	for _, t := range tpcs {
		fmt.Println(t)
	}
	c.Close()
	os.Exit(0)
}

func parseUserOffset() int64 {
	args := flag.Args()

	if len(args) > 0 {
		offset, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			printErrorAndExit(69, "Invalid offset: %s", err)
		}
		return offset
	}
	return 0
}

func main() {
	peers := kafkatoolkit.ParsePeers(*peerList)

	if *listTopics {
		cliListTopics(peers)
	}

	conf := sarama.NewConfig()
	client, err := sarama.NewClient(peers, conf)
	if err != nil {
		printErrorAndExit(69, "Failed to initialize client: %s", err)
	}

	topics, err := client.Topics()
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	found := false
	for _, t := range topics {
		if t == *topic {
			found = true
			break
		}
	}
	if !found {
		printErrorAndExit(70, "topic not found")
	}

	offset := parseUserOffset()

	parts, err := client.Partitions(*topic)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	lastOffset, err := client.GetOffset(*topic, parts[0], sarama.OffsetNewest)
	if err != nil {
		printErrorAndExit(69, "Failed to get offset: %s", err)
	}

	if *head {
		if offset > lastOffset {
			offset = lastOffset
		}
	} else {
		if offset = lastOffset - offset; offset < 0 {
			offset = 0
		}
	}
	log.Printf("Offset: %d", offset)

	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		printErrorAndExit(69, "Failed to get consumer: %s", err)
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
	for _, partition := range parts {

		pc, err := c.ConsumePartition(*topic, partition, offset)
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
		var filter []byte
		if *filterString != "" {
			filter = []byte(*filterString)
		}

		if *printHeader {

			for msg := range messages {
				if kafkatoolkit.Filter(filter, msg.Value) {
					continue
				}

				ms := string(msg.Value)
				if ms[len(ms)-1] != '\n' {
					fmt.Printf("[ %s/%d/%d/%s ] %s\n", *topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				} else {
					fmt.Printf("[ %s/%d/%d/%s ] %s", *topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				}
			}

		} else {
			for msg := range messages {
				if kafkatoolkit.Filter(filter, msg.Value) {
					continue
				}

				ms := string(msg.Value)
				if ms[len(ms)-1] != '\n' {
					fmt.Printf("%s\n", msg.Value)
				} else {
					fmt.Printf("%s", msg.Value)
				}
			}
		}

	}()

	wg.Wait()
	logger.Println("Done consuming:", *topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

/*func parsePartitions() ([]int32, error) {
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
}*/

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
