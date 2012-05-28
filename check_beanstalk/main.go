// Nagios Checks for Beanstalk Job Queue
package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/kr/beanstalk"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	// semi-common params
	tube := flag.String("tube", "default", "tube to use")
	wage := flag.Int("wage", 240, "warn if tube jobs older than this many seconds")
	cage := flag.Int("cage", 500, "critical if tube jobs older than this many seconds")
	port := flag.Int("port", 11300, "beanstalk port number")
	host := flag.String("host", "localhost", "beanstalk server to use")

	// distinct args
	showold := flag.Bool("showold", false, "Show tubes with old messages")
	ping := flag.Bool("ping", false, "Ping tube (put and get same item")

	flag.Parse()

	// make sure only one distinct arg was selected
	argcount := 0
	if *showold {
		argcount++
	}

	if *ping {
		argcount++
	}

	if argcount != 1 {
		fmt.Println("Error: wrong number of runmodes selected.")
		fmt.Println("Select one of showold, ping")
		flag.PrintDefaults()
		os.Exit(64)
	}

	hostname := strings.Join([]string{*host, strconv.Itoa(*port)}, ":")

	// dispatch to command function
	if *showold {

	} else if *ping {
		Ping(hostname, *tube)
	}

	_ = tube
	_ = wage
	_ = cage

	os.Exit(70)
}

func connect(hostname string) *beanstalk.Conn {
	c, err := beanstalk.Dial("tcp", hostname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to %s\n", hostname)
		os.Exit(2)
	}

	return c
}

// show tubes
func List(hostname string) {
	c := connect(hostname)
	tubes, err := c.ListTubes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not list tubes: %s\n", err)
	}

	for i := range tubes {
		fmt.Println(tubes[i])
	}

	os.Exit(0)
}

// try putting and getting a message from the tube
func Ping(hostname string, tube string) {
	c := connect(hostname)
	c.Tube = beanstalk.Tube{c, tube}
	body := []byte("check_beanstalk_ping")

	putid, err := c.Put(body, 1, 0, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Put failed: %s\n", err)
		os.Exit(2)
	}

	p, err := c.Peek(putid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Peek failed: %s\n", err)
		os.Exit(2)
	}

	c.Delete(putid)

	if bytes.Equal(p, body) != true {
		fmt.Fprintf(os.Stderr, "Unknown jobs in test tube\n")
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr, "PUT->Peek OK\n")
	os.Exit(0)
}
