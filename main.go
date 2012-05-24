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
	id := flag.Uint64("id", 0, "Message ID")
	tube := flag.String("tube", "default", "tube to use")
	age := flag.Int("age", 60, "age in seconds")
	port := flag.Int("port", 11300, "beanstalk port number")
	text := flag.String("text", "", "message to put in tube")
	host := flag.String("host", "localhost", "beanstalk server to use")
	jobs := flag.Int("jobs", 1, "number of jobs - used with kick")
	pri := flag.Uint64("pri", 0, "priority level")

	// distinct args
	showold := flag.Bool("showold", false, "Show tubes with old messages")
	list := flag.Bool("list", false, "List tubes")
	clear := flag.Bool("clear", false, "Clear tube")
	peek := flag.Bool("peek", false, "Peek next item in tube")
	put := flag.Bool("put", false, "Put text in tube")
	ping := flag.Bool("ping", false, "Ping tube (put and get same item")
	kick := flag.Bool("kick", false, "Kick job")
	bury := flag.Bool("bury", false, "Bury job")

	flag.Parse()

	prio := uint32(*pri)

	// make sure only one distinct arg was selected
	argcount := 0
	if *showold {
		argcount++
	}

	if *list {
		argcount++
	}

	if *clear {
		argcount++
	}

	if *peek {
		argcount++
	}

	if *put {
		argcount++
	}

	if *ping {
		argcount++
	}

	if *kick {
		argcount++
	}

	if *bury {
		argcount++
	}

	if argcount != 1 {
		fmt.Println("Error: wrong number of runmodes selected.")
		fmt.Println("Select one of showold, list, clear, peek, put, ping, kick, bury")
		flag.PrintDefaults()
		os.Exit(64)
	}

	hostname := strings.Join([]string{*host, strconv.Itoa(*port)}, ":")

	// dispatch to command function
	if *showold {

	} else if *list {
		List(hostname)
	} else if *clear {

	} else if *peek {
		PeekReady(hostname, *tube)
	} else if *put {

	} else if *ping {
		Ping(hostname, *tube)
	} else if *kick {
		Kick(hostname, *tube, *jobs)
	} else if *bury {
		Bury(hostname, *tube, *id, prio)
	}

	_ = id
	_ = tube
	_ = age
	_ = text

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

// kick jobs in tube
func Kick(hostname string, tube string, jobs int) {
	c := connect(hostname)
	c.Tube = beanstalk.Tube{c, tube}
	n, err := c.Kick(jobs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error kicking jobs: %s\n", err)
	}

	fmt.Fprintf(os.Stdout, "Kicked %d jobs from tube %s\n", n, tube)
	os.Exit(0)
}

// bury job in tube
func Bury(hostname string, tube string, job uint64, priority uint32) {
	c := connect(hostname)
	c.Tube = beanstalk.Tube{c, tube}
	err := c.Bury(job, priority)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not bury job %d in tube %s\n", job, tube)
		os.Exit(2)
	}
	os.Exit(0)
}

// show tubes older than X

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

// Peek next in tube
func PeekReady(hostname string, tube string) {
	c := connect(hostname)
	c.Tube = beanstalk.Tube{c, tube}
	id, body, err := c.PeekReady()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not peek next in tube %s:\n", tube, err)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stdout, "ID: %d\nBody: %s\n", id, body)
	os.Exit(0)
}

// delete everthing in tube

// peek tube

// put something in tube

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
