// Manipulate Beanstalk Job Queue
package main

import (
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
	port := flag.Int("port", 11300, "beanstalk port number")
	text := flag.String("text", "", "message to put in tube")
	host := flag.String("host", "localhost", "beanstalk server to use")
	jobs := flag.Int("jobs", 1, "number of jobs - used with kick")
	pri := flag.Int("pri", 0, "priority level")
	ttr := flag.Int("ttr", 120, "time to reserve in seconds")
	delay := flag.Duration("delay", 0, "delay time in seconds")
	age := flag.Duration("age", 60, "tube age in seconds")

	// distinct args
	list := flag.Bool("list", false, "List tubes")
	clear := flag.Bool("clear", false, "Clear tube")
	peek := flag.Bool("peek", false, "Peek next item in tube")
	put := flag.Bool("put", false, "Put text in tube")
	kick := flag.Bool("kick", false, "Kick job")
	bury := flag.Bool("bury", false, "Bury job")
	listold := flag.Bool("listold", false, "List tubes older than age")
	stats := flag.Bool("stats", false, "Show tube stats")

	flag.Parse()

	prio := uint32(*pri)

	// make sure only one distinct arg was selected
	argcount := 0

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

	if *kick {
		argcount++
	}

	if *bury {
		argcount++
	}

	if *listold {
		argcount++
	}

	if *stats {
		argcount++
	}

	if argcount != 1 {
		fmt.Println("Error: wrong number of runmodes selected.")
		fmt.Println("Select one of list, clear, peek, put, kick, bury, status")
		flag.PrintDefaults()
		os.Exit(64)
	}

	hostname := strings.Join([]string{*host, strconv.Itoa(*port)}, ":")
	delaysec := time.Duration(*delay) * time.Second
	ttrsec := time.Duration(*ttr) * time.Second

	// dispatch to command function
	if *list {
		List(hostname)
	} else if *clear {
		Clear(hostname, *tube)
	} else if *peek {
		PeekReady(hostname, *tube)
	} else if *put {
		Put(hostname, *tube, *text, ttrsec, uint32(*pri), delaysec)
	} else if *kick {
		Kick(hostname, *tube, *jobs)
	} else if *bury {
		Bury(hostname, *tube, *id, prio)
	} else if *listold {
		ListOld(hostname, time.Duration(*age)*time.Second)
	} else if *stats {
		Stats(hostname, *tube)
	}

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
func ListOld(hostname string, age time.Duration) {
	c := connect(hostname)
	tubes, err := c.ListTubes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not list tubes: %s\n", err)
	}

	for i := range tubes {
		c.Tube = beanstalk.Tube{c, tubes[i]}
		stats, err := c.Stats()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not stat tube %s\n", tubes[i])
			os.Exit(2)
		}
		fmt.Fprintf(os.Stdout, "Status for tube %s\n%+v\n", tubes[i], stats)
	}
}

// Show tube stats
func Stats(hostname string, tube string) {
	c := connect(hostname)
	c.Tube = beanstalk.Tube{c, tube}
	stats, err := c.Stats()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not stat tube %s because: %s\n", tube, err)
	}
	fmt.Fprintf(os.Stdout, "Stats for %s\n", tube)

	for i := range stats {
		fmt.Fprintf(os.Stdout, "%s: %s\n", i, stats[i])
	}
	os.Exit(0)
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
func Clear(hostname string, tube string) {

}

// put something in tube
func Put(hostname string, tube string, message string, ttr time.Duration, pri uint32, delay time.Duration) {
	c := connect(hostname)
	c.Tube = beanstalk.Tube{c, tube}
	id, err := c.Put([]byte(message), pri, delay, ttr)
	if err != nil {
		fmt.Fprintf(os.Stdout, "Could not put to tube %s: %s\n", tube, err)
		os.Exit(2)
	}

	fmt.Println(id)
	os.Exit(0)
}
