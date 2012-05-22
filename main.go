package main

import (
  "github.com/kr/beanstalk"
  "os"
  "fmt"
  "time"
  "bytes"
)

func main() {
  if len(os.Args) != 3 {
    fmt.Fprintf(os.Stderr, "Usage: %s HOSTNAME TUBE\n", os.Args[0])
    os.Exit(1)
  }

  hostname := os.Args[1]
  tube := os.Args[2]

  c, err := beanstalk.Dial("tcp", hostname)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not connect to %s\n", hostname)
    os.Exit(2)
  }

  c.Tube = beanstalk.Tube{c, tube}
  body := []byte("check_beanstalk_ping")

  putid, err := c.Put(body, 1, 0, 5 * time.Second)
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
