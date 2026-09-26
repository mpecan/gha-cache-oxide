// e2eproxy starts forgejo-runner's real cache proxy in front of an
// external cache server and registers runs, exactly as the runner does
// for a job with cache.external_server set.
//
// Usage: e2eproxy <external-server-url> <secret> <port> <repo> [writeIsolationKey...]
// Prints one line per run: "<writeIsolationKey>=<ACTIONS_CACHE_URL>".
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"code.forgejo.org/forgejo/runner/v13/act/cacheproxy"
)

func main() {
	target, secret, repo := os.Args[1], os.Args[2], os.Args[4]
	port, _ := strconv.Atoi(os.Args[3])
	h, err := cacheproxy.StartHandler(target, "127.0.0.1", uint16(port), "", secret, nil, nil)
	if err != nil {
		panic(err)
	}
	keys := append([]string{""}, os.Args[5:]...)
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	for _, wik := range keys {
		runID, err := h.AddRun(h.CreateRunData(repo, "7", ts, wik))
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s=%s/%s/\n", wik, h.ExternalURL(), runID)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
