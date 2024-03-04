package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/namsral/flag"
)

func connectToCertStream() (*websocket.Conn, error) {
	dialer := &websocket.Dialer{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	conn, _, err := dialer.DialContext(ctx, "wss://certstream.calidog.io/", nil)

	if err != nil {
		return nil, fmt.Errorf("error dialing CertStream: %w", err)
	}
	return conn, nil
}

// reconnect attempts to reconnect to CertStream with exponential backoff.
func reconnect(ctx context.Context) (*websocket.Conn, error) {
	backoff := time.Second // Initial backoff duration
	for {
		conn, err := connectToCertStream()
		if err == nil {
			log.Println("Connection established")
			return conn, nil // Successful connection established
		}

		select {
		case <-ctx.Done():
			return nil, errors.New("context canceled")
		case <-time.After(backoff):
			backoff *= 2 // Double the backoff duration for next attempt
			log.Printf("Connection attempt failed, retrying in %s...\n", backoff)
		}
	}
}

func filterDomains(data map[string]interface{}, domains []string) bool {
	certData, ok := data["data"]
	if !ok {
		return false
	}

	leafCert, ok := certData.(map[string]interface{})["leaf_cert"]
	if !ok {
		return false
	}

	allDomains, ok := leafCert.(map[string]interface{})["all_domains"]
	if !ok {
		return false
	}

	domainList, ok := allDomains.([]interface{})
	if !ok {
		return false
	}

	for _, domainInterface := range domainList {
		domain, ok := domainInterface.(string)
		if !ok {
			continue
		}

		for _, matchDomain := range domains {
			if strings.HasSuffix(domain, matchDomain) {
				return true
			}
		}
	}
	return false
}

func populateFlags() (*flag.FlagSet, error) {
	fs := flag.NewFlagSetWithEnvPrefix("test", "CERTSTREAMER", 0)
	fs.String("domain_filter", "", "comma seperated domain to match against")
	fs.String("elastic_token", "", "Elasticsearch token")
	fs.String("elastic_url", "", "Elasticsearch URL")
	fs.Parse(os.Args[1:])

	if fs.Lookup("elastic_token").Value.String() == "" {
		return nil, errors.New("elastic_token is required")
	}
	if fs.Lookup("elastic_url").Value.String() == "" {
		return nil, errors.New("elastic_url is required")
	}
	return fs, nil
}

func publishToElasticsearch(ctx context.Context, es *elastic.Client, index string, document interface{}) error {

	req := elastic.IndexRequest{
		Index:    index,
		Body:     document,
		Pipeline: "certstreamer",
	}
}

func main() {
	// Setup flags
	fs, err := populateFlags()
	if err != nil {
		log.Fatal(err)
	}

	// Create domain list to filter for
	domains := strings.Split(fs.Lookup("domain_filter").Value.String(), ",")

	var conn *websocket.Conn
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		// Attempt to connect or reconnect with backoff
		conn, err = reconnect(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Error reading message: %s\n", err)
				break
			}

			if messageType == websocket.TextMessage {
				var data map[string]interface{}
				if err := json.Unmarshal(message, &data); err != nil {
					log.Printf("Error unmarshalling JSON: %s\n", err)
					continue
				}

				if filterDomains(data, domains) {
					jsonData, _ := json.MarshalIndent(data, "", "  ")
					if err != nil {
						log.Printf("Error marshalling JSON: %s\n", err)
						continue
					}
					log.Printf("Match: %s\n", jsonData)
				}
			}
		}
	}
}
