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

	"github.com/elastic/go-elasticsearch/v8"
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
	fs.String("elastic_namespace", "prod", "Elasticsearch data stream name")
	fs.Parse(os.Args[1:])

	if fs.Lookup("elastic_token").Value.String() == "" {
		return nil, errors.New("elastic_token is required")
	}
	if fs.Lookup("elastic_url").Value.String() == "" {
		return nil, errors.New("elastic_url is required")
	}
	return fs, nil
}

type X509 struct {
	AltNames           []interface{}          `json:"alternative_names"`
	Issuer             map[string]interface{} `json:"issuer"`
	Subject            map[string]interface{} `json:"subject"`
	NotBefore          int64                  `json:"not_before"`
	NotAfter           int64                  `json:"not_after"`
	SerialNumber       string                 `json:"serial_number"`
	SignatureAlgorithm string                 `json:"signature_algorithm"`
}

type CertData struct {
	Timestamp string `json:"@timestamp"`
	X509      X509   `json:"x509"`
}

func populateCertData(data map[string]interface{}) (CertData, error) {
	leafCert, ok := data["data"].(map[string]interface{})["leaf_cert"]
	if !ok {
		return CertData{}, errors.New("leaf_cert not found")
	}

	certData := CertData{
		Timestamp: time.Now().Format(time.RFC3339),
		X509: X509{
			AltNames:           leafCert.(map[string]interface{})["all_domains"].([]interface{}),
			Issuer:             leafCert.(map[string]interface{})["issuer"].(map[string]interface{}),
			Subject:            leafCert.(map[string]interface{})["subject"].(map[string]interface{}),
			NotBefore:          int64(leafCert.(map[string]interface{})["not_before"].(float64)),
			NotAfter:           int64(leafCert.(map[string]interface{})["not_after"].(float64)),
			SerialNumber:       leafCert.(map[string]interface{})["serial_number"].(string),
			SignatureAlgorithm: leafCert.(map[string]interface{})["signature_algorithm"].(string),
		},
	}
	return certData, nil
}

func createIndexTemplate(es *elasticsearch.Client, templateName string) error {
	var templateDefinition = `
	{
	  "index_patterns": ["logs-certstreamer-*"],
	  "data_stream": {},
	  "template": {
		"mappings": {
		  "properties": {
			"@timestamp": { "type": "date" },
			"x509": {
			  "properties": {
				"alternative_names": { "type": "keyword" },
				"issuer": { "type": "nested" },
				"subject": { "type": "nested" },
				"not_before": { "type": "date" },
				"not_after": { "type": "date" },
				"serial_number": { "type": "keyword" },
				"signature_algorithm": { "type": "keyword" }
			  }
			}
		  }
		}
	  }
	}
	`
	_, err := es.Indices.PutIndexTemplate(templateName, strings.NewReader(string(templateDefinition)))
	if err != nil {
		return fmt.Errorf("error creating index template: %w", err)
	}
	return nil
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

	// Create Elastic Client
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{fs.Lookup("elastic_url").Value.String()},
		APIKey:    fs.Lookup("elastic_token").Value.String(),
	})
	if err != nil {
		log.Fatal(err)
	}
	createIndexTemplate(es, "logs-certstreamer-"+fs.Lookup("elastic_namespace").Value.String())

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
					event, err := populateCertData(data)
					if err != nil {
						log.Printf("Error populating cert data: %s\n", err)
						continue
					}
					document, err := json.Marshal(event)
					if err != nil {
						log.Printf("Error marshalling JSON: %s\n", err)
						continue
					}
					_, err = es.Index("logs-certstreamer-"+fs.Lookup("elastic_namespace").Value.String(), strings.NewReader(string(document)))
					if err != nil {
						log.Printf("Error indexing document: %s\n", err)
					}

				}
			}
		}
	}
}
