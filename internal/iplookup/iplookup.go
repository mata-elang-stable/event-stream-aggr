// Package iplookup provides a client to interact with a lookup API service.
package iplookup

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
)

// Client holds the configuration for the API client
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new API client with the given base URL
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

//TODO: Implement the LookupIPs method using a worker pool
// LookupIPs performs an IP lookup for a list of IPs and respond with array of JSON objects
func (c *Client) LookupIPs(ips []string) error {
	// Make sure there are no duplicate IPs to avoid unnecessary requests
	uniqueIPs := make(map[string]struct{})
	for _, ip := range ips {
		uniqueIPs[ip] = struct{}{}
	}
	
	// Perform lookup for each IP
	for ip := range uniqueIPs {
		if err := c.LookupIP(ip); err != nil {
			return fmt.Errorf("failed to lookup IP %s: %w", ip, err)
		}
	}

	return nil
}


//TODO: Implement the LookupIP method
// LookupIP performs an IP lookup and prints the raw JSON response
func (c *Client) LookupIP(ip string) error {
	// Parse base URL
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return fmt.Errorf("invalid base URL: %w", err)
	}

	// Construct full path
	base.Path = path.Join(base.Path, "/lookup")

	// Set query parameters
	params := url.Values{}
	params.Add("ip", ip)
	base.RawQuery = params.Encode()

	// Create request
	req, err := http.NewRequest("GET", base.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read and print response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	fmt.Println(string(body))
	return nil
}