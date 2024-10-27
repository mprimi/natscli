// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"golang.org/x/exp/maps"
)

type observeOpenObserveCmd struct {
	username       string
	password       string
	endpoint       *url.URL
	sampleInterval time.Duration
}

type OpenObserveSampleType string

const OOGauge OpenObserveSampleType = "gauge"

func configureOpenObserveCommand(srv *fisk.CmdClause) {
	c := &observeOpenObserveCmd{}

	// Example: nats -s  nats://127.0.0.1:17001,nats://127.0.0.1:17002,nats://127.0.0.1:17003,nats://127.0.0.1:17004,nats://127.0.0.1:17005 --user sys --password sys observe openobserve -u me@example.com -p 1234 -a http://localhost:5080/api/default/ingest/metrics/_json
	gather := srv.Command("openobserve", "publish metrics to OpenObserve").Action(c.openObserve)
	gather.Flag("oo-user", "OpenObserve username").Short('u').Required().StringVar(&c.username)
	gather.Flag("oo-password", "OpenObserve password").Short('p').Required().StringVar(&c.password)
	gather.Flag("oo-url", "OpenObserve endpoint JSON Metrics API endpoint URL").Short('a').Required().URLVar(&c.endpoint)
	gather.Flag("interval", "Sampling interval").Short('i').Default("3s").DurationVar(&c.sampleInterval)
}

const openObserveMetricsPrefix = "NATS_"

type openObserveServerSample struct {
	Name       string                `json:"__name__"`
	Type       OpenObserveSampleType `json:"__type__"`
	Timestamp  int64                 `json:"_timestamp"`
	Value      float64               `json:"value"`
	ServerName string                `json:"server_name"`
	ServerId   string                `json:"server_id"`
	Hostname   string                `json:"hostname"`
	Version    string                `json:"version"`
	Cluster    string                `json:"cluster"`
}

func (c *observeOpenObserveCmd) createServerSample(z server.Varz, metricName string, value float64, ooType OpenObserveSampleType) openObserveServerSample {
	return openObserveServerSample{
		Name:       openObserveMetricsPrefix + metricName,
		ServerName: z.Name,
		ServerId:   z.ID,
		Hostname:   z.Host,
		Version:    z.Version,
		Cluster:    z.Cluster.Name,
		Timestamp:  z.Now.UnixMilli(),
		Type:       ooType,
		Value:      value,
	}
}

type openObserveAccountSample struct {
	Name      string                `json:"__name__"`
	Type      OpenObserveSampleType `json:"__type__"`
	Timestamp int64                 `json:"_timestamp"`
	Value     float64               `json:"value"`
	Account   string                `json:"account"`
	System    string                `json:"system"`
	JetStream string                `json:"jetstream"`
}

func (c *observeOpenObserveCmd) createAccountSample(i server.AccountInfo, metricName string, value float64, ooType OpenObserveSampleType) openObserveAccountSample {
	system, jetstream := "false", "false"
	if i.IsSystem {
		system = "true"
	}
	if i.JetStream {
		jetstream = "true"
	}
	return openObserveAccountSample{
		Name:      openObserveMetricsPrefix + "ACCT_" + metricName,
		Account:   i.AccountName,
		System:    system,
		JetStream: jetstream,
		Timestamp: time.Now().UnixMilli(),
		Type:      ooType,
		Value:     value,
	}
}

func (c *observeOpenObserveCmd) openObserve(_ *fisk.ParseContext) error {

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	// Discover servers, create map with servers info
	serverInfoMap, err := c.discoverServers(nc)
	if err != nil {
		return fmt.Errorf("failed to discover servers: %w", err)
	}

	// Discover accounts
	accountNames, err := c.discoverAccounts(nc)
	if err != nil {
		return fmt.Errorf("failed to discover accounts: %w", err)
	}

	ticker := time.NewTicker(c.sampleInterval)
	ctx := context.Background()
sampleLoop:
	for {
		select {
		case <-ctx.Done():
			break sampleLoop

		case <-ticker.C:

			serverSamples, err := c.captureServerMetrics(nc, serverInfoMap)
			if err != nil {
				c.log("failed to capture server metrics: %s", err)
			}

			err = c.publishMetricSamples(serverSamples)
			if err != nil {
				c.log("failed to publish server metrics: %s", err)
			}

			accountSamples, err := c.captureAccountMetrics(nc, accountNames)
			if err != nil {
				c.log("failed to capture account metrics: %s", err)
			}

			err = c.publishMetricSamples(accountSamples)
			if err != nil {
				c.log("failed to publish account metrics: %s", err)
			}

		}
	}
	return nil
}

func (c *observeOpenObserveCmd) discoverServers(nc *nats.Conn) (map[string]*server.ServerInfo, error) {
	var serverInfoMap = make(map[string]*server.ServerInfo)

	c.log("Broadcasting PING to discover servers... (this may take a few seconds)")
	err := doReqAsync(nil, "$SYS.REQ.SERVER.PING", doReqAsyncWaitFullTimeoutInterval, nc, func(b []byte) {
		var apiResponse server.ServerAPIResponse
		if err := json.Unmarshal(b, &apiResponse); err != nil {
			c.log("Failed to deserialize PING response: %s", err)
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		_, exists := serverInfoMap[apiResponse.Server.ID]
		if exists {
			c.log("Duplicate server %s (%s) response to PING, ignoring", serverId, serverName)
			return
		}

		serverInfoMap[serverId] = apiResponse.Server
		c.log("Discovered server '%s' (%s)", serverName, serverId)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to gather server responses: %w", err)
	}
	c.log("Discovered %d servers", len(serverInfoMap))
	return serverInfoMap, nil
}

func (c *observeOpenObserveCmd) discoverAccounts(nc *nats.Conn) ([]string, error) {
	var accountNamesSet = make(map[string]interface{})

	c.log("Broadcasting PING to discover accounts... (this may take a few seconds)")
	err := doReqAsync(nil, "$SYS.REQ.SERVER.PING.ACCOUNTZ", doReqAsyncWaitFullTimeoutInterval, nc, func(b []byte) {
		//var apiResponse server.ServerAPIResponse
		var apiResponse serverAPIResponseNoData
		if err := json.Unmarshal(b, &apiResponse); err != nil {
			c.log("Failed to deserialize accounts PING response: %s", err)
			return
		}

		var accountsResponse server.Accountz
		if err := json.Unmarshal(apiResponse.Data, &accountsResponse); err != nil {
			c.log("Failed to deserialize accounts response body: %s", err)
			return
		}

		for _, accountName := range accountsResponse.Accounts {
			accountNamesSet[accountName] = nil
		}

		c.log("Discovered %d accounts via server %s", len(accountsResponse.Accounts), apiResponse.Server.Name)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to gather account responses: %w", err)
	}
	c.log("Discovered %d accounts", len(accountNamesSet))
	return maps.Keys(accountNamesSet), nil
}

func (c *observeOpenObserveCmd) captureServerMetrics(nc *nats.Conn, serverInfoMap map[string]*server.ServerInfo) ([]openObserveServerSample, error) {
	const endpoint = "VARZ"

	varzMetrics := map[string]func(varz server.Varz) float64{
		"CPU":       func(varz server.Varz) float64 { return varz.CPU },
		"MEM":       func(varz server.Varz) float64 { return float64(varz.Mem) },
		"CONNS":     func(varz server.Varz) float64 { return float64(varz.Connections) },
		"SUBS":      func(varz server.Varz) float64 { return float64(varz.Subscriptions) },
		"ROUTES":    func(varz server.Varz) float64 { return float64(varz.Routes) },
		"SLOW_CONS": func(varz server.Varz) float64 { return float64(varz.SlowConsumers) },
		"IN_BYTES":  func(varz server.Varz) float64 { return float64(varz.InBytes) },
		"OUT_BYTES": func(varz server.Varz) float64 { return float64(varz.OutBytes) },
		"IN_MSGS":   func(varz server.Varz) float64 { return float64(varz.InMsgs) },
		"OUT_MSGS":  func(varz server.Varz) float64 { return float64(varz.OutMsgs) },
	}

	samples := make([]openObserveServerSample, 0, len(serverInfoMap)*(len(varzMetrics)))

	for serverId, serverInfo := range serverInfoMap {
		serverName := serverInfo.Name

		subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.%s", serverId, endpoint)

		responses, err := doReq(nil, subject, 1, nc)
		if err != nil {
			c.log("Failed to request %s from server %s: %s", endpoint, serverName, err)
			continue
		}

		if len(responses) != 1 {
			c.log("Unexpected number of responses to %s from server %s: %d", endpoint, serverName, len(responses))
			continue
		}

		responseBytes := responses[0]

		var apiResponse serverAPIResponseNoData
		if err = json.Unmarshal(responseBytes, &apiResponse); err != nil {
			c.log("Failed to deserialize %s response from server %s: %s", endpoint, serverName, err)
			continue
		}

		var varZ = server.Varz{}

		err = json.Unmarshal(apiResponse.Data, &varZ)
		if err != nil {
			c.log("Failed to deserialize %s response data from server %s: %s", endpoint, serverName, err)
			continue
		}

		// Append one metrics sample for each extractor
		for name, f := range varzMetrics {
			samples = append(samples, c.createServerSample(varZ, name, f(varZ), OOGauge))
		}
	}

	c.log("Captured %d metric samples from %d servers", len(samples), len(serverInfoMap))
	return samples, nil
}

func (c *observeOpenObserveCmd) captureAccountMetrics(nc *nats.Conn, accountNames []string) ([]openObserveAccountSample, error) {
	const endpoint = "INFO"

	accountInfoMetrics := map[string]func(info server.AccountInfo) float64{
		"EXPORTS":  func(info server.AccountInfo) float64 { return float64(len(info.Exports)) },
		"IMPORTS":  func(info server.AccountInfo) float64 { return float64(len(info.Imports)) },
		"MAPPINGS": func(info server.AccountInfo) float64 { return float64(len(info.Mappings)) },
		"SUBS":     func(info server.AccountInfo) float64 { return float64(info.SubCnt) },
		"LEAFS":    func(info server.AccountInfo) float64 { return float64(info.LeafCnt) },
	}

	samples := make([]openObserveAccountSample, 0, len(accountNames)*len(accountInfoMetrics))

	for _, accountName := range accountNames {
		subject := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.%s", accountName, endpoint)
		var accountInfo server.AccountInfo
		var responseReceived bool

		err := doReqAsync(nil, subject, 1, nc, func(b []byte) {
			var apiResponse serverAPIResponseNoData
			err := json.Unmarshal(b, &apiResponse)
			if err != nil {
				c.log("Failed to deserialize %s response for account %s: %s", endpoint, accountName, err)
				return
			}

			err = json.Unmarshal(apiResponse.Data, &accountInfo)
			if err != nil {
				c.log("Failed to deserialize %s response for account %s: %s", endpoint, accountName, err)
				return
			}
			responseReceived = true
		})
		if err != nil {
			c.log("Failed to request %s for account %s: %s", endpoint, accountName, err)
			continue
		} else if !responseReceived {
			c.log("No response for %s for account %s: %s", endpoint, accountName, err)
			continue
		}

		// Append one metrics sample for each extractor
		for name, f := range accountInfoMetrics {
			samples = append(samples, c.createAccountSample(accountInfo, name, f(accountInfo), OOGauge))
		}
	}

	c.log("Captured %d metric samples from %d accounts", len(samples), len(accountNames))
	return samples, nil
}

func (c *observeOpenObserveCmd) publishMetricSamples(samples any) error {
	// Buffer for JSON serialized samples
	var b bytes.Buffer

	// Encode samples into buffer
	encoder := json.NewEncoder(&b)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(samples)
	if err != nil {
		return fmt.Errorf("failed to marshal samples: %w", err)
	}

	// Prepare request
	r := bytes.NewReader(b.Bytes())
	req, err := http.NewRequest("POST", c.endpoint.String(), r)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "NATS CLI "+nats.Version)

	// Execute request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w [%v]", err, resp)
	}
	defer resp.Body.Close()

	// TODO check response body for missing samples
	//log.Println(resp.StatusCode)
	//body, err := io.ReadAll(resp.Body)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Println(string(body))
	return nil
}

func (c *observeOpenObserveCmd) log(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}
