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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/archive"
	"github.com/nats-io/nats-server/v2/server"
)

type paGatherCmd struct {
	archiveFilePath        string
	noConsumerInfo         bool
	noStreamInfo           bool
	noPrintProgress        bool
	noServerEndpoints      bool
	noAccountEndpoints     bool
	captureServerProfiles  bool
	captureLogWriter       io.Writer
	serverEndpointConfigs  []gatherEndpointCaptureConfig
	accountEndpointConfigs []gatherEndpointCaptureConfig
	serverProfileTypes     []string
}

// gatherEndpointCaptureConfig configuration for capturing and tagging server and account endpoints
type gatherEndpointCaptureConfig struct {
	apiSuffix     string
	responseValue any
	typeTag       *archive.Tag
}

func configurePaGatherCommand(srv *fisk.CmdClause) {
	c := &paGatherCmd{
		serverEndpointConfigs: []gatherEndpointCaptureConfig{
			{
				"VARZ",
				server.Varz{},
				archive.TagServerVars(),
			},
			{
				"CONNZ",
				server.Connz{},
				archive.TagServerConnections(),
			},
			{
				"ROUTEZ",
				server.Routez{},
				archive.TagServerRoutes(),
			},
			{
				"GATEWAYZ",
				server.Gatewayz{},
				archive.TagServerGateways(),
			},
			{
				"LEAFZ",
				server.Leafz{},
				archive.TagServerLeafs(),
			},
			{
				"SUBSZ",
				server.Subsz{},
				archive.TagServerSubs(),
			},
			{
				"JSZ",
				server.JSInfo{},
				archive.TagServerJetStream(),
			},
			{
				"ACCOUNTZ",
				server.Accountz{},
				archive.TagServerAccounts(),
			},
			{
				"HEALTHZ",
				server.HealthStatus{},
				archive.TagServerHealth(),
			},
		},
		accountEndpointConfigs: []gatherEndpointCaptureConfig{
			{
				"CONNZ",
				server.Connz{},
				archive.TagAccountConnections(),
			},
			{
				"LEAFZ",
				server.Leafz{},
				archive.TagAccountLeafs(),
			},
			{
				"SUBSZ",
				server.Subsz{},
				archive.TagAccountSubs(),
			},
			{
				"INFO",
				server.AccountInfo{},
				archive.TagAccountInfo(),
			},
			{
				"JSZ",
				server.JetStreamStats{},
				archive.TagAccountJetStream(),
			},
		},
		serverProfileTypes: []string{
			"goroutine",
			"heap",
			"allocs",
		},
	}

	gather := srv.Command("gather", "capture a variety of data from a deployment into an archive file").Action(c.gather)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.archiveFilePath)
	gather.Flag("no-server-endpoints", "skip capturing of server endpoints").UnNegatableBoolVar(&c.noServerEndpoints)
	gather.Flag("no-account-endpoints", "skip capturing of account endpoints").UnNegatableBoolVar(&c.noAccountEndpoints)
	gather.Flag("no-streams", "skip capturing of individual stream").UnNegatableBoolVar(&c.noStreamInfo)
	gather.Flag("no-consumers", "skip capturing of stream consumers").UnNegatableBoolVar(&c.noConsumerInfo)
	gather.Flag("profiles", "capture profiles for each servers").UnNegatableBoolVar(&c.captureServerProfiles)
	gather.Flag("no-progress", "silence log messages about progress during gathering").UnNegatableBoolVar(&c.noPrintProgress)
}

/*
Overview of gathering strategy:

 1. Query $SYS.REQ.SERVER.PING to discover servers
    ··Foreach answer, save server name and ID

 2. Query $SYS.REQ.SERVER.PING.ACCOUNTZ to discover accounts
    ··Foreach answer, save list of known accounts
    ··Also track the system account name

 3. Foreach known server
    ··Foreach server endpoint
    ····Request $SYS.REQ.SERVER.<Server ID>.<Endpoint>, save the response
    ··Foreach profile type
    ····Request $SYS.REQ.SERVER.<Profile>.PROFILEZ and save the response

 4. Foreach known account
    ··Foreach server endpoint
    ····Foreach response to $SYS.REQ.ACCOUNT.<Account name>.<Endpoint>
    ······Save the response

 5. Foreach known account
    ··Foreach response to $SYS.REQ.SERVER.PING.JSZ filtered by account
    ····Foreach stream in response
    ······Save the stream info
*/
func (c *paGatherCmd) gather(_ *fisk.ParseContext) error {

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	// If no output path is specified, create one
	if c.archiveFilePath == "" {
		c.archiveFilePath = filepath.Join(os.TempDir(), "archive.zip")
	}

	// Gathering prints messages to stdout, but they are also written into this buffer.
	// A copy of the output is included in the archive itself.
	var captureLogBuffer bytes.Buffer
	c.captureLogWriter = &captureLogBuffer

	// Create an archive writer
	aw, err := archive.NewWriter(c.archiveFilePath)
	if err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer func() {
		// Add the output of this command (so far) to the archive as additional log artifact
		if c.captureLogWriter != nil {
			err = aw.AddCaptureLog(bytes.NewReader(captureLogBuffer.Bytes()))
			if err != nil {
				fmt.Printf("Failed to add capture log artifact: %s", err)
			}
			c.captureLogWriter = nil
		}

		err := aw.Close()
		if err != nil {
			fmt.Printf("Failed to close archive: %s", err)
		}
		fmt.Printf("📁 Archive created at: %s\n", c.archiveFilePath)
	}()

	// Collect some runtime metadata and store it as additional artifact
	{
		username := "?"
		currentUser, err := user.Current()
		if err != nil {
			c.logWarning("Failed to capture username: %s", err)
		} else {
			username = fmt.Sprintf("%s (%s)", currentUser.Username, currentUser.Name)
		}

		err = aw.AddCaptureMetadata(&struct {
			Timestamp              time.Time `json:"capture_timestamp"`
			ConnectedServerName    string    `json:"connected_server_name"`
			ConnectedServerVersion string    `json:"connected_server_version"`
			ConnectURL             string    `json:"connect_url"`
			UserName               string    `json:"user_name"`
			CLIVersion             string    `json:"cli_version"`
		}{
			Timestamp:              time.Now(),
			ConnectedServerName:    nc.ConnectedServerName(),
			ConnectedServerVersion: nc.ConnectedServerVersion(),
			ConnectURL:             nc.ConnectedUrl(),
			UserName:               username,
			CLIVersion:             Version,
		})
		if err != nil {
			return fmt.Errorf("failed to save capture metadata: %w", err)
		}
	}

	// CustomServerAPIResponse is a modified version of server.ServerAPIResponse that inhibits deserialization of the
	// `data` field by using `json.RawMessage`. This is necessary because deserializing into a generic object (i.e. map)
	// can cause loss of precision for large numbers.
	type CustomServerAPIResponse struct {
		Server *server.ServerInfo `json:"server"`
		Data   json.RawMessage    `json:"data,omitempty"`
		Error  *server.ApiError   `json:"error,omitempty"`
	}

	/*
		1. Servers discovery
	*/

	// Discover servers by broadcasting a PING and then collecting responses
	var serverInfoMap = make(map[string]*server.ServerInfo)
	c.logProgress("⏳ Broadcasting PING to discover servers... (this may take a few seconds)")
	err = doReqAsync(nil, "$SYS.REQ.SERVER.PING", 0, nc, func(b []byte) {
		var apiResponse server.ServerAPIResponse
		if err = json.Unmarshal(b, &apiResponse); err != nil {
			c.logWarning("Failed to deserialize PING response: %s", err)
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		_, exists := serverInfoMap[apiResponse.Server.ID]
		if exists {
			c.logWarning("Duplicate server %s (%s) response to PING, ignoring", serverId, serverName)
			return
		}

		serverInfoMap[serverId] = apiResponse.Server
		c.logProgress("📣 Discovered server '%s' (%s)", serverName, serverId)
	})
	if err != nil {
		return fmt.Errorf("failed to PING: %w", err)
	}
	c.logProgress("ℹ️ Discovered %d servers", len(serverInfoMap))

	/*
		2. Accounts discovery
	*/

	// Broadcast PING.ACCOUNTZ to discover (active) accounts
	// N.B. Inactive accounts (no connections) cannot be discovered this way
	c.logProgress("⏳ Broadcasting PING to discover accounts... ")
	var accountIdsToServersCountMap = make(map[string]int)
	var systemAccount = ""
	err = doReqAsync(nil, "$SYS.REQ.SERVER.PING.ACCOUNTZ", len(serverInfoMap), nc, func(b []byte) {
		var apiResponse CustomServerAPIResponse
		err = json.Unmarshal(b, &apiResponse)
		if err != nil {
			c.logWarning("Failed to deserialize ACCOUNTZ response, ignoring")
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		// Ignore responses from servers not discovered earlier.
		// We are discarding useful data, but limiting additional collection to a fixed set of nodes
		// simplifies querying and analysis. Could always re-run gather if a new server just joined.
		if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
			c.logWarning("Ignoring ACCOUNTZ response from unknown server: %s", serverName)
			return
		}

		var accountsResponse server.Accountz
		err = json.Unmarshal(apiResponse.Data, &accountsResponse)
		if err != nil {
			c.logWarning("Failed to deserialize PING.ACCOUNTZ response: %s", err)
			return
		}

		c.logProgress("📣 Discovered %d accounts on server %s", len(accountsResponse.Accounts), serverName)

		// Track how many servers known any given account
		for _, accountId := range accountsResponse.Accounts {
			_, accountKnown := accountIdsToServersCountMap[accountId]
			if !accountKnown {
				accountIdsToServersCountMap[accountId] = 0
			}
			accountIdsToServersCountMap[accountId] += 1
		}

		// Track system account (normally, only one for the entire ensemble)
		if accountsResponse.SystemAccount == "" {
			c.logWarning("Server %s system account is not set", serverName)
		} else if systemAccount == "" {
			systemAccount = accountsResponse.SystemAccount
			c.logProgress("ℹ️ Discovered system account name: %s", systemAccount)
		} else if systemAccount != accountsResponse.SystemAccount {
			// This should not happen under normal circumstances!
			c.logWarning("Multiple system accounts detected (%s, %s)", systemAccount, accountsResponse.SystemAccount)
		} else {
			// Known system account matches the one in the response, nothing to do
		}
	})
	if err != nil {
		return fmt.Errorf("failed to PING.ACCOUNTZ: %w", err)
	}
	c.logProgress("ℹ️ Discovered %d accounts over %d servers", len(accountIdsToServersCountMap), len(serverInfoMap))

	/*
		3. Servers endpoints and profiles gathering
	*/

	// Capture configured endpoints for each known server
	if c.noServerEndpoints {
		c.logProgress("Skipping servers endpoints data gathering")
	} else {
		c.logProgress("⏳ Querying %d endpoints on %d known servers...", len(c.serverEndpointConfigs), len(serverInfoMap))
		capturedCount := 0
		for serverId, serverInfo := range serverInfoMap {
			serverName := serverInfo.Name
			for _, endpoint := range c.serverEndpointConfigs {

				subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.%s", serverId, endpoint.apiSuffix)

				endpointResponse := reflect.New(reflect.TypeOf(endpoint.responseValue)).Interface()

				responses, err := doReq(nil, subject, 1, nc)
				if err != nil {
					c.logWarning("Failed to request %s from server %s: %s", endpoint.apiSuffix, serverName, err)
					continue
				}

				if len(responses) != 1 {
					c.logWarning("Unexpected number of responses to %s from server %s: %d", endpoint.apiSuffix, serverName, len(responses))
					continue
				}

				responseBytes := responses[0]

				var apiResponse CustomServerAPIResponse
				if err = json.Unmarshal(responseBytes, &apiResponse); err != nil {
					c.logWarning("Failed to deserialize %s response from server %s: %s", endpoint.apiSuffix, serverName, err)
					continue
				}

				err = json.Unmarshal(apiResponse.Data, endpointResponse)
				if err != nil {
					c.logWarning("Failed to deserialize %s response data from server %s: %s", endpoint.apiSuffix, serverName, err)
					continue
				}

				tags := []*archive.Tag{
					archive.TagServer(serverName), // Source server
					endpoint.typeTag,              // Type of artifact
				}

				if serverInfo.Cluster != "" {
					tags = append(tags, archive.TagCluster(serverInfo.Cluster))
				} else {
					tags = append(tags, archive.TagNoCluster())
				}

				err = aw.Add(endpointResponse, tags...)
				if err != nil {
					return fmt.Errorf("failed to add response to %s from to archive: %w", subject, err)
				}

				capturedCount += 1
			}
		}
		c.logProgress("ℹ️ Captured %d endpoint responses from %d servers", capturedCount, len(serverInfoMap))
	}

	// Capture configured profiles for each server
	if !c.captureServerProfiles {
		c.logProgress("Skipping server profiles gathering")
	} else {
		c.logProgress("⏳ Querying %d profiles endpoints on %d known servers...", len(c.serverProfileTypes), len(serverInfoMap))
		capturedCount := 0
		for serverId, serverInfo := range serverInfoMap {
			serverName := serverInfo.Name
			for _, profileType := range c.serverProfileTypes {

				subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.PROFILEZ", serverId)
				payload := server.ProfilezOptions{
					Name:  profileType,
					Debug: 0,
				}

				responses, err := doReq(payload, subject, 1, nc)
				if err != nil {
					c.logWarning("Failed to request profile %s from server %s: %s", profileType, serverName, err)
					continue
				}

				if len(responses) != 1 {
					c.logWarning("Unexpected number of responses for PROFILEZ from server %s: %d", serverName, len(responses))
					continue
				}

				responseBytes := responses[0]

				var apiResponse struct {
					Server *server.ServerInfo     `json:"server"`
					Data   *server.ProfilezStatus `json:"data,omitempty"`
					Error  *server.ApiError       `json:"error,omitempty"`
				}
				if err = json.Unmarshal(responseBytes, &apiResponse); err != nil {
					c.logWarning("Failed to deserialize PROFILEZ response from server %s: %s", serverName, err)
					continue
				}
				if apiResponse.Error != nil {
					c.logWarning("Failed to retrieve profile %s from server %s: %s", profileType, serverName, apiResponse.Error.Description)
					continue
				}

				profileStatus := apiResponse.Data
				if profileStatus.Error != "" {
					c.logWarning("Failed to retrieve profile %s from server %s: %s", profileType, serverName, profileStatus.Error)
					continue
				}

				tags := []*archive.Tag{
					archive.TagServer(serverName), // Source server
					archive.TagServerProfile(),
					archive.TagProfileName(profileType),
				}

				if serverInfo.Cluster != "" {
					tags = append(tags, archive.TagCluster(serverInfo.Cluster))
				} else {
					tags = append(tags, archive.TagNoCluster())
				}

				profileDataBytes := apiResponse.Data.Profile

				err = aw.AddObject(bytes.NewReader(profileDataBytes), tags...)
				if err != nil {
					return fmt.Errorf("failed to add profile %s from to archive: %w", profileType, err)
				}

				capturedCount += 1

			}
		}
		c.logProgress("ℹ️ Captured %d server profiles from %d servers", capturedCount, len(serverInfoMap))
	}

	/*
		4. Accounts endpoints gathering
	*/

	// Capture configured endpoints for each known account
	if c.noAccountEndpoints {
		c.logProgress("Skipping accounts endpoints data gathering")
	} else {
		type Responder struct {
			ClusterName string
			ServerName  string
		}
		capturedCount := 0
		c.logProgress("⏳ Querying %d endpoints for %d known accounts...", len(c.accountEndpointConfigs), len(accountIdsToServersCountMap))
		for accountId, serversCount := range accountIdsToServersCountMap {
			for _, endpoint := range c.accountEndpointConfigs {
				subject := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.%s", accountId, endpoint.apiSuffix)
				endpointResponses := make(map[Responder]interface{}, serversCount)

				err = doReqAsync(nil, subject, serversCount, nc, func(b []byte) {
					var apiResponse CustomServerAPIResponse
					err := json.Unmarshal(b, &apiResponse)
					if err != nil {
						c.logWarning("Failed to deserialize %s response for account %s: %s", endpoint.apiSuffix, accountId, err)
						return
					}

					serverId := apiResponse.Server.ID

					// Ignore responses from servers not discovered earlier.
					// We are discarding useful data, but limiting additional collection to a fixed set of nodes
					// simplifies querying and analysis. Could always re-run gather if a new server just joined.
					if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
						c.logWarning("Ignoring ACCOUNT.%s response from unknown server ID: %s\n", endpoint.apiSuffix, serverId)
						return
					}

					endpointResponse := reflect.New(reflect.TypeOf(endpoint.responseValue)).Interface()
					err = json.Unmarshal(apiResponse.Data, endpointResponse)
					if err != nil {
						c.logWarning("Failed to deserialize ACCOUNT.%s response for account %s: %s\n", endpoint.apiSuffix, accountId, err)
						return
					}

					responder := Responder{
						ClusterName: apiResponse.Server.Cluster,
						ServerName:  apiResponse.Server.Name,
					}

					if _, isDuplicateResponse := endpointResponses[responder]; isDuplicateResponse {
						c.logWarning("Ignoring duplicate ACCOUNT.%s response from server %s", endpoint.apiSuffix, responder.ServerName)
						return
					}

					endpointResponses[responder] = endpointResponse
				})
				if err != nil {
					c.logWarning("Failed to request %s for account %s: %s", endpoint.apiSuffix, accountId, err)
					continue
				}

				// Store all responses for this account endpoint
				for responder, endpointResponse := range endpointResponses {
					clusterTag := archive.TagNoCluster()
					if responder.ClusterName != "" {
						clusterTag = archive.TagCluster(responder.ClusterName)
					}

					tags := []*archive.Tag{
						archive.TagAccount(accountId),
						archive.TagServer(responder.ServerName),
						clusterTag,
						endpoint.typeTag,
					}

					err = aw.Add(endpointResponse, tags...)
					if err != nil {
						return fmt.Errorf("failed to add response to %s to archive: %w", subject, err)
					}

					capturedCount += 1
				}
			}
		}
		c.logProgress("ℹ️ Captured %d endpoint responses from %d accounts", capturedCount, len(accountIdsToServersCountMap))
	}

	/*
		5. Streams endpoints gathering
	*/

	// Discover list of streams in each account, and capture info for each one
	if c.noStreamInfo {
		c.logProgress("Skipping streams data gathering")
	} else {
		c.logProgress("⏳ Gathering streams data...")
		capturedCount := 0
		for accountId, numServers := range accountIdsToServersCountMap {

			// Skip system account, JetStream is probably not enabled
			if accountId == systemAccount {
				continue
			}

			jszOptions := server.JSzOptions{
				Account:    accountId,
				Streams:    true,
				Consumer:   !c.noConsumerInfo, // Capture consumers, unless configured to skip
				Config:     true,
				RaftGroups: true,
			}

			jsInfoResponses := make(map[string]*server.JSInfo, numServers)
			err = doReqAsync(jszOptions, "$SYS.REQ.SERVER.PING.JSZ", numServers, nc, func(b []byte) {
				var apiResponse CustomServerAPIResponse
				err := json.Unmarshal(b, &apiResponse)
				if err != nil {
					c.logWarning("Failed to deserialize JSZ response for account %s: %s", accountId, err)
					return
				}

				serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

				// Ignore responses from servers not discovered earlier.
				// We are discarding useful data, but limiting additional collection to a fixed set of nodes
				// simplifies querying and analysis. Could always re-run gather if a new server just joined.
				if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
					c.logWarning("Ignoring JSZ response from unknown server: %s", serverName)
					return
				}

				if _, isDuplicateResponse := jsInfoResponses[serverName]; isDuplicateResponse {
					c.logWarning("Ignoring duplicate JSZ response for account %s from server %s", accountId, serverName)
					return
				}

				jsInfoResponse := &server.JSInfo{}
				err = json.Unmarshal(apiResponse.Data, jsInfoResponse)
				if err != nil {
					c.logWarning("Failed to deserialize JSZ response data for account %s: %s", accountId, err)
					return
				}

				if len(jsInfoResponse.AccountDetails) == 0 {
					// No account details in response, don't bother saving this
					//c.logWarning("🐛 Skip JSZ response from %s, no accounts details", serverName)
					return
				} else if len(jsInfoResponse.AccountDetails) > 1 {
					// Server will respond with multiple accounts if the one specified in the request is not found
					// https://github.com/nats-io/nats-server/pull/5229
					//c.logWarning("🐛 Skip JSZ response from %s, account not found", serverName)
					return
				}

				jsInfoResponses[serverName] = jsInfoResponse
			})
			if err != nil {
				c.logWarning("Failed to request JSZ for account %s: %s", accountId, err)
				continue
			}

			streamNamesMap := make(map[string]any)

			// Capture stream info from each known replica
			for serverName, jsInfo := range jsInfoResponses {
				// Cases where len(jsInfo.AccountDetails) != 1 are filtered above
				accountDetail := jsInfo.AccountDetails[0]

				for _, streamInfo := range accountDetail.Streams {
					streamName := streamInfo.Name

					_, streamKnown := streamNamesMap[streamName]
					if !streamKnown {
						c.logProgress("📣 Discovered stream %s in account %s", streamName, accountId)
					}

					tags := []*archive.Tag{
						archive.TagAccount(accountId),
						archive.TagServer(serverName), // Source server
						archive.TagStream(streamName),
						archive.TagStreamInfo(),
					}

					if streamInfo.Cluster != nil {
						tags = append(tags, archive.TagCluster(streamInfo.Cluster.Name))
					} else {
						tags = append(tags, archive.TagNoCluster())
					}

					err = aw.Add(streamInfo, tags...)
					if err != nil {
						return fmt.Errorf("failed to add stream %s info to archive: %w", streamName, err)
					}

					streamNamesMap[streamName] = nil
				}
			}

			c.logProgress("ℹ️ Discovered %d streams in account %s", len(streamNamesMap), accountId)
			capturedCount += len(streamNamesMap)

		}
		c.logProgress("ℹ️ Discovered %d streams in %d accounts", capturedCount, len(accountIdsToServersCountMap))
	}

	return nil
}

// logProgress prints updates to the gathering process. It can be turned off to make capture less verbose.
// Updates are also tee'd to the capture log
func (c *paGatherCmd) logProgress(format string, args ...any) {
	if !c.noPrintProgress {
		fmt.Printf(format+"\n", args...)
	}
	if c.captureLogWriter != nil {
		_, _ = fmt.Fprintf(c.captureLogWriter, format+"\n", args...)
	}
}

// logWarning prints non-fatal errors during the gathering process. Messages are also tee'd to the capture log
func (c *paGatherCmd) logWarning(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
	if c.captureLogWriter != nil {
		_, _ = fmt.Fprintf(c.captureLogWriter, format+"\n", args...)
	}
}
