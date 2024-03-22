package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/archive"
	"github.com/nats-io/nats-server/v2/server"
)

type PaGatherCmd struct {
	archiveFilePath    string
	noConsumerInfo     bool
	noStreamInfo       bool
	noPrintProgress    bool
	noServerEndpoints  bool
	noAccountEndpoints bool
}

type Endpoint struct {
	apiSuffix      string
	expectedStruct any
	typeTag        *archive.Tag
}

// CustomServerAPIResponse is a modified version of server.ServerAPIResponse that inhibits deserialization of the
// `data` field by making it a `json.RawMessage`.
// This is necessary because deserializing into a generic map can cause loss of precision for large numbers.
type CustomServerAPIResponse struct {
	Server *server.ServerInfo `json:"server"`
	Data   json.RawMessage    `json:"data,omitempty"`
	Error  *server.ApiError   `json:"error,omitempty"`
}

var serverEndpoints = []Endpoint{
	{
		"VARZ",
		server.Varz{},
		archive.TagServerVars(),
	},
	{
		"CONNZ",
		server.Connz{},
		archive.TagConnections(),
	},
	{
		"ROUTEZ",
		server.Routez{},
		archive.TagRoutes(),
	},
	{
		"GATEWAYZ",
		server.Gatewayz{},
		archive.TagGateways(),
	},
	{
		"LEAFZ",
		server.Leafz{},
		archive.TagLeafs(),
	},
	{
		"SUBSZ",
		server.Subsz{},
		archive.TagSubs(),
	},
	{
		"JSZ",
		server.JSInfo{},
		archive.TagJetStream(),
	},
	{
		"ACCOUNTZ",
		server.Accountz{},
		archive.TagAccounts(),
	},
	{
		"HEALTHZ",
		server.HealthStatus{},
		archive.TagHealth(),
	},
}

var accountEndpoints = []Endpoint{
	{
		"CONNZ",
		server.Connz{},
		archive.TagConnections(),
	},
	{
		"LEAFZ",
		server.Leafz{},
		archive.TagLeafs(),
	},
	{
		"SUBSZ",
		server.Subsz{},
		archive.TagSubs(),
	},
	{
		"INFO",
		server.AccountInfo{},
		archive.TagAccounts(),
	},
	{
		"JSZ",
		server.JetStreamStats{},
		archive.TagJetStream(),
	},
}

func configurePaGatherCommand(srv *fisk.CmdClause) {
	c := &PaGatherCmd{}

	gather := srv.Command("gather", "capture a variety of data from a deployment into an archive file").Action(c.gather)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.archiveFilePath)
	gather.Flag("no-server-endpoints", "skip capturing of server endpoints").UnNegatableBoolVar(&c.noServerEndpoints)
	gather.Flag("no-account-endpoints", "skip capturing of account endpoints").UnNegatableBoolVar(&c.noAccountEndpoints)
	gather.Flag("no-streams", "skip capturing of stream details").UnNegatableBoolVar(&c.noStreamInfo)
	gather.Flag("no-consumers", "skip capturing of stream consumer details").UnNegatableBoolVar(&c.noConsumerInfo)
	gather.Flag("no-progress", "silence log messages detailing progress during gathering").UnNegatableBoolVar(&c.noPrintProgress)
}

// gather Overview of gathering strategy:
//  1. Query $SYS.REQ.SERVER.PING to discover servers
//     ··Foreach answer, save server name and ID
//  2. Query $SYS.REQ.SERVER.PING.ACCOUNTZ to discover accounts
//     ··Foreach answer, save list of known accounts
//     ··Also track the system account name
//  3. Foreach known server
//     ··Foreach server endpoint
//     ··Request $SYS.REQ.SERVER.<Server ID>.<Endpoint>, save the response
//  4. Foreach known account
//     ··Foreach server endpoint
//     ····Foreach response to $SYS.REQ.ACCOUNT.<Account name>.<Endpoint>
//     ······Save the response
//  5. Foreach known account
//     ··Foreach response to $SYS.REQ.SERVER.PING.JSZ filtered by account
//     ····Foreach stream in response
//     ······Save the stream details
func (c *PaGatherCmd) gather(_ *fisk.ParseContext) error {
	// nats connection
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	// If no output path is provided, create one in os.Temp
	if c.archiveFilePath == "" {
		c.archiveFilePath = filepath.Join(os.TempDir(), "archive.zip")
	}

	// Create an archive writer
	aw, err := archive.NewWriter(c.archiveFilePath)
	if err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer func() {
		err := aw.Close()
		if err != nil {
			fmt.Printf("Failed to close archive: %s", err)
		}
		fmt.Printf("📁 Archive created at: %s\n", c.archiveFilePath)
	}()

	// Server ID -> ServerInfo map
	var serverInfoMap = make(map[string]*server.ServerInfo)

	// Discover servers by broadcasting a PING and then waiting for responses
	c.LogProgress("⏳ Broadcasting PING to discover servers... (this may take a few seconds)\n")
	err = doReqAsync(nil, "$SYS.REQ.SERVER.PING", 0, nc, func(b []byte) {
		var apiResponse server.ServerAPIResponse
		if err = json.Unmarshal(b, &apiResponse); err != nil {
			fmt.Printf("Failed to deserialize PING response: %s", err)
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		_, exists := serverInfoMap[apiResponse.Server.ID]
		if exists {
			fmt.Printf("Duplicate server %s (%s) response to PING, ignoring", serverId, serverName)
			return
		}

		serverInfoMap[serverId] = apiResponse.Server
		c.LogProgress("📣 Discovered server '%s' (%s)\n", serverName, serverId)
	})
	if err != nil {
		return fmt.Errorf("failed to PING: %w", err)
	}
	c.LogProgress("ℹ️ Discovered %d servers\n", len(serverInfoMap))

	// Account name -> count of servers
	var accountIdsToServersCountMap = make(map[string]int)
	var systemAccount = ""

	// Broadcast PING.ACCOUNTZ to discover accounts
	c.LogProgress("⏳ Broadcasting PING to discover accounts... \n")
	err = doReqAsync(nil, "$SYS.REQ.SERVER.PING.ACCOUNTZ", len(serverInfoMap), nc, func(b []byte) {
		var apiResponse CustomServerAPIResponse
		err = json.Unmarshal(b, &apiResponse)
		if err != nil {
			fmt.Printf("Failed to deserialize ACCOUNTZ response, ignoring\n")
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		// Ignore responses from servers not discovered earlier.
		// We are discarding useful data, but limiting additional collection to a fixed set of nodes
		// simplifies querying and analysis. Could always re-run gather if a new server just joined.
		if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
			fmt.Printf("Ignoring ACCOUNTZ response from unknown server: %s\n", serverName)
			return
		}

		var accountsResponse server.Accountz
		err = json.Unmarshal(apiResponse.Data, &accountsResponse)
		if err != nil {
			fmt.Printf("Failed to deserialize PING.ACCOUNTZ response: %s\n", err)
			return
		}

		c.LogProgress("📣 Discovered %d accounts on server %s\n", len(accountsResponse.Accounts), serverName)

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
			fmt.Printf("Server %s system account is not set\n", serverName)
		} else if systemAccount == "" {
			systemAccount = accountsResponse.SystemAccount
			c.LogProgress("ℹ️ Discovered system account name: %s\n", systemAccount)
		} else if systemAccount != accountsResponse.SystemAccount {
			// This should not happen under normal circumstances!
			fmt.Printf("Multiple system accounts detected (%s, %s)\n", systemAccount, accountsResponse.SystemAccount)
		} else {
			// Known system account matches the one in the response, nothing to do
		}
	})
	if err != nil {
		return fmt.Errorf("failed to PING.ACCOUNTZ: %w", err)
	}
	c.LogProgress("ℹ️ Discovered %d accounts over %d servers\n", len(accountIdsToServersCountMap), len(serverInfoMap))

	if c.noServerEndpoints {
		c.LogProgress("Skipping servers endpoints data gathering \n")
	} else {
		// For each known server, query a set of endpoints
		c.LogProgress("⏳ Querying %d endpoints on %d known servers...\n", len(serverEndpoints), len(serverInfoMap))
		capturedCount := 0
		for serverId, serverInfo := range serverInfoMap {
			serverName := serverInfo.Name
			for _, endpoint := range serverEndpoints {

				subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.%s", serverId, endpoint.apiSuffix)

				endpointResponse := reflect.New(reflect.TypeOf(endpoint.expectedStruct)).Interface()

				responses, err := doReq(nil, subject, 1, nc)
				if err != nil {
					fmt.Printf("Failed to request %s from server %s: %s", endpoint.apiSuffix, serverName, err)
					continue
				}

				if len(responses) != 1 {
					fmt.Printf("Unexpected number of responses to %s from server %s: %d", endpoint.apiSuffix, serverName, len(responses))
					continue
				}

				responseBytes := responses[0]

				var apiResponse CustomServerAPIResponse
				if err = json.Unmarshal(responseBytes, &apiResponse); err != nil {
					fmt.Printf("Failed to deserialize %s response from server %s: %s", endpoint.apiSuffix, serverName, err)
					continue
				}

				err = json.Unmarshal(apiResponse.Data, endpointResponse)
				if err != nil {
					fmt.Printf("Failed to deserialize %s response data from server %s: %s", endpoint.apiSuffix, serverName, err)
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
		c.LogProgress("ℹ️ Captured %d endpoint responses from %d servers\n", capturedCount, len(serverInfoMap))
	}

	if c.noAccountEndpoints {
		c.LogProgress("Skipping accounts endpoints data gathering \n")
	} else {
		// For each known account, query a set of endpoints
		capturedCount := 0
		c.LogProgress("⏳ Querying %d endpoints for %d known accounts...\n", len(accountEndpoints), len(accountIdsToServersCountMap))
		for accountId, serversCount := range accountIdsToServersCountMap {
			for _, endpoint := range accountEndpoints {
				subject := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.%s", accountId, endpoint.apiSuffix)
				endpointResponses := make(map[string]interface{}, serversCount)

				err = doReqAsync(nil, subject, serversCount, nc, func(b []byte) {
					var apiResponse CustomServerAPIResponse
					err := json.Unmarshal(b, &apiResponse)
					if err != nil {
						fmt.Printf("Failed to deserialize %s response for account %s: %s", endpoint.apiSuffix, accountId, err)
						return
					}

					serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

					// Ignore responses from servers not discovered earlier.
					// We are discarding useful data, but limiting additional collection to a fixed set of nodes
					// simplifies querying and analysis. Could always re-run gather if a new server just joined.
					if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
						fmt.Printf("Ignoring ACCOUNT.%s response from unknown server: %s\n", endpoint.apiSuffix, serverName)
						return
					}

					endpointResponse := reflect.New(reflect.TypeOf(endpoint.expectedStruct)).Interface()
					err = json.Unmarshal(apiResponse.Data, endpointResponse)
					if err != nil {
						fmt.Printf("Failed to deserialize ACCOUNT.%s response for account %s: %s\n", endpoint.apiSuffix, accountId, err)
						return
					}

					if _, isDuplicateResponse := endpointResponses[serverName]; isDuplicateResponse {
						fmt.Printf("Ignoring duplicate ACCOUNT.%s response from server %s\n", endpoint.apiSuffix, serverName)
						return
					}

					endpointResponses[serverName] = endpointResponse
				})
				if err != nil {
					fmt.Printf("Failed to request %s for account %s: %s\n", endpoint.apiSuffix, accountId, err)
					continue
				}

				// Store all responses for this account endpoint
				for serverName, endpointResponse := range endpointResponses {
					tags := []*archive.Tag{
						archive.TagAccount(accountId),
						archive.TagServer(serverName), // Source server
						endpoint.typeTag,              // Type of artifact
					}

					err = aw.Add(endpointResponse, tags...)
					if err != nil {
						return fmt.Errorf("failed to add response to %s to archive: %w", subject, err)
					}

					capturedCount += 1
				}
			}
		}
		c.LogProgress("ℹ️ Captured %d endpoint responses from %d accounts\n", capturedCount, len(accountIdsToServersCountMap))
	}

	// Capture streams info using JSZ, unless configured to skip
	if c.noStreamInfo {
		c.LogProgress("Skipping streams data gathering \n")
	} else {
		c.LogProgress("⏳ Gathering streams data... \n")
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
					fmt.Printf("Failed to deserialize JSZ response for account %s: %s\n", accountId, err)
					return
				}

				serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

				// Ignore responses from servers not discovered earlier.
				// We are discarding useful data, but limiting additional collection to a fixed set of nodes
				// simplifies querying and analysis. Could always re-run gather if a new server just joined.
				if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
					fmt.Printf("Ignoring JSZ response from unknown server: %s\n", serverName)
					return
				}

				if _, isDuplicateResponse := jsInfoResponses[serverName]; isDuplicateResponse {
					fmt.Printf("Ignoring duplicate JSZ response for account %s from server %s\n", accountId, serverName)
					return
				}

				jsInfoResponse := &server.JSInfo{}
				err = json.Unmarshal(apiResponse.Data, jsInfoResponse)
				if err != nil {
					fmt.Printf("Failed to deserialize JSZ response data for account %s: %s\n", accountId, err)
					return
				}

				if len(jsInfoResponse.AccountDetails) == 0 {
					// No account details in response, don't bother saving this
					//fmt.Printf("🐛 Skip JSZ response from %s, no accounts details\n", serverName)
					return
				} else if len(jsInfoResponse.AccountDetails) > 1 {
					// Server will respond with multiple accounts if the one specified in the request is not found
					// https://github.com/nats-io/nats-server/pull/5229
					//fmt.Printf("🐛 Skip JSZ response from %s, account not found\n", serverName)
					return
				}

				jsInfoResponses[serverName] = jsInfoResponse
			})
			if err != nil {
				fmt.Printf("Failed to request JSZ for account %s: %s\n", accountId, err)
				continue
			}

			streamNamesMap := make(map[string]any)

			for serverName, jsInfo := range jsInfoResponses {

				// Cases where len(jsInfo.AccountDetails) != 1 are filtered above
				accountDetails := jsInfo.AccountDetails[0]

				for _, streamDetail := range accountDetails.Streams {
					streamName := streamDetail.Name

					_, streamKnown := streamNamesMap[streamName]
					if !streamKnown {
						c.LogProgress("📣 Discovered stream %s in account %s\n", streamName, accountId)
					}

					tags := []*archive.Tag{
						archive.TagAccount(accountId),
						archive.TagServer(serverName), // Source server
						archive.TagStreamDetails(),
						archive.TagStream(streamName),
					}

					if streamDetail.Cluster != nil {
						tags = append(tags, archive.TagCluster(streamDetail.Cluster.Name))
					} else {
						tags = append(tags, archive.TagNoCluster())
					}

					err = aw.Add(streamDetail, tags...)
					if err != nil {
						return fmt.Errorf("failed to add stream %s details to archive: %w", streamName, err)
					}

					streamNamesMap[streamName] = nil
				}
			}

			c.LogProgress("ℹ️ Discovered %d streams in account %s\n", len(streamNamesMap), accountId)
			capturedCount += len(streamNamesMap)

		}
		c.LogProgress("ℹ️ Discovered %d streams in %d accounts\n", capturedCount, len(accountIdsToServersCountMap))
	}

	return nil
}

func (c *PaGatherCmd) LogProgress(format string, args ...any) {
	if !c.noPrintProgress {
		fmt.Printf(format, args...)
	}
}
