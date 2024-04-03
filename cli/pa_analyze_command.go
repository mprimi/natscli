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
	"errors"
	"fmt"
	"math"

	"github.com/choria-io/fisk"
	"github.com/mprimi/natscli/archive"
	"github.com/nats-io/nats-server/v2/server"
)

type paAnalyzeCmd struct {
	archivePath   string
	veryVerbose   bool
	examplesLimit uint
	allExamples   bool
}
type checkStatus int

func (s checkStatus) badge() string {
	switch s {
	case Pass:
		return "✅ PASS"
	case Fail:
		return "❌ FAIL"
	case SomeIssues:
		return "⚠️ WARN"
	case Skipped:
		return "◻️ SKIP"
	default:
		panic(s)
	}
}

const (
	Skipped checkStatus = iota
	Pass
	Fail
	SomeIssues
)

func configurePaAnalyzeCommand(srv *fisk.CmdClause) {
	c := &paAnalyzeCmd{}

	analyze := srv.Command("analyze", "perform checks against an archive generated by the Gather subcommand").Action(c.analyze)
	analyze.Arg("archive-path", "path to input archive to analyze").Required().StringVar(&c.archivePath)
	analyze.Flag("examples", "Maximum number of example issues to display per check").Default("5").UintVar(&c.examplesLimit)
	analyze.Flag("all-examples", "Display all issues detected by each check").UnNegatableBoolVar(&c.allExamples)
	// Hidden flags
	analyze.Flag("very-verbose", "Print a lot of intermediate detailed during analysis").Hidden().BoolVar(&c.veryVerbose)

}

func (cmd *paAnalyzeCmd) analyze(_ *fisk.ParseContext) error {
	// Configure based on options
	if cmd.allExamples {
		cmd.examplesLimit = 0
	}

	// Open archive
	ar, err := archive.NewReader(cmd.archivePath)
	if err != nil {
		return err
	}
	defer func() {
		err := ar.Close()
		if err != nil {
			fmt.Printf("Failed to close archive reader: %s\n", err)
		}
	}()

	// List of known checks
	var checks = []struct {
		checkName string
		checkFunc func(r *archive.Reader) (checkStatus, error)
	}{
		{
			"Server health",
			cmd.checkServerHealth,
		},
		{
			"Uniform server version",
			cmd.checkServerVersions,
		},
		{
			"Slow consumers",
			cmd.checkSlowConsumers,
		},
		{
			"Cluster memory usage",
			cmd.checkClusterMemoryUsageOutliers,
		},
		{
			"Lagging stream replicas",
			cmd.checkLaggingStreamReplicas,
		},
		{
			"CPU usage",
			cmd.checkCpuUsage,
		},
		{
			"High cardinality streams",
			cmd.checkHighCardinalityStreams,
		},
		{
			"High number of HA assets",
			cmd.checkHighCardinalityHAAssets,
		},
		{
			"Reserved resources usage",
			cmd.checkResourceLimits,
		},
		{
			"Account limits",
			cmd.checkAccountLimits,
		},
		{
			"Stream limits",
			cmd.checkStreamLimits,
		},
		{
			"Meta cluster state",
			cmd.checkMetaCluster,
		},
		{
			"Routes and gateways",
			cmd.checkRoutesAndGateways,
		},
	}

	// Run checks, one at the time
	checkOutcomes := make([]checkStatus, len(checks))
	for i, check := range checks {

		fmt.Printf("\n--\n")
		cmd.logDebug("Running check: %s", check.checkName)
		outcome, err := check.checkFunc(ar)
		if err != nil {
			return fmt.Errorf("check '%s' error: %w", check.checkName, err)
		}
		checkOutcomes[i] = outcome

		fmt.Printf("%s - %s\n--\n", outcome.badge(), check.checkName)
	}

	return nil
}

// checkServerVersions ensures all servers discovered are running the same version
func (cmd *paAnalyzeCmd) checkServerVersions(r *archive.Reader) (checkStatus, error) {
	var (
		serverTags           = r.ListServerTags()
		versionsToServersMap = make(map[string][]string)
		versionsList         = make([]string, 0)
	)

	examples := newCollectionOfExamples(cmd.examplesLimit)

	artifactType := archive.TagServerVars()
	outcome := Pass

	for _, serverTag := range serverTags {
		serverName := serverTag.Value
		var serverVarz server.Varz

		err := r.Load(&serverVarz, &serverTag, artifactType)
		if errors.Is(err, archive.ErrNoMatches) {
			cmd.logWarning("Artifact 'VARZ' is missing for server %s", serverName)
			continue
		} else if err != nil {
			return Skipped, fmt.Errorf("failed to load variables for server %s: %w", serverTag.Value, err)
		}

		version := serverVarz.Version

		_, exists := versionsToServersMap[version]
		if !exists {
			versionsToServersMap[version] = []string{}
			versionsList = append(versionsList, version)
			examples.Addf("%s - %s", serverName, version)
		}
		versionsToServersMap[version] = append(versionsToServersMap[version], serverName)
	}

	if len(versionsList) == 1 {
		cmd.logInfo("All servers are running version %s", versionsList[0])
	} else {
		cmd.logIssue("Servers are running %d different versions", len(versionsList))
		cmd.logExamples(examples)
	}

	return outcome, nil
}

func (cmd *paAnalyzeCmd) checkServerHealth(r *archive.Reader) (checkStatus, error) {
	serverTags := r.ListServerTags()
	artifactType := archive.TagHealth()
	notHealthy, healthy := 0, 0
	examples := newCollectionOfExamples(cmd.examplesLimit)

	for _, serverTag := range serverTags {
		serverName := serverTag.Value
		var health server.HealthStatus

		err := r.Load(&health, &serverTag, artifactType)
		if errors.Is(err, archive.ErrNoMatches) {
			cmd.logWarning("Artifact 'HEALTHZ' is missing for server %s", serverName)
			continue
		} else if err != nil {
			return Skipped, fmt.Errorf("failed to load health for server %s: %w", serverName, err)
		}

		if health.Status != "ok" {
			examples.Addf("%s: %d - %s", serverName, health.StatusCode, health.Status)
			notHealthy += 1
		} else {
			healthy += 1
		}
	}

	if notHealthy > 0 {
		cmd.logIssue("%d/%d servers are not healthy", notHealthy, healthy+notHealthy)
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	cmd.logInfo("%d/%d servers are healthy", healthy, healthy)
	return Pass, nil
}

// checkSlowConsumers alerts for any slow consumer found on any server
func (cmd *paAnalyzeCmd) checkSlowConsumers(r *archive.Reader) (checkStatus, error) {
	serverTags := r.ListServerTags()
	totalSlowConsumers := int64(0)
	serversWithSlowConsumers := make(map[string]int64)
	examples := newCollectionOfExamples(cmd.examplesLimit)

	for _, serverTag := range serverTags {
		serverName := serverTag.Value
		var serverVarz server.Varz
		err := r.Load(&serverVarz, &serverTag, archive.TagServerVars())
		if err != nil {
			return Skipped, fmt.Errorf("failed to load Varz for server %s: %w", serverName, err)
		}

		if slowConsumers := serverVarz.SlowConsumers; slowConsumers > 0 {
			serversWithSlowConsumers[serverName] = slowConsumers
			examples.Addf("%s: %d slow consumers", serverName, slowConsumers)
			totalSlowConsumers += slowConsumers
		}
	}

	if totalSlowConsumers > 0 {
		cmd.logIssue("Total slow consumers: %d over %d servers", totalSlowConsumers, len(serversWithSlowConsumers))
		cmd.logExamples(examples)
		return SomeIssues, nil
	}
	return Pass, nil
}

const checkClusterMemoryUsageOutlierThreshold = 0.5 // Warn if one node is using over 1.5x the average
// checkClusterMemoryUsageOutliers iterates over clusters and checks whether any of the server memory usage is
// significantly higher than the cluster average.
func (cmd *paAnalyzeCmd) checkClusterMemoryUsageOutliers(r *archive.Reader) (checkStatus, error) {
	typeTag := archive.TagServerVars()
	clusterNames := r.GetClusterNames()
	examples := newCollectionOfExamples(cmd.examplesLimit)

	clustersWithIssuesMap := make(map[string]interface{}, len(clusterNames))

	for _, clusterName := range clusterNames {
		clusterTag := archive.TagCluster(clusterName)

		serverNames := r.GetClusterServerNames(clusterName)
		clusterMemoryUsageMap := make(map[string]float64, len(serverNames))
		clusterMemoryUsageTotal := float64(0)
		numServers := 0 // cannot use len(serverNames) as some artifacts may be missing

		for _, serverName := range serverNames {
			serverTag := archive.TagServer(serverName)

			var serverVarz server.Varz
			err := r.Load(&serverVarz, clusterTag, serverTag, typeTag)
			if errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'VARZ' is missing for server %s in cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load VARZ for server %s in cluster %s: %w", serverName, clusterName, err)
			}

			numServers += 1
			clusterMemoryUsageMap[serverTag.Value] = float64(serverVarz.Mem)
			clusterMemoryUsageTotal += float64(serverVarz.Mem)
		}

		clusterMemoryUsageMean := clusterMemoryUsageTotal / float64(numServers)
		threshold := clusterMemoryUsageMean + (clusterMemoryUsageMean * checkClusterMemoryUsageOutlierThreshold)

		for serverName, serverMemoryUsage := range clusterMemoryUsageMap {
			if serverMemoryUsage > threshold {
				examples.Addf(
					"Cluster %s avg: %s, server %s: %s",
					clusterName,
					fiBytes(uint64(clusterMemoryUsageMean)),
					serverName,
					fiBytes(uint64(serverMemoryUsage)),
				)
				clustersWithIssuesMap[clusterName] = nil
			}
		}
	}

	if len(clustersWithIssuesMap) > 0 {
		cmd.logIssue(
			"Servers with memory usage %.0f%% above cluster average: %d in %d clusters",
			checkClusterMemoryUsageOutlierThreshold*100,
			examples.Count(),
			len(clustersWithIssuesMap),
		)
		cmd.logExamples(examples)
		return SomeIssues, nil
	}
	return Pass, nil
}

const checkLaggingStreamReplicasThreshold = 0.1 // Warn if a replica is 10% behind the maximum in group
// checkLaggingStreamReplicas inspects all streams and checks that no replica is behind (lastSeq) compared to the
// replica with the highest lastSeq
func (cmd *paAnalyzeCmd) checkLaggingStreamReplicas(r *archive.Reader) (checkStatus, error) {
	typeTag := archive.TagStreamDetails()
	accountNames := r.GetAccountNames()
	examples := newCollectionOfExamples(cmd.examplesLimit)

	if len(accountNames) == 0 {
		cmd.logInfo("No accounts found in archive")
	}

	accountsWithStreams := make(map[string]interface{})
	streamsInspected := make(map[string]interface{})
	laggingReplicas := 0

	for _, accountName := range accountNames {
		accountTag := archive.TagAccount(accountName)
		streamNames := r.GetAccountStreamNames(accountName)

		if len(streamNames) == 0 {
			cmd.logDebug("No streams found in account: %s", accountName)
		}

		for _, streamName := range streamNames {

			// Track accounts with at least one streams
			accountsWithStreams[accountName] = nil

			streamTag := archive.TagStream(streamName)
			serverNames := r.GetStreamServerNames(accountName, streamName)

			cmd.logDebug(
				"Inspecting account '%s' stream '%s', found %d servers: %v",
				accountName,
				streamName,
				len(serverNames),
				serverNames,
			)

			// Create map server->streamDetails
			replicasStreamDetails := make(map[string]*server.StreamDetail, len(serverNames))
			streamIsEmpty := true

			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)
				streamDetails := &server.StreamDetail{}
				err := r.Load(streamDetails, accountTag, streamTag, serverTag, typeTag)
				if errors.Is(err, archive.ErrNoMatches) {
					cmd.logWarning(
						"Artifact not found: %s for stream %s in account %s by server %s",
						typeTag.Value,
						streamName,
						accountName,
						serverName,
					)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to lookup stream artifact: %w", err)
				}

				if streamDetails.State.LastSeq > 0 {
					streamIsEmpty = false
				}

				replicasStreamDetails[serverName] = streamDetails
				// Track streams with least one artifact
				streamsInspected[accountName+"/"+streamName] = nil
			}

			// Check that all replicas are not too far behind the replica with the highest message & byte count
			if !streamIsEmpty {
				// Find the highest lastSeq
				highestLastSeq, highestLastSeqServer := uint64(0), ""
				for serverName, streamDetail := range replicasStreamDetails {
					lastSeq := streamDetail.State.LastSeq
					if lastSeq > highestLastSeq {
						highestLastSeq = lastSeq
						highestLastSeqServer = serverName
					}
				}
				cmd.logDebug(
					"Stream %s / %s highest last sequence: %d @ %s",
					accountName,
					streamName,
					highestLastSeq,
					highestLastSeqServer,
				)

				// Check if some server's sequence is below warning threshold
				lastSequenceThreshold := uint64(math.Max(0, float64(highestLastSeq)-(float64(highestLastSeq)*checkLaggingStreamReplicasThreshold)))
				for serverName, streamDetail := range replicasStreamDetails {
					lastSeq := streamDetail.State.LastSeq
					if lastSeq < lastSequenceThreshold {
						examples.Addf(
							"%s/%s server %s lastSequence: %d is behind highest lastSequence: %d on server: %s",
							accountName,
							streamName,
							serverName,
							lastSeq,
							highestLastSeq,
							highestLastSeqServer,
						)
						laggingReplicas += 1
					}
				}
			}
		}
	}

	cmd.logInfo("Inspected %d streams across %d accounts", len(streamsInspected), len(accountsWithStreams))

	if laggingReplicas > 0 {
		cmd.logIssue("Found %d replicas lagging behind", laggingReplicas)
		cmd.logExamples(examples)
		return SomeIssues, nil
	}
	return Pass, nil
}

const cpuUsageThreshold = 0.9 // Warn if any server is using more than 90% of the available CPU
// checkCpuUsage checks the CPU usage of all servers and alerts if any server is using more than 90% of the available CPU
func (cmd *paAnalyzeCmd) checkCpuUsage(r *archive.Reader) (checkStatus, error) {
	serverTags := r.ListServerTags()
	examples := newCollectionOfExamples(cmd.examplesLimit)

	for _, serverTag := range serverTags {
		serverName := serverTag.Value
		var serverVarz server.Varz

		if err := r.Load(&serverVarz, &serverTag, archive.TagServerVars()); errors.Is(err, archive.ErrNoMatches) {
			cmd.logWarning("Artifact 'VARZ' is missing for server %s", serverName)
			continue
		} else if err != nil {
			return Skipped, fmt.Errorf("failed to load VARZ for server %s: %w", serverName, err)
		}

		averageCpuUtilization := serverVarz.CPU / float64(serverVarz.Cores)

		if averageCpuUtilization > cpuUsageThreshold {
			examples.Addf("%s: %.0f%%", serverName, averageCpuUtilization)
		}
	}

	if examples.Count() > 0 {
		cmd.logIssue("Found servers with high CPU usage")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

const highCardinalityStreamsThreshold = 1_000_000 // Warn if any stream has more than 1,000,000 unique subjects
// checkHighCardinalityStreams checks the number of unique subjects in streams and alerts if any stream has a high number of unique subjects
func (cmd *paAnalyzeCmd) checkHighCardinalityStreams(r *archive.Reader) (checkStatus, error) {
	typeTag := archive.TagStreamDetails()
	accountNames := r.GetAccountNames()
	examples := newCollectionOfExamples(cmd.examplesLimit)

	for _, accountName := range accountNames {
		accountTag := archive.TagAccount(accountName)
		streamNames := r.GetAccountStreamNames(accountName)

		for _, streamName := range streamNames {
			streamTag := archive.TagStream(streamName)

			serverNames := r.GetStreamServerNames(accountName, streamName)
			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)

				var streamDetails server.StreamDetail

				if err := r.Load(&streamDetails, serverTag, accountTag, streamTag, typeTag); errors.Is(err, archive.ErrNoMatches) {
					cmd.logWarning("Artifact 'STREAM_DETAILS' is missing for stream %s in account %s", streamName, accountName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load STREAM_DETAILS for stream %s in account %s: %w", streamName, accountName, err)
				}

				if streamDetails.State.NumSubjects > highCardinalityStreamsThreshold {
					examples.Addf("%s/%s: %d subjects", accountName, streamName, streamDetails.State.NumSubjects)
					continue // no need to check other servers for this stream
				}
			}
		}
	}

	if examples.Count() > 0 {
		cmd.logIssue("Found streams with high subjects cardinality")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

const haAssetsCardinalityThreshold = 1000 // Warn if any server has more than 1000 high availability assets
// checkHighCardinalityStreams checks the number of high availability assets in servers and alerts if any server exceeds the threshold
func (cmd *paAnalyzeCmd) checkHighCardinalityHAAssets(r *archive.Reader) (checkStatus, error) {
	examples := newCollectionOfExamples(cmd.examplesLimit)

	clusterTags := r.ListClusterTags()
	for _, clusterTag := range clusterTags {
		clusterName := clusterTag.Value
		serverNames := r.GetClusterServerNames(clusterName)

		for _, serverName := range serverNames {

			var serverJSInfo server.JSInfo

			if err := r.Load(&serverJSInfo, &clusterTag, archive.TagServer(serverName), archive.TagJetStream()); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if serverJSInfo.HAAssets > haAssetsCardinalityThreshold {
				examples.Addf("%s: %d HA assets", serverName, serverJSInfo.HAAssets)
			}
		}
	}

	if examples.Count() > 0 {
		cmd.logIssue("Found servers with high a large amount of HA assets")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

const resourceLimitUsageThreshold = 0.90 // Warn if any server's memory/storage usage is 95% of reserved memory/storage
// checkResourceLimits checks the memory and storage usage of all servers and alerts if usage is too close to the reserved amounts
func (cmd *paAnalyzeCmd) checkResourceLimits(r *archive.Reader) (checkStatus, error) {
	examples := newCollectionOfExamples(cmd.examplesLimit)

	clusterTags := r.ListClusterTags()
	for _, clusterTag := range clusterTags {
		clusterName := clusterTag.Value

		serverNames := r.GetClusterServerNames(clusterName)
		for _, serverName := range serverNames {
			var serverJSInfo server.JSInfo
			if err := r.Load(&serverJSInfo, archive.TagCluster(clusterName), archive.TagServer(serverName), archive.TagJetStream()); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if serverJSInfo.ReservedMemory > 0 {
				memoryUsage := float64(serverJSInfo.Memory) / float64(serverJSInfo.ReservedMemory)
				if memoryUsage > resourceLimitUsageThreshold {
					examples.Addf("%s memory: usage: %dMb, reserved: %dMb", serverName, serverJSInfo.Memory/1024/1024, serverJSInfo.ReservedMemory/1024/1024)
				}
			}

			if serverJSInfo.ReservedStore > 0 {
				storageUsage := float64(serverJSInfo.Store) / float64(serverJSInfo.ReservedStore)
				if storageUsage > resourceLimitUsageThreshold {
					examples.Addf("%s storage: usage: %dMb, reserved: %dMb", serverName, serverJSInfo.Store/1024/1024, serverJSInfo.ReservedStore/1024/1024)
				}
			}
		}
	}

	if examples.Count() > 0 {
		cmd.logIssue("Found servers with high memory/storage usage")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

// check accounts for high usage of resources limited by account limits, does not check for jetstream limits
func (cmd *paAnalyzeCmd) checkAccountLimits(r *archive.Reader) (checkStatus, error) {

	type accountLimit struct {
		description    string
		usageThreshold float64
		limit          int64
		actual         int64
	}

	examples := newCollectionOfExamples(cmd.examplesLimit)
	serverTags := r.ListServerTags()
	typeTag := archive.TagAccounts()

	accountNames := r.GetAccountNames()
	for _, accountName := range accountNames {
		accountTag := archive.TagAccount(accountName)

		for _, serverTag := range serverTags {

			var accountInfo server.AccountInfo
			if err := r.Load(&accountInfo, &serverTag, accountTag, typeTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("AccountInfo is missing for account %s", accountName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load AccountInfo from server %s for account %s, error: %w", serverTag.Value, accountName, err)
			}

			if accountInfo.Claim == nil {
				// account does not have JWT claim
				continue
			}

			accountLimits := []accountLimit{
				{
					description:    "Connections",
					usageThreshold: 0.95,
					limit:          accountInfo.Claim.Limits.Conn,
					actual:         int64(accountInfo.ClientCnt),
				},
				{
					description:    "Leafnodes",
					usageThreshold: 0.9,
					limit:          accountInfo.Claim.Limits.LeafNodeConn,
					actual:         int64(accountInfo.LeafCnt),
				},
				{
					description:    "Subscriptions",
					usageThreshold: 1,
					limit:          accountInfo.Claim.Limits.Subs,
					actual:         int64(accountInfo.SubCnt),
				},
				{
					description:    "Streams",
					usageThreshold: 0.9,
					limit:          accountInfo.Claim.Limits.Streams,
					actual:         int64(len(r.GetAccountStreamNames(accountName))), // streams that belong to this account
				},
			}

			// check all account limits
			for _, accountLimit := range accountLimits {
				// skip if limit is disabled or unlimited
				if accountLimit.limit == 0 || accountLimit.limit == -1 {
					continue
				}

				// calculate usage percentage
				usage := 1.0 - (float64(accountLimit.limit-accountLimit.actual) / float64(accountLimit.limit))
				if usage > accountLimit.usageThreshold {
					examples.Addf("%s - %s: %d/%d", accountName, accountLimit.description, accountLimit.actual, accountLimit.limit)
				}
			}

		}
	}

	if examples.Count() > 0 {
		cmd.logIssue("Found accounts with high usage of limits")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkStreamLimits checks for streams that are close to their limits set in the stream config
func (cmd *paAnalyzeCmd) checkStreamLimits(r *archive.Reader) (checkStatus, error) {

	type streamLimit struct {
		description    string
		usageThreshold float64
		limit          int64
		actual         int64
	}

	typeTag := archive.TagStreamDetails()
	accountNames := r.GetAccountNames()
	examples := newCollectionOfExamples(cmd.examplesLimit)

	for _, accountName := range accountNames {
		accountTag := archive.TagAccount(accountName)
		streamNames := r.GetAccountStreamNames(accountName)

		for _, streamName := range streamNames {
			streamTag := archive.TagStream(streamName)

			serverNames := r.GetStreamServerNames(accountName, streamName)
			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)

				var streamDetails server.StreamDetail

				if err := r.Load(&streamDetails, serverTag, accountTag, streamTag, typeTag); errors.Is(err, archive.ErrNoMatches) {
					cmd.logWarning("Artifact 'STREAM_DETAILS' is missing for stream %s in account %s", streamName, accountName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load STREAM_DETAILS for stream %s in account %s: %w", streamName, accountName, err)
				}

				streamLimits := []streamLimit{
					{
						description:    "Messages",
						usageThreshold: 0.95,
						limit:          streamDetails.Config.MaxMsgs,
						actual:         int64(streamDetails.State.Msgs),
					},
					{
						description:    "Bytes",
						usageThreshold: 0.95,
						limit:          streamDetails.Config.MaxBytes,
						actual:         int64(streamDetails.State.Bytes),
					},
					{
						description:    "Consumers",
						usageThreshold: 0.90,
						limit:          int64(streamDetails.Config.MaxConsumers),
						actual:         int64(streamDetails.State.Consumers),
					},
				}

				for _, streamLimit := range streamLimits {
					// skip if limit is disabled or unlimited
					if streamLimit.limit == 0 || streamLimit.limit == -1 {
						continue
					}

					// calculate usage percentage
					usage := 1.0 - (float64(streamLimit.limit-streamLimit.actual) / float64(streamLimit.limit))

					if usage > streamLimit.usageThreshold {
						examples.Addf("%s/%s - %s: %d/%d", accountName, streamName, streamLimit.description, streamLimit.actual, streamLimit.limit)
					}
				}
			}
		}
	}

	if examples.Count() > 0 {
		cmd.logIssue("Found streams with high usage of limits")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

func (cmd *paAnalyzeCmd) checkMetaCluster(r *archive.Reader) (checkStatus, error) {
	examples := newCollectionOfExamples(cmd.examplesLimit)

	clusterTags := r.ListClusterTags()
	for _, clusterTag := range clusterTags {
		clusterName := clusterTag.Value
		serverNames := r.GetClusterServerNames(clusterName)

		for _, serverName := range serverNames {

			var serverJSInfo server.JSInfo

			if err := r.Load(&serverJSInfo, &clusterTag, archive.TagServer(serverName), archive.TagJetStream()); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if serverJSInfo.Meta == nil {
				cmd.logDebug("Server %s does not have meta cluster information", serverName)
				continue
			}

			for _, replica := range serverJSInfo.Meta.Replicas {
				if replica.Offline {
					if replica.Name == serverJSInfo.Meta.Leader {
						examples.Addf("%s reports leader %s as offline", serverName, replica.Name)
					} else {
						examples.Addf("%s reports replica %s as offline", serverName, replica.Name)
					}
				}
				if !replica.Current {
					examples.Addf("%s reports replica %s as not current", serverName, replica.Name)
				}
			}

		}
	}

	if examples.Count() > 0 {
		cmd.logIssue("Found unhealthy nodes in meta cluster")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

func (cmd *paAnalyzeCmd) checkRoutesAndGateways(r *archive.Reader) (checkStatus, error) {
	examples := newCollectionOfExamples(cmd.examplesLimit)

	type gatewaysInfo struct {
		numInboundGateways  int
		numOutboundGateways int
	}

	clusterNames := r.GetClusterNames()
	for _, clusterName := range clusterNames {
		clusterTag := archive.TagCluster(clusterName)
		serverNames := r.GetClusterServerNames(clusterName)

		numRoutesToServersMap := make(map[int][]string)
		numInboundGatewaysToServersMap := make(map[int][]string)
		numOutboundGatewaysToServersMap := make(map[int][]string)

		for _, serverName := range serverNames {
			serverTag := archive.TagServer(serverName)

			var (
				routez   server.Routez
				gateways server.Gatewayz
			)

			if err := r.Load(&routez, clusterTag, serverTag, archive.TagRoutes()); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'ROUTEZ' is missing for server %s cluster %s", serverName, clusterName)
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load ROUTEZ for server %s: %w", serverName, err)
			}

			if err := r.Load(&gateways, clusterTag, serverTag, archive.TagGateways()); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'GATEWAYZ' is missing for server %s cluster %s", serverName, clusterName)
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load GATEWAYZ for server %s: %w", serverName, err)
			}

			numRoutesToServersMap[routez.NumRoutes] = append(numRoutesToServersMap[routez.NumRoutes], serverName)
			numInboundGatewaysToServersMap[len(gateways.InboundGateways)] = append(numInboundGatewaysToServersMap[len(gateways.InboundGateways)], serverName)
			numOutboundGatewaysToServersMap[len(gateways.OutboundGateways)] = append(numOutboundGatewaysToServersMap[len(gateways.OutboundGateways)], serverName)

		}

		// check for servers with different number of routes and gateways
		addExample := false
		str := fmt.Sprintf("Cluster %s:\n", clusterName)
		if len(numRoutesToServersMap) > 1 {
			addExample = true
			for numRoutes, servers := range numRoutesToServersMap {
				str += fmt.Sprintf("     - %v: %d routes\n", servers, numRoutes)
			}
		}
		if len(numInboundGatewaysToServersMap) > 1 {
			addExample = true
			for numInboundGateways, servers := range numInboundGatewaysToServersMap {
				str += fmt.Sprintf("     - %v: %d inbound gateways\n", servers, numInboundGateways)
			}
		}
		if len(numOutboundGatewaysToServersMap) > 1 {
			addExample = true
			for numOutboundGateways, servers := range numOutboundGatewaysToServersMap {
				str += fmt.Sprintf("     - %v: %d outbound gateways\n", servers, numOutboundGateways)
			}
		}
		if addExample {
			examples.Addf(str)
		}

	}

	if examples.Count() > 0 {
		cmd.logIssue("Found nodes with inconsistent gateways")
		cmd.logExamples(examples)
		return SomeIssues, nil
	}

	return Pass, nil
}

// logSevereIssue for serious problems that need to be addressed
func (cmd *paAnalyzeCmd) logSevereIssue(format string, a ...any) {
	fmt.Printf("‼️  "+format+"\n", a...)
}

// logIssue for issues that need attention that need to be addressed
func (cmd *paAnalyzeCmd) logIssue(format string, a ...any) {
	fmt.Printf("❗️ "+format+"\n", a...)
}

// logInfo for neutral and positive messages
func (cmd *paAnalyzeCmd) logInfo(format string, a ...any) {
	fmt.Printf("ℹ️  "+format+"\n", a...)
}

// logWarning for issues running the check itself, but not serious enough to terminate with an error
func (cmd *paAnalyzeCmd) logWarning(format string, a ...any) {
	fmt.Printf("⚠️  "+format+"\n", a...)
}

// logDebug for very fine grained progress, disabled by default
func (cmd *paAnalyzeCmd) logDebug(format string, a ...any) {
	if cmd.veryVerbose {
		fmt.Printf("🔬  "+format+"\n", a...)
	}
}

// logExamples for printing some examples without risking flooding the output
func (cmd *paAnalyzeCmd) logExamples(examples *examplesCollection) {
	if len(examples.examples) > 0 {
		for _, example := range examples.examples {
			fmt.Printf("   - " + example + "\n")
		}
		if examples.dropped > 0 {
			fmt.Printf("   - ...%d more...\n", examples.dropped)
		}
	}
}

type examplesCollection struct {
	examples []string
	limit    int
	dropped  int
}

func (e *examplesCollection) Addf(format string, a ...any) {
	if len(e.examples) < e.limit {
		e.examples = append(e.examples, fmt.Sprintf(format, a...))
	} else {
		e.dropped += 1
	}
}

func (e *examplesCollection) Count() int {
	return e.dropped + len(e.examples)
}

func newCollectionOfExamples(limit uint) *examplesCollection {
	if limit == 0 {
		// If set to unlimited, set a very large limit
		limit = 1024
	}

	return &examplesCollection{
		examples: make([]string, 0, limit),
		limit:    int(limit),
		dropped:  0,
	}
}
