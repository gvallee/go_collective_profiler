//
// Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
//
// See LICENSE.txt for license information
//

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/gvallee/go_collective_profiler/internal/pkg/bandwidth"
	"github.com/gvallee/go_collective_profiler/internal/pkg/comm"
	"github.com/gvallee/go_collective_profiler/internal/pkg/patterns"
	"github.com/gvallee/go_collective_profiler/internal/pkg/profiler"
	"github.com/gvallee/go_collective_profiler/internal/pkg/timings"
	"github.com/gvallee/go_collective_profiler/pkg/counts"
	"github.com/gvallee/go_util/pkg/util"
)

func main() {
	helpFlag := flag.Bool("h", false, "Display help")
	verbose := flag.Bool("v", false, "Enable verbose mode")
	inputDirFlag := flag.String("input-dir", "", "Path to the directory where the input data is")
	outputDirFlag := flag.String("output-dir", "", "Path to the directory where to save the bandwidth data")
	profilerPathFlag := flag.String("profiler-src-dir", "", "When using the tools separately, path to directory where the profiler source is (optional)")
	flag.Parse()

	if *helpFlag {
		path := os.Args[0]
		cmd := filepath.Base(path)
		fmt.Printf("%s: calculate the bandwidth on a per collective call and rank basis based on counts of the collective operations gathered with the collective profiler\n", cmd)
		flag.Usage()
		os.Exit(0)
	}

	logFile := util.OpenLogFile("go_collective_profiler", "bandwidth")
	defer logFile.Close()
	if *verbose {
		nultiWriters := io.MultiWriter(os.Stdout, logFile)
		log.SetOutput(nultiWriters)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	if !util.PathExists(*inputDirFlag) {
		fmt.Printf("ERROR: %s does not exist\n", *inputDirFlag)
		os.Exit(1)
	}
	if !util.PathExists(*outputDirFlag) {
		fmt.Printf("ERROR: %s does not exist\n", *outputDirFlag)
		os.Exit(1)
	}
	_, filename, _, _ := runtime.Caller(0)
	codeBaseDir := filepath.Join(filepath.Dir(filename), "..", "..", "..")

	// Get data about the communicator
	profilerSrcDir := *profilerPathFlag
	if profilerSrcDir == "" {
		profilerSrcDir = codeBaseDir
	}
	commData, err := comm.GetData(profilerSrcDir, *inputDirFlag)
	if err != nil {
		fmt.Printf("ERROR: comm.GetData() failed: %s\n", err)
	}

	// Find all count files
	for leadRank, commIds := range commData.LeadMap {
		for _, commId := range commIds {
			var allCalls []int
			jobId, err := counts.GetJobIdFromLeadRank(*inputDirFlag, leadRank)
			if err != nil {
				fmt.Printf("ERROR: counts.GetJobIdFromLeadRank() failed: %s\n", err)
				os.Exit(1)
			}

			// Parse the count files
			commCounts, err := counts.LoadCommunicatorRawCompactFormatCounts(*inputDirFlag, jobId, leadRank, false)
			if err != nil {
				fmt.Printf("ERROR: counts.LoadCommunicatorRawCompactFormatCounts() failed: %s\n", err)
				os.Exit(1)
			}

			analysisResults := new(profiler.AnalysisResults)
			analysisResults.AllStats = make(map[int]counts.SendRecvStats)
			analysisResults.AllPatterns = make(map[int]patterns.Data)
			callsData, sendRecvStats, p, err := profiler.AnalyzeJobRankCounts(*inputDirFlag, jobId, leadRank, profiler.DefaultMsgSizeThreshold, false)
			if err != nil {
				fmt.Printf("ERROR: analyzeJobRankCounts() failed: %s\n", err)
				os.Exit(1)
			}
			analysisResults.TotalNumCalls += len(callsData)
			analysisResults.AllStats[leadRank] = sendRecvStats
			analysisResults.AllPatterns[leadRank] = p

			d := counts.CommDataT{
				LeadRank:  leadRank,
				CallData:  callsData,
				RawCounts: commCounts,
			}
			analysisResults.AllCallsData = append(analysisResults.AllCallsData, d)

			// Post-processing of the data. The data structures as they are now are a little non-obvious
			sendData := make(map[int]map[int]int)
			for callId, callData := range callsData {
				allCalls = append(allCalls, callId)
				sendData[callId] = make(map[int]int)
				ranksData := callData.SendData.Counts[callId]
				for rank, counts := range ranksData {
					for _, c := range counts {
						sendData[callId][rank] += c
					}
				}
			}
			recvData := make(map[int]map[int]int)
			for callId, callData := range callsData {
				recvData[callId] = make(map[int]int)
				ranksData := callData.RecvData.Counts[callId]
				for rank, counts := range ranksData {
					for _, c := range counts {
						recvData[callId][rank] += c
					}
				}
			}
			sort.Ints(allCalls)

			// Load all the execution times
			log.Println("-> Loading execution times...")
			jobIdTimeFile, err := timings.GetJobIdFromDataFiles(*inputDirFlag, "alltoallv", leadRank, commId)
			if err != nil {
				fmt.Printf("ERROR: timings.GetJobIdFromDataFiles() failed: %s\n", err)
				os.Exit(1)
			}
			execTimeFilename := timings.GetExecTimingFilename("alltoallv", leadRank, commId, jobIdTimeFile)
			execTimeFilePath := filepath.Join(*inputDirFlag, execTimeFilename)
			_, execTimesCallsData, _, err := timings.ParseTimingFile(execTimeFilePath, profilerSrcDir)
			if err != nil {
				fmt.Printf("ERROR: timings.ParseTimingFile() failed: %s", err)
				os.Exit(1)
			}

			// The number of ranks should be the same across all the data since it is for a single communicator
			// Based on this, we get the number of ranks based on how many ranks are involved in the first collective call
			log.Println("-> Calculating bandwidths...")
			numRanks := commCounts[0].Counts.CommSize
			if numRanks == 0 {
				fmt.Printf("ERROR: invalid number of ranks: %d\n", numRanks)
				os.Exit(1)
			}
			sendDatatypeSize := commCounts[0].Counts.SendDatatypeSize
			if sendDatatypeSize == 0 {
				fmt.Printf("ERROR: invalid send datatype size: %d\n", sendDatatypeSize)
				os.Exit(1)
			}
			recvDatatypeSize := commCounts[0].Counts.RecvDatatypeSize
			if recvDatatypeSize == 0 {
				fmt.Printf("ERROR: invalid recv datatype size: %d\n", recvDatatypeSize)
				os.Exit(1)
			}
			bwData, err := bandwidth.GetFromCallsCounts(len(callsData), numRanks, sendData, recvData, sendDatatypeSize, recvDatatypeSize, execTimesCallsData)
			if err != nil {
				fmt.Printf("ERROR: bandwidth.GetFromCallsCounts() failed: %s\n", err)
				os.Exit(1)
			}

			log.Println("-> Saving results...")
			outputFilePath := filepath.Join(*outputDirFlag, bandwidth.GetOutputFilename(leadRank))
			outputFile, err := os.Create(outputFilePath)
			if err != nil {
				fmt.Printf("ERROR: unable to create output file %s: %s\n", outputFilePath, err)
				os.Exit(1)
			}

			for _, callId := range allCalls {
				_, err := outputFile.WriteString(fmt.Sprintf("# Call %d\nsend BW (B/s)\treceive BW (B/s)\n", callId))
				if err != nil {
					fmt.Printf("ERROR: unable to write output to %s: %s\n", outputFilePath, err)
					os.Exit(1)
				}
				d := bwData.CallData[callId]
				for rank := 0; rank < numRanks; rank++ {
					_, err := outputFile.WriteString(fmt.Sprintf("%.2f\t%.2f\n", d.SendRankBW[rank], d.RecvRankBW[rank]))
					if err != nil {
						fmt.Printf("ERROR: unable to write output to %s: %s\n", outputFilePath, err)
						os.Exit(1)
					}
				}
				_, err = outputFile.WriteString("\n")
				if err != nil {
					fmt.Printf("ERROR: unable to write output to %s: %s\n", outputFilePath, err)
					os.Exit(1)
				}
			}
			fmt.Printf("Data successfully saved in %s\n", outputFilePath)
		}
	}
}
