//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/gvallee/go_collective_profiler/internal/pkg/bins"
	"github.com/gvallee/go_collective_profiler/pkg/comm"
	"github.com/gvallee/go_collective_profiler/pkg/counts"
	"github.com/gvallee/go_util/pkg/util"
)

func getBinsFromCountFile(inputFile string, jobId int, commId int, leadRank int, listBins []int, outputDir string) error {
	if inputFile == "" {
		return fmt.Errorf("undefined input file")
	}
	b, err := bins.GetFromFile(inputFile, listBins)
	if err != nil {
		return fmt.Errorf("unable to get bins: %w", err)
	}

	err = bins.Save(outputDir, jobId, commId, leadRank, b)
	if err != nil {
		return fmt.Errorf("unable to save data in %s: %w", outputDir, err)
	}
	return nil
}

func main() {
	verbose := flag.Bool("v", false, "Enable verbose mode")
	inputDirFlag := flag.String("input", "", "Input directory where all the data from the profiler is stored")
	binThresholds := flag.String("bins", "200", "Comma-separated list of thresholds to use for the creation of bins")
	outputDirFlag := flag.String("output", "", "Output directory")
	profilerPathFlag := flag.String("profiler-src-dir", "", "When using the tools separately, path to directory where the profiler source is (optional)")
	help := flag.Bool("h", false, "Help message")

	flag.Parse()

	cmdName := filepath.Base(os.Args[0])
	if *help {
		fmt.Printf("%s analyzes a given count file and classifying all the counts into bins", cmdName)
		fmt.Println("\nUsage:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	logFile := util.OpenLogFile("go_collective_profiler", cmdName)
	defer logFile.Close()
	if *verbose {
		nultiWriters := io.MultiWriter(os.Stdout, logFile)
		log.SetOutput(nultiWriters)
	} else {
		log.SetOutput(ioutil.Discard)
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
		os.Exit(1)
	}

	// Find all count files
	for leadRank, commIds := range commData.LeadMap {
		for _, commId := range commIds {
			listBins := bins.GetFromInputDescr(*binThresholds)
			jobId, err := counts.GetJobIdFromLeadRank(*inputDirFlag, leadRank)
			if err != nil {
				fmt.Printf("ERROR: counts.GetJobIdFromLeadRank() failed: %s\n", err)
				os.Exit(1)
			}
			log.Printf("Ready to create %d bins\n", len(listBins))

			countFilePath := counts.GetSendCountFile(jobId, leadRank)
			countFilePath = filepath.Join(*inputDirFlag, countFilePath)
			err = getBinsFromCountFile(countFilePath, jobId, commId, leadRank, listBins, *outputDirFlag)
			if err != nil {
				fmt.Printf("ERROR: getBinsFromCountFile() failed: %s\n", err)
				os.Exit(1)
			}
		}
	}
}
