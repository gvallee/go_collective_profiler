//
// Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
//
// See LICENSE.txt for license information
//

package bandwidth

import (
	"fmt"

	"github.com/gvallee/go_collective_profiler/internal/pkg/scale"
)

type CallData struct {
	// Amount of data sent per rank
	SendData map[int]int

	// Amount of data received per rank
	RecvData map[int]int

	// Send bandwidth per rank
	SendRankBW map[int]float64

	// Recv bandwidth per rank
	RecvRankBW map[int]float64

	ScaledSendRankBW map[int]float64

	ScaledSendRankBWUnit string

	ScaledRecvRankBW map[int]float64

	ScaledRecvRankBWUnit string
}

type CallsData struct {
	CallData map[int]*CallData
}

// Calculate the bandwidth based on the count data for one call.
// The input is assumed to be the data per rank.
func GetFromCallCounts(ranks int, sendCounts map[int]int, recvCounts map[int]int, sendDatatypeSize int, recvDatatypeSize int, execTimes map[int]float64) (*CallData, error) {
	d := new(CallData)
	d.SendData = make(map[int]int)
	d.RecvData = make(map[int]int)
	d.SendRankBW = make(map[int]float64)
	d.RecvRankBW = make(map[int]float64)
	d.ScaledSendRankBW = make(map[int]float64)
	d.ScaledRecvRankBW = make(map[int]float64)

	for rank := 0; rank < ranks; rank++ {
		var err error
		var scaledSendBW []float64
		var scaledRecvBW []float64
		d.SendRankBW[rank] = float64(sendCounts[rank]) / execTimes[rank]
		d.RecvRankBW[rank] = float64(recvCounts[rank]) / execTimes[rank]

		// Get the bandwidth in a more readable unit
		if d.SendRankBW[rank] != 0 {
			d.ScaledSendRankBWUnit, scaledSendBW, err = scale.Float64s("B/s", []float64{d.SendRankBW[rank]})
			if err != nil {
				return nil, err
			}
			d.ScaledSendRankBW[rank] = scaledSendBW[0]
		} else {
			d.ScaledSendRankBW[rank] = d.SendRankBW[rank]
			d.ScaledSendRankBWUnit = "B/s"
		}

		if d.RecvRankBW[rank] != 0 {
			d.ScaledRecvRankBWUnit, scaledRecvBW, err = scale.Float64s("B/s", []float64{d.RecvRankBW[rank]})
			if err != nil {
				return nil, err
			}
			d.ScaledRecvRankBW[rank] = scaledRecvBW[0]
		} else {
			d.ScaledRecvRankBW[rank] = d.RecvRankBW[rank]
			d.ScaledRecvRankBWUnit = "B/s"
		}
	}
	return d, nil
}

// GetFromCallsCounts calculates the bandwidth for all the collective operation calls
// The function expects the following input:
// numCalls: the total number of calls;
// ranks: the total number of ranks;
// sendCounts: all the send counts, for the outer map the keys are the call IDs, for the inner map, the keys are the rank and the value the total number of counts;
// recvCounts: all the receive counts, for the outer map the keys are the call IDs, for the inner map, the keys are the rank and the value the total number of counts;
// sendDatatypeSize: the send datatype size;
// recvDatatypeSize: the recv datatype size;
// execTimes: all the execution times per call and per rank, for the outer map, the keys are the call IDs, for the inner map, the keys are the ranks and the value the execution time.
// The function return a structure storing all the bandwidths.
func GetFromCallsCounts(numCalls int, ranks int, sendCounts map[int]map[int]int, recvCounts map[int]map[int]int, sendDatatypeSize int, recvDatatypeSize int, execTimes map[int]map[int]float64) (*CallsData, error) {
	var err error
	d := new(CallsData)
	d.CallData = make(map[int]*CallData)
	for call := 0; call < numCalls; call++ {
		d.CallData[call], err = GetFromCallCounts(ranks, sendCounts[call], recvCounts[call], sendDatatypeSize, recvDatatypeSize, execTimes[call])
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

func GetOutputFilename(leadRank int) string {
	return fmt.Sprintf("bandwidth-percall-comm%d.md", leadRank)
}
