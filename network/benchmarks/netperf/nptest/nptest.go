/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
 nptest.go

 Dual-mode program - runs as both the orchestrator and as the worker nodes depending on command line flags
 The RPC API is contained wholly within this file.
*/

package main

// Imports only base Golang packages
import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type point struct {
	mss       int
	bandwidth string
	index     int
}

var mode string
var port string
var host string
var worker string
var kubenode string
var podname string

var workerStateMap map[string]*workerState

var iperfTCPOutputRegexp *regexp.Regexp
var iperfTCPOutputFloatRegexp *regexp.Regexp
var iperfSCTPOutputRegexp *regexp.Regexp
var iperfSCTPOutputFloatRegexp *regexp.Regexp
var iperfUDPOutputRegexp *regexp.Regexp
var netperfOutputRegexp *regexp.Regexp
var iperfCPUOutputRegexp *regexp.Regexp

var dataPoints map[string][]point
var dataPointKeys []string
var datapointsFlushed bool

var globalLock sync.Mutex

const (
	workerMode           = "worker"
	orchestratorMode     = "orchestrator"
	iperf3Path           = "/usr/local/bin/iperf3"
	netperfPath          = "/usr/local/bin/netperf"
	netperfServerPath    = "/usr/local/bin/netserver"
	outputCaptureFile    = "/tmp/output.txt"
	mssMin               = 96
	mssMax               = 1460
	parallelStreams      = 8
	rpcServicePort       = "5202"
	localhostIPv4Address = "127.0.0.1"
	netperfDataPort      = "12866" // necessary to match firewall port for "Virtual IP"
	testPodsPerGroup     = 3
	testInitialGroup     = 0
)

const (
	iperfTCPTest  = iota
	iperfUDPTest  = iota
	iperfSctpTest = iota
	netperfTest   = iota
)

// NetPerfRPC service that exposes RegisterClient and ReceiveOutput for clients
type NetPerfRPC int

// ClientRegistrationData stores a data about a single client
type ClientRegistrationData struct {
	Host     string
	KubeNode string
	Worker   string
	IP       string
}

// IperfClientWorkItem represents a single task for an Iperf client
type IperfClientWorkItem struct {
	Host string
	Port string
	MSS  int
	Type int
	Bw   int
}

// IperfServerWorkItem represents a single task for an Iperf server
type IperfServerWorkItem struct {
	ListenPort string
	Timeout    int
}

// WorkItem represents a single task for a worker
type WorkItem struct {
	IsClientItem bool
	IsServerItem bool
	IsIdle       bool
	ClientItem   IperfClientWorkItem
	ServerItem   IperfServerWorkItem
}

type workerState struct {
	sentServerItem bool
	idle           bool
	IP             string
	worker         string
}

// WorkerOutput stores the results from a single worker
type WorkerOutput struct {
	Output string
	Code   int
	Worker string
	Type   int
	MSS    int
}

type testcase struct {
	SourceNode      string
	DestinationNode string
	Label           string
	ClusterIP       bool
	Finished        bool
	MSS             int
	Type            int
	Reported        bool
}

var testcases = make(map[string][]*testcase)
var currentJobIndex = make(map[string]int)

var testGroups int
var testDuration int = 10
var testLogState bool = false
var mssStepSize int = 64
var testProto string = "all"
var testTool string = "all"
var testCooldown int = 10
var testFlowList = [5]string{"Same VM using Pod IP", "Same VM using Virtual IP", "Remote VM using Pod IP", "Remote VM using Virtual IP", "Hairpin Pod to own Virtual IP"}
var testTcpRateList string = ""
var testTcpRateMap = make(map[string]int)
var testSctpRateList string = ""
var testSctpRateMap = make(map[string]int)
var testUdpFlowList = [5]string{"Same VM using Pod IP", "Same VM using Virtual IP", "Remote VM using Pod IP", "Remote VM using Virtual IP"}
var testUdpRateList string = ""
var testUdpRateMap = make(map[string]int)

func init() {
	flag.StringVar(&mode, "mode", "worker", "Mode for the daemon (worker | orchestrator)")
	flag.StringVar(&port, "port", rpcServicePort, "Port to listen on (defaults to 5202)")
	flag.StringVar(&host, "host", "", "IP address to bind to (defaults to 0.0.0.0)")
	flag.IntVar(&testGroups, "groups", 1, "how many groups of {w1, w2, w3} pods the test must launch (for scalability tests)")
	flag.BoolVar(&testLogState, "log", false, "control log output")
	flag.IntVar(&testDuration, "duration", 10, "iperf3 test duration (in seconds)")
	flag.IntVar(&mssStepSize, "step", 64, "MSS step size (in bytes)")
	flag.StringVar(&testTool, "tool", "all", "select test tool (all, iperf or netperf)")
	flag.StringVar(&testProto, "proto", "all", "select iperf test protocol (all, tcp, sctp or udp)")
	flag.IntVar(&testCooldown, "cooldown", 10, "test cooldown time between test steps")
	flag.StringVar(&testTcpRateList, "tcprate", "", "Tx rate (in Mbps) to be applied on iperf TCP test for each of the 5 test flows, up to five comma separated values (e.g. 23,45,23,67,78) ")
	flag.StringVar(&testSctpRateList, "sctprate", "", "Tx rate (in Mbps) to be applied on iperf SCTP test for each of the 5 test flows, up to five comma separated values (e.g. 23,45,23,67,78) ")
	flag.StringVar(&testUdpRateList, "udprate", "", "Tx rate (in Mbps) to be applied on iperf UDP test for each of the 4 test flows, up to four comma separated values (e.g. 23,45,23,67) ")

	workerStateMap = make(map[string]*workerState)

	// Regexes to parse the Mbits/sec out of iperf TCP, SCTP, UDP and netperf output
	iperfTCPOutputRegexp = regexp.MustCompile(`SUM.*\s+(\d+)\sMbits\/sec\s+receiver`)
	iperfTCPOutputFloatRegexp = regexp.MustCompile(`SUM.*\s+(\d+)\.(\d+)\sMbits\/sec\s+receiver`)
	iperfSCTPOutputRegexp = regexp.MustCompile(`SUM.*\s+(\d+)\sMbits\/sec\s+receiver`)
	iperfSCTPOutputFloatRegexp = regexp.MustCompile(`SUM.*\s+(\d+)\.(\d+)\sMbits\/sec\s+receiver`)
	iperfUDPOutputRegexp = regexp.MustCompile("\\s+(\\S+)\\sMbits/sec\\s+\\S+\\s+ms\\s+")
	netperfOutputRegexp = regexp.MustCompile("\\s+\\d+\\s+\\d+\\s+\\d+\\s+\\S+\\s+(\\S+)\\s+")
	iperfCPUOutputRegexp = regexp.MustCompile(`local/sender\s(\d+\.\d+)%\s\((\d+\.\d+)%\w/(\d+\.\d+)%\w\),\sremote/receiver\s(\d+\.\d+)%\s\((\d+\.\d+)%\w/(\d+\.\d+)%\w\)`)

	dataPoints = make(map[string][]point)

}

func initOrchestrator() {
	log.Printf("====>>> init testcases testGroups=%d\n", testGroups)

	tcpRateSlice := strings.Split(testTcpRateList, ",")
	for i := 0; i < len(testFlowList); i++ {
		testTcpRateMap[testFlowList[i]] = 0
		if i < len(tcpRateSlice) {
			if rate, err := strconv.Atoi(tcpRateSlice[i]); err == nil {
				testTcpRateMap[testFlowList[i]] = rate
			}
		}
	}

	sctpRateSlice := strings.Split(testSctpRateList, ",")
	for i := 0; i < len(testFlowList); i++ {
		testSctpRateMap[testFlowList[i]] = 0
		if i < len(sctpRateSlice) {
			if rate, err := strconv.Atoi(sctpRateSlice[i]); err == nil {
				testSctpRateMap[testFlowList[i]] = rate
			}
		}
	}

	udpRateSlice := strings.Split(testUdpRateList, ",")
	for i := 0; i < len(testUdpFlowList); i++ {
		testUdpRateMap[testFlowList[i]] = 0
		if i < len(udpRateSlice) {
			if rate, err := strconv.Atoi(udpRateSlice[i]); err == nil {
				testUdpRateMap[testUdpFlowList[i]] = rate
			}
		}
	}

	for group := testInitialGroup; group < testGroups; group++ {
		key := fmt.Sprintf("%03d", group)

		if testTool == "all" || testTool == "iperf" {
			if testProto == "all" || testProto == "tcp" {
				test := []*testcase{
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "1 iperf TCP. Same VM using Pod IP", Type: iperfTCPTest, ClusterIP: false, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "2 iperf TCP. Same VM using Virtual IP", Type: iperfTCPTest, ClusterIP: true, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w3", group), Label: "3 iperf TCP. Remote VM using Pod IP", Type: iperfTCPTest, ClusterIP: false, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w3", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "4 iperf TCP. Remote VM using Virtual IP", Type: iperfTCPTest, ClusterIP: true, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w2", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "5 iperf TCP. Hairpin Pod to own Virtual IP", Type: iperfTCPTest, ClusterIP: true, MSS: mssMin},
				}
				testcases[key] = append(testcases[key], test...)
			}
			if testProto == "all" || testProto == "sctp" {
				test := []*testcase{
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "6 iperf SCTP. Same VM using Pod IP", Type: iperfSctpTest, ClusterIP: false, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "7 iperf SCTP. Same VM using Virtual IP", Type: iperfSctpTest, ClusterIP: true, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w3", group), Label: "8 iperf SCTP. Remote VM using Pod IP", Type: iperfSctpTest, ClusterIP: false, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w3", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "9 iperf SCTP. Remote VM using Virtual IP", Type: iperfSctpTest, ClusterIP: true, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w2", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "10 iperf SCTP. Hairpin Pod to own Virtual IP", Type: iperfSctpTest, ClusterIP: true, MSS: mssMin},
				}
				testcases[key] = append(testcases[key], test...)
			}
			if testProto == "all" || testProto == "udp" {
				test := []*testcase{
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "11 iperf UDP. Same VM using Pod IP", Type: iperfUDPTest, ClusterIP: false, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "12 iperf UDP. Same VM using Virtual IP", Type: iperfUDPTest, ClusterIP: true, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w3", group), Label: "13 iperf UDP. Remote VM using Pod IP", Type: iperfUDPTest, ClusterIP: false, MSS: mssMin},
					{SourceNode: fmt.Sprintf("g%03d-netperf-w3", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "14 iperf UDP. Remote VM using Virtual IP", Type: iperfUDPTest, ClusterIP: true, MSS: mssMin},
				}
				testcases[key] = append(testcases[key], test...)
			}
		}
		if testTool == "all" || testTool == "netperf" {
			test := []*testcase{
				{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "15 netperf. Same VM using Pod IP", Type: netperfTest, ClusterIP: false},
				{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "16 netperf. Same VM using Virtual IP", Type: netperfTest, ClusterIP: true},
				{SourceNode: fmt.Sprintf("g%03d-netperf-w1", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w3", group), Label: "17 netperf. Remote VM using Pod IP", Type: netperfTest, ClusterIP: false},
				{SourceNode: fmt.Sprintf("g%03d-netperf-w3", group), DestinationNode: fmt.Sprintf("g%03d-netperf-w2", group), Label: "18 netperf. Remote VM using Virtual IP", Type: netperfTest, ClusterIP: true},
			}
			testcases[key] = append(testcases[key], test...)
		}
		currentJobIndex[key] = 0
	}
	log.Printf("====>>> init testcases len=%d", len(testcases))
	if !testLogState {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

}

func initWorker() {
	if !testLogState {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
}

func initializeOutputFiles() {
	fd, err := os.OpenFile(outputCaptureFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Failed to open output capture file", err)
		os.Exit(2)
	}
	fd.Close()
}

func main() {
	initializeOutputFiles()
	flag.Parse()
	if !validateParams() {
		fmt.Println("Failed to parse cmdline args - fatal error - bailing out")
		os.Exit(1)

	}
	grabEnv()
	fmt.Println("Running as", mode, "...")
	fmt.Println("Number of groups : ", testGroups)
	log.Println("log state", testLogState)

	if mode == orchestratorMode {
		initOrchestrator()
		orchestrate()
	} else {
		initWorker()
		startWork()
	}
	fmt.Println("Terminating npd")
}

func grabEnv() {
	worker = os.Getenv("worker")
	kubenode = os.Getenv("kubenode")
	podname = os.Getenv("HOSTNAME")
}

func validateParams() (rv bool) {
	rv = true
	if mode != workerMode && mode != orchestratorMode {
		fmt.Println("Invalid mode", mode)
		return false
	}

	if len(port) == 0 {
		fmt.Println("Invalid port", port)
		return false
	}

	if (len(host)) == 0 {
		if mode == orchestratorMode {
			host = os.Getenv("NETPERF_ORCH_SERVICE_HOST")
		} else {
			host = os.Getenv("NETPERF_ORCH_SERVICE_HOST")
		}
	}
	return
}

func allWorkersIdle(group string) bool {
	for key, v := range workerStateMap {
		if string(key[1:4]) == group && !v.idle {
			return false
		}
	}
	return true
}

func getWorkerPodIP(worker string) string {
	return workerStateMap[worker].IP
}

func allocateWorkToClient(workerS *workerState, reply *WorkItem) {

	group := string(workerS.worker[1:4])

	if !allWorkersIdle(group) {
		log.Printf("return !allWorkersIdle(group)")
		reply.IsIdle = true
		return
	}

	// System is all idle - pick up next work item to allocate to client
	for n, v := range testcases[group] {
		if v.Finished {
			log.Printf("continue [%s] is finished", v.Label)
			continue
		}
		if v.SourceNode != workerS.worker {
			log.Printf("return source=%s != worker=%s", v.SourceNode, workerS.worker)
			reply.IsIdle = true
			return
		}
		if _, ok := workerStateMap[v.DestinationNode]; !ok {
			log.Printf("return dest=%s not found", v.DestinationNode)
			reply.IsIdle = true
			return
		}
		fmt.Printf("[%s] Requesting jobrun '%s' from %s to %s for MSS %d\n", time.Now().Format(time.StampMilli), v.Label, v.SourceNode, v.DestinationNode, v.MSS)
		reply.ClientItem.Type = v.Type
		reply.IsClientItem = true
		workerS.idle = false
		currentJobIndex[group] = n

		if !v.ClusterIP {
			reply.ClientItem.Host = getWorkerPodIP(v.DestinationNode)
		} else {
			reply.ClientItem.Host = os.Getenv(fmt.Sprintf("G%s_NETPERF_W2_SERVICE_HOST", group))
		}

		switch {
		case v.Type == iperfTCPTest || v.Type == iperfUDPTest || v.Type == iperfSctpTest:
			reply.ClientItem.Port = "5201"
			reply.ClientItem.MSS = v.MSS
			switch v.Type {
			case iperfTCPTest:
				for testFlow, bw := range testTcpRateMap {
					if strings.Contains(v.Label, testFlow) {
						reply.ClientItem.Bw = bw
					}
				}
			case iperfSctpTest:
				for testFlow, bw := range testSctpRateMap {
					if strings.Contains(v.Label, testFlow) {
						reply.ClientItem.Bw = bw
					}
				}
			case iperfUDPTest:
				for testFlow, bw := range testUdpRateMap {
					if strings.Contains(v.Label, testFlow) {
						reply.ClientItem.Bw = bw
					}
				}
			default:
				reply.ClientItem.Bw = 0
			}

			v.MSS = v.MSS + mssStepSize
			// alwys execute the final step of 1460
			if (v.MSS > mssMax) && ((v.MSS - mssMax) < mssStepSize) {
				v.MSS = mssMax
			}
			if (v.MSS > mssMax) && ((v.MSS - mssMax) >= mssStepSize) {
				v.Finished = true
			}
			log.Printf("return next v.MSS=%d ", v.MSS)
			return

		case v.Type == netperfTest:
			reply.ClientItem.Port = "12865"
			return
		}
	}
	for _, testcase := range testcases {
		for _, v := range testcase {
			if !v.Finished {
				return
			}
			if !v.Reported {
				return
			}
		}
	}

	if !datapointsFlushed {
		fmt.Println("ALL TESTCASES AND MSS RANGES COMPLETE - GENERATING CSV OUTPUT")
		flushDataPointsToCsv()
		datapointsFlushed = true
	}

	reply.IsIdle = true
}

var runOnce bool = true

// RegisterClient registers a single and assign a work item to it
func (t *NetPerfRPC) RegisterClient(data *ClientRegistrationData, reply *WorkItem) error {
	globalLock.Lock()
	defer globalLock.Unlock()
	state, ok := workerStateMap[data.Worker]
	//log.Printf("INCOMING Worker=%s, IP=%s, Host=%s, KubeNode=%s", data.Worker, data.IP, data.Host, data.KubeNode)

	if !ok {
		// For new clients, trigger an iperf server start immediately
		state = &workerState{sentServerItem: true, idle: true, IP: data.IP, worker: data.Worker}
		workerStateMap[data.Worker] = state
		reply.IsServerItem = true
		reply.ServerItem.ListenPort = "5201"
		reply.ServerItem.Timeout = 3600
		fmt.Printf("[%s] got %d/%d pods announced\n", time.Now().Format(time.StampMilli), len(workerStateMap), (testGroups * testPodsPerGroup))
		return nil
	}

	// Worker defaults to idle unless the allocateWork routine below assigns an item
	state.idle = true

	// only release worker pods after all announced
	if len(workerStateMap) < (testGroups * testPodsPerGroup) {
		reply.IsIdle = true
		return nil
	}

	if testGroups > 1 {
		if runOnce {
			wait := (testGroups * testPodsPerGroup) + 5
			fmt.Printf("[%s] sleep for %d seconds to have all pods locked waiting a response, to help test syncing\n", time.Now().Format(time.StampMilli), wait)
			time.Sleep(time.Duration(wait) * time.Second)
			fmt.Printf("finished sleep for %d\n", wait)
			runOnce = false
		}
	}

	// Give the worker a new work item or let it idle loop another 5 seconds
	allocateWorkToClient(state, reply)

	if !reply.IsIdle {
		if reply.IsClientItem {
			log.Printf("\tREPLY: ClientItem.Host=%s, ClientItem.MSS=%d, ClientItem.Port=%s", reply.ClientItem.Host, reply.ClientItem.MSS, reply.ClientItem.Port)
		}
		if reply.IsServerItem {
			log.Printf("\tREPLY: ServerItem.ListenPort=%s", reply.ServerItem.ListenPort)
		}
	}

	return nil
}

func writeOutputFile(filename, data string) {
	fd, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Failed to append to existing file", filename, err)
		return
	}
	defer fd.Close()

	if _, err = fd.WriteString(data); err != nil {
		fmt.Println("Failed to append to existing file", filename, err)
	}
}

func registerDataPoint(label string, mss int, value string, index int) {
	if sl, ok := dataPoints[label]; !ok {
		dataPoints[label] = []point{{mss: mss, bandwidth: value, index: index}}
		dataPointKeys = append(dataPointKeys, label)
	} else {
		dataPoints[label] = append(sl, point{mss: mss, bandwidth: value, index: index})
	}
}

func flushDataPointsToCsv() {
	var buffer string

	// Write the MSS points for the X-axis before dumping all the testcase datapoints
	for _, points := range dataPoints {
		if len(points) == 1 {
			continue
		}
		buffer = fmt.Sprintf("%-45s, Maximum,", "MSS")
		for _, p := range points {
			buffer = buffer + fmt.Sprintf(" %d,", p.mss)
		}
		break
	}
	fmt.Println(buffer)

	for _, label := range dataPointKeys {
		buffer = fmt.Sprintf("%-45s,", label)
		points := dataPoints[label]
		var max float64
		for _, p := range points {
			fv, _ := strconv.ParseFloat(p.bandwidth, 64)
			if fv > max {
				max = fv
			}
		}
		buffer = buffer + fmt.Sprintf("%f,", max)
		for _, p := range points {
			buffer = buffer + fmt.Sprintf("%s,", p.bandwidth)
		}
		fmt.Println(buffer)
	}
	fmt.Println("END CSV DATA")
}

func parseIperfTCPBandwidth(output string) string {
	// Parses the output of iperf3 and grabs the group Mbits/sec from the output
	match := iperfTCPOutputRegexp.FindStringSubmatch(output)
	if match == nil {
		match = iperfTCPOutputFloatRegexp.FindStringSubmatch(output)
	}
	if match != nil && len(match) > 1 {
		return match[1]
	}
	return "0"
}

func parseIperfSctpBandwidth(output string) string {
	// Parses the output of iperf3 and grabs the group Mbits/sec from the output
	match := iperfSCTPOutputRegexp.FindStringSubmatch(output)
	if match == nil {
		match = iperfSCTPOutputFloatRegexp.FindStringSubmatch(output)
	}
	if match != nil && len(match) > 1 {
		return match[1]
	}
	return "0"
}

func parseIperfUDPBandwidth(output string) string {
	// Parses the output of iperf3 (UDP mode) and grabs the Mbits/sec from the output
	match := iperfUDPOutputRegexp.FindStringSubmatch(output)
	if match != nil && len(match) > 1 {
		return match[1]
	}
	return "0"
}

func parseIperfCPUUsage(output string) (string, string) {
	// Parses the output of iperf and grabs the CPU usage on sender and receiver side from the output
	match := iperfCPUOutputRegexp.FindStringSubmatch(output)
	if match != nil && len(match) > 1 {
		return match[1], match[4]
	}
	return "0", "0"
}

func parseNetperfBandwidth(output string) string {
	// Parses the output of netperf and grabs the Bbits/sec from the output
	match := netperfOutputRegexp.FindStringSubmatch(output)
	if match != nil && len(match) > 1 {
		return match[1]
	}
	return "0"
}

// ReceiveOutput processes a data received from a single client
func (t *NetPerfRPC) ReceiveOutput(data *WorkerOutput, reply *int) error {
	globalLock.Lock()
	defer globalLock.Unlock()
	group := string(data.Worker[1:4])
	testcase := testcases[group][currentJobIndex[group]]

	var outputLog string
	var bw string
	var cpuSender string
	var cpuReceiver string

	switch data.Type {
	case iperfTCPTest:
		outputLog = outputLog + fmt.Sprintln("Received TCP output from worker", data.Worker, "for test", testcase.Label,
			"from", testcase.SourceNode, "to", testcase.DestinationNode, "MSS:", data.MSS) + data.Output
		writeOutputFile(outputCaptureFile, outputLog)
		bw = parseIperfTCPBandwidth(data.Output)
		cpuSender, cpuReceiver = parseIperfCPUUsage(data.Output)
		registerDataPoint(testcase.Label, data.MSS, bw, currentJobIndex[group])

	case iperfSctpTest:
		outputLog = outputLog + fmt.Sprintln("Received SCTP output from worker", data.Worker, "for test", testcase.Label,
			"from", testcase.SourceNode, "to", testcase.DestinationNode, "MSS:", data.MSS) + data.Output
		writeOutputFile(outputCaptureFile, outputLog)
		bw = parseIperfSctpBandwidth(data.Output)
		cpuSender, cpuReceiver = parseIperfCPUUsage(data.Output)
		registerDataPoint(testcase.Label, data.MSS, bw, currentJobIndex[group])

	case iperfUDPTest:
		outputLog = outputLog + fmt.Sprintln("Received UDP output from worker", data.Worker, "for test", testcase.Label,
			"from", testcase.SourceNode, "to", testcase.DestinationNode, "MSS:", data.MSS) + data.Output
		writeOutputFile(outputCaptureFile, outputLog)
		bw = parseIperfUDPBandwidth(data.Output)
		registerDataPoint(testcase.Label, data.MSS, bw, currentJobIndex[group])

	case netperfTest:
		outputLog = outputLog + fmt.Sprintln("Received netperf output from worker", data.Worker, "for test", testcase.Label,
			"from", testcase.SourceNode, "to", testcase.DestinationNode) + data.Output
		writeOutputFile(outputCaptureFile, outputLog)
		bw = parseNetperfBandwidth(data.Output)
		registerDataPoint(testcase.Label, 0, bw, currentJobIndex[group])
		testcases[group][currentJobIndex[group]].Finished = true

	}
	if testcases[group][currentJobIndex[group]].Finished {
		testcases[group][currentJobIndex[group]].Reported = true
	}

	switch data.Type {
	case iperfTCPTest, iperfSctpTest:
		fmt.Println(time.Now().Format(time.StampMilli), " GROUP=", group, "Jobdone from worker", data.Worker, "Bandwidth was", bw, "Mbits/sec. CPU usage sender was", cpuSender, "%. CPU usage receiver was", cpuReceiver, "%.")
	default:
		fmt.Println(time.Now().Format(time.StampMilli), " GROUP=", group, "Jobdone from worker", data.Worker, "Bandwidth was", bw, "Mbits/sec")
	}

	return nil
}

func serveRPCRequests(port string) {
	baseObject := new(NetPerfRPC)
	err := rpc.Register(baseObject)
	if err != nil {
		log.Fatal("failed to register rpc", err)
	}
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("failed start server", err)
	}
}

// Blocking RPC server start - only runs on the orchestrator
func orchestrate() {
	serveRPCRequests(rpcServicePort)
}

// Walk the list of interfaces and find the first interface that has a valid IP
// Inside a container, there should be only one IP-enabled interface
func getMyIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return localhostIPv4Address
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback == 0 {
			addrs, _ := iface.Addrs()
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				return ip.String()
			}
		}
	}
	return "127.0.0.1"
}

func handleClientWorkItem(client *rpc.Client, workItem *WorkItem) {
	fmt.Printf("Orchestrator requests worker run item Type:%d Bw=%d MSS=%d\n", workItem.ClientItem.Type, workItem.ClientItem.Bw, workItem.ClientItem.MSS)
	switch {
	case workItem.ClientItem.Type == iperfTCPTest || workItem.ClientItem.Type == iperfUDPTest || workItem.ClientItem.Type == iperfSctpTest:
		outputString := iperfClient(workItem.ClientItem.Host, workItem.ClientItem.Port, workItem.ClientItem.MSS, workItem.ClientItem.Type, workItem.ClientItem.Bw)
		var reply int
		err := client.Call("NetPerfRPC.ReceiveOutput", WorkerOutput{Output: outputString, Worker: worker, Type: workItem.ClientItem.Type, MSS: workItem.ClientItem.MSS}, &reply)
		if err != nil {
			log.Fatal("failed to call client", err)
		}
	case workItem.ClientItem.Type == netperfTest:
		outputString := netperfClient(workItem.ClientItem.Host, workItem.ClientItem.Port, workItem.ClientItem.Type)
		var reply int
		err := client.Call("NetPerfRPC.ReceiveOutput", WorkerOutput{Output: outputString, Worker: worker, Type: workItem.ClientItem.Type}, &reply)
		if err != nil {
			log.Fatal("failed to call client", err)
		}
	}
	// Client COOLDOWN period before asking for next work item to replenish burst allowance policers etc
	time.Sleep(time.Duration(testCooldown) * time.Second)
}

// isIPv6: Determines if an address is an IPv6 address
func isIPv6(address string) bool {
	x := net.ParseIP(address)
	return x != nil && x.To4() == nil && x.To16() != nil
}

// startWork : Entry point to the worker infinite loop
func startWork() {
	for {
		var timeout time.Duration
		var client *rpc.Client
		var err error

		is_ipv6 := isIPv6(getMyIP())
		address := host
		if isIPv6(address) {
			address = "[" + address + "]"
		}

		timeout = 5
		for {
			fmt.Println("Attempting to connect to orchestrator at", host)
			client, err = rpc.DialHTTP("tcp", address+":"+port)
			if err == nil {
				break
			}
			fmt.Println("RPC connection to ", host, " failed:", err)
			time.Sleep(timeout * time.Second)
		}

		for {
			clientData := ClientRegistrationData{Host: podname, KubeNode: kubenode, Worker: worker, IP: getMyIP()}
			var workItem WorkItem

			if err := client.Call("NetPerfRPC.RegisterClient", clientData, &workItem); err != nil {
				// RPC server has probably gone away - attempt to reconnect
				fmt.Println("Error attempting RPC call", err)
				break
			}

			switch {
			case workItem.IsIdle == true:
				time.Sleep(5 * time.Second)
				continue

			case workItem.IsServerItem == true:
				fmt.Println("Orchestrator requests worker run iperf and netperf servers")
				go iperfServer(is_ipv6)
				go netperfServer()
				time.Sleep(1 * time.Second)

			case workItem.IsClientItem == true:
				handleClientWorkItem(client, &workItem)
			}
		}
	}
}

// Invoke and indefinitely run an iperf server
func iperfServer(is_ipv6 bool) {
	protocol := "-4"
	if is_ipv6 {
		protocol = "-6"
	}
	output, success := cmdExec(iperf3Path, []string{iperf3Path, "-s", host, "-J", protocol, "-i", "60"}, 15)
	if success {
		fmt.Println(output)
	}
}

// Invoke and indefinitely run netperf server
func netperfServer() {
	output, success := cmdExec(netperfServerPath, []string{netperfServerPath, "-D"}, 15)
	if success {
		fmt.Println(output)
	}
}

// Invoke and run an iperf client and return the output if successful.
func iperfClient(serverHost, serverPort string, mss int, workItemType int, bw int) (rv string) {
	iperfTesDuration := fmt.Sprintf("%d", testDuration)
	switch {
	case workItemType == iperfTCPTest:
		args := []string{iperf3Path, "-c", serverHost, "-V", "-N", "-i", "30", "-t", iperfTesDuration, "-f", "m", "-w", "512M", "-Z", "-P", strconv.Itoa(parallelStreams), "-M", strconv.Itoa(mss)}
		if bw > 0 {
			streamBw := bw / parallelStreams
			bwArg := []string{"-b", fmt.Sprintf("%dM", streamBw)}
			args = append(args, bwArg...)
		}
		output, success := cmdExec(iperf3Path, args, 15)
		if success {
			rv = output
		}

	case workItemType == iperfSctpTest:
		args := []string{iperf3Path, "-c", serverHost, "-V", "-N", "-i", "30", "-t", iperfTesDuration, "-f", "m", "-w", "512M", "-Z", "-P", strconv.Itoa(parallelStreams), "-M", strconv.Itoa(mss), "--sctp"}
		if bw > 0 {
			streamBw := bw / parallelStreams
			bwArg := []string{"-b", fmt.Sprintf("%dM", streamBw)}
			args = append(args, bwArg...)
		}
		output, success := cmdExec(iperf3Path, args, 15)
		if success {
			rv = output
		}

	case workItemType == iperfUDPTest:
		args := []string{iperf3Path, "-c", serverHost, "-i", "30", "-t", iperfTesDuration, "-f", "m", "-b", "0", "-u", "-l", strconv.Itoa(mss)}
		if bw > 0 {
			bwArg := []string{"-b", fmt.Sprintf("%dM", bw)}
			args = append(args, bwArg...)
		}
		output, success := cmdExec(iperf3Path, args, 15)
		if success {
			rv = output
		}
	}
	return
}

// Invoke and run a netperf client and return the output if successful.
func netperfClient(serverHost, serverPort string, workItemType int) (rv string) {
	output, success := cmdExec(netperfPath, []string{netperfPath, "-H", serverHost, "--", "-P", netperfDataPort}, 15)
	if success {
		fmt.Println(output)
		rv = output
	} else {
		fmt.Println("Error running netperf client", output)
	}

	return
}

func cmdExec(command string, args []string, timeout int32) (rv string, rc bool) {
	cmd := exec.Cmd{Path: command, Args: args}
	fmt.Println("===> Exec:", cmd.String())

	var stdoutput bytes.Buffer
	var stderror bytes.Buffer
	cmd.Stdout = &stdoutput
	cmd.Stderr = &stderror
	if err := cmd.Run(); err != nil {
		outputstr := stdoutput.String()
		errstr := stderror.String()
		fmt.Println("Failed to run", outputstr, "error:", errstr, err)
		return
	}

	rv = stdoutput.String()
	fmt.Println(">>===> Result:\n", rv)
	rc = true
	return
}
