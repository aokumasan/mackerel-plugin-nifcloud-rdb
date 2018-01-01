package mpnifcloudrdb

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	mp "github.com/mackerelio/go-mackerel-plugin"
)

// RDBPlugin mackerel plugin for NIFCLOUD RDB
type RDBPlugin struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Identifier      string
	Engine          string
	Prefix          string
	LabelPrefix     string
}

func getLastPoint(client *RdbClient, identifier, metricName string) (float64, error) {
	now := time.Now().In(time.UTC)
	const layout = "2006-01-02 15:04:05"
	params := make(map[string]string)
	params["Dimensions.member.1.Name"] = "DBInstanceIdentifier"
	params["Dimensions.member.1.Value"] = identifier
	params["EndTime"] = now.Format(layout)
	params["StartTime"] = now.Add(time.Duration(180) * time.Second * -1).Format(layout) // 3 min (to fetch at least 1 data-point)
	params["MetricName"] = metricName

	response, err := client.NiftyGetMetricStatistics(params)
	if err != nil {
		return 0, err
	}

	datapoints := response.NiftyGetMetricStatisticsResult.Datapoints[0].Member
	if len(datapoints) == 0 {
		return 0, errors.New("fetched no datapoints")
	}

	latest := new(time.Time)
	var latestVal float64
	for _, dp := range datapoints {
		if dp.Timestamp.Before(*latest) {
			continue
		}

		latest = &dp.Timestamp
		latestVal = float64(dp.Sum) / float64(dp.SampleCount)
	}
	return latestVal, nil
}

func getEndpointFromRegion(region string) (string, error) {
	var endpoints = map[string]string{
		"east-1": "https://rdb.jp-east-1.api.cloud.nifty.com/",
		"east-2": "https://rdb.jp-east-2.api.cloud.nifty.com/",
		"east-3": "https://rdb.jp-east-3.api.cloud.nifty.com/",
		"east-4": "https://rdb.jp-east-4.api.cloud.nifty.com/",
		"west-1": "https://rdb.jp-west-1.api.cloud.nifty.com/",
	}
	v, ok := endpoints[region]
	if !ok {
		return "", fmt.Errorf("An invalid region was specified")
	}
	return v, nil
}

func (p RDBPlugin) rdbMetrics() (metrics []string) {
	for _, v := range p.GraphDefinition() {
		for _, vv := range v.Metrics {
			metrics = append(metrics, vv.Name)
		}
	}
	return
}

// FetchMetrics interface for mackerel-plugin
func (p RDBPlugin) FetchMetrics() (map[string]float64, error) {
	endpoint, err := getEndpointFromRegion(p.Region)
	if err != nil {
		return nil, err
	}
	client := NewRdbClient(endpoint, p.AccessKeyID, p.SecretAccessKey)

	stat := make(map[string]float64)
	for _, met := range p.rdbMetrics() {
		v, err := getLastPoint(client, p.Identifier, met)
		if err == nil {
			stat[met] = v
		} else {
			log.Printf("%s: %s", met, err)
		}
	}
	return stat, nil
}

// GraphDefinition interface for mackerel plugin
func (p RDBPlugin) GraphDefinition() map[string]mp.Graphs {
	return map[string]mp.Graphs{
		p.Prefix + ".BinLogDiskUsage": {
			Label: p.LabelPrefix + " BinLogDiskUsage",
			Unit:  "bytes",
			Metrics: []mp.Metrics{
				{Name: "BinLogDiskUsage", Label: "Usage"},
			},
		},
		p.Prefix + ".CPUUtilization": {
			Label: p.LabelPrefix + " CPU Utilization",
			Unit:  "percentage",
			Metrics: []mp.Metrics{
				{Name: "CPUUtilization", Label: "CPUUtilization"},
			},
		},
		p.Prefix + ".DatabaseConnections": {
			Label: p.LabelPrefix + " Database Connections",
			Unit:  "float",
			Metrics: []mp.Metrics{
				{Name: "DatabaseConnections", Label: "DatabaseConnections"},
			},
		},
		p.Prefix + ".DiskQueueDepth": {
			Label: p.LabelPrefix + " DiskQueueDepth",
			Unit:  "bytes",
			Metrics: []mp.Metrics{
				{Name: "DiskQueueDepth", Label: "Depth"},
			},
		},
		p.Prefix + ".FreeableMemory": {
			Label: p.LabelPrefix + " Freeable Memory",
			Unit:  "bytes",
			Metrics: []mp.Metrics{
				{Name: "FreeableMemory", Label: "FreeableMemory"},
			},
		},
		p.Prefix + ".FreeStorageSpace": {
			Label: p.LabelPrefix + " Free Storage Space",
			Unit:  "bytes",
			Metrics: []mp.Metrics{
				{Name: "FreeStorageSpace", Label: "FreeStorageSpace"},
			},
		},
		p.Prefix + ".SwapUsage": {
			Label: p.LabelPrefix + " Swap Usage",
			Unit:  "bytes",
			Metrics: []mp.Metrics{
				{Name: "SwapUsage", Label: "SwapUsage"},
			},
		},
		p.Prefix + ".IOPS": {
			Label: p.LabelPrefix + " IOPS",
			Unit:  "iops",
			Metrics: []mp.Metrics{
				{Name: "ReadIOPS", Label: "Read"},
				{Name: "WriteIOPS", Label: "Write"},
			},
		},
		p.Prefix + ".Throughput": {
			Label: p.LabelPrefix + " Throughput",
			Unit:  "bytes/sec",
			Metrics: []mp.Metrics{
				{Name: "ReadThroughput", Label: "Read"},
				{Name: "WriteThroughput", Label: "Write"},
			},
		},
	}
}

func (p RDBPlugin) MetricKeyPrefix() string {
	if p.Prefix == "" {
		p.Prefix = "rdb"
	}
	return p.Prefix
}

func Do() {
	optRegion := flag.String("region", "", "Region")
	optAccessKeyID := flag.String("access-key-id", "", "Access Key ID")
	optSecretAccessKey := flag.String("secret-access-key", "", "Secret Access Key")
	optIdentifier := flag.String("identifier", "", "DB Instance Identifier")
	optPrefix := flag.String("metric-key-prefix", "rdb", "Metric key prefix")
	optLabelPrefix := flag.String("metric-label-prefix", "", "Metric Label prefix")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	flag.Parse()

	rdb := RDBPlugin{
		Prefix: *optPrefix,
	}
	if *optLabelPrefix == "" {
		if *optPrefix == "rdb" {
			rdb.LabelPrefix = "RDB"
		} else {
			rdb.LabelPrefix = strings.Title(*optPrefix)
		}
	} else {
		rdb.LabelPrefix = *optLabelPrefix
	}

	rdb.Region = *optRegion
	rdb.Identifier = *optIdentifier
	rdb.AccessKeyID = *optAccessKeyID
	rdb.SecretAccessKey = *optSecretAccessKey

	helper := mp.NewMackerelPlugin(rdb)
	helper.Tempfile = *optTempfile

	helper.Run()
}
