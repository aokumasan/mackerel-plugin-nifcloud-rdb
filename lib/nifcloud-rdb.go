package mpnifcloudrdb

import (
	"errors"
	"flag"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alice02/nifcloud-sdk-go/nifcloud"
	"github.com/alice02/nifcloud-sdk-go/nifcloud/credentials"
	"github.com/alice02/nifcloud-sdk-go/nifcloud/session"
	"github.com/alice02/nifcloud-sdk-go/service/rdb"
	mp "github.com/mackerelio/go-mackerel-plugin"
)

const layout = "2006-01-02T15:04:05Z"

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

func getLastPoint(client *rdb.Rdb, dimension *rdb.RequestDimensionsStruct, metricName string) (float64, error) {
	now := time.Now().In(time.UTC)

	response, err := client.NiftyGetMetricStatistics(&rdb.NiftyGetMetricStatisticsInput{
		Dimensions: []*rdb.RequestDimensionsStruct{dimension},
		StartTime:  nifcloud.Time(now.Add(time.Duration(180) * time.Second * -1)), // 3 min (to fetch at least 1 data-point)
		EndTime:    nifcloud.Time(now),
		MetricName: nifcloud.String(metricName),
	})
	if err != nil {
		return 0, err
	}

	datapoints := response.Datapoints
	if len(datapoints) == 0 {
		return 0, errors.New("fetched no datapoints")
	}

	latest := new(time.Time)
	var latestVal float64
	for _, dp := range datapoints {
		ts, _ := time.Parse(layout, *dp.Timestamp)
		if ts.Before(*latest) {
			continue
		}

		latest = &ts
		sum, _ := strconv.ParseFloat(*dp.Sum, 64)
		count, _ := strconv.ParseFloat(*dp.SampleCount, 64)
		latestVal = sum / count
	}

	return latestVal, nil
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
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	config := nifcloud.NewConfig()
	if p.AccessKeyID != "" && p.SecretAccessKey != "" {
		config = config.WithCredentials(credentials.NewStaticCredentials(p.AccessKeyID, p.SecretAccessKey, ""))
	}
	if p.Region != "" {
		config = config.WithRegion(p.Region)
	}

	client := rdb.New(sess, config)

	stat := make(map[string]float64)

	perInstance := &rdb.RequestDimensionsStruct{
		Name:  nifcloud.String("DBInstanceIdentifier"),
		Value: nifcloud.String(p.Identifier),
	}

	var wg sync.WaitGroup
	for _, met := range p.rdbMetrics() {
		wg.Add(1)
		go func(met string) {
			defer wg.Done()
			v, err := getLastPoint(client, perInstance, met)
			if err == nil {
				stat[met] = v
			} else {
				log.Printf("%s: %s", met, err)
			}
		}(met)
	}
	wg.Wait()

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
