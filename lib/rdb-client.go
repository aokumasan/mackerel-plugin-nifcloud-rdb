package mpnifcloudrdb

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
)

const (
	apiVersion = "2013-05-15N2013-12-16"
	service    = "rdb"
)

// RdbClient ...
type RdbClient struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
}

// NiftyGetMetricsStatisticsResponse ...
type NiftyGetMetricsStatisticsResponse struct {
	NiftyGetMetricStatisticsResult NiftyGetMetricsStatisticsResult `xml:"NiftyGetMetricStatisticsResult"`
	ResponseMetadata               ResponseMetadata                `xml:"ResponseMetadata"`
}

// NiftyGetMetricsStatisticsResult ...
type NiftyGetMetricsStatisticsResult struct {
	Datapoints []DataPoints `xml:"Datapoints"`
	Label      string       `xml:"Label"`
}

// ResponseMetadata ...
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// DataPoints ...
type DataPoints struct {
	Member []Member `xml:"member"`
}

// Member ...
type Member struct {
	NiftyTargetName string
	Timestamp       time.Time
	Sum             float64
	SampleCount     int
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

// NewRdbClient ...
func NewRdbClient(region, accessKeyID, secretAccessKey string) *RdbClient {
	return &RdbClient{
		Region:          region,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}
}

// Request ...
func (c *RdbClient) Request(action string, params map[string]string) ([]byte, error) {
	values := url.Values{}
	values.Set("Action", action)
	for k, v := range params {
		values.Set(k, v)
	}
	body := strings.NewReader(values.Encode())
	endpoint, err := getEndpointFromRegion(c.Region)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", endpoint, body)
	if err != nil {
		return nil, err
	}
	signer := v4.NewSigner(
		credentials.NewStaticCredentials(c.AccessKeyID, c.SecretAccessKey, ""),
	)
	_, err = signer.Sign(req, body, service, c.Region, time.Now())
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

// NiftyGetMetricStatistics ...
func (c *RdbClient) NiftyGetMetricStatistics(params map[string]string) (response NiftyGetMetricsStatisticsResponse, err error) {
	body, err := c.Request("NiftyGetMetricStatistics", params)
	if err != nil {
		return
	}
	err = xml.Unmarshal(body, &response)
	if err != nil {
		return
	}
	return
}
