package mpnifcloudrdb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const apiVersion = "2013-05-15N2013-12-16"

// RdbClient ...
type RdbClient struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}

// Auth ...
type Auth struct {
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

var unreserved = make([]bool, 128)
var hex = "0123456789ABCDEF"
var b64 = base64.StdEncoding

func init() {
	// RFC3986
	u := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890-_.~"
	for _, c := range u {
		unreserved[c] = true
	}
}

func encode(s string) string {
	encode := false
	for i := 0; i != len(s); i++ {
		c := s[i]
		if c > 127 || !unreserved[c] {
			encode = true
			break
		}
	}
	if !encode {
		return s
	}
	e := make([]byte, len(s)*3)
	ei := 0
	for i := 0; i != len(s); i++ {
		c := s[i]
		if c > 127 || !unreserved[c] {
			e[ei] = '%'
			e[ei+1] = hex[c>>4]
			e[ei+2] = hex[c&0xF]
			ei += 3
		} else {
			e[ei] = c
			ei++
		}
	}
	return string(e[:ei])
}

func sign(auth Auth, method, path string, params map[string]string, host string) {
	params["AccessKeyId"] = auth.AccessKeyID
	params["SignatureVersion"] = "2"
	params["SignatureMethod"] = "HmacSHA256"

	var sarray []string
	for k, v := range params {
		sarray = append(sarray, encode(k)+"="+encode(v))
	}
	sort.StringSlice(sarray).Sort()
	joined := strings.Join(sarray, "&")
	payload := method + "\n" + host + "\n" + path + "\n" + joined
	hash := hmac.New(sha256.New, []byte(auth.SecretAccessKey))
	hash.Write([]byte(payload))
	signature := make([]byte, b64.EncodedLen(hash.Size()))
	b64.Encode(signature, hash.Sum(nil))

	params["Signature"] = string(signature)
}

func multimap(p map[string]string) url.Values {
	q := make(url.Values, len(p))
	for k, v := range p {
		q[k] = []string{v}
	}
	return q
}

// NewRdbClient ...
func NewRdbClient(endpoint, accessKeyID, secretAccessKey string) *RdbClient {
	return &RdbClient{
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}
}

// Request ...
func (c *RdbClient) Request(action string, params map[string]string) ([]byte, error) {
	params["Action"] = action
	params["Version"] = apiVersion
	params["Timestamp"] = time.Now().In(time.UTC).Format(time.RFC3339)

	var auth Auth
	if c.AccessKeyID != "" && c.SecretAccessKey != "" {
		auth = Auth{
			AccessKeyID:     c.AccessKeyID,
			SecretAccessKey: c.SecretAccessKey,
		}
	}
	endpoint, _ := url.Parse(c.Endpoint)
	sign(auth, "GET", "/", params, endpoint.Host)
	endpoint.RawQuery = multimap(params).Encode()
	resp, err := http.Get(endpoint.String())
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
