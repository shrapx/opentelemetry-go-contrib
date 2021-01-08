package otelgin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
	"go.opentelemetry.io/otel/label"
	otelmetric "go.opentelemetry.io/otel/metric"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	meterKey  = "otel-go-contrib-meter"
	meterName = "go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	labelHost   = "Host"
	labelType   = "Type"
	labelCode   = "Code"
	labelAction = "Action"
)

func setMetricLatency(ctx context.Context, metric *otelmetric.Int64ValueRecorder, latency time.Duration, labels ...label.KeyValue) {
	if metric == nil {
		return
	}
	metric.Record(ctx, latency.Milliseconds(), labels...)
}

func setMetricOkRate(ctx context.Context, metric *otelmetric.Int64ValueRecorder, ok int64, labels ...label.KeyValue) {
	if metric == nil {
		return
	}
	metric.Record(ctx, ok, labels...)
}

func stringOrNone(v string) string {
	if v == "" {
		return "none"
	}
	return v
}

func extractRequestLabels(r *http.Request) []label.KeyValue {
	var host string
	var Type string
	var action string

	if r != nil {
		host = r.Host
		Type = r.Method
		action = r.URL.Path
		if t := r.Header.Get("X-Amz-Target"); t != "" {
			action = t
		}
		if bodyBytes, err := ioutil.ReadAll(r.Body); err == nil {
			_ = r.Body.Close()
			r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			if q, err := parser.ParseQuery(&ast.Source{Input: string(bodyBytes)}); err == nil {
				for _, op := range q.Operations {
					action = op.Name
					Type = string(op.Operation)
					fmt.Println("extracted graphql name", action, "operation", Type)
					break
				}
			}
		}
	}
	return []label.KeyValue{
		label.String(labelHost, stringOrNone(host)),
		label.String(labelAction, stringOrNone(action)),
		label.String(labelType, stringOrNone(Type)),
	}
}

func extractResponseLabels(r *http.Response) []label.KeyValue {
	var code string
	if r != nil {
		code = fmt.Sprintf("%v", r.StatusCode)
		if bodyBytes, err := ioutil.ReadAll(r.Body); err == nil {
			_ = r.Body.Close()
			r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			if v := getErrorCodeAppSync(bodyBytes); v != "" {
				code = v
			}
		}
	}
	return []label.KeyValue{
		label.String(labelCode, stringOrNone(code)),
	}
}

// getErrorCodeAppSync
// supports 1st Error
func getErrorCodeAppSync(body []byte) string {
	var appsyncError struct {
		Errors []struct {
			ErrorType string `json:"errorType"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &appsyncError); err == nil {
		if len(appsyncError.Errors) != 0 {
			if appsyncError.Errors[0].ErrorType != "" {
				fmt.Println("debug got appsync errorType", appsyncError.Errors[0].ErrorType)
				return appsyncError.Errors[0].ErrorType
			}
		}
	} else {
		fmt.Println("debug appsync error response", err.Error())
	}
	return ""
}

// getErrorCodeGraphQL
func getErrorCodeGraphQL(body []byte) string {
	var j struct {
		Data   json.RawMessage  `json:"data"`
		Errors []gqlerror.Error `json:"errors"`
	}
	if err := json.Unmarshal(body, &j); err == nil {
		if len(j.Errors) != 0 {
			// what to return? https://spec.graphql.org/draft/#sec-Errors
			return j.Errors[0].Message
		}
	}
	return ""
}
