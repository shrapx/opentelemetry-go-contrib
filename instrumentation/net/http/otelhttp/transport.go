// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
)

// Transport implements the http.RoundTripper interface and wraps
// outbound HTTP(S) requests with a span.
type Transport struct {
	rt http.RoundTripper

	tracer            trace.Tracer
	propagators       propagation.TextMapPropagator
	spanStartOptions  []trace.SpanOption
	filters           []Filter
	spanNameFormatter func(string, *http.Request) string

	okRate  *metric.Int64ValueRecorder
	latency *metric.Int64ValueRecorder
}

var _ http.RoundTripper = &Transport{}

// NewTransport wraps the provided http.RoundTripper with one that
// starts a span and injects the span context into the outbound request headers.
func NewTransport(base http.RoundTripper, opts ...Option) *Transport {
	t := Transport{
		rt: base,
	}

	defaultOpts := []Option{
		WithSpanOptions(trace.WithSpanKind(trace.SpanKindClient)),
		WithSpanNameFormatter(defaultTransportFormatter),
	}

	c := newConfig(append(defaultOpts, opts...)...)
	t.applyConfig(c)

	return &t
}

func (t *Transport) applyConfig(c *config) {
	t.tracer = c.Tracer
	t.propagators = c.Propagators
	t.spanStartOptions = c.SpanStartOptions
	t.filters = c.Filters
	t.spanNameFormatter = c.SpanNameFormatter
	if c.WithOkRate {
		t.okRate = &c.OkRate
	}
	if c.WithLatency {
		t.latency = &c.Latency
	}
}

func defaultTransportFormatter(_ string, r *http.Request) string {
	return r.Method
}

// RoundTrip creates a Span and propagates its context via the provided request's headers
// before handing the request to the configured base RoundTripper. The created span will
// end when the response body is closed or when a read from the body returns io.EOF.
func (t *Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	for _, f := range t.filters {
		if !f(r) {
			// Simply pass through to the base RoundTripper if a filter rejects the request
			return t.rt.RoundTrip(r)
		}
	}

	opts := append([]trace.SpanOption{}, t.spanStartOptions...) // start with the configured options

	ctx, span := t.tracer.Start(r.Context(), t.spanNameFormatter("", r), opts...)

	r = r.WithContext(ctx)
	span.SetAttributes(semconv.HTTPClientAttributesFromHTTPRequest(r)...)
	t.propagators.Inject(ctx, r.Header)

	var labels []label.KeyValue
	labels = append(labels, extractRequestLabels(r)...)
	timeStart := time.Now()

	res, err := t.rt.RoundTrip(r)

	labels = append(labels, extractResponseLabels(res)...)
	t.setMetricLatency(ctx, time.Now().Sub(timeStart), labels...)

	if err != nil {
		span.RecordError(err)
		span.End()
		t.setMetricOkRate(ctx, 0, labels...)

		return res, err
	}
	t.setMetricOkRate(ctx, 1, labels...)

	span.SetAttributes(semconv.HTTPAttributesFromHTTPStatusCode(res.StatusCode)...)
	span.SetStatus(semconv.SpanStatusFromHTTPStatusCode(res.StatusCode))
	res.Body = &wrappedBody{ctx: ctx, span: span, body: res.Body}

	return res, err
}

// supports 1st graphql operation
func extractRequestLabels(r *http.Request) []label.KeyValue {
	var host string
	var method string
	var path string
	var target string
	var graphqlName string
	var graphqlType string
	if r != nil {
		host = r.Host
		method = r.Method
		path = r.URL.Path
		target = r.Header.Get("X-Amz-Target")
		if bodyBytes, err := ioutil.ReadAll(r.Body); err == nil {
			_ = r.Body.Close()
			r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			if q, err := parser.ParseQuery(&ast.Source{Input: string(bodyBytes)}); err == nil {
				for _, op := range q.Operations {
					graphqlName = op.Name
					graphqlType = string(op.Operation) // Query / Mutation / Subscription
					break
				}
			}
		}
	}
	return []label.KeyValue{
		label.String(LabelHTTPHost, stringOrNone(host)),
		label.String(LabelHTTPMethod, stringOrNone(method)),
		label.String(LabelHTTPPath, stringOrNone(path)),
		label.String(LabelAMZTarget, stringOrNone(target)),
		label.String(LabelGQLName, stringOrNone(graphqlName)),
		label.String(LabelGQLType, stringOrNone(graphqlType)),
	}
}

func extractResponseLabels(r *http.Response) []label.KeyValue {
	var statusCode int
	var errorCode string
	if r != nil {
		statusCode = r.StatusCode
		if bodyBytes, err := ioutil.ReadAll(r.Body); err == nil {
			_ = r.Body.Close()
			r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			if v := getErrorCodeDynamoDB(bodyBytes); v != "" {
				errorCode = v
			} else if v := getErrorCodeAppSync(bodyBytes); v != "" {
				errorCode = v
			} else if v := getErrorCodeS3(bodyBytes); v != "" {
				errorCode = v
			}
		}
	}
	return []label.KeyValue{
		label.Int(LabelHTTPStatusCode, statusCode),
		label.String(LabelParsedErrorCode, stringOrNone(errorCode)),
	}
}

// getErrorCodeDynamoDB
// {
//  "__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
//  "message":"Requested resource not found"
// }
func getErrorCodeDynamoDB(body []byte) string {
	var dynamoError struct {
		Type string `json:"__type"`
	}
	if err := json.Unmarshal(body, &dynamoError); err == nil {
		if dynamoError.Type != "" {
			return dynamoError.Type
		}
	}
	return ""
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
				return appsyncError.Errors[0].ErrorType
			}
		}
	}
	return ""
}

// getErrorCodeS3
// <?xml version="1.0" encoding="UTF-8"?>\n
// <Error>
//   <Code>AccessDenied</Code>
//   <Message>Access Denied</Message>
//   <RequestId>ACD0C265B7996CBE</RequestId>
//   <HostId>lb9+EDiBO9RnEfi1JmlLNSLr7aB+/+Zd1+Gw9VRTPurWk5zJ7uBXZO298yvTnumMhAHEILlwhLY=</HostId>
// </Error>
func getErrorCodeS3(body []byte) string {
	var s3Error struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	if err := xml.Unmarshal(body, &s3Error); err == nil {
		if s3Error.Code != "" {
			return s3Error.Code
		}
	}
	return ""
}

// getErrorCodeGraphQL
func getErrorCodeGraphQL(body []byte) string {
	var j struct{
		Data json.RawMessage `json:"data"`
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

func (t *Transport) setMetricLatency(ctx context.Context, latency time.Duration, labels ...label.KeyValue) {
	if t.latency != nil {
		latencyMillis := latency.Milliseconds()
		t.latency.Record(ctx, latencyMillis, labels...)
	}
}

func (t *Transport) setMetricOkRate(ctx context.Context, ok int64, labels ...label.KeyValue) {
	if t.okRate != nil {
		t.okRate.Record(ctx, ok, labels...)
	}
}

func stringOrNone(v string) string {
	if v == "" {
		return "none"
	}
	return v
}

type wrappedBody struct {
	ctx  context.Context
	span trace.Span
	body io.ReadCloser
}

var _ io.ReadCloser = &wrappedBody{}

func (wb *wrappedBody) Read(b []byte) (int, error) {
	n, err := wb.body.Read(b)

	switch err {
	case nil:
		// nothing to do here but fall through to the return
	case io.EOF:
		wb.span.End()
	default:
		wb.span.RecordError(err)
	}
	return n, err
}

func (wb *wrappedBody) Close() error {
	wb.span.End()
	return wb.body.Close()
}
