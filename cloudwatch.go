package cloudwatch

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// Throttling and limits from http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/cloudwatch_limits.html
const (
	// The maximum rate of a GetLogEvents request is 10 requests per second per AWS account.
	readThrottle = time.Second / 10

	// The maximum rate of a PutLogEvents request is 5 requests per second per log stream.
	writeThrottle = time.Second / 5

	// maximum message size is 1048576, but we have some metadata
	maximumBatchSize = 1048576 / 2
)

// now is a function that returns the current time.Time. It's a variable so that
// it can be stubbed out in unit tests.
var now = time.Now

// client duck types the aws sdk client for testing.
type client interface {
	PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
	CreateLogStream(ctx context.Context, params *cloudwatchlogs.CreateLogStreamInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
	GetLogEvents(ctx context.Context, params *cloudwatchlogs.GetLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetLogEventsOutput, error)
	DescribeLogStreams(ctx context.Context, params *cloudwatchlogs.DescribeLogStreamsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
}

// Group wraps a log stream group and provides factory methods for creating
// readers and writers for streams.
type Group struct {
	group  string
	client *cloudwatchlogs.Client
}

// NewGroup returns a new Group instance.
func NewGroup(group string, client *cloudwatchlogs.Client) *Group {
	return &Group{
		group:  group,
		client: client,
	}
}

// Existing uses an existing log group created previously
func (g *Group) existing(ctx context.Context, stream string) (io.Writer, error) {
	result, err := g.client.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        &g.group,
		LogStreamNamePrefix: &stream,
		OrderBy:             types.OrderByLogStreamName,
		Descending:          aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	if len(result.LogStreams) == 0 {
		return nil, errors.New("Log stream not found " + stream)
	}

	// since values are sorted the stream with exact match will be first
	logStream := result.LogStreams[0]

	return NewWriterWithToken(g.group, stream, logStream.UploadSequenceToken, g.client), nil
}

// Create creates a log stream in the group and returns an io.Writer for it.
func (g *Group) Create(stream string) (io.Writer, error) {
	return g.CreateWithContext(context.Background(), stream)
}

// CreateWithContext creates a log stream in the group and returns an io.Writer for it.
func (g *Group) CreateWithContext(ctx context.Context, stream string) (io.Writer, error) {
	if _, err := g.client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  &g.group,
		LogStreamName: &stream,
	}); err != nil {
		var alreadyExists *types.ResourceAlreadyExistsException
		if errors.As(err, &alreadyExists) {
			return g.existing(ctx, stream)
		}
		return nil, err
	}

	return NewWriter(g.group, stream, g.client), nil
}

// Open returns an io.Reader to read from the log stream.
func (g *Group) Open(stream string) (io.Reader, error) {
	return NewReader(g.group, stream, g.client), nil
}
