package sqs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	"mailbaby/internal/queue/driver/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func init() {
	queue.Register(config.DriverSQS, New)
}

// SQSQueue implements queue.Queue for AWS SQS.
type SQSQueue struct {
	cfg    *config.Config
	sCfg   config.SQSConfig
	client *sqs.Client
	closed bool
	mu     sync.RWMutex
	common.BaseStats
}

// New creates and initializes a new AWS SQS Queue instance.
func New(cfg *config.Config) (queue.Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", queue.ErrInvalidConfig)
	}

	sCfg := cfg.Queue.SQS
	if err := sCfg.Validate(); err != nil {
		return nil, fmt.Errorf("sqs: config validation failed: %w", err)
	}

	var optFns []func(*awsconfig.LoadOptions) error
	if sCfg.Region != "" {
		optFns = append(optFns, awsconfig.WithRegion(sCfg.Region))
	}
	if sCfg.AccessKeyID != "" && sCfg.SecretAccessKey != "" {
		creds := credentials.NewStaticCredentialsProvider(sCfg.AccessKeyID, sCfg.SecretAccessKey, sCfg.SessionToken)
		optFns = append(optFns, awsconfig.WithCredentialsProvider(creds))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		return nil, fmt.Errorf("sqs: load aws config failed: %w", err)
	}

	var sqsOptFns []func(*sqs.Options)
	if sCfg.Endpoint != "" {
		sqsOptFns = append(sqsOptFns, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(sCfg.Endpoint)
		})
	}

	client := sqs.NewFromConfig(awsCfg, sqsOptFns...)

	return &SQSQueue{
		cfg:    cfg,
		sCfg:   sCfg,
		client: client,
	}, nil
}

// Driver returns DriverSQS.
func (q *SQSQueue) Driver() config.QueueDriver {
	return config.DriverSQS
}

// Name returns the configured SQS queue URL.
func (q *SQSQueue) Name() string {
	return q.sCfg.QueueURL
}

// Producer creates a new Producer client.
func (q *SQSQueue) Producer() (queue.Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &sqsProducer{q: q}, nil
}

// Consumer creates a new Consumer client.
func (q *SQSQueue) Consumer() (queue.Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &sqsConsumer{q: q}, nil
}

// Ping checks if the SQS queue is reachable.
func (q *SQSQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.ErrQueueClosed
	}

	_, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.sCfg.QueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	return err
}

// Stats returns SQS approximate queue attributes.
func (q *SQSQueue) Stats(ctx context.Context) (queue.Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.Stats{}, queue.ErrQueueClosed
	}

	res, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.sCfg.QueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})
	if err != nil {
		return queue.Stats{
			Driver:    config.DriverSQS,
			Name:      q.sCfg.QueueURL,
			InFlight:  q.InFlight,
			Total:     q.TotalSent,
			Consumers: int(q.ActiveCons),
		}, nil
	}

	var ready, inFlight, delayed int64
	if val, ok := res.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]; ok {
		ready, _ = strconv.ParseInt(val, 10, 64)
	}
	if val, ok := res.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]; ok {
		inFlight, _ = strconv.ParseInt(val, 10, 64)
	}
	if val, ok := res.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)]; ok {
		delayed, _ = strconv.ParseInt(val, 10, 64)
	}

	return queue.Stats{
		Driver:    config.DriverSQS,
		Name:      q.sCfg.QueueURL,
		Ready:     ready,
		InFlight:  inFlight,
		Delayed:   delayed,
		Total:     q.TotalSent,
		Consumers: int(q.ActiveCons),
		Extra: map[string]any{
			"region": q.sCfg.Region,
		},
	}, nil
}

// Close marks the queue closed.
func (q *SQSQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
}

type sqsProducer struct {
	q *SQSQueue
}

func (p *sqsProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if msg == nil {
		return queue.ErrInvalidMessage
	}

	var po queue.PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	queueURL := p.q.sCfg.QueueURL
	if po.Topic != "" {
		queueURL = po.Topic
	}

	attrs := make(map[string]types.MessageAttributeValue)
	for k, v := range msg.Headers {
		attrs[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	for k, v := range po.Headers {
		attrs[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	if msg.ID != "" {
		attrs["MessageId"] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(msg.ID),
		}
	}

	delaySeconds := int32(0)
	if po.Delay > 0 {
		delaySeconds = int32(po.Delay.Seconds())
	} else if msg.Delay > 0 {
		delaySeconds = int32(msg.Delay.Seconds())
	}

	input := &sqs.SendMessageInput{
		QueueUrl:          aws.String(queueURL),
		MessageBody:       aws.String(string(msg.Payload)),
		MessageAttributes: attrs,
		DelaySeconds:      delaySeconds,
	}

	if po.Key != "" {
		input.MessageGroupId = aws.String(po.Key)
	} else if msg.Key != "" {
		input.MessageGroupId = aws.String(msg.Key)
	}

	_, err := p.q.client.SendMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	p.q.IncTotalSent(1)
	return nil
}

func (p *sqsProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (p *sqsProducer) Close() error {
	return nil
}

type sqsConsumer struct {
	q *SQSQueue
}

func (c *sqsConsumer) Consume(ctx context.Context, handler queue.Handler, opts ...queue.ConsumeOption) error {
	if handler == nil {
		return queue.ErrNilHandler
	}

	var co queue.ConsumeOptions
	for _, opt := range opts {
		opt(&co)
	}

	queueURL := c.q.sCfg.QueueURL
	if co.Topic != "" {
		queueURL = co.Topic
	}

	concurrency := 1
	if co.Concurrency > 0 {
		concurrency = co.Concurrency
	} else if c.q.cfg != nil && c.q.cfg.Queue.Concurrency > 0 {
		concurrency = c.q.cfg.Queue.Concurrency
	}

	waitTime := int32(20)
	if c.q.sCfg.WaitTimeSeconds > 0 {
		waitTime = c.q.sCfg.WaitTimeSeconds
	}

	visTimeout := int32(30)
	if c.q.sCfg.VisibilityTimeout > 0 {
		visTimeout = c.q.sCfg.VisibilityTimeout
	}

	maxMsgs := int32(10)
	if c.q.sCfg.MaxNumberOfMessages > 0 {
		maxMsgs = c.q.sCfg.MaxNumberOfMessages
	}
	if co.BatchSize > 0 && int32(co.BatchSize) < maxMsgs {
		maxMsgs = int32(co.BatchSize)
	}
	if maxMsgs > 10 {
		maxMsgs = 10 // SQS hard limit
	}

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = queue.Chain(co.Middlewares...)(handler)
	}

	c.q.IncActiveCons(concurrency)
	defer c.q.DecActiveCons(concurrency)

	backoff := common.NewBackoff(200*time.Millisecond, 5*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				output, err := c.q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:              aws.String(queueURL),
					MaxNumberOfMessages:   maxMsgs,
					WaitTimeSeconds:       waitTime,
					VisibilityTimeout:     visTimeout,
					MessageAttributeNames: []string{"All"},
				})

				if err != nil {
					if ctx.Err() != nil {
						return
					}
					if !backoff.Wait(ctx) {
						return
					}
					continue
				}

				for _, sqsMsg := range output.Messages {
					c.processMessage(ctx, queueURL, sqsMsg, finalHandler, co)
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

func (c *sqsConsumer) processMessage(ctx context.Context, queueURL string, sqsMsg types.Message, handler queue.Handler, co queue.ConsumeOptions) {
	c.q.IncInFlight()
	defer c.q.DecInFlight()

	headers := make(map[string]string)
	for k, v := range sqsMsg.MessageAttributes {
		if v.StringValue != nil {
			headers[k] = *v.StringValue
		}
	}

	msgID := ""
	if sqsMsg.MessageId != nil {
		msgID = *sqsMsg.MessageId
	}
	payload := ""
	if sqsMsg.Body != nil {
		payload = *sqsMsg.Body
	}

	qMsg := &queue.Message{
		ID:        msgID,
		Topic:     queueURL,
		Payload:   []byte(payload),
		Headers:   headers,
		Timestamp: time.Now(),
		Attempts:  1,
		Raw:       sqsMsg,
	}

	receiptHandle := ""
	if sqsMsg.ReceiptHandle != nil {
		receiptHandle = *sqsMsg.ReceiptHandle
	}

	qMsg.SetAckFunc(func(cctx context.Context) error {
		if receiptHandle == "" {
			return nil
		}
		_, err := c.q.client.DeleteMessage(cctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: aws.String(receiptHandle),
		})
		return err
	})

	qMsg.SetNackFunc(func(cctx context.Context, requeue bool) error {
		if receiptHandle == "" {
			return nil
		}
		if requeue {
			_, err := c.q.client.ChangeMessageVisibility(cctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          aws.String(queueURL),
				ReceiptHandle:     aws.String(receiptHandle),
				VisibilityTimeout: 0,
			})
			return err
		}
		return nil
	})

	if co.AutoAck {
		_ = qMsg.Ack(ctx)
	}

	err := handler(ctx, qMsg)
	if err == nil && !qMsg.IsAcknowledged() {
		_ = qMsg.Ack(ctx)
	} else if err != nil && !qMsg.IsAcknowledged() {
		// Retries are handled inside the runtime engine; delete the message so
		// it does not become immediately visible again after failure.
		_ = qMsg.Ack(ctx)
	}
}

func (c *sqsConsumer) Receive(ctx context.Context, opts ...queue.ReceiveOption) (*queue.Message, error) {
	var ro queue.ReceiveOptions
	for _, opt := range opts {
		opt(&ro)
	}

	timeout := int32(5)
	if ro.Timeout > 0 {
		timeout = int32(ro.Timeout.Seconds())
	}

	output, err := c.q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.q.sCfg.QueueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     timeout,
	})
	if err != nil {
		return nil, err
	}
	if len(output.Messages) == 0 {
		return nil, queue.ErrTimeout
	}

	sqsMsg := output.Messages[0]
	qMsg := &queue.Message{
		ID:        aws.ToString(sqsMsg.MessageId),
		Topic:     c.q.sCfg.QueueURL,
		Payload:   []byte(aws.ToString(sqsMsg.Body)),
		Timestamp: time.Now(),
		Attempts:  1,
		Raw:       sqsMsg,
	}

	handle := aws.ToString(sqsMsg.ReceiptHandle)
	qMsg.SetAckFunc(func(cctx context.Context) error {
		_, err := c.q.client.DeleteMessage(cctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(c.q.sCfg.QueueURL),
			ReceiptHandle: aws.String(handle),
		})
		return err
	})
	qMsg.SetNackFunc(func(cctx context.Context, requeue bool) error { return nil })
	return qMsg, nil
}

func (c *sqsConsumer) Close() error {
	return nil
}
