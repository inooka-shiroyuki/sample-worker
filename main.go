package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Handler func(*sqs.Message)

func main() {
	// Siganal受信時にループを抜けるために利用
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &taskA{q: &fakeQueue{}}
	b := &taskB{q: &fakeQueue{}}

	//a := &taskA{q: newAmazonSimpleQueue("queueA")}
	//b := &taskB{q: newAmazonSimpleQueue("queueB")}

	// Queueの種類毎にループ生成
	go a.run(ctx)
	go b.run(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan)

	// Signal待受
	sig := <-sigChan
	fmt.Printf("\nSignal: %d\n", sig)
	// Signal受信したらcancel()
	cancel()
}

func poll(ctx context.Context, q Queue, h Handler) {
	for {
		ms, _ := q.Receive(ctx)
		if len(ms) > 0 {

			var wg sync.WaitGroup
			wg.Add(len(ms))

			for _, m := range ms {
				go func(m *sqs.Message) {
					defer wg.Done()
					h(m)
					q.Delete(m)
				}(m)
			}

			wg.Wait()
		}

		select {
		// cancel()実行時にctx.Done()を受け取る
		case <-ctx.Done():
			fmt.Println("poll(): context done.")
			return
		default:
		}
	}
}

type taskA struct {
	q Queue
}

func (t *taskA) handleMessage(m *sqs.Message) {
	fmt.Printf("run task A. message: %s\n", *m.Body)
}

func (t *taskA) run(ctx context.Context) {
	poll(ctx, t.q, t.handleMessage)
}

type taskB struct {
	q Queue
}

func (t *taskB) handleMessage(m *sqs.Message) {
	fmt.Printf("run task B. message: %s\n", *m.Body)
}

func (t *taskB) run(ctx context.Context) {
	poll(ctx, t.q, t.handleMessage)
}

type Queue interface {
	Receive(context.Context) ([]*sqs.Message, error)
	Delete(*sqs.Message) error
}

type fakeQueue struct {
	cnt int
}

func (q *fakeQueue) Receive(context.Context) ([]*sqs.Message, error) {
	time.Sleep(1 * time.Second)
	q.cnt++
	c := strconv.Itoa(q.cnt)
	fmt.Printf("receive messsage :%d\n", q.cnt)
	return []*sqs.Message{{Body: &c}}, nil
}

func (q *fakeQueue) Delete(m *sqs.Message) error {
	fmt.Printf("delete messsage :%s\n", *m.Body)
	return nil
}

type amazonSimpleQueue struct {
	client *sqs.SQS
}

func newAmazonSimpleQueue(p string) *amazonSimpleQueue {
	sess, _ := session.NewSessionWithOptions(session.Options{
		Config:  *aws.NewConfig().WithRegion("ap-northeast-1"),
		Profile: p,
	})
	return &amazonSimpleQueue{client: sqs.New(sess)}
}

func (q *amazonSimpleQueue) Receive(context.Context) ([]*sqs.Message, error) {
	receiveParams := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String("endpoint"),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(20),
	}
	resp, err := q.client.ReceiveMessage(receiveParams)
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func (q *amazonSimpleQueue) Delete(m *sqs.Message) error {
	_, err := q.client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String("endpoint"),
		ReceiptHandle: aws.String(*m.ReceiptHandle),
	})
	return err
}
