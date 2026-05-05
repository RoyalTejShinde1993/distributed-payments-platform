package kafka

import (
    "context"
    "time"

    kafkago "github.com/segmentio/kafka-go"
)

type Producer struct {
    writer *kafkago.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
    return &Producer{
        writer: kafkago.NewWriter(kafkago.WriterConfig{
            Brokers:  brokers,
            Topic:    topic,
            Balancer: &kafkago.LeastBytes{},
            Async:    false,
        }),
    }
}

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
    return p.writer.WriteMessages(ctx, kafkago.Message{
        Key:   key,
        Value: value,
        Time:  time.Now(),
    })
}

func (p *Producer) Close() error {
    return p.writer.Close()
}
