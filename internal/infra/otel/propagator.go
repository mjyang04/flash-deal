package otel

import (
	segkafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
)

// KafkaHeaderCarrier adapts kafka-go Headers to propagation.TextMapCarrier
// so OTel context can be injected/extracted across producer/consumer.
type KafkaHeaderCarrier []segkafka.Header

func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *KafkaHeaderCarrier) Set(key, value string) {
	for i := range *c {
		if (*c)[i].Key == key {
			(*c)[i].Value = []byte(value)
			return
		}
	}
	*c = append(*c, segkafka.Header{Key: key, Value: []byte(value)})
}

func (c KafkaHeaderCarrier) Keys() []string {
	ks := make([]string, 0, len(c))
	for _, h := range c {
		ks = append(ks, h.Key)
	}
	return ks
}

var _ propagation.TextMapCarrier = (*KafkaHeaderCarrier)(nil)
