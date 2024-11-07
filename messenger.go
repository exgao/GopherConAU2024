package main

import (
	"fmt"
)

const (
	topicHeader      = "topic"
	routingKeyHeader = "routingKey"
)

type (
	Messenger struct {
		queues  map[string]chan Message
		routing map[string][]chan Message
	}

	Binding struct {
		Topic      string
		RoutingKey string
		throttler  Throttler
	}

	Queue struct {
		Name     string
		Binding  []Binding
		Consumer Consumer
	}

	Message struct {
		Headers map[string]string
		Body    string
	}
)

func NewMessenger() *Messenger {
	m := &Messenger{
		queues:  make(map[string]chan Message),
		routing: make(map[string][]chan Message),
	}
	return m
}

func (m *Messenger) DeclareQueue(name string) {
	if _, ok := m.queues[name]; !ok {
		m.queues[name] = make(chan Message, 100)
	}
}

func (m *Messenger) BindQueue(queueName string, topic string, routingKey string) error {
	routingChan, ok := m.queues[queueName]
	if !ok {
		fmt.Printf("queue %s not found\n", queueName)
		return fmt.Errorf("queue %s not found", queueName)
	}
	key := fmt.Sprintf("%s-%s", topic, routingKey)
	if _, exists := m.routing[key]; !exists {
		m.routing[key] = []chan Message{}
	}
	m.routing[key] = append(m.routing[key], routingChan)
	return nil
}

func (m *Messenger) Publish(topic string, routingKey string, body string) {
	if m == nil {
		fmt.Printf("messenger is not set up\n")
		return
	}

	msg := Message{
		Headers: map[string]string{
			topicHeader:      topic,
			routingKeyHeader: routingKey,
		},
		Body: body,
	}
	key := fmt.Sprintf("%s-%s", topic, routingKey)

	for _, routingChan := range m.routing[key] {
		routingChan <- msg
	}
}

func (d *Message) GetHeader(key string) string {
	if len(d.Headers) == 0 {
		return ""
	}
	return d.Headers[key]
}

func (d *Message) SetHeader(key string, val string) {
	if len(d.Headers) == 0 {
		d.Headers = make(map[string]string)
	}
	d.Headers[key] = val
}
