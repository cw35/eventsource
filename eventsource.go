package eventsource

import (
	"bytes"
	"container/list"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type eventMessage struct {
	id           string
	event        string
	data         string
	subscribeKey string
}

type retryMessage struct {
	retry time.Duration
}

const (
	ConsumerStatusConnected int = 0x1
	ConsumerStatusStaled    int = 0x2
)

type eventSource struct {
	customHeadersFunc       func(*http.Request) [][]byte
	getConsumerSubscribeKey func(*http.Request) string
	getConsumerSessionKey   func(*http.Request) string

	messageSentListener    []func(string, string, string)
	consumerStatusListener []func(string, string, int)

	sink           chan message
	staled         chan *consumer
	add            chan *consumer
	close          chan bool
	idleTimeout    time.Duration
	retry          time.Duration
	timeout        time.Duration
	closeOnTimeout bool
	gzip           bool

	consumersLock sync.RWMutex
	consumers     *list.List
}

type Settings struct {
	// SetTimeout sets the write timeout for individual messages. The
	// default is 2 seconds.
	Timeout time.Duration

	// CloseOnTimeout sets whether a write timeout should close the
	// connection or just drop the message.
	//
	// If the connection gets closed on a timeout, it's the client's
	// responsibility to re-establish a connection. If the connection
	// doesn't get closed, messages might get sent to a potentially dead
	// client.
	//
	// The default is true.
	CloseOnTimeout bool

	// Sets the timeout for an idle connection. The default is 30 minutes.
	IdleTimeout time.Duration

	// Gzip sets whether to use gzip Content-Encoding for clients which
	// support it.
	//
	// The default is true.
	Gzip bool
}

func DefaultSettings() *Settings {
	return &Settings{
		Timeout:        2 * time.Second,
		CloseOnTimeout: true,
		IdleTimeout:    30 * time.Minute,
		Gzip:           true,
	}
}

// EventSource interface provides methods for sending messages and closing all connections.
type EventSource interface {
	// it should implement ServerHTTP method
	ServeHTTP(http.ResponseWriter, *http.Request)

	// send message to all consumers
	BroadcastEventMessage(data, event, id string)

	SendEventMessage(data, event, id, subscribeKey string)

	// send retry message to all consumers
	BroadcastRetryMessage(duration time.Duration)

	GetConsumerSubscribeKeys() []string
	GetConsumerSessionKeys() []string

	AddMessageSentListener(listeners []func(string, string, string))
	AddConsumerStatusListener(listeners []func(string, string, int))

	// consumers count
	ConsumersCount() int

	// close and clear all consumers
	Close()
}

type message interface {
	// The message to be sent to clients
	prepareMessage() []byte
	getSubscribeKey() string
	getId() string
}

func (m *eventMessage) getSubscribeKey() string {
	return m.subscribeKey
}

func (m *eventMessage) getId() string {
	return m.id
}

func (m *eventMessage) prepareMessage() []byte {
	var data bytes.Buffer
	if len(m.id) > 0 {
		data.WriteString(fmt.Sprintf("id: %s\n", strings.Replace(m.id, "\n", "", -1)))
	}
	if len(m.event) > 0 {
		data.WriteString(fmt.Sprintf("event: %s\n", strings.Replace(m.event, "\n", "", -1)))
	}
	if len(m.data) > 0 {
		lines := strings.Split(m.data, "\n")
		for _, line := range lines {
			data.WriteString(fmt.Sprintf("data: %s\n", line))
		}
	}
	data.WriteString("\n")
	return data.Bytes()
}

func controlProcess(es *eventSource) {
	for {
		select {
		case em := <-es.sink:
			message := em.prepareMessage()
			func() {
				es.consumersLock.RLock()
				defer es.consumersLock.RUnlock()

				for e := es.consumers.Front(); e != nil; e = e.Next() {
					c := e.Value.(*consumer)

					// Only send this message if the consumer isn't staled, and the subscribe key matches
					subscribeKey := em.getSubscribeKey()
					if !c.staled && (len(subscribeKey) == 0 || subscribeKey == c.subscribeKey) {
						select {
						case c.in <- message:
							go func() {
								sessionKey := c.sessionKey
								if id := em.getId(); len(id) > 0 {
									for _, handler := range es.messageSentListener {
										handler(id, subscribeKey, sessionKey)
									}
								}
							}()

						default:
						}
					}
				}
			}()
		case <-es.close:
			close(es.sink)
			close(es.add)
			close(es.staled)
			close(es.close)

			func() {
				es.consumersLock.RLock()
				defer es.consumersLock.RUnlock()

				for e := es.consumers.Front(); e != nil; e = e.Next() {
					c := e.Value.(*consumer)
					close(c.in)

					go func(subscribeKey, sessionKey string) {
						for _, handler := range es.consumerStatusListener {
							handler(subscribeKey, sessionKey, ConsumerStatusStaled)
						}
					}(c.subscribeKey, c.sessionKey)
				}
			}()

			es.consumersLock.Lock()
			defer es.consumersLock.Unlock()

			es.consumers.Init()
			return
		case c := <-es.add:
			func() {
				es.consumersLock.Lock()
				defer es.consumersLock.Unlock()

				es.consumers.PushBack(c)

				go func(subscribeKey, sessionKey string) {
					for _, handler := range es.consumerStatusListener {
						handler(subscribeKey, sessionKey, ConsumerStatusConnected)
					}
				}(c.subscribeKey, c.sessionKey)
			}()
		case c := <-es.staled:
			toRemoveEls := make([]*list.Element, 0, 1)
			func() {
				es.consumersLock.RLock()
				defer es.consumersLock.RUnlock()

				for e := es.consumers.Front(); e != nil; e = e.Next() {
					if e.Value.(*consumer) == c {
						toRemoveEls = append(toRemoveEls, e)
					}
				}

				go func(subscribeKey, sessionKey string) {
					for _, handler := range es.consumerStatusListener {
						handler(subscribeKey, sessionKey, ConsumerStatusStaled)
					}
				}(c.subscribeKey, c.sessionKey)
			}()
			func() {
				es.consumersLock.Lock()
				defer es.consumersLock.Unlock()

				for _, e := range toRemoveEls {
					es.consumers.Remove(e)
				}
			}()
			close(c.in)
		}
	}
}

// New creates new EventSource instance.
func New(settings *Settings, customHeadersFunc func(*http.Request) [][]byte, getConsumerSessionKey func(*http.Request) string,
	getConsumerSubscribeKey func(*http.Request) string) EventSource {

	if settings == nil {
		settings = DefaultSettings()
	}

	es := new(eventSource)
	es.messageSentListener = []func(string, string, string){}
	es.consumerStatusListener = []func(string, string, int){}
	es.getConsumerSubscribeKey = getConsumerSubscribeKey
	es.getConsumerSessionKey = getConsumerSessionKey
	es.customHeadersFunc = customHeadersFunc
	es.sink = make(chan message, 1)
	es.close = make(chan bool)
	es.staled = make(chan *consumer, 1)
	es.add = make(chan *consumer)
	es.consumers = list.New()
	es.timeout = settings.Timeout
	es.idleTimeout = settings.IdleTimeout
	es.closeOnTimeout = settings.CloseOnTimeout
	es.gzip = settings.Gzip
	go controlProcess(es)
	return es
}

func (es *eventSource) Close() {
	es.close <- true
}

// ServeHTTP implements http.Handler interface.
func (es *eventSource) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	cons, err := newConsumer(resp, req, es)
	if err != nil {
		log.Print("Can't create connection to a consumer: ", err)
		return
	}
	es.add <- cons
}

func (es *eventSource) sendMessage(m message) {
	es.sink <- m
}

func (es *eventSource) BroadcastEventMessage(data, event, id string) {
	es.SendEventMessage(data, event, id, "")
}

func (es *eventSource) SendEventMessage(data, event, id, subscribeKey string) {
	em := &eventMessage{id, event, data, subscribeKey}
	es.sendMessage(em)
}

func (m *retryMessage) getSubscribeKey() string {
	return ""
}

func (m *retryMessage) getId() string {
	return ""
}

func (m *retryMessage) prepareMessage() []byte {
	return []byte(fmt.Sprintf("retry: %d\n\n", m.retry/time.Millisecond))
}

func (es *eventSource) BroadcastRetryMessage(t time.Duration) {
	es.sendMessage(&retryMessage{t})
}

func (es *eventSource) ConsumersCount() int {
	es.consumersLock.RLock()
	defer es.consumersLock.RUnlock()

	return es.consumers.Len()
}

func (es *eventSource) GetConsumerSubscribeKeys() []string {
	es.consumersLock.RLock()
	defer es.consumersLock.RUnlock()

	subscribeKeyMap := map[string]bool{}
	for e := es.consumers.Front(); e != nil; e = e.Next() {
		c := e.Value.(*consumer)
		// Only send this message if the consumer isn't staled, and the subscribe key matches
		if !c.staled && len(c.subscribeKey) > 0 {
			subscribeKeyMap[c.subscribeKey] = true
		}
	}

	subscribeKeys := []string{}
	for key, _ := range subscribeKeyMap {
		subscribeKeys = append(subscribeKeys, key)
	}

	return subscribeKeys
}

func (es *eventSource) GetConsumerSessionKeys() []string {
	es.consumersLock.RLock()
	defer es.consumersLock.RUnlock()

	sessionKeyMap := map[string]bool{}
	for e := es.consumers.Front(); e != nil; e = e.Next() {
		c := e.Value.(*consumer)
		// Only send this message if the consumer isn't staled, and the subscribe key matches
		if !c.staled && len(c.sessionKey) > 0 {
			sessionKeyMap[c.sessionKey] = true
		}
	}

	sessionKeys := []string{}
	for key, _ := range sessionKeyMap {
		sessionKeys = append(sessionKeys, key)
	}

	return sessionKeys
}

func (es *eventSource) AddMessageSentListener(listeners []func(string, string, string)) {
	es.messageSentListener = append(es.messageSentListener, listeners...)
}

func (es *eventSource) AddConsumerStatusListener(listeners []func(string, string, int)) {
	es.consumerStatusListener = append(es.consumerStatusListener, listeners...)
}
