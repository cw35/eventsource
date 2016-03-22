package main

import (
	"github.com/cw35/eventsource"
	"log"
	"net/http"
	"time"
)

func getSubscribeKey(req *http.Request) string {
	return req.Header.Get("Authorization")
}

func getSessionKey(req *http.Request) string {
	return req.Header.Get("Authorization")
}

func consumerStatusListener(subscribeKey, sessionKey string, status int) {
	log.Println("consumerStatusListener", subscribeKey, sessionKey, status)
}

func messageSentListener(messageId, subscribeKey, sessionKey string) {
	log.Println("messageSentListener", messageId, subscribeKey, sessionKey)
}

func customHeaders(req *http.Request) [][]byte {
	return [][]byte{
		[]byte("Cache-Control: no-cache"),
		[]byte("Connection: keep-alive"),
	}
}

func main() {
	es := eventsource.New(nil, customHeaders, getSessionKey, getSubscribeKey)
	es.AddMessageSentListener([]func(string, string, string){messageSentListener})
	es.AddConsumerStatusListener([]func(string, string, int){consumerStatusListener})
	defer es.Close()
	http.Handle("/", http.FileServer(http.Dir("./public")))
	http.Handle("/events", es)
	go func() {
		for {
			es.BroadcastEventMessage("hello", "", "")
			log.Printf("Hello has been sent (consumers: %d)", es.ConsumersCount())
			time.Sleep(2 * time.Second)
		}
	}()
	log.Print("Open URL http://localhost:8080/ in your browser.")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
