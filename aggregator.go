package main

import (
	"flag"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// the aggregate structure in which to form
// each message from the ws api
type Aggregate struct {
	ticker    string
	startTime float64
	endTime   float64
	open      float64
	close     float64
	high      float64
	low       float64
	volume    float64
}

// The slice to push aggregate messages to
var aggWindowSlice []Aggregate

// The slice in which to compare in case of "OOO" messages
var aggWindowSliceCopy []Aggregate

// Processing function that takes messages and
// adds to the slices for comparing and printing
func processMessage(msg interface{}) {

	for _, agg := range msg.([]interface{}) {

		pair := agg.(map[string]interface{})["pair"]
		close := agg.(map[string]interface{})["c"]
		end := agg.(map[string]interface{})["e"]
		high := agg.(map[string]interface{})["h"]
		low := agg.(map[string]interface{})["l"]
		open := agg.(map[string]interface{})["o"]
		start := agg.(map[string]interface{})["s"]
		volume := agg.(map[string]interface{})["vw"]

		aggToAdd := Aggregate{}

		if pair != nil {

			aggToAdd.ticker = pair.(string)
			aggToAdd.startTime = start.(float64)
			aggToAdd.endTime = end.(float64)
			aggToAdd.open = open.(float64)
			aggToAdd.close = close.(float64)
			aggToAdd.high = high.(float64)
			aggToAdd.low = low.(float64)
			aggToAdd.volume = volume.(float64)

			aggWindowSlice = append(aggWindowSlice, aggToAdd)

			aggWindowSliceCopy = append([]Aggregate(nil), aggWindowSlice...)

			sortAggregates()

		}
	}

}

// Using insertion sort since messages
// are usually in order
func sortAggregates() []Aggregate {

	var length = len(aggWindowSlice)

	for i := 1; i < length; i++ {
		j := i
		for j > 0 {
			if aggWindowSlice[j-1].startTime < aggWindowSlice[j].startTime {
				aggWindowSlice[j-1], aggWindowSlice[j] = aggWindowSlice[j], aggWindowSlice[j-1]
			}
			j = j - 1
		}
	}

	return aggWindowSlice

}

// Compare function reflect.DeepEqual on slices
// is recursive and element level
func compareAggregateWindows() bool {

	return reflect.DeepEqual(aggWindowSlice, aggWindowSliceCopy)
}

// Print logic for time interval
func printForInterval(duration time.Duration) {

	same := compareAggregateWindows()

	for x := range time.Tick(duration) {

		if same {
			printSingleAgg(x)
		} else {
			printAggWindow(x)
		}
	}
}

// Formats and prints message.
func printAgg(agg Aggregate) {

	timeStringFromSNotation := fmt.Sprintf("%f", agg.startTime)
	intTime, _ := strToInt(timeStringFromSNotation)
	withoutMillisecondsTime := intTime / 1000
	humanTime := time.Unix(int64(withoutMillisecondsTime), 0).Format((time.RFC3339))

	fmt.Printf("%q - open: %v, close: %v, high: %v, low: %v, volume: %v \n", humanTime, agg.open, agg.close, agg.high, agg.low, agg.volume)
}

// Converts epoch string to integer
func strToInt(str string) (int, error) {
	decimalSplit := strings.Split(str, ".")
	return strconv.Atoi(decimalSplit[0])
}

// Prints single message when messages come in order
func printSingleAgg(t time.Time) {

	if len(aggWindowSlice) > 1 {

		aggToPrint := aggWindowSlice[0]
		printAgg(aggToPrint)

	}
}

// Prints the whole window, after sorting, when
// messages come in out of order
func printAggWindow(t time.Time) {
	for _, agg := range aggWindowSlice {
		printAgg(agg)
	}
}

func main() {

	ticker := flag.String("ticker", "BTC-USD", "symbol for crypto exchange (default BTC-USD")

	var prefix = "XA."

	channel := prefix + *ticker

	apiKey := flag.String("apiKey", "", "the api key to access polygon.io crypto websocket API")

	flag.Parse()

	fmt.Printf("Aggregating for %s , please wait for data to flow in... \n", *ticker)

	c, _, err := websocket.DefaultDialer.Dial("wss://socket.polygon.io/crypto", nil)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"action\":\"auth\",\"params\":\"%s\"}", *apiKey)))
	_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"action\":\"subscribe\",\"params\":\"%s\"}", channel)))

	// Buffered channel to account for bursts or spikes in data:
	chanMessages := make(chan interface{}, 10000)

	// Print messages to console every 30 seconds
	go func() {
		printForInterval(30 * time.Second)
	}()

	// Read messages off the buffered queue:
	go func() {
		for msgBytes := range chanMessages {
			processMessage(msgBytes)
		}
	}()

	// As little logic as possible in the reader loop:
	for {
		var msg interface{}
		err := c.ReadJSON(&msg)
		// Ideally use c.ReadMessage instead of ReadJSON so you can parse the JSON data in a
		// separate go routine. Any processing done in this loop increases the chances of disconnects
		// due to not consuming the data fast enough.
		if err != nil {
			panic(err)
		}
		chanMessages <- msg
	}

}
