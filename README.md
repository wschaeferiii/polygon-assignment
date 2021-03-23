# polygon-assignment
A take home assignment for polygon.io.  The assignment was to aggregate market info for crypto exchanges using their websocket API
and give information on 30 second intervals.  Underlying data comes from https://polygon.io/docs/websockets/connect

# How to run
### 1. First install packages
`go install`
### 2. Then build binary file
`go build aggregator.go`
### 3. Then run program with argument flags.
`./aggregator -apiKey={YOUR_API_KEY} -ticker=BTC-USD`
### NOTE: Only supports crypto tickers (exchanges?) that the polygon.io crypto websocket API supports
