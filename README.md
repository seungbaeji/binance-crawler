# Binance Historical Data Crawler

# How To Use

```bash
>>> python -m main btcusdt ethusdt
```

## Historical Data

- https://www.binance.com/en/landing/data
- https://github.com/binance/binance-public-data#where-do-i-access-it
- BTC/USDT 마켓의 데이터를 사용했다.

### Klines

- 캔들차트 데이터를 나타낸다.
- 속성의 설명은 아래와 같다.

  - openTime (int): 거래 시작시간
  - open (float): 시가
  - high (float): 최고가
  - low (float): 최저가
  - close (float): 종가
  - volume (float): BTC 거래량
  - quoteAssetVolume (float): USDT 거래량
  - numberOfTrades (int): 거래횟수
  - takerBuy (makerSell) BaseAssetVolume (float): 테이커 매수주문이 기여한 BTC 거래량
  - takerBuy (makerSell) QuoteAssetVolume (float): 테이커 매수주문이 기여한 USDT 거래량

> 1. quote는 '시세를 매기다' 라는 동사이다. 따라서 각 마켓의 기본통화의 거래량을 의미한다.
> 2. volume = takerBuyBaseAssetVolume + takerSellBaseAssetVolume

### Checksum

## Real-time data

- https://binance-docs.github.io/apidocs/futures/en/#market-data-endpoints

```json
{
  "e": "aggTrade", // Event type
  "E": 123456789, // Event time
  "s": "BTCUSDT", // Symbol
  "a": 5933014, // Aggregate trade ID
  "p": "0.001", // Price
  "q": "100", // Quantity
  "f": 100, // First trade ID
  "l": 105, // Last trade ID
  "T": 123456785, // Trade time
  "m": true // Is the buyer the market maker?
}
```

# Reference

- https://github.com/binance/binance-public-data/#trades-1
- https://dev.binance.vision/t/taker-buy-base-asset-volume/6026
- https://www.trailingcrypto.com/support/article/understanding-trade-volume-relative-volume-base-quote-volume
- https://developers.binance.com/docs/binance-trading-api/futures#aggregate-trade-streams
