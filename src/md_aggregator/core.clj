(ns md-aggregator.core
  (:gen-class)
  (:require [clojure.core.async :refer [<! go-loop]]
            [md-aggregator.binance :as binance]
            [md-aggregator.ftx :as ftx]
            [md-aggregator.huobi :as huobi]
            [md-aggregator.kraken :as kraken]
            [md-aggregator.okex :as okex]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [generate-channel-map]]
            [taoensso.timbre :as log]))

(def symbols (atom [:ETH :BTC]))
(def trade-channels (atom {}))

(defn start-trade-consumers [trade-channels]
  (doseq [[sym ch] trade-channels]
    (go-loop []
      (let [x (<! ch)]
        ;; todo: send to appropriate channel based on symbol
        (recur)))))

(def inits [binance/init ftx/init huobi/init kraken/init okex/init])

(defn -main [& args]
  (log/swap-config! assoc :appenders {:spit (log/spit-appender {:fname "./logs/app.log"})})
  (statsd/reset-statsd!)
  (reset! trade-channels (generate-channel-map @symbols))
  (start-trade-consumers @trade-channels)
  (doseq [init inits]
    (init @trade-channels)))

