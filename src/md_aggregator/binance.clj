(ns md-aggregator.binance
  (:require [aleph.http :as http]
            [cheshire.core :refer [parse-string]]
            [clojure.core.async :refer [put!]]
            [clojure.string :refer [join lower-case]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [coin re-key key-mapping trade-stats]]))

;; TODO: stream will get disconnected at 24h mark, need to handle this

(def url "wss://fstream.binance.com")
(def exch :binance)
(def tags [(str "exch" exch)])
(def connection (atom nil))
(def trade-channels (atom {}))
(def coin-tags (atom {}))

(defn handle [raw]
  (let [{:keys [s p q T m]} (:data (parse-string raw true))]
    (statsd/count :ws-msg 1 tags)
    (when s
      (let [sym (keyword s)]
        (when-let [c (sym @trade-channels)]
          (let [price (Double/parseDouble p)
                size (Double/parseDouble q)]
            (put! c {:price price
                     :size size
                     :side (if m :sell :buy)
                     :time T
                     :source exch})
            (trade-stats price size T tags (sym @coin-tags))))))))

(defn rename [k]
  (keyword (str (name k) "USDT")))

(defn init [t-channels]
  (reset! trade-channels (re-key t-channels rename))
  (reset! coin-tags (key-mapping t-channels rename #(str coin %)))
  (let [full-url
        (str
         url
         "/stream?streams="
         (join "/" (map #(str (lower-case (name %)) "@trade") (keys @trade-channels))))
        conn @(http/websocket-client full-url {:epoll? true})]
    (reset! connection conn)
    (s/consume handle conn)))

