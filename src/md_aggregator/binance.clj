(ns md-aggregator.binance
  (:require [aleph.http :as http]
            [cheshire.core :refer [parse-string]]
            [clojure.core.async :refer [put!]]
            [clojure.string :refer [join lower-case]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [info-map trade-stats]]
            [taoensso.timbre :as log]))

;; TODO: stream will get disconnected at 24h mark, need to handle this

(def url "wss://fstream.binance.com")
(def exch :binance)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))

(defn handle [raw]
  (let [{:keys [s p q T m] :as payload} (:data (parse-string raw true))]
    (statsd/count :ws-msg 1 tags)
    (if s
      (let [{:keys [channel] :as meta-info} ((keyword s) info)
            trade {:price (Double/parseDouble p)
                   :size (Double/parseDouble q)
                   :side (if m :sell :buy)
                   :time T
                   :source exch}]
        (put! channel trade)
        (trade-stats trade tags meta-info))
      (log/warn "unhandled binance message:" payload))))

(defn rename [k]
  (keyword (str (name k) "USDT")))

(defn full-url []
  (str
   url
   "/stream?streams="
   (join "/" (map #(str (lower-case (name %)) "@trade") (keys info)))))

(defn ws-conn []
  (http/websocket-client (full-url) {:epoll? true}))

(defn connect! []
  (let [conn @(ws-conn)]
    (reset! connection conn)
    (s/consume handle conn)
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))

