(ns md-aggregator.huobi
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :refer [parse-stream generate-string]]
            [clojure.core.async :refer [put!]]
            [clojure.java.io :refer [reader]]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [info-map trade-stats]]
            [taoensso.timbre :as log])
  (:import (java.util.zip GZIPInputStream)))

(def url "wss://api.hbdm.vn/linear-swap-ws")
(def exch :huobi)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))
(def base "market.%s.trade.detail")

(defn subscribe [conn channels]
  (doseq [ch channels]
    (s/put! conn (generate-string {:sub ch}))))

(defn handle [raw]
  (with-open [rdr (reader (GZIPInputStream. (bs/to-input-stream raw)))]
    (let [{:keys [ch subbed status ping tick] :as payload} (parse-stream rdr true)]
      (statsd/count :ws-msg 1 tags)
      (cond
        (some? ch) (let [{:keys [channel] :as meta-info} ((keyword ch) info)]
                     (when channel
                       (doseq [{:keys [price quantity direction ts]} (:data tick)
                               :let [trade {:price (double price)
                                            :size (double quantity)
                                            :side (keyword direction)
                                            :time ts
                                            :source exch}]]
                         (put! channel trade)
                         (trade-stats trade tags meta-info))))
        (some? subbed) (log/info subbed "subscription status:" status)
        (some? ping) (s/put! @connection (generate-string {:pong ping}))
        :else (log/warn "unhandled huobi message: " payload)))))

(defn rename [k]
  (keyword (format base (str (name k) "-USDT"))))


(declare connect!)

(defn ws-conn []
  (d/catch
      (http/websocket-client url {:epoll? true
                                  :max-frame-payload 131072})
      (fn [e]
        (log/error (name exch) "ws problem:" e)
        (connect!))))

(defn connect! []
  (let [conn @(ws-conn)]
    (log/info "connecting to" (name exch) "...")
    (reset! connection conn)
    (s/consume handle conn)
    (subscribe conn (keys info))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))

