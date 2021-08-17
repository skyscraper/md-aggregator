(ns md-aggregator.huobi
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :refer [parse-stream generate-string]]
            [clojure.core.async :refer [put!]]
            [clojure.java.io :refer [reader]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [coin re-key key-mapping trade-stats]]
            [taoensso.timbre :as log])
  (:import (java.util.zip GZIPInputStream)))

(def url "wss://api.hbdm.com/linear-swap-ws")
(def exch :huobi)
(def tags [(str "exch" exch)])
(def connection (atom nil))
(def trade-channels (atom {}))
(def coin-tags (atom {}))
(def base "market.%s.trade.detail")

(defn subscribe [conn channels]
  (doseq [ch channels]
    (s/put! conn (generate-string {:sub ch}))))

(defn handle [raw]
  (with-open [rdr (reader (GZIPInputStream. (bs/to-input-stream raw)))]
    (let [{:keys [ch subbed status ping tick] :as payload} (parse-stream rdr true)]
      (statsd/count :ws-msg 1 tags)
      (cond
        (some? ch) (let [kw-ch (keyword ch)
                         c (kw-ch @trade-channels)]
                     (when c
                       (doseq [{:keys [price quantity direction ts]} (:data tick)
                               :let [updated {:price (double price)
                                              :size (double quantity)
                                              :side (keyword direction)
                                              :time ts
                                              :source exch}]]
                         (put! c updated)
                         (trade-stats (:price updated) (:size updated) (:time updated)
                                      tags (kw-ch @coin-tags)))))
        (some? subbed) (log/info subbed "subscription status:" status)
        (some? ping) (s/put! @connection (generate-string {:pong ping}))
        :else (log/warn "unhandled huobi message: " payload)))))

(defn rename [k]
  (keyword (format base (str (name k) "-USDT"))))

(defn init [t-channels]
  (let [conn @(http/websocket-client url {:epoll? true})]
    (reset! trade-channels (re-key t-channels rename))
    (reset! coin-tags (key-mapping t-channels rename #(str coin %)))
    (reset! connection conn)
    (s/consume handle conn)
    (subscribe conn (keys @trade-channels))))

