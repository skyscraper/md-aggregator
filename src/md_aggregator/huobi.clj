(ns md-aggregator.huobi
  (:require [byte-streams :as bs]
            [clojure.core.async :refer [>! go]]
            [clojure.java.io :refer [reader]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume info-map inv-false trade-stats ws-conn]]
            [taoensso.timbre :as log])
  (:import (java.util.zip GZIPInputStream)))

(def url "wss://api.hbdm.vn/linear-swap-ws")
(def exch :huobi)
(def tags [(str "exch" exch) inv-false])
(def ws-timeout 20000)
(def info {})
(def connection (atom nil))
(def ws-props {:max-frame-payload 131072})
(def base "market.%s.trade.detail")

(defn subscribe [conn channels]
  (doseq [ch channels]
    (s/put! conn (json/write-value-as-string {:sub ch}))))

(defn normalize [{:keys [price quantity direction ts]}]
  {:price (double price)
   :size (double quantity)
   :side (keyword direction)
   :time ts
   :source exch})

(defn handle [raw]
  (with-open [rdr (reader (GZIPInputStream. (bs/to-input-stream raw)))]
    (let [{:keys [ch subbed status ping tick] :as payload}
          (json/read-value rdr json/keyword-keys-object-mapper)]
      (statsd/count :ws-msg 1 tags)
      (cond
        (some? ch) (let [{:keys [channel] :as meta-info} ((keyword ch) info)]
                     (when channel
                       (go
                         (doseq [x (:data tick)
                                 :let [trade (normalize x)]]
                           (>! channel trade)
                           (trade-stats trade tags meta-info)))))
        (some? subbed) (log/info subbed "subscription status:" status)
        (some? ping) (s/put! @connection (json/write-value-as-string {:pong ping}))
        :else (log/warn "unhandled huobi message: " payload)))))

(defn rename [k]
  (keyword (format base (str (name k) "-USDT"))))

(defn connect! []
  (let [conn @(ws-conn exch url ws-props connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (subscribe conn (keys info))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))

