(ns md-aggregator.kraken
  (:require [aleph.http :as http]
            [clojure.core.async :refer [put!]]
            [jsonista.core :as json]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [info-map trade-stats]]
            [taoensso.timbre :as log]))


(def url "wss://futures.kraken.com/ws/v1")
(def exch :kraken)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))
(def liq-types #{:liquidation :termination})

(defn subscribe [conn instruments]
  (s/put! conn (json/write-value-as-string
                {:event :subscribe :feed :trade :product_ids instruments})))

(defn handle [raw]
  (let [{:keys [event feed product_ids] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (cond
      (some? event)
      (if (some? feed)
        (log/info event feed product_ids)
        (log/info event))
      (some? feed)
      (condp = (keyword feed)
        :trade (let [{:keys [product_id price qty side time type]} payload
                     {:keys [channel] :as meta-info} ((keyword product_id) info)]
                 (when channel
                   (let [trade {:price (double price)
                                :size (double (/ qty price)) ;; inverse future
                                :side (keyword side)
                                :time time
                                :liquidation (if ((keyword type) liq-types) true false)
                                :source exch}]
                     (put! channel trade)
                     (trade-stats trade tags meta-info))))
        :trade_snapshot (log/info "received initial trade snapshot, ignoring...")
        (log/warn "received unknown feed type:" feed))
      :else
      (log/warn "received unhandled kraken message:" payload))))

(defn rename [k]
  (keyword (str "PI_" (name (if (= :BTC k) :XBT k)) "USD")))

(declare connect!)

(defn ws-conn []
  (d/catch
      (http/websocket-client url {:epoll? true
                                  :heartbeats {:send-after-idle 6e4
                                               :timeout 6e4}})
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

