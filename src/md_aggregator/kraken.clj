(ns md-aggregator.kraken
  (:require [aleph.http :as http]
            [cheshire.core :refer [parse-string generate-string]]
            [clojure.core.async :refer [put!]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [coin re-key key-mapping trade-stats]]
            [taoensso.timbre :as log]))


(def url "wss://futures.kraken.com/ws/v1")
(def exch :kraken)
(def tags [(str "exch" exch)])
(def connection (atom nil))
(def trade-channels (atom {}))
(def liq-types #{:liquidation :termination})
(def coin-tags (atom {}))

(defn subscribe [conn instruments]
  (s/put! conn (generate-string
                {:event :subscribe :feed :trade :product_ids instruments})))

(defn handle [raw]
  (let [{:keys [event feed product_ids] :as payload} (parse-string raw true)]
    (statsd/count :ws-msg 1 tags)
    (cond
      (some? event) (if (some? feed)
                      (log/info event feed product_ids)
                      (log/info event))
      (some? feed) (condp = (keyword feed)
                     :trade (let [{:keys [product_id price qty side time type]} payload
                                  kw-type (keyword type)
                                  kw-product (keyword product_id)]
                              (when-let [c (kw-product @trade-channels)]
                                (let [updated {:price (double price)
                                               :size (double (/ qty price)) ;; inverse future
                                               :side (keyword side)
                                               :time time
                                               :liquidation (if (kw-type liq-types) true false)
                                               :source exch}]
                                  (put! c updated)
                                  (trade-stats (:price updated) (:size updated) (:time updated)
                                               tags (kw-product @coin-tags)))))
                     :trade_snapshot (log/info "received initial trade snapshot, ignoring...")
                     (log/warn "received unknown feed type:" feed))
      :else (log/warn "received unhandled kraken message:" payload))))

(defn rename [k]
  (keyword (str "PI_" (name (if (= :BTC k) :XBT k)) "USD")))

(defn init [t-channels]
  (let [conn @(http/websocket-client url {:epoll? true
                                          :heartbeats {:send-after-idle 6e4
                                                       :timeout 6e4}})]
    (reset! trade-channels (re-key t-channels rename))
    (reset! coin-tags (key-mapping t-channels rename #(str coin %)))
    (reset! connection conn)
    (s/consume handle conn)
    (subscribe conn (keys @trade-channels))))

