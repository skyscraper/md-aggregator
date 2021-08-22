(ns md-aggregator.deribit
  (:require [clojure.core.async :refer [>! go]]
            [clojure.string :refer [includes?]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [info-map trade-stats ws-conn]]
            [taoensso.timbre :as log]))

(def url "wss://www.deribit.com/ws/api/v2")
(def exch :deribit)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))
(def ws-props {:max-frame-payload 131072
               :heartbeats {:send-after-idle 3e4
                            :timeout 3e4}})

(defn subscribe [conn channels]
  (s/put! conn (json/write-value-as-string {:jsonrpc "2.0"
                                            :id (System/currentTimeMillis)
                                            :method "public/subscribe"
                                            :params {:channels channels}})))

(defn handle [raw]
  (let [{:keys [result method params] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (if method
      (condp = (keyword method)
        :subscription
        (let [{:keys [data]} params
              {:keys [channel] :as meta-info}
              ((keyword (:channel params)) info)]
          (when channel
            (go
              (doseq [{:keys [price amount direction timestamp liquidation]} data
                      :let [trade {:price price
                                   :size (double (/ amount price))
                                   :side (keyword direction)
                                   :time timestamp
                                   :liquidation (and (some? liquidation)
                                                     (includes? liquidation "T"))
                                   :source exch}]]
                (>! channel trade)
                (trade-stats trade tags meta-info)))))
        (log/warn "unhandled deribit method" payload))
      (if result
        (log/info "subscribed to:" result)
        (log/warn "unhandled result:" payload)))))

(defn rename [k]
  (keyword (str "trades." (name k) "-PERPETUAL.raw")))

(defn connect! []
  (let [conn @(ws-conn exch url ws-props connect!)]
    (reset! connection conn)
    (s/consume handle conn)
    (subscribe conn (keys info))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))

