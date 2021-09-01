(ns md-aggregator.kraken-inv
  (:require [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume info-map inv-true process-single
                                         subscribe ws-conn]]
            [taoensso.timbre :as log]))

(def url "wss://futures.kraken.com/ws/v1")
(def exch :kraken) ;; name this inv if kraken releases linear contracts
(def tags [(str "exch" exch) inv-true])
(def ws-timeout 60000)
(def info {})
(def connection (atom nil))
(def ws-props {:heartbeats {:send-after-idle 6e4
                            :timeout 6e4}})
(def liq-types #{:liquidation :termination})

(defn normalize [price qty side time type]
  {:price (double price)
   :size (double (/ qty price)) ;; inverse future
   :side (keyword side)
   :time time
   :liquidation (if ((keyword type) liq-types) true false)
   :source exch})

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
        :trade (let [{:keys [product_id price qty side time type]} payload]
                 (process-single (normalize price qty side time type)
                                 tags
                                 ((keyword product_id) info)))
        :trade_snapshot (log/info "received initial trade snapshot, ignoring...")
        (log/warn "received unknown feed type:" feed))
      :else
      (log/warn "received unhandled kraken message:" payload))))

(defn rename [k]
  (keyword (str "PI_" (name (if (= :BTC k) :XBT k)) "USD")))

(defn connect! []
  (let [conn @(ws-conn exch url ws-props connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (subscribe conn [{:event :subscribe :feed :trade :product_ids (keys info)}])
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))
