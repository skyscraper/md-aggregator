(ns md-aggregator.binance-inv
  (:require [clojure.core.async :refer [put!]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.binance :as binance]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume get-ct-sizes info-map inv-true trade-stats ws-conn]]
            [taoensso.timbre :as log]))

(def api-url "https://dapi.binance.com")
(def ex-info-ep "/dapi/v1/exchangeInfo")
(def url "wss://dstream.binance.com")
(def exch :binance-inv)
(def tags [(str "exch" binance/exch) inv-true])
(def ws-timeout 20000)
(def info {})
(def connection (atom nil))
(def ct-size (atom {}))

(defn handle [raw]
  (let [{:keys [s p q T m] :as payload} (:data (json/read-value raw json/keyword-keys-object-mapper))]
    (statsd/count :ws-msg 1 tags)
    (if s
      (let [kw-sym (keyword s)
            {:keys [channel] :as meta-info} (kw-sym info)
            cts (kw-sym @ct-size)
            {:keys [price] :as norm} (binance/normalize p q m T)
            trade (update norm :size #(/ (* % cts) price))]
        (put! channel trade)
        (trade-stats trade tags meta-info))
      (log/warn "unhandled binance-inv message:" payload))))

(defn ct-r-fn [acc {:keys [symbol contractSize contractType]}]
  (let [k (keyword symbol)]
    (if (and (k info) (= :PERPETUAL (keyword contractType)))
      (assoc acc k (double contractSize))
      acc)))

(defn rename [k]
  (keyword (str (name k) "USD_PERP")))

(defn connect! []
  (let [conn @(ws-conn exch (binance/full-url url (keys info)) nil connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (reset! ct-size (get-ct-sizes exch (str api-url ex-info-ep) :symbols ct-r-fn))
  (connect!))

