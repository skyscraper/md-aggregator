(ns md-aggregator.binance-inv
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [clojure.core.async :refer [put!]]
            [clojure.string :refer [join lower-case]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume info-map trade-stats ws-conn]]
            [taoensso.timbre :as log]))

(def api-url "https://dapi.binance.com")
(def ex-info-ep "/dapi/v1/exchangeInfo")
(def url "wss://dstream.binance.com")
(def exch :binance-inv)
(def tags [(str "exch" exch)])
(def ws-timeout 10000)
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
            price (Double/parseDouble p)
            trade {:price price
                   :size (/ (* (Double/parseDouble q) cts) price) ;; inverse future
                   :side (if m :sell :buy)
                   :time T
                   :source exch}]
        (put! channel trade)
        (trade-stats trade tags meta-info))
      (log/warn "unhandled binance-inv message:" payload))))

(defn reset-ct-sizes! []
  (log/info "resetting ct sizes...")
  (let [data (-> (str api-url ex-info-ep)
                 http/get
                 deref
                 :body
                 bs/to-string
                 (json/read-value json/keyword-keys-object-mapper)
                 :symbols)]
    (reset! ct-size (reduce (fn [acc {:keys [symbol contractSize contractType]}]
                              (let [k (keyword symbol)]
                                (if (and (k info) (= :PERPETUAL (keyword contractType)))
                                  (assoc acc k (double contractSize))
                                  acc)))
                            {}
                            data))))

(defn rename [k]
  (keyword (str (name k) "USD_PERP")))

(defn full-url [syms]
  (str
   url
   "/stream?streams="
   (join "/" (map #(str (lower-case (name %)) "@trade") syms))))

(def full-url-memo (memoize full-url))

(defn connect! []
  (let [conn @(ws-conn exch (full-url-memo (keys info)) nil connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (reset-ct-sizes!)
  (connect!))

