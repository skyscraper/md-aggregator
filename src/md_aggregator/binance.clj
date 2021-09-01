(ns md-aggregator.binance
  (:require [clojure.string :refer [join lower-case]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume info-map inv-false process-single
                                         ws-conn]]
            [taoensso.timbre :as log]))

(def url "wss://fstream.binance.com")
(def exch :binance)
(def tags [(str "exch" exch) inv-false])
(def ws-timeout 10000)
(def info {})
(def connection (atom nil))

(defn normalize [p q m T]
  {:price (Double/parseDouble p)
   :size (Double/parseDouble q)
   :side (if m :sell :buy)
   :time T
   :source exch})

(defn handle [raw]
  (let [{:keys [s p q T m] :as payload} (:data (json/read-value raw json/keyword-keys-object-mapper))]
    (statsd/count :ws-msg 1 tags)
    (if s
      (process-single (normalize p q m T) tags ((keyword s) info))
      (log/warn "unhandled binance message:" payload))))

(defn rename [k]
  (keyword (str (name k) "USDT")))

(defn full-url [base syms]
  (str
   base
   "/stream?streams="
   (join "/" (map #(str (lower-case (name %)) "@trade") syms))))

(defn connect! []
  (let [conn @(ws-conn exch (full-url url (keys info)) nil connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))
