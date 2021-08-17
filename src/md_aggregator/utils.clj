(ns md-aggregator.utils
  (:require [clojure.core.async :refer [<! chan go-loop timeout]]
            [clojure.string :refer [upper-case]]
            [java-time :refer [instant zoned-date-time]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]))

;; constants
(def coin "coin")

;; formatting
(defn uc-kw
  "upper-case keyword"
  [kw-or-str]
  (-> kw-or-str name upper-case keyword))

;; core.async
(defn generate-channel-map [symbols]
  (reduce
   #(assoc %1 (uc-kw %2) (chan 1000))
   {}
   symbols))

;; maps
(defn re-key [x rename-fn]
  (reduce-kv
   (fn [m k v] (assoc m (rename-fn k) v))
   {}
   x))

(defn key-mapping [x key-fn value-fn]
  (reduce-kv
   (fn [m k _] (assoc m (key-fn k) (value-fn k)))
   {}
   x))

;; time
(defn epoch [dt-str]
  (.toEpochMilli (instant (zoned-date-time dt-str))))

;; ping
(defn ping-loop [conn interval payload]
  (go-loop []
    (<! (timeout interval))
    (when @(s/put! conn payload)
      (recur))))

;; trade stats
(defn trade-stats [price size time tags coin-tag]
  (let [trade-delay (- (System/currentTimeMillis) time)
        amt (* price size)]
    (statsd/distribution :trade-delay trade-delay tags)
    (statsd/count :trade 1 tags)
    (statsd/count :notional amt (conj tags coin-tag))))
