(ns md-aggregator.utils
  (:require [clojure.core.async :refer [<! chan go-loop timeout]]
            [clojure.string :refer [lower-case upper-case]]
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

;; exchange specific map
(defn info-map [existing rename-fn trade-channels]
  (reduce-kv
   (fn [acc sym ch]
     (assoc
      acc
      (rename-fn sym)
      {:channel ch
       :coin-tag (str coin sym)
       :price-gauge (keyword (str (lower-case (name sym)) "-price"))}))
   existing
   trade-channels))

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
(defn trade-stats [{:keys [price size time]} tags {:keys [coin-tag price-gauge]}]
  (let [trade-delay (- (System/currentTimeMillis) time)
        amt (* price size)]
    (statsd/distribution :trade-delay trade-delay tags)
    (statsd/count :trade 1 tags)
    (statsd/count :notional amt (conj tags coin-tag))
    (statsd/gauge price-gauge price tags)))
