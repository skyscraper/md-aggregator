(ns md-aggregator.utils
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [clojure.core.async :refer [<! >! chan go go-loop timeout]]
            [clojure.string :refer [lower-case upper-case]]
            [java-time :refer [instant zoned-date-time]]
            [jsonista.core :as json]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [taoensso.timbre :as log]))

;; constants
(def coin-str "coin")
(def side-str "side")
(def inv-true "inverse:true")
(def inv-false "inverse:false")

;; formatting
(defn uc-kw
  "upper-case keyword"
  [kw-or-str]
  (-> kw-or-str name upper-case keyword))

;; core.async
(defn generate-channel-map [symbols]
  (reduce
   #(assoc %1 (uc-kw %2) (chan 10000))
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
       :coin-tag (str coin-str sym)
       :price-gauge (keyword (str (lower-case (name sym)) "-price"))}))
   existing
   trade-channels))

;; time
(defn epoch [dt-str]
  (.toEpochMilli (instant (zoned-date-time dt-str))))

;; ws
(defn ws-conn [exch url props connect-fn]
  (log/info "connecting to" (name exch) "...")
  (d/catch
      (http/websocket-client url (merge {:epoll? true} props))
      (fn [e]
        (log/error (name exch) "ws problem:" e)
        (connect-fn))))

(defn subscribe [conn payloads]
  (doseq [p payloads]
    (s/put! conn (json/write-value-as-string p))))

;; consume
(defn consume [exch conn ws-timeout handle-fn]
  (go-loop []
    (if-let [raw @(s/try-take! conn ws-timeout)]
      (do
        (handle-fn raw)
        (recur))
      (do
        (log/error (format "Stopped receiving %s websocket data! Closing connection..." (name exch)))
        (s/close! conn)))))

;; ping
(defn ping-loop [conn interval payload]
  (go-loop []
    (<! (timeout interval))
    (when @(s/put! conn payload)
      (recur))))

;; contract sizing
(defn get-ct-sizes [exch url data-kw r-fn]
  (log/info (format "fetching ct sizes for %s..." (name exch)))
  (let [data (-> url
                 http/get
                 deref
                 :body
                 bs/to-string
                 (json/read-value json/keyword-keys-object-mapper)
                 data-kw)]
    (reduce r-fn {} data)))

;; trade stats
(defn trade-stats [{:keys [price size time side]} tags {:keys [coin-tag price-gauge]}]
  (let [trade-delay (- (System/currentTimeMillis) time)
        amt (* price size)]
    (statsd/distribution :trade-delay trade-delay tags)
    (statsd/count :trade 1 tags)
    (statsd/count :notional amt (conj tags coin-tag (str side-str side)))
    (statsd/gauge price-gauge price tags)))

;; process incoming market data and report stats
(defn process-single [trade tags {:keys [channel] :as meta-info}]
  (go
    (when (>! channel trade)
      (trade-stats trade tags meta-info))))

(defn process [trades tags {:keys [channel] :as meta-info}]
  (go-loop [vs (seq trades)]
    (when (and vs (>! channel (first vs)))
      (trade-stats (first vs) tags meta-info)
      (recur (next vs)))))
