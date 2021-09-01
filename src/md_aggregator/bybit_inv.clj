(ns md-aggregator.bybit-inv
  (:require [clojure.string :refer [join lower-case]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.bybit :as bybit]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume info-map inv-true ping-loop
                                         process subscribe ws-conn]]
            [taoensso.timbre :as log]))

(def url "wss://stream.bybit.com/realtime")
(def exch :bybit-inv)
(def tags [(str "exch" bybit/exch) inv-true])
(def ws-timeout 30000)
(def info {})
(def connection (atom nil))
(def ping (json/write-value-as-string {:op :ping}))
(def ping-interval 30000)
(def sub-base {:op :subscribe})

(defn normalize [{:keys [price size trade_time_ms side]}]
  {:price (double price)
   :size (double (/ size price))
   :side (keyword (lower-case side))
   :time trade_time_ms
   :source bybit/exch})

(defn handle [raw]
  (let [{:keys [request success ret_msg topic data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (cond
      (some? request) (if success
                        (condp = (keyword (:op request))
                          :subscribe (log/info "subscribed to" (join "," (:args request)))
                          :ping (log/debug "pong")
                          (log/info request))
                        (log/warn ret_msg))
      (some? topic) (process (map normalize data) tags ((keyword topic) info))
      :else (log/warn (str "unhandled bybit-inv event:" payload)))))

(defn rename [k]
  (keyword (format "trade.%sUSD" (name k))))

(defn connect! []
  (let [conn @(ws-conn exch url nil connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (ping-loop conn ping-interval ping)
    (subscribe conn [(assoc sub-base :args (keys info))])
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))
