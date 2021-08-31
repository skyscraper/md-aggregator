(ns md-aggregator.huobi-inv
  (:require [byte-streams :as bs]
            [clojure.core.async :refer [>! go]]
            [clojure.java.io :refer [reader]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.huobi :as huobi]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume info-map inv-true trade-stats ws-conn]]
            [taoensso.timbre :as log])
  (:import (java.util.zip GZIPInputStream)))

(def url "wss://api.hbdm.vn/swap-ws")
(def tags [(str "exch" huobi/exch) inv-true])
(def ws-timeout 20000)
(def info {})
(def connection (atom nil))
(def ws-props {:max-frame-payload 131072})

(defn handle [raw]
  (with-open [rdr (reader (GZIPInputStream. (bs/to-input-stream raw)))]
    (let [{:keys [ch subbed status ping tick] :as payload}
          (json/read-value rdr json/keyword-keys-object-mapper)]
      (statsd/count :ws-msg 1 tags)
      (cond
        (some? ch) (let [{:keys [channel] :as meta-info} ((keyword ch) info)]
                     (when channel
                       (go
                         (doseq [x (:data tick)
                                 :let [trade (huobi/normalize x)]]
                           (>! channel trade)
                           (trade-stats trade tags meta-info)))))
        (some? subbed) (log/info subbed "subscription status:" status)
        (some? ping) (s/put! @connection (json/write-value-as-string {:pong ping}))
        :else (log/warn "unhandled huobi-inv message: " payload)))))

(defn rename [k]
  (keyword (format huobi/base (str (name k) "-USD"))))

(defn connect! []
  (let [conn @(ws-conn huobi/exch url ws-props connect!)]
    (reset! connection conn)
    (consume huobi/exch conn ws-timeout handle)
    (huobi/subscribe conn (keys info))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))

