(ns md-aggregator.ftx
  (:require [clojure.core.async :refer [>! go]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [epoch info-map ping-loop trade-stats ws-conn]]
            [taoensso.timbre :as log]))

(def url "wss://ftx.com/ws/")
(def exch :ftx)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))
(def ws-props {:max-frame-payload 131072})
(def ping (json/write-value-as-string {:op :ping}))
(def ping-interval 15000)

(defn subscribe [conn markets]
  (doseq [market markets]
    (s/put! conn (json/write-value-as-string {:op :subscribe
                                              :channel :trades
                                              :market market}))))

(defn handle [raw]
  (let [{:keys [channel market type code msg data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (condp = (keyword type)
      :update (let [{:keys [channel] :as meta-info} ((keyword market) info)]
                (go
                  (doseq [x data
                          :let [trade (-> (update x :time epoch)
                                          (update :side keyword)
                                          (assoc :source exch))]]
                    (>! channel trade)
                    (trade-stats trade tags meta-info))))
      :partial (log/warn (format "received partial event: %s" payload))
      :info (do (log/info (format "ftx info: %s %s" code msg))
                (when (= code 20001)
                  (log/info (name exch) "server requested us to reconnect...")
                  (.close @connection))) ;; theoretically registered callback will fire
      :subscribed (log/info (format "subscribed to %s %s" market channel))
      :unsubscribed (log/info (format "unsubscribed from %s %s" market channel))
      :error (log/error (format "ftx error: %s %s" code msg))
      :pong (log/debug "pong")
      (log/warn (str "unhandled ftx event: " payload)))))

(defn rename [k]
  (keyword (str (name k) "-PERP")))

(defn connect! []
  (let [conn @(ws-conn exch url ws-props connect!)]
    (reset! connection conn)
    (s/consume handle conn)
    (ping-loop conn ping-interval ping)
    (subscribe conn (keys info))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))

