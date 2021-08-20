(ns md-aggregator.ftx
  (:require [aleph.http :as http]
            [clojure.core.async :refer [put!]]
            [jsonista.core :as json]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [epoch info-map ping-loop trade-stats]]
            [taoensso.timbre :as log]))

(def url "wss://ftx.com/ws/")
(def exch :ftx)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))
(def ping (json/write-value-as-string {:op :ping}))
(def ping-interval 15000)

(defn subscribe [conn markets]
  (doseq [market markets]
    (s/put! conn (json/write-value-as-string {:op :subscribe :channel :trades :market market}))))

(defn handle [raw]
  (let [{:keys [channel market type code msg data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (condp = (keyword type)
      :update (let [{:keys [channel] :as meta-info} ((keyword market) info)]
                (doseq [x data
                        :let [trade (-> (update x :time epoch)
                                        (update :side keyword)
                                        (assoc :source exch))]]
                  (put! channel trade)
                  (trade-stats trade tags meta-info)))
      :partial (log/warn (format "received partial event: %s" payload)) ;; not currently implemented
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

(declare connect!)

(defn ws-conn []
  (d/catch
      (http/websocket-client url {:epoll? true
                                  :max-frame-payload 131072})
      (fn [e]
        (log/error (name exch) "ws problem:" e)
        (connect!))))

(defn connect! []
  (let [conn @(ws-conn)]
    (log/info "connecting to" (name exch) "...")
    (reset! connection conn)
    (s/consume handle conn)
    (ping-loop conn ping-interval ping)
    (subscribe conn (keys info))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))

