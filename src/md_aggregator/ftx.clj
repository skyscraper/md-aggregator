(ns md-aggregator.ftx
  (:require [aleph.http :as http]
            [cheshire.core :refer [parse-string generate-string]]
            [clojure.core.async :refer [put!]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [coin epoch ping-loop re-key key-mapping trade-stats]]
            [taoensso.timbre :as log]))

(def url "wss://ftx.com/ws/")
(def exch :ftx)
(def tags [(str "exch" exch)])
(def ping (generate-string {:op :ping}))
(def ping-interval 15000)
(def connection (atom nil))
(def trade-channels (atom {}))
(def coin-tags (atom {}))

(defn subscribe [conn chan markets]
  (doseq [market markets]
    (s/put! conn (generate-string {:op :subscribe :channel chan :market market}))))

(defn handle [raw]
  (let [{:keys [channel market type code msg data] :as payload}
        (-> (parse-string raw true)
            (update :type keyword)
            (update :market keyword))]
    (statsd/count :ws-msg 1 tags)
    (condp = type
      :update (when-let [c (market @trade-channels)]
                (doseq [trade data
                        :let [{:keys [price size time] :as updated}
                              (-> (update trade :time epoch)
                                  (update :side keyword)
                                  (assoc :source exch))]]
                  (put! c updated)
                  (trade-stats price size time tags (market @coin-tags))))
      :partial (log/warn (format "received partial event: %s" payload)) ;; not currently implemented
      :info (do (log/info (format "ftx info: %s %s" code msg))
                (when (= code 20001)
                  (let [conn @(http/websocket-client url {:epoll? true})]
                    (reset! connection conn)
                    (s/consume handle conn)
                    (ping-loop conn ping-interval ping)
                    (subscribe conn :trades (keys @trade-channels)))))
      :subscribed (log/info (format "subscribed to %s %s" market channel))
      :unsubscribed (log/info (format "unsubscribed from %s %s" market channel))
      :error (log/error (format "ftx error: %s %s" code msg))
      :pong (log/debug "pong")
      (log/warn (str "unhandled ftx event: " payload)))))

(defn rename [k]
  (keyword (str (name k) "-PERP")))

(defn init [t-channels]
  (let [conn @(http/websocket-client url {:epoll? true})]
    (reset! trade-channels (re-key t-channels rename))
    (reset! coin-tags (key-mapping t-channels rename #(str coin %)))
    (reset! connection conn)
    (s/consume handle conn)
    (ping-loop conn ping-interval ping)
    (subscribe conn :trades (keys @trade-channels))))

