(ns md-aggregator.kucoin
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :refer [parse-string generate-string]]
            [clojure.core.async :refer [<! put! go-loop timeout]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [info-map trade-stats]]
            [taoensso.timbre :as log]))

(def api-url "https://api-futures.kucoin.com")
(def token-ep "/api/v1/bullet-public")
(def exch :kucoin)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))
(def ping {:type :ping})

(defn on-connect [conn interval]
  (go-loop []
    (<! (timeout interval))
    (when @(s/put! conn (generate-string (assoc ping :id (System/currentTimeMillis))))
      (recur))))

(defn subscribe [conn instruments]
  ;; TODO: find actual trade data endpoint... doesn't seem to exist
  (doseq [instrument instruments]
    (s/put! conn (generate-string
                  {:id (System/currentTimeMillis)
                   :type :subscribe
                   :topic (str "/contractMarket/execution" instrument)
                   :response true}))))

(defn handle [raw]
  (let [{:keys [type subject data] :as payload} (parse-string raw true)]
    (statsd/count :ws-msg 1 tags)
    (condp = (keyword type)
      :message (when (= :match (keyword subject))
                 (let [{:keys [symbol side matchSize price time]} data
                       kw-sym (keyword symbol)
                       {:keys [channel] :as meta-info} (kw-sym info)]
                   (when channel
                     (let [trade {:price (double price)
                                  :size (double matchSize)
                                  :side (keyword side)
                                  :time (long (/ time 1e5))
                                  :source exch}]
                       (put! channel trade)
                       (trade-stats trade tags meta-info)))))
      :welcome (log/info "kucoin token accepted")
      :ack (log/info "kucoin subscription acknowledged")
      (log/warn "unhandled kucoin message:" payload))))

(defn rename [k]
  (keyword (str (name (if (= :BTC k) :XBT k)) "USDTM")))

(defn init [trade-channels]
  (let [{:keys [token instanceServers]} (-> (str api-url token-ep)
                                            http/post
                                            deref
                                            :body
                                            bs/to-string
                                            (parse-string true)
                                            :data)
        {:keys [endpoint pingInterval]} (first instanceServers)
        conn @(http/websocket-client
               (str endpoint "?token=" token "&connectId=" (System/currentTimeMillis))
               {:epoll? true})]
    (alter-var-root #'info info-map rename trade-channels)
    (reset! connection conn)
    (s/consume handle conn)
    (on-connect conn pingInterval)
    (subscribe conn (keys info))))

