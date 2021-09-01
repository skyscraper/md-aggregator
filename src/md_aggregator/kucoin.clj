(ns md-aggregator.kucoin
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [clojure.core.async :refer [<! go-loop timeout]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume info-map inv-false
                                         process-single subscribe ws-conn]]
            [taoensso.timbre :as log]))

(def api-url "https://api-futures.kucoin.com")
(def token-ep "/api/v1/bullet-public")
(def exec-ep "/contractMarket/execution")
(def exch :kucoin)
(def tags [(str "exch" exch) inv-false])
(def ws-timeout 60000)
(def info {})
(def connection (atom nil))
(def ping {:type :ping})

(defn sub-payload [instrument]
  ;; TODO: find actual trade data endpoint... doesn't seem to exist
  {:id (System/currentTimeMillis)
   :type :subscribe
   :topic (str exec-ep instrument)
   :response true})

(defn on-connect [conn interval]
  (go-loop []
    (<! (timeout interval))
    (when @(s/put! conn (json/write-value-as-string (assoc ping :id (System/currentTimeMillis))))
      (recur))))

(defn normalize [{:keys [side matchSize price time]}]
  {:price (double price)
   :size (double matchSize)
   :side (keyword side)
   :time (long (/ time 1e5))
   :source exch})

(defn handle [raw]
  (let [{:keys [type subject data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)]
    (statsd/count :ws-msg 1 tags)
    (condp = (keyword type)
      :message (when (= :match (keyword subject))
                 (process-single (normalize data) tags ((keyword (:symbol data)) info)))
      :welcome (log/info "kucoin token accepted")
      :ack (log/info "kucoin subscription acknowledged")
      (log/warn "unhandled kucoin message:" payload))))

(defn rename [k]
  (keyword (str (name (if (= :BTC k) :XBT k)) "USDTM")))

(defn full-url [endpoint token]
  (str endpoint "?token=" token "&connectId=" (System/currentTimeMillis)))

(defn connect! []
  (let [{:keys [token instanceServers]} (-> (str api-url token-ep)
                                            http/post
                                            deref
                                            :body
                                            bs/to-string
                                            (json/read-value json/keyword-keys-object-mapper)
                                            :data)
        {:keys [endpoint pingInterval]} (first instanceServers)
        conn @(ws-conn exch (full-url endpoint token) nil connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (on-connect conn pingInterval)
    (subscribe conn (map sub-payload (keys info)))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (connect!))
