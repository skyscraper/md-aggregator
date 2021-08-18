(ns md-aggregator.okex
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :refer [parse-string generate-string]]
            [clojure.core.async :refer [put!]]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [coin re-key key-mapping trade-stats]]
            [taoensso.timbre :as log]))

(def api-url "https://aws.okex.com/api/v5/public")
(def inst-ep "/instruments?instType=SWAP")
(def url "wss://wsaws.okex.com:8443/ws/v5/public")
(def exch :okex)
(def tags [(str "exch" exch)])
(def ping "ping")
(def connection (atom nil))
(def trade-channels (atom {}))
(def coin-tags (atom {}))
(def ct-size (atom {}))

(defn subscribe [conn instruments]
  (let [base {:channel :trades}]
    (s/put! conn (generate-string
                  {:op :subscribe :args (mapv #(assoc base :instId %) instruments)}))))

(defn handle [raw]
  (let [{:keys [arg data] :as payload} (parse-string raw true)
        {:keys [channel instId]} arg]
    (statsd/count :ws-msg 1 tags)
    (condp = (keyword channel)
      :trades (when instId
                (let [kw-inst (keyword instId)]
                  (when-let [c (kw-inst @trade-channels)]
                    (doseq [{:keys [px sz side ts]} data
                            :let [updated {:price (Double/parseDouble px)
                                           :size (* (Double/parseDouble sz) (kw-inst @ct-size))
                                           :side (keyword side)
                                           :time (Long/parseLong ts)
                                           :source exch}]]
                      (put! c updated)
                      (trade-stats (:price updated) (:size updated) (:time updated)
                                   tags (kw-inst @coin-tags))))))
      (log/warn (str "unhandled okex event: " payload)))))

(defn reset-ct-sizes! []
  (let [data (-> (str api-url inst-ep)
                 http/get
                 deref
                 :body
                 bs/to-string
                 (parse-string true)
                 :data)]
    (reset! ct-size (reduce (fn [acc {:keys [instId ctVal ctType]}]
                              (let [k (keyword instId)]
                                (if (and (k @coin-tags) (= :linear (keyword ctType)))
                                  (assoc acc k (Double/parseDouble ctVal))
                                  acc)))
                            {}
                            data))))

(defn rename [k]
  (keyword (str (name k) "-USDT-SWAP")))

(defn init [t-channels]
  (let [conn @(http/websocket-client url {:epoll? true
                                          :compression? true
                                          :heartbeats {:send-after-idle 3e4
                                                       :payload ping
                                                       :timeout 3e4}})]
    (reset! trade-channels (re-key t-channels rename))
    (reset! coin-tags (key-mapping t-channels rename #(str coin %)))
    (reset! connection conn)
    (reset-ct-sizes!)
    (s/consume handle conn)
    (subscribe conn (keys @trade-channels))))

