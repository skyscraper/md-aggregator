(ns md-aggregator.okex
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [clojure.core.async :refer [>! go]]
            [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [info-map trade-stats ws-conn]]
            [taoensso.timbre :as log]))

(def api-url "https://aws.okex.com/api/v5/public")
(def inst-ep "/instruments?instType=SWAP")
(def url "wss://wsaws.okex.com:8443/ws/v5/public")
(def exch :okex)
(def tags [(str "exch" exch)])
(def info {})
(def connection (atom nil))
(def ws-props {:max-frame-payload 131072
               :compression? true
               :heartbeats {:send-after-idle 3e4
                            :payload "ping"
                            :timeout 3e4}})
(def ct-size (atom {}))

(defn subscribe [conn instruments]
  (let [base {:channel :trades}]
    (s/put! conn (json/write-value-as-string
                  {:op :subscribe :args (mapv #(assoc base :instId %) instruments)}))))

(defn handle [raw]
  (let [{:keys [event arg data] :as payload}
        (json/read-value raw json/keyword-keys-object-mapper)
        {:keys [channel instId]} arg]
    (statsd/count :ws-msg 1 tags)
    (if event
      (log/info event channel instId)
      (condp = (keyword channel)
        :trades (when instId
                  (let [kw-inst (keyword instId)
                        cts (kw-inst @ct-size)
                        {:keys [channel] :as meta-info} (kw-inst info)]
                    (when channel
                      (go
                        (doseq [{:keys [px sz side ts]} data
                                :let [trade {:price (Double/parseDouble px)
                                             :size (* (Double/parseDouble sz) cts)
                                             :side (keyword side)
                                             :time (Long/parseLong ts)
                                             :source exch}]]
                          (>! channel trade)
                          (trade-stats trade tags meta-info))))))
        (log/warn (str "unhandled okex event: " payload))))))

(defn reset-ct-sizes! []
  (log/info "resetting ct sizes...")
  (let [data (-> (str api-url inst-ep)
                 http/get
                 deref
                 :body
                 bs/to-string
                 (json/read-value json/keyword-keys-object-mapper)
                 :data)]
    (reset! ct-size (reduce (fn [acc {:keys [instId ctVal ctType]}]
                              (let [k (keyword instId)]
                                (if (and (k info) (= :linear (keyword ctType)))
                                  (assoc acc k (Double/parseDouble ctVal))
                                  acc)))
                            {}
                            data))))

(defn rename [k]
  (keyword (str (name k) "-USDT-SWAP")))

(defn connect! []
  (let [conn @(ws-conn exch url ws-props connect!)]
    (reset! connection conn)
    (s/consume handle conn)
    (subscribe conn (keys info))
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (reset-ct-sizes!)
  (connect!))

