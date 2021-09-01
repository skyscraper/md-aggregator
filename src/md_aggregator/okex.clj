(ns md-aggregator.okex
  (:require [jsonista.core :as json]
            [manifold.stream :as s]
            [md-aggregator.statsd :as statsd]
            [md-aggregator.utils :refer [consume get-ct-sizes info-map inv-false
                                         process subscribe ws-conn]]
            [taoensso.timbre :as log]))

(def api-url "https://aws.okex.com/api/v5/public")
(def inst-ep "/instruments?instType=SWAP")
(def url "wss://wsaws.okex.com:8443/ws/v5/public")
(def exch :okex)
(def tags [(str "exch" exch) inv-false])
(def ws-timeout 20000)
(def info {})
(def connection (atom nil))
(def ws-props {:max-frame-payload 131072
               :compression? true
               :heartbeats {:send-after-idle 3e4
                            :payload "ping"
                            :timeout 3e4}})
(def sub-base {:channel :trades})
(def ct-size (atom {}))

(defn normalize [{:keys [px sz side ts]} cts]
  {:price (Double/parseDouble px)
   :size (* (Double/parseDouble sz) cts)
   :side (keyword side)
   :time (Long/parseLong ts)
   :source exch})

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
                        trades (map #(normalize % cts) data)]
                    (process trades tags (kw-inst info))))
        (log/warn (str "unhandled okex event: " payload))))))

(defn ct-r-fn [acc {:keys [instId ctVal ctType]}]
  (let [k (keyword instId)]
    (if (and (k info) (= :linear (keyword ctType)))
      (assoc acc k (Double/parseDouble ctVal))
      acc)))

(defn rename [k]
  (keyword (str (name k) "-USDT-SWAP")))

(defn connect! []
  (let [conn @(ws-conn exch url ws-props connect!)]
    (reset! connection conn)
    (consume exch conn ws-timeout handle)
    (subscribe conn [{:op :subscribe
                      :args (mapv #(assoc sub-base :instId %) (keys info))}])
    (s/on-closed conn connect!)))

(defn init [trade-channels]
  (alter-var-root #'info info-map rename trade-channels)
  (reset! ct-size (get-ct-sizes exch (str api-url inst-ep) :data ct-r-fn))
  (connect!))
