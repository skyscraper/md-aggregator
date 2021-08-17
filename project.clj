(defproject md-aggregator "0.1.0-SNAPSHOT"
  :description "crypto market data aggregator"
  :url "http://github.com/skyscraper/md-aggregator"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"
            :distribution :repo}
  :dependencies [[aleph "0.4.7-alpha7"]
                 [byte-streams "0.2.5-alpha2"]
                 [cheshire "5.10.1"]
                 [clojure.java-time "0.3.2"]
                 [com.datadoghq/java-dogstatsd-client "2.13.0"]
                 [com.taoensso/timbre "5.1.2"]
                 [environ "1.2.0"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.3.618"]]
  :repl-options {:init-ns md-aggregator.core}
  :main md-aggregator.core
  :aot :all)
