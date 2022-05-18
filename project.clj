(defproject md-aggregator "0.1.0-SNAPSHOT"
  :description "crypto market data aggregator"
  :url "http://github.com/skyscraper/md-aggregator"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"
            :distribution :repo}
  :dependencies [[aleph "0.4.7"]
                 [byte-streams "0.2.5-alpha2"]
                 [clojure.java-time "0.3.3"]
                 [com.datadoghq/java-dogstatsd-client "4.0.0"]
                 [com.taoensso/timbre "5.2.1"]
                 [environ "1.2.0"]
                 [metosin/jsonista "0.3.5"]
                 [nrepl "0.9.0"]
                 [org.clojure/clojure "1.11.1"]
                 [org.clojure/core.async "1.5.648"]]
  :repl-options {:init-ns md-aggregator.core}
  :main md-aggregator.core
  :aot :all)
