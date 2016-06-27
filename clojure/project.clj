(defproject geo-window "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [twitter-api "0.7.8"]
                 [clj-time "0.12.0"]
                 [org.apache.kafka/kafka_2.11 "0.10.0.0-cp1" :exclusions [javax.jms/jms
                                                                          com.sun.jdmk/jmxtools
                                                                          com.sun.jmx/jmxri]]
                 [org.apache.kafka/kafka-clients "0.10.0.0-cp1"]
                 [org.apache.kafka/connect-api "0.10.0.0-cp1"]
                 [org.apache.kafka/connect-runtime "0.10.0.0-cp1"]
                 [org.apache.kafka/kafka-streams "0.10.0.0-cp1"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [org.clojure/tools.logging "0.3.1"]]
  :repositories [["confluent" "http://packages.confluent.io/maven/"]]
  ;:main ^:skip-aot geo-window.core
  :aot [geo-window.twitter-timestamp]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
