(ns geo-window.stream
  (:import
   [com.fasterxml.jackson.databind JsonNode]
   [com.fasterxml.jackson.databind.node JsonNodeFactory]
   [com.fasterxml.jackson.databind.node ObjectNode]
   [org.apache.kafka.clients.consumer ConsumerConfig]
   [org.apache.kafka.common.serialization Deserializer]
   [org.apache.kafka.common.serialization Serde]
   [org.apache.kafka.common.serialization Serdes]
   [org.apache.kafka.common.serialization Serializer]
   [org.apache.kafka.connect.json JsonSerializer]
   [org.apache.kafka.connect.json JsonDeserializer]
   [org.apache.kafka.streams KafkaStreams]
   [org.apache.kafka.streams KeyValue]
   [org.apache.kafka.streams StreamsConfig]
   [org.apache.kafka.streams.kstream KStreamBuilder]
   [org.apache.kafka.streams.kstream KStream]
   [org.apache.kafka.streams.kstream KTable]
   [org.apache.kafka.streams.kstream KeyValueMapper]
   [org.apache.kafka.streams.kstream Predicate]
   [org.apache.kafka.streams.kstream TimeWindows]
   [org.apache.kafka.streams.kstream ValueJoiner]
   [org.apache.kafka.streams.kstream ValueMapper]
   [org.apache.kafka.streams.kstream Windowed]
   [java.util Properties]
   [geo_window twitter_timestamp])
  (:require [clojure.data.json :as json]
            [geo-window.hexbin :as hexbin]))

(def default-props
  (doto (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG "twitter-users2")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092")
    (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG "localhost:2181")
    (.put StreamsConfig/TIMESTAMP_EXTRACTOR_CLASS_CONFIG twitter_timestamp)

    ;; setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")))

(defmacro pred
  [args & body]
  `(reify
     Predicate
     (test [this ~@args]
       (boolean (do ~@body)))))

(defmacro kv-mapper
  [args & body]
  `(reify
     KeyValueMapper
     (apply [this ~@args]
       ~@body)))

(defn trace-deser
  []
  (let [deser (JsonDeserializer.)]
    (reify
      Deserializer
      (configure [this configs isKey]
        (.configure deser configs isKey))
      (deserialize [this topic data]
        (when data
          (try
            (.deserialize deser topic data)
            (catch Exception e
              (binding [*out* *err*]
                (println "Exception:" e)
                (println "deser data:" (String. data "UTF-8")))))))
      (close [this]
        (.close deser)))))

(defn add-hexbin-key
  "Adds a key of \"lon,lat\" with the center of the bin in which this tweet goes"
  [radius point0]
  (let [hb (hexbin/hexbinner radius point0)]
    (kv-mapper
     [k v]
     (if v
       (let [coords (-> v (.get "geo") (.get "coordinates"))
             lat (-> coords (.get 0) .doubleValue)
             lon (-> coords (.get 1) .doubleValue)
             [h-lon h-lat] (hb [lon lat])]
         (KeyValue. (format "%f,%f" h-lon h-lat) v))
       (KeyValue. k v)))))

(defn stream
  "Testing streams from my twitter topic"
  [topic-in topic-out]
  (let [builder (KStreamBuilder.)
        json-serde (Serdes/serdeFrom (JsonSerializer.) (trace-deser))
        raw-tweets (.stream builder (Serdes/String) json-serde (into-array String [topic-in]))
        geo-only (-> raw-tweets (.filterNot (pred [k v]
                                        ; (binding [*out* *err*] (println "filter" (.get v "geo")))
                                                  (when v (-> v (.get "geo") .isNull)))))
        hexbinned (-> geo-only (.map (add-hexbin-key (double 1/240) [0.0 0.0])))
        counts (-> hexbinned
                   (.filterNot (pred [k v] (nil? k)))
                   (.countByKey (Serdes/String) "Tweets2"))
        _ (.to counts (Serdes/String) (Serdes/Long) topic-out)
        ; _ (.to raw-tweets (Serdes/String) json-serde topic-out)
        streams (KafkaStreams. builder default-props)]
    (.start streams)
    (Thread/sleep 50000)
    (.close streams)))
