(ns geo-window.twitter-timestamp
  (:gen-class
   :implements [org.apache.kafka.streams.processor.TimestampExtractor])
  (:import [com.fasterxml.jackson.databind JsonNode]
           [org.apache.kafka.clients.consumer ConsumerRecord]
           [org.apache.kafka.streams.processor TimestampExtractor]))

(defn -extract
  "Get the timestamp from the object"
  [this ^ConsumerRecord record]
  (let [val (.value record)]
    (cond
      (instance? JsonNode val) (-> val (.get "timestamp_ms") .asText Long/parseLong)
      (nil? val) 0
      :else (throw (IllegalArgumentException.
                    (str "twitter-timestamp doesn't recognize " val))))))
