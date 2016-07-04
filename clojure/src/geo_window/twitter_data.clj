(ns geo-window.twitter-data
  (:use
   [twitter.oauth]
   [twitter.callbacks]
   [twitter.callbacks.handlers]
   [twitter.api.streaming])
  (:require
   [clojure.data.json :as json]
   [http.async.client :as ac]
   [twitter.callbacks :as cb]
   [clojure.string :as str]
   [clojure.tools.logging :as log])
  (:import [twitter.callbacks.protocols AsyncStreamingCallback]
           (kafka.consumer Consumer ConsumerConfig KafkaStream)
           (kafka.producer KeyedMessage ProducerConfig)
           (kafka.javaapi.producer Producer)
           (java.util Properties)
           (java.util.concurrent Executors)
           [java.util Date]))

;;; Kafka producer for tweets

(def kafka-broker "127.0.0.1:9092")
(def kafka-twitter-topic "raw-tweets")

(defn- create-producer
  "Creates a producer that can be used to send a message to Kafka"
  [brokers]
  (let [props (Properties.)]
    (doto props
      (.put "metadata.broker.list" brokers)
      (.put "serializer.class" "kafka.serializer.StringEncoder")
      (.put "request.required.acks" "1"))
    (Producer. (ProducerConfig. props))))

(defn- send-to-producer
  "Send a string message to Kafka"
  [producer topic message]
  (let [data (KeyedMessage. topic nil message)]
    (.send producer data)))

(defn producer-callback
  []
  (let [producer (create-producer kafka-broker)]
    (fn [s]
      (send-to-producer producer kafka-twitter-topic s))))

;;; Twitter API runner for tweets

;;; TODO move these defs into the env
(def app-consumer-key (System/getenv "APP_CONSUMER_KEY"))
(def app-consumer-secret (System/getenv "APP_CONSUMER_SECRET"))
(def user-access-token (System/getenv "USER_ACCESS_TOKEN"))
(def user-access-token-secret (System/getenv "USER_ACCESS_TOKEN_SECRET"))

(def my-creds
  (make-oauth-creds app-consumer-key
                    app-consumer-secret
                    user-access-token
                    user-access-token-secret))

(defn split-tweets
  "Take a string and break it into a seq of complete tweets and the leftover text"
  [s]
  (let [m (re-matcher #"(?s)(.*?)\r\n" s)
        matches (take-while identity (repeatedly #(re-find m)))
        total-len (reduce + (map #(.length (first %)) matches))]
    [(seq (map second matches)) (subs s total-len)]))

(defn group-by-tweet
  "Call f each time we get a batch of text separated by \\r\\n which is how twitter organizes its
  stream"
  [f]
  (let [accum (atom "")]                ; I assume that the responses are single threaded, otherwise
    (fn [_ baos]                        ; there's no way to make sense of the stream
      (let [s (str @accum baos)
            [tweets tail] (split-tweets s)]
        (swap! accum (constantly tail))
        (doseq [t tweets]
          (f t))))))

(def last-tweet (atom nil))

(def save-last-tweet-callback
  (let [cb (group-by-tweet #(swap! last-tweet (constantly (->> %1 json/read-json))))]
    (AsyncStreamingCallback. cb ; (comp println #(:text %) json/read-json #(str %2))
                             (comp println response-return-everything)
                             exception-print)))

(def save-last-geotweet-callback
  (let [cb (group-by-tweet #(let [parsed (->> %1 json/read-json)]
                              (when (:geo parsed)
                               (swap! last-tweet (constantly parsed)))))]
    (AsyncStreamingCallback. cb ; (comp println #(:text %) json/read-json #(str %2))
                             (comp println response-return-everything)
                             exception-print)))

(def tweet-producer-callback
  (let [to-kafka (producer-callback)
        cb (group-by-tweet to-kafka)]
    (AsyncStreamingCallback. cb ; (comp println #(:text %) json/read-json #(str %2))
                             (comp println response-return-everything)
                             exception-print)))

(def text-print-callback
  (let [cb (group-by-tweet #(-> %1 json/read-json :text println))]
    (AsyncStreamingCallback. cb ; (comp println #(:text %) json/read-json #(str %2))
                             (comp println response-return-everything)
                             exception-print)))

;;; Code to cancel and restart the feed if we don't hear from it for a number of seconds

(defn current-millis
  []
  (.getTime (Date.)))

(defn resilient-status
  [params creds callback]
  (let [last-update (atom (current-millis))
        running (atom true)
        wrapped-callback (AsyncStreamingCallback.
                          (fn [& args]
                            (swap! last-update (fn [_] (current-millis)))
                            (apply (:on-bodypart callback) args))
                          (:on-failure callback)
                          (:on-exception callback))
        action (atom (statuses-filter :params params :oauth-creds creds :callbacks wrapped-callback))
        cancel-fn (fn []
                    (swap! running (constantly false))
                    ((:cancel (meta @action))))
        restart-thread (Thread. (fn []
                                  (while @running
                                    (when (> (- (current-millis) @last-update) (* 60 1000))
                                      ((:cancel (meta @action)))
                                      (swap! action (fn [_] (statuses-filter :params params :oauth-creds creds
                                                                             :callbacks wrapped-callback)))
                                      (log/warn "Restarting twitter status stream"))
                                    (Thread/sleep (* 30 1000)))))]
    (.start restart-thread)
    {:cancel cancel-fn :restart-thread restart-thread :running running :last-update last-update
     :action action}))

(defn cancel
  [action]
  ((:cancel action)))

(defn warriors []
  (statuses-filter :params {:track "Warriors"}
                   :oauth-creds my-creds
                   :callbacks text-print-callback))

(defn sf-tweets []
  (resilient-status {:locations "-122.55,37.7040,-122.3549,37.8324"} my-creds tweet-producer-callback))
