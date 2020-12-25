(ns konserve-firebase.io
  "IO function for interacting with database"
  (:require [firebase.core :as fire]
            [clojure.string :as str]
            [konserve-firebase.konserve.finalizer :as finalizer]
            #?(:clj [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            #?(:cljs ["buffer" :refer [Buffer]])
            #?(:cljs [oops.core :refer [ocall]]))
  (:import  #?@(:clj [[java.util Base64 Base64$Decoder Base64$Encoder]
                      [java.io ByteArrayOutputStream]])))


#?(:clj (set! *warn-on-reflection* 1))

#?(:clj (def ^Base64$Encoder b64encoder (. Base64 getEncoder)))
#?(:clj (def ^Base64$Decoder b64decoder (. Base64 getDecoder)))

(defn encode-to-string [data]
  #?(:clj (.encodeToString b64encoder (.toByteArray ^ByteArrayOutputStream data))
     :cljs (ocall data :toString "base64")))

(defn decode [^String data]
  #?(:clj (.decode b64decoder data)
     :cljs (ocall Buffer :from data "base64")))

(defn chunk-str [string]
  (when string
    (let [len (count string)
          chunk-map  (if (> len 5000000)
                       (let [chunks (str/split string #"(?<=\G.{5000000})")]
                         (for [n (range (count chunks))]
                           {(str "p" n) (nth chunks n)}))
                       {:p0 string})]
      (apply merge {} chunk-map))))

(defn combine-str [data-str]
  (when data-str
    (->> (dissoc data-str :headers) (into (sorted-map)) vals vec str/join)))

(defn prep-write
  [data]
  (let [[meta val] data]
    {:meta  (when meta
              (chunk-str (encode-to-string meta)))
     :data  (when val
              (chunk-str (encode-to-string  val)))}))

(defn prep-read
  [data']
  (let [res (let [meta (combine-str (:meta data'))
                  data (combine-str (:data data'))]
              [(when meta
                 (finalizer/split-header (decode meta)))
               (when data
                 (finalizer/split-header (decode data)))])]
    res))

(defn it-exists?
  [store id]
  (async/go
    (let [ch (async/chan)]
      (fire/read (:db store) (str (:root store) "/" id "/data") {:async ch})
      (boolean (seq (async/<! ch))))))

(defn get-it
  [store id]
  (async/go
    (let [ch (async/chan)]
      (fire/read (:db store) (str (:root store) "/" id) {:async ch})
      (let [resp (async/<! ch)]
        (prep-read resp)))))

(defn get-it-only
  [store id]
  (async/go
    (let [ch (async/chan)]
      (fire/read (:db store) (str (:root store) "/" id "/data") {:async ch})
      (let [resp (async/<! ch)]
 
        (when (seq resp) (->> resp combine-str decode finalizer/split-header))))))

(defn get-meta
  [store id]
  (async/go
    (let [ch (async/chan)]
      (fire/read (:db store) (str (:root store) "/" id "/meta") {:async ch})
      (let [resp (async/<! ch)]

        (when (seq resp) (->> resp ^String (combine-str) decode finalizer/split-header))))))

(defn update-it
  [store id data]

  (async/go
    (let [ch (async/chan)]

      (fire/update! (:db store) (str (:root store) "/" id) (prep-write data) {:async ch})
      (async/<! ch))))

(defn delete-it
  [store id]
  (fire/delete! (:db store) (str (:root store) "/" id)))

(defn get-keys
  [store]
  (let [resp (fire/read (:db store) (str (:root store)) {:query {:shallow true}})
        key-stream (seq (keys resp))
        getmeta (fn [id] (get-meta store (name id)))]
    (map getmeta key-stream)))

(defn raw-get-it-only
  [store id]
  (async/go
    (let [ch (async/chan)]
      (fire/read (:db store) (str (:root store) "/" id "/data") {:async ch})
      (let [resp (async/<! ch)]
        (when (seq resp) (->> resp ^String (combine-str) decode))))))

(defn raw-get-meta
  [store id]
  (async/go
    (let [ch (async/chan)]
      (fire/read (:db store) (str (:root store) "/" id "/meta") {:async ch})
      (let [resp (async/<! ch)]
        (when (seq resp) (->> resp ^String (combine-str) decode))))))

(defn raw-update-it-only
  [store id data]
  (when data
    (fire/update! (:db store) (str (:root store) "/" id "/data")
                  (chunk-str (encode-to-string data)) {:print "silent"})))

(defn raw-update-meta
  [store id meta]
  (async/go
    (let [ch (async/chan)]
      (if meta
        (do (fire/write! (:db store) (str (:root store) "/" id "/meta")
                         (chunk-str (encode-to-string meta)) {:async ch})
            (async/<! ch))
        []))))
