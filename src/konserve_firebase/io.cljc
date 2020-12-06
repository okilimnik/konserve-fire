(ns konserve-firebase.io
  "IO function for interacting with database"
  (:require [firebase.core :as fire]
            [clojure.string :as str]
            #?(:cljs [oops.core :refer [ocall oget]]))
  (:import  #?(:clj [java.util Base64 Base64$Decoder Base64$Encoder])
            #?(:clj [java.io ByteArrayInputStream])))

#?(:clj (set! *warn-on-reflection* 1))

#?(:clj (def ^Base64$Encoder b64encoder (. Base64 getEncoder)))
#?(:clj (def ^Base64$Decoder b64decoder (. Base64 getDecoder)))

(defn encode-to-string [data]
  #?(:clj (.encodeToString b64encoder ^"[B" data)
     :cljs (ocall data :toString "base64")))

(defn decode [data]
  #?(:clj (.decode b64decoder data)
     :cljs (Buffer. data "base64")))

(defn to-byte-array [data]
  #?(:clj (byte-array data)
     :cljs (js/Uint8Array. data)))

(defn to-byte-array-input-stream [data]
  #?(:clj (ByteArrayInputStream. data)
     :cljs (js->clj (oget (ocall data :toJSON) :data))))

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

(defn split-header [bytes]
  (when bytes
    (let [data  (->> bytes vec (split-at 4))
          streamer (fn [header data] (list (to-byte-array header) (-> data to-byte-array to-byte-array-input-stream)))]
      (apply streamer data))))

(defn prep-write 
  [data]
  (let [[meta val] data]
    {:meta  (when meta 
              (chunk-str (encode-to-string meta)))
     :data  (when val 
              (chunk-str (encode-to-string  val)))}))

(defn prep-read 
  [data']
  (let [meta (combine-str (:meta data'))
        data (combine-str (:data data'))]
    [ (when meta 
        (split-header (decode meta))) 
      (when data  
        (split-header (decode data)))]))

(defn it-exists? 
  [store id]
    (let [resp (fire/read (:db store) (str (:root store) "/" id "/data") {:query {:shallow true}})]
    (some? resp)))
  
(defn get-it 
  [store id]
  (let [resp (fire/read (:db store) (str (:root store) "/" id))]
    (prep-read resp)))

(defn get-it-only 
  [store id]
  (let [resp (fire/read (:db store) (str (:root store) "/" id "/data"))]
    (when resp (->> resp ^String (combine-str) decode split-header))))

(defn get-meta
  [store id]
  (let [resp (fire/read (:db store) (str (:root store) "/" id "/meta"))]
    (when resp (->> resp ^String (combine-str) decode split-header))))

(defn update-it 
  [store id data]
  (fire/update! (:db store) (str (:root store) "/" id) (prep-write data) {:print "silent"}))

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
  (let [resp (fire/read (:db store) (str (:root store) "/" id "/data"))]
    (when resp (->> resp ^String (combine-str) decode))))

(defn raw-get-meta 
  [store id]
  (let [resp (fire/read (:db store) (str (:root store) "/" id "/meta"))]
    (when resp (->> resp ^String (combine-str) decode))))
  
(defn raw-update-it-only 
  [store id data]
  (when data
    (fire/update! (:db store) (str (:root store) "/" id "/data") 
      (chunk-str (encode-to-string data)) {:print "silent"})))

(defn raw-update-meta
  [store id meta]
  (when meta
    (fire/write! (:db store) (str (:root store) "/" id "/meta") 
      (chunk-str (encode-to-string meta)) {:print "silent"})))
