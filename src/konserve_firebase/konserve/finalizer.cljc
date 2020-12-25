(ns konserve-firebase.konserve.finalizer
  (:require
   [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
   #?@(:cljs [["buffer" :refer [Buffer]]
              [oops.core :refer [ocall oget]]])
   #?(:clj [clojure.java.io :as io]))
  #?(:clj (:import [java.io ByteArrayOutputStream])))

(defn to-byte-array [data]
  #?(:clj (.toByteArray data)
     :cljs (js/Int8Array. data)))

(defn header [store-layout serializer compressor encryptor]
  #?(:clj (let [mbaos (ByteArrayOutputStream.)]
            (.write mbaos ^byte store-layout)
            (.write mbaos ^byte serializer)
            (.write mbaos ^byte compressor)
            (.write mbaos ^byte encryptor)
            mbaos)
     :cljs (js/Int8Array. #js [store-layout serializer compressor encryptor])))

(defn concat-buffers [buffer1 buffer2]
  #?(:clj (do (.write buffer1 buffer2)
              buffer1)
     :cljs (let [new-array (doto (js/Int8Array. (+ (oget buffer1 :length) (oget buffer2 :length)))
                             (ocall :set buffer1)
                             (ocall :set buffer2 (oget buffer1 :length)))
                 new-buffer   (ocall Buffer :from new-array)]
             new-buffer)))

(defn to-vec [bytes]
  #?(:clj (vec bytes)
     :cljs (js/Int8Array. bytes 0 (oget bytes :length))))

(defn to-byte-array-input-stream [data]
  #?(:clj (io/input-stream data)
     :cljs (ocall Buffer :from data)))

(defn split-at! [index data]
  #?(:clj (split-at index data)
     :cljs [(ocall data :subarray 0 index)
            (ocall data :subarray index (oget data :length))]))

(defn split-header [bytes]
  (when bytes
    (let [data (->> bytes to-vec (split-at! 4))
          streamer (fn [header res] (list #?(:clj (vec header)
                                             :cljs (vec header))
                                          #?(:clj (-> res byte-array io/input-stream)
                                             :cljs (ocall Buffer :from res))))
          res (apply streamer data)]
      res)))

(defrecord Finalizer []
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    bytes)
  (-serialize [_ header write-handlers val]
    (concat-buffers header (to-byte-array val))))

(def finalizer
  (Finalizer.))