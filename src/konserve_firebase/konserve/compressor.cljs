(ns konserve-firebase.konserve.compressor
  (:require
   ["lz4" :as LZ4]
   ["buffer" :refer [Buffer]]
   [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
   [konserve.utils :refer [invert-map]]
   [oops.core :refer [ocall oget]]))

(defrecord NullCompressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defn encode [bytes]
  (let [input (ocall Buffer :from bytes)
        input-length (oget input :length)
        encoded (ocall LZ4 :encodeBound input-length)
        output (ocall Buffer :alloc encoded)
        compressed-size (ocall LZ4 :encodeBlock input output)]
    (ocall output :slice 0 compressed-size)))

(defn decode [bytes]
  (let [input (ocall Buffer :from bytes)
        uncompressed (ocall Buffer :alloc (* (oget input :length) 5))
        uncompressed-size (ocall LZ4 :decodeBlock bytes uncompressed)]
    (ocall uncompressed :slice 0 uncompressed-size)))

(defrecord Lz4Compressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
                (let [lz4-byte (decode bytes)]
                  (-deserialize serializer read-handlers lz4-byte)))
  (-serialize [_ bytes write-handlers val]
              (let [lz4-byte (encode val)]
                (-serialize serializer bytes write-handlers lz4-byte))))

(defn null-compressor [serializer]
  (NullCompressor. serializer))

(defn lz4-compressor [serializer]
  (Lz4Compressor. serializer))

(def byte->compressor
  {0 null-compressor
   1 lz4-compressor})

(def compressor->byte
  (invert-map byte->compressor))