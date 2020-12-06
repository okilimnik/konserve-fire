(ns konserve-firebase.core
  "Address globally aggregated immutable key-value store(s)."
  (:require #?(:clj [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [konserve.serializers :as ser]
            #?(:clj [konserve.compressor :as comp]
               :cljs [konserve-firebase.konserve.compressor :as comp])
            #?(:clj [konserve.encryptor :as encr]
               :cljs [konserve-firebase.konserve.encryptor :as encr])
            #?(:cljs ["buffer" :refer [Buffer]])
            #?(:cljs [oops.core :refer [ocall oget]])
            [hasch.core :as hasch]
            [konserve-firebase.io :as io]
            [firebase.core :as fire]
            [clojure.string :as str]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]]
            #?(:clj [konserve.storage-layout :refer [SplitLayout]]
               :cljs [konserve-firebase.konserve.storage-layout :refer [SplitLayout]]))
  (:import  #?(:clj [java.io ByteArrayOutputStream])))

#?(:clj (set! *warn-on-reflection* 1))

(defn empty-byte-array []
  #?(:clj (ByteArrayOutputStream.)
     :cljs (Buffer.)))

(defn to-byte-array [mbaos]
  #?(:clj (.toByteArray mbaos)
     :cljs mbaos))

(defn to-bytes [store-layout serializer compressor encryptor]
  #?(:clj (let [mbaos (ByteArrayOutputStream.)]
            (.write mbaos ^byte store-layout)
            (.write mbaos ^byte serializer)
            (.write mbaos ^byte compressor)
            (.write mbaos ^byte encryptor))
     :cljs (ocall Buffer :from (js/Uint8Array #js [store-layout serializer compressor encryptor]))))

(def Error #?(:clj Exception
              :cljs js/Error))

(def store-layout 1)

(defn str-uuid
  [key]
  (str (hasch/uuid key)))

(defn prep-ex
  [^String message e]
  #?(:clj (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)})
     :cljs (ex-info message {:error (js->clj e :keywordize-keys true)})))

(defn prep-stream
  [stream]
  {:input-stream stream
   :size nil})

(defrecord FireStore [store default-serializer serializers compressor encryptor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists?
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (async/put! res-ch (io/it-exists? store (str-uuid key)))
          (catch Error e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
      res-ch))

  (-get
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (if (some? res)
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Error e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-meta store (str-uuid key))]
            (if (some? res)
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Error e (async/put! res-ch (prep-ex "Failed to retrieve metadata from store" e)))))
      res-ch))

  (-update-in
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[fkey & rkey] key-vec
                [[mheader ometa'] [vheader oval']] (io/get-it store (str-uuid fkey))
                old-val [(when ometa'
                           (let [mserializer (ser/byte->serializer  (get mheader 1))
                                 mcompressor (comp/byte->compressor (get mheader 2))
                                 mencryptor  (encr/byte->encryptor  (get mheader 3))
                                 reader (-> mserializer mencryptor mcompressor)]
                             (-deserialize reader read-handlers ometa')))
                         (when oval'
                           (let [vserializer (ser/byte->serializer  (get vheader 1))
                                 vcompressor (comp/byte->compressor (get vheader 2))
                                 vencryptor  (encr/byte->encryptor  (get vheader 3))
                                 reader (-> vserializer vencryptor vcompressor)]
                             (-deserialize reader read-handlers oval')))]
                [nmeta nval] [(meta-up-fn (first old-val))
                              (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                mbaos (atom (empty-byte-array))
                vbaos (atom (empty-byte-array))]
            (when nmeta
              (reset! mbaos (to-bytes
                             store-layout
                             (ser/serializer-class->byte (type serializer))
                             (comp/compressor->byte compressor)
                             (encr/encryptor->byte encryptor)))
              (-serialize writer mbaos write-handlers nmeta))
            (when nval
              (reset! vbaos (to-bytes
                             store-layout
                             (ser/serializer-class->byte (type serializer))
                             (comp/compressor->byte compressor)
                             (encr/encryptor->byte encryptor)))
              (-serialize writer vbaos write-handlers nval))
            (io/update-it store (str-uuid fkey) [(to-byte-array @mbaos) (to-byte-array @vbaos)])
            (async/put! res-ch [(second old-val) nval]))
          (catch Error e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
      res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (io/delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Error e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
      res-ch))

  PBinaryAsyncKeyValueStore
  (-bget
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (if (some? res)
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch (locked-cb (prep-stream data))))
              (async/close! res-ch)))
          (catch Error e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[[mheader old-meta'] [_ old-val]] (io/get-it store (str-uuid key))
                old-meta (when old-meta'
                           (let [mserializer (ser/byte->serializer  (get mheader 1))
                                 mcompressor (comp/byte->compressor (get mheader 2))
                                 mencryptor  (encr/byte->encryptor  (get mheader 3))
                                 reader (-> mserializer mencryptor mcompressor)]
                             (-deserialize reader read-handlers old-meta')))
                new-meta (meta-up-fn old-meta)
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                mbaos (atom nil)
                vbaos (atom nil)]
            (when new-meta
              (reset! mbaos (to-bytes store-layout
                                      (ser/serializer-class->byte (type serializer))
                                      (comp/compressor->byte compressor)
                                      (encr/encryptor->byte encryptor)))
              (-serialize writer mbaos write-handlers new-meta))
            (when input
              (reset! vbaos (to-bytes store-layout
                                      (ser/serializer-class->byte (type serializer))
                                      (comp/compressor->byte compressor)
                                      (encr/encryptor->byte encryptor)))
              (-serialize writer vbaos write-handlers input))
            (io/update-it store (str-uuid key) [(to-byte-array @mbaos) (to-byte-array @vbaos)])
            (async/put! res-ch [old-val input]))
          (catch Error e (async/put! res-ch (prep-ex "Failed to write binary value in store" e)))))
      res-ch))

  PKeyIterable
  (-keys
    [_]
    (let [res-ch (async/chan)]
      (async/go
        (try
          (let [key-stream (io/get-keys store)
                keys' (when key-stream
                        (for [[header k] key-stream]
                          (let [rserializer (ser/byte->serializer (get header 1))
                                rcompressor (comp/byte->compressor (get header 2))
                                rencryptor  (encr/byte->encryptor  (get header 3))
                                reader (-> rserializer rencryptor rcompressor)]
                            (-deserialize reader read-handlers k))))
                keys (doall (map :key keys'))]
            (doall
             (map #(async/put! res-ch %) keys))
            (async/close! res-ch))
          (catch Error e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
      res-ch))

  SplitLayout
  (-get-raw-meta [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [res (io/raw-get-meta store (str-uuid key))]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Error e (async/put! res-ch (prep-ex "Failed to retrieve raw metadata from store" e)))))
      res-ch))
  (-put-raw-meta [this key binary]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (io/raw-update-meta store (str-uuid key) binary)
          (async/close! res-ch)
          (catch Error e (async/put! res-ch (prep-ex "Failed to write raw metadata to store" e)))))
      res-ch))
  (-get-raw-value [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [res (io/raw-get-it-only store (str-uuid key))]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Error e (async/put! res-ch (prep-ex "Failed to retrieve raw value from store" e)))))
      res-ch))
  (-put-raw-value [this key binary]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (io/raw-update-it-only store (str-uuid key) binary)
          (async/close! res-ch)
          (catch Error e (async/put! res-ch (prep-ex "Failed to write raw value to store" e)))))
      res-ch)))

(defn new-firebase-store
  "Creates an new store based on Firebase's realtime database."
  [{:keys [root db default-serializer serializers compressor encryptor read-handlers write-handlers]
    :or  {root "/konserve-firebase"
          db nil
          default-serializer :FressianSerializer
          compressor comp/lz4-compressor
          encryptor encr/null-encryptor
          read-handlers (atom {})
          write-handlers (atom {})}}]
  (let [res-ch (async/chan 1)]
    (async/go
      (try
        (let [final-db db
              final-root (if (str/starts-with? root "/") root (str "/" root))]
          (when-not final-db
            (throw (prep-ex "No database specified and one could not be automatically determined." (Error.))))
          (async/put! res-ch
                      (map->FireStore {:store {:db final-db :root final-root}
                                       :default-serializer default-serializer
                                       :serializers (merge ser/key->serializer serializers)
                                       :compressor compressor
                                       :encryptor encryptor
                                       :read-handlers read-handlers
                                       :write-handlers write-handlers
                                       :locks (atom {})})))
        (catch Error e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))
    res-ch))

(defn delete-store [fire-store]
  (let [res-ch (async/chan 1)]
    (async/go
      (try
        (let [store (:store fire-store)]
          (fire/delete! (:db store) (str (:root store)))
          (async/close! res-ch))
        (catch Error e (async/put! res-ch (prep-ex "Failed to delete store" e)))))
    res-ch))
