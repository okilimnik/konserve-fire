(ns konserve-firebase.core
  "Address globally aggregated immutable key-value store(s)."
  (:require #?(:clj [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [konserve-firebase.konserve.serializers :as ser]
            [konserve-firebase.konserve.compressor :as comp]
            [konserve-firebase.konserve.encryptor :as encr]
            #?(:cljs ["buffer" :refer [Buffer]])
            #?(:cljs [oops.core :refer [ocall oget]])
            [konserve-firebase.konserve.finalizer :as fin]
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
            [konserve-firebase.konserve.storage-layout :refer [SplitLayout]]))

#?(:clj (set! *warn-on-reflection* 1))

(def store-layout 1)

(defn str-uuid
  [key]
  (str (hasch/uuid key)))

(defn prep-ex
  [^String message e]
  #?(:clj (ex-info message {:error (.getMessage ^Exception e) :cause (.getCause ^Exception e) :trace (.getStackTrace ^Exception e)})
     :cljs (ex-info message {:error (js->clj e :keywordize-keys true)})))

(defn prep-stream
  [stream]
  {:input-stream stream
   :size nil})

(defn read-data [header res read-handlers]
  (let [serializer (ser/byte->serializer (get header 1))
        compressor (comp/byte->compressor (get header 2))
        encryptor  (encr/byte->encryptor  (get header 3))
        reader (-> fin/finalizer serializer compressor encryptor)]
    (-deserialize reader read-handlers res)))

(defn get-writer []
  (let [serializer (ser/byte->serializer  1)
        compressor (comp/byte->compressor 1)
        encryptor  (encr/byte->encryptor 0)]
    (-> fin/finalizer encryptor compressor serializer)))

(defn get-header []
  (fin/header store-layout 1 1 0))

(defrecord FireStore [store default-serializer serializers compressor encryptor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists?
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (async/put! res-ch (async/<! (io/it-exists? store (str-uuid key))))
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
      res-ch))

  (-get
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (async/<! (io/get-it-only store (str-uuid key)))]
            (if (some? res)
              (async/put! res-ch (read-data header res read-handlers))
              (async/close! res-ch)))
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-meta store (str-uuid key))]
            (if (some? res)
              (async/put! res-ch (read-data header res read-handlers))
              (async/close! res-ch)))
          (catch #?(:clj Exception
                    :cljs js/Error)
                 e (async/put! res-ch (prep-ex "Failed to retrieve metadata from store" e)))))
      res-ch))

  (-update-in
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/go
        (let [[fkey & rkey] key-vec
              [[mheader ometa'] [vheader oval']] (async/<! (io/get-it store (str-uuid fkey)))
              old-val [(when ometa'
                         (read-data mheader ometa' read-handlers))
                       (when oval'
                         (read-data vheader oval' read-handlers))]
              [nmeta nval] [(meta-up-fn (first old-val))
                            (if rkey
                              (apply update-in (second old-val) rkey up-fn args)
                              (apply up-fn (second old-val) args))]
              writer (get-writer)
              mbaos (atom nil)
              vbaos (atom nil)]
          (when nmeta
            (reset! mbaos (-serialize writer (get-header) write-handlers nmeta)))
          (when nval
            (reset! vbaos (-serialize writer (get-header) write-handlers nval)))
          (async/<! (io/update-it store (str-uuid fkey) [@mbaos @vbaos]))
          (async/put! res-ch [(second old-val) nval])))
      res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc
    [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (io/delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
      res-ch))

  PBinaryAsyncKeyValueStore
  (-bget
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (if (some? res)
              (async/put! res-ch (locked-cb (prep-stream (read-data header res read-handlers))))
              (async/close! res-ch)))
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[[mheader old-meta'] [_ old-val]] (io/get-it store (str-uuid key))
                old-meta (when old-meta'
                           (read-data mheader old-meta' read-handlers))
                new-meta (meta-up-fn old-meta)
                writer (get-writer)
                mbaos (atom nil)
                vbaos (atom nil)]
            (when new-meta
              (reset! mbaos (-serialize writer (get-header) write-handlers new-meta)))
            (when input
              (reset! vbaos (-serialize writer (get-header) write-handlers input)))
            (async/<! (io/update-it store (str-uuid key) [@mbaos @vbaos]))
            (async/put! res-ch [old-val input]))
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to write binary value in store" e)))))
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
                          (read-data header k read-handlers)))
                keys (doall (map :key keys'))]
            (doall
             (map #(async/put! res-ch %) keys))
            (async/close! res-ch))
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
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
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to retrieve raw metadata from store" e)))))
      res-ch))
  (-put-raw-meta [this key binary]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (io/raw-update-meta store (str-uuid key) binary)
          (async/close! res-ch)
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to write raw metadata to store" e)))))
      res-ch))
  (-get-raw-value [this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [res (io/raw-get-it-only store (str-uuid key))]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to retrieve raw value from store" e)))))
      res-ch))
  (-put-raw-value [this key binary]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (io/raw-update-it-only store (str-uuid key) binary)
          (async/close! res-ch)
          (catch #?(:clj Exception
                    :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to write raw value to store" e)))))
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
            (throw (prep-ex "No database specified and one could not be automatically determined." #?(:clj (Exception.)
                                                                                                      :cljs (js/Error.)))))
          (async/put! res-ch
                      (map->FireStore {:store {:db final-db :root final-root}
                                       :default-serializer default-serializer
                                       :serializers (merge ser/key->serializer serializers)
                                       :compressor compressor
                                       :encryptor encryptor
                                       :read-handlers read-handlers
                                       :write-handlers write-handlers
                                       :locks (atom {})})))
        (catch #?(:clj Exception
                  :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))
    res-ch))

(defn delete-store [fire-store]
  (let [res-ch (async/chan 1)]
    (async/go
      (try
        (let [store (:store fire-store)]
          (fire/delete! (:db store) (str (:root store)))
          (async/close! res-ch))
        (catch #?(:clj Exception
                  :cljs js/Error) e (async/put! res-ch (prep-ex "Failed to delete store" e)))))
    res-ch))
