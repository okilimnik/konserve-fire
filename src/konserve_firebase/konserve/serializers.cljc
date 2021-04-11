(ns konserve-firebase.konserve.serializers
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            #?@(:clj [[clojure.data.fressian :as fress]
                      [incognito.fressian :refer [incognito-read-handlers
                                                  incognito-write-handlers]]])
            #?@(:cljs [[fress.api :as fress]
                       [fress.writer :as w]
                       ["buffer" :refer [Buffer]]
                       #?(:cljs [oops.core :refer [ocall oget]])
                       [incognito.fressian :refer [incognito-read-handlers incognito-write-handlers]]])
            [incognito.edn :refer [read-string-safe]])
  #?(:clj (:import [java.io FileOutputStream FileInputStream DataInputStream DataOutputStream]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [java.io ByteArrayOutputStream])))

#?(:clj
   (defrecord FressianSerializer [serializer custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [res (fress/read bytes
                             :handlers (-> (merge fress/clojure-read-handlers
                                                  custom-read-handlers
                                                  (incognito-read-handlers read-handlers))
                                           fress/associative-lookup))]
         (if serializer
           (-deserialize serializer read-handlers res)
           res)))
     (-serialize [_ bytes write-handlers val]
       (let [baos (ByteArrayOutputStream.)
             w (fress/create-writer baos :handlers (-> (merge
                                                        fress/clojure-write-handlers
                                                        custom-write-handlers
                                                        (incognito-write-handlers write-handlers))
                                                       fress/associative-lookup
                                                       fress/inheritance-lookup))]
         (fress/write-object w val)
         (if serializer
           (-serialize serializer bytes write-handlers baos)
           baos)))))

#?(:cljs
   (defrecord FressianSerializer [serializer custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [reader   (fress/create-reader bytes
                                           :handlers (merge custom-read-handlers
                                                            (incognito-read-handlers read-handlers)))
             res (fress/read-object reader)]
         (if serializer
           (-deserialize serializer read-handlers res)
           res)))
     (-serialize [_ bytes write-handlers val]
       (let [buf (fress/byte-stream)
             writer (fress/create-writer buf
                                         :handlers (merge
                                                    custom-write-handlers
                                                    (incognito-write-handlers write-handlers)))]
         (fress/write-object writer val)
         (if serializer
           (-serialize serializer bytes write-handlers @buf)
           @buf)))))

(defn fressian-serializer
  ([serializer] (fressian-serializer serializer {} {}))
  ([serializer read-handlers write-handlers] (map->FressianSerializer {:serializer serializer
                                                                       :custom-read-handlers read-handlers
                                                                       :custom-write-handlers write-handlers})))
(defrecord StringSerializer []
  PStoreSerializer
  (-deserialize [_ read-handlers s]
    (read-string-safe @read-handlers s))
  (-serialize [_ output-stream _ val]
    #?(:clj
       (binding [clojure.core/*out* output-stream]
         (pr val)))
    #?(:cljs
       (pr-str val))))

(defn string-serializer []
  (map->StringSerializer {}))

(defn construct->class [m]
  (->> (map (fn [[k v]] [(case k
                           0 StringSerializer
                           1 FressianSerializer) k]) m)
       (into {})))

(def byte->serializer
  {0 string-serializer
   1 fressian-serializer})

(def serializer-class->byte
  (construct->class byte->serializer))

(defn construct->keys [m]
  (->> (map (fn [[k v]] [(case k
                           0 :StringSerializer
                           1 :FressianSerializer) v]) m)
       (into {})))

(def key->serializer
  (construct->keys byte->serializer))

#?(:clj (defn construct->byte [m n]
          (->> (map (fn [[k0 v0] [k1 v1]] [k0 k1]) m n)
               (into {}))))

#?(:clj (def byte->key
          (construct->byte byte->serializer key->serializer)))