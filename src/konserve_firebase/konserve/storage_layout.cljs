(ns konserve-firebase.konserve.storage-layout)

(defprotocol LinearLayout
  ;; Location 1: [4-header-bytes 4-bytes-for-meta-size serialized-meta serialized-data]
  (-get-raw [store key])
  (-put-raw [store key blob]))

(defprotocol SplitLayout
  ;; Location 1: [4-header-bytes serialized-meta]
  ;; Location 2: [4-header-bytes serialized-data]
  (-get-raw-meta [store key])
  (-put-raw-meta [store key blob])
  (-get-raw-value [store key])
  (-put-raw-value [store key blob]))