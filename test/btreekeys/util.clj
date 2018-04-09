(ns btreekeys.util
  (:require  [clojure.test :as t]))

(defn remove-bytes
  [m]
  (dissoc m :btreekeys/bytes))
