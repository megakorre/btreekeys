(ns btreekeys.query-test
  (:require
   [btreekeys.core :as bt]
   [btreekeys.query :as q]
   [btreekeys.util :refer [remove-bytes]]
   [clojure.test :refer :all])
  (:import java.util.TreeSet
           java.util.Comparator
           com.google.common.primitives.UnsignedBytes
           java.util.Arrays))

(set! *warn-on-reflection* true)

(deftype SortedSetIterator [^TreeSet coll position]
  q/Iterator
  (next-item [_]
    (when-let [^bytes key (if @position
                     (.higher coll @position)
                     (.first coll))]
      (reset! position key)
      (Arrays/copyOf key (alength key))))
  (current-key [_] @position)
  (seek [_ key-prefix]
    (when-let [^bytes key (.ceiling coll key-prefix)]
      (reset! position key)
      (Arrays/copyOf key (alength key)))))

(defmethod bt/key-structure ::key
  [_]
  [[:head :long]
   [:body :long]
   [:tail :long]])

(defn iterator
  [sorted-set]
  (SortedSetIterator. sorted-set (atom nil)))

(do
  (def ^TreeSet sample-set
    (TreeSet. (UnsignedBytes/lexicographicalComparator)))
  (.add ^TreeSet sample-set
        (bt/make-key ::key {:head 1
                            :body 1
                            :tail 1}))
  (.add sample-set
        (bt/make-key ::key {:head 2
                            :body 1
                            :tail 1}))

  (.add sample-set
        (bt/make-key ::key {:head 1
                            :body 4
                            :tail 230}))

  (.add sample-set
        (bt/make-key ::key {:head 1
                            :body 5
                            :tail 230}))
  (dotimes [body 4]
    (dotimes [tail 5]
      (.add sample-set
            (bt/make-key ::key {:head 1
                                :body body
                                :tail tail})))))

(deftest test-queries
  (are [q v] (= v (map remove-bytes (q/execute-query ::key (iterator sample-set) q)))
    ;; find all
    {}
    (map remove-bytes
         (map
           #(bt/parse-key ::key %)
           sample-set))

    ;; find all start
    {:body {:q :any :start 2}}
    (->> sample-set
         (map #(bt/parse-key ::key %))
         (map remove-bytes)
         (filter #(<= 2 (:body %))))

     ;; find exact match
    {:head {:q :eq :value 1}
     :body {:q :eq :value 1}
     :tail {:q :eq :value 1}}
    [{:head 1 :body 1 :tail 1}]

    ;; find first off every group
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :limit 1}}
    [{:head 1, :body 2, :tail 0}
     {:head 1, :body 3, :tail 0}]

    ;; find first off first existing group
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3 4 5] :limit 1}
     :tail {:q :eq :value 230}}
    [{:head 1, :body 4, :tail 230}]

    ;; return nothing on limit of 0
    {:head {:q :eq :value 1}
     :body {:q :in :values [2]}
     :tail {:q :any :limit 0}}
    []

    ;; find first off every group skip first group
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3] :start 3 :limit 0}
     :tail {:q :any :limit 1}}
    []

    ;; find first off every group skip first group
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3] :start 3}
     :tail {:q :any :limit 1}}
    [{:head 1, :body 3, :tail 0}]

    ;; find first off every group skip first group
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3] :after 2}
     :tail {:q :any :limit 1}}
    [{:head 1, :body 3, :tail 0}]

    ;; find all in every group
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any}}
    [{:head 1, :body 2, :tail 0}
     {:head 1, :body 2, :tail 1}
     {:head 1, :body 2, :tail 2}
     {:head 1, :body 2, :tail 3}
     {:head 1, :body 2, :tail 4}
     {:head 1, :body 3, :tail 0}
     {:head 1, :body 3, :tail 1}
     {:head 1, :body 3, :tail 2}
     {:head 1, :body 3, :tail 3}
     {:head 1, :body 3, :tail 4}]

    ;; find first start 2
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :limit 1 :start 2}}
    [{:head 1, :body 2, :tail 2}
     {:head 1, :body 3, :tail 2}]

    ;; find first 2 start 2
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :limit 2 :start 2}}
    [{:head 1, :body 2, :tail 2}
     {:head 1, :body 2, :tail 3}
     {:head 1, :body 3, :tail 2}
     {:head 1, :body 3, :tail 3}]

    ;; find all start 2
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :start 2}}
    [{:head 1, :body 2, :tail 2}
     {:head 1, :body 2, :tail 3}
     {:head 1, :body 2, :tail 4}
     {:head 1, :body 3, :tail 2}
     {:head 1, :body 3, :tail 3}
     {:head 1, :body 3, :tail 4}]

    ;; find all after 2
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :after 2}}
    [{:head 1, :body 2, :tail 3}
     {:head 1, :body 2, :tail 4}
     {:head 1, :body 3, :tail 3}
     {:head 1, :body 3, :tail 4}]

    {:head {:q :any :limit 1}
     :body {:q :any :limit 1}
     :tail {:q :any :start 2}}
    [{:head 1, :body 0, :tail 2}
     {:head 1, :body 0, :tail 3}
     {:head 1, :body 0, :tail 4}]

    {:head {:q :any :limit 1}
     :body {:q :any :limit 1 :start 1}
     :tail {:q :any :start 2}}
    [{:head 1, :body 1, :tail 2}
     {:head 1, :body 1, :tail 3}
     {:head 1, :body 1, :tail 4}]))
