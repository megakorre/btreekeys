(ns btreekeys.query-test
  (:require
   [btreekeys.core :as bt]
   [btreekeys.query :as q]
   [clojure.test :refer :all])
  (:import java.util.TreeSet
           java.util.Comparator
           com.google.common.primitives.UnsignedBytes
           com.google.common.primitives.SignedBytes
           java.util.Arrays))

(deftype SortedSetIterator [coll position]
  q/Iterator
  (current-key [_] @position)
  (seek [_ key-prefix]
    (when-let [key (.ceiling coll key-prefix)]
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
  (def sample-set
    (TreeSet. (UnsignedBytes/lexicographicalComparator)))
  (.add sample-set
        (bt/make-key ::key {:head 1
                            :body 1
                            :tail 1}))
  (.add sample-set
        (bt/make-key ::key {:head 2
                            :body 1
                            :tail 1}))
  (dotimes [body 4]
    (dotimes [tail 5]
      (.add sample-set
            (bt/make-key ::key {:head 1
                                :body body
                                :tail tail})))))

(deftest test-queries
  (are [q v] (= v (seq (q/execute-query ::key (iterator sample-set) q)))

    ;; find first off every group
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :limit 1}}
    [{:head 1, :body 2, :tail 0}
     {:head 1, :body 3, :tail 0}]

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


    ;; find first after 2
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :limit 1 :after 2}}
    [{:head 1, :body 2, :tail 2}
     {:head 1, :body 3, :tail 2}]

    ;; find all after 2
    {:head {:q :eq :value 1}
     :body {:q :in :values [2 3]}
     :tail {:q :any :after 2}}
    [{:head 1, :body 2, :tail 2}
     {:head 1, :body 2, :tail 3}
     {:head 1, :body 2, :tail 4}
     {:head 1, :body 3, :tail 2}
     {:head 1, :body 3, :tail 3}
     {:head 1, :body 3, :tail 4}]

    {:head {:q :any :limit 1}
     :body {:q :any :limit 1}
     :tail {:q :any :after 2}}
    [{:head 1, :body 0, :tail 2}
     {:head 1, :body 0, :tail 3}
     {:head 1, :body 0, :tail 4}]

    {:head {:q :any :limit 1}
     :body {:q :any :limit 1 :after 1}
     :tail {:q :any :after 2}}
    [{:head 1, :body 1, :tail 2}
     {:head 1, :body 1, :tail 3}
     {:head 1, :body 1, :tail 4}]))
