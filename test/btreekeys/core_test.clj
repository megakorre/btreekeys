(ns btreekeys.core-test
  (:require [clojure.test :refer :all]
            [btreekeys.core :as bt]))

(defmethod bt/key-structure ::sample
  [_]
  [[:head :long]
   [:body :long]
   [:tail :long]])

(deftest test-make-key
  (is (= (seq (bt/make-key ::sample
                           {:head 123
                            :body 321
                            :tail 765}))
         [0, 0, 0, 0, 0, 0, 0, 123, 0, 0, 0, 0,
          0, 0, 1, 65, 0, 0, 0, 0, 0, 0, 2, -3])))

(deftest test-parse-key
  (let [original {:head 123 :body 321 :tail 765}
        k (bt/make-key ::sample original)]
    (is (= (bt/parse-key ::sample k) original))))

(deftest test-make-prefix
  (is (= (seq (bt/make-key-prefix
                ::sample
                :head 231213112313
                :body 432432434323))
         [0, 0, 0, 53, -43, 96, 27, -7, 0, 0, 0,
          100, -82, -5, 76, -109])))

(deftest keysegment-range-test
  (are [k r] (= r (bt/keysegment-range ::sample k))
    :head [0 7]
    :body [8 15]
    :tail [16 23]))

(deftest test-parse-prefix
  (is (= (bt/parse-key-prefix
           ::sample
           [:head :body]
           (bt/make-key-prefix
             ::sample
             :head 231213112313
             :body 432432434323))
         {:head 231213112313
          :body 432432434323})))

(deftest increment-key-segment-test
  (let [key (bt/make-key
              ::sample
              {:head 123 :body 122 :tail 123})]
    (is (bt/increment-key-segment!
          ::sample :body key))
    (is (= (seq key)
           [0 0 0 0 0 0 0 123
            0 0 0 0 0 0 0 123
            0 0 0 0 0 0 0 123])))
  (let [max-key-source
        [0 0 0 0 0 0 0 1
         127 127 127 127 127 127 127 127
         0 0 0 0 0 0 0 1]
        max-key (byte-array max-key-source)]
    (is (not (bt/increment-key-segment!
               ::sample :body max-key)))
    (is (= (seq max-key) max-key-source))))

(deftest test-extract-keysegment
  (let [key (bt/make-key-prefix
              ::sample
              :head 1 :body 2 :tail 3)]
    (are [k v] (= v (bt/extract-keysegment
                      ::sample k key))
      :head 1
      :body 2
      :tail 3)))
