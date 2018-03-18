(ns btreekeys.query
  (:require
   [btreekeys.core :as bt]
   [clojure.pprint :refer [pprint]])
  (:import java.util.Arrays))

(defprotocol Iterator
  (seek [this key-prefix])
  (current-key [this]))

(defmacro seek-in-prefix
  "seeks but returns nil if the seek left the bounds
   of the prefix"
  [structure-type prefix-keys iterator-binding key-binding]
  (let [prefix-structure (bt/prefix-structure
                           structure-type prefix-keys)
        prefix-size (bt/structure-size prefix-structure)]
    `(let [prefix-val-before# (Arrays/copyOfRange
                                ~key-binding 0 ~prefix-size)]
       (when-let [key# (seek ~iterator-binding ~key-binding)]
         (when (Arrays/equals
                 prefix-val-before#
                 (Arrays/copyOfRange key# 0 ~prefix-size))
           key#)))))

(defn normalize-query
  [structure query-map]
  (for [[keysegment-key keysegment-type] structure
        :let [value (query-map keysegment-key)]]
    [keysegment-key
     (if (or (nil? value)
             (= value :any))
       {:any true}
       value)]))

(defmethod bt/key-structure ::keyref
  [_]
  [[:df-type :long]
   [:join-keyref :md5]
   [:subtag :md5]
   [:key-hash :md5]
   [:date :rev-date]
   [:df-seq-id :long]])

(defmulti query-code :match-type)

(defmethod query-code :in
  [{:keys [match-value keysegment-key]} cont]
  (fn [prefix-bindings]
    (let [value-binding (gensym (str (name keysegment-key) "-in-binding"))]
      `(doseq [~value-binding (sort ~match-value)]
         ~(cont (assoc prefix-bindings
                       keysegment-key value-binding))))))

(defmethod query-code :eq
  [{:keys [match-value keysegment-key] :as context} cont]
  (query-code
    (assoc context :match-type :in :match-value [match-value])
    cont))

(defn- with-seq-and-continue
  [{:keys [prefix-bindings cont prefix-keys]
    :or {prefix-keys (keys prefix-bindings)}
    {:keys [structure-type
            key-binding
            iterator-binding
            keysegment-key]} :context}
   & body]
  (let [value-binding (gensym (name keysegment-key))]
    `(when-let [~key-binding (seek-in-prefix
                               ~structure-type
                               ~(into [] prefix-keys)
                               ~iterator-binding ~key-binding)]
       (let [~value-binding (bt/extract-keysegment
                              ~structure-type
                              ~keysegment-key
                              ~key-binding)]
         ~(cont (assoc prefix-bindings
                       keysegment-key value-binding)))
       ~@body)))

(defn- make-binding-prefix
  [structure-type prefix-bindings]
  `(bt/make-key-prefix
     ~structure-type
     ~@(flatten (seq prefix-bindings))))

(defmethod query-code :any
  [{:keys [keysegment-key iterator-binding key-binding structure-type]
    :as context} cont]
  (fn [prefix-bindings]
    `(loop [~key-binding ~(make-binding-prefix structure-type prefix-bindings)]
       ~(with-seq-and-continue
          {:context context
           :prefix-bindings prefix-bindings
           :cont cont}
          `(when (bt/increment-key-segment!
                   ~structure-type ~keysegment-key
                   ~key-binding)
             (recur ~key-binding))))))

(defmethod query-code :first
  [{:keys [keysegment-key iterator-binding structure-type key-binding]
    :as context} cont]
  (fn [prefix-bindings]
    `(let [~key-binding ~(make-binding-prefix structure-type prefix-bindings)]
       ~(with-seq-and-continue
          {:context context
           :prefix-bindings prefix-bindings
           :cont cont}))))

(defmethod query-code :first-after
  [{:keys [keysegment-key iterator-binding match-value structure-type
           key-binding]
    :as context} cont]
  (fn [prefix-bindings]
    (let [prefix-keys (keys prefix-bindings)
          prefix-bindings (assoc prefix-bindings keysegment-key match-value)]
      `(let [~key-binding ~(make-binding-prefix structure-type prefix-bindings)]
         ~(with-seq-and-continue
            {:context context
             :prefix-keys prefix-keys
             :prefix-bindings prefix-bindings
             :cont cont})))))

(defmethod query-code :after
  [{:keys [keysegment-key iterator-binding match-value structure-type
           key-binding]
    :as context} cont]
  (fn [prefix-bindings]
    (let [match-binding (gensym (str (name keysegment-key) "-after"))
          prefix-keys (keys prefix-bindings)
          prefix-bindings (assoc prefix-bindings
                                 keysegment-key
                                 match-binding)]
      `(let [~match-binding ~match-value]
         (loop [~key-binding ~(make-binding-prefix structure-type prefix-bindings)]
           ~(with-seq-and-continue
              {:context context
               :prefix-keys prefix-keys
               :prefix-bindings prefix-bindings
               :cont cont}
              `(when (bt/increment-key-segment!
                       ~structure-type ~keysegment-key
                       ~key-binding)
                 (recur ~key-binding))))))))

(deftype QueryIterator [iterator runner]
  clojure.lang.Seqable
  (seq [this]
    (seq (into [] this)))
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (runner iterator rf init))
  clojure.lang.Sequential)

(defmethod print-method QueryIterator [v ^java.io.Writer w]
  (print-method (seq v) w))

(defmacro execute-query
  [structure-type
   iterator-expr
   query-map]
  (let [query (normalize-query (bt/key-structure structure-type) query-map)
        iterator-binding (gensym "iterator")
        key-binding (gensym "key-binding")
        submit-key! (gensym "submit-key!")
        toplevel-runner
        (reduce
          (fn [next-cont [keysegment-key mapping]]
            (query-code
              {:keysegment-key keysegment-key
               :key-binding key-binding
               :match-type (first (keys mapping))
               :match-value (first (vals mapping))
               :structure-type structure-type
               :iterator-binding iterator-binding}
              next-cont))
          (fn [prefix-bindings]
            `(when-let [key# (current-key ~iterator-binding)]
               (~submit-key! (bt/parse-key ~structure-type key#))))
          (reverse query))]
    `(QueryIterator.
       ~iterator-expr
       (fn [iterator# rf# init#]
         (let [current-acc# (volatile! init#)
               ~iterator-binding iterator#
               ~submit-key! (fn [key#]
                              (vreset! current-acc# (rf# @current-acc# key#))
                              (when (reduced? @current-acc#)
                                (throw (ex-info "exit early" {::exit true}))))]
           (try
             ~(toplevel-runner {})
             @current-acc#
             (catch Exception e#
               (if (::exit (ex-info e#))
                 @@current-acc#
                 (throw e#)))))))))
