(ns btreekeys.query
  (:require
   [btreekeys.core :as bt]
   [clojure.pprint :refer [pprint]])
  (:import java.util.Arrays
           java.util.TreeSet
           java.util.Comparator
           com.google.common.primitives.UnsignedBytes
           java.nio.ByteBuffer))

(defprotocol Iterator
  (next-item [this])
  (seek [this key-prefix])
  (current-key [this]))

(defn >byte?
  [^bytes byte-a ^bytes byte-b]
  (let [comparator (UnsignedBytes/lexicographicalComparator)]
    (> 0 (.compare comparator byte-a byte-b))))

(defn seek-in-prefix-code
  "seeks but returns nil if the seek left the bounds
   of the prefix"
  [{:keys [structure-type iterator-binding key-binding start-bind after-bind]}
   prefix-keys]
  (let [prefix-structure (bt/prefix-structure
                           structure-type prefix-keys)
        prefix-size (bt/structure-size prefix-structure)
        seek-code (cond
                    after-bind `(if (>byte? ~key-binding ~after-bind)
                                  (do (seek ~iterator-binding ~after-bind)
                                      (next-item ~iterator-binding))
                                  (seek ~iterator-binding ~key-binding))
                    start-bind `(if (>byte? ~key-binding ~start-bind)
                                  (seek ~iterator-binding ~start-bind)
                                  (seek ~iterator-binding ~key-binding))
                    :else `(seek ~iterator-binding ~key-binding))]
    (if (seq prefix-keys)
      `(let [prefix-val-before# (Arrays/copyOfRange
                                  (bytes ~key-binding) 0 ~prefix-size)]
         (when-let [~key-binding ~seek-code]
           (when (Arrays/equals
                   prefix-val-before#
                   (Arrays/copyOfRange (bytes ~key-binding) 0 ~prefix-size))
             ~key-binding)))
      `(when-let [~key-binding ~seek-code] ~key-binding))))

(defmacro next-while-in-prefix
  [structure-type prefix-keys
   iterator-binding key-binding & body]
  (let [prefix-structure (bt/prefix-structure
                           structure-type prefix-keys)
        prefix-size (bt/structure-size prefix-structure)]
    (if (seq prefix-keys)
      `(let [prefix-val-before# (Arrays/copyOfRange
                                  (bytes ~key-binding) 0 ~prefix-size)]
         (loop []
           (when-let [~key-binding (next-item ~iterator-binding)]
             (when (Arrays/equals
                   prefix-val-before#
                   (Arrays/copyOfRange (bytes ~key-binding) 0 ~prefix-size))
               ~@body
               (recur)))))
      `(loop []
         (when-let [~key-binding (next-item ~iterator-binding)]
           ~@body
           (recur))))))

(defn optimize-query
  "Use next instead of seek when there are no restrictions
   on keysegments below the current iteration point "
  [query]
  (reverse
    (drop-while
      (fn [[k query-item]]
        (= query-item {:q :any :limit Long/MAX_VALUE :start nil :after nil}))
      (reverse query))))

(defn normalize-query
  [structure query-map]
  (for [[keysegment-key keysegment-type] structure
        :let [value (query-map keysegment-key)]]
    [keysegment-key
     (merge
       {:limit Long/MAX_VALUE
        :after nil
        :start nil}
       (if (nil? value) {:q :any} value))]))

(defmulti query-code :q)

(defmacro lexicographical-sort
  [structure-type keysegment-key skip-binding items]
  (let [value-binding (gensym (name keysegment-key))]
    `(let [tree-set# (TreeSet. ^Comparator
                           (UnsignedBytes/lexicographicalComparator))]
       (doseq [~value-binding ~items]
         (.add tree-set# (bt/make-keysegment
                           ~structure-type
                           ~keysegment-key
                           ~value-binding)))
       (if ~skip-binding
         (seq (.tailSet tree-set# ~skip-binding))
         (seq tree-set#)))))

(defmethod query-code :in
  [{:keys [values keysegment-key structure-type start after
           limit vals-produced-binding]} cont]
  (fn [prefix-bindings]
    (let [value-binding (gensym (str (name keysegment-key) "-in-binding"))
          start-binding (gensym "start")]
      `(when (> ~limit 0)
         (let [~start-binding (when-let [start# ~(or after start)]
                                (bt/make-keysegment
                                  ~structure-type ~keysegment-key start#))]
           ~@(when after
               [`(bt/increment-byte-range!
                   ~start-binding 0 (dec (alength ~start-binding)))])
           (loop [produced-count# 0
                  val-produced# (volatile! false)
                  [^bytes ~value-binding
                   & rest#] (lexicographical-sort
                              ~structure-type
                              ~keysegment-key
                              ~start-binding
                              ~values)]
             (vswap! ~vals-produced-binding conj val-produced#)
             (when ~value-binding
               ~(cont (assoc prefix-bindings
                             keysegment-key value-binding))
               (let [new-produced-count# (if @val-produced#
                                           (inc produced-count#)
                                           produced-count#)]
                 (when (< new-produced-count# ~limit)
                   (recur
                     new-produced-count#
                     (volatile! false)
                     rest#))))))))))

(defmethod query-code :eq
  [{:keys [value keysegment-key] :as context} cont]
  (query-code (assoc context :q :in :values [value]) cont))

(defmethod query-code :any
  [{:keys [keysegment-key iterator-binding key-binding structure-type
           limit start after vals-produced-binding]
    :as context} cont]
  (fn [prefix-bindings]
    (let [value-binding (gensym (name keysegment-key))
          prefix-keys (keys prefix-bindings)
          prefix-bindings
          (if (or after start)
            (assoc
              prefix-bindings
              keysegment-key
              `(bt/make-keysegment
                 ~structure-type
                 ~keysegment-key
                 ~(or after start)))
            prefix-bindings)]
      `(when (> ~limit 0)
         (let [~key-binding (bt/make-byte-prefix
                              ~structure-type
                              ~@(apply concat prefix-bindings))]
           ~@(when after
               [`(bt/increment-key-segment!
                   ~structure-type ~keysegment-key
                   ~key-binding)])
           (loop [produced-count# 0
                  val-produced# (volatile! false)
                  ~key-binding ~key-binding]
             (vswap! ~vals-produced-binding conj val-produced#)
             (when-let [~key-binding ~(seek-in-prefix-code
                                        context
                                        (into [] prefix-keys))]
               (let [~value-binding (bt/extract-keysegment-byte
                                      ~structure-type
                                      ~keysegment-key
                                      ~key-binding)]
                 ~(cont (assoc prefix-bindings keysegment-key value-binding)))
               (let [new-produced-count# (if @val-produced#
                                           (inc produced-count#)
                                           produced-count#)]
                 (when (and (< new-produced-count# ~limit)
                            (bt/increment-key-segment!
                              ~structure-type ~keysegment-key
                              ~key-binding))
                   (recur
                     new-produced-count#
                     (volatile! false)
                     ~key-binding))))))))))

(deftype QueryIterator [iterator runner]
  clojure.lang.Seqable
  (seq [this] (seq (into [] this)))
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
  (let [query (->> query-map
                   (normalize-query (bt/key-structure structure-type))
                   optimize-query)
        iterator-binding (gensym "iterator")
        key-binding (with-meta (gensym "key-binding") {:tag bytes})
        submit-key! (gensym "submit-key!")
        vals-produced-binding (gensym "vals-produced")
        start-bind (gensym "global-start")
        after-bind (gensym "global-after")
        base-context {:key-binding key-binding
                      :vals-produced-binding vals-produced-binding
                      :structure-type structure-type
                      :start-bind (when (:btreekeys/start query-map) start-bind)
                      :after-bind (when (:btreekeys/after query-map) after-bind)
                      :iterator-binding iterator-binding}
        toplevel-runner
        (reduce
          (fn [next-cont [keysegment-key mapping]]
            (query-code
              (merge
                mapping
                base-context
                {:keysegment-key keysegment-key})
              next-cont))
          (fn [prefix-bindings]
            `(let [~key-binding (bt/make-byte-prefix
                                  ~structure-type
                                  ~@(flatten (seq prefix-bindings)))]
               (when-let [~key-binding ~(seek-in-prefix-code
                                          base-context
                                          (into [] (keys prefix-bindings)))]
                 (~submit-key! (bt/parse-key ~structure-type ~key-binding))
                 (next-while-in-prefix
                   ~structure-type
                   ~(into [] (keys prefix-bindings))
                   ~iterator-binding
                   ~key-binding
                   (~submit-key! (bt/parse-key ~structure-type ~key-binding))))))
          (reverse query))]
    `(QueryIterator.
       ~iterator-expr
       (fn [iterator# rf# init#]
         (let [current-acc# (volatile! init#)
               ~vals-produced-binding (volatile! ())
               ~iterator-binding iterator#
               ~start-bind ~(:btreekeys/start query-map)
               ~after-bind ~(:btreekeys/after query-map)
               ~submit-key! (fn [key#]
                              (doseq [ref# @~vals-produced-binding]
                                (vreset! ref# true))
                              (vreset! ~vals-produced-binding ())
                              (vreset! current-acc# (rf# @current-acc# key#))
                              (when (reduced? @current-acc#)
                                (throw (ex-info "exit early" {::exit true}))))]
           (try
             ~(toplevel-runner {})
             @current-acc#
             (catch Exception e#
               (if (::exit (ex-data e#))
                 @@current-acc#
                 (throw e#)))))))))
