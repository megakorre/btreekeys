(ns btreekeys.core
  ""
  (:import java.nio.ByteBuffer
           java.util.Arrays))

(defmulti key-structure
  "overide to add a new keystructure. "
  identity)

(defmulti keysegment-size
  "returns the byte size of key part type"
  identity)

(defmulti keysegment-write-code
  "returns the code to write the keypart to the bite buffer"
  (fn [keypart-type
      buffer-expr
      value-expr] keypart-type))

(defmulti keysegment-read-code
  "returns the code to reads the keypart from a bite buffer"
  (fn [keypart-type
      buffer-expr] keypart-type))

(defn- assert-structure-variances
  [structure-type]
  (when-let [key (->> (key-structure structure-type)
                      (map first)
                      (frequencies)
                      (filter (fn [[k c]] (> c 1)))
                      (first))]
    (throw (ex-info "key appeared more then once in key-structure"
                    {:structure-type structure-type :key key}))))

(defn keysegment-type
  [structure-type keysegment-key]
  (->> (key-structure structure-type)
       (some (fn [[k t]] (when (= k keysegment-key) t)))))

(defn- assert-key-in-structure
  [structure-type keysegment-key]
  (when-not (keysegment-type structure-type keysegment-key)
    (throw (ex-info "keysegment-key does not appear in key-structure"
                    {:structure-type structure-type
                     :keysegment-key keysegment-key}))))

(defn keysegment-range
  [structure-type keysegment-key]
  (let [structure (key-structure structure-type)
        before-keysegment (take-while
                            (fn [[k t]] (not= k keysegment-key))
                            structure)
        size-before-keysegment
        (reduce + (map (comp keysegment-size second)
                       before-keysegment))]
    [size-before-keysegment
     (dec
       (+ size-before-keysegment
          (keysegment-size
            (keysegment-type structure-type keysegment-key))))]))

(defn key-size
  [structure-type]
  (reduce
    +
    (map (comp keysegment-size second)
         (key-structure structure-type))))

(defn structure-size
  [structure]
  (->> structure
       (map (comp keysegment-size second))
       (reduce +)))

(defn- make-key-code
  [structure prefix-expressions]
  (let [key-size (structure-size structure)
        buffer-sym (gensym "buffer")]
    `(let [~buffer-sym (ByteBuffer/allocate ~key-size)]
       ~@(for [[keysegment-key keysegment-type] structure]
           (keysegment-write-code
             keysegment-type
             buffer-sym
             (get prefix-expressions keysegment-key)))
       (.array ~buffer-sym))))

(defmacro make-key
  [structure-type input-map-expr]
  (assert-structure-variances structure-type)
  (let [input-map-sym (gensym "input-map")
        structure (key-structure structure-type)]
    `(let [~input-map-sym ~input-map-expr]
       ~(make-key-code
          structure
          (into {} (for [[k _] structure]
                     [k `(get ~input-map-sym ~k)]))))))

(defn- assert-is-valid-prefix
  [_ _]
  ;; TODO
 )

(defn prefix-structure
  [structure-type prefix-keys]
  (let [prefix-set (set prefix-keys)]
    (take-while
      #(prefix-set (first %))
      (key-structure structure-type))))

(defmacro make-key-prefix
  [structure-type & {:as prefix-expressions}]
  (assert-structure-variances structure-type)
  (assert-is-valid-prefix structure-type (keys prefix-expressions))
  (make-key-code
    (prefix-structure structure-type (keys prefix-expressions))
    prefix-expressions))

(defn- parse-code
  [structure input-bytes-expr]
  (let [buffer-sym (gensym "buffer")]
    `(let [~buffer-sym (ByteBuffer/wrap ~input-bytes-expr)]
       (into
         {}
         [~@(for [[keysegment-key keysegment-type] structure]
              `[~keysegment-key
                ~(keysegment-read-code
                   keysegment-type
                   buffer-sym)])]))))

(defmacro parse-key
  [structure-type input-bytes-expr]
  (assert-structure-variances structure-type)
  (parse-code
    (key-structure structure-type)
    input-bytes-expr))

(defmacro parse-key-prefix
  [structure-type prefix-keys input-bytes-expr]
  (assert-structure-variances structure-type)
  (assert-is-valid-prefix structure-type prefix-keys)
  (parse-code
    (prefix-structure structure-type prefix-keys)
    input-bytes-expr))

(defn increment-byte-range!
  [^bytes input-array ^long range-start ^long range-end]
  (loop [idx range-end]
    (when (>= idx range-start)
      (let [val (aget input-array idx)]
        (if (= 127 val)
          (recur (dec idx))
          (do (aset-byte input-array idx (+ val 1))
              true))))))

(defmacro increment-key-segment!
  [structure-type keysegment-key bytes-expr]
  (assert-structure-variances structure-type)
  (assert-key-in-structure structure-type keysegment-key)
  (let [[start end] (keysegment-range structure-type keysegment-key)]
    `(increment-byte-range! ~bytes-expr ~start ~end)))

(defmacro extract-keysegment
  [structure-type keysegment-key bytes-expr]
  (assert-structure-variances structure-type)
  (assert-key-in-structure structure-type keysegment-key)
  (let [[segment-start _segment-end]
        (keysegment-range structure-type keysegment-key)
        keysegment-type (keysegment-type structure-type keysegment-key)
        buffer-binding (gensym "buffer")]
    `(let [~buffer-binding
           (ByteBuffer/wrap
             ~bytes-expr ~segment-start ~(keysegment-size keysegment-type))]
       ~(keysegment-read-code keysegment-type buffer-binding))))

;; =============================================================================
;; built in keysegment types

(defmethod keysegment-size :long
  [_] Long/BYTES)

(defmethod keysegment-write-code :long
  [_ buffer-expr long-expr]
  `(.putLong ^ByteBuffer ~buffer-expr ~long-expr))

(defmethod keysegment-read-code :long
  [_ buffer-expr]
  `(.getLong ^ByteBuffer ~buffer-expr))
