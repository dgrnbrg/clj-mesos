(ns clj-mesos.marshalling
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.reflect :as reflect])
  (:import [org.apache.mesos
            Scheduler]))

(defn clojurify-name
  "Converts from camelCase or TITLE_CASE to clojure-case"
  [name]
  (->
    (if (every? #(Character/isUpperCase %) (filter #(Character/isLetter %) name))
      ;;Handle TITLE_CASE
      (str/lower-case name)
      ;;Handle camelCase
      (loop [[first :as name] name result ""]
        (if (not= name "")
          (recur (.substring name 1)
                 (str result
                      (if (Character/isUpperCase first)
                        (str \- (Character/toLowerCase first))
                        first)))
          result)))
    (str/replace "_" "-")))

(declare proto->map)

(defn- handle-value-type
  "Takes a value-type protocol buffer and extracts the value."
  [proto]
  (condp = (.getType proto)
      org.apache.mesos.Protos$Value$Type/SCALAR
      (.. proto getScalar getValue)
      org.apache.mesos.Protos$Value$Type/SET
      (set (.. proto getSet getItemList))
      org.apache.mesos.Protos$Value$Type/TEXT
      (.. proto getText getValue)
      org.apache.mesos.Protos$Value$Type/RANGES
      (mapv proto->map (.. proto getRanges getRangeList))
      (throw (ex-info "Unknown value" {}))))

(defn proto->map
  "Takes a protocol buffer and converts it to a map."
  [proto]
  (cond
    ;; Handle enums, since they're not composite
    (.. proto getClass isEnum)
    (-> proto .name clojurify-name keyword)

    (instance? com.google.protobuf.Descriptors$EnumValueDescriptor proto)
    (-> proto .getName clojurify-name keyword)

    (.. proto getClass isPrimitive)
    proto

    (#{Boolean Long Short Character Double Float} (.getClass proto))
    proto

    :else
    (let [fields (seq (.getAllFields proto))]
      (cond
        ;; Mesos tagged values are special
        (= "mesos.Value" (.. proto getDescriptorForType getFullName))
        (handle-value-type proto)
        (= "mesos.Attribute" (.. proto getDescriptorForType getFullName))
        {:name (.getName proto) :value (handle-value-type proto)}
        (= "mesos.Resource" (.. proto getDescriptorForType getFullName))
        {:name (.getName proto) :value (handle-value-type proto)}
        ;; Some Mesos values are just a single "value", which we'll treat specially
        (= 1 (count fields))
        (let [[[_ v]] fields] v)
        ;; Everything else is a message, which is just a struct
        :else
        (let [processed (for [[desc v] fields
                              :let [name (.getName desc)
                                    v (cond
                                        (or (string? v) (integer? v) (float? v)) v
                                        (instance? com.google.protobuf.ByteString v) (.toByteArray v)
                                        (and (.isRepeated desc)
                                             (#{"mesos.Resource"
                                                "mesos.Attribute"}
                                               (.. desc getMessageType getFullName)))
                                        (->> v
                                             (map (fn [map-entry]
                                                    (let [{:keys [name value]} (proto->map map-entry)]
                                                      [(keyword (clojurify-name name)) value])))
                                             (into {}))
                                        (.isRepeated desc) (mapv proto->map v)
                                        :else (proto->map v))]]
                          [(keyword (clojurify-name name)) v])]
          (into {} processed))))))

(defn javaify-enum-name
  [n]
  (-> n
      name
      (str/replace "-" "_")
      (str/upper-case)))

(defn get-descriptor
  [proto]
  (clojure.lang.Reflector/invokeStaticMethod proto "getDescriptor" (into-array [])))

(defn new-builder
  [proto]
  (clojure.lang.Reflector/invokeStaticMethod proto "newBuilder" (into-array [])))

(defn fieldName->class
  "Maps a protobuf fieldName to className.

  Example:
  (= (fieldName->className \"mesos.Status\") org.apache.mesos.Proto$Status)"
  [fieldName]
  (let [packageName "org.apache.mesos.Protos"
        classes (-> fieldName
                    (str/split #"\.")
                    (rest))
        className (str/join "$" (conj classes packageName))]
    (Class/forName className)))

(defn recursive-build
  "Takes a protobuf builder and a map, and recursively builds the protobuf."
  [builder m]
  (let [desc (.getDescriptorForType builder)
        fields (.getFields desc)]
    (if (= 1 (count fields))
      (.setField builder (first fields) m)
      (reduce (fn [builder field]
                (let [name (clojurify-name (.getName field))
                      value (get m (keyword name) ::missing)
                      message? (= (.getType field) com.google.protobuf.Descriptors$FieldDescriptor$Type/MESSAGE)
                      enum? (= (.getType field) com.google.protobuf.Descriptors$FieldDescriptor$Type/ENUM)
                      ;; Fix ups for simplicity
                      value (cond
                              ;; Fix up Resources and Attributes
                              (and message?
                                   (#{"mesos.Attribute" "mesos.Resource"} (.. field getMessageType getFullName)))
                              (mapv (fn [[k v]]
                                      (let [type (cond
                                                   (set? v) :set
                                                   (float? v) :scalar
                                                   (every? #(and (contains? % :begin) (contains? % :end)) v) :ranges)]
                                        (assoc
                                          {:name (clojure.core/name k)
                                           :type type}
                                          type
                                          v)))
                                    (if (= value ::missing) [] value))
                              enum?
                              (if (= value ::missing)
                                value
                                (.getValueDescriptor
                                 (java.lang.Enum/valueOf
                                  (fieldName->class (.. field getEnumType getFullName))
                                  (javaify-enum-name value))))

                              :else
                              value)
                      include
                      (cond
                        (.isRepeated field)
                        (fn [value-processor]
                          (doseq [v value]
                            (try
                              (.addRepeatedField builder field (value-processor v))
                              (catch Exception e
                                (throw (ex-info "Could not marshall repeated field" {:field field :value v :desc desc} e)))))
                          builder)

                        :else
                        (fn [value-processor]
                          (try
                            (.setField builder field (value-processor value))
                            (catch Exception e
                              (throw (ex-info "Could not marshall field" {:field field :value value :desc desc} e))))))]
                  (when (= value ::missing)
                    (assert (not (.isRequired field)) (str "Missing required field " (.getName field) " in message " (.getFullName desc))))
                  (cond
                    (= value ::missing)
                    builder

                    message?
                    (include
                      #(.build (recursive-build (.newBuilderForField builder field) %)))

                    (= (.getType field ) com.google.protobuf.Descriptors$FieldDescriptor$Type/BYTES)
                    (include #(com.google.protobuf.ByteString/copyFrom %))

                    :else
                    (include identity))))
              builder
              fields))))

(defn map->proto
  "Takes a protocol buffer class and a map, and converts the map into the appropriate type."
  [proto m]
  (let [desc (get-descriptor proto)]
    (cond
      (instance? com.google.protobuf.Descriptors$EnumDescriptor desc)
      (clojure.lang.Reflector/invokeStaticMethod proto "valueOf" (into-array [(javaify-enum-name m)]))
      (instance? com.google.protobuf.Descriptors$Descriptor desc)
      (.build (recursive-build (new-builder proto) m)))))

(defn class-to-type
  [class-symbol]
  (let [name (name class-symbol)
        class (or ({"int" java.lang.Integer
                    "byte<>" (Class/forName "[B")
                    "boolean" java.lang.Boolean} name)
                  (Class/forName name))]
    class))

(defn make-proxy-body
  [class fns]
  (let [impls (->> fns
                   (map (fn [[fname & fntail]]
                          [fname fntail]))
                   (into {}))
        body (map (fn [{:keys [name parameter-types] :as signature}]
                    (let [params parameter-types
                          marshalling-fns
                          (map (fn [param]
                                 (let [supers (supers (class-to-type param))]
                                   ;; TODO: refactor to use contains? to avoid the nil? special case
                                   (cond
                                     (nil? supers)
                                     ::skip
                                     (supers com.google.protobuf.AbstractMessage)
                                     `proto->map
                                     (supers java.util.Collection)
                                     `(fn [l#] (mapv proto->map l#))
                                     :else
                                     ::skip)))
                               params)
                          args (or (first (get impls name))
                                   (repeat (count marshalling-fns) '_))
                          marshalled-let
                          `(let [~@(mapcat (fn [sym f]
                                             (if (= f ::skip)
                                               nil
                                               [sym (list f sym)]))
                                           args marshalling-fns)]
                             ~@(rest (get impls name)))]
                      `(~name [~@args]
                              (log/debug "In callback: " ~(clojure.core/name name) "with args" ~@args)
                              (~'try
                                ~(if (contains? impls name)
                                   marshalled-let
                                   nil)
                                (~'catch Throwable t#
                                  (log/error t# ~(str "Error in " (clojure.core/name name))))))))
                  (:members (reflect/reflect (class-to-type class))))]
    `(proxy [~class] []
       ~@body)))

(defn distinct-by
  [f coll]
  (loop [result []
         seen #{}
         [h & t] coll]
    (if h
      (if (contains? seen (f h))
        (recur result seen t)
        (recur (conj result h) (conj seen (f h)) t))
      result)))

(defn marshall-one-param
  "Generate the binding to marshall the given sym to the given param-type"
  [sym param-type erased-type]
  (let [type (class-to-type param-type)
        supers (conj (supers type) type)]
    (cond
      (contains? supers com.google.protobuf.AbstractMessage)
      [sym (list `map->proto param-type sym)]
      (contains? supers java.util.Collection)
      [sym `(map (partial map->proto ~erased-type) ~sym)]
      :else
      [])))

(defn marshall-params
  "Generate a binding that marshalls the given sym to either the scalar
   or collection type depending on whether the symbol is a scalar or collection
   at runtime"
  [sym scalar-type collection-type]
  (let [[_ marshall-scalar] (marshall-one-param
                              sym scalar-type nil)
        [_ marshall-collection] (marshall-one-param
                                  sym 'java.util.Collection collection-type)]
    [sym `(if (or (vector? ~sym)
                  (seq? ~sym)
                  (nil? ~sym))
            ~marshall-collection
            ~marshall-scalar)]))

(defn make-reflective-param-marshalling
  [name driver-type arities erased-types]
  (for [[arity-size arity-types] (group-by count arities)]
    ;; We must check that for each position, there's only one non-collection arity
    (do (assert (->> (apply map vector arity-types)
                     (map (fn [ts] (remove #(= 'java.util.Collection %) ts)))
                     (every? #(<= (count (distinct %)) 1)))
                "Each position must have only one distinct non-collection arity")
        (let [params (repeatedly arity-size gensym)
              driver-sym (with-meta (gensym "driver") {:tag driver-type})
              param-marshalling (mapcat (fn [sym param-types collection-type]
                                          (if (= 1 (count (distinct param-types)))
                                            (marshall-one-param sym (first param-types) collection-type)
                                            ;; TODO: handle multi case
                                            (marshall-params sym (some #(and (not= 'java.util.Collection %) %) param-types) collection-type)))
                                        params (apply map vector arity-types) (concat erased-types (repeat nil)))
              invocation (list* `. driver-sym name params)]
          `([~driver-sym ~@params]
            (log/debug "In driver fn" ~(clojure.core/name name))
            (let [~@param-marshalling]
              (proto->map ~invocation)))))))

(defn make-reflective-fn
  "Takes a data structure from clojure.reflect/reflect's members and returns
   the syntax for a fn that invokes the function, marshalling protobufs automatically.

   erased-types contains the types to use for collection arguments in that
   argument position. At runtime, we'll check to see if the argument is a collection
   or not (where collection means seq, vector, or nil). If it's a collection, it'll
   marshall it according to the specified type. Otherwise, it'll assume that there's
   a single arity that we can use to marshall to.
   "
  [name driver-type arities return-type erased-types]
  (when (= (count (distinct (map count arities))) (count arities)) (str name " has difficult arities: " (pr-str arities)))
  `(defn ~(symbol (clojurify-name (clojure.core/name name)))
     ~(str/join "\n  " (map #(str "type signature: "(str/join " " %)) arities))
     ~@(make-reflective-param-marshalling name driver-type arities erased-types)))

(defmacro defdriver
  [driver & handlers]
  (let [methods (->> (reflect/reflect (class-to-type driver)) :members seq (group-by :name))
        handlers (apply hash-map handlers)]
    (cons `do
          (mapv (fn [[method-name methods]]
                  (make-reflective-fn
                    method-name
                    driver
                    (map :parameter-types methods)
                    (-> methods first :return-type)
                    (handlers method-name)
                    #_(fn [sym]
                      `(mapv (partial map->proto ~(handlers method-name)) ~sym))))
                methods))))
