(ns clj-mesos.core
  (:require [clojure.string :as str]
            [clojure.reflect :as reflect])
  (:use [clojure.pprint :only [pprint]])
  (:import [org.apache.mesos
            Scheduler
            ]))

(defn scheduler
  "Returns a mesos scheduler object."
  [& {:keys [registered
             reregistered
             resource-offers
             offer-rescinded
             status-update
             framework-message
             disconnected
             slave-lost
             executor-lost
             error]}]
  
  )

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
                                           :type type #_(case type
                                                   :set org.apache.mesos.Protos$Value$Type/SET
                                                   :scalar org.apache.mesos.Protos$Value$Type/SCALAR
                                                   :ranges org.apache.mesos.Protos$Value$Type/RANGES)}
                                          type
                                          v)))
                                    (if (= value ::missing) [] value))
                              enum?
                              (.getValueDescriptor
                                (java.lang.Enum/valueOf
                                  (case (.. field getEnumType getFullName)
                                    "mesos.Value.Type" org.apache.mesos.Protos$Value$Type
                                    "mesos.TaskState" org.apache.mesos.Protos$TaskState
                                    "mesos.Status" org.apache.mesos.Protos$Status)
                                  (javaify-enum-name value)))
                              :else
                              value)
                      include
                      (cond
                        (.isRepeated field)
                        (fn [value-processor]
                          (doseq [v value] (.addRepeatedField builder field (value-processor v)))
                          builder)
                        :else
                        (fn [value-processor]
                          (.setField builder field (value-processor value))))]
                  (when (= value ::missing)
                    (assert (not (.isRequired field)) "Missing required field"))
                  (cond
                    (= value ::missing)
                    builder
                    message?
                    (include
                      #(.build (recursive-build (.newBuilderForField builder field) %)))
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
                    "byte<>" (Class/forName "[B")} name)
                  (Class/forName name))]
    class))

(supers (class-to-type 'org.apache.mesos.Scheduler))
(defn make-reify-body
  [class impls]
  (map (fn [{:keys [name parameter-types] :as signature}]
         (let [params parameter-types
               marshalling-fns (map (fn [param]
                                      (let [supers (supers (class-to-type param))]
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
               args (or (first (get impls name)) (repeat (count marshalling-fns) '_))
               marshalled-let `(let [~@(mapcat (fn [sym f]
                                                 (if (= f ::skip)
                                                   nil
                                                   [sym (list f sym)]))
                                               args marshalling-fns)]
                                 ~@(rest (get impls name)))]
           `(~name [~'_ ~@args]
                   ~(if (contains? impls name)
                      marshalled-let
                      nil)))
         )
       (:members (reflect/reflect class))))
 
(defmacro scheduler
  [& fns]
  (let [fns (->> fns
                 (map (fn [[fname & fntail]]
                        [fname fntail]))
                 (into {}))]
    (list* 'reify 'org.apache.mesos.Scheduler (make-reify-body org.apache.mesos.Scheduler fns))))

(pprint(make-reify-body org.apache.mesos.Scheduler {'registered '[[driver fid masterinfo] (println hi)]}))
(pprint (macroexpand-1 '(scheduler
  (registered [driver fid master-info]
              (println "registered" fid)))))


(scheduler
  (registered [driver fid master-info]
              (println "registered" fid)))
