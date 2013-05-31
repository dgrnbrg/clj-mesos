(ns clj-mesos.core
  (:require [clojure.string :as str])
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

(println (.RANGES org.apache.mesos.Protos$Value$Type))
(map->proto org.apache.mesos.Protos$FrameworkID "hello")
(let [fid org.apache.mesos.Protos$FrameworkID]
 (clojure.pprint/pprint (filter #(:static (:flags %)) (:members (clojure.reflect/reflect ))))
  )
(defn javaify-enum-name
  [n]
  (-> n
      name
      (str/replace "-" "_")
      (str/upper-case)))

(defn map->proto
  "Takes a protocol buffer class and a map, and converts the map into the appropriate type."
  [proto m]
  (cond
    ((supers proto) com.google.protobuf.ProtocolMessageEnum)
    (.. proto (getField (javaify-enum-name m)) (get nil))

    )
  )
