(ns clj-mesos.executor
  (:require [clojure.reflect :as reflect]
            [clojure.string :as str])
  (:use clj-mesos.marshalling))

(defdriver org.apache.mesos.ExecutorDriver)

(defmacro executor
  [& fns]
  (make-proxy-body 'org.apache.mesos.Executor fns))

(alter-meta! #'executor assoc :doc (str "methods: " (str/join " " (map :name (:members (reflect/reflect org.apache.mesos.Executor))))))

(defn driver
  [executor]
  (org.apache.mesos.MesosExecutorDriver. executor))
