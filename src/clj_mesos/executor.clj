(ns clj-mesos.executor
  (:use clj-mesos.marshalling))

(defdriver org.apache.mesos.ExecutorDriver)

(defmacro executor
  [& fns]
  (make-proxy-body 'org.apache.mesos.Executor fns))

(defn driver
  [executor]
  (org.apache.mesos.MesosExecutorDriver. executor))
