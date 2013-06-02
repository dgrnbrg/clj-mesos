(ns clj-mesos.executor
  (:use clj-mesos.marshalling))

(defdriver org.apache.mesos.ExecutorDriver)

(defmacro executor
  [& fns]
  (let [fns (->> fns
                 (map (fn [[fname & fntail]]
                        [fname fntail]))
                 (into {}))]
    (list* 'reify 'org.apache.mesos.Executor (make-reify-body org.apache.mesos.Executor fns))))

