(ns clj-mesos.scheduler
  (:use clj-mesos.marshalling))

(defdriver org.apache.mesos.SchedulerDriver
  launchTasks org.apache.mesos.Protos$TaskInfo
  requestResources org.apache.mesos.Protos$Request)

(defmacro scheduler
  [& fns]
  (let [fns (->> fns
                 (map (fn [[fname & fntail]]
                        [fname fntail]))
                 (into {}))]
    (list* 'reify 'org.apache.mesos.Scheduler (make-reify-body org.apache.mesos.Scheduler fns))))

