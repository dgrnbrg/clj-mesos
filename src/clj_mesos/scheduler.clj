(ns clj-mesos.scheduler
  (:use clj-mesos.marshalling))

(defdriver org.apache.mesos.SchedulerDriver
  launchTasks org.apache.mesos.Protos$TaskInfo
  requestResources org.apache.mesos.Protos$Request)

(defmacro scheduler
  [& fns]
  (make-proxy-body 'org.apache.mesos.Scheduler fns))

(defn driver
  [scheduler framework-info master]
  (org.apache.mesos.MesosSchedulerDriver.
    scheduler
    (map->proto org.apache.mesos.Protos$FrameworkInfo framework-info)
    master))

(comment
  (def myscheduler (scheduler (registered [driver fid mi]
                                          (println "registered" fid mi)) 
                              (resourceOffers [driver offers] (clojure.pprint/pprint offers))))
  (def overdriver
    (driver
      myscheduler {:user "" :name "testframework"} "localhost:5050"))
  (start overdriver)
  (stop overdriver))
