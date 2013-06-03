(ns clj-mesos.scheduler
  (:use clj-mesos.marshalling))

(defdriver org.apache.mesos.SchedulerDriver
  launchTasks org.apache.mesos.Protos$TaskInfo
  requestResources org.apache.mesos.Protos$Request)

(defmacro scheduler
  [& fns]
  (make-proxy-body 'org.apache.mesos.Scheduler fns))

(comment
  (def myscheduler (scheduler (registered [driver fid mi]
                                          (println "registered" fid mi)) 
                              (resourceOffers [driver offers] (clojure.pprint/pprint offers))))
  (def fi (map->proto org.apache.mesos.Protos$FrameworkInfo {:user "" :name "testframework"}))
  (def overdriver
    (org.apache.mesos.MesosSchedulerDriver.
      myscheduler fi "localhost:5050"))
  (start overdriver)
  (stop overdriver))
