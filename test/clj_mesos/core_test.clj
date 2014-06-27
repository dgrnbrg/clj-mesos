(ns clj-mesos.core-test
  (:require [clojure.test :refer :all]
            [clj-mesos.marshalling :refer :all]))

(deftest clojure-case
  (is (= "hello-world" (clojurify-name "helloWorld")))
  (is (= "hello-world" (clojurify-name "HELLO_WORLD"))))

(deftest pb-conversions
  (let [offer-id (.. (org.apache.mesos.Protos$OfferID/newBuilder)
                     (setValue "hello")
                     build)
        driver-abort org.apache.mesos.Protos$Status/DRIVER_ABORTED
        framework-id (.. (org.apache.mesos.Protos$FrameworkID/newBuilder)
                         (setValue "goodbye")
                         build)
        slave-id (.. (org.apache.mesos.Protos$SlaveID/newBuilder)
                     (setValue "foo")
                     build)
        offer
        (.. (org.apache.mesos.Protos$Offer/newBuilder)
            (setId offer-id)
            (setFrameworkId framework-id)
            (setSlaveId slave-id)
            (setHostname "localhost")
            (addResources (.. (org.apache.mesos.Protos$Resource/newBuilder)
                              (setName "cpu")
                              (setType org.apache.mesos.Protos$Value$Type/SCALAR)
                              (setScalar (.. (org.apache.mesos.Protos$Value$Scalar/newBuilder)
                                             (setValue 12.0)
                                             build))
                              build))
            (addResources (.. (org.apache.mesos.Protos$Resource/newBuilder)
                              (setName "mem")
                              (setType org.apache.mesos.Protos$Value$Type/SCALAR)
                              (setScalar (.. (org.apache.mesos.Protos$Value$Scalar/newBuilder)
                                             (setValue 1024.0)
                                             build))
                              build))
            build)]
    (is (= (proto->map offer-id) "hello"))
    (is (= (proto->map driver-abort) :driver-aborted))
    (is  (= (proto->map offer)
            {:id "hello",
             :framework-id "goodbye",
             :slave-id "foo",
             :hostname "localhost"
             :resources {:cpu 12.0
                         :mem 1024.0}}))
    (is (= 22.22 (proto->map (.. (org.apache.mesos.Protos$Value/newBuilder)
                                 (setType org.apache.mesos.Protos$Value$Type/SCALAR)
                                 (setScalar (.. (org.apache.mesos.Protos$Value$Scalar/newBuilder)
                                                (setValue 22.22)
                                                build))
                                 build))))
    (is (= #{"hello" "world"} (proto->map (.. (org.apache.mesos.Protos$Value/newBuilder)
                                              (setType org.apache.mesos.Protos$Value$Type/SET)
                                              (setSet (.. (org.apache.mesos.Protos$Value$Set/newBuilder)
                                                          (addItem "hello")
                                                          (addItem "world")
                                                          build))
                                              build))))
    (is (= "hello world" (proto->map (.. (org.apache.mesos.Protos$Value/newBuilder)
                                         (setType org.apache.mesos.Protos$Value$Type/TEXT)
                                         (setText (.. (org.apache.mesos.Protos$Value$Text/newBuilder)
                                                      (setValue "hello world")
                                                      build))
                                         build))))
    (is (= [{:begin 0 :end 5} {:begin 8 :end 10}]
           (proto->map (.. (org.apache.mesos.Protos$Value/newBuilder)
                           (setType org.apache.mesos.Protos$Value$Type/RANGES)
                           (setRanges (.. (org.apache.mesos.Protos$Value$Ranges/newBuilder)
                                          (addRange (.. (org.apache.mesos.Protos$Value$Range/newBuilder)
                                                        (setBegin 0)
                                                        (setEnd 5)
                                                        build))
                                          (addRange (.. (org.apache.mesos.Protos$Value$Range/newBuilder)
                                                        (setBegin 8)
                                                        (setEnd 10)
                                                        build))
                                          build))
                           build))))
    (is (= {:name "cpu" :value 12.0} (proto->map (.. (org.apache.mesos.Protos$Resource/newBuilder)
                                                     (setName "cpu")
                                                     (setType org.apache.mesos.Protos$Value$Type/SCALAR)
                                                     (setScalar (.. (org.apache.mesos.Protos$Value$Scalar/newBuilder)
                                                                    (setValue 12.0)
                                                                    build))
                                                     build))))))

(defn roundtrip-via
  [proto data]
  (is (->> data
           (map->proto proto)
           (proto->map)
           (= data))))

(deftest pb-conversions-back
  (is (= (map->proto org.apache.mesos.Protos$Value$Type :ranges) org.apache.mesos.Protos$Value$Type/RANGES))
  (roundtrip-via org.apache.mesos.Protos$Offer {:id "hello",
                                                :framework-id "goodbye",
                                                :slave-id "foo",
                                                :hostname "localhost"})
  (roundtrip-via org.apache.mesos.Protos$Offer {:id "hello",
                                                :framework-id "goodbye",
                                                :slave-id "foo",
                                                :hostname "localhost"
                                                :resources {:cpu 12.0
                                                            :mem 1024.0}
                                                :executor-ids ["one" "two" "three"]})
  (roundtrip-via org.apache.mesos.Protos$FrameworkID "hello"))
