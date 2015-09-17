# clj-mesos

A Clojure library that provides integration with Mesos via the Java API.

[![Build Status](https://travis-ci.org/dgrnbrg/clj-mesos.svg)](https://travis-ci.org/dgrnbrg/clj-mesos)

## Usage

This library has the exact same functions and callbacks as the [Java Mesos API](http://mesos.apache.org/api/latest/java/),
except for the Log API. You can declare an instance of a Scheduler or an Executor with a proxy-like interface:

```clojure
(def myscheduler
  (clj-mesos.scheduler/scheduler (registered [driver fid mi]
                                   (println "registered" fid mi))
                                 (resourceOffers [driver offers]
                                   (clojure.pprint/pprint offers))))
```

Any unimplemented callbacks will just be noops.

To use the Scheduler or Executor, you can create a driver and use the functions to activate it:

```clojure
;; Create a driver
(def overdriver
  (clj-mesos.scheduler/driver
    myscheduler {:user "" :name "testframework"} "localhost:5050"))

;; Call a function on the driver
(clj-mesos.scheduler/start overdriver)
(clj-mesos.scheduler/stop overdriver))
(clj-mesos.scheduler/revive-offers)
```

Note that the functions you call on the driver are using normal Clojure case rules: all lowercase,
with `-`s separating the words. On the other hand, the callbacks use the Java camelCase standard.

## Stability

clj-mesos has been used to support a major production workload since June 2014.

## License

Copyright © 2013 David Greenberg

Distributed under the Eclipse Public License, the same as Clojure.
