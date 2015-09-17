(defproject clj-mesos "0.22.2"
  :description "A fully-featured Mesos binding for Clojure"
  :url "http://github.com/dgrnbrg/clj-mesos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.google.protobuf/protobuf-java "2.5.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.apache.mesos/mesos "0.22.1"]]
  :global-vars {*warn-on-reflection* true}
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow" "-Xcheck:jni"]
  :deploy-repositories  [["releases" :clojars]]
  :repositories {"apache-releases" "http://repository.apache.org/content/repositories/releases/"})
