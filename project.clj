(defproject clj-mesos "0.20.2"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.google.protobuf/protobuf-java "2.5.0"]
                 [org.apache.mesos/mesos "0.20.0"]]
  :global-vars {*warn-on-reflection* true}
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow" "-Xcheck:jni"]
  :repositories {"apache-releases" "http://repository.apache.org/content/repositories/releases/"})
