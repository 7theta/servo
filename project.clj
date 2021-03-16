;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(defproject com.7theta/servo "1.9.0"
  :description "A rehinkdb client library designed to integrate with signum"
  :url "https://github.com/7theta/servo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.7theta/via "6.2.3"]
                 [com.7theta/tempus "0.2.0"]

                 [com.rethinkdb/rethinkdb-driver "2.4.4"]
                 [inflections "0.13.2"]

                 [com.7theta/utilis "1.10.0"]
                 [integrant "0.8.0"]]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[org.clojure/tools.namespace "1.1.0"]
                                  [integrant/repl "0.3.2"]]
                   :source-paths ["dev"]}}
  :clean-targets ^{:protect false} ["out" "target"]
  :prep-tasks ["compile"]
  :scm {:name "git"
        :url "https://github.com/7theta/servo"})
