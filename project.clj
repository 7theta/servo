;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(defproject com.7theta/servo "1.2.3"
  :description "A rehinkdb client library designed to integrate with signum"
  :url "https://github.com/7theta/servo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.7theta/via "3.2.3"]

                 [com.rethinkdb/rethinkdb-driver "2.3.3"]
                 [inflections "0.13.2"]

                 [com.7theta/utilis "1.5.1"]
                 [integrant "0.7.0"]]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[org.clojure/tools.namespace "0.3.0"]
                                  [integrant/repl "0.3.1"]]
                   :source-paths ["dev"]}}
  :clean-targets ^{:protect false} ["out" "target"]
  :prep-tasks ["compile"]
  :scm {:name "git"
        :url "https://github.com/7theta/servo"})
