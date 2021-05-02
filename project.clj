;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   MIT License (https://opensource.org/licenses/MIT) which can also be
;;   found in the LICENSE file at the root of this distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(defproject com.7theta/servo "2.0.0"
  :description "A rehinkdb client library designed to integrate with signum"
  :url "https://github.com/7theta/servo"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.7theta/via "7.0.0"]
                 [com.7theta/tempus "0.3.1"]

                 [aleph "0.4.7-alpha7"]
                 [gloss "0.2.6"]
                 [metosin/jsonista "0.3.1"]
                 [inflections "0.13.2"]

                 [com.7theta/utilis "1.12.2"]
                 [integrant "0.8.0"]]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[org.clojure/tools.namespace "1.1.0"]
                                  [integrant/repl "0.3.2"]]
                   :source-paths ["dev"]}}
  :clean-targets ^{:protect false} ["out" "target"]
  :prep-tasks ["compile"]
  :scm {:name "git"
        :url "https://github.com/7theta/servo"})
