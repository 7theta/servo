;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   MIT License (https://opensource.org/licenses/MIT) which can also be
;;   found in the LICENSE file at the root of this distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application."
  (:require [servo.connection :as servo]
            [signum.signal :refer [signal alter!]]
            [signum.subs :refer [subscribe]]

            [integrant.core :as ig]
            [integrant.repl :as igr]
            [integrant.repl.state :refer [system]]

            [clojure.tools.namespace.repl :refer [refresh refresh-all]]
            [clojure.repl :refer [apropos dir doc find-doc pst source]]
            [clojure.test :refer [run-tests run-all-tests]]

            [clojure.pprint :refer [pprint]]
            [clojure.reflect :refer [reflect]]))

(def config
  {:servo/connection
   {:db-server {:host "localhost" :port 28015}
    :db-name :test
    :trace true}

   :servo/subs
   {:db-connection (ig/ref :servo/connection)}})

(ig/load-namespaces config)
(igr/set-prep! (constantly config))

(defonce dbc (atom nil))

(defn go
  []
  (igr/go)
  (reset! dbc (get system :servo/connection))
  :ok)

(defn halt
  []
  (igr/halt)
  (reset! dbc nil))

(defn reset
  []
  (halt)
  (igr/reset)
  (go))
