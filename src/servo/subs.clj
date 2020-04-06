;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http"//www.eclipse.org/legal/epl-v10.html")
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns servo.subs
  (:require [servo.connection :as db]
            [via.endpoint :as via]
            [signum.atom :as s]
            [signum.subs :refer [reg-sub]]
            [utilis.map :refer [map-vals]]
            [integrant.core :as ig]
            [via.endpoint :as via]))

(defmethod ig/init-key :servo/subs [_ {:keys [db-connection]}]
  (let [subscriptions (atom {})]
    (reg-sub
     :servo/subscribe
     (fn [[_ expr]]
       (let [value-ref (s/atom {})
             db-subscription (db/subscribe db-connection expr value-ref)]
         (swap! subscriptions assoc expr db-subscription)
         {:value-ref value-ref
          :db-subscription db-subscription}))
     (fn [{:keys [db-subscription]} expr]
       (swap! subscriptions dissoc expr)
       (db/dispose db-connection db-subscription))
     (fn [{:keys [value-ref]} expr]
       @value-ref))
    {:db-connection db-connection
     :subscriptions subscriptions}))

(defmethod ig/halt-key! :servo/subs [_ {:keys [db-connection subscriptions]}]
  (doseq [[_ subscription] @subscriptions]
    (db/dispose db-connection subscription)))
