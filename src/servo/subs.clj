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
            [signum.subs :refer [reg-sub track-signal]]
            [utilis.map :refer [map-vals]]
            [integrant.core :as ig]
            [via.endpoint :as via]))

(defmethod ig/init-key :servo/subs [_ {:keys [db-connection]}]
  (let [subscriptions (atom {})]
    (reg-sub
     :servo/subscribe
     (fn [[_ expr]]
       (if-let [sub (get @subscriptions expr)]
         (first sub)
         (let [value-atom (s/atom {})
               db-subscription (db/subscribe db-connection expr value-atom)]
           (swap! subscriptions assoc expr [value-atom db-subscription])
           (track-signal value-atom
                         :on-dispose #(do (swap! subscriptions dissoc expr)
                                          (db/dispose db-connection db-subscription))))))
     (fn [value _]
       @value))
    {:db-connection db-connection
     :subscriptions subscriptions}))

(defmethod ig/halt-key! :servo/subs [_ {:keys [db-connection subscriptions]}]
  (doseq [[_ subscription] (vals @subscriptions)]
    (db/dispose db-connection subscription)))
