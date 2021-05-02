;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   MIT License (https://opensource.org/licenses/MIT) which can also be
;;   found in the LICENSE file at the root of this distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns servo.subs
  (:require [servo.connection :as db]
            [via.endpoint :as via]
            [signum.signal :as s]
            [signum.subs :refer [reg-sub]]
            [utilis.map :refer [map-vals]]
            [integrant.core :as ig]
            [via.endpoint :as via]))

(defmethod ig/init-key :servo/subs [_ {:keys [db-connection]}]
  (let [subscriptions (atom {})]
    (reg-sub
     :servo/subscribe
     (fn [[_ expr]]
       (let [value-ref (db/subscribe db-connection expr)]
         (swap! subscriptions assoc expr value-ref)
         {:value-ref value-ref}))
     (fn [{:keys [value-ref]} expr]
       (swap! subscriptions dissoc expr)
       (db/dispose db-connection value-ref))
     (fn [{:keys [value-ref]} expr]
       @value-ref))
    {:subscriptions subscriptions}))

(defmethod ig/halt-key! :servo/subs [_ {:keys [db-connection subscriptions]}]
  (doseq [[_ subscription] @subscriptions]
    (db/dispose db-connection subscription)))
