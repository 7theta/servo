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
            [signum.subs :refer [reg-sub]]
            [utilis.map :refer [map-vals]]
            [integrant.core :as ig]))

(defmethod ig/init-key :servo/subs [_ {:keys [db-connection]}]
  (reg-sub
   :servo/subscribe
   (fn [[_ query]]
     (let [value-ref (db/subscribe db-connection query)]
       {:value-ref value-ref}))
   (fn [{:keys [value-ref]} query]
     (db/dispose db-connection value-ref))
   (fn [{:keys [value-ref]} query]
     @value-ref)))
