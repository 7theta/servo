;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http"//www.eclipse.org/legal/epl-v10.html")
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns servo.connection
  (:refer-clojure :exclude [compile])
  (:require [utilis.fn :refer [fsafe]]
            [utilis.map :refer [compact]]
            [inflections.core :refer [hyphenate underscore]]
            [integrant.core :as ig])
  (:import [com.rethinkdb RethinkDB]
           [com.rethinkdb.net Connection Cursor]
           [com.rethinkdb.gen.ast ReqlExpr ReqlFunction1]
           [java.util Iterator]))

(defonce ^RethinkDB r (RethinkDB/r))

(declare compile connect disconnect changes ensure-db ->rt-name ->rt rt->)

(defmethod ig/init-key :servo/connection [_ {:keys [db-server db-name] :as opts}]
  (connect opts))

(defmethod ig/halt-key! :servo/connection [_ connection]
  (disconnect connection))

(defn connect
  [{:keys [db-server db-name]}]
  (let [{:keys [host port timeout] :or {host "localhost" port 28015 timeout 5000}} (compact db-server)
        connection (-> r .connection
                       (.hostname host) (.port port) (.timeout timeout)
                       (.db db-name)
                       .connect)
        db-connection {:db-server db-server
                       :db-name db-name
                       :connection connection
                       :subscriptions (atom {})}]
    (ensure-db db-connection db-name)
    db-connection))

(defn disconnect
  [{:keys [^Connection connection subscriptions]}]
  (doall (map (fsafe future-cancel) (vals @subscriptions)))
  (.close connection)
  nil)

(defn ensure-db
  [{:keys [^Connection connection db-name]} db-name]
  (let [db-name (->rt-name db-name)]
    (when-not ((set (-> r .dbList (.run connection))) db-name)
      (-> r (.dbCreate db-name) (.run connection)))))

(defn ensure-table
  [{:keys [^Connection connection db-name]} table-name]
  (let [table-name (->rt-name table-name)]
    (when-not ((set (-> r (.db db-name) .tableList (.run connection))) table-name)
      (-> r (.db db-name) (.tableCreate table-name) (.run connection)))))

(defn delete-table
  [{:keys [^Connection connection db-name]} table-name]
  (let [table-name (->rt-name table-name)]
    (-> r (.db db-name) (.tableDrop table-name) (.run connection))))

(defn ensure-index
  [{:keys [^Connection connection db-name]} table-name field-name & {:keys [multi]}]
  (let [table-name (->rt-name table-name)
        field-name (->rt-name field-name)]
    (when-not ((set (-> r (.db db-name) (.table table-name)
                        .indexList (.run connection))) field-name)
      (-> r (.db db-name) (.table table-name)
          (.indexCreate field-name)
          (#(if multi (.optArg % "multi" true) %))
          (#(.run ^ReqlExpr % connection))))))

(defn delete-index
  [{:keys [^Connection connection db-name]} table-name field-name]
  (let [table-name (->rt-name table-name)
        field-name (->rt-name field-name)]
    (-> r (.db db-name) (.table table-name)
        (.indexDrop field-name)
        (.run connection))))

(defn uuid
  [{:keys [^Connection connection db-name]}]
  (-> r (.uuid) (.run connection)))

(defn pred
  [expr]
  (reify com.rethinkdb.gen.ast.ReqlFunction1
    (apply [this row]
      (compile expr row))))

(defn ^ReqlExpr compile
  ([expr] (compile expr r))
  ([expr r]
   (reduce (fn [^ReqlExpr expr [op & opts]]
             (let [opts (mapv #(cond-> %
                                 (and (vector? %)
                                      (not= :pred op)) compile)
                              opts)]
               (case op
                 :db (.db expr (->rt-name (first opts)))
                 :table (.table expr (->rt-name (first opts)))
                 :changes (.changes expr)
                 :opt-arg (let [[option value] opts]
                            (.optArg expr (->rt-name option) (cond-> value (keyword? value) name)))
                 :get (.get expr (first opts))
                 :get-all (let [[field value] (cond->> opts (= 1 (count opts)) (cons :id))]
                            (-> expr
                                (.getAll (into-array (cond-> value (not (coll? value)) vector)))
                                (.optArg "index" (->rt-name field))))
                 :insert (.insert expr (let [value (first opts)
                                             value (cond-> value (or (map? value) (not (coll? value))) vector)]
                                         (->> value (map ->rt)
                                              (into-array clojure.lang.IPersistentMap))))
                 :update (.update expr (->rt (first opts)))
                 :delete (.delete expr)
                 :filter (cond

                           (map? (first opts))
                           (let [[m {:keys [default] :or {default false}}] opts]
                             (-> expr
                                 (.filter (->rt m))
                                 (.optArg "default" default)))

                           (instance? com.rethinkdb.gen.ast.ReqlFunction1 (first opts))
                           (.filter expr (first opts))

                           :else
                           (let [[field value {:keys [default] :or {default false}}] opts]
                             (-> expr
                                 (.filter (.hashMap r (->rt-name field) value))
                                 (.optArg "default" default))))
                 :between (let [[field lower upper] opts]
                            (-> expr
                                (.between lower upper)
                                (.optArg "index" (->rt-name field))))
                 :order-by (let [[index & [direction]] opts
                                 direction (or direction :asc)]
                             (-> expr
                                 (.orderBy)
                                 (.optArg "index"
                                          (if (= direction :asc)
                                            (.asc r (->rt-name index))
                                            (.desc r (->rt-name index))))))
                 :get-field (.getField expr (->rt-name (first opts)))
                 :contains (.contains expr (first opts))
                 :nth (.nth expr (first opts))
                 :pred (pred (first opts))
                 :count (.count expr)
                 :skip (.skip expr (first opts))
                 :limit (.limit expr (first opts))
                 :slice (let [[start end] opts] (.slice expr start end)))))
           r expr)))

(defn run
  [{:keys [^Connection connection db-name]} expr]
  (let [result (.run (compile expr) connection)]
    (cond
      (or (map? result) (instance? java.util.HashMap result)) (rt-> result)
      (seq? result) (map rt-> result)
      (and (not-any? (partial = :changes) (map first expr))
           (instance? Cursor result)) (map rt-> (.toList ^Cursor result))
      :else result)))

(defn subscribe
  [db-connection expr value-ref]
  (reset! value-ref (let [results (run db-connection expr)]
                      (if (map? results)
                        (rt-> results)
                        (->> results (map rt->)
                             (reduce (fn [value row] (assoc! value (:id row) row))
                                     (transient {}))
                             persistent!))))
  (changes db-connection expr
           (fn [changes]
             (swap! value-ref (fn [m]
                                (reduce (fn [m {:keys [type id value]}]
                                          (if (= :remove type)
                                            (dissoc m id)
                                            (assoc m id value))) m changes))))))

(defn dispose
  [{:keys [subscriptions]} subscription]
  (when-let [sub (clojure.core/get @subscriptions subscription)]
    (future-cancel sub)
    (swap! subscriptions dissoc subscription))
  nil)

;;; Private

(defn- changes
  [db-connection expr callback-fn]
  (let [sub-id (str (java.util.UUID/randomUUID))
        sub (future
              (try
                (let [cursor (run db-connection
                               (concat expr
                                       [[:changes]
                                        #_[:opt-arg :include-initial true]
                                        [:opt-arg :squash true]
                                        [:opt-arg :include-types true]
                                        #_[:opt-arg :include-states true]]))]
                  (loop [changes (.iterator ^Cursor cursor)]
                    (let [raw-change (.next ^Iterator changes)]
                      (when-let [change (not-empty
                                         (-> (rt-> raw-change)
                                             (update :type keyword)))]
                        (cond
                          (get-in change [:new-val :deleted])
                          (callback-fn [{:type :remove
                                         :id (get-in change [:new-val :id])}])

                          (=  (:type change) :change)
                          (callback-fn [{:type :remove
                                         :id (get-in change [:old-val :id])}
                                        {:type :add
                                         :id (get-in change [:new-val :id])
                                         :value (:new-val change)}])

                          (=  (:type change) :add)
                          (callback-fn [{:type :add
                                         :id (get-in change [:new-val :id])
                                         :value (:new-val change)}])

                          :else
                          nil)))
                    (recur changes)))
                (catch Exception e
                  (when-not (= "java.lang.InterruptedException"
                               (-> e Throwable->map :via first :message))
                    (println "Error (" (pr-str expr) "):"
                             (pr-str (Throwable->map e)))
                    (println (.printStackTrace e))
                    (swap! (:subscriptions db-connection) dissoc sub-id)))))]
    (swap! (:subscriptions db-connection) assoc sub-id sub)
    sub-id))

(defn- ->rt-name
  [s]
  (underscore (name s)))

(defn- xform-map
  [m kf vf]
  (into {} (map (fn [[k v]]
                  [(kf k)
                   (cond
                     (or (instance? java.util.HashMap v)
                         (map? v))
                     (xform-map v kf vf)

                     (and (or (instance? java.util.ArrayList v)
                              (coll? v))
                          (not (and (string? (first v))
                                    (re-find #"^servo/.*$" (first v))))
                          (not (keyword? (first v))))
                     (mapv #(if (map? %) (xform-map % kf vf) %) v)

                     :else (vf v))]) m)))

(defn- ->rt
  [m]
  (xform-map m
             (fn [k] (->rt-name  k))
             (fn [v]
               (cond
                 (keyword? v)
                 ["servo/kw" (name v)]

                 :else v))))

(defn- rt->
  [m]
  (xform-map m
             (fn [k] (keyword (hyphenate k)))
             (fn [v]
               (cond
                 (and (or (instance? java.util.ArrayList v)
                          (vector? v)) (= "servo/kw" (first v)))
                 (keyword (second v))

                 :else v))))
