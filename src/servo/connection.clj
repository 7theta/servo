;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   MIT License (https://opensource.org/licenses/MIT) which can also be
;;   found in the LICENSE file at the root of this distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns servo.connection
  (:refer-clojure :exclude [send])
  (:require [servo.util.scram :as scram]
            [signum.signal :refer [signal alter!]]
            [tempus.core :as t]
            [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [gloss.core :as gloss]
            [gloss.io :as gloss-io]
            [byte-streams :as bs]
            [jsonista.core :as json]
            [inflections.core :refer [hyphenate underscore]]
            [utilis.map :refer [compact map-keys]]
            [utilis.types.number :refer [string->long string->double]]
            [clojure.string :as st]
            [integrant.core :as ig])
  (:import [tempus.core DateTime]
           [java.nio ByteBuffer ByteOrder]
           [java.io ByteArrayOutputStream]))

(declare connect disconnect ->rql-connection ensure-db await-tables-ready
         json-mapper ->rt-name ->rt-query ->rt-query-term
         send handle-message token request-types)

(defmethod ig/init-key :servo/connection [_ {:keys [db-server db-name] :as options}]
  (connect options))

(defmethod ig/halt-key! :servo/connection [_ connection]
  (try (disconnect connection) (catch Exception _ nil)))

(defn connect
  [{:keys [db-server db-name await-ready trace]
    :or {await-ready true}}]
  (let [{:keys [host port username password]
         :or {host "localhost"
              port 28015
              username "admin"
              password ""}} (compact db-server)
        tcp-connection (tcp/client {:host host :port port})]
    @(s/put! @tcp-connection (byte-array [0xc3 0xbd 0xc2 0x34]))
    (let [version (try (:server_version (-> @(s/take! @tcp-connection)
                                            bs/to-string
                                            (json/read-value json/keyword-keys-object-mapper)))
                       (catch Exception _
                         (throw (ex-info ":servo/connection initial handshake failed"))))
          send (fn [message]
                 (s/put! @tcp-connection
                         (let [json-str (json/write-value-as-string message json-mapper)]
                           ;; Rethink requires JSON strings to be "null terminated"
                           (byte-array (inc (count json-str)) (map int json-str)))))
          recv #(json/read-value (bs/to-string @(s/take! @tcp-connection)) json-mapper)
          {:keys [message client-initial-scram]} (scram/initial-proposal {:username "admin"})
          _ (send message)
          {:keys [server-scram server-authentication]} (scram/server-challenge {:client-initial-scram client-initial-scram}
                                                                               (recv))
          {:keys [message server-signature]} (scram/authentication-request {:client-initial-scram client-initial-scram
                                                                            :server-authentication server-authentication
                                                                            :server-scram server-scram})
          _ (send message)
          {:keys [authentication success]} (recv)]
      (when-not (and success (scram/validate-server {:server-signature server-signature
                                                     :server-authentication authentication}))
        (throw (ex-info ":servo/connection authentication failed"
                        {:client-initial-scram client-initial-scram
                         :server-scram server-scram})))
      (let [connection {:server-version version
                        :db-name db-name
                        :db-term {:db (:query (->rt-query [[:db db-name]]))}
                        :query-counter (atom (long 0))
                        :queries (atom {})
                        :feeds (atom {})
                        :subscriptions (atom {})
                        :tcp-connection @tcp-connection
                        :rql-connection (->rql-connection @tcp-connection)
                        :response-buffer 100
                        :trace trace}]
        (s/consume (partial handle-message connection) (:rql-connection connection))
        (ensure-db connection db-name)
        (when await-ready (await-tables-ready connection))
        connection))))

(defn disconnect
  [{:keys [queries feeds subscriptions rql-connection tcp-connection]}]
  (when rql-connection (s/close! rql-connection))
  (when tcp-connection (s/close! tcp-connection))
  (reset! queries nil)
  (reset! feeds nil)
  (reset! subscriptions nil))

(defonce ^:private var-counter (atom 0))

(defmacro func
  [params body]
  (let [vars (reduce (fn [params p] (assoc params p (swap! var-counter inc))) {} params)]
    `[:func
      [(into [:make-array] ~(mapv vars params))
       (into [~(first body)] ~(mapv (fn [v]
                                      (if-let [vi (get vars v)]
                                        [10 [vi]]
                                        v)) (rest body)))]]))

(defn run
  [{:keys [db-term queries] :as connection} query & {:keys [query-type] :or {query-type :start}}]
  (let [token (token connection)
        response-d (d/deferred)
        {:keys [response-fn query]} (->rt-query query)]
    (swap! queries assoc token {:query query
                                :response-d response-d
                                :response-fn response-fn})
    (when-not @(send connection token [(get request-types query-type) query db-term])
      (swap! queries dissoc token)
      (throw (ex-info ":servo/connection send failed" {:connection connection :query query})))
    response-d))

(defn subscribe
  [{:keys [feeds subscriptions] :as connection} query]
  (when (= :changes (first (last query)))
    (throw (ex-info ":servo/connection subscribe query should not include :changes")))
  (let [feed @(run connection (concat query [[:changes {:squash true
                                                        :include-initial true
                                                        :include-types true}]]))
        token (get-in @feeds [feed :token])
        single (= :atom-feed (get-in @feeds [feed :type]))
        value-ref (signal (if single nil []))]
    (swap! subscriptions assoc value-ref feed)
    (s/consume (fn [change]
                 (cond
                   (#{"remove" "uninitial"} (:type change))
                   (alter! value-ref
                           (if single
                             (constantly nil)
                             (comp vec (partial remove #(= (:id %) (get-in change [:old-val :id]))))))

                   (= "change" (:type change))
                   (alter! value-ref
                           (if single
                             (constantly (:new-val change))
                             (partial mapv #(if (= (get-in change [:new-val :id]) (:id %)) (:new-val change) %))))

                   (#{"add" "initial"} (:type change))
                   (alter! value-ref
                           (if single
                             (constantly (:new-val change))
                             #(conj % (:new-val change))))

                   :else (println ":servo/connection unknown change type" (pr-str query) (pr-str change))))
               feed)
    value-ref))

(def ->seq s/stream->seq)

(defn dispose
  [{:keys [feeds subscriptions] :as connection} value-ref]
  (let [feed (get @subscriptions value-ref)]
    (send connection (get-in @feeds [feed :token]) [(get request-types :stop)])
    (s/close! feed)
    (swap! feeds dissoc feed)
    (swap! subscriptions dissoc value-ref))
  nil)

(defn noreply-wait
  [connection]
  (run connection nil :query-type :noreply-wait))

(defn server-info
  [connection]
  (run connection nil :query-type :server-info))

(defn ensure-table
  [connection table-name]
  (let [table-list (set @(run connection [[:table-list]]))]
    (when-not (get table-list table-name)
      @(run connection [[:table-create table-name]]))))

(defn ensure-index
  [connection table-name index-name & options]
  (let [index-list (set @(run connection [[:table table-name] [:index-list]]))]
    (when-not (get index-list index-name)
      @(run connection [[:table table-name] (cond-> [:index-create index-name]
                                              (not-empty options) (conj options))]))))


;;; Private

(def ^:private json-mapper (json/object-mapper
                            {:encode-key-fn (comp underscore name)
                             :decode-key-fn (comp keyword hyphenate)}))

(defn- token
  [{:keys [query-counter]}]
  (swap! query-counter inc))

(def ^:private rql-protocol
  (gloss/compile-frame
   [:int64-be (gloss/finite-frame :int32-le (gloss/string :utf-8))]
   (fn [[token query]]
     [token (json/write-value-as-string query json-mapper)])
   (fn [[token query-str]]
     [token (json/read-value query-str json-mapper)])))

(defn- ->rql-connection
  [tcp-connection]
  (let [out (s/stream)]
    (s/connect (s/map (partial gloss-io/encode rql-protocol) out) tcp-connection)
    (s/splice out (gloss-io/decode-stream tcp-connection rql-protocol))))

(defn- send
  [{:keys [rql-connection trace]} token query]
  (when trace (println (format ">> 0x%04x" token) (pr-str query)))
  (s/put! rql-connection [token query]))

(declare response-types response-note-types response-error-types)

(defn- handle-message
  [{:keys [queries feeds response-buffer trace] :as connection} [token response]]
  (let [{:keys [r t e n]} response
        {:keys [query response-d response-fn]} (get @queries token)
        success #(when response-d (d/success! response-d %))
        error #(when response-d (d/error! response-d %))
        handle-seq (fn [& {:keys [close] :or {close false}}]
                     (when-not (and (d/realized? response-d) (s/stream? @response-d))
                       (let [feed (s/stream response-buffer (map response-fn))]
                         (swap! feeds assoc feed {:type (get response-note-types (first n))
                                                  :token token})
                         (success feed)))
                     (s/put-all! @response-d r)
                     (when (and response-d close)
                       (s/close! @response-d)))]
    (when trace (println (format "<< 0x%04x" token) (pr-str response)))
    (try
      (case (get response-types t)
        :success-atom
        (do (success (response-fn (first r)))
            (swap! queries dissoc token))

        :success-sequence
        (do (handle-seq :close true)
            (swap! queries dissoc token))

        :success-partial
        (do (handle-seq :close false)
            (send connection token [(get request-types :continue)]))

        :wait-complete
        (do (success (response-fn nil))
            (swap! queries dissoc token))

        :server-info
        (do (success (response-fn (first r)))
            (swap! queries dissoc token))

        :client-error
        (do (error (ex-info ":servo/connection client-error"
                            {:query query :error (get response-error-types e) :error-text (first r)}))
            (swap! queries dissoc token))

        :compile-error
        (do (error (ex-info ":servo/connection compile-error"
                            {:query query :error (get response-error-types e) :error-text (first r)}))
            (swap! queries dissoc token))

        :runtime-error
        (do (error (ex-info ":servo/connection runtime-error"
                            {:query query :error (get response-error-types e) :error-text (first r)}))
            (swap! queries dissoc token)))
      (catch Exception e
        (error (ex-info ":servo/connection response error"
                        {:query query :token token :response response :error e}))
        (swap! queries dissoc token)))))

(declare term-types response-types response-error-types
         ->rt ->rt-key ->rt-name ->rt-value
         rt-> rt-string-> rt-key-> rt-name-> rt-value->)

(defn- ->rt-query-term
  [[id & parameters]]
  (compact
   (merge
    {:id (get term-types id)}
    (let [nested-fields #(reduce merge
                                 ((fn xform [params]
                                    (map (fn [v]
                                           (cond
                                             (or (keyword? v) (string? v)) {(->rt-key v) true}
                                             (map? v) (->> (map (fn [[k v]]
                                                                  [(->rt-key k) (reduce merge (xform v))]) v)
                                                           (into {}))))
                                         params)) parameters))]
      (case id
        :db-list {:response-fn (partial map rt-name->)}
        :db {:arguments [(->rt-name (first parameters))]}
        :db-create {:arguments [(->rt-name (first parameters))]}
        :table-list {:response-fn (partial map rt-name->)}
        :index-list {:response-fn (partial map rt-name->)}
        :table {:arguments [(->rt-name (first parameters))]
                :response-fn rt->}
        :table-create {:arguments [(->rt-name (first parameters))]}
        :table-drop {:arguments [(->rt-name (first parameters))]}
        :index-create {:arguments [(->rt-name (first parameters))]}
        :index-wait {:arguments [(->rt-name (first parameters))]}
        :index-rename {:arguments [map] ->rt-name parameters}
        :index-drop {:arguments [(->rt-name (first parameters))]}
        :get {:arguments [(->rt-value (first parameters))]
              :response-fn rt->}
        :get-all {:arguments [(first parameters)]
                  :options (when-let [index (second parameters)]
                             {"index" (->rt-name (:index index))})
                  :response-fn rt->}
        :insert (let [[value options] parameters]
                  {:arguments [(if (or (map? value) (not (coll? value)))
                                 (->rt value)
                                 [(get term-types :make-array)
                                  (map ->rt value)])]
                   :options (->rt options)})
        :update (let [[value options] parameters]
                  {:arguments [(->rt value)]
                   :options (->rt options)})
        :filter (let [[value options] parameters]
                  (merge
                   (cond
                     (map? value)
                     {:arguments [(->rt value)]})
                   {:options options
                    :response-fn rt->}))
        :between (let [[lower upper options] parameters]
                   {:arguments (map ->rt-value [lower upper])
                    :options (when options
                               {"index" (->rt-name (:index options))})})
        :order-by (if (map? (first parameters))
                    (let [{:keys [index]} (first parameters)]
                      {:options {"index" (if (and (vector? index)
                                                  (#{:asc :desc} (first index)))
                                           (let [[order index] index]
                                             [(get term-types order) [(->rt-name index)]])
                                           (->rt-name index))}})
                    {:arguments [(->rt-name (first parameters))]})
        :get-field {:arguments [(->rt-name (first parameters))]}
        :pluck {:arguments [(nested-fields)]}
        :with-fields {:arguments [(nested-fields)]}
        :without {:arguments [(nested-fields)]}
        :contains {:arguments [(first (->rt-value (first parameters)))]}
        :nth {:arguments [(first parameters)]}
        ;;:pred (pred (first parameters))
        :distinct (when-some [index (first parameters)]
                    {:arguments [{"index" (->rt-name index)}]})
        ;;:slice (let [[start end] parameters] {:arguments [[start end]]})
        :during (let [[start end] parameters] {:arguments [[(->rt-value start) (->rt-value end)]]})
        ;; :func (let [[params body] (first parameters)]
        ;;         [(->rt-query-term params) (->rt-query-term body)])
        :changes {:options (map-keys (comp underscore name) (first parameters))
                  :response-fn #(-> % (update :new-val rt->) (update :old-val rt->) compact)}
        {:arguments parameters})))))

(defn- ->rt-query
  [query]
  {:query (reduce (fn [query term]
                    (let [{:keys [id arguments options]} (->rt-query-term term)]
                      (remove nil? [id (cond->> arguments query (cons query)
                                                true (remove nil?)) options]))) nil query)
   :response-fn (or (->> (map (comp :response-fn ->rt-query-term) query)
                         (remove nil?)
                         last)
                    rt->)})

(defn- xform-map
  [m kf vf]
  (into {} (map (fn [[k v]]
                  [(kf k)
                   (cond
                     (and (map? v) (= "TIME" (get v :$reql-type$))) (vf v)
                     (instance? DateTime v) (vf v)
                     (map? v) (xform-map v kf vf)
                     :else (vf v))]) m)))

(defn- ->rt-name
  [s]
  (underscore
   (str (when (keyword? s) (when-let [ns (namespace s)] (str ns "/")))
        (name s))))

(defn- rt-name->
  [s]
  (keyword (hyphenate s)))

(defn- ->rt-key
  [k]
  (cond
    (keyword? k) (->rt-name k)
    (string? k) (str "servo/str=" (->rt-name k))
    (double? k) (str "servo/double=" (str k))
    (int? k) (str "servo/long=" (str k))
    :else (throw (ex-info ":servo/connection unsupported type for key" {:key k}))))

(defn- rt-key->
  [k]
  (let [k (cond-> k (keyword? k) name)
        coerced (rt-string-> k)]
    (if (= k coerced)
      (keyword (hyphenate k))
      coerced)))

(defn- ->rt-value
  [v]
  (cond
    (keyword? v) (str "servo/keyword=" (->rt-name v))
    (instance? DateTime v) {"$reql_type$" "TIME"
                            "epoch_time" (t/into :long v)
                            "timezone" "+00:00"}
    (or (vector? v) (seq? v)) [(get term-types :make-array)
                               (map ->rt v)]
    :else v))

(defn- rt-value->
  [v]
  (cond
    (and (map? v) (= "TIME" (get v :$reql-type$))) (t/from :long (* 1000 (:epoch-time v)))
    (or (vector? v) (seq? v)) (map rt-> v)
    (string? v) (rt-string-> v)
    :else v))

(defn- rt-string->
  [s]
  (let [[_ type-fn value-str] (re-find #"^servo/(.*)=(.*)$" s)]
    (if (and type-fn value-str)
      ((case type-fn
         "keyword" (comp keyword hyphenate)
         "str" str
         "double" string->double
         "long" string->long) value-str)
      s)))

(defn- ->rt
  [v]
  (cond
    (or (seq? v) (vector? v)) [(get term-types :make-array) (map ->rt v)]
    (map? v) (xform-map v ->rt-key ->rt-value)
    :else (->rt-value v)))

(defn- rt->
  [m]
  (cond
    (map? m) (not-empty (xform-map m rt-key-> rt-value->))
    :else (rt-value-> m)))

(defn- ensure-db
  [connection db-name]
  (when-not ((set @(run connection [[:db-list]])) (rt-name-> db-name))
    @(run connection [[:db-create (->rt-name db-name)]])))

(defn- await-tables-ready
  [{:keys [trace] :as connection}]
  (let [table-list @(run connection [[:table-list]])
        await-tables-ready (fn []
                             (->> table-list
                                  (map (fn [table]
                                         @(run connection [[:table table] [:status]])))
                                  (filter (comp :all-replicas-ready :status))
                                  count))]
    (loop []
      (let [ready-count (await-tables-ready)]
        (when (< ready-count (count table-list))
          (when trace (println (format ":servo/connection %s/%s tables ready."
                                       ready-count
                                       (count table-list))))
          (Thread/sleep 1000)
          (recur))))
    (when trace (println ":servo/connection all tables ready"))))

(def ^:private request-types
  {:start 1
   :continue 2
   :stop 3
   :noreply-wait 4
   :server-info 5})

(def ^:private response-types
  {1 :success-atom
   2 :success-sequence
   3 :success-partial
   4 :wait-complete
   5 :server-info
   16 :client-error
   17 :compile-error
   18 :runtime-error})

(def ^:private response-error-types
  {1000000 :internal
   2000000 :resource-limit
   3000000 :query-logic
   3100000 :non-existence
   4100000 :op-failed
   4200000 :op-indeterminate
   5000000 :user
   6000000 :permission-error})

(def ^:private response-note-types
  {1 :sequence-feed
   2 :atom-feed
   3 :order-by-limit-feed
   4 :unioned-feed
   5 :includes-states})

(def ^:private term-types
  {:datum 1
   :make-array 2
   :make-obj 3
   :javascript 11
   :uuid 169
   :http 153
   :error 12
   :implicit-var 13
   :db 14
   :table 15
   :get 16
   :get-all 78
   :eq 17
   :ne 18
   :lt 19
   :le 20
   :gt 21
   :ge 22
   :not 23
   :add 24
   :sub 25
   :mul 26
   :div 27
   :mod 28
   :floor 183
   :ceil 184
   :round 185
   :append 29
   :prepend 80
   :difference 95
   :set-insert 88
   :set-intersection 89
   :set-union 90
   :set-difference 91
   :slice 30
   :skip 70
   :limit 71
   :offsets-of 87
   :contains 93
   :get-field 31
   :keys 94
   :values 186
   :object 143
   :has-fields 32
   :with-fields 96
   :pluck 33
   :without 34
   :merge 35
   :between-deprecated 36
   :between 182
   :reduce 37
   :map 38
   :fold 187
   :filter 39
   :concat-map 40
   :order-by 41
   :distinct 42
   :count 43
   :is-empty 86
   :union 44
   :nth 45
   :bracket 170
   :inner-join 48
   :outer-join 49
   :eq-join 50
   :zip 72
   :range 173
   :insert-at 82
   :delete-at 83
   :change-at 84
   :splice-at 85
   :coerce-to 51
   :type-of 52
   :update 53
   :delete 54
   :replace 55
   :insert 56
   :db-create 57
   :db-drop 58
   :db-list 59
   :table-create 60
   :table-drop 61
   :table-list 62
   :config 174
   :status 175
   :wait 177
   :reconfigure 176
   :rebalance 179
   :sync 138
   :grant 188
   :index-create 75
   :index-drop 76
   :index-list 77
   :index-status 139
   :index-wait 140
   :index-rename 156
   :set-write-hook 189
   :get-write-hook 190
   :funcall 64
   :branch 65
   :or 66
   :and 67
   :for-each 68
   :func 69
   :asc 73
   :desc 74
   :info 79
   :match 97
   :upcase 141
   :downcase 142
   :sample 81
   :default 92
   :json 98
   :iso8601 99
   :to-iso8601 100
   :epoch-time 101
   :to-epoch-time 102
   :now 103
   :in-timezone 104
   :during 105
   :date 106
   :time-of-day 126
   :timezone 127
   :year 128
   :month 129
   :day 130
   :day-of-week 131
   :day-of-year 132
   :hours 133
   :minutes 134
   :seconds 135
   :time 136
   :monday 107
   :tuesday 108
   :wednesday 109
   :thursday 110
   :friday 111
   :saturday 112
   :sunday 113
   :january 114
   :february 115
   :march 116
   :april 117
   :may 118
   :june 119
   :july 120
   :august 121
   :september 122
   :october 123
   :november 124
   :december 125
   :literal 137
   :group 144
   :sum 145
   :avg 146
   :min 147
   :max 148
   :split 149
   :ungroup 150
   :random 151
   :changes 152
   :arguments 154
   :binary 155
   :geojson 157
   :to-geojson 158
   :point 159
   :line 160
   :polygon 161
   :distance 162
   :intersects 163
   :includes 164
   :circle 165
   :get-intersecting 166
   :fill 167
   :get-nearest 168
   :polygon-sub 171
   :to-json-string 172
   :minval 180
   :maxval 181
   :bit-and 191
   :bit-or 192
   :bit-xor 193
   :bit-not 194
   :bit-sal 195
   :bit-sar 196})
