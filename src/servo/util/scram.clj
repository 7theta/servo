;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   MIT License (https://opensource.org/licenses/MIT) which can also be
;;   found in the LICENSE file at the root of this distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns servo.util.scram
  (:require [servo.util.crypto :as crypto]
            [servo.util.base64 :as base64]
            [utilis.types.number :refer [string->long]]
            [clojure.set :refer [map-invert]]
            [clojure.string :as st]))

(declare encode decode)

(defn initial-proposal
  [{:keys [username]}]
  (let [nonce-length 18
        nonce (base64/encode (crypto/nonce nonce-length))
        client-initial-scram {:username username :nonce nonce}]
    {:client-initial-scram client-initial-scram
     :message {:protocol-version 0
               :authentication-method "SCRAM-SHA-256"
               :authentication (str "n,," (encode client-initial-scram))}}))

(defn server-challenge
  [{:keys [client-initial-scram]} {:keys [success authentication] :as response}]
  (when-not success ex-info ":servo/connection initial handshake failed"
            {:client-initial-scram client-initial-scram
             :authenticaton authentication})
  (let [{:keys [nonce salt iterations] :as server-scram} (decode authentication)
        iterations (string->long iterations)]
    (if-not (st/starts-with? nonce (:nonce client-initial-scram))
      (throw (ex-info ":servo/connection invalid nonce from server"
                      {:client-initial-scram client-initial-scram
                       :server-scram server-scram}))
      {:server-authentication authentication
       :server-scram (assoc server-scram :iterations iterations)})))

(defn authentication-request
  [{:keys [client-initial-scram server-authentication server-scram]}]
  (let [client-scram {:header-and-channel-binding "biws"
                      :nonce (:nonce server-scram)}
        salted-password (crypto/pbkdf2 "" (:salt server-scram) (:iterations server-scram))
        client-key (crypto/hmac salted-password "Client Key")
        stored-key (crypto/sha256 client-key)
        authentication-message (str (encode client-initial-scram) ","
                                    server-authentication ","
                                    (encode client-scram))
        client-signature (crypto/hmac stored-key authentication-message)
        client-proof (crypto/xor client-key client-signature)
        server-key (crypto/hmac salted-password "Server Key")
        server-signature (crypto/hmac server-key authentication-message)]
    {:message {:authentication (encode (assoc client-scram :client-proof (base64/encode client-proof)))}
     :server-signature server-signature}))

(defn validate-server
  [{:keys [server-signature server-authentication]}]
  (crypto/digest= server-signature
                  (base64/decode (:server-signature (decode server-authentication)))))

;;; Private

(def ^:private attributes
  {"a" :identity
   "n" :username
   "r" :nonce
   "c" :header-and-channel-binding
   "s" :salt
   "i" :iterations
   "p" :client-proof
   "v" :server-signature
   "e" :error})

(defn encode
  [{:keys [username nonce header-and-channel-binding client-proof] :as attribtes}]
  (let [serialize #(-> % (st/replace #"=" "=3D") (st/replace #"," "=2C"))
        keys (map-invert attributes)]
    (->> [(when username (str (:username keys) "=" (serialize username)))
          (when nonce (str (:nonce keys) "=" nonce))
          (when header-and-channel-binding (str (:header-and-channel-binding keys) "=" header-and-channel-binding))
          (when client-proof (str (:client-proof keys) "=" client-proof))]
         (remove nil?)
         (st/join #","))))

(defn decode
  [attribute-str]
  (->> (st/split attribute-str #",")
       (map (fn [attribute]
              [(get attributes (subs attribute 0 1))
               (subs attribute 2)]))
       (into {})))
