;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   MIT License (https://opensource.org/licenses/MIT) which can also be
;;   found in the LICENSE file at the root of this distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns servo.util.crypto
  (:require [servo.util.base64 :as base64])
  (:import [javax.crypto.spec PBEKeySpec]
           [javax.crypto SecretKeyFactory Mac]
           [javax.crypto.spec SecretKeySpec]
           [java.security SecureRandom MessageDigest]
           [java.nio.charset StandardCharsets]))

(defonce ^:private secure-random (SecureRandom.))

(defn nonce
  [length]
  (doto (byte-array length)
    (#(.nextBytes ^SecureRandom secure-random %))))

(defn pbkdf2
  [^String password ^String salt iterations]
  (let [spec (PBEKeySpec. (.toCharArray password)
                          (base64/decode salt)
                          (int iterations) 256)]
    (-> (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")
        (.generateSecret spec)
        (.getEncoded))))

(defn hmac
  [key ^String string]
  (.doFinal (doto (Mac/getInstance "HmacSHA256")
              (.init (SecretKeySpec. key "HmacSHA256")))
            (.getBytes string StandardCharsets/UTF_8)))

(defn sha256
  [value]
  (.digest (MessageDigest/getInstance "SHA-256") value))

(defn digest=
  [a b]
  (MessageDigest/isEqual a b))

(defn xor
  [a b]
  (byte-array (map (fn [a b] (bit-xor a b)) a b)))
