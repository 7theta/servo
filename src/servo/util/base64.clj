;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   MIT License (https://opensource.org/licenses/MIT) which can also be
;;   found in the LICENSE file at the root of this distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns servo.util.base64
  (:import [java.util Base64]))

(defn encode
  [b]
  (.encodeToString (Base64/getEncoder) b))

(defn decode
  [s]
  (.decode (Base64/getDecoder) ^String s))
