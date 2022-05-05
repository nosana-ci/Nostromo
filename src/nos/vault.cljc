(ns nos.vault
  (:require [aero.core :as aero]
            [integrant.core :as ig]))

(defprotocol Vault
  (get-secret [vault id] "Get a secret"))

(extend-protocol Vault
  clojure.lang.PersistentArrayMap
  (get-secret [vault id]
    (get vault id)))

(defmethod ig/init-key :nos/vault
  [_ {path :path profile :profile}]
  (aero/read-config path {:profile profile}))
