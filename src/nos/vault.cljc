(ns nos.vault
  (:require [aero.core :as aero]))

(defprotocol Vault
  (get-secret [vault id] "Get a secret"))

(extend-protocol Vault
  clojure.lang.PersistentArrayMap
  (get-secret [vault id]
    (get vault id)))

(extend-protocol Vault
  clojure.lang.PersistentHashMap
  (get-secret [vault id]
    (get vault id)))

(defn use-vault [{:keys [nos/vault-path system/profile] :as system}]
  (assoc system :nos/vault
         (aero/read-config vault-path {:profile profile})))
