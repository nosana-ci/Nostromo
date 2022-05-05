(ns nos.store
  (:require
   [integrant.core :as ig]
   [clojure.core.async :as a :refer [>! <! >!! <!! go go-loop chan put!]]
   [konserve.protocols :refer [-exists? -get-meta -get -assoc-in
                               -update-in -dissoc -bget -bassoc
                               -keys]]
   [hasch.core :refer [uuid]]
   [konserve.filestore :refer [new-fs-store]]
   [konserve.core :as kv :refer [go-locked]]
   [konserve.serializers :as ser]
   [taoensso.timbre :as timbre :refer [trace]]
   [clojure.data.fressian :as fres]))


(defn log-prepend
  "Prepend an element to a Konserve append log"
  [store key elem]
  (trace "append on key " key)
  (go-locked
   store key
   (let [head (<! (-get store key))
         [append-log? last-id first-id] head
         new-elem {:next first-id
                   :elem elem}
         id (uuid)]
     (when (and head (not= append-log? :append-log))
       (throw (ex-info "This is not an append-log." {:key key})))
     (<! (-update-in store [id] (partial kv/meta-update key :append-log) (fn [_] new-elem) []))
     (<! (-update-in store [key] (partial kv/meta-update key :append-log)
                     (fn [_] [:append-log (or last-id id) id]) []))
     [last-id id])))

(defn log-take
  "Loads `n` recent items of the append log stored at `key`"
  [store key n]
  (trace "log on key " key)
  (go
    (let [head (<! (kv/get store key))
          [append-log? last-id first-id] head]
      (when (and head (not= append-log? :append-log))
        (throw (ex-info "This is not an append-log." {:key key})))
      (when first-id
        (loop [{:keys [next elem]} (<! (kv/get store first-id))
               hist []]
          (if (and next (< (count hist) (dec n)))
            (recur (<! (kv/get store next))
                   (conj hist elem))
            (conj hist elem)))))))

(defmethod ig/init-key :nos/store [_ {:keys [path]}]
  (<!! (new-fs-store path)))
