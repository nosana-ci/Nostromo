(ns nos.core
  (:refer-clojure :exclude [ref read-string future?])
  (:require
   [clojure.spec.alpha :as s]
   [clojure.edn :as edn]
   [clojure.core.async :as a :refer [>! <! >!! <!! go go-loop chan put!]]
   [clojure.string :as str]
   [konserve.filestore :refer [new-fs-store]]
   [konserve.core :as kv]
   [clojure.java.io :as io]
   [nano-id.core :refer [nano-id]]
   [tick.alpha.api :as tick]
   [taoensso.timbre :refer [log]]
   [nos.vault :as vault]
   [nos.store :refer [log-prepend]]
   [cheshire.core :as json]
   [clojure.java.shell :refer [sh]]
   [weavejester.dependency :as dep]))

;; specs
(s/def ::single-kw qualified-keyword?)
(s/def ::vector vector?)
(s/def ::config map?)

(s/def ::op keyword?)
(s/def ::operator (s/keys :req-un [::op ::id ::args ::deps]))
(s/def ::ops (s/coll-of ::operator))

(s/def ::name string?)
(s/def ::id (s/or :string string? :keyword keyword?))
(s/def ::flow (s/keys :req-un [::ops]
                      :opt-un [::name ::id ::description]))
(s/check-asserts true)

(defn- run-op-dispatch [operator flow args]
  (log :info "Running operator " operator)
  operator)

(defmulti run-op #'run-op-dispatch)

;; helpers
(defn future? [form] (or
                      (and (vector? form) (= ::future (first form)))
                      (= :await-probe form)))
(defn error? [form] (and (vector? form) (= ::error (first form))))
(defn uuid [] (nano-id))
(defn make-future [] [::future (uuid)])

(defn current-time
  "Return current time in seconds since epoch

  Helper function."
  []
  #?(:clj (quot (System/currentTimeMillis) 1000)
     :cljs (quot (.getTime (js/Date.)) 1000)))

;; graphs
(defn ref [key] [::ref key])
(defn ref? [key] (and (vector? key) (or (= (first key) ::ref)
                                        (= (first key) ::dep))))

(defn reflike? [key]
  (and (vector? key) (or (= (first key) ::ref)
                         (= (first key) ::vault)
                         (= (first key) ::dep)
                         (= (first key) ::str))))

(defn vault [key] [::vault key])
(defn ref-nodes [node] [::nodes node])
(defn new-nodes? [key] (and (vector? key) (= (first key) ::nodes)))
(defn dep [key] [::dep key])
(defn ref-local [key] [::ref-local key])

(defn fmap [f m]
  (into (empty m) (for [[k v] m] [k (f v)])))

(defn fmap*
  [f m]
  (fmap #(if (map? %)
           (fmap* f %)
           (f %))
        m))

(defmulti ref-val
  "Resolve the value for an op argument given a flow result set"
  (fn [r results vault]
    (if (reflike? r)
      (first r)
      (if (map? r)
        ::map
        (if (coll? r)
          ::coll
          ::pass)))))

(defmethod ref-val ::ref [r results _] (get results (second r)))
(defmethod ref-val ::vault [r _ vault] (vault/get-secret vault (second r)))
(defmethod ref-val ::dep [_ _ _] ::skip)
;; the ::str inline operator concatenates arguments as strings and is recursive
(defmethod ref-val ::str [r results vault] (apply str (map #(ref-val % results vault) (rest r))))
(defmethod ref-val ::pass [r _ _] r)
(defmethod ref-val ::coll [r res vault] (map #(ref-val % res vault) r))
(defmethod ref-val ::map [r res vault] (fmap* #(ref-val % res vault) r))

(defn ref-key [ref] (second ref))

;; default ops
(defmethod run-op :get         [_ _ [m k]] (get m k))
(defmethod run-op :flow-id     [_ {:keys [id]} _] id)
(defmethod run-op :get-in      [_ _ [m k]] (get-in m k))
(defmethod run-op :prn         [_ _ [& d]] (apply prn d) d)
(defmethod run-op :uuid        [_ _ _] (uuid))
(defmethod run-op :assoc       [_ _ [& pieces]] (apply assoc pieces))
(defmethod run-op :str         [_ _ [& pieces]] (reduce str pieces))
(defmethod run-op :slurp       [_ _ [uri]] (slurp uri))
(defmethod run-op :spit        [_ _ [file uri]] (spit uri file))
(defmethod run-op :json-decode [_ _ [json]] (json/decode json))
(defmethod run-op :sleep       [_ _ [duration arg]] (Thread/sleep duration) arg)
(defmethod run-op :count       [_ _ [coll]] (count coll))
(defmethod run-op :data        [_ _ [d]]  d)
(defmethod run-op :concat      [_ _ [a b]] (into [] (concat a b)))
(defmethod run-op :vec         [_ _ [& e]] (into [] e))
(defmethod run-op :await       [_ _ [& e]] (make-future))
(defmethod run-op :fx          [_ _ [& a]] (into [] (concat [::fx-set] a)))

#?(:clj
   (defmethod run-op :download
     [_ _ [url out-path]] (spit out-path (slurp url)) out-path)
   (defmethod run-op :file-exist?
     [_ _ [file-path]] (.exists (io/as-file file-path)))
   (defmethod run-op :sh
     [_ _ [& args]] (apply clojure.java.shell/sh args)))

(defmethod run-op :when [k _ [cond & nodes]]
  (ref-nodes (when cond nodes)))



(defn load-flow [flow-id store]
  (kv/get-in store [flow-id]))

(defn save-flow [flow store]
  (kv/assoc-in store [(:id flow)] flow))

(defn read-string [s]
  (edn/read-string {:readers {'ref ref 'vault vault 'dep dep
                              'ref-local ref-local}} s))


(defn find-refs [v]
  (->> v (filter ref?) (map ref-key) set))

(defn build
  ([program] (build (uuid) program))
  ([uuid program]
   (merge {:id uuid
           :execution-path []
           :results {}} program)))

(defn add-ops [flow nodes]
  (update flow :ops #(into [] (concat % nodes))))

(defn load-program [program-id store]
  (-> (<!! (kv/get-in store [:programs program-id]))
      read-string
      build))

(defn deliver-future [{:keys [chan]} future-id data]
  (go (>! chan [:deliver future-id data]))
  true)

(defn set-input [flow input] (assoc-in flow [:results :input] input))



(defn resolve-args [args {:keys [results] :as p} vault]
  (ref-val args results vault))

;; effects
(defn fx? [res] (and (coll? res) (isa? (first res) ::fx)))

(defn handle-fx-dispatch [flow-engine op fx flow]
  (if (fx? fx)
    (first fx)
    nil))

(defmulti handle-fx #'handle-fx-dispatch)

(derive ::fx-set ::fx)    ; seq of multiple effects
(derive ::future ::fx)    ; resolved by a future delivery
(derive ::nodes ::fx)     ; add new nodes to the graph
(derive ::rerun ::fx)     ; ignore op result and rerun on next execution
(derive ::rerun ::fx)     ; ignore op result and rerun on next execution
(derive ::error ::fx)     ; base error handler

(defmethod handle-fx ::fx-set [fe op [_ & fxs] flow]
  (reduce #(handle-fx fe op %2 %1) flow fxs))

;; register a future in the store
(defmethod handle-fx ::future [{:keys [store]} op [_ future-id :as res] flow]
  (log :info "Registering future " res)
  (go (<! (kv/assoc-in store [future-id :deliver] [(:id flow) (:id op)])))
  (assoc-in flow [:results (:id op)] res))

(defmethod handle-fx ::nodes [_ op [_ nodes] flow]
  (-> flow (add-ops nodes)
      (assoc-in [:results (:id op)] [::new-nodes (count nodes)])))

(defmethod handle-fx ::rerun [_ op _ flow]
  (update-in flow [:results] dissoc (:id op)))

(defmethod handle-fx ::error [_ op [_ msg :as res] flow]
  (log :error "Exception in op " op msg)
  (-> flow
      (assoc-in [:results (:id op)] res)
      (assoc :execution-path [])))

(defmethod handle-fx :default [_ op res flow]
  (assoc-in flow [:results (:id op)] res))



;;(run-graph [[:for (flow/ref :input) [:prn :#i (flow/dep :input)]]] [1 2])

(defn execute-op
  "Execute an operator `op` in `flow`."
  [op args flow]
  (let [res (try
              (run-op (:op op) flow args)
              (catch Exception e
                (println "Exception executing operator " e)
                [::error (ex-message e)]))]

    ;; (as-> flow f
    ;;   (try
    ;;     (handle-fx engine op res f)
    ;;     (catch Exception e
    ;;       (println "Exception handling fx for operator " e))))
    res
    ))

(defn do-flow-step
  [op {:keys [results] :or {results []} :as flow} vault]
  (cond
    (nil? op) flow
    (contains? results op) flow
    :else
    (let [args (resolve-args (:args op) flow vault)
          deps (resolve-args (:deps op) flow vault)
          chkf #(or (future? %) (nil? %))]
      ;; if there are unresolved args or deps we don't do anything
      (if (or (some chkf (concat args deps)))
        flow
        (execute-op op args flow)))))

(defn run-flow!
  "Execute pending operators in a `flow`and ex

  Ops are executed in order. If an op has any unresolved dependencies or
  argument it is skipped."
  ([{:keys [store vault] :as fe} flow]

   (go
     ;; TODO: recent flows can now contain duplicate items, this code should
     ;; probably be moved to the initial triggering event
     (<! (log-prepend store :recent-flows (:id flow))))

   (let [orig-flow (-> flow
                       (assoc :execution-path (:ops flow)))]
     (loop [{:keys [id ops results] [op & path] :execution-path :as ff} orig-flow]
       (let [f (update ff :execution-path rest)]
         (cond
           (nil? op) f
           (contains? results op) (recur f)
           :else
           (let [args (resolve-args (:args op) f vault)
                 deps (resolve-args (:deps op) f vault)
                 chkf #(or (future? %) (nil? %))]
             (if (some chkf (concat args deps))
               (recur f)          ; if a dep is pending or a future, do not run-op
               (do
                 (go (<! (kv/assoc store :active-flow [id (:id op)])))
                 (let [res (execute-op op args f)
                       new-flow (handle-fx fe op res f)]
                   (go
                     ;; TODO: we're writing to store every step for the demo, not the nicest design
                     (<! (kv/dissoc store :active-flow))
                     (<! (kv/assoc store id new-flow)))
                   (recur new-flow)))))))))))
