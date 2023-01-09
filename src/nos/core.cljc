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
  (log :info (str "Running operator " (:id operator) " (" (:op operator) ")"))
  (keyword (:op operator)))

(defmulti run-op #'run-op-dispatch)

;; helpers
(defn future? [form] (or
                      (and (vector? form) (= ::future (first form)))
                      (= :await-probe form)))
(defn error? [form] (when (and (vector? form) (= :nos/error (first form))) form))
(defn uuid [] (nano-id))
(defn make-future [] [::future (uuid)])

(defn current-time
  "Return current time in seconds since epoch."
  []
  #?(:clj (quot (System/currentTimeMillis) 1000)
     :cljs (quot (.getTime (js/Date.)) 1000)))

;; graphs
(defn ref [key] [::ref key])
(defn ref? [key] (and (vector? key) (or (= (first key) ::ref)
                                        (= (first key) ::dep))))

(defn reflike? [key]
  (and (vector? key)
       (let [v (keyword (first key))]
         (or (= v :nos/ref)
             (= v :nos/vault)
             (= v :nos/dep)
             (= v :nos/str)
             (isa? v ::ref)))))

(defn vault [key] [::vault key])
(defn ref-nodes [node] [::nodes node])
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
  "Resolves the value for an op argument given a flow result set."
  (fn [r _state _vault]
    (if (reflike? r)
      (keyword (first r))
      (if (map? r)
        :nos/map
        (if (coll? r)
          :nos/coll
          :nos/pass)))))

(defmethod ref-val :nos/ref [r state _] (get state (second r)))
(defmethod ref-val :nos/vault [r _ vault] (vault/get-secret vault (second r)))
(defmethod ref-val :nos/dep [_ _ _] :nos/skip)
;; the ::str inline operator concatenates arguments as strings and is recursive
(defmethod ref-val :nos/str [r state vault] (apply str (map #(ref-val % state vault) (rest r))))
(defmethod ref-val :nos/pass [r _ _] r)
(defmethod ref-val :nos/coll [r res vault] (map #(ref-val % res vault) r))
(defmethod ref-val :nos/map [r res vault] (fmap* #(ref-val % res vault) r))
(defmethod ref-val :nos/state [r state vault] state)

(defn ref-key [ref] (second ref))

;; default ops
(defmethod run-op :nos/get         [_ _ [m k]] (get m k))
(defmethod run-op :nos/flow-id     [_ {:keys [id]} _] id)
(defmethod run-op :nos/get-in      [_ _ [m k]] (get-in m k))
(defmethod run-op :nos/prn         [_ _ [& d]] (apply prn d) (into [] d))
(defmethod run-op :nos/uuid        [_ _ _] (uuid))
(defmethod run-op :nos/assoc       [_ _ [& pieces]] (apply assoc pieces))
(defmethod run-op :nos/str         [_ _ [& pieces]] (reduce str pieces))
(defmethod run-op :nos/slurp       [_ _ [uri]] (slurp uri))
(defmethod run-op :nos/spit        [_ _ [file uri]] (spit uri file))
(defmethod run-op :nos/json-decode [_ _ [json]] (json/decode json))
(defmethod run-op :nos/sleep       [_ _ [duration arg]] (Thread/sleep duration) arg)
(defmethod run-op :nos/count       [_ _ [coll]] (count coll))
(defmethod run-op :nos/data        [_ _ [d]]  d)
(defmethod run-op :nos/concat      [_ _ [a b]] (into [] (concat a b)))
(defmethod run-op :nos/vec         [_ _ [& e]] (into [] e))
(defmethod run-op :nos/await       [_ _ [& e]] (make-future))
(defmethod run-op :nos/fx          [_ _ [& a]] (into [] (concat [:nos/fx-set] a)))
(defmethod run-op :nos/flow        [_ _ args] (conj [:nos/flow] args))

#?(:clj
   (defmethod run-op :nos/download
     [_ _ [{:keys [url path]}]] (spit path (slurp url)) path)
   (defmethod run-op :nos/file-exist?
     [_ _ [file-path]] (.exists (io/as-file file-path)))
   (defmethod run-op :nos/sh
     [_ _ [& args]] (apply clojure.java.shell/sh args)))

(defmethod run-op :nos/when [k _ [cond & nodes]]
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
  "Fill in missing fields of a partial flow."
  ([program] (build (uuid) program))
  ([uuid program]
   (merge {:id             uuid
           :execution-path []
           :state          {}}
          program)))

(defn add-ops [flow nodes]
  (update flow :ops #(into [] (concat % nodes))))

(defn load-program [program-id store]
  (-> (<!! (kv/get-in store [:programs program-id]))
      read-string
      build))

(defn deliver-future [{:keys [chan]} future-id data]
  (go (>! chan [:deliver future-id data]))
  true)

(defn set-input [flow input] (assoc-in flow [:state :input] input))

(defn resolve-args [args {:keys [state] :as p} vault]
  (ref-val args state vault))

;; effects
(defn fx? [res] (and (coll? res) (isa? (first res) ::fx)))

(defn handle-fx-dispatch [flow-engine op fx flow]
  (if (fx? fx)
    (first fx)
    nil))

(defmulti handle-fx #'handle-fx-dispatch)

(derive :nos/fx-set ::fx)    ; seq of multiple effects
(derive :nos/future ::fx)    ; resolved by a future delivery
(derive :nos/flow ::fx)      ; merge a new flow into the current
(derive :nos/rerun ::fx)     ; ignore op result and rerun on next execution
(derive :nos/error ::fx)     ; base error handler

(defmethod handle-fx :nos/fx-set [fe op [_ & fxs] flow]
  (reduce #(handle-fx fe op %2 %1) flow fxs))

;; register a future in the store
(defmethod handle-fx :nos/future
  [{:keys [store]} op [_ future-id :as res] flow]
  (log :info "Registering future " res)
  (go (<! (kv/assoc-in store [future-id :deliver] [(:id flow) (:id op)])))
  (assoc-in flow [:state (:id op)] res))

(defmethod handle-fx :nos/flow
  [_ op [_ {:keys [ops]}] flow]
  ;; note: the state of the new flow is currently ignored
  (-> flow
      (add-ops ops)
      (assoc-in [:state (:id op)] [::new-ops (count ops)])
      (update :execution-path concat ops)))

(defmethod handle-fx :nos/rerun [_ op _ flow]
  (update-in flow [:state] dissoc (:id op)))

;; This is the default error handler for Nostromo operators. It will
;; assosciate the results of the op in the state, reset execution
;; path, and if `allow-failure?` is false for both the flow and the op
;; finish the flow.
(defmethod handle-fx :nos/error [_ op [_ msg :as res] flow]
  (log :error "Exception in op " (:id op) msg)
  (let [allow-failure? (or (= true (:allow-failure? op))
                           (and (= true (:allow-failure? flow))
                                (not (= false (:allow-failure? op)))))]
    (cond-> flow
      true                 (assoc-in [:state (:id op)] res)
      (not allow-failure?) (assoc :status :failed
                                  :execution-path []))))

(defmethod handle-fx :default [_ op res flow]
  (assoc-in flow [:state (:id op)] res))

(defn ex-chain [e]
  (loop [e e
         result []]
    (if (nil? e)
      result
      (recur (ex-cause e) (conj result e)))))

(defn ex-print
  [^Throwable e]
  (let [indent "  "]
    (doseq [e (ex-chain e)]
      (println (-> e class .getCanonicalName))
      (print indent)
      (println (ex-message e))
      (when-let [data (ex-data e)]
        (print indent)
        (clojure.pprint/pprint data)))))

(defn execute-op
  "Execute an operator `op` in `flow`.
  Returns a vector of effects. If the flow has `:default-args`
  specified for op, then `args` will be merged into this value."
  [op args {:keys [default-args] :as flow}]
  (let [args (if (contains? default-args (keyword (:op op)))
               (merge (get default-args (keyword (:op op))) args)
               args)

        res (try
              (run-op op flow args)
              (catch Exception e
                (println "Exception executing operator " e)
                (ex-print e)
                [:nos/error (ex-message e)]))]
    res))

(defn finished?
  "Check if a flows status is in a finished state.
  Finished states are `:failed` and `:succeeded`, or when every op has
  a results."
  [{:keys [status state ops] :as _flow}]

  (or (= status :failed) (= status :succeeded)
      (every? state (map :id ops))))

(defn run-flow!
  "Execute pending operators in a `flow`and ex.
  Ops are executed in order. If an op has any unresolved dependencies
  or argument it is skipped."
  ([{:keys [store vault] :as fe} flow]
   (if (finished? flow)
     flow
     (let [orig-flow (-> flow
                         (assoc :execution-path (:ops flow)))]
       (loop [{:keys       [id ops state]
               [op & path] :execution-path :as ff} orig-flow]
         (let [f (update ff :execution-path rest)]
           (cond
             (nil? op)                  f
             (contains? state (:id op)) (recur f)
             :else
             (let [args  (resolve-args (:args op) f vault)
                   deps  (resolve-args (:deps op) f vault)
                   chkf  #(or (future? %) (nil? %))
                   error (or (some error? args) (error? args))]
               (prn args)
               (if (or (some chkf (concat args deps))
                       error)
                 (recur f)          ; if a dep is pending or a future, do not run-op
                 (do
                   (let [res      (execute-op op args f)
                         new-flow (handle-fx fe op res f)]
                     (go (<! (kv/assoc store id new-flow)))
                     (recur new-flow))))))))))))
