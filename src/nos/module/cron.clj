(ns nos.module.cron
  (:require[integrant.core :as ig]
           [clojure.core.async :as async :refer [go >! <! go-loop close! <!!]]
           [nos.core :as flow]
           [chime.core :as chime]
           [konserve.core :as kv]
           [chime.core-async :refer [chime-ch]])
  (:import [java.time Instant Duration]))

(def props
  {:threadPool.class "org.quartz.simpl.SimpleThreadPool"
   :threadPool.threadCount 1
   :plugin.triggHistory.class "org.quartz.plugins.history.LoggingTriggerHistoryPlugin"
   :plugin.jobHistory.class "org.quartz.plugins.history.LoggingJobHistoryPlugin"})

(derive :nos.module/cron :duct/daemon)
(derive :nos.module/cron :nos/trigger)

(derive ::probe ::flow/fx)
;;(run-graph [[:sci/eval (flow/ref :input) "(prn \"aa\") :await-probe"] [:probe :sci/eval 2000 (flow/dep :input)]] "bb")

(defn- probe
  "Starts a async loop that executes a probe

  Once the `chimes-ch` is closed or when `flow-id` has a result registered for
  `target-op` then `probe-id` is deregistered from the `store`."
  [store probes chan flow-id target-op chimes-ch probe-id]
  (go-loop []
    (when-let [time (<! chimes-ch)]
      (let [f (<! (flow/load-flow flow-id store))
            target-res (get (:results f) target-op)]
        ;; if our target op has a result, we cancel the timer
        (if (or (not f) (and (some? target-res) (not (= :await-probe target-res))))
          (do (prn "Deregistering probe for " flow-id target-res f target-op)
              ;; TODO: deliver the future of the probe
              (swap! probes dissoc probe-id)
              (<! (kv/update store :probes dissoc probe-id))
              (close! chimes-ch))
          (do (>! chan [:fx flow-id target-op [::flow/rerun]])
              (>! chan [:trigger flow-id])
              (recur)))))))

;; A probe executes a `flow` every `interval` milliseconds. It will monitor a
;; `target-op` in that flow. When that `target-op` has generated a result the
;; probe will cancel itself.
;;
;; Note that the `chimes` channel produces the ticks and `interval` is only used
;; to persist recurrent probes to the store. If a probe should only tick
;; once (like in a delay) interval should be nil and chimes an array with a time
;; instant.
;;
;; Probes are registered in the store under [:probes {<id> <prob>}] and are
;; reloaded on startup by the `cron-engine` system. The current active probes
;; are stored there with their chime channels.
(defmethod flow/handle-fx ::probe
  [{:keys [store probes chan] :as fe} op [_ target-op chimes interval :as fx] flow]
  (prn "Starting Probe Timer for Interval " interval)
  (let [[_ probe-id :as fut] (flow/make-future)
        chimes-ch (chime-ch chimes)]
    (when interval
      (go (<! (kv/assoc-in store [:probes probe-id]
                           {:flow-id (:id flow)
                            :target-op target-op
                            :interval interval}))))
    (swap! probes assoc probe-id chimes-ch)
    (probe store probes chan (:id flow) target-op chimes-ch probe-id)
    (assoc-in flow [:results op] fut)))

;; Wrapper op that creates an fx
(defmethod flow/run-op :probe [_ _ [target-op interval]]
  (prn "Starting Probe " target-op interval)
  [::probe target-op (chime/periodic-seq (Instant/now) (Duration/ofMillis interval)) interval])

;; Probe that ticks a single time after a delay
(defmethod flow/run-op :probe-after [op-id flow [delay-s & nodes]]
  (prn nodes)
  (if (contains? (:nodes flow) [:data op-id :count])
    [::flow/nodes (into [] nodes)]
    [::flow/fx-set
     [::probe op-id [(.plusSeconds (Instant/now) delay-s)] nil]
     [::flow/nodes [[[:data op-id :count] 1]]]
     :await-probe]))

(defmethod ig/init-key :nos/probes [_ {:keys [store chan]}]
  (let [probes (<!! (kv/get store :probes))
        probes-atom (atom {})]
    ;; persist probes on reboots
    (doseq [[id {:keys [interval flow-id target-op]}] probes]
      (let [chimes-ch (chime-ch (chime/periodic-seq
                                 (Instant/now)
                                 (Duration/ofMillis interval)))]
        (probe store probes-atom chan flow-id target-op chimes-ch id)
        (swap! probes-atom assoc id chimes-ch)))
    probes-atom))

(defn remove-all-probes! [{:keys [store probes]}]
  (let [store-probes (<!! (kv/get store :probes))]
    (doall
     (for [[probe-id _] store-probes]
       (do (when-let [probe-ch (get @probes probe-id)]
             (close! probe-ch))
           (swap! probes dissoc probe-id)
           (<!! (kv/update store :probes dissoc probe-id)))))))

(defn run-load-flow [scheduler {:keys [store chan]} input-data program-id]
  (go
    (let [flow-id (-> program-id
                      (flow/load-program store)
                      (flow/set-input input-data)
                      (flow/save-flow store)
                      <! second :id)]
      (>! chan [:trigger flow-id]))))
