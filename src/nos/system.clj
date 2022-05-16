(ns nos.system
  (:require
   [clojure.string :as str]
   [konserve.core :as kv]
   [clojure.java.io :as io]
   [nos.core :as nos]
   [integrant.core :as ig]
   [clojure.core.async :as a :refer [>! <! >!! <!! go go-loop chan put!]]
   [taoensso.timbre :refer [log]])
  (:import org.apache.commons.io.FilenameUtils
           java.util.Base64))

(defn get-log-filename [log-dir flow-id]
  (str log-dir flow-id ".txt"))

(defn- process-event!
  "Trigger a flow after delivering an external result.

  The value of `op` will be set to `result` in `flow-id` and the flow is then
  run from start."
  ([fe flow-id]
   (process-event! fe flow-id [nil nil]))

  ([{:keys [vault store cron log-dir] :as fe} flow-id [op result]]
   (log :info "Starting flow " flow-id)

   (let [log-file-name (get-log-filename log-dir flow-id)
         _ (log :info "Log file name is " log-file-name)
         _ (io/make-parents log-file-name)]
     (with-open [log-file (io/writer log-file-name :append true)]
       (let [flow (<!! (nos/load-flow flow-id (:store fe)))]
         (binding [*out* log-file]
           (prn "Starting flow " flow-id)
           (as-> flow f
             (if op (assoc-in f [:results op] result) f)
             (nos/run-flow! fe f)
             (kv/assoc store flow-id f)
             (<!! f))))))))

(derive :nos/program-loader :duct/daemon)

(defmethod ig/init-key :nos/program-loader
  [_ {:keys [store path]}]
  (let [files (->> (file-seq (io/file path))
                   (filter #(.isFile %))
                   (filter #(-> % .toPath (str/ends-with? ".edn"))))]
    (doall
     (for [f files]
       (let [program-id (-> f .toPath .getFileName .toString
                            (FilenameUtils/removeExtension))
             f-edn (-> f slurp)]
         (<!! (kv/assoc-in store [:programs program-id] f-edn))
         program-id)))))

(defmethod ig/init-key :nos/flow-chan [_ _] (chan 2000))

(defmethod ig/init-key :nos/flow-engine
  [_ props]
  (let [{:keys [store] :as fe} props]
    (go-loop []
      (when-let [[event & data :as m] (<! (:chan fe))]
        (log :info "Received message " m)
        (case event
          :trigger (process-event! fe (first data))
          :deliver
          (if-let [[flow-id op] (<! (kv/get-in store [(first data) :deliver]))]
            (do (log :info "Deliver future" (first data))
                (process-event! fe flow-id [op (second data)]))
            (log :warn "Future not registered" (first data)))
          :fx
          (let [[flow-id op fxs] data
                flow (<! (nos/load-flow flow-id store))]
            (log :info "Process fx" fxs)
            (->> flow
                 (nos/handle-fx fe op fxs)
                 (kv/assoc store flow-id)
                 <!)))

        (recur)))
    fe))

(defmethod ig/halt-key! :nos/flow-engine
  [_ {:keys [store chan]}]
  (when chan
    (a/close! chan)))
