(ns nos.ops.sci
  (:require [sci.core :as sci]
            [nos.core :as nos]
            [clojure.string :as string]))

(def default-bindings {'uuid nos/uuid})

(def default-namespaces
  {})

(def default-classes {})

(defmethod nos/run-op :sci/eval [_ _ [& args]]
  (let [input (drop-last args)
        code (last args)]
    (let [sci-input (sci/new-var 'input input)]
      (sci/binding [sci/out *out*]
        (sci/eval-string code {:bindings (merge {'input input} default-bindings)
                               :namespaces default-namespaces
                               :classes default-classes})))))
