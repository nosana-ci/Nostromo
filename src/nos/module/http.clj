(ns nos.module.http
  (:require [integrant.core :as ig]
            [ataraxy.response :as response]
            [clojure.core.async :as async :refer [<!! go <! >!]]
            [konserve.core :as kv]
            [nos.core :as flow]
            [aleph.http :as http]
            [ring.middleware.multipart-params :refer [wrap-multipart-params]]
            [clojure.java.io :as io]
            [ring.middleware.cors :refer [wrap-cors]]))

(derive :nos.module/http :duct/module)
(derive :nos.trigger/http :duct/daemon)
(derive :nos.trigger/http :nos/trigger)

(defn- add-http-callback [store callback-id program-id]
  (prn "... adding http callback " callback-id program-id)
  (<!! (kv/assoc-in store [callback-id] program-id)))


(def default-post-options {:content-type :json
                           :as :json})


(defmethod ig/init-key :nos.http.middleware/wrap-multipart-params
  [_ options]
  #(wrap-multipart-params %))

(defmethod ig/init-key :nos.http.middleware/wrap-cors
  [_ options]
  #(wrap-cors %
              :access-control-allow-origin [#".*"]
              :access-control-allow-methods [:get :put :post :delete]))

(defmethod ig/init-key :nos.handler/http-callback
  [_ {{:keys [store chan] :as fe} :flow}]
  (fn [{[_ path body-data multipart-data] :ataraxy/result
       method :request-method :as m}]
    (let [data (if (and (nil? body-data)                ;  quick patch: seraliaze path to support file-uploads
                        (some? multipart-data))
                 (update-in multipart-data ["file" :tempfile] #(str (.toPath %)))
                 body-data)
          trigger-id (keyword (str "http." (name method)) path)
          [type res-id] (<!! (kv/get-in store [trigger-id]))]
      (when res-id
        (case type
          :nos.http/resource [::response/ok (io/resource res-id)]
          :nos.flow/program
          (do (go
                (try
                  (let [flow-id (-> res-id
                                    (flow/load-program store)
                                    (flow/set-input data)
                                    (flow/save-flow store)
                                    <! second :id)]
                    (>! chan [:trigger flow-id]))
                  (catch Exception e (prn "!! http error " e))))
              [::response/ok "OK"]))))))

(defmethod ig/init-key :nos.trigger/http
  [[_ id] {:keys [program-id flow]}]
  (add-http-callback (:store flow) id [::flow/program program-id])
  id)

(defn- make-trigger [{:keys [type schema path program]} flow-engine]
  {[:nos.trigger/http (keyword (str "http." (name type) path))]
   {:program-id program :flow flow-engine}})

(defmethod ig/init-key :nos.module/http
  [_ {:keys [prefix paths] :or {paths []}}]
  (fn [config]
    (let [[router-key router] (ig/find-derived-1 config :duct/router)
          flow-engine (-> config
                          (ig/find-derived-1 :nos/flow-engine)
                          first
                          ig/ref)
          new-router
          (-> router
              (assoc-in [:routes prefix]
                        {[:post "/" 'path {'?body :body-params
                                           '?multipart :multipart-params}]
                         [::callback 'path '?body '?multipart]
                         [:get "/" 'path]
                         [::callback-get 'path]})
              (assoc-in [:handlers ::callback]
                        (ig/ref :nos.handler/http-callback))
              (assoc-in [:handlers ::callback-get]
                        (ig/ref :nos.handler/http-callback)))]
      (merge
       config
       {:nos.handler/http-callback {:flow flow-engine}}
       {router-key new-router}
       (->> paths
            (map #(make-trigger % flow-engine))
            (reduce merge))))))
