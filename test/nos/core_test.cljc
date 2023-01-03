(ns nos.core-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [nos.core :as nos]
            [konserve.memory :refer [new-mem-store]]
            [clojure.core.async :as a :refer [>! <! >!! <!! go go-loop chan
                                              put!]]))

(defn- test-flow-engine []
  {:store (<!! (new-mem-store))
   :chan  (chan)
   :vault {:secret 42}})

(deftest refs-test
  (testing "detects refs"
    (is (nos/reflike? [:nos/vault]))
    (is (nos/reflike? [:nos/ref :node]))
    (is (nos/reflike? [:nos/dep 1]))
    (is (nos/reflike? [:nos/str "adf" "asdf"]))))

(def base-flow
  {:name        "Flow"
   :description "Flow description"
   :state       {:input "INPUT"}
   :ops
   [#_{:op   :sci/eval
       :id   :op-1
       :args [ (nos/ref :input) "(println \"input = \" input)"]
       :deps []}
    {:op   :nos/prn
     :id   :op-1
     :args ["hello"]
     :deps []}]})

(defn flow-with-ops [ops]
  (assoc base-flow :ops ops))

(deftest flow-spec
  (is (s/valid? ::nos/flow base-flow))
  (is (not (s/valid? ::nos/flow {}))))

(deftest run-op
  (is (= 12 (nos/run-op {:op :nos/get}
                        base-flow
                        [{:test 12} :test]))))

(deftest ref-val
  (testing "can reference values nested inside a map in a vector"
    (is (= (nos/ref-val [{:map-key [:nos/ref :some-key]}
                         [:nos/ref :missing]]
                        {:some-key 99} {})
           '({:map-key 99} nil)))))

(deftest run-flow
  (let [fe (test-flow-engine)]
    (testing "can run simple flow"
      (let [res (nos/run-flow! fe base-flow)]
        (is (= (get-in res [:state :op-1 0]) "hello"))))))

(deftest flow-op
  (testing "can run sub flows"
    (let [fe   (test-flow-engine)
          flow (flow-with-ops [{:op   :nos/prn
                                :id   :op-1
                                :args ["hello"]
                                :deps []}
                               {:op   :nos/flow
                                :id   :flow
                                :args [{:ops [{:op   :nos/prn
                                               :id   :flow-1-op-1
                                               :args ["sub-hello"]}]}]}])
          res  (nos/run-flow! fe flow)]
      (is (= (get-in res [:state :op-1 0]) "hello"))
      (is (= (get-in res [:state :flow]) [::nos/new-ops 1]))
      (is (= (get-in res [:state :flow-1-op-1 0]) "sub-hello")))))
