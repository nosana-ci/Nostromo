(ns nos.core-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [nos.core :as nos]))

(deftest refs-test
  (testing "detects refs"
    (is (nos/reflike? [::nos/vault]))
    (is (nos/reflike? [::nos/ref :node]))
    (is (nos/reflike? [::nos/dep 1]))
    (is (nos/reflike? [::nos/str "adf" "asdf"]))))

(def base-flow
  {:name "Flow"
   :description "Flow description"
   :results {:input "INPUT"}
   :ops
   [{:op :sci/eval
     :id :op-1
     :args [ (nos/ref :input) "(println \"input = \" input)"]
     :deps []}]})

(deftest flow-spec
  (is (s/valid? ::nos/flow base-flow))
  (is (not (s/valid? ::nos/flow {}))))

(deftest run-op
  (is (= 12 (nos/run-op :get base-flow [{:test 12} :test]))))

(deftest ref-val
  (is (= (nos/ref-val [{:map-key [:nos.core/ref :some-key]}
                       [:nos.core/ref :missing]]
                      {:some-key 99} {})
         '({:map-key 99} nil))))
