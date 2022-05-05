(ns nos.ops.git
  (:require
   [taoensso.timbre :refer [log]]
   [nos.core :as flow]
   [clj-jgit.porcelain :as jgit])
  (:import [java.io FileNotFoundException]))

(defmethod flow/run-op :nos.git/clone
  [_ _ [url dest]]
  (log :info "Start cloning repo " url " to " dest)
  (let [repo (jgit/git-clone url :dir dest)]
    dest))

;; returns the path
(defmethod flow/run-op :nos.git/ensure-repo
  [_ _ [url path]]
  (try
    (let [repo (jgit/load-repo path)]
      (log :info "Pulling recent changes into " path)
      (jgit/git-fetch repo)
      path)
    (catch FileNotFoundException e
      (log :info "Repo not found, cloning " url " into " path)
      (let [repo (jgit/git-clone url :dir path)]
        path)))
  path)

(defmethod flow/run-op :nos.git/checkout
  [_ _ [repo-path rev]]
  (let [repo (jgit/load-repo repo-path)]
    (try
      (jgit/git-checkout repo :name rev)
      repo-path
      (catch org.eclipse.jgit.api.errors.RefNotFoundException e
        (log :error "Git ref " rev " does not exist" )
        [::flow/error (str "Git ref " rev " does not exist")])
      (catch Exception e
        [::flow/error "Git clone error" (ex-message e)]))))

(defmethod flow/run-op :nos.git/status
  [_ _ [repo-path]]
  (log :info "Checking status of " repo-path)
  (let [repo (jgit/load-repo repo-path)
        status (jgit/git-status repo)]
    status))

(defmethod flow/run-op :nos.git/get-last-commit
  [_ _ [repo-path]]
  (log :info "Opening repo at " repo-path)
  (let [repo (jgit/load-repo repo-path)
        rev (:id (first (jgit/git-log repo :max-count 1)))
        ident (.getAuthorIdent rev)]
    {:short-msg (.getShortMessage rev)
     :msg (.getFullMessage rev)
     :author {:email (.getEmailAddress ident)
              :name (.getName ident)}
     :time (.getCommitTime rev)}))
