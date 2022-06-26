(ns nos.ops.docker
  (:require
   [nos.core :as nos]
   [failjure.core :as f]
   [clojure.string :as str]
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [contajners.core :as c]
   [taoensso.timbre :as log]
   [clj-compress.core :refer [create-archive]])
  (:import [java.io BufferedReader]
           [java.nio ByteBuffer CharBuffer]
           [java.nio.charset Charset StandardCharsets]
           [java.util Arrays]
           [org.apache.commons.compress.archivers.tar TarArchiveOutputStream]
           [org.apache.commons.io FilenameUtils]
           [org.apache.commons.compress.utils IOUtils]))

(def api-version "v4.0.0")

(defn- relativise-path [base path]
  (let [f (io/file base)
        uri (.toURI f)
        relative (.relativize uri (-> path io/file .toURI))]
    (.getPath relative)))

(defn create-tar [archive-name input-files]
  (let [out-stream (io/output-stream archive-name)
        tar-stream (TarArchiveOutputStream. out-stream)]
    ;; support filenames > 100 bytes
    (.setLongFileMode tar-stream TarArchiveOutputStream/LONGFILE_POSIX)
    (doseq [file-name input-files]
      (let [file (io/file file-name)]
        (doseq [f (if (.isDirectory file) (file-seq file) [file])]
          (when (.isFile f)
            (let [entry-name (relativise-path (FilenameUtils/getPath file-name) (-> f .getPath))
                  entry (.createArchiveEntry tar-stream f entry-name)]
              (.putArchiveEntry tar-stream entry)
              (when (.isFile f)
                (IOUtils/copy (io/input-stream f) tar-stream))
              (.closeArchiveEntry tar-stream))))))
    (.finish tar-stream)
    (.close tar-stream)
    archive-name))

;; Some functions below are from bob-cd
;;
;; Copyright 2018-2022 Rahul De
;; Source: https://github.com/bob-cd/bob/blob/main/runner/src/runner/engine.clj
(defn sh-tokenize
  "Tokenizes a shell command given as a string into the command and its args.

  Either returns a list of tokens or throws an IllegalStateException.
  Sample input: sh -c 'while sleep 1; do echo \\\"${RANDOM}\\\"; done'
  Output: [sh, -c, while sleep 1; do echo \"${RANDOM}\"; done]"
  [command]
  (let [[escaped? current-arg args state]
        (loop [cmd         command
               escaped?    false
               state       :no-token
               current-arg ""
               args        []]
          (if (or (nil? cmd) (zero? (count cmd)))
            [escaped? current-arg args state]
            (let [char ^Character (first cmd)]
              (if escaped?
                (recur (rest cmd) false state (str current-arg char) args)
                (case state
                  :single-quote       (if (= char \')
                                        (recur (rest cmd) escaped? :normal current-arg args)
                                        (recur (rest cmd) escaped? state (str current-arg char) args))
                  :double-quote       (case char
                                        \" (recur (rest cmd) escaped? :normal current-arg args)
                                        \\ (let [next (second cmd)]
                                             (if (or (= next \") (= next \\))
                                               (recur (drop 2 cmd) escaped? state (str current-arg next) args)
                                               (recur (drop 2 cmd) escaped? state (str current-arg char next) args)))
                                        (recur (rest cmd) escaped? state (str current-arg char) args))
                  (:no-token :normal) (case char
                                        \\ (recur (rest cmd) true :normal current-arg args)
                                        \' (recur (rest cmd) escaped? :single-quote current-arg args)
                                        \" (recur (rest cmd) escaped? :double-quote current-arg args)
                                        (if-not (Character/isWhitespace char)
                                          (recur (rest cmd) escaped? :normal (str current-arg char) args)
                                          (if (= state :normal)
                                            (recur (rest cmd) escaped? :no-token "" (conj args current-arg))
                                            (recur (rest cmd) escaped? state current-arg args))))
                  (throw (IllegalStateException. (format "Invalid shell command: %s, unexpected token %s found."
                                                         command
                                                         state))))))))]
    (if escaped?
      (conj args (str current-arg \\))
      (if (not= state :no-token)
        (conj args current-arg)
        args))))

(defn docker-pull [conn image]
  (let [client (c/client {:engine :podman
                          :category :libpod/images
                          :conn conn
                          :version api-version})]
    (c/invoke client {:op :ImagePullLibpod
                      :params {:reference image}
                      :throw-exceptions true})))

(defn commit-container
  "Creates a new image from a container
  Returns image identifier or a failure."
  [container-id conn]
  (f/try-all [client (c/client {:engine :podman
                                :category :libpod/commit
                                :conn conn
                                :version api-version})
              params {:container container-id}
              result (c/invoke client
                               {:op :ImageCommitLibpod
                                :params params
                                :throw-exceptions true})]
             (:Id result)
             (f/when-failed [err]
                            (log/errorf "Could not commit image: %s" (f/message err))
                            err)))

(defn stream-log
  "Stream logs of container with `id` through `reaction-fn`

  Each line of the docker output is fed to the `reaction-fn`. This function
  takes 2 arguments: the type of the log line and the string content.

  This also demuxes the docker headers of each line. The header of each log line
  has the following format:

  := [8]byte{STREAM_TYPE, 0, 0, 0, SIZE1, SIZE2, SIZE3, SIZE4}"
  [client id reaction-fn]
  (let [log-stream (c/invoke client {:op :ContainerLogsLibpod
                                     :params {:name id
                                              :follow true
                                              :stdout true
                                              :stderr true}
                                     :as :stream
                                     :throw-exceptions true})]
    (future
      (with-open [rdr (io/reader log-stream)]
        (loop [r (BufferedReader. rdr)]
          (let [log-type (.read r)]
            (when (> log-type -1)
              (.skip r 3)
              (let [buf (char-array 4)
                    _ (.read r buf 0 4)
                    bts (.getBytes (String. buf) (StandardCharsets/UTF_8))
                    byte-buf (ByteBuffer/wrap bts)
                    size (.getInt byte-buf)
                    line-buf (char-array size)
                    _ (.read r line-buf 0 size)
                    line (String. line-buf)]
                (reaction-fn line log-type))
              (recur r))))))))

(defn put-container-archive
  "Copies a tar input stream to a path in the container"
  [client id archive-input-stream path]
  (let [result (f/try*
                (with-open [xin archive-input-stream]
                  (c/invoke client
                            {:op :PutContainerArchiveLibpod
                             :params {:name id
                                      :path path}
                             :data xin
                             :throw-exceptions true})))]
    (when (f/failed? result)
      (log/errorf "Could not put archive in container: %s" result)
      result)))

(defn get-container-archive
  "Returns a tar stream of a path in the container by id."
  [client id path]
  (f/try-all [result (c/invoke client
                               {:op :ContainerArchiveLibpod
                                :params {:name id
                                         :path path}
                                :as :stream
                                :throw-exceptions true})]
             result
             (f/when-failed [err]
                            (log/errorf "Error fetching container archive: %s" err)
                            err)))

(defn create-tar-archive [arch-name path]
  (create-tar arch-name [path]))

(defn copy-resources-to-container! [client container-id resources]
  (doseq [{:keys [source dest]} resources]
    (let [temp-archive (create-tar "resource.tar" [source])]
      (put-container-archive client container-id (io/input-stream temp-archive) dest))))

(defn do-command!
  "Runs a command in a container from the `image` and returns result image

  `log-fn` is a function like #(prn \"CNT: \" %)"
  [client cmd image work-dir log-fn conn]
  (f/try-all [result (c/invoke client {:op :ContainerCreateLibpod
                                       :data {:image image
                                              :command (sh-tokenize cmd)
                                              :env {}
                                              :work_dir work-dir
                                              :cgroups_mode "disabled"}
                                       :throw-exceptions true})
              container-id (:Id result)

              _ (c/invoke client {:op :ContainerStartLibpod
                                  :params {:name container-id}
                                  :throw-exceptions true})

              _ (log/debugf "Attaching to container %s for logs" container-id)
              _ (stream-log client container-id log-fn)

              status (c/invoke client {:op :ContainerWaitLibpod
                                       :params {:name container-id}
                                       :throw-exceptions true})

              image (commit-container container-id conn)]
             (if (zero? status)
               image
               (let [msg (format "Container %s exited with non-zero status %d" container-id status)]
                 (log/debug msg)
                 (f/fail msg)))
             (f/when-failed [e]
                            (log/error "error running command"))))

(defn do-commands!
  "Runs an sequence of `commands` starting from `image`

  Returns a vector of the results of each command"
  [client commands image work-dir conn]
  (loop [cmds commands
         results [{:img image :cmd nil :time (nos/current-time) :log nil}]]
    (let [[cmd & rst] cmds
          img (-> results last :img)
          log-file (java.io.File/createTempFile "log" ".txt")]
      (if (nil? cmd)
        [:success results]
        (f/if-let-failed? [;; this log command will log lines as a nested json array
                           result-image-id
                           (with-open [w (io/writer log-file)]
                             (.write w "[")
                             (let [result-image-id
                                   (do-command! client cmd img work-dir
                                                #(.write w (str "[" %2 "," (json/encode %1) "],")) conn)]
                               (.write w "[1,\"\"]]")
                               result-image-id))]
                          (do
                            (log/error "ERROR " image work-dir result-image-id)
                            [:pipeline-failed  (concat results [{:error (f/message result-image-id)
                                                                 :time (nos/current-time)
                                                                 :cmd cmd
                                                                 :log (.getAbsolutePath log-file)}])])
                          (recur rst (concat results [{:img result-image-id
                                                       :time (nos/current-time)
                                                       :cmd cmd
                                                       :log (.getAbsolutePath log-file)}])))))))

;; resources = copied from local disk to container before run
;; artifacts = copied from container to local disk after run
(defmethod nos/run-op :docker/run [_ _ [{:keys [image cmd conn artifacts resources work-dir]
                                         :or {conn {:uri "http://localhost:8080"}
                                              work-dir "/root"
                                              resources []
                                              artifacts []}}]]
  (f/try-all [_ (docker-pull conn image)
              _ (log/debugf "Pulled image %s" image)
              client (c/client {:engine :podman
                                :category :libpod/containers
                                :conn conn
                                :version api-version})

              result (c/invoke client
                               {:op :ContainerCreateLibpod
                                :data {:image image}
                                :throw-exceptions true})

              container-id (:Id result)

              _ (when (not-empty resources)
                  (log/debugf "Copying resources to container %s" container-id)
                  (copy-resources-to-container! client container-id resources))
              image (commit-container container-id conn)

              last-image (do-commands! client cmd image work-dir conn)]
             last-image
             (f/when-failed [err]
                            (log/errorf ":docker/run failed" )
                            (prn err)
                            [:error (f/message err)])))

(comment
  (flow/run-op :docker/run nil
              [{:image "alpine" :cmd ["touch /root/test" "ls -l /root"] :conn {:uri "http://localhost:8080"}}]))
