;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.daemon.supervisor
  (:import [java.io OutputStreamWriter BufferedWriter IOException])
  (:import [backtype.storm.scheduler ISupervisor]
           [backtype.storm.utils LocalState Time Utils]
           [backtype.storm.daemon Shutdownable]
           [backtype.storm.daemon.common SupervisorInfo]
           [backtype.storm Constants]
           [java.net JarURLConnection]
           [java.net URI]
           [org.apache.commons.io FileUtils]
           [java.io File])
  (:use [backtype.storm config util log timer])
  (:use [backtype.storm.daemon common])
  (:require [backtype.storm.daemon [worker :as worker]]
            [backtype.storm [process-simulator :as psim] [cluster :as cluster] [event :as event]]
            [clojure.set :as set])
  (:import [org.apache.zookeeper data.ACL ZooDefs$Ids ZooDefs$Perms])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.ISupervisor] void]]))

(defmulti download-storm-code cluster-mode)
(defmulti launch-worker (fn [supervisor & _] (cluster-mode (:conf supervisor))))

;; used as part of a map from port to this
(defrecord LocalAssignment [storm-id executors])

(defprotocol SupervisorDaemon
  (get-id [this])
  (get-conf [this])
  (shutdown-all-workers [this])
  )

(defn- assignments-snapshot [storm-cluster-state callback assignment-versions]
  (let [storm-ids (.assignments storm-cluster-state callback)] ;; 读取zk中所有有调度的topology id
    (let [new-assignments
          (->>
           (dofor [sid storm-ids]
                  (let [recorded-version (:version (get assignment-versions sid))] ;; 取出相应的缓存中的调度信息
                    (if-let [assignment-version (.assignment-version storm-cluster-state sid callback)] ;;取出目前在zk中分配信息的版本号
                      (if (= assignment-version recorded-version)
                        {sid (get assignment-versions sid)} ;; 保持不变
                        {sid (.assignment-info-with-version storm-cluster-state sid callback)}) ;; 否则从zk中读取最新的分配和相应的版本号
                      {sid nil})))
           (apply merge)
           (filter-val not-nil?))]

      ;; 返回最新的分配和版本信息，为啥assignment和version中大部分都重复了？
      {:assignments (into {} (for [[k v] new-assignments] [k (:data v)]))
       :versions new-assignments})))

(defn- read-my-executors [assignments-snapshot storm-id assignment-id]
  (let [assignment (get assignments-snapshot storm-id) ;; 读取给定topology的分配信息
        my-executors (filter (fn [[_ [node _]]] (= node assignment-id)) ;; 过滤掉未分配到本机的分配信息
                           (:executor->node+port assignment))
        port-executors (apply merge-with ;; 获得{port [executors]}，本机的端口分配信息
                          concat
                          (for [[executor [_ port]] my-executors]
                            {port [executor]}
                            ))]
    ;; 得到{port LocalAssignment}
    (into {} (for [[port executors] port-executors]
               ;; need to cast to int b/c it might be a long (due to how yaml parses things)
               ;; doall is to avoid serialization/deserialization problems with lazy seqs
               [(Integer. port) (LocalAssignment. storm-id (doall executors))]
               ))))

(defn- read-assignments
  "Returns map from port to struct containing :storm-id and :executors"
  ([assignments-snapshot assignment-id]
    ;;对所有的 topology，读取本机的分配信息，合并得到{port LocalAssignment}，在出现一个端口被多个topology使用时抛出运行时异常
     (->> (dofor [sid (keys assignments-snapshot)] (read-my-executors assignments-snapshot sid assignment-id))
          (apply merge-with (fn [& ignored] (throw-runtime "Should not have multiple topologies assigned to one port")))))
  ([assignments-snapshot assignment-id existing-assignment retries]
    ;; 调用两参数版本，重置重试次数为0
     (try (let [assignments (read-assignments assignments-snapshot assignment-id)]
            (reset! retries 0)
            assignments)
          (catch RuntimeException e ;; 如果读取异常，增加重试次数，返回当前已有的分配信息
            (if (> @retries 2) (throw e) (swap! retries inc))
            (log-warn (.getMessage e) ": retrying " @retries " of 3")
            existing-assignment))))

(defn- read-storm-code-locations
  [assignments-snapshot]
  (map-val :master-code-dir assignments-snapshot))

(defn- read-downloaded-storm-ids [conf]
  (map #(url-decode %) (read-dir-contents (supervisor-stormdist-root conf)))
  )

;; 将本地的心跳文件转化为LocalState对象，读取心跳信息
(defn read-worker-heartbeat [conf id]
  (let [local-state (worker-state conf id)]
    (.get local-state LS-WORKER-HEARTBEAT)
    ))


(defn my-worker-ids [conf]
  (read-dir-contents (worker-root conf)))

;; 从本地文件，读取worker的心跳信息{workerid heartbeat}
(defn read-worker-heartbeats
  "Returns map from worker id to heartbeat"
  [conf]
  (let [ids (my-worker-ids conf)] ;; 读取本地文件系统worker下的各workerid文件名
    (into {}
      (dofor [id ids]
        [id (read-worker-heartbeat conf id)]))
    ))

;; 是否是当前的一个分配的worker
(defn matches-an-assignment? [worker-heartbeat assigned-executors]
  (let [local-assignment (assigned-executors (:port worker-heartbeat))]
    (and local-assignment ;; 存在该分配
         (= (:storm-id worker-heartbeat) (:storm-id local-assignment)) ;; 是该topology的分配
         (= (disj (set (:executors worker-heartbeat)) Constants/SYSTEM_EXECUTOR_ID) ;; executors集合相同，去除系统executorid
            (set (:executors local-assignment))))))

(let [dead-workers (atom #{})]
  (defn get-dead-workers []
    @dead-workers)
  (defn add-dead-worker [worker]
    (swap! dead-workers conj worker))
  (defn remove-dead-worker [worker]
    (swap! dead-workers disj worker)))

;; 判断worker是否心跳超时，超时时间由conf中SUPERVISOR-WORKER-TIMEOUT-SECS项配置
(defn is-worker-hb-timed-out? [now hb conf]
  (> (- now (:time-secs hb))
     (conf SUPERVISOR-WORKER-TIMEOUT-SECS)))

;; 读取本地的worker心跳，判断worker状态，返回{workerid [state hb]}
(defn read-allocated-workers
  "Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead (timed out or never wrote heartbeat)"
  [supervisor assigned-executors now]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        id->heartbeat (read-worker-heartbeats conf) ;; 读取本地文件系统中保存的{workerid heartbeat}信息
        approved-ids (set (keys (.get local-state LS-APPROVED-WORKERS)))] ;; 读取所有允许的worker，这个是在哪设置的？
    (into
     {}
     (dofor [[id hb] id->heartbeat]
            (let [state (cond
                         (not hb) ;; 存在本地workerid目录，但是没有心跳，worker未启动
                           :not-started
                         (or (not (contains? approved-ids id)) ;; 不属于允许的workerid
                             (not (matches-an-assignment? hb assigned-executors))) ;; 或者不是当前的所分配的worker
                           :disallowed
                         (or
                          (when (get (get-dead-workers) id) ;; 如果已经加入了死亡worker列表
                            (log-message "Worker Process " id " has died!")
                            true)
                          (is-worker-hb-timed-out? now hb conf)) ;; 或者heartbeat超时
                           :timed-out
                         true
                           :valid)] ;; 否则是合法的
              (log-debug "Worker " id " is " state ": " (pr-str hb) " at supervisor time-secs " now)
              [id [state hb]]
              ))
     )))

;; 等待worker启动，写入heartbeat
(defn- wait-for-worker-launch [conf id start-time]
  (let [state (worker-state conf id)] ;; 構造worker heartbeat localstate
    (loop []
      (let [hb (.get state LS-WORKER-HEARTBEAT)]
        (when (and
               (not hb)
               (<
                (- (current-time-secs) start-time)
                (conf SUPERVISOR-WORKER-START-TIMEOUT-SECS)
                ))
          (log-message id " still hasn't started")
          (Time/sleep 500)
          (recur)
          )))
    (when-not (.get state LS-WORKER-HEARTBEAT)
      (log-message "Worker " id " failed to start")
      )))

(defn- wait-for-workers-launch [conf ids]
  (let [start-time (current-time-secs)]
    (doseq [id ids] ;; 为每个id中的worker，等待worker启动，写入heartbeat
      (wait-for-worker-launch conf id start-time))
    ))

(defn generate-supervisor-id []
  (uuid))

(defnk worker-launcher [conf user args :environment {} :log-prefix nil :exit-code-callback nil]
  (let [_ (when (clojure.string/blank? user)
            (throw (java.lang.IllegalArgumentException.
                     "User cannot be blank when calling worker-launcher.")))
        wl-initial (conf SUPERVISOR-WORKER-LAUNCHER)
        storm-home (System/getProperty "storm.home")
        wl (if wl-initial wl-initial (str storm-home "/bin/worker-launcher"))
        command (concat [wl user] args)]
    (log-message "Running as user:" user " command:" (pr-str command))
    (launch-process command :environment environment :log-prefix log-prefix :exit-code-callback exit-code-callback)
  ))

(defnk worker-launcher-and-wait [conf user args :environment {} :log-prefix nil]
  (let [process (worker-launcher conf user args :environment environment)]
    (if log-prefix
      (read-and-log-stream log-prefix (.getInputStream process)))
      (try
        (.waitFor process)
      (catch InterruptedException e
        (log-message log-prefix " interrupted.")))
      (.exitValue process)))

(defn- rmr-as-user
  "Launches a process owned by the given user that deletes the given path
  recursively.  Throws RuntimeException if the directory is not removed."
  [conf id user path]
  (worker-launcher-and-wait conf
                            user
                            ["rmr" path]
                            :log-prefix (str "rmr " id))
  (if (exists-file? path)
    (throw (RuntimeException. (str path " was not deleted")))))

;; 清理worker其他内容
(defn try-cleanup-worker [conf id user]
  (try
    (if (.exists (File. (worker-root conf id)))
      (do
        (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
          (rmr-as-user conf id user (worker-root conf id))
          (do
            (rmr (worker-heartbeats-root conf id)) ;; 删除heartbeat路径
            ;; this avoids a race condition with worker or subprocess writing pid around same time
            (rmpath (worker-pids-root conf id)) ;; 删除pid目录
            (rmpath (worker-root conf id)))) ;; 删除worker目录
        (remove-worker-user! conf id) ;; 删除worker-user文件
        (remove-dead-worker id) ;; 从dead-worker中移除该worker
      ))
  (catch IOException e
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later"))
  (catch RuntimeException e
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later")
    )
  (catch java.io.FileNotFoundException e (log-message (.getMessage e)))
    ))

;; 关闭worker
(defn shutdown-worker [supervisor id]
  (log-message "Shutting down " (:supervisor-id supervisor) ":" id)
  (let [conf (:conf supervisor)
        pids (read-dir-contents (worker-pids-root conf id)) ;; 读取该worker的pid信息
        thread-pid (@(:worker-thread-pids-atom supervisor) id) ;; supervisordata中缓存的worker->threadpid信息
        as-user (conf SUPERVISOR-RUN-WORKER-AS-USER) ;; SUPERVISOR-RUN-WORKER-AS-USER选项是否被选上
        user (get-worker-user conf id)] ;;　读取的worker-user文件内容
    (when thread-pid ;; 当thread-pid有内容时
      (psim/kill-process thread-pid))
    (doseq [pid pids]
      (if as-user
        (worker-launcher-and-wait conf user ["signal" pid "9"] :log-prefix (str "kill -15 " pid)) ;; 如果SUPERVISOR-RUN-WORKER-AS-USER被选上
        (kill-process-with-sig-term pid))) ;; 使用sigterm终止进程，kill -15
    ;; 等待1秒钟，完成cleanup过程
    (if-not (empty? pids) (sleep-secs 1)) ;; allow 1 second for execution of cleanup threads on worker.
    (doseq [pid pids]
      (if as-user
        (worker-launcher-and-wait conf user ["signal" pid "9"] :log-prefix (str "kill -9 " pid))
        (force-kill-process pid)) ;; 使用kill -9强制杀死
      (if as-user
        (rmr-as-user conf id user (worker-pid-path conf id pid))
        (try
          (rmpath (worker-pid-path conf id pid)) ;; 强制删除属于该进程的pid文件
          (catch Exception e)))) ;; on windows, the supervisor may still holds the lock on the worker directory
    (try-cleanup-worker conf id user))
  (log-message "Shut down " (:supervisor-id supervisor) ":" id))

(def SUPERVISOR-ZK-ACLS
  [(first ZooDefs$Ids/CREATOR_ALL_ACL)
   (ACL. (bit-or ZooDefs$Perms/READ ZooDefs$Perms/CREATE) ZooDefs$Ids/ANYONE_ID_UNSAFE)])

(defn supervisor-data [conf shared-context ^ISupervisor isupervisor]
  {:conf conf ;; conf配置
   :shared-context shared-context
   :isupervisor isupervisor ;; isupervisor对象
   :active (atom true) ;; 是否激活的标志位，初始为true
   :uptime (uptime-computer) ;; 返回一个计算启动时间的函数
   :worker-thread-pids-atom (atom {}) ;; worker线程的pid映射
   ;; 获取zk封装对象，用于读取zookeeper中数据与nimbus交互
   :storm-cluster-state (cluster/mk-storm-cluster-state conf :acls (when
                                                                     (Utils/isZkAuthenticationConfiguredStormServer
                                                                       conf)
                                                                     SUPERVISOR-ZK-ACLS))
   :local-state (supervisor-state conf) ;; 根据配置给定的路径生成的superviosr local-state对象，存储在本地文件系统，带版本的map
   :supervisor-id (.getSupervisorId isupervisor) ;; 获取supervisor id
   :assignment-id (.getAssignmentId isupervisor) ;; 获取assignement id实际上同supervisor id
   :my-hostname (hostname conf) ;; 读取配置文件中配置的本地主机名，如果未配置，通过InetAddress.getLocalHost.getCanaonicalHostName获取主机名
   :curr-assignment (atom nil) ;; used for reporting used ports when heartbeating
   :heartbeat-timer (mk-timer :kill-fn (fn [t] ;; 构造心跳计时器
                               (log-error t "Error when processing event")
                               (exit-process! 20 "Error when processing an event")
                               ))
   :event-timer (mk-timer :kill-fn (fn [t] ;; 构造事件计时器
                                         (log-error t "Error when processing event")
                                         (exit-process! 20 "Error when processing an event")
                                         ))
   :assignment-versions (atom {})
   :sync-retry (atom 0) ;; 同步重试次数
   :download-lock (Object.) ;; 下载锁
   })

;; supervisor分配的同步函数，停止死掉的worker，启动新的worker替代
(defn sync-processes [supervisor]
  (let [conf (:conf supervisor)
        download-lock (:download-lock supervisor)
        ^LocalState local-state (:local-state supervisor)
        storm-cluster-state (:storm-cluster-state supervisor)
        assigned-executors (defaulted (.get local-state LS-LOCAL-ASSIGNMENTS) {}) ;; 获取目前LocalState中保存的分配信息
        now (current-time-secs)
        allocated (read-allocated-workers supervisor assigned-executors now) ;; 读取本地的worker心跳，判断worker状态，返回{workerid [state hb]}
        keepers (filter-val ;; 选出状态合法的worker
                 (fn [[state _]] (= state :valid))
                 allocated)
        keep-ports (set (for [[id [_ hb]] keepers] (:port hb))) ;; 状态合法worker的端口
        reassign-executors (select-keys-pred (complement keep-ports) assigned-executors) ;; 找出需要重新分配的端口
        new-worker-ids (into ;; 生成新workerid {port workerid}
                        {}
                        (for [port (keys reassign-executors)]
                          [port (uuid)]))
        ]
    ;; 1. to kill are those in allocated that are dead or disallowed
    ;; 2. kill the ones that should be dead
    ;;     - read pids, kill -9 and individually remove file
    ;;     - rmr heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log)
    ;; 3. of the rest, figure out what assignments aren't yet satisfied
    ;; 4. generate new worker ids, write new "approved workers" to LS
    ;; 5. create local dir for worker id
    ;; 5. launch new workers (give worker-id, port, and supervisor-id)
    ;; 6. wait for workers launch

    (log-debug "Syncing processes")
    (log-debug "Assigned executors: " assigned-executors)
    (log-debug "Allocated: " allocated)
    (doseq [[id [state heartbeat]] allocated]
      (when (not= :valid state) ;; 关闭状态不为valid的worker
        (log-message
         "Shutting down and clearing state for id " id
         ". Current supervisor time: " now
         ". State: " state
         ", Heartbeat: " (pr-str heartbeat))
        (shutdown-worker supervisor id)
        ))
    (doseq [id (vals new-worker-ids)]
      (local-mkdirs (worker-pids-root conf id)) ;; 为新分配的workerid创建pid根目录
      (local-mkdirs (worker-heartbeats-root conf id))) ;; 为新分配的workerid构建新heartbeat目录
    (.put local-state LS-APPROVED-WORKERS ;; 向本地supervisor local-state中写入LS-APPROVED-WORKERS
          (merge
           (select-keys (.get local-state LS-APPROVED-WORKERS)
                        (keys keepers))
           (zipmap (vals new-worker-ids) (keys new-worker-ids))
           ))

    ;; check storm topology code dir exists before launching workers
    (doseq [[port assignment] reassign-executors]
      (let [downloaded-storm-ids (set (read-downloaded-storm-ids conf)) ;; 读取已经下载代码的topology
            storm-id (:storm-id assignment)
            cached-assignment-info @(:assignment-versions supervisor)
            assignment-info (if (and (not-nil? cached-assignment-info) (contains? cached-assignment-info storm-id ))
                              (get cached-assignment-info storm-id) ;; 如果缓存中存在，获取缓存中该topology的分配信息
                              (.assignment-info-with-version storm-cluster-state storm-id nil)) ;; 如果不存在，从zk中读取
	          storm-code-map (read-storm-code-locations assignment-info) ;; 獲取topology代碼未知信息code-map
            master-code-dir (if (contains? storm-code-map :data) (storm-code-map :data)) ;; codemap中有:data字段，獲取該字段值
            stormroot (supervisor-stormdist-root conf storm-id)] ;; 本地的路徑
        (if-not (or (contains? downloaded-storm-ids storm-id) (.exists (File. stormroot)) (nil? master-code-dir))
          (download-storm-code conf storm-id master-code-dir download-lock)) ;; 如果需要下載，則下載代碼
        ))

    (wait-for-workers-launch ;; 等待woker启动
     conf
     (dofor [[port assignment] reassign-executors]
            (let [id (new-worker-ids port)] ;; 取出新的workerid
              (try
                (log-message "Launching worker with assignment "
                             (pr-str assignment)
                             " for this supervisor "
                             (:supervisor-id supervisor)
                             " on port "
                             port
                             " with id "
                             id
                             )
                (launch-worker supervisor ;; 启动worker
                               (:storm-id assignment)
                               port
                               id)
                (catch java.io.FileNotFoundException e
                  (log-message "Unable to launch worker due to "
                               (.getMessage e)))
                (catch java.io.IOException e
                  (log-message "Unable to launch worker due to "
                               (.getMessage e))))
         id)))
    ))

(defn assigned-storm-ids-from-port-assignments [assignment]
  (->> assignment
       vals
       (map :storm-id)
       set))

(defn shutdown-disallowed-workers [supervisor]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        assigned-executors (defaulted (.get local-state LS-LOCAL-ASSIGNMENTS) {})
        now (current-time-secs)
        allocated (read-allocated-workers supervisor assigned-executors now)
        disallowed (keys (filter-val
                                  (fn [[state _]] (= state :disallowed))
                                  allocated))]
    (log-debug "Allocated workers " allocated)
    (log-debug "Disallowed workers " disallowed)
    (doseq [id disallowed]
      (shutdown-worker supervisor id))
    ))

;; 返回一个函数，该函数用来比较localstate中保存的当前分配和zk中的最新分配有什么不同，更新分配信息，
;; 下载分配到本机的topology代码，配置，jar包，停止未继续使用的worker，删除不需要的topology代码文件
;; 将sync-processes函数加入到processes-event-manager队列中
(defn mk-synchronize-supervisor [supervisor sync-processes event-manager processes-event-manager]
  (fn this []
    (let [conf (:conf supervisor)
          download-lock (:download-lock supervisor)
          storm-cluster-state (:storm-cluster-state supervisor)
          ^ISupervisor isupervisor (:isupervisor supervisor)
          ^LocalState local-state (:local-state supervisor)
          sync-callback (fn [& ignored] (.add event-manager this)) ;; 构造event-manager中加入本函数的回调函数
          assignment-versions @(:assignment-versions supervisor) ;; assignment versions是做什么的，初始为(atom {})
          {assignments-snapshot :assignments versions :versions}  (assignments-snapshot ;; 获得当前assignment的snapshot
                                                                   storm-cluster-state sync-callback ;; 并注册zk wacher回调函数
                                                                  assignment-versions)
          storm-code-map (read-storm-code-locations assignments-snapshot) ;; 提取出storm代码在nimbus上的路径
          downloaded-storm-ids (set (read-downloaded-storm-ids conf)) ;; 从本地路径查看哪些topology的代码已经下载
          existing-assignment (.get local-state LS-LOCAL-ASSIGNMENTS) ;; 从本地路径查看已经保存在本地localstate的分配信息
          all-assignment (read-assignments assignments-snapshot ;; 读取zk中本机的分配信息，如果读取失败，使用之前本地保存的{port LocalAssignment}
                                           (:assignment-id supervisor)
                                           existing-assignment
                                           (:sync-retry supervisor))
          new-assignment (->> all-assignment
                              (filter-key #(.confirmAssigned isupervisor %))) ;; 目前的standalone supervisor实现中该函数始终返回true
          assigned-storm-ids (assigned-storm-ids-from-port-assignments new-assignment) ;; 获取分配到本机的topology id集合
          ]
      (log-debug "Synchronizing supervisor")
      (log-debug "Storm code map: " storm-code-map)
      (log-debug "Downloaded storm ids: " downloaded-storm-ids)
      (log-debug "All assignment: " all-assignment)
      (log-debug "New assignment: " new-assignment)

      ;; 下载分配到本机topology的代码，配置文件和jar包
      ;; download code first
      ;; This might take awhile
      ;;   - should this be done separately from usual monitoring?
      ;; should we only download when topology is assigned to this supervisor?
      (doseq [[storm-id master-code-dir] storm-code-map]
        (when (and (not (downloaded-storm-ids storm-id))
                   (assigned-storm-ids storm-id))
          (download-storm-code conf storm-id master-code-dir download-lock)))

      (log-debug "Writing new assignment "
                 (pr-str new-assignment))
      ;; 比较新的分配和旧的分配有什么不同，本处比较分配的端口
      (doseq [p (set/difference (set (keys existing-assignment))
                                (set (keys new-assignment)))]
        (.killedWorker isupervisor (int p)));; 杀死目前分配了，而新分配中没有的端口的worker，目前的standalone-supervisor实现没做任何操作
      (.assigned isupervisor (keys new-assignment)) ;; 目前standalone-superviosr未做任何操作
      (.put local-state ;; 将新的分配写入本地local-state中
            LS-LOCAL-ASSIGNMENTS
            new-assignment)
      (reset! (:assignment-versions supervisor) versions) ;; 更新缓存的分配版本
      (reset! (:curr-assignment supervisor) new-assignment) ;; 更新缓存的分配信息
      ;; remove any downloaded code that's no longer assigned or active
      ;; important that this happens after setting the local assignment so that
      ;; synchronize-supervisor doesn't try to launch workers for which the
      ;; resources don't exist
      ;; 在更新local state后清除不需要的storm代码，在更新后清除很重要，以免supervisor试图启动的topo代码不存在
      (if on-windows? (shutdown-disallowed-workers supervisor)) ;; 如果是在windows上，停止不允许的worker
      (doseq [storm-id downloaded-storm-ids]
        (when-not (assigned-storm-ids storm-id)
          (log-message "Removing code for storm id "
                       storm-id)
          (try
            (rmr (supervisor-stormdist-root conf storm-id))
            (catch Exception e (log-message (.getMessage e))))
          ))
      ;; 将sync-processes函数加入到processes-event-manager队列中
      (.add processes-event-manager sync-processes)
      )))

;; in local state, supervisor stores who its current assignments are
;; another thread launches events to restart any dead processes if necessary
(defserverfn mk-supervisor [conf shared-context ^ISupervisor isupervisor]
  (log-message "Starting Supervisor with conf " conf)
  (.prepare isupervisor conf (supervisor-isupervisor-dir conf)) ;; 以conf和isupervisor路径调用standalone-supervisor的prepare方法
  (FileUtils/cleanDirectory (File. (supervisor-tmp-dir conf))) ;; 清理supervisor临时目录
  (let [supervisor (supervisor-data conf shared-context isupervisor) ;; 构造supervisor-data数据
        [event-manager processes-event-manager :as managers] [(event/event-manager false) (event/event-manager false)] ;; 构造eventmanager
        sync-processes (partial sync-processes supervisor) ;; 用supervisor-data绑定sync-processes函数
        synchronize-supervisor (mk-synchronize-supervisor supervisor sync-processes event-manager processes-event-manager);; 同步分配情况的函数
        heartbeat-fn (fn [] (.supervisor-heartbeat! ;; 心跳函数为写入SupervisorInfo到临时节点
                               (:storm-cluster-state supervisor)
                               (:supervisor-id supervisor)
                               (SupervisorInfo. (current-time-secs)
                                                (:my-hostname supervisor)
                                                (:assignment-id supervisor)
                                                (keys @(:curr-assignment supervisor))
                                                ;; used ports
                                                (.getMetadata isupervisor)
                                                (conf SUPERVISOR-SCHEDULER-META)
                                                ((:uptime supervisor)))))]
    (heartbeat-fn);;立即调用心跳函数
    ;; should synchronize supervisor so it doesn't launch anything after being down (optimization)
    (schedule-recurring (:heartbeat-timer supervisor)
                        0
                        (conf SUPERVISOR-HEARTBEAT-FREQUENCY-SECS)
                        heartbeat-fn)
    (when (conf SUPERVISOR-ENABLE) ;; 默认true
      ;; This isn't strictly necessary, but it doesn't hurt and ensures that the machine stays up
      ;; to date even if callbacks don't all work exactly right
      (schedule-recurring (:event-timer supervisor) 0 10 (fn [] (.add event-manager synchronize-supervisor))) ;; 每10秒进行一次zk分配同步
      (schedule-recurring (:event-timer supervisor) ;; 每supervisor.monitor.frequency.secs，默认3秒查看本地的worker心跳
                          0
                          (conf SUPERVISOR-MONITOR-FREQUENCY-SECS)
                          (fn [] (.add processes-event-manager sync-processes))))
    (log-message "Starting supervisor with id " (:supervisor-id supervisor) " at host " (:my-hostname supervisor))
    (reify
     Shutdownable
     (shutdown [this]
               (log-message "Shutting down supervisor " (:supervisor-id supervisor))
               (reset! (:active supervisor) false)
               (cancel-timer (:heartbeat-timer supervisor))
               (cancel-timer (:event-timer supervisor))
               (.shutdown event-manager)
               (.shutdown processes-event-manager)
               (.disconnect (:storm-cluster-state supervisor)))
     SupervisorDaemon
     (get-conf [this]
       conf)
     (get-id [this]
       (:supervisor-id supervisor))
     (shutdown-all-workers [this]
       (let [ids (my-worker-ids conf)]
         (doseq [id ids]
           (shutdown-worker supervisor id)
           )))
     DaemonCommon
     (waiting? [this]
       (or (not @(:active supervisor))
           (and
            (timer-waiting? (:heartbeat-timer supervisor))
            (timer-waiting? (:event-timer supervisor))
            (every? (memfn waiting?) managers)))
           ))))

(defn kill-supervisor [supervisor]
  (.shutdown supervisor)
  )

;; 根据配置文件中SUPERVISOR-RUN-WORKER-AS-USER字段，调用worker-launcher-and-wait函数
;; SUPERVISOR-RUN-WORKER-AS-USER在哪里定义？clojure-config-name函数将减号换为下划线
(defn setup-storm-code-dir [conf storm-conf dir]
 (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
  (worker-launcher-and-wait conf (storm-conf TOPOLOGY-SUBMITTER-USER) ["code-dir" dir] :log-prefix (str "setup conf for " dir))))

;; distributed implementation
(defmethod download-storm-code
    :distributed [conf storm-id master-code-dir download-lock]
    ;; Downloading to permanent location is atomic
    (let [tmproot (str (supervisor-tmp-dir conf) file-path-separator (uuid)) ;; 创建历史目录
          stormroot (supervisor-stormdist-root conf storm-id)] ;; 为该topology创建stormdist目录
      (locking download-lock
            (log-message "Downloading code for storm id "
                         storm-id
                         " from "
                         master-code-dir)
            (FileUtils/forceMkdir (File. tmproot))

            ;; 下载jar包，downloadFromMaster使用NimbusClient提供的接口
            (Utils/downloadFromMaster conf (master-stormjar-path master-code-dir) (supervisor-stormjar-path tmproot))
            ;; 下载stormcode文件
            (Utils/downloadFromMaster conf (master-stormcode-path master-code-dir) (supervisor-stormcode-path tmproot))
            ;; 下载stormconf文件
            (Utils/downloadFromMaster conf (master-stormconf-path master-code-dir) (supervisor-stormconf-path tmproot))
            ;; 解压jar包到resource文件夹下
            (extract-dir-from-jar (supervisor-stormjar-path tmproot) RESOURCES-SUBDIR tmproot)
            ;; 如果stormroot不存在，将tmp移动到stormroot，否则删除tmp文件夹
            (if-not (.exists (File. stormroot))
              (FileUtils/moveDirectory (File. tmproot) (File. stormroot))
              (FileUtils/deleteDirectory (File. tmproot)))
            ;; 根据conf配置中SUPERVISOR-RUN-WORKER-AS-USER字段调用相应函数
            (setup-storm-code-dir conf (read-supervisor-storm-conf conf storm-id) stormroot)
            (log-message "Finished downloading code for storm id "
                         storm-id
                         " from "
                         master-code-dir))
      ))

;; 貌似是登陆信息写入日志
(defn write-log-metadata-to-yaml-file! [storm-id port data conf]
  (let [file (get-log-metadata-file storm-id port)]
    ;;run worker as user needs the directory to have special permissions
    ;; or it is insecure
    (when (and (not (conf SUPERVISOR-RUN-WORKER-AS-USER))
               (not (.exists (.getParentFile file))))
      (.mkdirs (.getParentFile file)))
    (let [writer (java.io.FileWriter. file)
        yaml (Yaml.)]
      (try
        (.dump yaml data writer)
        (finally
          (.close writer))))))

(defn write-log-metadata! [storm-conf user worker-id storm-id port conf]
  (let [data {TOPOLOGY-SUBMITTER-USER user
              "worker-id" worker-id
              LOGS-GROUPS (sort (distinct (remove nil?
                                           (concat
                                             (storm-conf LOGS-GROUPS)
                                             (storm-conf TOPOLOGY-GROUPS)))))
              LOGS-USERS (sort (distinct (remove nil?
                                           (concat
                                             (storm-conf LOGS-USERS)
                                             (storm-conf TOPOLOGY-USERS)))))}]
    (write-log-metadata-to-yaml-file! storm-id port data conf)))

(defn jlp [stormroot conf]
  (let [resource-root (str stormroot File/separator RESOURCES-SUBDIR)
        os (clojure.string/replace (System/getProperty "os.name") #"\s+" "_")
        arch (System/getProperty "os.arch")
        arch-resource-root (str resource-root File/separator os "-" arch)]
    (str arch-resource-root File/pathSeparator resource-root File/pathSeparator (conf JAVA-LIBRARY-PATH))))

(defn substitute-childopts
  "Generates runtime childopts by replacing keys with topology-id, worker-id, port"
  [value worker-id topology-id port]
  (let [replacement-map {"%ID%"          (str port)
                         "%WORKER-ID%"   (str worker-id)
                         "%TOPOLOGY-ID%"    (str topology-id)
                         "%WORKER-PORT%" (str port)}
        sub-fn #(reduce (fn [string entry]
                          (apply clojure.string/replace string entry))
                        %
                        replacement-map)]
    (cond
      (nil? value) nil
      (list? value) (map sub-fn value)
      :else (-> value sub-fn (clojure.string/split #"\s+")))))

(defn java-cmd []
  (let [java-home (.get (System/getenv) "JAVA_HOME")]
    (if (nil? java-home)
      "java"
      (str java-home file-path-separator "bin" file-path-separator "java")
      )))

;; 启动worker进程
(defmethod launch-worker
    :distributed [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          run-worker-as-user (conf SUPERVISOR-RUN-WORKER-AS-USER)
          storm-home (System/getProperty "storm.home")
          storm-options (System/getProperty "storm.options")
          storm-conf-file (System/getProperty "storm.conf.file")
          storm-log-dir (or (System/getProperty "storm.log.dir") (str storm-home file-path-separator "logs"))
          storm-conf (read-storm-config)
          storm-log-conf-dir (storm-conf "storm.logback.conf.dir")
          storm-logback-conf-dir (or storm-log-conf-dir (str storm-home file-path-separator "logback"))
          stormroot (supervisor-stormdist-root conf storm-id)
          jlp (jlp stormroot conf) ;; java library path
          stormjar (supervisor-stormjar-path stormroot)
          storm-conf (read-supervisor-storm-conf conf storm-id)
          topo-classpath (if-let [cp (storm-conf TOPOLOGY-CLASSPATH)] ;; 还可以通过配置文件给定classpath
                           [cp]
                           [])
          classpath (-> (current-classpath)
                        (add-to-classpath [stormjar]) ;; jar包解压目录加入classpath
                        (add-to-classpath topo-classpath))
          top-gc-opts (storm-conf TOPOLOGY-WORKER-GC-CHILDOPTS)
          gc-opts (substitute-childopts (if top-gc-opts top-gc-opts (conf WORKER-GC-CHILDOPTS)) worker-id storm-id port)
          user (storm-conf TOPOLOGY-SUBMITTER-USER)
          logfilename (logs-filename storm-id port)
          worker-childopts (when-let [s (conf WORKER-CHILDOPTS)]
                             (substitute-childopts s worker-id storm-id port))
          topo-worker-childopts (when-let [s (storm-conf TOPOLOGY-WORKER-CHILDOPTS)]
                                  (substitute-childopts s worker-id storm-id port))
          topology-worker-environment (if-let [env (storm-conf TOPOLOGY-ENVIRONMENT)]
                                        (merge env {"LD_LIBRARY_PATH" jlp})
                                        {"LD_LIBRARY_PATH" jlp})
          ;; 构造启动命令
          command (concat
                    [(java-cmd) "-server"]
                    worker-childopts
                    topo-worker-childopts
                    gc-opts
                    [(str "-Djava.library.path=" jlp)
                     (str "-Dlogfile.name=" logfilename)
                     (str "-Dstorm.home=" storm-home)
                     (str "-Dstorm.conf.file=" storm-conf-file)
                     (str "-Dstorm.options=" storm-options)
                     (str "-Dstorm.log.dir=" storm-log-dir)
                     (str "-Dlogback.configurationFile=" storm-logback-conf-dir file-path-separator "worker.xml")
                     (str "-Dstorm.id=" storm-id)
                     (str "-Dworker.id=" worker-id)
                     (str "-Dworker.port=" port)
                     "-cp" classpath
                     "backtype.storm.daemon.worker"
                     storm-id
                     (:assignment-id supervisor)
                     port
                     worker-id])
          command (->> command (map str) (filter (complement empty?)))]
      (log-message "Launching worker with command: " (shell-cmd command))
      (write-log-metadata! storm-conf user worker-id storm-id port conf) ;; 写log-metadata，log-metadata的作用？？用户权限管理？
      (set-worker-user! conf worker-id user) ;; 写入worker-user文件，worker-user文件什么用？？用户权限管理？
      (let [log-prefix (str "Worker Process " worker-id)
           callback (fn [exit-code]
                          (log-message log-prefix " exited with code: " exit-code)
                          (add-dead-worker worker-id))]
        (remove-dead-worker worker-id) ;; 从dead-worker集合中删除workerid
        (if run-worker-as-user
          (let [worker-dir (worker-root conf worker-id)]
            (worker-launcher conf user ["worker" worker-dir (write-script worker-dir command :environment topology-worker-environment)] :log-prefix log-prefix :exit-code-callback callback))
          (launch-process command :environment topology-worker-environment :log-prefix log-prefix :exit-code-callback callback)
      ))))

;; local implementation

(defn resources-jar []
  (->> (.split (current-classpath) File/pathSeparator)
       (filter #(.endsWith  % ".jar"))
       (filter #(zip-contains-dir? % RESOURCES-SUBDIR))
       first ))

(defmethod download-storm-code
    :local [conf storm-id master-code-dir download-lock]
    (let [stormroot (supervisor-stormdist-root conf storm-id)]
      (locking download-lock
            (FileUtils/copyDirectory (File. master-code-dir) (File. stormroot))
            (let [classloader (.getContextClassLoader (Thread/currentThread))
                  resources-jar (resources-jar)
                  url (.getResource classloader RESOURCES-SUBDIR)
                  target-dir (str stormroot file-path-separator RESOURCES-SUBDIR)]
              (cond
               resources-jar
               (do
                 (log-message "Extracting resources from jar at " resources-jar " to " target-dir)
                 (extract-dir-from-jar resources-jar RESOURCES-SUBDIR stormroot))
               url
               (do
                 (log-message "Copying resources at " (URI. (str url)) " to " target-dir)
                 (if (= (.getProtocol url) "jar" )
                   (extract-dir-from-jar (.getFile (.getJarFileURL (.openConnection url))) RESOURCES-SUBDIR stormroot)
                   (FileUtils/copyDirectory (File. (.getPath (URI. (str url)))) (File. target-dir)))
                 )
               )
              )
            )))

(defmethod launch-worker
    :local [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          pid (uuid)
          worker (worker/mk-worker conf
                                   (:shared-context supervisor)
                                   storm-id
                                   (:assignment-id supervisor)
                                   port
                                   worker-id)]
      (set-worker-user! conf worker-id "")
      (psim/register-process pid worker)
      (swap! (:worker-thread-pids-atom supervisor) assoc worker-id pid)
      ))

(defn -launch [supervisor]
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (let [supervisor (mk-supervisor conf nil supervisor)]
      (add-shutdown-hook-with-force-kill-in-1-sec #(.shutdown supervisor))))) ;; 加入jvm shutdownhook，并且等待一秒后强制发送-9信号

(defn standalone-supervisor []
  (let [conf-atom (atom nil)
        id-atom (atom nil)]
    (reify ISupervisor
      (prepare [this conf local-dir] ;; 初始化本地LocalState，如果当前本地id为空，初始化uuid并设置
        (reset! conf-atom conf);; 设置配置
        (let [state (LocalState. local-dir);; 设置本地LocalState
              curr-id (if-let [id (.get state LS-ID)];; 如果当前id为空，通过uuid获取supervisorid
                        id
                        (generate-supervisor-id))]
          (.put state LS-ID curr-id)
          (reset! id-atom curr-id))
        )
      (confirmAssigned [this port]
        true) ;; 始终返回true
      (getMetadata [this]
        (doall (map int (get @conf-atom SUPERVISOR-SLOTS-PORTS))));; 获取配置中获取的所有端口号
      (getSupervisorId [this] ;; 返回supervisor id
        @id-atom)
      (getAssignmentId [this] ;; 同样返回supervisor id
        @id-atom)
      (killedWorker [this port] ;; 未做任何操作
        )
      (assigned [this ports] ;; 未做任何操作
        ))))

(defn -main []
  (-launch (standalone-supervisor)))
