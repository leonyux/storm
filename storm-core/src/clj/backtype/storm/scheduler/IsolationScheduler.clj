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
(ns backtype.storm.scheduler.IsolationScheduler
  (:use [backtype.storm util config log])
  (:require [backtype.storm.scheduler.DefaultScheduler :as DefaultScheduler])
  (:import [java.util HashSet Set List LinkedList ArrayList Map HashMap])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :init init
    :constructors {[] []}
    :state state 
    :implements [backtype.storm.scheduler.IScheduler]))

(defn -init []
  [[] (container)])

(defn -prepare [this conf]
  (container-set! (.state this) conf))

;; 返回executor分组信息，根据worker数目轮询分组，得到#{#{executers}...}分组信息
(defn- compute-worker-specs "Returns mutable set of sets of executors"
  [^TopologyDetails details]
  ;; 从TopologyDetails中读取{executor componentid}信息
  (->> (.getExecutorToComponent details)
       ;; 反转映射得到{componentid [executors]}
       reverse-map
       ;; 得到([executors]..)
       (map second)
       ;; [executors]
       (apply concat)
       ;; ([workerid executor]...) 轮转将executor分配到各worker上，workerid 0开始
       (map vector (repeat-seq (range (.getNumWorkers details))))
       ;; group同一worker上的executor
       (group-by first)
       ;; 得到((executers)...)分组信息
       (map-val #(map second %))
       vals
       (map set)
       (HashSet.)
       ))

(defn isolated-topologies [conf topologies]
  (let [tset (-> conf (get ISOLATION-SCHEDULER-MACHINES) keys set)]
    (filter (fn [^TopologyDetails t] (contains? tset (.getName t))) topologies)
    ))

;; map from topology id -> set of sets of executors
(defn topology-worker-specs [iso-topologies]
  (->> iso-topologies
       (map (fn [t] {(.getId t) (compute-worker-specs t)}))
       (apply merge)))

;; 将topology的worker均匀的分配到给定的机器数目，并不可以指定具体的机器
(defn machine-distribution [conf ^TopologyDetails topology]
  (let [name->machines (get conf ISOLATION-SCHEDULER-MACHINES)
        machines (get name->machines (.getName topology))
        workers (.getNumWorkers topology)]
    ;; 将workers均匀的分配到给定的机器上
    (-> (integer-divided workers machines)
        (dissoc 0)
        (HashMap.)
        )))

;; 生成{tid {workers machines}}worker分配信息
(defn topology-machine-distribution [conf iso-topologies]
  (->> iso-topologies
       (map (fn [t] {(.getId t) (machine-distribution conf t)}))
       (apply merge)))

;; 把当前的分配按照host分组{host [[slot topid #{executors}]...]}
(defn host-assignments [^Cluster cluster]
  (letfn [(to-slot-specs [^SchedulerAssignment ass]
            (->> ass
                 .getExecutorToSlot
                 reverse-map
                 (map (fn [[slot executors]]
                        [slot (.getTopologyId ass) (set executors)]))))]
  (->> cluster
       .getAssignments
       vals
       (mapcat to-slot-specs)
       (group-by (fn [[^WorkerSlot slot & _]] (.getHost cluster (.getNodeId slot))))
       )))

;; 从distribution中去除当前节点的分配
(defn- decrement-distribution! [^Map distribution value]
  (let [v (-> distribution (get value) dec)]
    (if (zero? v)
      (.remove distribution value)
      (.put distribution value v))))

;; returns list of list of slots, reverse sorted by number of slots
(defn- host-assignable-slots [^Cluster cluster]
  (-<> cluster
       .getAssignableSlots;; 获取所有可以分配的slot，包括已占用的，从supervisor信息中统计
       (group-by #(.getHost cluster (.getNodeId ^WorkerSlot %)) <>);; 按照主机group这些slot
       (dissoc <> nil)
       (sort-by #(-> % second count -) <>);; 按可用slot数从大到小排序
       shuffle ;;打乱顺序？
       (LinkedList. <>)
       ))

;; 统计每个节点上使用了的slot，{host->slots}
(defn- host->used-slots [^Cluster cluster]
  (->> cluster
       .getUsedSlots
       (group-by #(.getHost cluster (.getNodeId ^WorkerSlot %)))
       ))

;; 从大到小排列worker的分配信息（worknums)
(defn- distribution->sorted-amts [distribution]
  (->> distribution
       (mapcat (fn [[val amt]] (repeat amt val)))
       (sort-by -)
       ))

(defn- allocated-topologies [topology-worker-specs]
  (->> topology-worker-specs
    (filter (fn [[_ worker-specs]] (empty? worker-specs)))
    (map first)
    set
    ))

(defn- leftover-topologies [^Topologies topologies filter-ids-set]
  (->> topologies
       .getTopologies
       (filter (fn [^TopologyDetails t] (not (contains? filter-ids-set (.getId t)))))
       (map (fn [^TopologyDetails t] {(.getId t) t}))
       (apply merge)
       (Topologies.)
       ))

;; for each isolated topology:
;;   compute even distribution of executors -> workers on the number of workers specified for the topology
;;   compute distribution of workers to machines
;; determine host -> list of [slot, topology id, executors]
;; iterate through hosts and: a machine is good if:
;;   1. only running workers from one isolated topology
;;   2. all workers running on it match one of the distributions of executors for that topology
;;   3. matches one of the # of workers
;; blacklist the good hosts and remove those workers from the list of need to be assigned workers
;; otherwise unassign all other workers for isolated topologies if assigned

(defn remove-elem-from-set! [^Set aset]
  (let [elem (-> aset .iterator .next)]
    (.remove aset elem)
    elem
    ))

;; get host -> all assignable worker slots for non-blacklisted machines (assigned or not assigned)
;; will then have a list of machines that need to be assigned (machine -> [topology, list of list of executors])
;; match each spec to a machine (who has the right number of workers), free everything else on that machine and assign those slots (do one topology at a time)
;; blacklist all machines who had production slots defined
;; log isolated topologies who weren't able to get enough slots / machines
;; run default scheduler on isolated topologies that didn't have enough slots + non-isolated topologies on remaining machines
;; set blacklist to what it was initially
(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (let [conf (container-get (.state this))
        ;; 获取黑名单中的节点        
        orig-blacklist (HashSet. (.getBlacklistedHosts cluster))
        ;; 获取隔离调度的topology TopologyDetails集合
        iso-topologies (isolated-topologies conf (.getTopologies topologies))
        ;; 获取隔离调度的topology Topology id集合
        iso-ids-set (->> iso-topologies (map #(.getId ^TopologyDetails %)) set)
        ;; 获取executor分组信息，根据分配的worker数，得到{tid #{#{executers}...}}分组信息
        topology-worker-specs (topology-worker-specs iso-topologies)
        ;; 生成{tid {workers machines}}worker分配信息，按照配置的worker数和机器数做分配计划
        topology-machine-distribution (topology-machine-distribution conf iso-topologies)
        ;; 把当前的分配按照host分组{host [[slot topid #{executors}]...]}，为当前真实分配信息
        host-assignments (host-assignments cluster)]
    (doseq [[host assignments] host-assignments]
      ;; 取出第一个slot的topolgyid
      (let [top-id (-> assignments first second)
            ;; 从需隔离调度的topology信息中获取该topology的worker计划分布信息{workers machines}
            distribution (get topology-machine-distribution top-id)
            ;; 从需隔离调度的topology信息中获取#{#{executors}..}executor的计划分组信息
            ^Set worker-specs (get topology-worker-specs top-id)
            ;; 当前的实际分配中，该节点上的worker数量
            num-workers (count assignments)
            ]
        ;; 判断该topology是否属于需要隔离调度的topology，是否该节点的slot都分配给了一个topology
                       ;; 判断该节点上的分配的slot是否满足了计算的分配数目要求，executor是否正确分配
        (if (and (contains? iso-ids-set top-id)
                 (every? #(= (second %) top-id) assignments)
                 (contains? distribution num-workers)
                 (every? #(contains? worker-specs (nth % 2)) assignments))
          (do (decrement-distribution! distribution num-workers);; 从distribution中去除当前节点的分配，注意HashMap可变
              (doseq [[_ _ executors] assignments] (.remove worker-specs executors));; 从worker-specs中去除本机executor的分配，注意HashSet可变
              (.blacklistHost cluster host)) ;; 将host放入blacklist
          (doseq [[slot top-id _] assignments]
            (when (contains? iso-ids-set top-id);; 当该topology属于需要隔离分配，但是没满足条件的，释放所占用slot
              (.freeSlot cluster slot)
              ))
          )))
    
    ;; 上面已经移除了满足了隔离分配要求的分配计划，下面这段针对未满足计划的进行分配
    ;; 统计每个节点上占用了的slot, {host #{slots}}
    (let [host->used-slots (host->used-slots cluster)
          ^LinkedList sorted-assignable-hosts (host-assignable-slots cluster)] ;; 排序了的可分配的{host #{slots}}，最后的shuffle什么用？排除了blacklist节点
      ;; TODO: can improve things further by ordering topologies in terms of who needs the least workers
      ;; 对每个topology的#{#{executors}..}executor分组进行操作
      (doseq [[top-id worker-specs] topology-worker-specs
              ;; 从大到小的worker分配,(workers1 workers2 ....)
              :let [amts (distribution->sorted-amts (get topology-machine-distribution top-id))]]
        (doseq [amt amts
                :let [[host host-slots] (.peek sorted-assignable-hosts)]]
          ;; 当节点可分配slot满足该拓扑的worker数时，从可分配slots中移除该节点，并且释放该节点的slots
          (when (and host-slots (>= (count host-slots) amt))
            (.poll sorted-assignable-hosts)
            (.freeSlots cluster (get host->used-slots host));; 释放已占用的slot
            (doseq [slot (take amt host-slots)
                    :let [executors-set (remove-elem-from-set! worker-specs)]]
              (.assign cluster slot top-id executors-set))
            (.blacklistHost cluster host))
          )))
    
    (let [failed-iso-topologies (->> topology-worker-specs
                                  (mapcat (fn [[top-id worker-specs]]
                                    (if-not (empty? worker-specs) [top-id])
                                    )))]
      (if (empty? failed-iso-topologies)
        ;; run default scheduler on non-isolated topologies
        (-<> topology-worker-specs
             allocated-topologies
             (leftover-topologies topologies <>)
             (DefaultScheduler/default-schedule <> cluster))
        (do
          (log-warn "Unable to isolate topologies " (pr-str failed-iso-topologies) ". No machine had enough worker slots to run the remaining workers for these topologies. Clearing all other resources and will wait for enough resources for isolated topologies before allocating any other resources.")
          ;; clear workers off all hosts that are not blacklisted
          (doseq [[host slots] (host->used-slots cluster)]
            (if-not (.isBlacklistedHost cluster host)
              (.freeSlots cluster slots)
              )))
        ))
    (.setBlacklistedHosts cluster orig-blacklist);; 还原blacklist，中间blacklist没看到祈祷作用？
    ))
