(ns mc032.implementation.scheduler
  (:require [mc032.implementation.scheduler.errors :as-alias scheduler.errors]
            [promesa.core :as p]
            [malli.core :as m]))

(defprotocol IScheduler
  (-schedule! [it f-id args] "Returns a promesa.protocols/IPromise"))

(defprotocol IPoolNodePicker
  (-pick [it f-id] "Returns a promesa.protocols/IPromise"))

(defprotocol IRPCInterface
  (-invoke! [this node-ref-id f-id args] "Returns promesa.protocols/IPromise"))

(defrecord MockRPCInterace
    [f-id->function-map*]
  IRPCInterface
  (-invoke! [_ node-ref-id f-id args]
    (let [f (get @f-id->function-map* f-id)]
      (p/future
        (println (format "Starting execution of function `%s` in node `%s`" f-id node-ref-id))
        (let [f-result (apply f args)]
          (println (format "Finished execution of function `%s` in node `%s`" f-id node-ref-id))
          f-result)))))

(defn pick
  [pool-node-picker f-id]
  (try
    (-pick pool-node-picker f-id)
    (catch Exception e
      (throw (ex-info "Error picking node for function."
                      {:type ::scheduler.errors/pick
                       :f-id f-id}
                      e)))))

(defn invoke!
  [rpc-interface node-ref-id f-id args]
  (try
    (-invoke! rpc-interface node-ref-id f-id args)
    (catch Exception e
      (throw (ex-info "Error invoking function through rpc-interface."
                      {:type ::scheduler.errors/invoke!
                       :rpc-interface rpc-interface
                       :f-id f-id
                       :args args}
                      e)))))

(defrecord StatefulScheduler
    [pool-node-picker
     rpc-interface]
  IScheduler
  (-schedule! [_ f-id args]
    (p/future
      (let [node-ref-id @(pick pool-node-picker f-id)]
        @(invoke! rpc-interface node-ref-id f-id args)))))

(def f-id-schema uuid?)

(def node-ref-id uuid?)

(def pool-db-map-schema
  (m/schema
   [:map-of {:registry {::f-id f-id-schema
                        ::node-ref-id node-ref-id}}
    ::f-id [:set ::node-ref-id]]))

(comment
  (m/explain
   pool-db-map-schema
   (doto (let [node-ref-ids (into #{} (map (fn [_] (random-uuid)) (range 3)))]
           (into {} (map (fn [_] [(random-uuid) node-ref-ids]) (range 4))))
     prn))
  
  (require '[malli.error :as me])

  (-> (m/explain
       pool-db-map-schema
       (let [node-ref-ids (into #{} (map (fn [_] (-> (random-uuid) str keyword)) (range 4)))]
         (into {} (map (fn [_] [(random-uuid) node-ref-ids]) (range 10)))))
      (me/humanize)))

(defrecord NaiveMapDBPoolNodePicker
    [pool-db-map*]
  IPoolNodePicker
  (-pick [_ f-id]
    (p/future
      (let [available-nodes (get @pool-db-map* f-id)]
        (rand-nth (seq available-nodes))))))

