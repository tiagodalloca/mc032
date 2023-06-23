(ns mc032.implementation.scheduler
  (:require 
   [mc032.implementation.scheduler.errors :as-alias errors]
   [mc032.implementation.rpc-interface :refer [invoke!]]
   [mc032.implementation.pool-node-picker :refer [pick]]
   [mc032.implementation.function :as function]
   [promesa.core :as p]))

(defprotocol IScheduler
  (-schedule! [it f-id args] "Returns a promesa.protocols/IPromise"))

(defn schedule!
  ([scheduler f-invocation args]
   (try
     (-schedule! scheduler f-invocation args)
     (catch Exception e
       (throw (ex-info "Error scheduling function."
                       {:type ::errors/schedule!
                        :scheduler scheduler
                        :f-invocation f-invocation
                        :args args}
                       e))))))

(defrecord StatefulScheduler
    [pool-node-picker
     rpc-interface]
  IScheduler
  (-schedule! [_ f-invocation args]
    (p/let [f-id (::function/f-id f-invocation)
            node-ref-id (pick pool-node-picker f-id)]
      (invoke! rpc-interface node-ref-id f-invocation args))))

(defn stateful-scheduler
  [pool-node-picker
   rpc-interface]
  (->StatefulScheduler pool-node-picker rpc-interface))

