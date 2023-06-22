(ns mc032.development.scheduler
  (:require [mc032.implementation.scheduler :as scheduler]
            [integrant.core :as ig]
            [integrant.repl :refer [clear go halt init prep reset reset-all]]
            [integrant.repl.state :as state]
            [promesa.core :as p]))

(def config
  {:implementation/stateful-scheduler
   {:pool-node-picker (ig/ref :implementation/naive-map-db-pool-node-picker)
    :rpc-interface (ig/ref :implementation/mock-rpc-interface)}

   :implementation/mock-rpc-interface
   {:f-id->function-map* (ig/ref :implementation/f-id->function-map*)}

   :implementation/f-id->function-map* {}

   :implementation/naive-map-db-pool-node-picker
   {:pool-db-map*  (ig/ref :implementation/pool-db-map*)}
   
   :implementation/pool-db-map* {}})

(defmethod ig/init-key :implementation/stateful-scheduler
  [_ {:keys [pool-node-picker rpc-interface]}]
  (scheduler/->StatefulScheduler pool-node-picker rpc-interface))

(comment
  (defmethod ig/halt-key! :implementation/stateful-scheduler [_ scheduler]
    nil))

(defmethod ig/init-key :implementation/mock-rpc-interface
  [_ {:keys [f-id->function-map*]}]
  (scheduler/->MockRPCInterace f-id->function-map*))

(defmethod ig/init-key :implementation/f-id->function-map*
  [_ _]
  (atom {"f1" #'+
         "f2" #'-}))

(defmethod ig/init-key :implementation/naive-map-db-pool-node-picker
  [_ {:keys [pool-db-map*]}]
  (scheduler/->NaiveMapDBPoolNodePicker pool-db-map*))

(defmethod ig/init-key :implementation/pool-db-map*
  [_ _]
  (atom {"f1" #{"n1" "n2"}
         "f2" #{"n1"}}))

(integrant.repl/set-prep! (constantly config))

(comment
  (prep)
  (init)
  (reset-all)
  state/system

  (def scheduler
    (get state/system :implementation/stateful-scheduler))

  @(scheduler/-schedule! scheduler "f2" [1 2 3]))

