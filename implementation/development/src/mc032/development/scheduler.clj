(ns mc032.development.scheduler
  (:require [mc032.implementation.scheduler :as impl.scheduler]
            [mc032.implementation.function :as impl.function]
            [mc032.implementation.message-system-client :as impl.msg]
            [mc032.implementation.rpc-interface :as impl.rpc]
            [mc032.implementation.pool-node-picker :as impl.pool-node-picker]
            [integrant.core :as ig]
            [integrant.repl :refer [clear go halt init prep reset reset-all]]
            [integrant.repl.state :as state]
            [promesa.core :as p]
            [promesa.exec.csp :as sp]))

(def config
  {:implementation/stateful-scheduler
   {:pool-node-picker (ig/ref :implementation/naive-map-db-pool-node-picker)
    :rpc-interface (ig/ref :implementation/mock-rpc-interface)}

   :implementation/mock-rpc-interface
   {:f-id->function-map* (ig/ref :implementation/f-id->function-map*)
    :msg-sys-client (ig/ref :implementation/mock-message-system-client)}

   :implementation/f-id->function-map* {}

   :implementation/mock-message-system-client {}

   :implementation/naive-map-db-pool-node-picker
   {:pool-db-map*  (ig/ref :implementation/pool-db-map*)}
   
   :implementation/pool-db-map* {}})

(defmethod ig/init-key :implementation/stateful-scheduler
  [_ {:keys [pool-node-picker rpc-interface]}]
  (impl.scheduler/stateful-scheduler pool-node-picker rpc-interface))

(defmethod ig/init-key :implementation/mock-rpc-interface
  [_ {:keys [f-id->function-map* msg-sys-client]}]
  (impl.rpc/mock-rpc-interface f-id->function-map* msg-sys-client))

(defmethod ig/init-key :implementation/f-id->function-map*
  [_ _]
  (atom {"f1" #'+
         "f2" #'-
         "f-juliana" (fn [& args]
                       (println "Multiplicação da juliana args: " args)
                       (apply * args))}))

(defmethod ig/init-key :implementation/mock-message-system-client
  [_ _]
  (impl.msg/mock-message-system-client))

(defmethod ig/init-key :implementation/naive-map-db-pool-node-picker
  [_ {:keys [pool-db-map*]}]
  (impl.pool-node-picker/naive-map-db-pool-node-picker pool-db-map*))

(defmethod ig/init-key :implementation/pool-db-map*
  [_ _]
  (atom {"f1" #{"n1" "n2"}
         "f2" #{"n1"}
         "f-juliana" #{"worker-especial"}}))

(integrant.repl/set-prep! (constantly config))

(comment
  (prep)
  (init)
  (reset-all)
  state/system

  (def scheduler
    (get state/system :implementation/stateful-scheduler))

  (def msg-sys-client
    (get state/system :implementation/mock-message-system-client))

  (def f-invocation (impl.function/f-invocation "f-juliana" (random-uuid)))

  (let [[sub-id sub-ch]
        (impl.msg/sub! msg-sys-client (impl.function/f-invocation-result-id f-invocation))]
    (def sub-id sub-id)
    (def sub-ch sub-ch))

  (sp/go
    (let [result (sp/<! sub-ch)]
      (println (str "Sending invocation result to client: " result))))

  (def f-invocatoin-id @(impl.scheduler/schedule! scheduler f-invocation [1 2 3])))

