(ns mc032.implementation.rpc-interface
  (:require [mc032.implementation.rpc-interface.erros :as-alias errors]
            [mc032.implementation.function :as function]
            [mc032.implementation.message-system-client :refer [pub!]]
            [promesa.exec :as px]))

(defprotocol IRPCInterface
  (-invoke! [this node-ref-id f-id args] "Returns promesa.protocols/IPromise"))

(defn invoke!
  [rpc-interface node-ref-id f-invocation args]
  (try
    (-invoke! rpc-interface node-ref-id f-invocation args)
    (catch Exception e
      (throw (ex-info "Error invoking function through rpc-interface."
                      {:type ::errors/invoke!
                       :rpc-interface rpc-interface
                       :f-id f-invocation
                       :args args}
                      e)))))

(defrecord MockRPCInterace
    [f-id->function-map*
     msg-sys-client]
  IRPCInterface
  (-invoke! [_ node-ref-id f-invocation args]
    (let [f-id (::function/f-id f-invocation)
          f (get @f-id->function-map* f-id)
          f-invocation-result-id (function/f-invocation-result-id f-invocation)]
      (px/submit!
       (fn []
         (try
           (println (format "Starting execution of function `%s` in node `%s`" f-id node-ref-id))
           (let [f-result (apply f args)]
             (println (format "Finished execution of function `%s` in node `%s`" f-id node-ref-id))
             (pub! msg-sys-client f-invocation-result-id f-result))
           (catch Exception e
             (pub! msg-sys-client f-invocation-result-id
                   (ex-info "Failed execution of function."
                            {:node-ref-id node-ref-id
                             :f-invocation f-invocation
                             :args args}
                            e))))))
      f-invocation-result-id)))

(defn mock-rpc-interface
  [f-id->function-map* msg-sys-client]
  (->MockRPCInterace f-id->function-map* msg-sys-client))

