(ns mc032.implementation.function)

(defn f-invocation
  [f-id invocation-id]
  {::f-id f-id
   ::invocation-id invocation-id})

(defn f-invocation-result-id
  [{::keys [f-id invocation-id]}]
  (str f-id \: invocation-id "/result"))
