(ns mc032.implementation.pool-node-picker
  (:require [mc032.implementation.pool-node-picker.errors :as-alias errors]
            [promesa.core :as p]
            [malli.core :as m]))

(defprotocol IPoolNodePicker
  (-pick [it f-id] "Returns a promesa.protocols/IPromise"))

(defn pick
  [pool-node-picker f-id]
  (try
    (-pick pool-node-picker f-id)
    (catch Exception e
      (throw (ex-info "Error picking node for function."
                      {:type ::errors/pick
                       :f-id f-id}
                      e)))))

(def f-id-schema uuid?)

(def node-ref-id uuid?)

(def pool-db-map-schema
  (m/schema
   [:map-of {:registry {::f-id f-id-schema
                        ::node-ref-id node-ref-id}}
    ::f-id [:set ::node-ref-id]]))

(defrecord NaiveMapDBPoolNodePicker
    [pool-db-map*]
  IPoolNodePicker
  (-pick [_ f-id]
    (p/future
      (let [available-nodes (get @pool-db-map* f-id)]
        (rand-nth (seq available-nodes))))))

(defn naive-map-db-pool-node-picker
  [pool-db-map*]
  (->NaiveMapDBPoolNodePicker pool-db-map*))

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

