(ns mc032.implementation.message-system-client
  (:require [implementation.message-system-client.errors :as-alias errors]
            [promesa.core :as p]
            [promesa.exec.csp :as sp]))

(defprotocol MessageSystemClient
  "A protocol for a message system client with subscribe, unsubscribe, and publish methods."
  (-sub! [this ch-id]
    "Subscribe to the specified channel (ch-id), returning a unique subscription id and a CSP channel for receiving messages.")
  (-unsub! [this ch-id sub-id]
    "Unsubscribe the channel associated with the given subscription id (sub-id) from the topic (ch-id).")
  (-pub! [this ch-id message]
    "Publish the given message to the specified channel (ch-id), returning a truthy value if successful."))

(defn sub!
  [msg-sys-client ch-id]
  (try
    (-sub! msg-sys-client ch-id)
    (catch Exception e
      (throw (ex-info "Error subscribing to channel."
                      {:type ::errors/sub!
                       :msg-sys-client msg-sys-client
                       :ch-id ch-id}
                      e)))))

(defn unsub!
  [msg-sys-client ch-id sub-id]
  (try
    (-unsub! msg-sys-client ch-id sub-id)
    (catch Exception e
      (throw (ex-info "Error unsubscribing to channel."
                      {:type ::errors/unsub!
                       :msg-sys-client msg-sys-client
                       :ch-id ch-id
                       :sub-id sub-id}
                      e)))))

(defn pub!
  [msg-sys-client ch-id msg]
  (try
    (-pub! msg-sys-client ch-id msg)
    (catch Exception e
      (throw (ex-info "Error publishing to channel."
                      {:type ::errors/pub!
                       :msg-sys-client msg-sys-client
                       :ch-id ch-id
                       :msg msg}
                      e)))))

(defrecord MockMessageSystemClient
    [ch-id->subs-map*]
  MessageSystemClient  
  (-sub! [_ ch-id]
    (let [sub-id (random-uuid)
          sp-chan (sp/chan :buf 8)]
      (swap! ch-id->subs-map* update ch-id assoc sub-id sp-chan)
      [sub-id sp-chan]))
  (-unsub! [_ ch-id sub-id]
    (swap! ch-id->subs-map* update ch-id dissoc sub-id))
  (-pub! [_ ch-id message]
    (let [subs (get @ch-id->subs-map* ch-id)]
      (doseq [[_ sp-chan] subs]
        (p/race [(sp/put sp-chan message)
                 (p/delay 500 ::timeout)])))))

(defn mock-message-system-client
  ([]
   (mock-message-system-client {}))
  ([ch-id->subs-map]
   (->MockMessageSystemClient (atom ch-id->subs-map))))

(comment
  (def msg-sys-client (->MockMessageSystemClient (atom {})))
  (def sub1 (sub! msg-sys-client :ch1))
  (def sub1-ch (second sub1))
  (def sub1-id (first sub1))
  (pub! msg-sys-client :ch1 "fart")

  (sp/go (-> (sp/<! sub1-ch) println)))
