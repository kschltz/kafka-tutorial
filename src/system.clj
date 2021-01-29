(ns system
 (:require [integrant.core :as ig]
           [kafka.admin.ig :as kafka]))

(defonce state (atom false))

(def config
 {::kafka/adm-client                  {"bootstrap.servers" "localhost:9092"}
  [:business/distópico ::kafka/topic] {:adm-client               (ig/ref ::kafka/adm-client)
                                       :topic/name               "incrivelmente-distopico"
                                       :topic/partition-count    2
                                       :topic/replication-factor 1}
  [:business/utópico ::kafka/topic]   {:adm-client               (ig/ref ::kafka/adm-client)
                                       :topic/name               "incrivelmente-utopico"
                                       :topic/partition-count    3
                                       :topic/replication-factor 1}})

(defn start! []
 (if-not @state
  (reset! state (ig/init config))))

(defn stop! []
 (when @state
  (ig/halt! @state)
  (reset! state false)))




