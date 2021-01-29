(ns kafka.admin.ig
 (:require [integrant.core :as ig]
           [kafka.admin :as k.adm]))

(defmethod ig/init-key ::adm-client
 [k cfg]
 (prn "Initializing:" k)
 (k.adm/admin-client cfg))

(defmethod ig/init-key ::topic
 [k {:keys       [adm-client]
     :topic/keys [name partition-count replication-factor]
     :as         t-conf}]
 (let [create? (not (k.adm/topic-exists? adm-client name))]
  (when create?
   (k.adm/create-topic adm-client {:topic-name         name
                                   :partition-count    partition-count
                                   :replication-factor replication-factor}))
  (-> t-conf
      (dissoc :adm-client)
      (assoc :new? create?))))
