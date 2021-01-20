(ns kafka.admin
 (:require [jackdaw.admin :as j.admin]
           [jackdaw.client :as j.cli])
 (:import (org.apache.kafka.common.serialization Serdes)))

(def default-config
 {"bootstrap.servers" "localhost:9092"})

(def default-serdes
 {:key-serde   (Serdes/Long)
  :value-serde (Serdes/String)})

(defn admin-client []
 (j.admin/->AdminClient default-config))

(defn create-topic
 [admin-client & {:keys [topic-name]}]
 (j.admin/create-topics! (admin-client)
                         [{:topic-name         topic-name
                           :partition-count    3
                           :replication-factor 1}]))

(defn producer []
 (j.cli/producer default-config
                 default-serdes))

(defn consumer [group-id]
 (j.cli/consumer (merge default-config {:group.id group-id})
                 default-serdes))

(defn do-in-another-thread [group-id f]
 (future
  (let [c (j.cli/subscribe (consumer group-id)
                           [{:topic-name "Genoveva"}])]
   (while true
    (->> (j.cli/poll c 1000)
         (run! f))
    (.commitSync c)))))

(comment

 (def pcs (->> (range 2)
               (map (fn [n]
                     (do-in-another-thread
                      "printer"
                      #(prn "consumer #"n %))))
               (into [])))

 (with-open [adm (admin-client)]
  (j.admin/topics-ready? adm [{:topic-name "Genoveva"}]))

 @(j.cli/produce! (producer)
                 {:topic-name "Genoveva"} "TESTE")
 (j.cli/produce! (producer)
                 {:topic-name "Genoveva"} 998 "TESTE"))