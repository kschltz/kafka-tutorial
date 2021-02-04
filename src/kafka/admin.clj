(ns kafka.admin
 (:require [jackdaw.admin :as j.admin]
           [jackdaw.client :as j.cli]
           [jackdaw.serdes :as serdes]
           [jackdaw.serdes.edn2 :as edn.serdes]
           [jackdaw.streams :as streams])
 (:import (org.apache.kafka.common.serialization Serdes)
          (org.apache.kafka.streams StreamsConfig)))

(def default-config
 {"bootstrap.servers" "localhost:9092"})

(def default-serdes
 {:key-serde   (Serdes/Long)
  :value-serde (Serdes/String)})

(defn admin-client [& conf-maps]
 (j.admin/->AdminClient
  (apply merge default-config conf-maps)))

(defn topic-exists? [admin-client topic-name]
 (j.admin/topic-exists? admin-client
                        {:topic-name topic-name}))

(defn create-topic
 [admin-client {:keys [topic-name
                       partition-count
                       replication-factor]
                :or   {partition-count    3
                       replication-factor 1}}]
 (j.admin/create-topics! admin-client
                         [{:topic-name         topic-name
                           :partition-count    partition-count
                           :replication-factor replication-factor}]))

(defn producer [& {:keys [cfg
                          serdes]}]
 (j.cli/producer (merge default-config
                        cfg)
                 (apply merge default-serdes serdes)))

(defn consumer [{:keys [group-id]
                 :as   cfg}]
 (j.cli/consumer (merge default-config cfg {:group.id group-id})
                 default-serdes))

(defn do-in-another-thread [group-id f]
 (future
  (let [c (j.cli/subscribe (consumer {:group-id group-id})
                           [{:topic-name "Genoveva"}])]
   (while true
    (->> (j.cli/poll c 1000)
         (run! f))
    (.commitSync c)))))


(defn generate-merchs []
 (repeatedly
  #(hash-map
    :merch/id (long (rand-int 80000))
    :merch/name (->> (fn [] (char (rand-int 98)))
                     (repeatedly 25)
                     (reduce str)))))
(defn generate-pos []
 (repeatedly
  #(hash-map
    :merch/id (long (rand-int 80000))
    :pos/number (rand-int 50))))


(defn produce-merchs [n & {:keys [fixed]}]
 (with-open [prod (producer :serdes {:value-serde (serdes/edn-serde)})]
  (->> (or fixed (generate-merchs))
       (take n)
       (run! (fn [{:merch/keys [id]
                   :as         msg}]
              (prn "merch" msg)
              (prn @(j.cli/produce! prod
                                    {:topic-name "merchant-source"}
                                    id msg)))))))
(defn produce-pos [n & {:keys [fixed]}]
 (with-open [prod (producer :serdes {:value-serde (serdes/edn-serde)})]
  (->> (or  fixed (generate-pos))
       (take n)
       (run! (fn [{:merch/keys [id]
                   :as         msg}]
              (prn "pos" msg)
              (prn @(j.cli/produce! prod
                                    {:topic-name "pos-source"}
                                    id msg)))))))


(defn print-stream [stream msg]
 (streams/for-each! stream  #(prn msg %)))

(defn topology []
 (let [app-conf {"application.id"            "merch-app"
                 "commit.interval.ms"        100
                 "bootstrap.servers"         "localhost:9092"
                 "default.key.serde"         "org.apache.kafka.common.serialization.Serdes$LongSerde"
                 "default.value.serde"       "jackdaw.serdes.EdnSerde"}

       b (streams/streams-builder)
       merch-table (streams/ktable b {:topic-name "merchant-source"})
       pos-table (streams/ktable b {:topic-name "pos-source"})
       full-table (streams/join merch-table pos-table
                                (fn [merch pos] (merge merch pos)))]

  (-> merch-table
       streams/to-kstream
       (print-stream "MERCH-TABLE: "))

  (-> pos-table
       streams/to-kstream
       (print-stream "POS-TABLE: "))
  ;;emits only 'complete events'
  (doto full-table
   (-> streams/to-kstream
       streams/group-by-key
       streams/count
       streams/to-kstream
       (print-stream "COUNT:"))
   (-> streams/to-kstream
       (print-stream "FULL-EVENT:")))


  (doto (streams/kafka-streams b app-conf)
    streams/start)))

(defn run-example [size & {:merch/keys [id]}]
 (let [t (topology)]
  (if id
   (do (produce-merchs
        size :fixed [{:merch/id   id
                      :merch/name "yo soy yo"}])
       (produce-pos
        size :fixed [{:merch/id   id
                      :pos/number (rand-int 500)}]))
   (do (produce-merchs size)
       (produce-pos size)))

  t))