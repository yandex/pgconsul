(ns jepsen.pgconsul-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.pgconsul :as pgconsul]))

(def pg_nodes ["pgconsul_postgresql1_1.pgconsul_pgconsul_net"
               "pgconsul_postgresql2_1.pgconsul_pgconsul_net"
               "pgconsul_postgresql3_1.pgconsul_pgconsul_net"])

(def zk_nodes ["pgconsul_zookeeper1_1.pgconsul_pgconsul_net"
               "pgconsul_zookeeper2_1.pgconsul_pgconsul_net"
               "pgconsul_zookeeper3_1.pgconsul_pgconsul_net"])

(deftest pgconsul-test
  (is (:valid? (:results (jepsen/run! (pgconsul/pgconsul-test pg_nodes zk_nodes))))))
