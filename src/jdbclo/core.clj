(ns jdbclo.core
 (:import (java.sql DriverManager))
 (:require clojure.string))

(def ^:dynamic *conn* nil)
(def ^:dynamic *stmt* nil)

(defn build-connection-url [ db-spec ]
  (clojure.string/join ":" [
    "jdbc"
    (:subprotocol db-spec)
    (:subname db-spec)
  ])
)

(defn open-connection [ db-spec ]
  (Class/forName (:classname db-spec))
  (DriverManager/getConnection 
                    (build-connection-url db-spec)
                    (:user db-spec)
                    (:password db-spec))
)

(defn create-statement [ conn ]
  (.createStatement conn))

(defn execute-update [ stmt query-string ]
  (.executeUpdate stmt query-string))

(defn close-statement [ stmt ]
  (.close stmt))

(defn close-connection [ conn ]
  (.close conn))

(defmacro with-connection [ db-spec & body ]
  `(binding [*conn* (open-connection ~db-spec)]
     (try
       (do ~@body)
       (finally (close-connection *conn*))
     )
   )
)

(defmacro with-statement [ & body ]
  `(binding [*stmt* (create-statement *conn*)]
     (try
       (do ~@body)
       (finally (close-statement *stmt*))
     )
   )
)

(defn update! [ query-string ]
  (execute-update *stmt* query-string))

(defn demo-connect []
  (def db-spec 
	  {:classname "com.mysql.jdbc.Driver"
	   :subprotocol "mysql"
	   :subname "//elecsprint.hoenn.pkmn:3306/clojure-test"
	   :user "clojure"
	   :password "clojurepass"})
  ; (def conn (open-connection db-spec))
  (with-connection db-spec
    (with-statement
      (update! "CREATE TABLE tbl (a INT)")))
  ; (close-connection conn) 
)

(defn -main
  "I don't do a whole lot."
  []
  (demo-connect)
  (println "Hello, World!"))
