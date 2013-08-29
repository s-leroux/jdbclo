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

(defn execute-query [ stmt query-string ]
  (.executeQuery stmt query-string))

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

(defn execute! [ query-string ]
  (execute-update *stmt* query-string))

(defmacro with-query-results [ rows query & body ]
  `(let [stmt# (create-statement *conn*)]
    (try 
      (let [ ~rows (resultset-seq (execute-query stmt# ~query)) ]
        ~@body)
      (finally (close-statement stmt#)))
  )
)

(defn demo-connect []
  (def db-spec 
	  {:classname "com.mysql.jdbc.Driver"
	   :subprotocol "mysql"
	   :subname "//elecsprint.hoenn.pkmn:3306/clojure-test"
	   :user "clojure"
	   :password "clojurepass"})
  (with-connection db-spec
    (with-statement
      (execute! "DROP TABLE IF EXISTS tbl")
      (execute! "CREATE TABLE tbl (a INT)")
      (execute! "INSERT INTO tbl VALUES(1)"))
    (with-query-results rows "SELECT * FROM tbl"
      (println rows))
  )
)

(defn -main
  "I don't do a whole lot."
  []
  (demo-connect)
  (println "Hello, World!"))