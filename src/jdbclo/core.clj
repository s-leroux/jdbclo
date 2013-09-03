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

(defn prepare-statement 
  ( [ conn query-string ]
  (.prepareStatement conn query-string))
  ( [ query-string ]
  (.prepareStatement *conn* query-string))
)

(defn execute-update [ stmt query-string ]
  (.executeUpdate stmt query-string))

(defn execute-query [ stmt query-string ]
  (.executeQuery stmt query-string))

(defn close-statement [ stmt ]
  (.close stmt))

(defn close-connection [ conn ]
  (.close conn))

(defmacro with-connection* [ db-spec f & args ]
  `(with-open [conn# (open-connection ~db-spec)]
    (apply ~f conn# ~args)
  )
)

(defmacro with-connection [ db-spec & body ]
  `(with-connection* ~db-spec (fn [conn#] (binding [*conn* conn#]  ~@body)))
)

(defmacro with-named-connection [ conn db-spec & body ]
  `(with-connection ~db-spec 
     (let [~conn *conn*]
       ~@body
     )
  )
)

(defmacro using-connection [ conn & body ]
  `(binding[*conn* ~conn] ~@body))

(defn execute! [ query-string ]
  (with-open [stmt (create-statement *conn*)]
    (execute-update stmt query-string)))

(defn execute-using! [ stmt & params ]
  (doall (map-indexed #(.setObject stmt (inc %1) %2) params))
  (.executeUpdate stmt)
)

(defmacro rollback []
 `(.rollback *conn*))

(defmacro commit []
 `(.commit *conn*))

(defmacro with-query-results [ rows query & body ]
  `(let [stmt# (create-statement *conn*)]
    (try 
      (let [ ~rows (resultset-seq (execute-query stmt# ~query)) ]
        ~@body)
      (finally (close-statement stmt#)))
  )
)

(defmacro with-transaction [ & body ]
  `(try
    (do
      (.setAutoCommit *conn* false)
      ~@body
      (commit)
    )
    (finally (rollback)))
)

(def db-spec 
 {:classname "com.mysql.jdbc.Driver"
  :subprotocol "mysql"
  :subname "//elecsprint.hoenn.pkmn:3306/clojure-test"
  :user "clojure"
  :password "clojurepass"})

(defn demo-connect []
  (with-connection db-spec
    (execute! "DROP TABLE IF EXISTS tbl")
    (execute! "CREATE TABLE tbl (a INT)")
    (execute! "INSERT INTO tbl VALUES(1)")
    (with-transaction
      (execute! "INSERT INTO tbl VALUES(2)")
      (rollback)
      (execute! "INSERT INTO tbl VALUES(3)")
    )
    (with-query-results rows "SELECT * FROM tbl"
      (println rows))
  )
)

(defn low-level-demo-connect []
  (with-open [conn (open-connection db-spec)
              stmt (prepare-statement conn "INSERT INTO tbl VALUES(?)")]

    (execute-using! stmt 5)
    (execute-using! stmt 6)
    (execute-using! stmt 7)
    (execute-using! stmt 8)
  )
)

(defn double-connect-demo []
  (with-open [c1 (open-connection db-spec)
              c2 (open-connection db-spec)]
    (using-connection c1
       (execute! "INSERT INTO tbl VALUES(10)")
    )
    (using-connection c2
       (execute! "INSERT INTO tbl VALUES(20)")
    )

    (using-connection c1
      (with-query-results rows "SELECT * FROM tbl"
        (println rows))
    )
    (using-connection c2
      (with-query-results rows "SELECT * FROM tbl"
        (println rows))
    )
  )
)

(defn -main
  "I don't do a whole lot."
  []
  (demo-connect)
  (low-level-demo-connect)
  (double-connect-demo)
  (println "Hello, World!"))
