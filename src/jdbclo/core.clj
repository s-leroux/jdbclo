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

;;
;; High-level objects are handled using with-/using- functions
;;

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

(defn prepare-statement 
  ( [ conn query-string ]
  (.prepareStatement conn query-string))
  ( [ query-string ]
  (.prepareStatement *conn* query-string))
)

(defmacro with-prepared-statement [ query-string & body ]
  `(with-open [stmt# (prepare-statement ~query-string)]
    (using-prepared-statement stmt# ~@body)
  )
)

(defmacro using-prepared-statement [ stmt & body ]
  `(binding[*stmt* ~stmt] ~@body))

(defn execute! [ query-string ]
  (with-open [stmt (create-statement *conn*)]
    (execute-update stmt query-string)))

(defn execute-using! [ & params ]
  (doall (map-indexed #(.setObject *stmt* (inc %1) %2) params))
  (.executeUpdate *stmt*)
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

(defn prepared-statement-demo []
  (with-connection db-spec
    (with-prepared-statement "INSERT INTO tbl VALUES(?)"
      (execute-using! 100)
      (execute-using! 200)
      (execute-using! 300)
    )
  )
)

(defn low-level-demo-connect []
  (with-open [conn (open-connection db-spec)
              stmt (prepare-statement conn "INSERT INTO tbl VALUES(?)")]

    (using-prepared-statement stmt
      (execute-using! 5)
      (execute-using! 6)
      (execute-using! 7)
      (execute-using! 8)
    )
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
  (prepared-statement-demo)
  (double-connect-demo)
  (println "Hello, World!"))
