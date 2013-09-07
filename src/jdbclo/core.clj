(ns jdbclo.core
 (:import (java.sql DriverManager))
 (:require clojure.string))

(def ^:dynamic *conn* nil)

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

(defn execute-update [ stmt ]
  (.executeUpdate stmt))

(defn bind [ stmt & params ]
  (dorun (map-indexed #(.setObject stmt (inc %1) %2) params))
  stmt)

(defn execute-query [ stmt ]
  (.executeQuery stmt))

(defn close-statement [ stmt ]
  (.close stmt))

(defn close-connection [ conn ]
  (.close conn))

;;
;; To capture the current connection:
;;  (with-connection* db-spec (fn [conn] ( body )))
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

(defn callable-statement
  [ query-string ]
  (.prepareCall *conn* query-string))

(defmacro with-statement [ stmt query-string & body ]
  `(with-open [~stmt (prepare-statement ~query-string)]
    ~@body
  )
)

(defmacro with-callable [ stmt query-string & body ]
  `(with-open [~stmt (callable-statement ~query-string)]
    ~@body
  )
)

(defn execute! [ stmt-or-query-string ]
  (if (instance? java.sql.Statement stmt-or-query-string)
      (execute-update stmt-or-query-string)
      (with-statement pstmt stmt-or-query-string
        (execute! pstmt))))

(defmacro rollback []
 `(.rollback *conn*))

(defmacro commit []
 `(.commit *conn*))

(defmacro with-query-results* [ rows stmt & body ]
  `(let [ ~rows (resultset-seq (execute-query ~stmt)) ]
     ~@body))

(defmacro with-query-results [ rows query & body ]
  `(if (instance? java.sql.Statement ~query)
       (with-query-results* ~rows ~query ~@body)
       (with-statement stmt# ~query
         (with-query-results* ~rows stmt# ~@body))))

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
    (with-statement stmt "INSERT INTO tbl VALUES(?)"
      (execute! (bind stmt 100))
      (execute! (bind stmt 200))
      (execute! (bind stmt 300))
    )
    (with-statement stmt "SELECT * FROM tbl WHERE a > ?"
      (with-query-results rows (bind stmt 5)
        (println rows)
      )
    )
  )
)

(defn low-level-demo-connect []
  (with-open [conn (open-connection db-spec)
              stmt (prepare-statement conn "INSERT INTO tbl VALUES(?)")]

    (execute! (bind stmt 5))
    (execute! (bind stmt 6))
    (execute! (bind stmt 7))
    (execute! (bind stmt 8))
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
