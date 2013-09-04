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

(defn execute-update [ stmt & params]
  (dorun (map-indexed #(.setObject stmt (inc %1) %2) params))
  (.executeUpdate stmt))

(defn execute-query [ stmt query-string ]
  (.executeQuery stmt query-string))

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

(defmacro with-statement [ stmt query-string & body ]
  `(with-open [~stmt (prepare-statement ~query-string)]
    ~@body
  )
)

(defn execute! [ stmt & params ]
  (if (instance? java.sql.Statement stmt)
      (apply execute-update stmt params)
      (with-statement pstmt stmt (apply execute! pstmt params))))

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
    (with-statement stmt "INSERT INTO tbl VALUES(?)"
      (execute! stmt 100)
      (execute! stmt 200)
      (execute! stmt 300)
    )
  )
)

(defn low-level-demo-connect []
  (with-open [conn (open-connection db-spec)
              stmt (prepare-statement conn "INSERT INTO tbl VALUES(?)")]

    (execute! stmt 5)
    (execute! stmt 6)
    (execute! stmt 7)
    (execute! stmt 8)
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
