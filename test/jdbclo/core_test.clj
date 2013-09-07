(ns jdbclo.core-test
  (:require [clojure.test :refer :all]
            [jdbclo.core :refer :all]))

;;
;; Test database
;;
(comment
(def db-spec 
 {:classname "com.mysql.jdbc.Driver"
  :subprotocol "mysql"
  :subname "//elecsprint.hoenn.pkmn:3306/clojure-test"
  :user "clojure"
  :password "clojurepass"})
)

(deftest test-connection
  (testing "low-level connection"
    (with-open [conn (open-connection db-spec)]
      (is (not= nil conn))))

  (testing "context-based connection"
    (with-connection db-spec
      (is (not= nil *conn*))))
)

(defn db-setup [] 
  (execute! "DROP PROCEDURE IF EXISTS doInsert")
  (execute! "DROP PROCEDURE IF EXISTS dblValue")
  (execute! "DROP TABLE IF EXISTS test_tbl")

  (execute! "CREATE TABLE test_tbl (a INT, b INT)")
  (execute! "CREATE PROCEDURE dblValue(IN a INT, OUT b BIGINT)
             BEGIN
               SET b = 2*a;
             END")
  (execute! "CREATE PROCEDURE doInsert(IN a INT, IN b INT)
             BEGIN
               INSERT INTO test_tbl VALUES (a,b);
             END")
)

(defn db-populate []
  (with-statement stmt "INSERT INTO test_tbl VALUES(?,?)"
    (doseq [i (range 1 10)] (execute! (bind stmt i (* 10 i)))))
)

(deftest test-requests
  (testing "basic requests"
    (with-connection db-spec
      (db-setup)

      (let [s1 (execute! "INSERT INTO test_tbl VALUES(1,10)")
            s2 (execute! "INSERT INTO test_tbl VALUES(2,20)")
            s3 (execute! "INSERT INTO test_tbl VALUES(3,30)")
            s4 (execute! "DELETE FROM test_tbl WHERE a < 3")]
        (is (= 1 s1))
        (is (= 1 s2))
        (is (= 1 s3))
        (is (= 2 s4)))
     ))

  (testing "prepared statements"
    (with-connection db-spec
      (db-setup)

      (with-statement stmt "INSERT INTO test_tbl VALUES(?,?)"
        (let [s1 (execute! (bind stmt 1 10))
              s2 (execute! (bind stmt 2 20))
              s3 (execute! (bind stmt 3 30))
              s4 (execute! "DELETE FROM test_tbl WHERE a < 3")]
          (is (= 1 s1))
          (is (= 1 s2))
          (is (= 1 s3))
          (is (= 2 s4)))
      )))

  (testing "simple queries"
    (with-connection db-spec
      (db-setup)
      (db-populate)

      (let [query "SELECT * FROM test_tbl ORDER BY a ASC"
            rows (with-query-results r query (doall r))]
        (is (= (count rows) 9))
        (is (= (first rows) { :a 1 :b 10 }))
      )))

  (testing "parameterized queries"
    (with-connection db-spec
      (db-setup)
      (db-populate)

      (let [query "SELECT * FROM test_tbl WHERE a > ? ORDER BY a ASC"]
        (with-statement stmt query
	  (bind stmt 4)
          (let [rows (with-query-results r stmt (doall r))]
            (is (= (count rows) 5))
            (is (= (first rows) { :a 5 :b 50 })))))
      ))

  (testing "Callable statement"
    (with-connection db-spec
      (db-setup)
      (let [query "{call doInsert(25,30)}"]
        (with-callable stmt query
          (execute! stmt)))
      (let [query "SELECT * FROM test_tbl ORDER BY a ASC"
            rows (with-query-results r query (doall r))]
        (is(= rows '({:a 25 :b 30}))))
  ))

  (testing "Callable statement with out param."
    (with-connection db-spec
      (db-setup)
      (let [query "{call dblValue(?,?)}"]
        (with-callable stmt query
          ; (.registerOutParameter stmt 1 'java.sql.Types.INTEGER)
          (bind stmt 25)
          (execute! stmt)
          (let [[p1 p2] (unbind stmt)]
	    (println (list p1 p2))
            (is (= p2 50)))))
  ))

  (testing "Output param. types"
    (with-connection db-spec
      (db-setup)
      (execute! "DROP PROCEDURE IF EXISTS paramTest")
      (execute! "CREATE PROCEDURE paramTest(OUT a CHAR(255),
					    OUT b VARCHAR(255),
					    OUT d NUMERIC(3,1),
					    OUT e DECIMAL(3,1),
					    OUT f BIT,
					    OUT g BOOLEAN,
					    OUT h TINYINT,
					    OUT i SMALLINT,
					    OUT j INTEGER,
					    OUT k BIGINT,
					    OUT l REAL,
					    OUT m FLOAT,
					    OUT n DOUBLE)
                 BEGIN
                   SET a = \"a\";
		   SET b = \"b\";
		   SET d = 1.2;
		   SET e = 1.2;
		   SET f = 1;
		   SET g = true;
		   SET h = 12;
		   SET i = 128;
		   SET j = 32768;
		   SET k = 4000000;
		   SET l = 1.2;
		   SET m = 1.2;
		   SET n = 1.2;
                 END")
      (let [query "{call paramTest(?,?,?,?,?,?,?,?,?,?,?,?,?)}"]
        (with-callable stmt query
          ; (.registerOutParameter stmt 1 'java.sql.Types.INTEGER)
          (execute! stmt)
          (let [[a b d e f g h i j k l m n] (unbind stmt)]
            (is (= a "a"))
            (is (= b "b"))
            (is (= d 1.2M))
            (is (= e 1.2M))
            (is (= f true))
            (is (= g true))
            (is (= h 12))
            (is (= i 128))
            (is (= j 32768))
            (is (= k 4000000))
            (is (= l 1.2))
            (is (= m (float 1.2)))
            (is (= n 1.2))
	  )))
  ))

  (testing "Output NULL"
    (with-connection db-spec
      (db-setup)
      (execute! "DROP PROCEDURE IF EXISTS nullOutTest")
      (execute! "CREATE PROCEDURE nullOutTest(OUT a CHAR(255))
                 BEGIN
                   SET a = NULL;
                 END")
      (let [query "{call nullOutTest(?)}"]
        (with-callable stmt query
          (execute! stmt)
          (let [[a] (unbind stmt)]
            (is (= a nil))
	  )))
  ))
)
