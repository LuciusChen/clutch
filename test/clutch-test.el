;;; clutch-test.el --- ERT tests for database workflows -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; ERT tests for the clutch user interface layer.
;;
;; Unit tests run without a database server.
;; Native live tests cover MySQL, PostgreSQL, MongoDB, and Redis.  The live
;; runner starts or reuses local containers, preferring Podman on Linux and
;; OrbStack-backed Docker on macOS:
;;   ./test/run-native-live-tests.sh
;;
;; Manual live setup:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 127.0.0.1:55306:3306 mysql:8
;;   docker run -d -e POSTGRES_INITDB_ARGS=--auth-host=md5 -e POSTGRES_PASSWORD=test -p 127.0.0.1:55432:5432 postgres:16 -c password_encryption=md5
;;   docker run -d -p 127.0.0.1:57017:27017 mongo:7
;;   docker run -d -p 127.0.0.1:56379:6379 redis:7-alpine
;;
;; Run unit tests from the repository root:
;;   emacs --batch -Q -L . -L test -L ../mysql.el -L ~/repos/pgsql.el \
;;     --eval '(setq load-prefer-newer t)' \
;;     -l ert -l clutch-test \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; Test configuration

(defvar clutch-column-displayers)

(defvar clutch--result-source-table)

(defvar clutch--connection-params)

(defvar clutch--result-server-pageable)

(defvar clutch--result-server-rewritable)

(defvar clutch--local-sort-original-rows)

(defvar clutch--local-sort-column-index)

(defvar tramp-rpc-use-controlmaster)

(declare-function make-clutch-jdbc-conn "clutch-db-jdbc" (&rest slot-value-pairs))

(declare-function make-mysql-conn "mysql" (&rest args))

(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))

(declare-function clutch-db-pg--make-connection "clutch-db-pg" (&rest args))

(defvar clutch-test-backend 'mysql)

(defvar clutch-test-host "127.0.0.1")

(defvar clutch-test-port 3306)

(defvar clutch-test-user "root")

(defvar clutch-test-password nil)

(defvar clutch-test-database "mysql")

(defvar clutch-test-url nil
  "Raw JDBC URL for generic live tests.")

(defvar clutch-test-display-name nil
  "Display name for generic JDBC live tests.")

(defvar clutch-test-props nil
  "JDBC connection properties for live tests.")

(require 'clutch-test-sql)
(require 'clutch-test-console)
(require 'clutch-test-object)
(require 'clutch-test-connection)
(require 'clutch-test-debug)
(require 'clutch-test-backends)
(require 'clutch-test-ui)
(require 'clutch-test-schema)
(require 'clutch-test-edit)
(require 'clutch-test-result)
(require 'clutch-test-query)
(require 'clutch-document)

;;;; Test backend matrix

(ert-deftest clutch-test-backend-matrix-selects-live-workflow-capabilities ()
  "Live backend matrix should replace hard-coded workflow backend lists."
  :tags '(:smoke)
  (let ((clutch-test-backend 'jdbc)
        (clutch-test-url "jdbc:duckdb:/tmp/clutch-test.duckdb"))
    (should (eq (clutch-test-live-backend-id) 'duckdb))
    (should (clutch-test-live-backend-capability-p :result-workflow))
    (should (clutch-test-live-backend-capability-p :updateable-workflow))
    (should-not
     (clutch-test-live-backend-capability-p :manual-savepoint)))
  (let ((clutch-test-backend 'clickhouse)
        (clutch-test-url nil))
    (should (eq (clutch-test-live-backend-id) 'clickhouse))
    (should (clutch-test-live-backend-capability-p :result-workflow))
    (should-not
     (clutch-test-live-backend-capability-p :updateable-workflow)))
  (should (clutch-test-live-backend-capability-p :object-describe 'mysql))
  (should (clutch-test-live-backend-capability-p :ctid-row-identity 'pg))
  (dolist (backend '(mysql pg sqlserver oracle))
    (should
     (clutch-test-live-backend-capability-p :manual-savepoint backend)))
  (should-not
   (clutch-test-live-backend-capability-p :manual-savepoint 'jdbc))
  (should-not (clutch-test-live-backend-capability-p :result-workflow
                                                     'mongodb))
  (should (string-match-p
           "MySQL/PostgreSQL"
           (clutch-test-capability-skip-message :object-describe))))

(provide 'clutch-test)

;;; clutch-test.el ends here
