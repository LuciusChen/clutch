;;; clutch-db-test.el --- ERT tests for database backends -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; ERT tests for the clutch-db generic database interface.
;;
;; Unit tests run without a database server.
;; Native live tests require MySQL, PostgreSQL, MongoDB, and Redis.  The live runner
;; starts or reuses local containers, preferring Podman on Linux and OrbStack-backed
;; Docker on macOS:
;;   ./test/run-native-live-tests.sh
;;
;; Manual live setup:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 127.0.0.1:55306:3306 mysql:8
;;   docker run -d -e POSTGRES_INITDB_ARGS=--auth-host=md5 -e POSTGRES_PASSWORD=test -p 127.0.0.1:55432:5432 postgres:16 -c password_encryption=md5
;;   docker run -d -p 127.0.0.1:57017:27017 mongo:7
;;   docker run -d -p 127.0.0.1:56379:6379 redis:7-alpine
;;
;; Note: MySQL 8 defaults to `caching_sha2_password'.  The native mysql
;; client retries with TLS when the server requires a secure channel; local
;; container certificates are typically self-signed, so the MySQL live helpers
;; bind `mysql-tls-verify-server' to nil unless the test environment installs a
;; trusted CA.
;;
;; Run unit tests from the repository root:
;;   emacs --batch -Q -L . -L test -L ../mysql.el -L ../pg-el \
;;     --eval '(setq load-prefer-newer t)' \
;;     -l ert -l clutch-db-test \
;;     -f ert-run-tests-batch-and-exit
;;
;; Prefer ./test/run-native-live-tests.sh for native live coverage.

;;; Code:

(require 'cl-lib)
(require 'ert)
(require 'eieio)
(require 'clutch-backend)
(require 'clutch-connection)
(require 'clutch-db-jdbc)
(require 'clutch-mongodb)
(require 'clutch-redis)
(require 'redis)
(require 'mongodb)

(eval-when-compile
  (require 'pg))

(defvar clutch-debug-buffer-name "*clutch-debug*")
;; `mysql-tls-verify-server' is defined in mysql.el; declare it
;; special here so local test bindings remain dynamic even before the backend
;; requires mysql.el.
(defvar mysql-tls-verify-server)
(defvar clutch-connection)
(defvar clutch-jdbc--error-details-by-conn)
(defvar clutch-jdbc--busy-request-ids)
(defvar clutch-jdbc--ignored-response-ids)
(defvar clutch-db--foreground-connections)
(defvar mysql-type-long)
(defvar mysql-type-float)
(defvar mysql-type-double)
(defvar mysql-type-decimal)
(defvar mysql-type-longlong)
(defvar mysql-type-date)
(defvar mysql-type-time)
(defvar mysql-type-datetime)
(defvar mysql-type-timestamp)
(defvar mysql-type-blob)
(defvar mysql-type-json)
(defvar clutch-db-mysql--connection-params)
(defvar clutch-db-mysql-cancel-timeout-seconds)
(defvar clutch-db-pg--oid-bool)
(defvar clutch-db-test--live-name-counter 0
  "Counter used to generate isolated live database object names.")
(declare-function clutch-db-mysql--type-category "clutch-db-mysql" (type character-set))
(declare-function clutch-db-mysql--convert-columns "clutch-db-mysql" (columns))
(declare-function clutch-db-mysql-connect "clutch-db-mysql" (params))
(declare-function clutch-db-pg--ctid-identity "clutch-db-pg" (conn table))
(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))
(declare-function clutch-db-pg--convert-columns "clutch-db-pg" (columns))
(declare-function clutch-db-pg--wrap-result "clutch-db-pg" (result))
(declare-function clutch-db-pg-connect "clutch-db-pg" (params))
(declare-function clutch-db-sqlite-connect "clutch-db-sqlite" (params))
(declare-function clutch--jdbc-backend-p "clutch-connection" (backend))
(declare-function clutch--manual-backend-choices "clutch-connection" ())
(declare-function clutch--backend-display-name-from-params "clutch-connection" (params))
(declare-function clutch--build-conn "clutch-connection" (params))
(declare-function clutch--format-value "clutch-ui" (value))
(declare-function clutch-db-value-to-literal "clutch-backend"
                  (conn value &optional fallback-format-fn))
(declare-function make-mysql-conn "mysql" (&rest args))
(declare-function make-mysql-result "mysql" (&rest args))
(declare-function mysql-current-database "mysql" (conn))
(declare-function mysql-connection-id "mysql" (conn))
(declare-function make-pgcon "pg" (&rest args))
(declare-function make-pgresult "pg" (&rest args))
(declare-function pgcon-connect-plist "pg" (conn))
(declare-function pgcon-dbname "pg" (conn))
(declare-function sqlite-available-p "sqlite" ())

;;;; Test configuration

(defvar clutch-db-test-mysql-host "127.0.0.1")
(defvar clutch-db-test-mysql-port 3306)
(defvar clutch-db-test-mysql-user "root")
(defvar clutch-db-test-mysql-password nil)
(defvar clutch-db-test-mysql-database "mysql")

(defvar clutch-db-test-pg-host "127.0.0.1")
(defvar clutch-db-test-pg-port 5432)
(defvar clutch-db-test-pg-user "postgres")
(defvar clutch-db-test-pg-password nil)
(defvar clutch-db-test-pg-database "postgres")

(defvar clutch-db-test-mongodb-live-enabled nil
  "Non-nil enables native MongoDB live tests.")
(defvar clutch-db-test-mongodb-url nil
  "MongoDB URI for native MongoDB live tests.")
(defvar clutch-db-test-mongodb-host "127.0.0.1"
  "Host for native MongoDB live tests.")
(defvar clutch-db-test-mongodb-port 27017
  "Port for native MongoDB live tests.")
(defvar clutch-db-test-mongodb-user nil
  "User for native MongoDB live tests.")
(defvar clutch-db-test-mongodb-password nil
  "Password for native MongoDB live tests.")
(defvar clutch-db-test-mongodb-database "clutch_test"
  "Database name for native MongoDB live tests.")
(defvar clutch-db-test-mongodb-auth-database nil
  "Authentication database for native MongoDB live tests.")
(defvar clutch-db-test-mongodb-props nil
  "Additional URI query properties for native MongoDB live tests.")

(defvar clutch-db-test-redis-live-enabled nil
  "Non-nil enables Redis live tests.")
(defvar clutch-db-test-redis-host "127.0.0.1"
  "Host for Redis live tests.")
(defvar clutch-db-test-redis-port 6379
  "Port for Redis live tests.")
(defvar clutch-db-test-redis-user nil
  "User for Redis live tests.")
(defvar clutch-db-test-redis-password nil
  "Password for Redis live tests.")
(defvar clutch-db-test-redis-database 0
  "Logical database for Redis live tests.")

(defconst clutch-db-test--pg-oid-bytea 17)
(defconst clutch-db-test--pg-oid-int8 20)
(defconst clutch-db-test--pg-oid-int4 23)
(defconst clutch-db-test--pg-oid-json 114)
(defconst clutch-db-test--pg-oid-float8 701)
(defconst clutch-db-test--pg-oid-date 1082)
(defconst clutch-db-test--pg-oid-time 1083)
(defconst clutch-db-test--pg-oid-timestamp 1114)
(defconst clutch-db-test--pg-oid-timestamptz 1184)
(defconst clutch-db-test--pg-oid-numeric 1700)
(defconst clutch-db-test--pg-oid-jsonb 3802)

(defun clutch-db-test--make-pgcon (&rest params)
  "Return a lightweight upstream `pgcon' built from PARAMS."
  (require 'pg)
  (let* ((dbname (or (plist-get params :database)
                     (plist-get params :dbname)
                     "test"))
         (conn (make-pgcon :dbname dbname :process nil)))
    (oset conn connect-plist
          (list 'method :tcp
                'host (or (plist-get params :host) "localhost")
                'port (or (plist-get params :port) 5432)
                'dbname dbname
                'user (plist-get params :user)
                'password (plist-get params :password)))
    conn))

(defun clutch-db-test--live-name (prefix)
  "Return an isolated live database object name using PREFIX."
  (format "%s_%d_%d"
          prefix
          (emacs-pid)
          (cl-incf clutch-db-test--live-name-counter)))

(defmacro clutch-db-test--with-local-mysql-tls (&rest body)
  "Run BODY with MySQL TLS verification disabled for local self-signed certs."
  (declare (indent 0))
  `(progn
     (require 'mysql)
     (cl-letf (((symbol-value 'mysql-tls-verify-server) nil))
       ,@body)))

(defmacro clutch-db-test--with-immediate-idle-metadata (scheduled-var &rest body)
  "Run BODY with idle metadata work executing immediately.
Capture the scheduled idle timer shape in SCHEDULED-VAR and treat the
connection as live and not busy."
  (declare (indent 1))
  `(cl-letf (((symbol-function 'run-with-idle-timer)
              (lambda (secs repeat fn &rest args)
                (setq ,scheduled-var (list secs repeat))
                (apply fn args)
                'fake-timer))
             ((symbol-function 'clutch-db-busy-p)
              (lambda (_conn) nil))
             ((symbol-function 'clutch-db-live-p)
              (lambda (_conn) t)))
     ,@body))

;;;; Unit tests — connection parameter normalization

(ert-deftest clutch-db-test-normalize-mysql-connect-params-canonicalizes-disabled ()
  "MySQL plaintext options should normalize to `:ssl-mode disabled'."
  (let ((params (clutch-db--normalize-connect-params
                 'mysql
                 '(:host "127.0.0.1" :tls nil :ssl-mode off))))
    (should (eq (plist-get params :clutch-tls-mode) 'disable))
    (should (eq (plist-get params :ssl-mode) 'disabled))
    (should-not (plist-member params :tls))))

(ert-deftest clutch-db-test-normalize-mysql-connect-params-rejects-conflict ()
  "MySQL conflicting TLS options should fail early."
  (should-error
   (clutch-db--normalize-connect-params
    'mysql '(:host "127.0.0.1" :tls t :ssl-mode disabled))
   :type 'clutch-db-error))

(ert-deftest clutch-db-test-normalize-pg-connect-params-canonicalizes-require ()
  "PostgreSQL `:tls t' should normalize to `:sslmode require'."
  (let ((params (clutch-db--normalize-connect-params
                 'pg '(:host "127.0.0.1" :tls t))))
    (should (eq (plist-get params :clutch-tls-mode) 'require))
    (should (eq (plist-get params :sslmode) 'require))
    (should-not (plist-member params :tls))))

(ert-deftest clutch-db-test-normalize-pg-connect-params-preserves-prefer ()
  "PostgreSQL `:sslmode prefer' should stay in official form."
  (let ((params (clutch-db--normalize-connect-params
                 'pg '(:host "127.0.0.1" :sslmode "prefer"))))
    (should (eq (plist-get params :clutch-tls-mode) 'prefer))
    (should (eq (plist-get params :sslmode) 'prefer))
    (should-not (plist-member params :tls))))

(ert-deftest clutch-db-test-normalize-pg-connect-params-rejects-unsupported-mode ()
  "Unsupported PostgreSQL sslmodes should fail early."
  (should-error
   (clutch-db--normalize-connect-params
    'pg '(:host "127.0.0.1" :sslmode verify-ca))
   :type 'clutch-db-error))

(ert-deftest clutch-db-test-normalize-connect-params-rejects-removed-read-timeout ()
  "Removed connection timeout aliases should fail before reaching adapters."
  (should-error
   (clutch-db--normalize-connect-params
    'mysql '(:host "127.0.0.1" :read-timeout 5))
   :type 'user-error))

(ert-deftest clutch-db-test-normalize-connect-params-dispatches-registry-function ()
  "Connection parameter normalization should come from backend registry metadata."
  (let ((clutch-backend--registry
         '((alpha . (:normalize-fn clutch-db-test--alpha-normalize))
           (beta . (:normalize-fn clutch-db-test--beta-normalize)))))
    (cl-letf (((symbol-function 'clutch-db-test--alpha-normalize)
               (lambda (params)
                 (append params '(:normalized-by alpha))))
              ((symbol-function 'clutch-db-test--beta-normalize)
               (lambda (_params)
                 (ert-fail "Called the wrong backend normalizer"))))
      (should (equal (clutch-db--normalize-connect-params
                      'alpha '(:database "app"))
                     '(:database "app" :normalized-by alpha))))))

(ert-deftest clutch-db-test-native-document-surface-p-uses-surface-aliases ()
  "Native document surface detection should exclude SQL Interface aliases."
  (cl-letf (((symbol-function 'clutch-db-backend-key)
             (lambda (_conn) 'mongodb)))
    (should (clutch-db-native-document-surface-p 'mongo-conn nil))
    (should-not
     (clutch-db-native-document-surface-p
      'mongo-conn '(:surface "sql-interface")))))

(ert-deftest clutch-db-test-redis-registry-uses-key-value-model ()
  "Redis should be registered as a basic key/value backend."
  (should (eq (clutch-backend-support-level 'redis) 'basic))
  (should (eq (clutch-backend-data-model 'redis) 'key-value))
  (should (eq (clutch-backend-query-mode 'redis) #'clutch-redis-mode))
  (should (= (clutch-backend-default-port 'redis) 6379)))

(ert-deftest clutch-db-test-redis-query-maps-hgetall-to-pair-grid ()
  "Redis HGETALL responses should render as field/value rows."
  (let ((conn (make-clutch-redis-conn :client 'redis-client)))
    (cl-letf (((symbol-function 'redis-command)
               (lambda (client command &rest args)
                 (should (eq client 'redis-client))
                 (should (equal command "HGETALL"))
                 (should (equal args '("user:1")))
                 (mapcar (lambda (text)
                           (encode-coding-string text 'utf-8 t))
                         '("name" "Ada" "tier" "pro")))))
      (let* ((result (clutch-db-query conn "HGETALL user:1"))
             (columns (clutch-db-result-column-names
                       (clutch-db-result-columns result))))
        (should (equal columns '("field" "value")))
        (should (equal (clutch-db-result-rows result)
                       '(("name" "Ada") ("tier" "pro"))))))))

(ert-deftest clutch-db-test-redis-query-maps-zrange-by-withscores ()
  "Redis ZRANGE should render scores only when WITHSCORES is present."
  (let ((conn (make-clutch-redis-conn :client 'redis-client)))
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command &rest args)
                 (should (equal command "ZRANGE"))
                 (pcase args
                   (`("leaders" "0" "-1")
                    (mapcar (lambda (text)
                              (encode-coding-string text 'utf-8 t))
                            '("ada" "grace")))
                   (`("leaders" "0" "-1" "WITHSCORES")
                    (mapcar (lambda (text)
                              (encode-coding-string text 'utf-8 t))
                            '("ada" "10" "grace" "8")))
                   (_ (ert-fail "Unexpected ZRANGE arguments"))))))
      (let ((result (clutch-db-query conn "ZRANGE leaders 0 -1")))
        (should (equal (clutch-db-result-column-names
                        (clutch-db-result-columns result))
                       '("index" "value")))
        (should (equal (clutch-db-result-rows result)
                       '((0 "ada") (1 "grace")))))
      (let ((result (clutch-db-query conn
                                     "ZRANGE leaders 0 -1 WITHSCORES")))
        (should (equal (clutch-db-result-column-names
                        (clutch-db-result-columns result))
                       '("member" "score")))
        (should (equal (clutch-db-result-rows result)
                       '(("ada" "10") ("grace" "8"))))))))

(ert-deftest clutch-db-test-redis-scan-keys-iterates-cursors ()
  "Redis key discovery should use SCAN until cursor returns to zero."
  (let ((conn (make-clutch-redis-conn :client 'redis-client))
        calls)
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command cursor &rest args)
                 (push (list command cursor args) calls)
                 (pcase cursor
                   ("0" (list (encode-coding-string "7" 'utf-8 t)
                              (list (encode-coding-string "alpha" 'utf-8 t))))
                   ("7" (list (encode-coding-string "0" 'utf-8 t)
                              (list (encode-coding-string "beta" 'utf-8 t))))
                   (_ (ert-fail "Unexpected cursor"))))))
      (should (equal (clutch-db-list-tables conn) '("alpha" "beta")))
      (should (equal (nreverse calls)
                     '(("SCAN" "0" ("COUNT" 1000))
                       ("SCAN" "7" ("COUNT" 1000))))))))

(ert-deftest clutch-db-test-redis-key-entry-metadata-includes-value-type ()
  "Redis key metadata should include the value type for annotations."
  (let ((conn (make-clutch-redis-conn :client (make-redis-conn))))
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command &rest args)
                 (pcase (cons command args)
                   (`("SCAN" "0" "COUNT" 1000)
                    (list (encode-coding-string "0" 'utf-8 t)
                          (mapcar (lambda (text)
                                    (encode-coding-string text 'utf-8 t))
                                  '("user:1" "jobs"))))
                   (`("TYPE" "user:1")
                    (encode-coding-string "hash" 'utf-8 t))
                   (`("TYPE" "jobs")
                    (encode-coding-string "list" 'utf-8 t))
                   (_ (ert-fail "Unexpected Redis command"))))))
      (let ((entries (clutch-db-list-table-entries conn)))
        (should (equal entries
                       '((:name "user:1" :schema "0" :type "KEY")
                         (:name "jobs" :schema "0" :type "KEY"))))
        (should (equal (clutch-db-object-entry-metadata conn (car entries))
                       '(:name "user:1" :schema "0" :type "KEY"
                         :value-type "HASH")))
        (should (equal (clutch-db-object-entry-metadata conn (cadr entries))
                       '(:name "jobs" :schema "0" :type "KEY"
                         :value-type "LIST")))))))

(ert-deftest clutch-db-test-redis-object-browse-query-is-type-aware ()
  "Redis key browsing should choose a read command from the key type."
  (let ((conn (make-clutch-redis-conn :client 'redis-client)))
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command key)
                 (should (equal command "TYPE"))
                 (should (equal key "user:1"))
                 (encode-coding-string "hash" 'utf-8 t))))
      (should (equal (clutch-db-object-browse-query
                      conn '(:name "user:1" :type "KEY"))
                     "HGETALL \"user:1\"")))))

;;;; Unit tests — clutch-db-result struct

(ert-deftest clutch-db-test-result-struct ()
  "Test clutch-db-result struct creation and accessors."
  (let ((result (make-clutch-db-result
                 :connection 'fake-conn
                 :columns '((:name "id" :type-category numeric)
                            (:name "name" :type-category text))
                 :rows '((1 "alice") (2 "bob"))
                 :affected-rows 2
                 :last-insert-id 42
                 :warnings 0)))
    (should (clutch-db-result-p result))
    (should (eq (clutch-db-result-connection result) 'fake-conn))
    (should (= (length (clutch-db-result-columns result)) 2))
    (should (= (length (clutch-db-result-rows result)) 2))
    (should (= (clutch-db-result-affected-rows result) 2))
    (should (= (clutch-db-result-last-insert-id result) 42))
    (should (= (clutch-db-result-warnings result) 0))))

(ert-deftest clutch-db-test-result-empty ()
  "Test clutch-db-result with empty/nil values."
  (let ((result (make-clutch-db-result
                 :columns '((:name "v" :type-category numeric))
                 :rows nil
                 :affected-rows 0)))
    (should (clutch-db-result-p result))
    (should (null (clutch-db-result-connection result)))
    (should (null (clutch-db-result-rows result)))
    (should (= (clutch-db-result-affected-rows result) 0))
    (should (null (clutch-db-result-last-insert-id result)))))

(ert-deftest clutch-db-test-jdbc-fetch-all-preserves-row-order ()
  "JDBC fetch-all should preserve batch order while avoiding repeated tail scans."
  (let ((batches '((:rows (("alpha" 17) ("beta" 23)) :done nil)
                   (:rows (("omega" -9)) :done t)))
        (conn (make-clutch-jdbc-conn :params '(:rpc-timeout 9))))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn op params &optional timeout-seconds)
                 (should (equal op "fetch"))
                 (should (= (alist-get 'cursor-id params) 9))
                 (should (= timeout-seconds 9))
                 (pop batches))))
      (should (equal (clutch-jdbc--fetch-all conn 9)
                     '(("alpha" 17) ("beta" 23) ("omega" -9)))))))

(ert-deftest clutch-db-test-jdbc-fetch-all-maps-query-and-rpc-timeouts ()
  "JDBC fetch-all should pass the same effective query timeout as execute."
  (let ((conn (make-clutch-jdbc-conn :params '(:rpc-timeout 15
                                               :query-timeout 16)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:rows nil :done t))))
      (should (equal (clutch-jdbc--fetch-all conn 9) nil))
      (should (equal captured-op "fetch"))
      (should (= (alist-get 'cursor-id captured-params) 9))
      (should (= captured-timeout 15))
      ;; min(16, max(1, 15-5)) = min(16, 10) = 10
      (should (= (alist-get 'query-timeout-seconds captured-params) 10)))))

(ert-deftest clutch-db-test-jdbc-referencing-objects-maps-rpc-response ()
  "JDBC reverse-reference lookup should map RPC rows to object entries."
  (let (captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout-seconds)
                 (setq captured-op op
                       captured-params params)
                 '(:objects ((:name "SALES_ORDERS" :schema "APP"
                              :source-schema "APP_OWNER")
                             (:name "INVOICE_ITEMS" :schema "BILLING"))))))
      (should (equal
               (clutch-db-referencing-objects
                (make-clutch-jdbc-conn :conn-id 7 :params '(:backend oracle :schema "APP"))
                "CUSTOMERS")
               '((:name "SALES_ORDERS" :type "TABLE"
                  :schema "APP" :source-schema "APP_OWNER")
                 (:name "INVOICE_ITEMS" :type "TABLE"
                  :schema "BILLING" :source-schema "BILLING"))))
      (should (equal captured-op "get-referencing-objects"))
      (should (= (alist-get 'conn-id captured-params) 7))
      (should (equal (alist-get 'table captured-params) "CUSTOMERS"))
      (should (equal (alist-get 'schema captured-params) "APP")))))

(ert-deftest clutch-db-test-jdbc-connect-maps-timeouts ()
  "JDBC connect should map explicit timeout phases to the agent call."
  (let ((clutch-jdbc-oracle-manual-commit t)
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 7))))
      (let ((conn (clutch-db-jdbc-connect
                   'oracle
                   '(:host "db"
                     :port 1521
                     :database "svc"
                     :user "scott"
                     :password "tiger"
                     :connect-timeout 7
                     :read-idle-timeout 23
                     :rpc-timeout 41))))
        (should (equal captured-op "connect"))
        (should (= captured-timeout 41))
        (should (equal (alist-get 'driver-class captured-params)
                       "oracle.jdbc.OracleDriver"))
        (should (eq (alist-get 'auto-commit captured-params) clutch-jdbc--json-false))
        (should (= (alist-get 'connect-timeout-seconds captured-params) 7))
        (should (= (alist-get 'network-timeout-seconds captured-params) 23))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout) 41))
        (should (= (clutch-jdbc-conn-conn-id conn) 7))))))

(ert-deftest clutch-db-test-jdbc-connect-non-oracle-sends-autocommit-true ()
  "Non-Oracle JDBC connect should keep auto-commit enabled by default."
  (let (captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 8))))
      (clutch-db-jdbc-connect
       'sqlserver
       '(:host "db" :port 1433 :database "app" :user "sa" :password "secret"))
      (should (equal (alist-get 'driver-class captured-params)
                     "com.microsoft.sqlserver.jdbc.SQLServerDriver"))
      (should (eq (alist-get 'auto-commit captured-params) t)))))

(ert-deftest clutch-db-test-jdbc-connect-sql-interface-mongodb-contract ()
  "MongoDB SQL Interface JDBC should be a surface on the MongoDB backend."
  (let (captured-params conn)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 10))))
      (setq conn
            (clutch-db-jdbc-connect
             'mongodb
             '(:host "cluster0.a.query.mongodb.net"
               :port 27017
               :database "analytics"
               :surface sql-interface
               :auth-database "admin"
               :user "reporter"
               :password "secret"
               :props (("loglevel" . "SEVERE")))))
      (should (equal (alist-get 'url captured-params)
                     "jdbc:mongodb://cluster0.a.query.mongodb.net:27017/admin"))
      (should (equal (alist-get 'driver-class captured-params)
                     "com.mongodb.jdbc.MongoDriver"))
      (should (equal (alist-get 'props captured-params)
                     '(("database" . "analytics")
                       ("loglevel" . "SEVERE"))))
      (should (eq (alist-get 'auto-commit captured-params) t))
      (should (eq (clutch-jdbc-conn-driver conn) 'mongodb))
      (should (eq (plist-get (clutch-jdbc-conn-params conn) :driver)
                  'mongodb))
      (should (eq (plist-get (clutch-jdbc-conn-params conn) :surface)
                  'sql-interface))
      (should (eq (clutch-db-backend-key conn) 'mongodb))
      (should (equal (clutch-db-display-name conn) "MongoDB"))
      (setq captured-params nil)
      (clutch-db-jdbc-connect
       'mongodb
       '(:url "jdbc:mongodb://cluster0.a.query.mongodb.net/admin"
         :database "analytics"
         :surface sql-interface
         :user "reporter"
         :password "secret"))
      (should (equal (alist-get 'props captured-params)
                     '(("database" . "analytics"))))
      (should (equal (alist-get 'driver-class captured-params)
                     "com.mongodb.jdbc.MongoDriver"))
      (should (eq (alist-get 'auto-commit captured-params) t))
      (let ((err (should-error
                  (clutch-db-jdbc-connect
                   'mongodb
                   '(:host "127.0.0.1"
                     :port 27017
                     :database "app"))
                  :type 'clutch-db-error)))
        (should (string-match-p "native mongodb backend"
                                (error-message-string err))))
      (let ((err (should-error
                  (clutch-db-jdbc-connect
                   'mongodb
                   '(:host "cluster0.a.query.mongodb.net"
                     :surface sql-interface
                     :user "reporter"))
                  :type 'clutch-db-error)))
        (should (string-match-p "require :database"
                                (error-message-string err)))))))

(ert-deftest clutch-db-test-jdbc-connect-oracle-global-autocommit-override ()
  "Oracle connect should honor the global manual-commit default override."
  (let ((clutch-jdbc-oracle-manual-commit nil)
        captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 9))))
      (clutch-db-jdbc-connect
       'oracle
       '(:host "db" :port 1521 :database "svc" :user "scott" :password "tiger"))
      (should (equal (alist-get 'driver-class captured-params)
                     "oracle.jdbc.OracleDriver"))
      (should (eq (alist-get 'auto-commit captured-params) t)))))

(ert-deftest clutch-db-test-jdbc-connect-defaults-connect-timeout-separately-from-rpc ()
  "JDBC connect should keep default timeout phases separate."
  (let ((clutch-connect-timeout-seconds 10)
        (clutch-read-idle-timeout-seconds 30)
        (clutch-query-timeout-seconds 20)
        (clutch-jdbc-rpc-timeout-seconds 41)
        captured-timeout captured-params conn)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional timeout-seconds)
                 (setq captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 7))))
      (setq conn
            (clutch-db-jdbc-connect
             'oracle
             '(:host "db" :port 1521 :database "svc" :user "scott" :password "tiger")))
      (should (= captured-timeout 41))
      (should (equal (alist-get 'driver-class captured-params)
                     "oracle.jdbc.OracleDriver"))
      (should (= (alist-get 'connect-timeout-seconds captured-params) 10))
      (should (= (alist-get 'network-timeout-seconds captured-params) 30))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :connect-timeout) 10))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :read-idle-timeout) 30))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :query-timeout) 20))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout) 41)))))

(ert-deftest clutch-db-test-jdbc-query-maps-query-and-rpc-timeouts ()
  "JDBC query clamps query-timeout to rpc-timeout - 5 when it exceeds the margin."
  (let ((conn (make-clutch-jdbc-conn :conn-id 4
                                     :params '(:rpc-timeout 15
                                               :query-timeout 16)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:type "dml" :affected-rows 1))))
      (let ((result (clutch-db-query conn "delete from t")))
        (should (equal captured-op "execute"))
        (should (= captured-timeout 15))
        ;; min(16, max(1, 15-5)) = min(16, 10) = 10
        (should (= (alist-get 'query-timeout-seconds captured-params) 10))
        (should (= (clutch-db-result-affected-rows result) 1))))))

(ert-deftest clutch-db-test-jdbc-query-does-not-clamp-when-within-margin ()
  "JDBC query should not clamp query-timeout when it fits within rpc-timeout - 5."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:rpc-timeout 15
                                               :query-timeout 8)))
        captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn _op params &optional _timeout)
                 (setq captured-params params)
                 '(:type "dml" :affected-rows 0))))
      (clutch-db-query conn "delete from t where 1=0")
      ;; min(8, max(1, 10)) = min(8, 10) = 8 — no clamping
      (should (= (alist-get 'query-timeout-seconds captured-params) 8)))))

(ert-deftest clutch-db-test-jdbc-query-clamps-query-timeout-to-rpc-minus-five ()
  "JDBC query should clamp query-timeout to rpc-timeout - 5 in the default case."
  (let ((conn (make-clutch-jdbc-conn :conn-id 6
                                     :params '(:rpc-timeout 30
                                               :query-timeout 30)))
        captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn _op params &optional _timeout)
                 (setq captured-params params)
                 '(:type "dml" :affected-rows 1))))
      (clutch-db-query conn "update t set x = 1")
      ;; min(30, max(1, 25)) = min(30, 25) = 25
      (should (= (alist-get 'query-timeout-seconds captured-params) 25)))))

(ert-deftest clutch-db-test-jdbc-manual-commit-p-oracle ()
  "Oracle JDBC connections should default to manual-commit mode."
  (let ((clutch-jdbc-oracle-manual-commit t)
        (conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott"))))
    (should (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-manual-commit-p-sqlserver ()
  "Non-Oracle JDBC connections should default to auto-commit mode."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver sqlserver :user "sa"))))
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-manual-commit-unsupported-for-mongodb ()
  "MongoDB SQL Interface JDBC should not expose Clutch manual-commit controls."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 21
               :driver 'mongodb
               :params '(:driver mongodb
                         :surface sql-interface
                         :manual-commit t
                         :rpc-timeout 12))))
    (should-not (clutch-db-manual-commit-supported-p conn))
    (should-not (clutch-db-manual-commit-p conn))
    (should-error (clutch-db-set-auto-commit conn nil) :type 'user-error)
    (should-error (clutch-db-commit conn) :type 'user-error)
    (should-error (clutch-db-rollback conn) :type 'user-error)))

(ert-deftest clutch-db-test-jdbc-connect-sql-interface-mongodb-rejects-manual-commit ()
  "MongoDB SQL Interface surface should reject explicit :manual-commit at connect time."
  (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
            ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore))
    (should-error
     (clutch-db-jdbc-connect
      'mongodb
      '(:host "cluster0.a.query.mongodb.net"
        :database "analytics"
        :surface sql-interface
        :manual-commit t))
     :type 'clutch-db-error)))

(ert-deftest clutch-db-test-jdbc-manual-commit-p-oracle-global-override ()
  "Oracle JDBC connections should respect the global default override."
  (let ((clutch-jdbc-oracle-manual-commit nil)
        (conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott"))))
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-fallback-set-auto-commit-errors-consistently ()
  "Backends without manual commit support should use the public error wording."
  (let ((err (should-error (clutch-db-set-auto-commit 'opaque-conn nil)
                           :type 'user-error)))
    (should (string-match-p "Manual commit is not supported by this connection"
                            (error-message-string err)))))

(ert-deftest clutch-db-test-schema-transaction-effect-by-backend ()
  "DDL transaction effects should live behind the backend interface."
  (require 'clutch-db-mysql)
  (require 'clutch-db-pg)
  (should-not (clutch-db-schema-transaction-effect
               'opaque-conn "CREATE TABLE t (id int)"))
  (should (eq (clutch-db-schema-transaction-effect
               (make-mysql-conn :status-flags #x0002)
               "CREATE TABLE t (id int)")
              'clear))
  (should (eq (clutch-db-schema-transaction-effect
               (clutch-db-test--make-pgcon :database "test")
               "CREATE TABLE t (id int)")
              'dirty))
  (should (eq (clutch-db-schema-transaction-effect
               (make-clutch-jdbc-conn :params '(:driver oracle))
               "CREATE TABLE t (id int)")
              'clear))
  (should-not (clutch-db-schema-transaction-effect
               (make-clutch-jdbc-conn :params '(:driver sqlserver))
               "CREATE TABLE t (id int)")))

(ert-deftest clutch-db-test-jdbc-commit-fires-rpc ()
  "Clutch-db-commit should issue a commit RPC with the connection id."
  (let ((conn (make-clutch-jdbc-conn :conn-id 17
                                     :params '(:driver oracle :rpc-timeout 12)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 17))))
      (clutch-db-commit conn)
      (should (equal captured-op "commit"))
      (should (= (alist-get 'conn-id captured-params) 17))
      (should (= captured-timeout 12)))))

(ert-deftest clutch-db-test-jdbc-rollback-fires-rpc ()
  "Clutch-db-rollback should issue a rollback RPC with the connection id."
  (let ((conn (make-clutch-jdbc-conn :conn-id 18
                                     :params '(:driver oracle :rpc-timeout 13)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 18))))
      (clutch-db-rollback conn)
      (should (equal captured-op "rollback"))
      (should (= (alist-get 'conn-id captured-params) 18))
      (should (= captured-timeout 13)))))

(ert-deftest clutch-db-test-jdbc-set-auto-commit-fires-rpc ()
  "Clutch-db-set-auto-commit should issue set-auto-commit RPC with auto-commit value."
  (let ((conn (make-clutch-jdbc-conn :conn-id 19
                                     :params '(:driver oracle :rpc-timeout 12 :manual-commit t)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 19 :auto-commit t))))
      (clutch-db-set-auto-commit conn t)
      (should (equal captured-op "set-auto-commit"))
      (should (= (alist-get 'conn-id captured-params) 19))
      (should (eq (alist-get 'auto-commit captured-params) t))
      (should (= captured-timeout 12)))))

(ert-deftest clutch-db-test-jdbc-set-auto-commit-updates-params ()
  "Clutch-db-set-auto-commit should update :manual-commit in conn params."
  (let ((conn (make-clutch-jdbc-conn :conn-id 20
                                     :params '(:driver oracle :rpc-timeout 12 :manual-commit t))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc) (lambda (_op _params &optional _to) nil)))
      ;; Switch to auto-commit: manual-commit should become nil
      (clutch-db-set-auto-commit conn t)
      (should-not (plist-get (clutch-jdbc-conn-params conn) :manual-commit))
      ;; Switch back to manual-commit: manual-commit should become t
      (clutch-db-set-auto-commit conn nil)
      (should (plist-get (clutch-jdbc-conn-params conn) :manual-commit)))))

(ert-deftest clutch-db-test-native-mysql-manual-commit-follows-autocommit ()
  "Native MySQL manual-commit should mirror session autocommit state."
  (require 'clutch-db-mysql)
  (should-not
   (clutch-db-manual-commit-p
    (make-mysql-conn :status-flags #x0002)))
  (should
   (clutch-db-manual-commit-p
    (make-mysql-conn :status-flags 0))))

(ert-deftest clutch-db-test-native-mysql-transaction-methods-delegate ()
  "Native MySQL transaction methods should delegate to mysql.el helpers."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306))
        captured-auto-commit
        committed
        rolled-back)
    (cl-letf (((symbol-function 'mysql-set-autocommit)
               (lambda (_conn v)
                 (setq captured-auto-commit v)))
              ((symbol-function 'mysql-commit)
               (lambda (_conn)
                 (setq committed t)))
              ((symbol-function 'mysql-rollback)
               (lambda (_conn)
                 (setq rolled-back t))))
      (clutch-db-set-auto-commit conn nil)
      (should-not captured-auto-commit)
      (clutch-db-commit conn)
      (clutch-db-rollback conn)
      (should committed)
      (should rolled-back))))

(ert-deftest clutch-db-test-native-pg-toggle-enables-manual-mode ()
  "Native PostgreSQL should enter manual-commit mode after toggling autocommit off."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test")))
    (should-not (clutch-db-manual-commit-p conn))
    (clutch-db-set-auto-commit conn nil)
    (should (clutch-db-manual-commit-p conn))
    (clutch-db-set-auto-commit conn t)
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-native-pg-manual-mode-lazy-begin ()
  "Native PostgreSQL manual-commit should lazily BEGIN on the first foreground query."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        calls)
    (clutch-db-set-auto-commit conn nil)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (push sql calls)
                 (pcase sql
                   ("BEGIN"
                    (make-pgresult :connection conn :status "BEGIN"))
                   ("SELECT 1"
                    (make-pgresult :connection conn
                                   :status "SELECT 1"
                                   :attributes '(("n" 23 4))
                                   :tuples '((1))))
                   ("SELECT 2"
                    (make-pgresult :connection conn
                                   :status "SELECT 1"
                                   :attributes '(("n" 23 4))
                                   :tuples '((2))))
                   (_
                    (ert-fail (format "Unexpected SQL: %s" sql)))))))
      (let ((result1 (clutch-db-query conn "SELECT 1"))
            (result2 (clutch-db-query conn "SELECT 2")))
        (should (equal (nreverse calls)
                       '("BEGIN" "SELECT 1" "SELECT 2")))
        (should (= (caar (clutch-db-result-rows result1)) 1))
        (should (= (caar (clutch-db-result-rows result2)) 2))))))

(ert-deftest clutch-db-test-native-pg-toggle-auto-commit-rolls-back-failed-transaction ()
  "Native PostgreSQL should roll back an aborted manual transaction before enabling autocommit."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        calls)
    (clutch-db-set-auto-commit conn nil)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (push sql calls)
                 (pcase sql
                   ("BEGIN"
                    (make-pgresult :connection conn :status "BEGIN"))
                   ("UPDATE demo SET x = 1"
                    (signal 'pg-error '("statement failed")))
                   ("ROLLBACK"
                    (make-pgresult :connection conn :status "ROLLBACK"))
                   (_
                    (ert-fail (format "Unexpected SQL: %s" sql)))))))
      (should-error (clutch-db-query conn "UPDATE demo SET x = 1")
                    :type 'clutch-db-error)
      (clutch-db-set-auto-commit conn t)
      (should (equal (nreverse calls)
                     '("BEGIN" "UPDATE demo SET x = 1" "ROLLBACK")))
      (should-not (clutch-db-manual-commit-p conn)))))

(ert-deftest clutch-db-test-native-pg-transaction-control-allows-leading-comments ()
  "Native PostgreSQL should not inject lazy BEGIN before commented transaction control."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        calls)
    (clutch-db-set-auto-commit conn nil)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (push sql calls)
                 (make-pgresult :connection conn
                                :status
                                (cond
                                 ((string-match-p "COMMIT" sql) "COMMIT")
                                 ((string-match-p "BEGIN" sql) "BEGIN")
                                 (t "ROLLBACK"))))))
      (clutch-db-query conn "/* lead */ COMMIT")
      (clutch-db-query conn "-- lead\nBEGIN")
      (should (equal (nreverse calls)
                     '("/* lead */ COMMIT" "-- lead\nBEGIN"))))))

(ert-deftest clutch-db-test-native-pg-ctid-identity-reads-relkind-cell ()
  "PostgreSQL CTID identity should read the relkind cell, not its first char."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test")))
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn _sql)
                 (make-pgresult :connection conn
                                :status "SELECT 1"
                                :attributes '(("relkind" 25 -1))
                                :tuples '(("r"))))))
      (should (equal (clutch-db-pg--ctid-identity conn "demo")
                     '(:kind row-locator
                       :name "ctid"
                       :select-expressions ("ctid::text")
                       :where-sql "ctid = ?::tid"))))))

(ert-deftest clutch-db-test-default-row-identity-has-no-metadata-support ()
  "Default row identity should report no metadata support."
  (should-not (clutch-db-row-identity-candidates '(:backend unsupported)
                                                 "demo")))

(ert-deftest clutch-db-test-default-row-identity-surfaces-metadata-errors ()
  "Default row identity should not hide primary-key metadata errors."
  (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
             (lambda (_conn _table)
               (signal 'clutch-db-error '("metadata failed")))))
    (should-error (clutch-db-row-identity-candidates '(:backend broken)
                                                    "demo")
                  :type 'clutch-db-error)))

(ert-deftest clutch-db-test-sqlite-rowid-identity-in-memory ()
  "SQLite rowid tables should expose `rowid' as a row locator."
  (skip-unless (require 'clutch-db-sqlite nil t))
  (skip-unless (and (fboundp 'sqlite-available-p)
                    (sqlite-available-p)))
  (let* ((db-file (make-temp-file "clutch-sqlite-rowid-" nil ".db"))
         (conn (clutch-db-sqlite-connect (list :database db-file))))
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE demo (name TEXT)")
          (let ((candidate (car (clutch-db-row-identity-candidates
                                 conn "demo"))))
            (should (equal (plist-get candidate :kind) 'row-locator))
            (should (equal (plist-get candidate :name) "rowid"))
            (should (equal (plist-get candidate :select-expressions)
                           '("rowid")))
            (should (equal (plist-get candidate :where-sql) "rowid = ?"))))
      (clutch-db-disconnect conn)
      (ignore-errors (delete-file db-file)))))

(ert-deftest clutch-db-test-sqlite-memory-does-not-create-file ()
  "SQLite :memory: profiles should open an in-memory database."
  (skip-unless (require 'clutch-db-sqlite nil t))
  (skip-unless (and (fboundp 'sqlite-available-p)
                    (sqlite-available-p)))
  (let* ((dir (make-temp-file "clutch-sqlite-memory-" t))
         (default-directory (file-name-as-directory dir))
         conn)
    (unwind-protect
        (progn
          (setq conn (clutch-db-sqlite-connect '(:database ":memory:")))
          (clutch-db-query conn "CREATE TABLE demo (id INTEGER)")
          (should-not (file-exists-p (expand-file-name ":memory:" dir))))
      (when conn
        (clutch-db-disconnect conn))
      (delete-directory dir t))))

(ert-deftest clutch-db-test-sqlite-returning-yields-result-rows ()
  "SQLite DML with RETURNING should produce columns and rows."
  (skip-unless (require 'clutch-db-sqlite nil t))
  (skip-unless (and (fboundp 'sqlite-available-p)
                    (sqlite-available-p)))
  (let* ((db-file (make-temp-file "clutch-sqlite-returning-" nil ".db"))
         conn)
    (unwind-protect
        (progn
          (setq conn (clutch-db-sqlite-connect (list :database db-file)))
          (clutch-db-query conn
                           "CREATE TABLE demo (id INTEGER PRIMARY KEY, name TEXT)")
          (let ((result (clutch-db-query
                         conn
                         "INSERT INTO demo (name) VALUES ('a') RETURNING id, name")))
            (should (equal (mapcar (lambda (col) (plist-get col :name))
                                   (clutch-db-result-columns result))
                           '("id" "name")))
            (should (equal (clutch-db-result-rows result) '((1 "a"))))
            (should-not (clutch-db-result-affected-rows result))))
      (when conn
        (clutch-db-disconnect conn))
      (ignore-errors (delete-file db-file)))))

(ert-deftest clutch-db-test-jdbc-object-definition-table-uses-oracle-style-identifiers ()
  "Oracle synthesized JDBC DDL should quote only identifiers that need it."
  (let ((conn (make-clutch-jdbc-conn :conn-id 4
                                     :params '(:driver oracle :schema "CLUTCH"))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op _params &optional _timeout-seconds)
                 '(:columns ((:name "PK_MAIN" :type "CHAR" :nullable :json-false)
                             (:name "TYPE" :type "VARCHAR2" :nullable :json-false)
                             (:name "ACTION" :type "VARCHAR2" :nullable :json-false)
                             (:name "mixedCase" :type "VARCHAR2" :nullable :json-false))))))
      (let ((ddl (clutch-db-object-definition
                  conn '(:name "APP_EVENT_DATA" :type "TABLE"))))
        (should (string-match-p "CREATE TABLE APP_EVENT_DATA" ddl))
        (should (string-match-p "PK_MAIN CHAR" ddl))
        (should (string-match-p "\"TYPE\" VARCHAR2" ddl))
        (should (string-match-p "\"ACTION\" VARCHAR2" ddl))
        (should (string-match-p "\"mixedCase\" VARCHAR2" ddl))
        (should-not (string-match-p "\"APP_EVENT_DATA\"" ddl))
        (should-not (string-match-p "\"PK_MAIN\"" ddl))))))

(ert-deftest clutch-db-test-jdbc-oracle-row-identity-falls-back-to-rowid ()
  "Oracle JDBC should offer ROWID when no logical key is available."
  (let ((conn (make-clutch-jdbc-conn :conn-id 4
                                     :params '(:driver oracle
                                               :schema "CLUTCH"))))
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (_conn _table) nil))
              ((symbol-function 'clutch-jdbc--unique-not-null-identities)
               (lambda (_conn _table) nil)))
      (should (equal (clutch-db-row-identity-candidates conn "DEMO")
                     '((:kind row-locator
                        :name "ROWID"
                        :select-expressions ("ROWID")
                        :where-sql "ROWID = ?")))))))

(ert-deftest clutch-db-test-jdbc-refresh-schema-async-returns-table-names ()
  "Async JDBC schema refresh should return only table names to its callback."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params `(:driver oracle :user "scott"
                                               :rpc-timeout ,clutch-jdbc-rpc-timeout-seconds)))
        callback-result)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (_op _params callback &optional errback _timeout-seconds _conn)
                 (should-not errback)
                 (funcall callback '(:cursor-id nil
                                    :columns ("name" "type" "schema" "source_schema")
                                    :rows (("USERS" "TABLE" "SCOTT" "SCOTT")
                                           ("ORDERS_VIEW" "VIEW" "SCOTT" "SCOTT")
                                           ("PUBLIC_ORDERS" "SYNONYM" "APP" "PUBLIC")
                                           ("ORDERS" "TABLE" "SCOTT" "SCOTT"))
                                    :done t))
                 42)))
      (should (clutch-db-refresh-schema-async
               conn
               (lambda (tables)
                 (setq callback-result tables))))
      (should (equal callback-result '("USERS" "ORDERS"))))))

(ert-deftest clutch-db-test-jdbc-list-table-entries-preserves-source-schema ()
  "JDBC table entry listing should preserve schema/source metadata."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "app"))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (&rest _args)
                 '(:cursor-id nil
                   :columns ("name" "type" "schema" "source_schema")
                   :rows (("ORDERS" "SYNONYM" "DATA_OWNER" "APP")
                          ("USER_TABLES" "PUBLIC SYNONYM" "SYS" "PUBLIC"))
                   :done t))))
      (let ((entries (clutch-db-list-table-entries conn)))
        (should (equal entries
                       '((:name "ORDERS" :type "SYNONYM" :schema "DATA_OWNER" :source-schema "APP")
                         (:name "USER_TABLES" :type "PUBLIC SYNONYM" :schema "SYS" :source-schema "PUBLIC"))))))))

(ert-deftest clutch-db-test-jdbc-refresh-schema-async-uses-connection-rpc-timeout ()
  "Async JDBC schema refresh should respect per-connection rpc timeout."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "scott"
                                               :rpc-timeout 7)))
        captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (_op _params _callback &optional _errback timeout-seconds _conn)
                 (setq captured-timeout timeout-seconds)
                 42)))
      (should (clutch-db-refresh-schema-async conn #'ignore))
      (should (= captured-timeout 7)))))

(ert-deftest clutch-db-test-native-async-schedules-idle-call ()
  "Native metadata async methods should share the idle scheduling policy."
  (require 'clutch-db-mysql)
  (require 'clutch-db-pg)
  (dolist (conn (list (make-mysql-conn :host "127.0.0.1" :port 3306
                                       :user "root" :database "mysql")
                      (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                                  :user "postgres"
                                                  :database "test")))
    (dolist (case '((refresh-schema . ("users" "orders"))
                    (list-columns . ("id" "name"))
                    (column-details . ((:name "id" :type "integer")))
                    (table-comment . "Users table")
                    (list-objects . ((:name "users_pkey" :type "INDEX")))))
      (let ((op (car case))
            (expected (cdr case))
            callback-result
            scheduled)
        (clutch-db-test--with-immediate-idle-metadata scheduled
          (cl-letf (((symbol-function 'clutch-db-list-tables)
                     (lambda (context)
                       (should (eq context conn))
                       '("users" "orders")))
                    ((symbol-function 'clutch-db-list-columns)
                     (lambda (context table)
                       (should (eq context conn))
                       (should (equal table "users"))
                       '("id" "name")))
                    ((symbol-function 'clutch-db-column-details)
                     (lambda (context table)
                       (should (eq context conn))
                       (should (equal table "users"))
                       '((:name "id" :type "integer"))))
                    ((symbol-function 'clutch-db-table-comment)
                     (lambda (context table)
                       (should (eq context conn))
                       (should (equal table "users"))
                       "Users table"))
                    ((symbol-function 'clutch-db-list-objects)
                     (lambda (context category)
                       (should (eq context conn))
                       (should (eq category 'indexes))
                       '((:name "users_pkey" :type "INDEX")))))
            (should
             (eq
              (pcase op
                ('refresh-schema
                 (clutch-db-refresh-schema-async
                  conn (lambda (value) (setq callback-result value))))
                ('list-columns
                 (clutch-db-list-columns-async
                  conn "users" (lambda (value) (setq callback-result value))))
                ('column-details
                 (clutch-db-column-details-async
                  conn "users" (lambda (value) (setq callback-result value))))
                ('table-comment
                 (clutch-db-table-comment-async
                  conn "users" (lambda (value) (setq callback-result value))))
                ('list-objects
                 (clutch-db-list-objects-async
                  conn 'indexes (lambda (value) (setq callback-result value)))))
              'fake-timer))
            (should (equal scheduled '(0 nil)))
            (should (equal callback-result expected))))))))

(ert-deftest clutch-db-test-idle-metadata-call-reschedules-while-busy ()
  "Idle metadata calls should not run on a busy connection.
They should reschedule and only execute FN after `clutch-db-busy-p' becomes nil."
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        (scheduled 0)
        callback-result
        (busy-states '(t nil)))
    (cl-letf (((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn &rest args)
                 (setq scheduled (1+ scheduled))
                 (apply fn args)
                 'fake-timer))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn)
                 (prog1 (car busy-states)
                   (setq busy-states (or (cdr busy-states) '(nil))))))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 '("id" "name"))))
      (should (eq (clutch-db--schedule-idle-metadata-call
                   conn
                   (lambda (columns)
                     (setq callback-result columns))
                   nil
                   #'clutch-db-list-columns
                   "users")
                  'fake-timer))
      (should (= scheduled 2))
      (should (equal callback-result '("id" "name"))))))

(ert-deftest clutch-db-test-idle-metadata-call-reschedules-while-foreground-active ()
  "Idle metadata calls should not run during foreground work on the same CONN."
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        (clutch-db--foreground-connections (make-hash-table :test 'eq))
        timers
        callback-result
        (query-count 0))
    (cl-letf (((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn &rest args)
                 (push (lambda () (apply fn args)) timers)
                 'fake-timer))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 (cl-incf query-count)
                 '("id" "name"))))
      (puthash conn t clutch-db--foreground-connections)
      (should (eq (clutch-db--schedule-idle-metadata-call
                   conn
                   (lambda (columns)
                     (setq callback-result columns))
                   nil
                   #'clutch-db-list-columns
                   "users")
                  'fake-timer))
      (should (= (length timers) 1))
      (funcall (pop timers))
      (should-not callback-result)
      (should (= query-count 0))
      (should (= (length timers) 1))
      (remhash conn clutch-db--foreground-connections)
      (funcall (pop timers))
      (should (equal callback-result '("id" "name")))
      (should (= query-count 1)))))

(ert-deftest clutch-db-test-jdbc-list-columns-async-maps-column-names ()
  "JDBC async column-name preheat should normalize the returned names."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "scott")))
        callback-result)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (_op _params callback &optional _errback _timeout _conn)
                 (funcall callback
                          '(:columns ((:name "ID" :type "NUMBER")
                                      (:name "NAME" :type "VARCHAR2"))))
                 42)))
      (should (clutch-db-list-columns-async
               conn "USERS"
               (lambda (columns)
                 (setq callback-result columns))))
      (should (equal callback-result '("ID" "NAME"))))))

;;;; Unit tests — clutch-jdbc--collect-table-entries

(ert-deftest clutch-db-test-jdbc-collect-table-entries-direct ()
  "When :tables is present, collect-table-entries returns it directly."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott")))
        fetch-called)
    (cl-letf (((symbol-function 'clutch-jdbc--fetch-all)
               (lambda (_conn _cursor-id)
                 (setq fetch-called t)
                 '())))
      (let ((entries (clutch-jdbc--collect-table-entries
                      conn
                      '(:tables ((:name "USERS" :type "TABLE" :schema "SCOTT")
                                 (:name "ORDERS" :type "TABLE" :schema "SCOTT"))))))
        (should (equal entries '((:name "USERS" :type "TABLE" :schema "SCOTT")
                                 (:name "ORDERS" :type "TABLE" :schema "SCOTT"))))
        (should-not fetch-called)))))

(ert-deftest clutch-db-test-jdbc-collect-table-entries-legacy-cursor ()
  "Legacy cursor-format results are normalized to entry plists."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott")))
        fetch-cursor-id)
    (cl-letf (((symbol-function 'clutch-jdbc--fetch-all)
               (lambda (_conn cursor-id)
                 (setq fetch-cursor-id cursor-id)
                 '(("PRODUCTS" "TABLE" "SCOTT")))))
      (let ((entries (clutch-jdbc--collect-table-entries
                      conn
                      '(:rows (("USERS" "TABLE" "SCOTT"))
                        :cursor-id 42
                        :done nil))))
        (should (equal entries '((:name "USERS" :type "TABLE" :schema "SCOTT" :source-schema "SCOTT")
                                 (:name "PRODUCTS" :type "TABLE" :schema "SCOTT" :source-schema "SCOTT"))))
        (should (= fetch-cursor-id 42))))))

(ert-deftest clutch-db-test-jdbc-list-table-entries-keeps-object-types ()
  "JDBC list-table-entries should preserve view and synonym metadata."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver oracle :user "scott"))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op _params &optional _timeout)
                 '(:tables ((:name "USERS" :type "TABLE" :schema "SCOTT")
                            (:name "USER_VIEW" :type "VIEW" :schema "SCOTT")
                            (:name "USER_SYM" :type "SYNONYM" :schema "SCOTT"
                                    :target-schema "APP" :target-name "USERS"))))))
      (should
       (equal (clutch-db-list-table-entries conn)
              '((:name "USERS" :type "TABLE" :schema "SCOTT")
                (:name "USER_VIEW" :type "VIEW" :schema "SCOTT")
                (:name "USER_SYM" :type "SYNONYM" :schema "SCOTT"
                        :target-schema "APP" :target-name "USERS")))))))

(ert-deftest clutch-db-test-jdbc-conn-catalog-clickhouse-defaults-to-database ()
  "ClickHouse JDBC metadata should use :database as the default catalog."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default"))))
    (should (equal (clutch-jdbc--conn-catalog conn) "default"))))

(ert-deftest clutch-db-test-sql-interface-mongodb-uses-limit-offset-pagination ()
  "MongoDB SQL Interface should use SQL Interface LIMIT/OFFSET pagination."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :driver 'mongodb
                                     :params '(:driver mongodb
                                               :surface sql-interface))))
    (should (equal (clutch-db-build-paged-sql
                    conn "SELECT * FROM users" 2 25)
                   "SELECT * FROM users LIMIT 25 OFFSET 50"))))

(ert-deftest clutch-db-test-sql-interface-mongodb-bson-types-map-to-clutch-categories ()
  "MongoDB SQL Interface JDBC BSON type names should map to useful display categories."
  (should (eq (clutch-jdbc--type-category "DOCUMENT") 'json))
  (should (eq (clutch-jdbc--type-category "ARRAY") 'json))
  (should (eq (clutch-jdbc--type-category "BINDATA") 'blob))
  (should (eq (clutch-jdbc--type-category "OBJECTID") 'text)))

;;;; Unit tests — native MongoDB Clutch adapter

;; Protocol-level mongodb.el tests live in the standalone mongodb.el repository.

(defun clutch-db-test--make-mongodb-conn (&optional database client)
  "Return a lightweight native MongoDB Clutch connection for unit tests."
  (make-clutch-mongodb-conn
   :params (list :database (or database "app"))
   :database (or database "app")
   :client (or client 'mongodb-client)
   :closed nil
   :busy nil))

(ert-deftest clutch-db-test-mongodb-object-browse-query-uses-helper-syntax ()
  "Native MongoDB should provide object browsing syntax from the adapter."
  (let ((conn (clutch-db-test--make-mongodb-conn)))
    (dolist (case '(("users" . "db.getCollection(\"users\").find({}).limit(20);")
                    ("order-items" . "db.getCollection(\"order-items\").find({}).limit(20);")))
      (pcase-let ((`(,collection . ,expected) case))
        (should (equal (clutch-db-object-browse-query
                        conn (list :name collection :type "COLLECTION"))
                       expected))))))

(ert-deftest clutch-db-test-mongodb-ensure-mongodb-api-does-not-reload-stale-feature ()
  "Native MongoDB should report stale public APIs instead of reloading libraries."
  (let (loaded)
    (cl-letf (((symbol-function 'featurep)
               (lambda (feature &optional _subfeature)
                 (eq feature 'mongodb)))
              ((symbol-function 'fboundp)
               (lambda (symbol)
                 (not (eq symbol 'mongodb-connect))))
              ((symbol-function 'locate-library)
               (lambda (library)
                 (and (equal library "mongodb") "/tmp/mongodb.el")))
              ((symbol-function 'load)
               (lambda (file &rest _args)
                 (setq loaded file)
                 t)))
      (let ((err (should-error (clutch-mongodb--ensure-mongodb-client-api)
                               :type 'clutch-db-error)))
        (should (string-match-p "requires current mongodb.el public API"
                                (error-message-string err)))
        (should (string-match-p "mongodb-connect"
                                (error-message-string err))))
      (should-not loaded))))

(ert-deftest clutch-db-test-mongodb-errors-translate-labels-to-details-plist ()
  "Native MongoDB should translate labeled protocol errors to Clutch error shape."
  (let ((err (should-error
              (clutch-mongodb--with-mongodb-errors
                (signal 'mongodb-error
                        '("boom" :error-labels
                          ("TransientTransactionError"))))
              :type 'clutch-db-error)))
    (should (equal (cadr err) "boom"))
    (should (plistp (nth 2 err)))
    (should (equal (plist-get (nth 2 err) :mongodb-error-labels)
                   '("TransientTransactionError")))
    (should (equal
             (plist-get
              (clutch--make-connection-error-details
               '(:backend mongodb
                 :host "127.0.0.1"
                 :port 27017
                 :database "app")
               err)
              :mongodb-error-labels)
             '("TransientTransactionError")))))

(ert-deftest clutch-db-test-mongodb-connect-passes-native-params ()
  "Native MongoDB should pass saved params directly to `mongodb-connect'."
  (let (captured-params)
    (cl-letf (((symbol-function 'mongodb-connect)
               (lambda (params)
                 (setq captured-params params)
                 (make-mongodb-conn :database "app" :closed nil))))
      (let ((conn (clutch-mongodb-connect
                   '(:host "127.0.0.1"
                     :port 27017
                     :database "app"
                     :auth-database "admin"
                     :user "reporter"
                     :password "s p"
                     :tls t
                     :props (("retryWrites" . "true"))))))
        (should (eq (clutch-db-backend-key conn) 'mongodb))
        (should (equal (clutch-db-display-name conn) "MongoDB"))
        (should (equal (clutch-db-database conn) "app"))
        (should (mongodb-conn-p (clutch-mongodb-conn-client conn)))
        (should (equal captured-params
                       '(:host "127.0.0.1"
                         :port 27017
                         :database "app"
                         :auth-database "admin"
                         :user "reporter"
                         :password "s p"
                         :tls t
                         :props (("retryWrites" . "true")))))))))

(ert-deftest clutch-db-test-mongodb-connect-uses-url-database ()
  "Native MongoDB should use the database reported by `mongodb-connect'."
  (cl-letf (((symbol-function 'mongodb-connect)
             (lambda (_params)
               (make-mongodb-conn :database "analytics" :closed nil))))
    (let ((conn (clutch-mongodb-connect
                 '(:url "mongodb+srv://cluster.example.net/analytics?retryWrites=true"))))
      (should (equal (clutch-db-database conn) "analytics")))))

(ert-deftest clutch-db-test-mongodb-connect-sql-interface-delegates-to-jdbc ()
  "MongoDB SQL Interface should stay under the mongodb backend but use JDBC."
  (let (captured-driver captured-params)
    (cl-letf (((symbol-function 'clutch-db-jdbc-connect)
               (lambda (driver params)
                 (setq captured-driver driver
                       captured-params params)
                 'jdbc-conn))
              ((symbol-function 'clutch-mongodb--ensure-mongodb-client-api)
               (lambda ()
                 (ert-fail "SQL Interface should not load native mongodb.el API"))))
      (should (eq (clutch-mongodb-connect
                   '(:surface "sql-interface"
                     :host "cluster0.a.query.mongodb.net"
                     :database "analytics"))
                  'jdbc-conn))
      (should (eq captured-driver 'mongodb))
      (should (equal captured-params
                     '(:surface "sql-interface"
                       :host "cluster0.a.query.mongodb.net"
                       :database "analytics"))))))

(ert-deftest clutch-db-test-mongodb-list-index-objects ()
  "Native MongoDB indexes should map into Clutch object metadata."
  (let* ((conn (clutch-db-test--make-mongodb-conn "app" 'client))
         (id-index (mongodb-document
                    `(("name" . "_id_")
                      ("key" . ,(mongodb-document '(("_id" . 1)))))))
         (email-index (mongodb-document
                       `(("name" . "email_idx")
                         ("key" . ,(mongodb-document '(("email" . 1))))
                         ("unique" . t)))))
    (cl-letf (((symbol-function 'mongodb-list-collections)
               (lambda (_client _database &optional _filter _options)
                 '("users")))
              ((symbol-function 'mongodb-list-indexes)
               (lambda (_client database collection)
                 (should (equal database "app"))
                 (should (equal collection "users"))
                 (list id-index email-index))))
      (let* ((entries (clutch-db-list-objects conn 'indexes))
             (email-entry (cadr entries)))
        (should (equal (mapcar (lambda (entry) (plist-get entry :identity))
                               entries)
                       '("users._id_" "users.email_idx")))
        (should (equal (plist-get email-entry :name) "email_idx"))
        (should (equal (plist-get email-entry :schema) "app"))
        (should (equal (plist-get email-entry :target-table) "users"))
        (should (eq (plist-get email-entry :unique) t))
        (should (equal (clutch-db-object-details conn email-entry)
                       '((:name "email" :position 1 :descend "ASC"))))
        (should (string-match-p
                 "\"name\":\"email_idx\""
                 (clutch-db-object-definition conn email-entry)))))))

(ert-deftest clutch-db-test-mongodb-query-documents-to-grid ()
  "Native MongoDB query results should flatten top-level document keys."
  (let ((docs '((("_id" . (("$oid" . "64f")))
                 ("name" . "Ann")
                 ("score" . 10)
                 ("tags" . ["a" "b"])
                 ("meta" . "plain"))
                (("_id" . (("$oid" . "650")))
                 ("name" . "Bob")
                 ("meta" . (("ok" . t)))
                 ("active" . :false)))))
    (cl-letf (((symbol-function 'clutch-mongodb--eval)
               (lambda (_conn code)
                 (should (equal code "db.users.find()"))
                 docs)))
      (let* ((conn (clutch-db-test--make-mongodb-conn))
             (result (clutch-db-query conn "db.users.find()")))
        (should (equal (clutch-db-result-column-names
                        (clutch-db-result-columns result))
                       '("_id" "name" "score" "tags" "meta" "active"
                         "clutch__document")))
        (should (equal (mapcar (lambda (column)
                                 (plist-get column :type-category))
                               (clutch-db-result-columns result))
                       '(json text numeric json json text json)))
        (should (plist-get (car (last (clutch-db-result-columns result)))
                           :hidden))
        (should (plist-get (car (last (clutch-db-result-columns result)))
                           :document-source))
        (should (equal (clutch-db-result-rows result)
                       '(("{\"$oid\":\"64f\"}" "Ann" 10 "[\"a\",\"b\"]"
                          "plain" nil
                          (("_id" . (("$oid" . "64f")))
                           ("name" . "Ann")
                           ("score" . 10)
                           ("tags" . ["a" "b"])
                           ("meta" . "plain")))
                         ("{\"$oid\":\"650\"}" "Bob" nil nil
                          "{\"ok\":true}" "false"
                          (("_id" . (("$oid" . "650")))
                           ("name" . "Bob")
                           ("meta" . (("ok" . t)))
                           ("active" . :false))))))))))

(ert-deftest clutch-db-test-mongodb-result-context-records-source-collection ()
  "Native MongoDB query context should record collection result metadata."
  (let* ((conn (clutch-db-test--make-mongodb-conn))
         (code "db.getCollection(\"users\").find({active: true}).limit(5)")
         (context (clutch-db-query-result-context conn code)))
    (should (clutch-db-result-query-p conn code))
    (should (equal (plist-get context :source-table) "users"))
    (should-not (plist-get context :server-pageable))
    (should-not (plist-get context :server-rewritable))
    (should (equal (plist-get (plist-get context :row-identity-prep) :sql)
                   code))))

(ert-deftest clutch-db-test-mongodb-document-mutation-snippets ()
  "Native MongoDB should build document mutation helper snippets."
  (let* ((conn (clutch-db-test--make-mongodb-conn))
         (doc '(("_id" . 7) ("name" . "Ann") ("score" . 10))))
    (should
     (equal
      (clutch-db-document-mutation-snippets conn 'insert-one "users" (list doc))
      '("db.getCollection(\"users\").insertOne({\"_id\":7,\"name\":\"Ann\",\"score\":10});")))
    (should
     (equal
      (clutch-db-document-mutation-snippets conn 'insert-many "users" (list doc))
      '("db.getCollection(\"users\").insertMany([{\"_id\":7,\"name\":\"Ann\",\"score\":10}]);")))
    (should
     (equal
      (clutch-db-document-mutation-snippets
       conn 'update-one-set "users" (list doc) '("name"))
      '("db.getCollection(\"users\").updateOne({\"_id\":7}, {\"$set\":{\"name\":\"Ann\"}});")))
    (should
     (equal
      (clutch-db-document-mutation-snippets conn 'delete-one "users" (list doc))
      '("db.getCollection(\"users\").deleteOne({\"_id\":7});")))))

(ert-deftest clutch-db-test-mongodb-query-scalars-to-value-column ()
  "Native MongoDB scalar array results should use a value column."
  (cl-letf (((symbol-function 'clutch-mongodb--eval)
             (lambda (_conn _code) '(1 2 3))))
    (let* ((conn (clutch-db-test--make-mongodb-conn))
           (result (clutch-db-query conn "[1, 2, 3]")))
      (should (equal (clutch-db-result-column-names
                      (clutch-db-result-columns result))
                     '("value")))
      (should (equal (clutch-db-result-rows result)
                     '((1) (2) (3)))))))

(ert-deftest clutch-db-test-mongodb-eval-translates-find-helper-to-mongodb-api ()
  "Native MongoDB eval should translate shell helpers to mongodb.el calls."
  (let (captured)
    (cl-letf (((symbol-function 'mongodb-find)
               (lambda (client database collection filter projection limit skip
                             sort &optional options)
                 (setq captured
                       (list client database collection filter projection
                             limit skip sort options))
                 (list (list (cons "_id" "a")
                             (cons "name" "Ann"))))))
      (let* ((conn (clutch-db-test--make-mongodb-conn "app" 'client))
             (value (clutch-mongodb--eval
                     conn
                     "db.users.find({active: true}, {name: 1}).limit(20)")))
        (should (equal value '((("_id" . "a") ("name" . "Ann")))))
        (pcase-let ((`(,client ,database ,collection ,filter ,projection
                       ,limit ,skip ,sort ,options)
                     captured))
          (should (eq client 'client))
          (should (equal database "app"))
          (should (equal collection "users"))
          (should (mongodb-document-p filter))
          (should (equal (mongodb-document-pairs filter)
                         '(("active" . t))))
          (should (mongodb-document-p projection))
          (should (equal (mongodb-document-pairs projection)
                         '(("name" . 1))))
          (should (= limit 20))
          (should (null skip))
          (should (null sort))
          (should (null options)))))))

(ert-deftest clutch-db-test-mongodb-eval-translates-find-chain-options ()
  "Native MongoDB eval should translate supported find cursor options."
  (let (captured)
    (cl-letf (((symbol-function 'mongodb-find)
               (lambda (client database collection filter projection limit skip
                             sort &optional options)
                 (setq captured
                       (list client database collection filter projection
                             limit skip sort options))
                 nil)))
      (clutch-mongodb--eval
       (clutch-db-test--make-mongodb-conn "app" 'client)
       (concat
        "db.users.find({active: true}, {name: 1})"
        ".sort({createdAt: -1})"
        ".maxTimeMS(250)"
        ".batchSize(50)"
        ".comment('scan-users')"
        ".allowDiskUse(true)"
        ".skip(5).limit(10)"))
      (pcase-let ((`(,client ,database ,collection ,filter ,projection
                     ,limit ,skip ,sort ,options)
                   captured))
        (should (eq client 'client))
        (should (equal database "app"))
        (should (equal collection "users"))
        (should (mongodb-document-p filter))
        (should (equal (mongodb-document-pairs filter)
                       '(("active" . t))))
        (should (mongodb-document-p projection))
        (should (equal (mongodb-document-pairs projection)
                       '(("name" . 1))))
        (should (= limit 10))
        (should (= skip 5))
        (should (mongodb-document-p sort))
        (should (equal (mongodb-document-pairs sort)
                       '(("createdAt" . -1))))
        (should-not (assoc "sort" options))
        (should (= (cdr (assoc "maxTimeMS" options)) 250))
        (should (= (cdr (assoc "batchSize" options)) 50))
        (should (equal (cdr (assoc "comment" options)) "scan-users"))
        (should (eq (cdr (assoc "allowDiskUse" options)) t))))))

(ert-deftest clutch-db-test-mongodb-find-chain-boolean-options-validate ()
  "Native MongoDB cursor boolean helpers should reject non-boolean values."
  (should-error
   (clutch-mongodb--eval
    (clutch-db-test--make-mongodb-conn "app" 'client)
    "db.users.find({}).allowDiskUse('yes')")
   :type 'clutch-db-error))

(ert-deftest clutch-db-test-mongodb-eval-translates-aggregate-options ()
  "Native MongoDB eval should translate aggregate options and helper chains."
  (let (captured)
    (cl-letf (((symbol-function 'mongodb-aggregate)
               (lambda (client database collection pipeline &optional options)
                 (setq captured
                       (list client database collection pipeline options))
                 '((("_id" . "active") ("n" . 2))))))
      (let ((value
             (clutch-mongodb--eval
              (clutch-db-test--make-mongodb-conn "app" 'client)
              (concat
               "db.users.aggregate([{$match: {active: true}}], "
               "{allowDiskUse: false, comment: 'base'})"
               ".allowDiskUse(true)"
               ".batchSize(25)"
               ".comment('chain')"
               ".maxTimeMS(500)"))))
        (should (equal value '((("_id" . "active") ("n" . 2)))))))
    (pcase-let ((`(,client ,database ,collection ,pipeline ,options)
                 captured))
      (should (eq client 'client))
      (should (equal database "app"))
      (should (equal collection "users"))
      (should (vectorp pipeline))
      (should (= (length pipeline) 1))
      (let ((stage (aref pipeline 0)))
        (should (mongodb-document-p stage))
        (should (assoc "$match" (mongodb-document-pairs stage))))
      (should (mongodb-document-p options))
      (let ((pairs (mongodb-document-pairs options)))
        (should (eq (cdr (assoc "allowDiskUse" pairs)) t))
        (should (equal (cdr (assoc "comment" pairs)) "chain"))
        (should (= (cdr (assoc "batchSize" pairs)) 25))
        (should (= (cdr (assoc "maxTimeMS" pairs)) 500))))))

(ert-deftest clutch-db-test-mongodb-eval-translates-database-aggregate ()
  "Native MongoDB eval should translate db.aggregate() to database aggregate."
  (let (calls)
    (cl-letf (((symbol-function 'mongodb-aggregate-database)
               (lambda (client database pipeline &optional options)
                 (push (list client database pipeline options) calls)
                 `((("database" . ,database))))))
      (let ((value
             (clutch-mongodb--eval
              (clutch-db-test--make-mongodb-conn "app" 'client)
              (concat
               "db.aggregate([{$documents: [{n: 1}]}], "
               "{comment: 'db-agg'});"
               "db.getSiblingDB('admin').aggregate([{$currentOp: {}}])"))))
        (should (equal value '((("database" . "admin")))))))
    (setq calls (nreverse calls))
    (should (= (length calls) 2))
    (pcase-let ((`(,client ,database ,pipeline ,options) (car calls)))
      (should (eq client 'client))
      (should (equal database "app"))
      (should (vectorp pipeline))
      (let ((stage (aref pipeline 0)))
        (should (mongodb-document-p stage))
        (should (assoc "$documents" (mongodb-document-pairs stage))))
      (should (mongodb-document-p options))
      (should (equal (mongodb-document-pairs options)
                     '(("comment" . "db-agg")))))
    (pcase-let ((`(,client ,database ,pipeline ,options) (cadr calls)))
      (should (eq client 'client))
      (should (equal database "admin"))
      (should (vectorp pipeline))
      (let ((stage (aref pipeline 0)))
        (should (mongodb-document-p stage))
        (should (assoc "$currentOp" (mongodb-document-pairs stage))))
      (should-not options))))

(ert-deftest clutch-db-test-mongodb-eval-translates-explain-chain ()
  "Native MongoDB eval should translate find/aggregate explain chains."
  (let (calls)
    (cl-letf (((symbol-function 'mongodb-explain)
               (lambda (client database command &optional verbosity)
                 (push (list client database command verbosity) calls)
                 (list (cons "ok" 1)
                       (cons "queryPlanner"
                             (list (cons "namespace" "app.users")))))))
      (clutch-mongodb--eval
       (clutch-db-test--make-mongodb-conn "app" 'client)
       (concat
        "db.users.find({active: true}).limit(5).explain(true);"
        "db.users.aggregate([{$match: {active: true}}], "
        "{allowDiskUse: true}).explain('executionStats')")))
    (setq calls (nreverse calls))
    (should (= (length calls) 2))
    (let ((find-call (car calls))
          (aggregate-call (cadr calls)))
      (should (eq (nth 0 find-call) 'client))
      (should (equal (nth 1 find-call) "app"))
      (should (eq (nth 3 find-call) t))
      (let ((command (nth 2 find-call)))
        (should (equal (cdr (assoc "find" command)) "users"))
        (should (= (cdr (assoc "limit" command)) 5))
        (should (assoc "filter" command)))
      (should (equal (nth 3 aggregate-call) "executionStats"))
      (let ((command (nth 2 aggregate-call)))
        (should (equal (cdr (assoc "aggregate" command)) "users"))
        (should (assoc "pipeline" command))
        (should (eq (cdr (assoc "allowDiskUse" command)) t))))))

(ert-deftest clutch-db-test-mongodb-explain-chain-verbosity-validates ()
  "Native MongoDB explain helper should reject non-string/non-boolean verbosity."
  (should-error
   (clutch-mongodb--eval
    (clutch-db-test--make-mongodb-conn "app" 'client)
    "db.users.find({}).explain({mode: 'executionStats'})")
   :type 'clutch-db-error))

(ert-deftest clutch-db-test-mongodb-eval-translates-count-distinct-index-helpers ()
  "Native MongoDB eval should translate count, distinct, and index helpers."
  (let (calls)
    (cl-letf (((symbol-function 'mongodb-count-documents)
               (lambda (client database collection filter &optional options)
                 (push (list 'count client database collection filter options)
                       calls)
                 7))
              ((symbol-function 'mongodb-distinct)
               (lambda (client database collection field &optional filter options)
                 (push (list 'distinct client database collection field
                             filter options)
                       calls)
                 '("a" "b")))
              ((symbol-function 'mongodb-list-indexes)
               (lambda (client database collection)
                 (push (list 'list-indexes client database collection) calls)
                 '((("name" . "_id_")))))
              ((symbol-function 'mongodb-create-index)
               (lambda (client database collection keys &optional options)
                 (push (list 'create-index client database collection keys
                             options)
                       calls)
                 '(("ok" . 1))))
              ((symbol-function 'mongodb-drop-index)
               (lambda (client database collection index)
                 (push (list 'drop-index client database collection index)
                       calls)
                 '(("ok" . 1)))))
      (clutch-mongodb--eval
       (clutch-db-test--make-mongodb-conn "app" 'client)
       (concat
        "db.users.countDocuments({active: true}, {limit: 10});"
        "db.users.estimatedDocumentCount();"
        "db.users.distinct('name', {active: true});"
        "db.users.listIndexes();"
        "db.users.createIndex({active: 1}, {name: 'active_idx'});"
        "db.users.dropIndex('active_idx')")))
    (setq calls (nreverse calls))
    (should (= (length calls) 6))
    (pcase-let ((`(count ,client ,database ,collection ,filter ,options)
                 (nth 0 calls)))
      (should (eq client 'client))
      (should (equal database "app"))
      (should (equal collection "users"))
      (should (mongodb-document-p filter))
      (should (equal (mongodb-document-pairs filter)
                     '(("active" . t))))
      (should (mongodb-document-p options))
      (should (equal (mongodb-document-pairs options)
                     '(("limit" . 10)))))
    (pcase-let ((`(count ,_client ,_database ,_collection ,filter ,options)
                 (nth 1 calls)))
      (should-not filter)
      (should-not options))
    (pcase-let ((`(distinct ,_client ,_database ,_collection ,field
                            ,filter ,options)
                 (nth 2 calls)))
      (should (equal field "name"))
      (should (mongodb-document-p filter))
      (should (equal (mongodb-document-pairs filter)
                     '(("active" . t))))
      (should-not options))
    (should (equal (nth 3 calls)
                   '(list-indexes client "app" "users")))
    (pcase-let ((`(create-index ,_client ,_database ,_collection ,keys
                                ,options)
                 (nth 4 calls)))
      (should (mongodb-document-p keys))
      (should (equal (mongodb-document-pairs keys)
                     '(("active" . 1))))
      (should (mongodb-document-p options))
      (should (equal (mongodb-document-pairs options)
                     '(("name" . "active_idx")))))
    (should (equal (nth 5 calls)
                   '(drop-index client "app" "users" "active_idx")))))

(ert-deftest clutch-db-test-mongodb-eval-translates-update-helpers ()
  "Native MongoDB eval should translate update helpers to mongodb.el calls."
  (let (calls)
    (cl-letf (((symbol-function 'mongodb-update)
               (lambda (client database collection filter update
                             &optional multi options)
                 (push (list client database collection filter update
                             multi options)
                       calls)
                 '(("ok" . 1)))))
      (clutch-mongodb--eval
       (clutch-db-test--make-mongodb-conn "app" 'client)
       (concat
        "db.users.updateOne({name: 'Ann'}, {$set: {seen: true}}, "
        "{upsert: true});"
        "db.users.updateMany({active: true}, {$inc: {visits: 1}});"
        "db.users.replaceOne({_id: 'b'}, {_id: 'b', name: 'Bob'})")))
    (setq calls (nreverse calls))
    (should (= (length calls) 3))
    (pcase-let ((`(,client ,database ,collection ,filter ,update
                   ,multi ,options)
                 (car calls)))
      (should (eq client 'client))
      (should (equal database "app"))
      (should (equal collection "users"))
      (should (mongodb-document-p filter))
      (should (equal (mongodb-document-pairs filter)
                     '(("name" . "Ann"))))
      (should (mongodb-document-p update))
      (let ((set-doc (cdr (assoc "$set"
                                  (mongodb-document-pairs update)))))
        (should (mongodb-document-p set-doc))
        (should (equal (mongodb-document-pairs set-doc)
                       '(("seen" . t)))))
      (should-not multi)
      (should (mongodb-document-p options))
      (should (equal (mongodb-document-pairs options)
                     '(("upsert" . t)))))
    (pcase-let ((`(,_client ,_database ,_collection ,filter ,update
                   ,multi ,options)
                 (cadr calls)))
      (should (mongodb-document-p filter))
      (should (equal (mongodb-document-pairs filter)
                     '(("active" . t))))
      (should (mongodb-document-p update))
      (let ((inc-doc (cdr (assoc "$inc"
                                  (mongodb-document-pairs update)))))
        (should (mongodb-document-p inc-doc))
        (should (equal (mongodb-document-pairs inc-doc)
                       '(("visits" . 1)))))
      (should (eq multi t))
      (should-not options))
    (pcase-let ((`(,_client ,_database ,_collection ,filter ,replacement
                   ,multi ,options)
                 (caddr calls)))
      (should (mongodb-document-p filter))
      (should (equal (mongodb-document-pairs filter)
                     '(("_id" . "b"))))
      (should (mongodb-document-p replacement))
      (should (equal (mongodb-document-pairs replacement)
                     '(("_id" . "b")
                       ("name" . "Bob"))))
      (should-not multi)
      (should-not options))))

(ert-deftest clutch-db-test-mongodb-mql-parses-bson-constructors ()
  "Native MongoDB MQL parsing should preserve supported BSON constructors."
  (should (= (clutch-mongodb--mql-iso-date-millis
              "2024-01-02T03:04:05.678Z")
             1704164645678))
  (should (= (clutch-mongodb--mql-iso-date-millis
              "1970-01-01T00:00:00+08:00")
             -28800000))
  (let (captured-filter)
    (cl-letf (((symbol-function 'mongodb-find)
               (lambda (_client _database _collection filter
                              _projection _limit _skip _sort
                              &optional _options)
                 (setq captured-filter filter)
                 nil)))
      (clutch-mongodb--eval
       (clutch-db-test--make-mongodb-conn "app" 'client)
       (concat
        "db.events.find({"
        "createdAt: ISODate('2024-01-02T03:04:05.678Z'), "
        "ts: Timestamp(1700000000, 7), "
        "a: Int32('7'), b: NumberInt(8), "
        "c: Long(7), d: NumberLong('9223372036854775807'), "
        "price: Decimal128('12.3400'), tax: NumberDecimal('1.23')"
        "})")))
    (should (mongodb-document-p captured-filter))
    (let* ((pairs (mongodb-document-pairs captured-filter))
           (created-at (cdr (assoc "createdAt" pairs)))
           (timestamp (cdr (assoc "ts" pairs)))
           (a (cdr (assoc "a" pairs)))
           (b (cdr (assoc "b" pairs)))
           (c (cdr (assoc "c" pairs)))
           (d (cdr (assoc "d" pairs)))
           (price (cdr (assoc "price" pairs)))
           (tax (cdr (assoc "tax" pairs))))
      (should (mongodb-datetime-p created-at))
      (should (= (mongodb-datetime-millis created-at) 1704164645678))
      (should (mongodb-timestamp-p timestamp))
      (should (= (mongodb-timestamp-seconds timestamp) 1700000000))
      (should (= (mongodb-timestamp-increment timestamp) 7))
      (should (mongodb-int32-p a))
      (should (= (mongodb-int32-value a) 7))
      (should (mongodb-int32-p b))
      (should (= (mongodb-int32-value b) 8))
      (should (mongodb-int64-p c))
      (should (= (mongodb-int64-value c) 7))
      (should (mongodb-int64-p d))
      (should (= (mongodb-int64-value d) 9223372036854775807))
      (should (mongodb-decimal128-p price))
      (should (equal (mongodb-decimal128-value price) "12.3400"))
      (should (mongodb-decimal128-p tax))
      (should (equal (mongodb-decimal128-value tax) "1.23")))))

(ert-deftest clutch-db-test-mongodb-mql-rejects-regex-literals ()
  "Native MongoDB MQL parsing should keep regex literals outside basic support."
  (should-error
   (clutch-mongodb--eval
    (clutch-db-test--make-mongodb-conn "app" 'client)
    "db.users.find({name: /ann/i})")
   :type 'clutch-db-error))

(ert-deftest clutch-db-test-mongodb-run-command-uses-current-database ()
  "Native MongoDB db.runCommand() should execute on the current database."
  (let (captured)
    (cl-letf (((symbol-function 'mongodb-command)
               (lambda (client database command &optional _timeout)
                 (setq captured (list client database command))
                 '(("ok" . 1)))))
      (should (equal
               (clutch-mongodb--eval
                (clutch-db-test--make-mongodb-conn "app" 'client)
                "db.runCommand({ping: 1})")
               '(("ok" . 1)))))
    (pcase-let ((`(,client ,database ,command) captured))
      (should (eq client 'client))
      (should (equal database "app"))
      (should (mongodb-document-p command))
      (should (equal (mongodb-document-pairs command)
                     '(("ping" . 1)))))))

(ert-deftest clutch-db-test-mongodb-eval-translates-create-collection ()
  "Native MongoDB db.createCollection() should call the protocol helper."
  (let (captured)
    (cl-letf (((symbol-function 'mongodb-create-collection)
               (lambda (client database collection &optional options)
                 (setq captured (list client database collection options))
                 '(("ok" . 1)))))
      (should (equal
               (clutch-mongodb--eval
                (clutch-db-test--make-mongodb-conn "app" 'client)
                "db.createCollection('events', {capped: true, size: 4096})")
               '(("ok" . 1)))))
    (pcase-let ((`(,client ,database ,collection ,options) captured))
      (should (eq client 'client))
      (should (equal database "app"))
      (should (equal collection "events"))
      (should (mongodb-document-p options))
      (should (equal (mongodb-document-pairs options)
                     '(("capped" . t)
                       ("size" . 4096)))))))

(ert-deftest clutch-db-test-mongodb-eval-translates-sibling-db-helpers ()
  "Native MongoDB getSiblingDB() should target commands at the sibling database."
  (let (commands collections finds)
    (cl-letf (((symbol-function 'mongodb-command)
               (lambda (client database command &optional _timeout)
                 (push (list client database command) commands)
                 '(("ok" . 1))))
              ((symbol-function 'mongodb-create-collection)
               (lambda (client database collection &optional options)
                 (push (list client database collection options) collections)
                 '(("ok" . 1))))
              ((symbol-function 'mongodb-find)
               (lambda (client database collection filter projection limit skip
                             sort &optional options)
                 (push (list client database collection filter projection
                             limit skip sort options)
                       finds)
                 '((("_id" . "a"))))))
      (let ((conn (clutch-db-test--make-mongodb-conn "app" 'client)))
        (should (equal (clutch-mongodb--eval conn "db.getName()")
                       "app"))
        (should (equal (clutch-mongodb--eval
                        conn
                        "db.getSiblingDB('analytics')")
                       "analytics"))
        (should (equal (clutch-mongodb--eval
                        conn
                        "db.getSiblingDB('analytics').getName()")
                       "analytics"))
        (clutch-mongodb--eval
         conn
         (concat
          "db.getSiblingDB('analytics').runCommand({ping: 1});"
          "db.getSiblingDB('analytics').createCollection('events');"
          "db.getSiblingDB('analytics').getCollection('users').find({_id: 'a'});"
          "db.getSiblingDB('analytics').orders.find({_id: 'b'})"))))
    (setq commands (nreverse commands)
          collections (nreverse collections)
          finds (nreverse finds))
    (should (= (length commands) 1))
    (should (= (length collections) 1))
    (should (= (length finds) 2))
    (pcase-let ((`(,client ,database ,command) (car commands)))
      (should (eq client 'client))
      (should (equal database "analytics"))
      (should (mongodb-document-p command))
      (should (equal (mongodb-document-pairs command)
                     '(("ping" . 1)))))
    (pcase-let ((`(,client ,database ,collection ,options)
                 (car collections)))
      (should (eq client 'client))
      (should (equal database "analytics"))
      (should (equal collection "events"))
      (should-not options))
    (pcase-let ((`(,client ,database ,collection ,filter ,_projection
                         ,_limit ,_skip ,_sort ,_options)
                 (car finds)))
      (should (eq client 'client))
      (should (equal database "analytics"))
      (should (equal collection "users"))
      (should (equal (mongodb-document-pairs filter)
                     '(("_id" . "a")))))
    (pcase-let ((`(,_client ,database ,collection ,filter ,_projection
                          ,_limit ,_skip ,_sort ,_options)
                 (cadr finds)))
      (should (equal database "analytics"))
      (should (equal collection "orders"))
      (should (equal (mongodb-document-pairs filter)
                     '(("_id" . "b")))))))

(ert-deftest clutch-db-test-mongodb-metadata-uses-mongodb-helpers ()
  "Native MongoDB metadata should map databases and collections into Clutch objects."
  (let ((clutch-mongodb-schema-sample-size 2)
        sample-codes)
    (cl-letf (((symbol-function 'clutch-mongodb--eval)
               (lambda (_conn code)
                 (cond
                  ((equal code "db.getCollectionNames()")
                   '("users" "orders"))
                  ((string-match-p
                    (regexp-quote "db.getCollection(\"users\").find({}).limit(2)")
                    code)
                   (push code sample-codes)
                   '((("_id" . (("$oid" . "64f")))
                      ("name" . "Ann")
                      ("score" . 10)
                      ("profile" . (("age" . 30))))
                     (("_id" . (("$oid" . "650")))
                      ("score" . "high")
                      ("active" . :false))))
                  ((string-match-p "getCollectionInfos" code)
                   '((("name" . "users")
                      ("type" . "collection"))))
                  (t (error "unexpected code: %s" code))))))
      (let ((conn (clutch-db-test--make-mongodb-conn)))
        (should (equal (clutch-db-list-schemas conn) '("app")))
        (should (equal (clutch-db-list-tables conn) '("users" "orders")))
        (should (equal (clutch-db-list-table-entries conn)
                       '((:name "users" :schema "app" :type "COLLECTION")
                         (:name "orders" :schema "app" :type "COLLECTION"))))
        (should (equal (clutch-db-complete-tables conn "us") '("users")))
        (should (equal (clutch-db-list-columns conn "users")
                       '("_id" "name" "score" "profile" "profile.age" "active")))
        (should (equal (clutch-db-column-details conn "users")
                       '((:name "_id"
                          :type "BSON<object>"
                          :type-category json
                          :nullable t
                          :comment nil)
                         (:name "name"
                          :type "BSON<string>"
                          :type-category text
                          :nullable t
                          :comment "present in 1/2 sampled documents")
                         (:name "score"
                          :type "BSON<number|string>"
                          :type-category text
                          :nullable t
                          :comment nil)
                         (:name "profile"
                          :type "BSON<object>"
                          :type-category json
                          :nullable t
                          :comment "present in 1/2 sampled documents")
                         (:name "profile.age"
                          :type "BSON<number>"
                          :type-category numeric
                          :nullable t
                          :comment "present in 1/2 sampled documents")
                         (:name "active"
                          :type "BSON<bool>"
                          :type-category text
                          :nullable t
                          :comment "present in 1/2 sampled documents"))))
        (should (equal (length sample-codes) 2))
        (should (string-match-p "\"users\""
                                (clutch-db-object-definition
                                 conn '(:name "users" :type "COLLECTION"))))))))

(ert-deftest clutch-db-test-mongodb-schema-sample-size-rejects-invalid ()
  "Native MongoDB metadata should reject invalid schema sample sizes."
  (let ((clutch-mongodb-schema-sample-size 0))
    (should-error (clutch-mongodb--schema-sample-limit)
                  :type 'clutch-db-error)))

(ert-deftest clutch-db-test-mongodb-schema-sampling-rejects-non-documents ()
  "Native MongoDB metadata should reject non-document sampling results."
  (cl-letf (((symbol-function 'clutch-mongodb--eval)
             (lambda (&rest _args) 42)))
    (should-error
     (clutch-mongodb--sample-documents
      (clutch-db-test--make-mongodb-conn)
      "users")
     :type 'clutch-db-error)))

(ert-deftest clutch-db-test-mongodb-collection-validation-extracts-options ()
  "Native MongoDB should expose collection validation metadata."
  (let ((metadata '((("name" . "users")
                     ("type" . "collection")
                     ("options"
                      . (("validator"
                          . (("$jsonSchema"
                               . (("required" . ["status"])))))
                         ("validationAction" . "warn")
                         ("validationLevel" . "moderate"))))))
        captured-code)
    (cl-letf (((symbol-function 'clutch-mongodb--eval)
               (lambda (_conn code)
                 (setq captured-code code)
                 metadata)))
      (should
       (equal
        (clutch-db-object-action-metadata
         (clutch-db-test--make-mongodb-conn)
         '(:name "users" :type "COLLECTION")
         'show-validation)
        "{\"collection\":\"users\",\"configured\":true,\"validationAction\":\"warn\",\"validationLevel\":\"moderate\",\"validator\":{\"$jsonSchema\":{\"required\":[\"status\"]}}}"))
      (should (equal captured-code
                     "db.getCollectionInfos({name: \"users\"})")))))

(ert-deftest clutch-db-test-mongodb-collection-stats-uses-collstats-stage ()
  "Native MongoDB should expose collection storage statistics."
  (let (captured)
    (cl-letf (((symbol-function 'mongodb-aggregate)
               (lambda (client database collection pipeline &optional options)
                 (setq captured
                       (list client database collection pipeline options))
                 '((("ns" . "app.users")
                    ("storageStats"
                     . (("count" . 3)
                        ("size" . 246)
                        ("avgObjSize" . 82)
                        ("storageSize" . 20480)
                        ("nindexes" . 2)
                        ("totalIndexSize" . 40960)
                        ("totalSize" . 61440)
                        ("indexSizes" . (("_id_" . 20480)
                                         ("field_idx" . 20480))))))))))
      (let ((text
             (clutch-db-object-action-metadata
              (clutch-db-test--make-mongodb-conn "app")
              '(:name "users" :type "COLLECTION")
              'show-stats)))
        (should
         (equal
          text
          (concat
           "{\"collection\":\"users\",\"namespace\":\"app.users\","
           "\"count\":3,\"size\":246,\"avgObjSize\":82,"
           "\"storageSize\":20480,\"nindexes\":2,"
           "\"totalIndexSize\":40960,\"totalSize\":61440,"
           "\"indexSizes\":{\"_id_\":20480,\"field_idx\":20480}}")))))
    (pcase-let ((`(,client ,database ,collection ,pipeline ,options)
                 captured))
      (should (eq client 'mongodb-client))
      (should (equal database "app"))
      (should (equal collection "users"))
      (should-not options)
      (should (vectorp pipeline))
      (should (= (length pipeline) 1))
      (let* ((stage (aref pipeline 0))
             (coll-stats (clutch-mongodb--document-value
                          stage "$collStats"))
             (storage-stats (clutch-mongodb--document-value
                             coll-stats "storageStats")))
        (should (mongodb-document-p stage))
        (should (mongodb-document-p coll-stats))
        (should (mongodb-document-p storage-stats))
        (should-not (mongodb-document-elements storage-stats))))))

(ert-deftest clutch-db-test-mongodb-collection-profile-samples-nested-fields ()
  "Native MongoDB collection profiles should include nested field stats."
  (let ((clutch-mongodb-schema-sample-size 3)
        (sample-docs '((("_id" . (("$oid" . "64f")))
                        ("name" . "Ann")
                        ("profile" . (("age" . 30)))
                        ("items" . [(("sku" . "A") ("qty" . 2))])
                        ("status" . "active"))
                       (("_id" . (("$oid" . "650")))
                        ("name" . "Bob")
                        ("profile" . (("age" . 40)))
                        ("items" . [(("sku" . "A") ("qty" . 1))
                                     (("sku" . "B") ("qty" . 4))])
                        ("status" . "active"))
                       (("_id" . (("$oid" . "651")))
                        ("name" . "Cal")
                        ("status" . "blocked"))))
        (id-index (mongodb-document
                   `(("name" . "_id_")
                     ("key" . ,(mongodb-document '(("_id" . 1))))))))
    (cl-letf (((symbol-function 'clutch-mongodb--eval)
               (lambda (_conn code)
                 (should (string-match-p
                          "db.getCollection(\"users\").find({}).limit(3)"
                          code))
                 sample-docs))
              ((symbol-function 'mongodb-list-indexes)
               (lambda (_client _database collection)
                 (should (equal collection "users"))
                 (list id-index))))
      (let* ((text (clutch-db-collection-profile
                    (clutch-db-test--make-mongodb-conn "app")
                    "users"))
             (profile (json-parse-string text :object-type 'alist
                                         :array-type 'list))
             (fields (cdr (assoc 'fields profile)))
             (age (seq-find (lambda (field)
                              (equal (cdr (assoc 'path field))
                                     "profile.age"))
                            fields))
             (items (seq-find (lambda (field)
                                (equal (cdr (assoc 'path field))
                                       "items"))
                              fields))
             (item-sku (seq-find (lambda (field)
                                   (equal (cdr (assoc 'path field))
                                          "items.sku"))
                                 fields))
             (status (seq-find (lambda (field)
                                 (equal (cdr (assoc 'path field))
                                        "status"))
                               fields)))
        (should (= (cdr (assoc 'sampleSize profile)) 3))
        (should age)
        (should (equal (cdr (assoc 'type age)) "BSON<number>"))
        (should (= (cdr (assoc 'present age)) 2))
        (should items)
        (should (equal (cdr (assoc 'type items)) "BSON<array>"))
        (should item-sku)
        (should (equal (cdr (assoc 'type item-sku)) "BSON<string>"))
        (should (= (cdr (assoc 'present item-sku)) 2))
        (should (equal (cdr (assoc 'topValues status))
                       '(((value . "active") (count . 2))
                         ((value . "blocked") (count . 1)))))))))

(ert-deftest clutch-db-test-mongodb-index-insight-combines-definitions-and-usage ()
  "Native MongoDB index insight should combine listIndexes and indexStats."
  (let ((captured-pipeline nil)
        (id-index (mongodb-document
                   `(("name" . "_id_")
                     ("key" . ,(mongodb-document '(("_id" . 1)))))))
        (email-index (mongodb-document
                      `(("name" . "email_idx")
                        ("key" . ,(mongodb-document '(("email" . 1))))
                        ("unique" . t)))))
    (cl-letf (((symbol-function 'mongodb-list-indexes)
               (lambda (_client database collection)
                 (should (equal database "app"))
                 (should (equal collection "users"))
                 (list id-index email-index)))
              ((symbol-function 'mongodb-aggregate)
               (lambda (_client database collection pipeline &optional options)
                 (should (equal database "app"))
                 (should (equal collection "users"))
                 (should-not options)
                 (setq captured-pipeline pipeline)
                 '((("name" . "email_idx")
                    ("host" . "localhost:27017")
                    ("accesses" . (("ops" . 9)
                                   ("since" . "2026-06-11T00:00:00Z"))))))))
      (let* ((text (clutch-db-object-action-metadata
                    (clutch-db-test--make-mongodb-conn "app")
                    '(:name "users" :type "COLLECTION")
                    'index-insight))
             (insight (json-parse-string text :object-type 'alist
                                         :array-type 'list))
             (indexes (cdr (assoc 'indexes insight)))
             (email (seq-find (lambda (index)
                                (equal (cdr (assoc 'name index))
                                       "email_idx"))
                              indexes)))
        (should (vectorp captured-pipeline))
        (should (assoc "$indexStats"
                       (mongodb-document-pairs (aref captured-pipeline 0))))
        (should (eq (cdr (assoc 'unique email)) t))
        (should (equal (cdr (assoc 'usage email))
                       '((ops . 9)
                         (since . "2026-06-11T00:00:00Z"))))))))

(ert-deftest clutch-db-test-mongodb-explain-query-summarizes-current-helper ()
  "Native MongoDB explain should summarize the current find helper."
  (let (captured)
    (cl-letf (((symbol-function 'mongodb-explain)
               (lambda (client database command &optional verbosity)
                 (setq captured (list client database command verbosity))
                 '(("queryPlanner"
                    . (("winningPlan"
                        . (("stage" . "IXSCAN")
                           ("indexName" . "active_1")))))
                   ("executionStats"
                    . (("nReturned" . 5)
                       ("totalKeysExamined" . 5)
                       ("totalDocsExamined" . 5)
                       ("executionTimeMillis" . 2)))))))
      (let* ((text (clutch-db-explain-query
                    (clutch-db-test--make-mongodb-conn "app" 'client)
                    "db.users.find({active: true}).limit(5)"))
             (doc (json-parse-string text :object-type 'alist
                                     :array-type 'list))
             (summary (cdr (assoc 'summary doc))))
        (pcase-let ((`(,client ,database ,command ,verbosity) captured))
          (should (eq client 'client))
          (should (equal database "app"))
          (should (equal (cdr (assoc "find" command)) "users"))
          (should (= (cdr (assoc "limit" command)) 5))
          (should (equal verbosity "executionStats")))
        (should (equal (cdr (assoc 'winningStage summary)) "IXSCAN"))
        (should (equal (cdr (assoc 'indexName summary)) "active_1"))
        (should (eq (cdr (assoc 'collectionScan summary)) :false))
        (should (= (cdr (assoc 'totalDocsExamined summary)) 5))))))

(ert-deftest clutch-db-test-mongodb-set-current-schema-updates-database ()
  "Native MongoDB schema switching should change the logical database."
  (let* ((client (make-mongodb-conn :database "app" :closed nil))
         (conn (clutch-db-test--make-mongodb-conn "app" client)))
    (should (equal (clutch-db-current-schema conn) "app"))
    (clutch-db-set-current-schema conn "analytics")
    (should (equal (clutch-db-current-schema conn) "analytics"))
    (should (equal (clutch-db-database conn) "analytics"))
    (let (captured-database)
      (cl-letf (((symbol-function 'mongodb-find)
                 (lambda (_client database _collection _filter
                                  _projection _limit _skip _sort
                                  &optional _options)
                   (setq captured-database database)
                   nil)))
        (clutch-db-query conn "db.users.find()"))
      (should (equal captured-database "analytics")))))

(ert-deftest clutch-db-test-jdbc-clickhouse-list-table-entries-uses-system-tables ()
  "ClickHouse table discovery should query the current database."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default")))
        captured-sql)
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 (make-clutch-db-result
                  :rows '(("events" "MergeTree")
                          ("daily_mv" "View"))))))
      (should
       (equal (clutch-db-list-table-entries conn)
              '((:name "events" :type "TABLE" :schema "default"
                 :source-schema "default")
                (:name "daily_mv" :type "VIEW" :schema "default"
                 :source-schema "default"))))
      (should (string-match-p "FROM system\\.tables" captured-sql))
      (should (string-match-p "database = 'default'" captured-sql)))))

(ert-deftest clutch-db-test-jdbc-clickhouse-search-table-entries-filters-system-tables ()
  "ClickHouse table search should filter discovered table entries by prefix."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default"))))
    (cl-letf (((symbol-function 'clutch-jdbc--clickhouse-table-entries)
               (lambda (_conn)
                 '((:name "events" :type "TABLE" :schema "default")
                   (:name "clutch_live_smoke" :type "TABLE"
                    :schema "default" :source-schema "default")))))
      (should
       (equal (clutch-db-search-table-entries conn "clutch")
              '((:name "clutch_live_smoke" :type "TABLE"
                 :schema "default" :source-schema "default")))))))

(ert-deftest clutch-db-test-jdbc-clickhouse-column-details-omit-catalog ()
  "ClickHouse column metadata RPCs should omit catalog from metadata calls."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default")))
        calls)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (push (cons op params) calls)
                 (pcase op
                   ("get-primary-keys" '(:primary-keys ("id")))
                   ("get-foreign-keys" '(:foreign-keys nil))
                   ("get-columns" `(:columns ((:name "id" :type "UInt64"
                                               :nullable ,clutch-jdbc--json-false)
                                              (:name "name" :type "String"
                                               :nullable t))))
                   (_ (ert-fail (format "unexpected op: %s" op)))))))
      (should
       (equal (clutch-db-column-details conn "events")
              '((:name "id" :type "UInt64" :nullable nil
                 :primary-key t :foreign-key nil :comment nil)
                (:name "name" :type "String" :nullable t
                 :primary-key nil :foreign-key nil :comment nil))))
      (dolist (call calls)
        (should-not (alist-get 'catalog (cdr call)))
        (should-not (alist-get 'schema (cdr call)))))))

;;;; Unit tests — clutch-db-complete-tables (Oracle)

(ert-deftest clutch-db-test-jdbc-complete-tables-searches-rpc-without-schema-cache-dependency ()
  "JDBC completion should search remotely without reading Clutch schema cache."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 5
                                      :params '(:driver oracle :user "scott")))
         captured-op)
    (cl-letf (((symbol-function 'clutch--schema-status-entry)
               (lambda (&rest _)
                 (ert-fail "JDBC backend should not read schema status")))
              ((symbol-function 'clutch--schema-for-connection)
               (lambda (&rest _)
                 (ert-fail "JDBC backend should not read schema cache")))
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (setq captured-op op)
                 (should (equal (alist-get 'prefix params) "US"))
                 '(:tables ((:name "USERS") (:name "USER_ROLES"))))))
      (let ((result (clutch-db-complete-tables conn "US")))
        (should (equal captured-op "search-tables"))
        (should (equal (sort (copy-sequence result) #'string<)
                       '("USERS" "USER_ROLES")))))))

(ert-deftest clutch-db-test-jdbc-rpc-async-times-out-and-cleans-up ()
  "Async JDBC RPC should call ERRBACK and clear state on timeout."
  (let ((clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
        (clutch-jdbc-rpc-timeout-seconds 1)
        timeout-message
        timer-fn)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send)
               (lambda (_op _params) 77))
              ((symbol-function 'run-at-time)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer)))
      (clutch-jdbc--rpc-async
       "get-tables" '((conn-id . 1))
       #'ignore
       (lambda (message)
         (setq timeout-message message)))
      (funcall timer-fn)
      (should (string-match-p "timeout waiting for async response" timeout-message))
      (should-not (gethash 77 clutch-jdbc--async-callbacks)))))

(ert-deftest clutch-db-test-jdbc-dispatch-async-response-remembers-conn-scoped-errors ()
  "Async JDBC failures should remember diagnostics and call ERRBACK."
  (let ((clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
        remembered
        errback-message)
    (puthash 77 (list :errback (lambda (message)
                                 (setq errback-message message))
                      :conn 'conn-77
                      :op 'get-tables)
             clutch-jdbc--async-callbacks)
    (cl-letf (((symbol-function 'run-at-time)
               (lambda (_secs _repeat fn &rest args)
                 (apply fn args)
                 nil))
              ((symbol-function 'clutch-jdbc--remember-error-response)
               (lambda (conn op response)
                 (setq remembered (list conn op response))))
              ((symbol-function 'message)
               (lambda (&rest _args) nil)))
      (should (clutch-jdbc--dispatch-async-response
               '(:id 77 :ok :json-false :error "metadata blew up")))
      (should (equal remembered
                     '(conn-77 get-tables
                               (:id 77 :ok :json-false :error "metadata blew up"))))
      (should (equal errback-message "metadata blew up")))))

(ert-deftest clutch-db-test-jdbc-validate-agent-jar-rejects-mismatch ()
  "JDBC agent startup should reject a jar with the wrong checksum."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (clutch-jdbc-agent-sha256 "deadbeef"))
    (unwind-protect
        (progn
          (with-temp-file jar
            (insert "not a release jar"))
          (should-error (clutch-jdbc--validate-agent-jar jar) :type 'user-error))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-ensure-agent-cleans-stale-jars ()
  "Ensuring the agent should keep only the current versioned jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (stale-a (expand-file-name "clutch-jdbc-agent-0.1.0.jar" tmpdir))
         (stale-b (expand-file-name "clutch-jdbc-agent-0.1.1.jar" tmpdir))
         (clutch-jdbc-agent-sha256 nil))
    (unwind-protect
        (progn
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file jar
            (insert "current"))
          (with-temp-file stale-a
            (insert "old-a"))
          (with-temp-file stale-b
            (insert "old-b"))
          (clutch-jdbc-ensure-agent)
          (should (file-exists-p jar))
          (should-not (file-exists-p stale-a))
          (should-not (file-exists-p stale-b)))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-ensure-agent-allows-custom-jar-when-checksum-disabled ()
  "Checksum verification can be disabled for a local custom jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (clutch-jdbc-agent-sha256 nil)
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir)))
    (unwind-protect
        (progn
          (with-temp-file jar
            (insert "custom build"))
          (should (clutch-jdbc--agent-jar-valid-p jar))
          (should (progn (clutch-jdbc--validate-agent-jar jar) t)))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-requires-agent-command ()
  "Missing JDBC agent should instruct the user to run the explicit install command."
  (let ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
        (clutch-jdbc-agent-version "0.1.2")
        (clutch-jdbc-agent-dir nil))
    (setq clutch-jdbc-agent-dir tmpdir)
    (unwind-protect
        (let ((err (should-error (clutch-jdbc--setup-prerequisites 'oracle)
                                 :type 'user-error)))
          (should (string-match-p
                   "Run M-x clutch-jdbc-ensure-agent"
                   (cadr err))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-points-to-install-driver ()
  "Missing Maven drivers should point users at `clutch-jdbc-install-driver'."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2"))
    (unwind-protect
        (progn
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file jar (insert "placeholder"))
          (cl-letf (((symbol-function 'clutch-jdbc--agent-jar-valid-p)
                     (lambda (_jar) t)))
            (let ((err (should-error
                        (clutch-jdbc--setup-prerequisites 'sqlserver)
                        :type 'user-error)))
              (should (string-match-p
                       "Run M-x clutch-jdbc-install-driver RET sqlserver"
                       (cadr err))))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-reports-manual-driver-url ()
  "Missing manual JDBC drivers should report the download URL and destination."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (dest (expand-file-name "db2jcc4.jar" (expand-file-name "drivers" tmpdir))))
    (unwind-protect
        (progn
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file jar (insert "placeholder"))
          (cl-letf (((symbol-function 'clutch-jdbc--agent-jar-valid-p)
                     (lambda (_jar) t)))
            (let ((err (should-error (clutch-jdbc--setup-prerequisites 'db2)
                                     :type 'user-error)))
              (should (string-match-p "requires manual download" (cadr err)))
              (should (string-match-p "ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads"
                                      (cadr err)))
              (should (string-match-p (regexp-quote dest) (cadr err))))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-installs-oracle-i18n-companion ()
  "Installing Oracle JDBC should also install the orai18n companion jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         downloaded)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (_coords dest)
                     (push (file-name-nondirectory dest) downloaded)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'oracle)
          (should (member "ojdbc8.jar" downloaded))
          (should (member "orai18n.jar" downloaded)))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-removes-conflicting-oracle-jar ()
  "Installing an Oracle driver should remove the conflicting Oracle jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (_coords dest)
                     (with-temp-file dest (insert "jar")))))
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file (expand-file-name "drivers/ojdbc11.jar" tmpdir)
            (insert "jar"))
          (clutch-jdbc-install-driver 'oracle)
          (should (file-exists-p (expand-file-name "drivers/ojdbc8.jar" tmpdir)))
          (should-not (file-exists-p (expand-file-name "drivers/ojdbc11.jar" tmpdir))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-uses-sqlserver-jre11-artifact ()
  "Installing SQL Server JDBC should use the classifier-based Maven artifact."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         requested-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (setq requested-coords coords)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'sqlserver)
          (should (equal requested-coords
                         "com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre11"))
          (should (file-exists-p (expand-file-name "drivers/mssql-jdbc.jar" tmpdir))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-uses-duckdb-artifact ()
  "Installing DuckDB JDBC should use the Maven Central driver artifact."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         requested-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (setq requested-coords coords)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'duckdb)
          (should (equal requested-coords
                         "org.duckdb:duckdb_jdbc:1.5.3.0"))
          (should (file-exists-p (expand-file-name "drivers/duckdb_jdbc.jar" tmpdir))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-mongodb-installs-sql-interface-artifact ()
  "Installing MongoDB JDBC should install the SQL Interface driver artifact."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         requested-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (setq requested-coords coords)
                     (make-directory (file-name-directory dest) t)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'mongodb)
          (should (equal requested-coords
                         "org.mongodb:mongodb-jdbc:3.0.6:all"))
          (should (file-exists-p (expand-file-name "drivers/mongodb-jdbc.jar" tmpdir))))
      (delete-directory tmpdir t))))

;;;; Unit tests — props normalization

(ert-deftest clutch-db-test-jdbc-normalize-props-converts-plist ()
  "A plist :props should be converted to an alist before JSON encoding."
  (should (equal (clutch-jdbc--normalize-props '(:role "reporting" :schema "HR"))
                 '(("role" . "reporting") ("schema" . "HR"))))
  (should (equal (clutch-jdbc--normalize-props '(:key "val"))
                 '(("key" . "val"))))
  (should (null  (clutch-jdbc--normalize-props nil))))

(ert-deftest clutch-db-test-jdbc-normalize-props-passes-alist-through ()
  "An alist :props should be passed through unchanged."
  (let ((alist '(("role" . "reporting") ("schema" . "HR"))))
    (should (equal (clutch-jdbc--normalize-props alist) alist))))

(ert-deftest clutch-db-test-jdbc-connect-normalizes-plist-props ()
  "JDBC connect should send props as an alist even when given a plist."
  (let (captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 1))))
      (clutch-db-jdbc-connect
       'oracle
       '(:host "db" :port 1521 :database "svc"
         :user "scott" :password "tiger"
         :props (:role "reporting" :schema "HR"))))
    (should (equal (alist-get 'props captured-params)
                   '(("role" . "reporting") ("schema" . "HR"))))))

(ert-deftest clutch-db-test-jdbc-connect-generic-requires-driver-class ()
  "Generic JDBC connections must explicitly name the JDBC driver class."
  (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
            ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
            ((symbol-function 'clutch-jdbc--rpc)
             (lambda (&rest _args)
               (error "connect RPC should not run without :driver-class"))))
    (let ((err (should-error
                (clutch-db-jdbc-connect
                 'jdbc
                 '(:url "jdbc:kingbase8://127.0.0.1:54321/test"
                   :user "system"))
                :type 'clutch-db-error)))
      (should (string-match-p ":driver-class"
                              (error-message-string err))))))

(ert-deftest clutch-db-test-jdbc-connect-generic-sends-driver-class ()
  "Generic JDBC connections should pass :driver-class through to the agent."
  (let (captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 12))))
      (clutch-db-jdbc-connect
       'jdbc
       '(:url "jdbc:kingbase8://127.0.0.1:54321/test"
         :driver-class "com.kingbase8.Driver"
         :user "system")))
    (should (equal (alist-get 'driver-class captured-params)
                   "com.kingbase8.Driver"))))

;;;; Unit tests — row normalization

(ert-deftest clutch-db-test-jdbc-normalize-row-clob ()
  "Clob plists should be replaced with their :preview string."
  (let ((row (list "text"
                   '(:__type "clob" :length 1000 :preview "hello clob")
                   42)))
    (should (equal (clutch-jdbc--normalize-row row)
                   '("text" "hello clob" 42)))))

(ert-deftest clutch-db-test-jdbc-normalize-row-clob-nil-preview ()
  "Clob plists with no preview should normalize to nil."
  (let ((row (list '(:__type "clob" :length 0))))
    (should (equal (clutch-jdbc--normalize-row row) '(nil)))))

(ert-deftest clutch-db-test-jdbc-normalize-row-blob-with-text ()
  "Blob plists with :text should still normalize to the text string."
  (let ((row (list '(:__type "blob" :length 5 :text "hello"))))
    (should (equal (clutch-jdbc--normalize-row row) '("hello")))))

(ert-deftest clutch-db-test-jdbc-normalize-row-plain-values ()
  "Plain values should pass through normalize-row unchanged."
  (let ((row '(1 "str" nil t)))
    (should (equal (clutch-jdbc--normalize-row row) row))))

;;;; Unit tests — Redshift driver support

(ert-deftest clutch-db-test-jdbc-build-url-redshift ()
  "Redshift URL builder should produce a jdbc:redshift URL with default port 5439."
  (should (equal (clutch-jdbc--build-url
                  'redshift
                  '(:host "cluster.us-east-1.redshift.amazonaws.com" :database "mydb"))
                 "jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/mydb"))
  (should (equal (clutch-jdbc--build-url
                  'redshift
                  '(:host "cluster.example.com" :port 5440 :database "analytics"))
                 "jdbc:redshift://cluster.example.com:5440/analytics")))

(ert-deftest clutch-db-test-jdbc-display-name-redshift ()
  "Redshift connections should display as \"Redshift\"."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver redshift))))
    (should (equal (clutch-db-display-name conn) "Redshift"))))

(ert-deftest clutch-db-test-jdbc-install-driver-installs-redshift ()
  "Installing Redshift JDBC should download the redshift-jdbc42 Maven artifact."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         requested-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (setq requested-coords coords)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'redshift)
          (should (string-prefix-p "com.amazon.redshift:redshift-jdbc42:" requested-coords))
          (should (file-exists-p (expand-file-name "drivers/redshift-jdbc42.jar" tmpdir))))
      (delete-directory tmpdir t))))

;;;; Unit tests — ClickHouse driver support

(ert-deftest clutch-db-test-jdbc-build-url-clickhouse ()
  "ClickHouse URL builder should produce a jdbc:clickhouse URL with default port 8123."
  (should (equal (clutch-jdbc--build-url
                  'clickhouse
                  '(:host "ch.corp.com" :database "default"))
                 "jdbc:clickhouse://ch.corp.com:8123/default"))
  (should (equal (clutch-jdbc--build-url
                  'clickhouse
                  '(:host "ch.corp.com" :port 8443 :database "analytics"))
                 "jdbc:clickhouse://ch.corp.com:8443/analytics")))

(ert-deftest clutch-db-test-jdbc-display-name-clickhouse ()
  "ClickHouse connections should display as \"ClickHouse\"."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver clickhouse))))
    (should (eq (clutch-db-backend-key conn) 'clickhouse))
    (should (equal (clutch-db-display-name conn) "ClickHouse"))))

(ert-deftest clutch-db-test-jdbc-install-driver-installs-clickhouse ()
  "Installing ClickHouse JDBC should download the all-classifier artifact and companions."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         all-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (push coords all-coords)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'clickhouse)
          (should (cl-some (lambda (c) (string-match-p ":all\\'" c)) all-coords))
          (should (file-exists-p (expand-file-name "drivers/clickhouse-jdbc.jar" tmpdir)))
          (should (file-exists-p (expand-file-name "drivers/slf4j-api.jar" tmpdir)))
          (should (file-exists-p (expand-file-name "drivers/slf4j-nop.jar" tmpdir))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-download-maven-classifier ()
  "Maven downloader should handle 4-segment coords (with classifier)."
  (let (captured-url)
    (cl-letf (((symbol-function 'url-copy-file)
               (lambda (url _dest &rest _) (setq captured-url url))))
      ;; 4-segment: group:artifact:version:classifier
      (clutch-jdbc--download-maven-driver
       "com.clickhouse:clickhouse-jdbc:0.9.8:all" "/tmp/test.jar")
      (should (string-match-p "clickhouse-jdbc-0.9.8-all\\.jar" captured-url))
      ;; 3-segment: group:artifact:version (unchanged behavior)
      (clutch-jdbc--download-maven-driver
       "com.amazon.redshift:redshift-jdbc42:2.1.0.30" "/tmp/test2.jar")
      (should (string-match-p "redshift-jdbc42-2.1.0.30\\.jar" captured-url)))))

;;;; Unit tests — clutch-jdbc--conn-schema

(ert-deftest clutch-db-test-jdbc-conn-schema-oracle-defaults-to-user ()
  "Oracle with no explicit :schema returns the uppercased :user as the schema."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver oracle :user "app_user"))))
    (should (equal (clutch-jdbc--conn-schema conn) "APP_USER"))))

(ert-deftest clutch-db-test-jdbc-conn-schema-explicit-overrides-default ()
  "An explicit :schema is returned as-is, even for Oracle."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver oracle :user "app_user" :schema "REPORTING"))))
    (should (equal (clutch-jdbc--conn-schema conn) "REPORTING"))))

(ert-deftest clutch-db-test-jdbc-conn-schema-non-oracle-returns-nil ()
  "Non-Oracle drivers with no :schema return nil."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver sqlserver :user "sa"))))
    (should (null (clutch-jdbc--conn-schema conn)))))

(ert-deftest clutch-db-test-jdbc-list-schemas-filters-oracle-system-schemas ()
  "Oracle JDBC schema listing should filter common system schemas."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 7
               :params '(:driver oracle :user "app_user" :rpc-timeout 9))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op _params &optional _timeout-seconds)
                 '(:schemas ("SYS" "SYSTEM" "APP_USER" "ANALYTICS" "SALES")))))
      (should (equal (clutch-db-list-schemas conn)
                     '("APP_USER" "ANALYTICS" "SALES"))))))

(ert-deftest clutch-db-test-jdbc-set-current-schema-updates-params ()
  "Oracle JDBC schema switching should update both JDBC sessions and persist :schema."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 7
               :params '(:driver oracle :user "app_user" :rpc-timeout 9)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 7 :schema "ANALYTICS"))))
      (should (equal (clutch-db-set-current-schema conn "analytics") "ANALYTICS"))
      (should (equal captured-op "set-current-schema"))
      (should (= (alist-get 'conn-id captured-params) 7))
      (should (equal (alist-get 'schema captured-params) "ANALYTICS"))
      (should (= captured-timeout 9))
      (should (equal (plist-get (clutch-jdbc-conn-params conn) :schema)
                     "ANALYTICS")))))

(ert-deftest clutch-db-test-jdbc-set-current-schema-rejects-generic-driver ()
  "Generic JDBC connections should keep schema switching unsupported."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 7
               :params '(:driver jdbc :display-name "KingbaseES" :rpc-timeout 9))))
    (should-error
     (clutch-db-set-current-schema conn "public")
     :type 'user-error)))

(ert-deftest clutch-db-test-mysql-set-current-schema-updates-connection-database ()
  "MySQL schema switching should execute USE and update the connection database."
  (let ((conn (make-mysql-conn :database "sales"))
        executed-sql)
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (_conn sql)
                 (setq executed-sql sql)
                 (make-mysql-result :connection conn :affected-rows 0))))
      (should (equal (clutch-db-set-current-schema conn "analytics") "analytics"))
      (should (equal executed-sql "USE `analytics`"))
      (should (equal (mysql-current-database conn) "analytics")))))

;;;; Unit tests — clutch-jdbc--apply-timeout-defaults

(ert-deftest clutch-db-test-jdbc-apply-timeout-defaults-fills-missing ()
  "Empty params get all four timeouts filled from the global defcustoms."
  (let* ((clutch-connect-timeout-seconds 10)
         (clutch-read-idle-timeout-seconds 20)
         (clutch-query-timeout-seconds 30)
         (clutch-jdbc-rpc-timeout-seconds 40)
         (result (clutch-jdbc--apply-timeout-defaults nil)))
    (should (= (plist-get result :connect-timeout) 10))
    (should (= (plist-get result :read-idle-timeout) 20))
    (should (= (plist-get result :query-timeout) 30))
    (should (= (plist-get result :rpc-timeout) 40))))

(ert-deftest clutch-db-test-jdbc-apply-timeout-defaults-preserves-existing ()
  "Timeouts already present in params are not overwritten by global defaults."
  (let* ((clutch-connect-timeout-seconds 10)
         (clutch-read-idle-timeout-seconds 20)
         (clutch-query-timeout-seconds 30)
         (clutch-jdbc-rpc-timeout-seconds 40)
         (params '(:connect-timeout 99 :query-timeout 88))
         (result (clutch-jdbc--apply-timeout-defaults params)))
    (should (= (plist-get result :connect-timeout) 99))
    (should (= (plist-get result :read-idle-timeout) 20))
    (should (= (plist-get result :query-timeout) 88))
    (should (= (plist-get result :rpc-timeout) 40))))

;;;; Unit tests — backend registry

(ert-deftest clutch-db-test-backend-features ()
  "Test that backend features are correctly registered."
  (let ((mysql-features (alist-get 'mysql clutch-backend--registry))
        (pg-features (alist-get 'pg clutch-backend--registry))
        (jdbc-features (clutch-backend-feature 'jdbc))
        (clickhouse-features (clutch-backend-feature 'clickhouse))
        (mongodb-features (clutch-backend-feature 'mongodb)))
    ;; MySQL backend
    (should mysql-features)
    (should (eq (plist-get mysql-features :require) 'clutch-db-mysql))
    (should (eq (plist-get mysql-features :connect-fn) 'clutch-db-mysql-connect))
    (should (eq (plist-get mysql-features :normalize-fn)
                'clutch-db-mysql--normalize-connect-params))
    (should (equal (plist-get mysql-features :display-name) "MySQL"))
    (should (= (plist-get mysql-features :default-port) 3306))
    (should (eq (plist-get mysql-features :support-level) 'core))
    (should (eq (plist-get mysql-features :sql-product) 'mysql))
    ;; PostgreSQL backend
    (should pg-features)
    (should (eq (plist-get pg-features :require) 'clutch-db-pg))
    (should (eq (plist-get pg-features :connect-fn) 'clutch-db-pg-connect))
    (should (eq (plist-get pg-features :normalize-fn)
                'clutch-db-pg--normalize-connect-params))
    (should (equal (plist-get pg-features :display-name) "PostgreSQL"))
    (should (= (plist-get pg-features :default-port) 5432))
    (should (eq (plist-get pg-features :support-level) 'core))
    (should (eq (plist-get pg-features :sql-product) 'postgres))
    ;; Generic JDBC backend
    (should jdbc-features)
    (should (eq (plist-get jdbc-features :require) 'clutch-db-jdbc))
    (should (functionp (plist-get jdbc-features :connect-fn)))
    (should (eq (plist-get jdbc-features :support-level) 'basic))
    (should-not (clutch-backend-manual-choice-p 'jdbc))
    (should (eq (plist-get clickhouse-features :require) 'clutch-db-jdbc))
    (should (equal (plist-get clickhouse-features :display-name) "ClickHouse"))
    (should (= (plist-get clickhouse-features :default-port) 8123))
    (should (eq (plist-get clickhouse-features :support-level) 'basic))
    (should (eq (plist-get mongodb-features :require) 'clutch-mongodb))
    (should (eq (plist-get mongodb-features :connect-fn)
                'clutch-mongodb-connect))
    (should (equal (plist-get mongodb-features :display-name) "MongoDB"))
    (should (= (plist-get mongodb-features :default-port) 27017))
    (should (eq (plist-get mongodb-features :support-level) 'basic))
    (should (clutch-backend-manual-choice-p 'mongodb))
    (should-not (clutch-backend-feature 'sql-interface-mongodb))))

(ert-deftest clutch-db-test-backend-list-loads-optional-registries-in-order ()
  "User-facing backend lists should be derived from the backend registry."
  (let ((clutch-backend--registry
         '((mysql . (:require clutch-db-mysql))
           (pg . (:require clutch-db-pg))
           (sqlite . (:require clutch-db-sqlite)))))
    (cl-letf (((symbol-function 'require)
               (lambda (feature &optional _filename _noerror)
                 (when (eq feature 'clutch-db-jdbc)
                   (setq clutch-backend--registry
                         (append clutch-backend--registry
                                 '((jdbc . (:require clutch-db-jdbc))
                                   (oracle . (:require clutch-db-jdbc))
                                   (clickhouse . (:require clutch-db-jdbc))))))
                 t)))
      (should (equal (clutch-backends t)
                     '(mysql pg sqlite jdbc oracle clickhouse))))))

(ert-deftest clutch-db-test-backend-registry-exposes-data-models ()
  "Backend metadata should distinguish supported data models."
  (should (eq (clutch-backend-data-model 'mysql) 'relational))
  (should (eq (clutch-backend-data-model 'pg) 'relational))
  (should (eq (clutch-backend-data-model 'sqlite) 'relational))
  (should (eq (clutch-backend-data-model 'mongodb) 'document))
  (should (eq (clutch-backend-data-model 'redis) 'key-value)))

(ert-deftest clutch-db-test-sql-surface-p-follows-data-model-and-surface ()
  "SQL surface detection should not treat every non-document backend as SQL."
  (let ((clutch-backend--registry
         (append clutch-backend--registry
                 '((docdb . (:display-name "DocDB"
                              :data-model document
                              :surfaces
                              ((query-service . (:execution-model sql
                                                 :transport jdbc)))))))))
    (cl-letf (((symbol-function 'clutch-db-backend-key)
               (lambda (conn)
                 (pcase conn
                   ('mysql-conn 'mysql)
                   ('mongo-conn 'mongodb)
                   ('doc-conn 'docdb)
                   ('redis-conn 'redis)
                   (_ nil)))))
      (should (clutch-db-sql-surface-p 'mysql-conn nil))
      (should-not (clutch-db-sql-surface-p 'mongo-conn nil))
      (should (clutch-db-sql-surface-p
               'mongo-conn '(:backend mongodb :surface sql-interface)))
      (should (clutch-db-sql-surface-p
               'doc-conn '(:backend docdb :surface query-service)))
      (should (clutch-backend-jdbc-transport-p
               'docdb '(:surface query-service)))
      (should-not (clutch-db-sql-surface-p 'doc-conn nil))
      (should-not (clutch-db-sql-surface-p 'redis-conn nil)))))

(ert-deftest clutch-db-test-backend-query-mode-follows-surface-metadata ()
  "Query console modes should come from backend registry surface metadata."
  (let ((clutch-backend--registry
         '((mongodb . (:query-mode clutch-mongodb-mode
                       :query-mode-require clutch-document
                       :surfaces ((sql-interface . (:query-mode clutch-mode)))))))
        required)
    (cl-letf (((symbol-function 'require)
               (lambda (feature &optional _filename _noerror)
                 (push feature required)
                 t)))
      (should (eq (clutch-backend-query-mode
                   'mongodb
                   '(:backend mongodb))
                  'clutch-mongodb-mode))
      (should (equal required '(clutch-document)))
      (setq required nil)
      (should (eq (clutch-backend-query-mode
                   'mongodb
                   '(:backend mongodb :surface sql-interface))
                  'clutch-mode))
      (should-not required))))

(ert-deftest clutch-db-test-manual-backend-choices-follow-registry ()
  "Manual connect choices should include registered concrete backends."
  (require 'clutch)
  (let ((choices (clutch--manual-backend-choices)))
    (should (member 'mysql choices))
    (should (member 'clickhouse choices))
    (should (member 'mongodb choices))
    (should-not (member 'sql-interface-mongodb choices))
    (should-not (member 'jdbc choices))))

(ert-deftest clutch-db-test-manual-backends-have-registry-metadata ()
  "Manual connect prompts should get display names and ports from the registry."
  (require 'clutch)
  (dolist (backend (clutch--manual-backend-choices))
    (should (clutch-backend-display-name backend))
    (unless (eq backend 'sqlite)
      (should (numberp (clutch-backend-default-port backend))))))

(ert-deftest clutch-db-test-connect-requires-selected-backend-only ()
  "`clutch-db-connect' should require only the selected adapter."
  (let ((clutch-backend--registry
         '((mysql . (:require clutch-db-mysql :connect-fn clutch-db-mysql-connect))
           (pg . (:require clutch-db-pg :connect-fn clutch-db-pg-connect))))
        required)
    (cl-letf (((symbol-function 'require)
               (lambda (feature &optional _filename _noerror)
                 (push feature required)
                 t))
              ((symbol-function 'clutch-db-mysql-connect)
               (lambda (params)
                 (cons 'mysql-conn params)))
              ((symbol-function 'clutch-db-pg-connect)
               (lambda (params)
                 (cons 'pg-conn params)))
              ((symbol-function 'clutch-db-init-connection)
               #'ignore))
      (should (equal (clutch-db-connect 'mysql '(:database "app"))
                     '(mysql-conn :database "app")))
      (should (equal (nreverse required) '(clutch-db-mysql))))))

(ert-deftest clutch-db-test-generic-jdbc-display-name-prefers-custom-label ()
  "Generic JDBC connections should honor a user-facing display label."
  (require 'clutch)
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver jdbc :display-name "KingbaseES"))))
    (should (clutch--jdbc-backend-p 'jdbc))
    (should (clutch--jdbc-backend-p 'clickhouse))
    (should (equal (clutch--backend-display-name-from-params
                    '(:backend jdbc :display-name "KingbaseES"))
                   "KingbaseES"))
    (should (equal (clutch-db-display-name conn) "KingbaseES"))))

(ert-deftest clutch-db-test-build-conn-routes-generic-jdbc-through-jdbc-backend ()
  "The generic JDBC backend should pass :url through to `clutch-db-connect'."
  (require 'clutch)
  (let (captured-backend captured-params)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) "secret"))
              ((symbol-function 'clutch-db-connect)
               (lambda (backend params)
                 (setq captured-backend backend
                       captured-params params)
                 'fake-conn)))
      (should (eq (clutch--build-conn
                   '(:backend jdbc
                     :url "jdbc:kingbase8://127.0.0.1:54321/test"
                     :display-name "KingbaseES"
                     :user "system"))
                  'fake-conn))
      (should (eq captured-backend 'jdbc))
      (should (equal (plist-get captured-params :url)
                     "jdbc:kingbase8://127.0.0.1:54321/test"))
      (should (equal (plist-get captured-params :display-name) "KingbaseES"))
      (should (equal (plist-get captured-params :password) "secret"))
      (should-not (plist-member captured-params :backend)))))

(ert-deftest clutch-db-test-unknown-backend ()
  "Test that connecting with unknown backend signals error."
  (should-error
   (clutch-db-connect 'unknown '(:host "localhost"))
   :type 'user-error))

(ert-deftest clutch-db-test-native-backend-missing-package-errors-clearly ()
  "Missing optional protocol packages should raise a direct backend error."
  (dolist (case '((mysql mysql "mysql.el")
                  (pg pg "pg.el")))
    (pcase-let ((`(,backend ,protocol ,expected) case))
      (ert-info ((symbol-name backend))
        (let ((orig-require (symbol-function 'require)))
          (cl-letf (((symbol-function 'require)
                     (lambda (requested &optional filename noerror)
                       (if (eq requested protocol)
                           (if noerror nil
                             (signal 'file-missing
                                     (list "Cannot open load file"
                                           "No such file or directory"
                                           (symbol-name protocol))))
                         (funcall orig-require requested filename noerror)))))
            (condition-case err
                (progn
                  (clutch-db-connect backend '(:host "localhost"))
                  (should nil))
              (clutch-db-error
               (should (string-match-p expected (cadr err)))))))))))

(ert-deftest clutch-db-test-jdbc-driver-backend-loads-jdbc-on-demand ()
  "Driver-style JDBC backends should load `clutch-db-jdbc' on demand."
  (let ((clutch-backend--registry
         '((mysql . (:require clutch-db-mysql :connect-fn clutch-db-mysql-connect))
           (pg . (:require clutch-db-pg :connect-fn clutch-db-pg-connect))
           (sqlite . (:require clutch-db-sqlite :connect-fn clutch-db-sqlite-connect))))
        required
        captured-params)
    (cl-letf (((symbol-function 'require)
               (lambda (feature &optional _filename _noerror)
                 (pcase feature
                   ('clutch-db-jdbc
                    (setq required t)
                    (push (cons 'oracle
                                (list :require 'clutch-db-jdbc
                                      :connect-fn (lambda (params)
                                                    (setq captured-params params)
                                                    'fake-conn)))
                          clutch-backend--registry)
                    t)
                   (_ t))))
              ((symbol-function 'clutch-db-init-connection)
               (lambda (_conn) nil)))
      (should (eq (clutch-db-connect 'oracle '(:user "scott" :database "orcl"))
                  'fake-conn))
      (should required)
      (should (equal captured-params '(:user "scott" :database "orcl"))))))

;;;; Unit tests — MySQL type category mapping

(ert-deftest clutch-db-test-mysql-type-categories ()
  "Test MySQL type to category mapping."
  (require 'clutch-db-mysql)
  (require 'mysql)
  ;; Numeric types
  (should (eq (clutch-db-mysql--type-category mysql-type-long 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-float 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-double 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-decimal 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-longlong 33) 'numeric))
  ;; Date/time types
  (should (eq (clutch-db-mysql--type-category mysql-type-date 33) 'date))
  (should (eq (clutch-db-mysql--type-category mysql-type-time 33) 'time))
  (should (eq (clutch-db-mysql--type-category mysql-type-datetime 33) 'datetime))
  (should (eq (clutch-db-mysql--type-category mysql-type-timestamp 33) 'datetime))
  ;; BLOB/TEXT split by charset
  (should (eq (clutch-db-mysql--type-category mysql-type-blob 63) 'blob))
  (should (eq (clutch-db-mysql--type-category mysql-type-blob 33) 'text))
  ;; JSON
  (should (eq (clutch-db-mysql--type-category mysql-type-json 63) 'json))
  ;; Unknown type defaults to text
  (should (eq (clutch-db-mysql--type-category 9999 0) 'text)))

(ert-deftest clutch-db-test-mysql-convert-columns ()
  "Test MySQL column conversion."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let* ((mysql-cols (list (list :name "id" :type mysql-type-long :character-set 33)
                           (list :name "data" :type mysql-type-json :character-set 63)
                           (list :name "blob_bin" :type mysql-type-blob :character-set 63)
                           (list :name "blob_txt" :type mysql-type-blob :character-set 33)
                           (list :name "created" :type mysql-type-datetime :character-set 33)))
         (converted (clutch-db-mysql--convert-columns mysql-cols)))
    (should (= (length converted) 5))
    (should (equal (plist-get (nth 0 converted) :name) "id"))
    (should (eq (plist-get (nth 0 converted) :type-category) 'numeric))
    (should (equal (plist-get (nth 1 converted) :name) "data"))
    (should (eq (plist-get (nth 1 converted) :type-category) 'json))
    (should (equal (plist-get (nth 2 converted) :name) "blob_bin"))
    (should (eq (plist-get (nth 2 converted) :type-category) 'blob))
    (should (equal (plist-get (nth 3 converted) :name) "blob_txt"))
    (should (eq (plist-get (nth 3 converted) :type-category) 'text))
    (should (equal (plist-get (nth 4 converted) :name) "created"))
    (should (eq (plist-get (nth 4 converted) :type-category) 'datetime))))

;;;; Unit tests — PostgreSQL type category mapping

(ert-deftest clutch-db-test-pg-type-categories ()
  "Test PostgreSQL OID to category mapping."
  (require 'clutch-db-pg)
  ;; Numeric types
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-int4) 'numeric))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-int8) 'numeric))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-float8) 'numeric))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-numeric) 'numeric))
  ;; Date/time types
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-date) 'date))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-time) 'time))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-timestamp) 'datetime))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-timestamptz) 'datetime))
  ;; BLOB/JSON
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-bytea) 'blob))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-json) 'json))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-jsonb) 'json))
  (should (eq (clutch-db-pg--type-category clutch-db-pg--oid-bool) 'text))
  ;; Unknown OID defaults to text
  (should (eq (clutch-db-pg--type-category 999999) 'text)))

(ert-deftest clutch-db-test-pg-convert-columns ()
  "Test PostgreSQL column conversion."
  (require 'clutch-db-pg)
  (let* ((pg-cols '(("id" 23 4)
                    ("data" 3802 -1)
                    ("created" 1114 8)))
         (converted (clutch-db-pg--convert-columns pg-cols)))
    (should (= (length converted) 3))
    (should (equal (plist-get (nth 0 converted) :name) "id"))
    (should (eq (plist-get (nth 0 converted) :type-category) 'numeric))
    (should (equal (plist-get (nth 1 converted) :name) "data"))
    (should (eq (plist-get (nth 1 converted) :type-category) 'json))
    (should (equal (plist-get (nth 2 converted) :name) "created"))
    (should (eq (plist-get (nth 2 converted) :type-category) 'datetime))))

(ert-deftest clutch-db-test-pg-wrap-result-normalizes-temporal-values ()
  "PostgreSQL results should normalize upstream pg-el temporal values."
  (require 'clutch-db-pg)
  (let* ((conn (clutch-db-test--make-pgcon :database "test"))
         (date (encode-time 0 0 0 1 6 2024))
         (timestamp (encode-time 30 45 13 15 1 2024))
         (timestamptz (encode-time 45 30 8 7 4 2026))
         (pg-result (make-pgresult
                     :connection conn
                     :attributes `(("due_on" ,clutch-db-test--pg-oid-date 4)
                                   ("starts_at" ,clutch-db-test--pg-oid-time 8)
                                   ("opened_at" ,clutch-db-test--pg-oid-timestamp 8)
                                   ("shipped_at" ,clutch-db-test--pg-oid-timestamptz 8))
                     :tuples `((,date "09:10:11.250" ,timestamp ,timestamptz))))
         (result (clutch-db-pg--wrap-result pg-result))
         (row (car (clutch-db-result-rows result))))
    (should (equal (nth 0 row)
                   '(:year 2024 :month 6 :day 1)))
    (should (equal (nth 1 row)
                   '(:hours 9 :minutes 10 :seconds 11 :negative nil)))
    (should (equal (nth 2 row)
                   '(:year 2024 :month 1 :day 15
                     :hours 13 :minutes 45 :seconds 30)))
    (should (equal (nth 3 row)
                   '(:year 2026 :month 4 :day 7
                     :hours 8 :minutes 30 :seconds 45)))))

;;;; Unit tests — SQL building (paged queries)

(ert-deftest clutch-db-test-mysql-build-paged-sql ()
  "Test MySQL paged SQL generation."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost")))
    ;; Basic pagination
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 0" sql)))
    ;; Page 2
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 1 10)))
      (should (string-match-p "OFFSET 10" sql)))
    ;; Explicit offset for last-window pagination
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 9 10 nil 70)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 70" sql)))
    ;; With order
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10
                                              '("name" . "ASC"))))
      (should (string-match-p "ORDER BY" sql))
      (should (string-match-p "ASC" sql)))
    ;; Replacing existing ORDER BY for result-driven sort
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM t ORDER BY created_at DESC"
                0 10 '("name" . "ASC"))))
      (should (string-match-p "ORDER BY `name` ASC" sql))
      (should-not (string-match-p "ORDER BY created_at DESC.*ORDER BY" sql)))
    ;; Already has LIMIT — no modification
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t LIMIT 5" 0 10)))
      (should (equal sql "SELECT * FROM t LIMIT 5")))
    ;; Nested LIMIT should not disable outer pagination
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM (SELECT * FROM t LIMIT 1) AS sub"
                0 10)))
      (should (string-match-p "FROM (SELECT \\* FROM t LIMIT 1) AS sub" sql))
      (should (string-match-p "LIMIT 10 OFFSET 0\\'" sql)))))

(ert-deftest clutch-db-test-pg-build-paged-sql ()
  "Test PostgreSQL paged SQL generation."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "localhost")))
    ;; Basic pagination
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 0" sql)))
    ;; Page 3, page-size 25
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 2 25)))
      (should (string-match-p "LIMIT 25" sql))
      (should (string-match-p "OFFSET 50" sql)))
    ;; Explicit offset for last-window pagination
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 9 10 nil 70)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 70" sql)))
    ;; With descending order
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10
                                              '("id" . "DESC"))))
      (should (string-match-p "ORDER BY" sql))
      (should (string-match-p "DESC" sql)))
    ;; Replacing existing ORDER BY for result-driven sort
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM t ORDER BY created_at DESC"
                0 10 '("id" . "ASC"))))
      (should (string-match-p "ORDER BY \"id\" ASC" sql))
      (should-not (string-match-p "ORDER BY created_at DESC.*ORDER BY" sql)))
    ;; Query with trailing semicolon
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t;" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should-not (string-match-p ";\\s*LIMIT" sql)))
    ;; Nested LIMIT should not disable outer pagination
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM (SELECT * FROM t LIMIT 1) AS sub"
                0 10)))
      (should (string-match-p "FROM (SELECT \\* FROM t LIMIT 1) AS sub" sql))
      (should (string-match-p "LIMIT 10 OFFSET 0\\'" sql)))))

(ert-deftest clutch-db-test-jdbc-build-paged-sql-with-explicit-offset ()
  "JDBC pagination should support explicit offsets for last-window pages."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver sqlserver))))
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 9 10 nil 70)))
      (should (string-match-p "OFFSET 70 ROWS" sql))
      (should (string-match-p "FETCH NEXT 10 ROWS ONLY" sql))))
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle))))
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 9 10 nil 70)))
      (should (string-match-p "ROWNUM <= 80" sql))
      (should (string-match-p "rn > 70" sql)))))

(ert-deftest clutch-db-test-jdbc-build-paged-sql-preserves-user-order-by ()
  "Generic JDBC pagination should not append a second top-level ORDER BY."
  (let* ((conn (make-clutch-jdbc-conn :params '(:driver sqlserver)))
         (sql (clutch-db-build-paged-sql
               conn
               "SELECT * FROM t ORDER BY created_at DESC"
               0 10)))
    (should (string-match-p "ORDER BY created_at DESC" sql))
    (should-not (string-match-p "ORDER BY created_at DESC.*ORDER BY" sql))
    (should (string-match-p "OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY\\'" sql))))

(ert-deftest clutch-db-test-jdbc-build-paged-sql-duckdb-uses-limit-offset ()
  "DuckDB JDBC pagination should use LIMIT/OFFSET."
  (let* ((conn (make-clutch-jdbc-conn :params '(:url "jdbc:duckdb:/tmp/test.duckdb")))
         (sql (clutch-db-build-paged-sql
               conn
               "SELECT * FROM t ORDER BY created_at DESC"
               0 10)))
    (should (string-match-p "ORDER BY created_at DESC LIMIT 10 OFFSET 0\\'" sql))
    (should-not (string-match-p "FETCH NEXT" sql))))

(ert-deftest clutch-db-test-jdbc-oracle-source-table-name-canonicalizes-unquoted ()
  "Oracle JDBC source tables should follow Oracle identifier case rules."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle))))
    (should (equal (clutch-db--source-table-name conn "users") "USERS"))
    (should (equal (clutch-db--source-table-name conn "APP.users") "USERS"))
    (should (equal (clutch-db--source-table-name conn "\"MixedCase\"")
                   "MixedCase"))
    (should (equal (clutch-db--source-table-name conn "APP.\"MixedCase\"")
                   "MixedCase"))))

(ert-deftest clutch-db-test-jdbc-build-paged-sql-limit-offset-dialects ()
  "LIMIT/OFFSET JDBC dialects should not use OFFSET/FETCH."
  (dolist (params '((:driver redshift)
                    (:driver clickhouse)
                    (:url "jdbc:redshift://cluster.example.com:5439/analytics")
                    (:url "jdbc:clickhouse://ch.example.com:8123/default")))
    (let* ((conn (make-clutch-jdbc-conn :params params))
           (sql (clutch-db-build-paged-sql
                 conn
                 "SELECT * FROM t ORDER BY created_at DESC"
                 0 10)))
      (should (string-match-p "ORDER BY created_at DESC LIMIT 10 OFFSET 0\\'" sql))
      (should-not (string-match-p "FETCH NEXT" sql)))))

;;;; Unit tests — SQL escaping

(ert-deftest clutch-db-test-mysql-escape ()
  "Test MySQL identifier and literal escaping via generic interface."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost")))
    ;; Identifier escaping
    (should (equal (clutch-db-escape-identifier conn "table")
                   "`table`"))
    (should (equal (clutch-db-escape-identifier conn "my`table")
                   "`my``table`"))
    ;; Literal escaping
    (should (equal (clutch-db-escape-literal conn "hello")
                   "'hello'"))
    (should (equal (clutch-db-escape-literal conn "it's")
                   "'it\\'s'"))))

(ert-deftest clutch-db-test-pg-escape ()
  "Test PostgreSQL identifier and literal escaping via generic interface."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "localhost")))
    ;; Identifier escaping
    (should (equal (clutch-db-escape-identifier conn "table")
                   "\"table\""))
    (should (equal (clutch-db-escape-identifier conn "my\"table")
                   "\"my\"\"table\""))
    ;; Literal escaping
    (should (equal (clutch-db-escape-literal conn "hello")
                   "E'hello'"))
    (should (equal (clutch-db-escape-literal conn "it's")
                   "E'it''s'"))))

(ert-deftest clutch-db-test-jdbc-clickhouse-escape ()
  "ClickHouse JDBC should leave simple identifiers bare and backtick-escape others."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver clickhouse))))
    (should (equal (clutch-db-escape-identifier conn "background_schedule_pool_log")
                   "background_schedule_pool_log"))
    (should (equal (clutch-db-escape-identifier conn "order-items")
                   "`order-items`"))
    (should (equal (clutch-db-escape-identifier conn "my`table")
                   "`my``table`"))))

;;;; Unit tests — metadata accessors

(ert-deftest clutch-db-test-mysql-metadata ()
  "Test MySQL metadata accessors."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "example.com" :port 3307
                                :user "testuser" :database "testdb")))
    (should (equal (clutch-db-host conn) "example.com"))
    (should (= (clutch-db-port conn) 3307))
    (should (equal (clutch-db-user conn) "testuser"))
    (should (equal (clutch-db-database conn) "testdb"))
    (should (eq (clutch-db-backend-key conn) 'mysql))
    (should (equal (clutch-db-display-name conn) "MySQL"))))

(ert-deftest clutch-db-test-mysql-symbol-help-parses-help-result ()
  "MySQL HELP parsing should live behind the backend interface."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "localhost"))
        captured-sql)
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 (make-mysql-result
                  :rows '(("ABS"
                           "Syntax:\n\nABS(X)\n\nReturns absolute value.\nURL: https://example.invalid"
                           ""))))))
      (should (equal (clutch-db-symbol-help conn "abs")
                     '(:sig "ABS(X)" :desc "Returns absolute value.")))
      (should (equal captured-sql "HELP 'ABS'")))))

(ert-deftest clutch-db-test-pg-metadata ()
  "Test PostgreSQL metadata accessors."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "example.com" :port 5433
                                          :user "pguser" :database "pgdb")))
    (should (equal (clutch-db-host conn) "example.com"))
    (should (= (clutch-db-port conn) 5433))
    (should (equal (clutch-db-user conn) "pguser"))
    (should (equal (clutch-db-database conn) "pgdb"))
    (should (eq (clutch-db-backend-key conn) 'pg))
    (should (equal (clutch-db-display-name conn) "PostgreSQL"))))

(ert-deftest clutch-db-test-pg-list-schemas-filters-system-schemas ()
  "PostgreSQL schema listing should omit built-in system schemas."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        captured-sql)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 (make-pgresult :tuples '(("app") ("public"))))))
      (should (equal (clutch-db-list-schemas conn) '("app" "public")))
      (should (string-match-p "information_schema" captured-sql))
      (should (string-match-p "NOT LIKE 'pg" captured-sql)))))

(ert-deftest clutch-db-test-pg-list-tables-uses-current-schema ()
  "PostgreSQL table listing should be scoped to the active search_path schema."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        captured-sql)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 (make-pgresult :tuples '(("users"))))))
      (should (equal (clutch-db-list-tables conn) '("users")))
      (should (string-match-p "schemaname = current_schema()" captured-sql))
      (should-not (string-match-p "NOT IN" captured-sql)))))

(ert-deftest clutch-db-test-pg-current-schema-caches-result ()
  "PostgreSQL current schema lookup should cache the result on the connection."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        (calls 0))
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn _sql)
                 (setq calls (1+ calls))
                 (make-pgresult :tuples '(("public"))))))
      (should (equal (clutch-db-current-schema conn) "public"))
      (should (equal (clutch-db-current-schema conn) "public"))
      (should (= calls 1)))))

(ert-deftest clutch-db-test-pg-set-current-schema-updates-search-path-cache ()
  "PostgreSQL schema switching should issue SET search_path and update cache."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        executed-sql)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq executed-sql sql)
                 (make-pgresult :tuples nil))))
      (should (equal (clutch-db-set-current-schema conn "app") "app"))
      (should (equal executed-sql "SET search_path TO \"app\""))
      (should (equal (clutch-db-current-schema conn) "app")))))

(ert-deftest clutch-db-test-pg-connect-applies-schema-via-search-path ()
  "PostgreSQL connect should restore a requested schema via search_path."
  (require 'clutch-db-pg)
  (let (captured-args executed-sql)
    (cl-letf (((symbol-function 'pg-connect-plist)
               (lambda (&rest args)
                 (setq captured-args args)
                 (clutch-db-test--make-pgcon :host "127.0.0.1" :port 54321
                                             :user "system" :database "test")))
              ((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq executed-sql sql)
                 (make-pgresult :tuples nil))))
      (let ((conn (clutch-db-pg-connect
                   '(:host "127.0.0.1"
                     :port 54321
                     :database "test"
                     :user "system"
                     :password "123456"
                     :schema "app"))))
        (should (equal (nth 0 captured-args) "test"))
        (should (equal (nth 1 captured-args) "system"))
        (should-not (plist-member (nthcdr 2 captured-args) :schema))
        (should (equal executed-sql "SET search_path TO \"app\""))
        (should (equal (clutch-db-current-schema conn) "app"))))))

(ert-deftest clutch-db-test-pg-connect-normalizes-tls-to-sslmode ()
  "PostgreSQL backend connect should map canonical SSLMODE to pg-el TLS args."
  (require 'clutch-db-pg)
  (let (captured-args)
    (cl-letf (((symbol-function 'pg-connect-plist)
               (lambda (&rest args)
                 (setq captured-args args)
                 (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                             :user "postgres" :database "test")))
              ((symbol-function 'pg-exec)
               (lambda (&rest _args)
                 (make-pgresult :tuples nil))))
      (clutch-db-pg-connect
       '(:host "127.0.0.1"
         :port 5432
         :database "test"
         :user "postgres"
         :password "secret"
         :tls t))
      (should (eq (plist-get (nthcdr 2 captured-args) :tls-options) t))
      (should-not (plist-member (nthcdr 2 captured-args) :tls)))))

(ert-deftest clutch-db-test-pg-connect-applies-timeout-defaults ()
  "PostgreSQL connect should apply Clutch timeout defaults at the adapter boundary."
  (require 'clutch-db-pg)
  (let ((clutch-connect-timeout-seconds 12)
        (clutch-read-idle-timeout-seconds 34)
        (clutch-query-timeout-seconds 56)
        captured-timeouts
        executed-sql)
    (cl-letf (((symbol-function 'pg-connect-plist)
               (lambda (&rest _args)
                 (setq captured-timeouts
                       (list pg-connect-timeout pg-read-timeout))
                 (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                             :user "postgres" :database "test")))
              ((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq executed-sql sql)
                 (make-pgresult :tuples nil))))
      (clutch-db-pg-connect
       '(:host "127.0.0.1"
         :port 5432
         :database "test"
         :user "postgres"
         :password "secret"))
      (should (equal captured-timeouts '(12 34)))
      (should (equal executed-sql "SET statement_timeout = 56000")))))

(ert-deftest clutch-db-test-mysql-connect-normalizes-tls-nil-to-ssl-mode ()
  "MySQL backend connect should pass canonical `:ssl-mode' to `mysql-connect'."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let (captured-args)
    (cl-letf (((symbol-function 'mysql-connect)
               (lambda (&rest args)
                 (setq captured-args args)
                 (make-mysql-conn :host "127.0.0.1" :port 3306
                                  :user "root" :database "mysql-wire"))))
      (clutch-db-mysql-connect
       '(:host "127.0.0.1"
         :port 3306
         :database "mysql"
         :user "root"
         :password "secret"
         :tls nil))
      (should-not (plist-member captured-args :clutch-tls-mode))
      (should (eq (plist-get captured-args :ssl-mode) 'disabled))
      (should-not (plist-member captured-args :tls)))))

(ert-deftest clutch-db-test-mysql-connect-applies-timeout-defaults ()
  "MySQL connect should apply Clutch timeout defaults at the adapter boundary."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((clutch-connect-timeout-seconds 12)
        (clutch-read-idle-timeout-seconds 34)
        captured-args)
    (cl-letf (((symbol-function 'mysql-connect)
               (lambda (&rest args)
                 (setq captured-args args)
                 (make-mysql-conn :host "127.0.0.1" :port 3306
                                  :user "root" :database "mysql-wire"))))
      (clutch-db-mysql-connect
       '(:host "127.0.0.1"
         :port 3306
         :database "mysql"
         :user "root"
         :password "secret"))
      (should (= (plist-get captured-args :connect-timeout) 12))
      (should (= (plist-get captured-args :read-idle-timeout) 34)))))

(ert-deftest clutch-db-test-mysql-connect-strips-pass-entry-before-wire-connect ()
  "MySQL backend connect should not pass `:pass-entry' to `mysql-connect'."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let (captured-args)
    (cl-letf (((symbol-function 'mysql-connect)
               (lambda (&rest args)
                 (setq captured-args args)
                 (make-mysql-conn :host "127.0.0.1" :port 3306
                                       :user "root" :database "mysql-wire"))))
      (clutch-db-mysql-connect
       '(:host "127.0.0.1"
         :port 3306
         :database "mysql"
         :user "root"
         :password "secret"
         :pass-entry "prod-db"))
      (should-not (plist-member captured-args :pass-entry))
      (should (equal (plist-get captured-args :password) "secret")))))

(ert-deftest clutch-db-test-mysql-interrupt-kills-query-and-drains-original-conn ()
  "MySQL interrupt should use a helper connection and keep the session usable."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let* ((conn (make-mysql-conn :host "127.0.0.1"
                                :port 3306
                                :user "root"
                                :database "test"
                                :connection-id 123
                                :read-idle-timeout 30))
         (killer (make-mysql-conn :connection-id 999))
         (captured-connect-args nil)
         (captured-sql nil)
         (drained nil)
         (disconnected nil))
    (puthash conn
             '(:host "127.0.0.1"
               :port 3306
               :user "root"
               :password "secret"
               :database "test"
               :read-idle-timeout 30)
             clutch-db-mysql--connection-params)
    (cl-letf (((symbol-function 'mysql-connect)
               (lambda (&rest args)
                 (setq captured-connect-args args)
                 killer))
              ((symbol-function 'mysql-query)
               (lambda (mysql-conn sql)
                 (when (eq mysql-conn killer)
                   (setq captured-sql sql))
                 (make-mysql-result :connection mysql-conn :status "OK")))
              ((symbol-function 'mysql-drain-query-response)
               (lambda (mysql-conn timeout)
                 (should (eq mysql-conn conn))
                 (should (= timeout clutch-db-mysql-cancel-timeout-seconds))
                 (setq drained t)
                 (signal 'mysql-query-error '("[1317] Query execution was interrupted"))))
              ((symbol-function 'mysql-disconnect)
               (lambda (mysql-conn)
                 (when (eq mysql-conn killer)
                   (setq disconnected t)))))
      (should (clutch-db-interrupt-query conn))
      (should (equal captured-sql "KILL QUERY 123"))
      (should (equal (plist-get captured-connect-args :password) "secret"))
      (should (equal (plist-get captured-connect-args :read-idle-timeout)
                     clutch-db-mysql-cancel-timeout-seconds))
      (should drained)
      (should disconnected)
      (should (= (mysql-conn-read-idle-timeout conn) 30)))))

(ert-deftest clutch-db-test-pg-interrupt-query-returns-t-after-cancel ()
  "PostgreSQL interrupt should report success when cancel completes."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432))
        called)
    (cl-letf (((symbol-function 'pg-cancel)
               (lambda (pg-conn)
                 (setq called pg-conn)
                 t)))
      (should (clutch-db-interrupt-query conn))
      (should (eq called conn)))))

(ert-deftest clutch-db-test-pg-interrupt-query-returns-nil-on-pg-error ()
  "PostgreSQL interrupt should degrade to nil when cancel errors."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432)))
    (cl-letf (((symbol-function 'pg-cancel)
               (lambda (_pg-conn)
                 (signal 'pg-connection-error '("cancel failed")))))
      (should-not (clutch-db-interrupt-query conn)))))

(ert-deftest clutch-db-test-jdbc-metadata-derived-from-url ()
  "Generic JDBC metadata accessors should derive host/port/database from :url."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver jdbc
                         :url "jdbc:kingbase8://127.0.0.1:54321/test"
                         :display-name "KingbaseES"
                         :user "system"))))
    (should (equal (clutch-db-host conn) "127.0.0.1"))
    (should (= (clutch-db-port conn) 54321))
    (should (equal (clutch-db-user conn) "system"))
    (should (equal (clutch-db-database conn) "test"))
    (should (equal (clutch-db-display-name conn) "KingbaseES"))))

;;;; Live integration tests — MySQL

(defun clutch-db-test--mysql-live-configured-p ()
  "Return non-nil when MySQL live connection data is configured."
  clutch-db-test-mysql-password)

(defun clutch-db-test--mysql-live-params ()
  "Return connection params for MySQL live tests."
  (list :host clutch-db-test-mysql-host
        :port clutch-db-test-mysql-port
        :user clutch-db-test-mysql-user
        :password clutch-db-test-mysql-password
        :database clutch-db-test-mysql-database))

(defun clutch-db-test--pg-live-configured-p ()
  "Return non-nil when PostgreSQL live connection data is configured."
  clutch-db-test-pg-password)

(defun clutch-db-test--pg-live-params ()
  "Return connection params for PostgreSQL live tests."
  (list :host clutch-db-test-pg-host
        :port clutch-db-test-pg-port
        :user clutch-db-test-pg-user
        :password clutch-db-test-pg-password
        :database clutch-db-test-pg-database))

(defun clutch-db-test--require-cross-sql-live-backends ()
  "Skip unless the current cross-backend live tests have both SQL backends."
  (unless (and (clutch-db-test--mysql-live-configured-p)
               (clutch-db-test--pg-live-configured-p))
    (ert-skip "Set both MySQL and PostgreSQL live credentials for cross-backend tests")))

(defmacro clutch-db-test--with-mysql (var &rest body)
  "Execute BODY with VAR bound to a MySQL connection.
Skips if `clutch-db-test-mysql-password' is nil."
  (declare (indent 1))
  `(if (not (clutch-db-test--mysql-live-configured-p))
       (ert-skip "Set clutch-db-test-mysql-password to enable MySQL live tests")
     ;; Local MySQL 8 containers usually present self-signed certs.  The native
     ;; client auto-upgrades to TLS for `caching_sha2_password', so disable
     ;; certificate verification here unless the caller has installed a trust
     ;; chain explicitly.
     (clutch-db-test--with-local-mysql-tls
       (let ((,var (clutch-db-connect
                    'mysql
                    (clutch-db-test--mysql-live-params))))
         (unwind-protect
             (progn ,@body)
           (clutch-db-disconnect ,var))))))

(defun clutch-db-test--assert-live-basic-query (conn)
  "Assert the standard live smoke query against CONN."
  (let ((result (clutch-db-query conn "SELECT 1 AS n, 'hello' AS s")))
    (should (clutch-db-result-p result))
    (should (= (length (clutch-db-result-columns result)) 2))
    (should (= (length (clutch-db-result-rows result)) 1))
    (let ((row (car (clutch-db-result-rows result))))
      (should (= (car row) 1))
      (should (equal (cadr row) "hello")))))

(defun clutch-db-test--assert-live-basic-dml (conn)
  "Assert the standard live DML round-trip against CONN."
  (clutch-db-query conn "CREATE TEMPORARY TABLE _db_test (id INT, val TEXT)")
  (let ((result (clutch-db-query conn
                 "INSERT INTO _db_test VALUES (1, 'a'), (2, 'b')")))
    (should (= (clutch-db-result-affected-rows result) 2)))
  (let ((result (clutch-db-query conn "SELECT * FROM _db_test")))
    (should (= (length (clutch-db-result-rows result)) 2))))

(defmacro clutch-db-test--define-live-basic-tests (prefix with-macro tags display-name)
  "Define shared live tests for PREFIX using WITH-MACRO, TAGS, and DISPLAY-NAME."
  `(progn
     (ert-deftest ,(intern (format "%s-live-connect" prefix)) ()
       :tags ',tags
       ,(format "Test %s connection via clutch-db-connect." display-name)
       (,with-macro conn
         (should (clutch-db-live-p conn))
         (should (equal (clutch-db-display-name conn) ,display-name))))
     (ert-deftest ,(intern (format "%s-live-query" prefix)) ()
       :tags ',tags
       ,(format "Test %s query via clutch-db-query." display-name)
       (,with-macro conn
         (clutch-db-test--assert-live-basic-query conn)))
     (ert-deftest ,(intern (format "%s-live-dml" prefix)) ()
       :tags ',tags
       ,(format "Test %s DML operations." display-name)
       (,with-macro conn
         (clutch-db-test--assert-live-basic-dml conn)))
     (ert-deftest ,(intern (format "%s-live-error" prefix)) ()
       :tags ',tags
       ,(format "Test %s error handling." display-name)
       (,with-macro conn
         (should-error (clutch-db-query conn "SELEC BAD")
                       :type 'clutch-db-error)))))

(defun clutch-db-test--mongodb-live-configured-p ()
  "Return non-nil when native MongoDB live connection data is configured."
  (and clutch-db-test-mongodb-live-enabled
       clutch-db-test-mongodb-database
       (or clutch-db-test-mongodb-url
           clutch-db-test-mongodb-host)))

(defun clutch-db-test--mongodb-live-params ()
  "Return connection params for native MongoDB live tests."
  (let ((params (if clutch-db-test-mongodb-url
                    (list :url clutch-db-test-mongodb-url)
                  (list :host clutch-db-test-mongodb-host
                        :port clutch-db-test-mongodb-port))))
    (setq params
          (plist-put params :database clutch-db-test-mongodb-database))
    (when clutch-db-test-mongodb-auth-database
      (setq params
            (plist-put params :auth-database
                       clutch-db-test-mongodb-auth-database)))
    (when clutch-db-test-mongodb-user
      (setq params (plist-put params :user clutch-db-test-mongodb-user)))
    (when clutch-db-test-mongodb-password
      (setq params
            (plist-put params :password clutch-db-test-mongodb-password)))
    (when clutch-db-test-mongodb-props
      (setq params (plist-put params :props clutch-db-test-mongodb-props)))
    params))

(defmacro clutch-db-test--with-mongodb (var &rest body)
  "Execute BODY with VAR bound to a live native MongoDB connection.
Skips unless `clutch-db-test-mongodb-live-enabled' is non-nil."
  (declare (indent 1))
  `(if (not (clutch-db-test--mongodb-live-configured-p))
       (ert-skip "Set clutch-db-test-mongodb-live-enabled and connection data to enable native MongoDB live tests")
     (require 'clutch-mongodb)
     (let ((,var (clutch-db-connect
                  'mongodb
                  (clutch-db-test--mongodb-live-params))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(defun clutch-db-test--redis-live-configured-p ()
  "Return non-nil when Redis live connection data is configured."
  (and clutch-db-test-redis-live-enabled
       clutch-db-test-redis-host
       clutch-db-test-redis-port))

(defun clutch-db-test--redis-live-params ()
  "Return connection params for Redis live tests."
  (let ((params (list :host clutch-db-test-redis-host
                      :port clutch-db-test-redis-port
                      :database clutch-db-test-redis-database)))
    (when clutch-db-test-redis-user
      (setq params (plist-put params :user clutch-db-test-redis-user)))
    (when clutch-db-test-redis-password
      (setq params (plist-put params :password clutch-db-test-redis-password)))
    params))

(defmacro clutch-db-test--with-redis (var &rest body)
  "Execute BODY with VAR bound to a live Redis connection."
  (declare (indent 1))
  `(if (not (clutch-db-test--redis-live-configured-p))
       (ert-skip "Set clutch-db-test-redis-live-enabled and connection data to enable Redis live tests")
     (require 'clutch-redis)
     (let ((,var (clutch-db-connect
                  'redis
                  (clutch-db-test--redis-live-params))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(defun clutch-db-test--redis-live-key (suffix)
  "Return an isolated Redis key name using SUFFIX."
  (clutch-db-test--live-name (format "clutch:redis:%s" suffix)))

(clutch-db-test--define-live-basic-tests
 clutch-db-test-mysql
 clutch-db-test--with-mysql
 (:db-live :mysql-live)
 "MySQL")

(ert-deftest clutch-db-test-mysql-live-interrupt-keeps-connection-usable ()
  :tags '(:db-live :mysql-live)
  "MySQL query interruption should keep the original session usable."
  (clutch-db-test--with-mysql conn
    (clutch-db-test--with-mysql watcher
      (let (worker)
        (unwind-protect
            (progn
              (setq worker
                    (make-thread
                     (lambda ()
                       (clutch-db-query conn "SELECT SLEEP(10)")
                       :completed)
                     "clutch-mysql-interrupt-test"))
              (let ((seen nil))
                (dotimes (_ 50)
                  (unless seen
                    (sleep-for 0.1)
                    (let* ((result
                            (clutch-db-query
                             watcher
                             (format
                              "SELECT INFO FROM INFORMATION_SCHEMA.PROCESSLIST WHERE ID = %d"
                              (mysql-connection-id conn))))
                           (info (caar (clutch-db-result-rows result))))
                      (setq seen
                            (and (stringp info)
                                 (string-match-p "SELECT SLEEP" info))))))
                (should seen))
              (thread-signal worker 'quit nil)
              (let ((joined (condition-case nil
                                (thread-join worker)
                              (quit :quit))))
                (should (eq joined :quit)))
              (should (clutch-db-interrupt-query conn))
              (let ((result (clutch-db-query conn "SELECT 1 AS n")))
                (should (= (caar (clutch-db-result-rows result)) 1))))
          (when (and worker (thread-live-p worker))
            (ignore-errors (thread-signal worker 'quit nil))
            (condition-case nil
                (thread-join worker)
              (quit nil)
              (error nil)))
          (ignore-errors (clutch-db-interrupt-query conn)))))))

(ert-deftest clutch-db-test-mysql-live-schema ()
  :tags '(:db-live :mysql-live)
  "Test MySQL schema introspection."
  (clutch-db-test--with-mysql conn
    ;; list-tables
    (let ((tables (clutch-db-list-tables conn)))
      (should (listp tables))
      (should (> (length tables) 0)))
    ;; list-columns
    (let ((columns (clutch-db-list-columns conn "user")))
      (should (listp columns))
      (should (member "Host" columns)))
    ;; object definition
    (let ((ddl (clutch-db-object-definition
                conn '(:name "user" :type "TABLE"))))
      (should (stringp ddl))
      (should (string-match-p "CREATE\\( TABLE\\| .* VIEW\\)" ddl)))))

(ert-deftest clutch-db-test-mysql-live-row-identity-uses-unique-not-null ()
  :tags '(:db-live :mysql-live)
  "MySQL should use a non-null unique key when no primary key exists."
  (clutch-db-test--with-mysql conn
    (let* ((table (format "clutch_rowid_unique_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (format "CREATE TABLE %s (code VARCHAR(32) NOT NULL, name VARCHAR(64), UNIQUE KEY uq_code (code))"
                    table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn table))))
              (should (equal (plist-get candidate :kind) 'unique-key))
              (should (equal (plist-get candidate :name) "uq_code"))
              (should (equal (plist-get candidate :columns) '("code")))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-mysql-live-row-identity-sees-generated-invisible-pk ()
  :tags '(:db-live :mysql-live)
  "MySQL generated invisible primary keys should behave as primary keys."
  (clutch-db-test--with-mysql conn
    (let* ((table (format "clutch_rowid_gipk_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table)))
      (condition-case nil
          (progn
            (ignore-errors
              (clutch-db-query
               conn
               "SET SESSION show_gipk_in_create_table_and_information_schema=ON"))
            (clutch-db-query
             conn "SET SESSION sql_generate_invisible_primary_key=ON"))
        (clutch-db-error
         (ert-skip "MySQL generated invisible primary keys are unavailable")))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (name VARCHAR(64))" table))
            (let ((show-keys
                   (clutch-db-query
                    conn
                    (format "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'"
                            table))))
              (unless (clutch-db-result-rows show-keys)
                (ert-skip "MySQL generated invisible primary key metadata is hidden")))
            (should (equal (clutch-db-primary-key-columns conn table)
                           '("my_row_id")))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn table))))
              (should (equal (plist-get candidate :kind) 'primary-key))
              (should (equal (plist-get candidate :columns) '("my_row_id")))))
        (ignore-errors (clutch-db-query conn drop-sql))
        (ignore-errors
          (clutch-db-query
           conn "SET SESSION sql_generate_invisible_primary_key=OFF"))))))

(ert-deftest clutch-db-test-mysql-live-index-details-filter-by-target-table ()
  :tags '(:db-live :mysql-live)
  "MySQL index details should stay scoped to the target table."
  (clutch-db-test--with-mysql conn
    (let* ((suffix (emacs-pid))
           (table-a (format "clutch_idx_a_%d" suffix))
           (table-b (format "clutch_idx_b_%d" suffix))
           (drop-a (format "DROP TABLE IF EXISTS %s" table-a))
           (drop-b (format "DROP TABLE IF EXISTS %s" table-b))
           (create-a
            (format "CREATE TABLE %s (id INT PRIMARY KEY, code INT, KEY idx_same (code))"
                    table-a))
           (create-b
            (format "CREATE TABLE %s (id INT PRIMARY KEY, other_code INT, KEY idx_same (other_code))"
                    table-b)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-a)
            (clutch-db-query conn drop-b)
            (clutch-db-query conn create-a)
            (clutch-db-query conn create-b)
            (let ((details-a (clutch-db-object-details
                              conn
                              (list :name "idx_same" :type "INDEX"
                                    :target-table table-a)))
                  (details-b (clutch-db-object-details
                              conn
                              (list :name "idx_same" :type "INDEX"
                                    :target-table table-b))))
              (should (equal (mapcar (lambda (col) (plist-get col :name))
                                     details-a)
                             '("code")))
              (should (equal (mapcar (lambda (col) (plist-get col :name))
                                     details-b)
                             '("other_code")))))
        (ignore-errors (clutch-db-query conn drop-a))
        (ignore-errors (clutch-db-query conn drop-b))))))

(ert-deftest clutch-db-test-mysql-object-definition-empty-rows-errors-cleanly ()
  "MySQL table definition should signal `clutch-db-error' on empty row sets."
  (let ((conn (make-mysql-conn :host "localhost")))
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (_conn _sql)
                 (make-mysql-result :rows nil))))
      (should-error
       (clutch-db-object-definition
        conn '(:name "missing_table" :type "TABLE"))
       :type 'clutch-db-error))))

;;;; Live integration tests — PostgreSQL

(defmacro clutch-db-test--with-pg (var &rest body)
  "Execute BODY with VAR bound to a PostgreSQL connection.
Skips if `clutch-db-test-pg-password' is nil."
  (declare (indent 1))
  `(if (not (clutch-db-test--pg-live-configured-p))
       (ert-skip "Set clutch-db-test-pg-password to enable PostgreSQL live tests")
     (let ((,var (clutch-db-connect
                  'pg
                  (clutch-db-test--pg-live-params))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(clutch-db-test--define-live-basic-tests
 clutch-db-test-pg
 clutch-db-test--with-pg
 (:db-live :pg-live)
 "PostgreSQL")

(ert-deftest clutch-db-test-pg-live-schema ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL schema introspection."
  (clutch-db-test--with-pg conn
    (let* ((table (clutch-db-test--live-name "clutch_schema"))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT)"
                     table))
            (let ((tables (clutch-db-list-tables conn)))
              (should (listp tables))
              (should (member table tables)))
            (let ((columns (clutch-db-list-columns conn table)))
              (should (listp columns))
              (should (member "id" columns))
              (should (member "name" columns)))
            (let ((ddl (clutch-db-object-definition
                        conn (list :name table :type "TABLE"))))
              (should (stringp ddl))
              (should (string-match-p "CREATE TABLE" ddl))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-pg-live-row-identity-uses-ctid ()
  :tags '(:db-live :pg-live)
  "PostgreSQL should use CTID when no logical key exists."
  (clutch-db-test--with-pg conn
    (let* ((table (format "clutch_rowid_ctid_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn
                             (format "CREATE TABLE %s (name TEXT)" table))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn table))))
              (should (equal (plist-get candidate :kind) 'row-locator))
              (should (equal (plist-get candidate :name) "ctid"))
              (should (equal (plist-get candidate :select-expressions)
                             '("ctid::text")))
              (should (equal (plist-get candidate :where-sql)
                             "ctid = ?::tid"))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

;;;; Live integration tests — MongoDB

(defun clutch-db-test--mongodb-live-collection (suffix)
  "Return an isolated MongoDB live collection name using SUFFIX."
  (format "clutch_%s_%d" suffix (emacs-pid)))

(defun clutch-db-test--visible-result-column-indexes (result)
  "Return non-hidden column indexes for RESULT."
  (cl-loop for column in (clutch-db-result-columns result)
           for index from 0
           unless (plist-get column :hidden)
           collect index))

(defun clutch-db-test--visible-result-column-names (result)
  "Return non-hidden column names for RESULT."
  (let ((columns (clutch-db-result-columns result)))
    (mapcar (lambda (index) (plist-get (nth index columns) :name))
            (clutch-db-test--visible-result-column-indexes result))))

(defun clutch-db-test--visible-result-rows (result)
  "Return non-hidden row values for RESULT."
  (let ((indexes (clutch-db-test--visible-result-column-indexes result)))
    (mapcar (lambda (row)
              (mapcar (lambda (index) (nth index row)) indexes))
            (clutch-db-result-rows result))))

(ert-deftest clutch-db-test-mongodb-live-connect ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB connection should return a live conn."
  (clutch-db-test--with-mongodb conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "MongoDB"))
    (should-not (clutch-db-manual-commit-supported-p conn))
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-mongodb-live-query ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB should evaluate MQL helper commands and return document grids."
  (clutch-db-test--with-mongodb conn
    (let ((collection (clutch-db-test--mongodb-live-collection "query")))
      (unwind-protect
          (progn
            (let* ((result (clutch-db-query conn "db.runCommand({ping: 1})"))
                   (columns (clutch-db-result-column-names
                             (clutch-db-result-columns result)))
                   (ok-pos (cl-position "ok" columns :test #'equal)))
              (should ok-pos)
              (should (= (nth ok-pos (car (clutch-db-result-rows result)))
                         1.0)))
            (ignore-errors
              (clutch-db-query
               conn
               (format "db.getCollection(%S).drop()" collection)))
            (clutch-db-query
             conn
             (format "db.createCollection(%S)" collection))
            (should (member collection (clutch-db-list-tables conn)))
            (clutch-db-query
             conn
             (format "db.getCollection(%S).deleteMany({})" collection))
            (clutch-db-query
             conn
             (format
              (concat
               "db.getCollection(%S).insertMany(["
               "{_id: 'a', n: 1, s: 'hello', code: 'a);b]/c', price: 12.5, "
               "createdAt: ISODate('2024-01-02T03:04:05.678Z'), "
               "n32: Int32(7), n64: Long(7), "
               "amount: Decimal128('12.3400'), "
               "ts: Timestamp(1700000000, 7)},"
               "{_id: 'b', n: 2, nested: {ok: true}}"
               "])")
              collection))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find({}, "
                           "{_id: 1, n: 1, s: 1, nested: 1})")
                          collection))
                 (result (clutch-db-query conn code))
                 (rows (seq-sort-by #'car #'string<
                                    (clutch-db-result-rows result))))
              (should (clutch-db-result-p result))
              (should (equal (mapcar (lambda (column)
                                       (plist-get column :name))
                                     (clutch-db-result-columns result))
                             '("_id" "n" "s" "nested"
                               "clutch__document")))
              (should (plist-get (car (last (clutch-db-result-columns result)))
                                 :hidden))
              (should (plist-get (car (last (clutch-db-result-columns result)))
                                 :document-source))
              (should (= (length rows) 2))
              (should (equal (seq-take (car rows) 3) '("a" 1 "hello")))
              (should (equal (seq-take (cadr rows) 2) '("b" 2)))
              (should (equal (nth 3 (cadr rows)) "{\"ok\":true}")))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).aggregate(["
                           "{$match: {_id: {$in: ['a', 'b']}}}, "
                           "{$group: {_id: null, total: {$sum: \"$n\"}}}"
                           "], {allowDiskUse: true, comment: 'agg-base'})"
                           ".batchSize(2)"
                           ".comment('agg-chain')"
                           ".maxTimeMS(1000)")
                          collection))
                   (result (clutch-db-query conn code))
                   (columns (mapcar (lambda (column)
                                      (plist-get column :name))
                                    (clutch-db-result-columns result)))
                   (total-pos (cl-position "total" columns :test #'equal)))
              (should total-pos)
	              (should (equal (mapcar (lambda (row)
	                                       (nth total-pos row))
	                                     (clutch-db-result-rows result))
	                             '(3))))
	            (let* ((result
	                    (clutch-db-query
	                     conn
	                     (concat
	                      "db.aggregate(["
	                      "{$documents: [{n: 1}, {n: 2}]}, "
	                      "{$group: {_id: null, total: {$sum: \"$n\"}}}"
	                      "])")))
	                   (columns (mapcar (lambda (column)
	                                      (plist-get column :name))
	                                    (clutch-db-result-columns result)))
	                   (total-pos (cl-position "total" columns :test #'equal)))
	              (should total-pos)
	              (should (equal (mapcar (lambda (row)
	                                       (nth total-pos row))
	                                     (clutch-db-result-rows result))
	                             '(3))))
	            (let* ((code (format
	                          (concat
	                           "db.getCollection(%S).find({_id: 'a'})"
	                           ".explain('executionStats')")
                          collection))
                   (result (clutch-db-query conn code))
                   (columns (mapcar (lambda (column)
                                      (plist-get column :name))
                                    (clutch-db-result-columns result))))
              (should (member "queryPlanner" columns))
              (should (member "executionStats" columns)))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find("
                           "{_id: {$gte: 'a'}}, {_id: 1, n: 1})"
                           ".sort({_id: -1})"
                           ".batchSize(1)"
                           ".maxTimeMS(1000)"
                           ".comment('clutch-live')"
                           ".allowDiskUse(true)"
                           ".skip(0)"
                           ".limit(1)")
                          collection))
                   (result (clutch-db-query conn code)))
              (should (equal (clutch-db-test--visible-result-column-names
                              result)
                             '("_id" "n")))
              (should (equal (clutch-db-test--visible-result-rows result)
                             '(("b" 2)))))
            (clutch-db-query
             conn
             (format
              (concat
               "db.getCollection(%S).updateOne("
               "{_id: 'c'}, {$set: {n: 3, group: 'upd'}}, "
               "{upsert: true});"
               "db.getCollection(%S).insertOne({_id: 'd', group: 'upd', n: 0});"
               "db.getCollection(%S).updateMany({group: 'upd'}, {$inc: {n: 1}});"
               "db.getCollection(%S).replaceOne("
               "{_id: 'd'}, {_id: 'd', group: 'replaced', n: 9});"
               "db.getCollection(%S).insertOne({_id: 'e', n: 5});"
               "db.getCollection(%S).deleteOne({_id: 'e'})")
              collection collection collection collection collection collection))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find("
                           "{_id: {$in: ['c', 'd', 'e']}}, "
                           "{_id: 1, n: 1, group: 1})"
                           ".sort({_id: 1})")
                          collection))
                   (result (clutch-db-query conn code))
                   (columns (mapcar (lambda (column)
                                      (plist-get column :name))
                                    (clutch-db-result-columns result)))
                   (rows (clutch-db-result-rows result)))
              (dolist (name '("_id" "n" "group"))
                (should (member name columns)))
              (cl-labels ((cell (row name)
                            (nth (cl-position name columns :test #'equal)
                                 row)))
                (should (equal
                         (mapcar (lambda (row)
                                   (list (cell row "_id")
                                         (cell row "n")
                                         (cell row "group")))
                                 rows)
                         '(("c" 4 "upd")
                           ("d" 9 "replaced"))))))
            (ignore-errors
              (clutch-db-query
               conn
               (format
                "db.getCollection(%S).dropIndex('group_n_idx')"
                collection)))
            (clutch-db-query
             conn
             (format
              (concat
               "db.getCollection(%S).createIndex("
               "{group: 1, n: -1}, {name: 'group_n_idx'})")
              collection))
            (let* ((result
                    (clutch-db-query
                     conn
                     (format "db.getCollection(%S).listIndexes()"
                             collection)))
                   (columns (mapcar (lambda (column)
                                      (plist-get column :name))
                                    (clutch-db-result-columns result)))
                   (name-pos (cl-position "name" columns :test #'equal)))
              (should name-pos)
              (should (member "group_n_idx"
                              (mapcar (lambda (row)
                                        (nth name-pos row))
                                      (clutch-db-result-rows result)))))
            (let* ((result
                    (clutch-db-query
                     conn
                     (format
                      "db.getCollection(%S).countDocuments({group: 'upd'})"
                      collection))))
              (should (equal (clutch-db-result-rows result) '((1)))))
            (let* ((result
                    (clutch-db-query
                     conn
                     (format
                      (concat
                       "db.getCollection(%S).distinct("
                       "'group', {_id: {$in: ['c', 'd']}})")
                      collection)))
                   (values (sort (mapcar #'car
                                          (clutch-db-result-rows result))
                                 #'string<)))
              (should (equal values '("replaced" "upd"))))
            (let* ((result
                    (clutch-db-query
                     conn
                     (format
                      "db.getCollection(%S).estimatedDocumentCount()"
                      collection)))
                   (count (caar (clutch-db-result-rows result))))
              (should (>= count 4)))
            (clutch-db-query
             conn
             (format "db.getCollection(%S).dropIndex('group_n_idx')"
                     collection))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find({price: 12.5}, "
                           "{_id: 1, price: 1})")
                          collection))
                   (result (clutch-db-query conn code)))
              (should (equal (clutch-db-test--visible-result-column-names
                              result)
                             '("_id" "price")))
              (should (equal (clutch-db-test--visible-result-rows result)
                             '(("a" 12.5)))))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find("
                           "{amount: Decimal128('12.3400')}, "
                           "{_id: 1, amount: 1})")
                          collection))
                   (result (clutch-db-query conn code)))
              (should (equal (clutch-db-test--visible-result-column-names
                              result)
                             '("_id" "amount")))
              (should (equal (clutch-db-test--visible-result-rows result)
                             '(("a" "{\"$numberDecimal\":\"12.3400\"}")))))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find("
                           "{createdAt: ISODate('2024-01-02T03:04:05.678Z')}, "
                           "{_id: 1, createdAt: 1})")
                          collection))
                   (result (clutch-db-query conn code)))
              (should (equal (clutch-db-test--visible-result-column-names
                              result)
                             '("_id" "createdAt")))
              (should (equal (clutch-db-test--visible-result-rows result)
                             '(("a" "{\"$date\":1704164645678}"))))))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find("
                           "{ts: Timestamp(1700000000, 7)}, "
                           "{_id: 1, ts: 1})")
                          collection))
                   (result (clutch-db-query conn code)))
              (should (equal (clutch-db-test--visible-result-column-names
                              result)
                             '("_id" "ts")))
              (should (equal (clutch-db-test--visible-result-rows result)
                             '(("a" "{\"$timestamp\":{\"t\":1700000000,\"i\":7}}")))))
            (let* ((code (format
                          (concat
                           "db.getCollection(%S).find("
                           "{n32: {$type: 'int'}, n64: {$type: 'long'}}, "
                           "{_id: 1, n32: 1, n64: 1})")
                          collection))
                   (result (clutch-db-query conn code)))
              (should (equal (clutch-db-test--visible-result-column-names
                              result)
                             '("_id" "n32" "n64")))
              (should (equal (clutch-db-test--visible-result-rows result)
                             '(("a" 7 7)))))
        (ignore-errors
          (clutch-db-query
           conn
           (format "db.getCollection(%S).drop()" collection)))))))

(ert-deftest clutch-db-test-mongodb-live-schema ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB metadata should expose databases, collections, and sampled keys."
  (clutch-db-test--with-mongodb conn
    (let* ((collection (clutch-db-test--mongodb-live-collection "schema"))
           (validation-collection (concat collection "_validation")))
      (unwind-protect
          (progn
            (clutch-db-query
             conn
             (format
              (concat
               "db.getCollection(%S).deleteMany({});"
               "db.getCollection(%S).insertOne({_id: 'sample-a', field: 1});"
               "db.getCollection(%S).insertOne({_id: 'sample-b', extra: true})")
              collection collection collection))
            (clutch-db-query
             conn
             (format
              "db.getCollection(%S).createIndex({field: 1}, {name: 'field_idx'})"
              collection))
            (clutch-db-query
             conn
             (format
              (concat
               "db.createCollection(%S, "
               "{validator: {$jsonSchema: {bsonType: 'object', "
               "required: ['status'], "
               "properties: {status: {bsonType: 'string'}}}}, "
               "validationAction: 'error', validationLevel: 'strict'})")
              validation-collection))
            (should (member (clutch-db-current-schema conn)
                            (clutch-db-list-schemas conn)))
            (should (member collection (clutch-db-list-tables conn)))
            (let ((entries (clutch-db-list-table-entries conn)))
              (should
               (seq-some
                (lambda (entry)
                  (and (equal (plist-get entry :name) collection)
                       (equal (plist-get entry :type) "COLLECTION")))
                entries)))
            (let ((columns (clutch-db-list-columns conn collection)))
              (should (member "_id" columns))
              (should (member "field" columns))
              (should (member "extra" columns)))
            (let* ((indexes (clutch-db-list-objects conn 'indexes))
                   (index (seq-find
                           (lambda (entry)
                             (and (equal (plist-get entry :name) "field_idx")
                                  (equal (plist-get entry :target-table)
                                         collection)))
                           indexes)))
              (should index)
              (should (equal (clutch-db-object-details conn index)
                             '((:name "field" :position 1 :descend "ASC"))))
              (should (string-match-p
                       "\"name\":\"field_idx\""
                       (clutch-db-object-definition conn index))))
            (should (string-match-p
                     collection
                     (clutch-db-object-definition
                      conn (list :name collection :type "COLLECTION"))))
            (let ((stats (clutch-db-object-action-metadata
                          conn
                          (list :name collection :type "COLLECTION")
                          'show-stats)))
              (should (string-match-p (format "\"collection\":\"%s\""
                                              collection)
                                      stats))
              (should (string-match-p "\"count\":2" stats))
              (should (string-match-p "\"storageSize\":" stats))
              (should (string-match-p "\"totalIndexSize\":" stats))
              (should (string-match-p "\"indexSizes\"" stats)))
            (let ((validation
                   (clutch-db-object-action-metadata
                    conn
                    (list :name validation-collection :type "COLLECTION")
                    'show-validation)))
              (should (string-match-p "\"configured\":true" validation))
              (should (string-match-p "\"validationAction\":\"error\""
                                      validation))
              (should (string-match-p "\"validationLevel\":\"strict\""
                                      validation))
              (should (string-match-p "\"\\$jsonSchema\"" validation))))
        (ignore-errors
          (clutch-db-query
           conn
           (format
            "db.getCollection(%S).drop();db.getCollection(%S).drop()"
            collection validation-collection)))))))

(ert-deftest clutch-db-test-mongodb-live-set-current-schema ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB schema switching should change the target database."
  (clutch-db-test--with-mongodb conn
    (let ((original (clutch-db-current-schema conn))
          (schema (format "%s_switch_%d"
                          clutch-db-test-mongodb-database
                          (emacs-pid)))
          (collection "clutch_switch"))
      (unwind-protect
          (progn
            (clutch-db-set-current-schema conn schema)
            (should (equal (clutch-db-current-schema conn) schema))
            (clutch-db-query
             conn
             (format "db.getCollection(%S).insertOne({_id: 'ok'})" collection))
            (should (member collection (clutch-db-list-tables conn))))
        (ignore-errors (clutch-db-query conn "db.dropDatabase()"))
        (clutch-db-set-current-schema conn original)))))

(ert-deftest clutch-db-test-mongodb-live-sibling-db ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB getSiblingDB() should target another database without switching."
  (clutch-db-test--with-mongodb conn
    (let ((original (clutch-db-current-schema conn))
          (schema (format "%s_sibling_%d"
                          clutch-db-test-mongodb-database
                          (emacs-pid)))
          (collection "clutch_sibling"))
      (unwind-protect
          (progn
            (should (equal
                     (caar (clutch-db-result-rows
                            (clutch-db-query conn "db.getName()")))
                     original))
            (should (equal
                     (caar
                      (clutch-db-result-rows
                       (clutch-db-query
                        conn
                        (format "db.getSiblingDB(%S).getName()" schema))))
                     schema))
            (clutch-db-query
             conn
             (format "db.getSiblingDB(%S).createCollection(%S)"
                     schema collection))
            (clutch-db-query
             conn
             (format
              (concat
               "db.getSiblingDB(%S).getCollection(%S)"
               ".insertOne({_id: 'ok', n: 1})")
              schema collection))
            (let* ((result
                    (clutch-db-query
                     conn
                     (format
                      (concat
                       "db.getSiblingDB(%S).getCollection(%S)"
                       ".find({_id: 'ok'}, {_id: 1, n: 1})")
                      schema collection)))
                   (columns (mapcar (lambda (column)
                                      (plist-get column :name))
                                    (clutch-db-result-columns result))))
              (should (equal columns '("_id" "n" "clutch__document")))
              (should (equal (clutch-db-test--visible-result-rows result)
                             '(("ok" 1)))))
            (should (equal (clutch-db-current-schema conn) original)))
        (ignore-errors
          (clutch-db-query
           conn
           (format "db.getSiblingDB(%S).dropDatabase()" schema)))))))

(ert-deftest clutch-db-test-mongodb-live-error ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB query errors should signal `clutch-db-error'."
  (clutch-db-test--with-mongodb conn
    (should-error (clutch-db-query conn "db.getCollection(")
                  :type 'clutch-db-error)
    (let ((collection (clutch-db-test--mongodb-live-collection "error")))
      (unwind-protect
          (progn
            (clutch-db-query
             conn
             (format
              (concat
               "db.getCollection(%S).deleteMany({});"
               "db.getCollection(%S).insertOne({_id: 'dup'})")
              collection collection))
            (let ((err (should-error
                        (clutch-db-query
                         conn
                         (format
                          "db.getCollection(%S).insertOne({_id: 'dup'})"
                          collection))
                        :type 'clutch-db-error)))
              (should (string-match-p
                       "\\(DuplicateKey\\|duplicate key\\|E11000\\)"
                       (error-message-string err)))))
        (ignore-errors
          (clutch-db-query
           conn
           (format "db.getCollection(%S).drop()" collection)))))))

(ert-deftest clutch-db-test-redis-live-connect ()
  :tags '(:db-live :redis-live)
  "Redis connection should return a live key/value conn."
  (clutch-db-test--with-redis conn
    (should (clutch-db-live-p conn))
    (should (eq (clutch-db-backend-key conn) 'redis))
    (should (equal (clutch-db-display-name conn) "Redis"))
    (should (equal (clutch-db-current-schema conn)
                   (format "%s" clutch-db-test-redis-database)))))

(ert-deftest clutch-db-test-redis-live-query ()
  :tags '(:db-live :redis-live)
  "Redis commands should render through Clutch result grids."
  (clutch-db-test--with-redis conn
    (let ((string-key (clutch-db-test--redis-live-key "string"))
          (hash-key (clutch-db-test--redis-live-key "hash"))
          (list-key (clutch-db-test--redis-live-key "list")))
      (unwind-protect
          (progn
            (should (equal
                     (clutch-db-result-rows
                      (clutch-db-query conn (format "SET %S hello" string-key)))
                     '(("OK"))))
            (should (equal
                     (clutch-db-result-rows
                      (clutch-db-query conn (format "GET %S" string-key)))
                     '(("hello"))))
            (clutch-db-query conn (format "HSET %S name Ada tier pro" hash-key))
            (let ((result (clutch-db-query conn (format "HGETALL %S" hash-key))))
              (should (equal (clutch-db-result-column-names
                              (clutch-db-result-columns result))
                             '("field" "value")))
              (should (member '("name" "Ada") (clutch-db-result-rows result)))
              (should (member '("tier" "pro") (clutch-db-result-rows result))))
            (clutch-db-query conn (format "RPUSH %S a b" list-key))
            (should (equal
                     (clutch-db-result-rows
                      (clutch-db-query conn (format "LRANGE %S 0 -1" list-key)))
                     '((0 "a") (1 "b")))))
        (ignore-errors
          (clutch-db-query conn (format "DEL %S %S %S"
                                        string-key hash-key list-key)))))))

(ert-deftest clutch-db-test-redis-live-schema ()
  :tags '(:db-live :redis-live)
  "Redis metadata should expose keys as KEY objects."
  (clutch-db-test--with-redis conn
    (let ((hash-key (clutch-db-test--redis-live-key "schema")))
      (unwind-protect
          (progn
            (clutch-db-query conn (format "HSET %S field value" hash-key))
            (should (member hash-key (clutch-db-list-tables conn)))
            (let ((entry (seq-find
                          (lambda (candidate)
                            (equal (plist-get candidate :name) hash-key))
                          (clutch-db-list-table-entries conn))))
              (should entry)
              (should (equal (plist-get entry :type) "KEY"))
              (should-not (plist-get entry :value-type))
              (should (equal
                       (plist-get
                        (clutch-db-object-entry-metadata conn entry)
                        :value-type)
                       "HASH"))
              (should (equal (clutch-db-object-browse-query conn entry)
                             (format "HGETALL %S" hash-key)))
              (let ((details (clutch-db-object-details conn entry)))
                (should (member '("Type" . "hash") details))
                (should (member '("Exists" . "yes") details)))))
        (ignore-errors
          (clutch-db-query conn (format "DEL %S" hash-key)))))))

(ert-deftest clutch-db-test-redis-live-error ()
  :tags '(:db-live :redis-live)
  "Redis command errors should signal `clutch-db-error'."
  (clutch-db-test--with-redis conn
    (let ((string-key (clutch-db-test--redis-live-key "error")))
      (unwind-protect
          (progn
            (clutch-db-query conn (format "SET %S hello" string-key))
            (should-error
             (clutch-db-query conn (format "LRANGE %S 0 -1" string-key))
             :type 'clutch-db-error))
        (ignore-errors
          (clutch-db-query conn (format "DEL %S" string-key)))))))

;;;; Live integration tests — JDBC / Oracle
;;
;; Oracle is the primary JDBC target for clutch.  These tests verify:
;;   • agent start-up and basic query round-trip
;;   • manual-commit mode (Oracle default, matches DataGrip behaviour)
;;   • explicit COMMIT and ROLLBACK RPCs
;;   • schema introspection via DatabaseMetaData
;;
;; To enable: set `clutch-db-test-jdbc-oracle-password' and point
;; `clutch-jdbc-agent-dir' at a directory that contains the jar and a
;; drivers/ subdirectory with ojdbc11.jar (or equivalent).
;;
;; Quick local setup (OrbStack):
;;   docker run -d --name clutch-oracle -e ORACLE_PASSWORD=test \
;;     -p 1521:1521 gvenzl/oracle-free:slim-faststart

(defvar clutch-db-test-jdbc-oracle-host "127.0.0.1"
  "Host for Oracle JDBC live tests.")
(defvar clutch-db-test-jdbc-oracle-port 1521
  "Port for Oracle JDBC live tests.")
(defvar clutch-db-test-jdbc-oracle-user "system"
  "User for Oracle JDBC live tests.")
(defvar clutch-db-test-jdbc-oracle-password nil
  "Password for Oracle JDBC live tests.  Non-nil enables the :jdbc-live suite.")
(defvar clutch-db-test-jdbc-oracle-service "freepdb1"
  "Service name for Oracle JDBC live tests (gvenzl/oracle-free default).")

(defvar clutch-db-test-jdbc-mssql-host "127.0.0.1"
  "Host for SQL Server JDBC live tests.")
(defvar clutch-db-test-jdbc-mssql-port 1433
  "Port for SQL Server JDBC live tests.")
(defvar clutch-db-test-jdbc-mssql-user "sa"
  "User for SQL Server JDBC live tests.")
(defvar clutch-db-test-jdbc-mssql-password nil
  "Password for SQL Server JDBC live tests.  Non-nil enables the :mssql-live suite.")
(defvar clutch-db-test-jdbc-mssql-database "master"
  "Database name for SQL Server JDBC live tests.")

(defvar clutch-db-test-jdbc-clickhouse-host "127.0.0.1"
  "Host for ClickHouse JDBC live tests.")
(defvar clutch-db-test-jdbc-clickhouse-port 8123
  "Port for ClickHouse JDBC live tests.")
(defvar clutch-db-test-jdbc-clickhouse-user "default"
  "User for ClickHouse JDBC live tests.")
(defvar clutch-db-test-jdbc-clickhouse-password nil
  "Password for ClickHouse JDBC live tests.
Non-nil enables the :clickhouse-live suite.")
(defvar clutch-db-test-jdbc-clickhouse-database "default"
  "Database name for ClickHouse JDBC live tests.")

(defvar clutch-db-test-sql-interface-mongodb-url nil
  "JDBC URL for MongoDB SQL Interface live tests.")
(defvar clutch-db-test-sql-interface-mongodb-host nil
  "Host for MongoDB SQL Interface live tests.")
(defvar clutch-db-test-sql-interface-mongodb-port 27017
  "Port for MongoDB SQL Interface live tests.")
(defvar clutch-db-test-sql-interface-mongodb-user nil
  "User for MongoDB SQL Interface live tests.")
(defvar clutch-db-test-sql-interface-mongodb-password nil
  "Password for MongoDB SQL Interface live tests.")
(defvar clutch-db-test-sql-interface-mongodb-database nil
  "Query database for MongoDB SQL Interface live tests.
Non-nil, together with URL or host, enables the :sql-interface-mongodb-live suite.")
(defvar clutch-db-test-sql-interface-mongodb-auth-database nil
  "Optional auth database for structured MongoDB SQL Interface live test URLs.")
(defvar clutch-db-test-sql-interface-mongodb-props nil
  "Additional JDBC properties for MongoDB SQL Interface live tests.")

(defun clutch-db-test--sql-interface-mongodb-live-configured-p ()
  "Return non-nil when MongoDB SQL Interface live connection data is configured."
  (and clutch-db-test-sql-interface-mongodb-database
       (or clutch-db-test-sql-interface-mongodb-url
           clutch-db-test-sql-interface-mongodb-host)))

(defun clutch-db-test--sql-interface-mongodb-live-params ()
  "Return connection params for MongoDB SQL Interface live tests."
  (let ((params (if clutch-db-test-sql-interface-mongodb-url
                    (list :url clutch-db-test-sql-interface-mongodb-url)
                  (list :host clutch-db-test-sql-interface-mongodb-host
                        :port clutch-db-test-sql-interface-mongodb-port))))
    (setq params
          (plist-put params :database clutch-db-test-sql-interface-mongodb-database))
    (when clutch-db-test-sql-interface-mongodb-auth-database
      (setq params
            (plist-put params :auth-database
                       clutch-db-test-sql-interface-mongodb-auth-database)))
    (when clutch-db-test-sql-interface-mongodb-user
      (setq params (plist-put params :user clutch-db-test-sql-interface-mongodb-user)))
    (when clutch-db-test-sql-interface-mongodb-password
      (setq params
            (plist-put params :password
                       clutch-db-test-sql-interface-mongodb-password)))
    (when clutch-db-test-sql-interface-mongodb-props
      (setq params (plist-put params :props clutch-db-test-sql-interface-mongodb-props)))
    (setq params (plist-put params :surface 'sql-interface))
    params))

(defmacro clutch-db-test--with-oracle (var &rest body)
  "Execute BODY with VAR bound to a live Oracle JDBC connection.
Skips if `clutch-db-test-jdbc-oracle-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-jdbc-oracle-password)
       (ert-skip "Set clutch-db-test-jdbc-oracle-password to enable Oracle live tests")
     (require 'clutch-db-jdbc)
     (let ((,var (clutch-db-connect
                  'oracle
                  (list :host clutch-db-test-jdbc-oracle-host
                        :port clutch-db-test-jdbc-oracle-port
                        :user clutch-db-test-jdbc-oracle-user
                        :password clutch-db-test-jdbc-oracle-password
                        :database clutch-db-test-jdbc-oracle-service))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(defmacro clutch-db-test--with-mssql (var &rest body)
  "Execute BODY with VAR bound to a live SQL Server JDBC connection.
Skips if `clutch-db-test-jdbc-mssql-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-jdbc-mssql-password)
       (ert-skip "Set clutch-db-test-jdbc-mssql-password to enable SQL Server live tests")
     (require 'clutch-db-jdbc)
     (let ((,var (clutch-db-connect
                  'sqlserver
                  (list :host clutch-db-test-jdbc-mssql-host
                        :port clutch-db-test-jdbc-mssql-port
                        :user clutch-db-test-jdbc-mssql-user
                        :password clutch-db-test-jdbc-mssql-password
                        :database clutch-db-test-jdbc-mssql-database
                        :props '(("trustServerCertificate" . "true"))))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(defmacro clutch-db-test--with-clickhouse (var &rest body)
  "Execute BODY with VAR bound to a live ClickHouse JDBC connection.
Skips if `clutch-db-test-jdbc-clickhouse-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-jdbc-clickhouse-password)
       (ert-skip "Set clutch-db-test-jdbc-clickhouse-password to enable ClickHouse live tests")
     (require 'clutch-db-jdbc)
     (let ((,var (clutch-db-connect
                  'clickhouse
                  (list :host clutch-db-test-jdbc-clickhouse-host
                        :port clutch-db-test-jdbc-clickhouse-port
                        :user clutch-db-test-jdbc-clickhouse-user
                        :password clutch-db-test-jdbc-clickhouse-password
                        :database clutch-db-test-jdbc-clickhouse-database))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(defmacro clutch-db-test--with-sql-interface-mongodb (var &rest body)
  "Execute BODY with VAR bound to a live MongoDB SQL Interface JDBC connection.
Skips unless `clutch-db-test-sql-interface-mongodb-database' and either
`clutch-db-test-sql-interface-mongodb-url' or
`clutch-db-test-sql-interface-mongodb-host' are set."
  (declare (indent 1))
  `(if (not (clutch-db-test--sql-interface-mongodb-live-configured-p))
       (ert-skip "Set clutch-db-test-sql-interface-mongodb-url/host and database to enable MongoDB SQL Interface live tests")
     (require 'clutch-db-jdbc)
     (let ((,var (clutch-db-connect
                  'mongodb
                  (clutch-db-test--sql-interface-mongodb-live-params))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-connect ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC connection should start the agent and return a live conn."
  (clutch-db-test--with-oracle conn
    (should (clutch-db-live-p conn))
    (should (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-oracle-live-query ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC query should return correct columns and rows."
  (clutch-db-test--with-oracle conn
    (let ((result (clutch-db-query conn "SELECT 1 AS n FROM DUAL")))
      (should (clutch-db-result-p result))
      (should (= (length (clutch-db-result-rows result)) 1)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-manual-commit ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC commit RPC should persist DML."
  (clutch-db-test--with-oracle conn
    (let* ((tbl (clutch-db-test--live-name "CC_TEST"))
           (drop-sql (format "DROP TABLE %s" tbl)))
      (unwind-protect
          (progn
            (ignore-errors (clutch-db-query conn drop-sql))
            (should (clutch-db-manual-commit-p conn))
            (clutch-db-query conn (format "CREATE TABLE %s (id NUMBER)" tbl))
            ;; DDL auto-commits in Oracle; subsequent DML starts a new tx.
            (clutch-db-query conn (format "INSERT INTO %s VALUES (1)" tbl))
            (clutch-db-commit conn)
            (let ((result (clutch-db-query
                           conn (format "SELECT COUNT(*) FROM %s" tbl))))
              (should (equal (caar (clutch-db-result-rows result)) "1"))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-rollback ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC rollback RPC should discard uncommitted DML."
  (clutch-db-test--with-oracle conn
    (let* ((tbl (clutch-db-test--live-name "CC_RB"))
           (drop-sql (format "DROP TABLE %s" tbl)))
      (unwind-protect
          (progn
            (ignore-errors (clutch-db-query conn drop-sql))
            (clutch-db-query conn (format "CREATE TABLE %s (id NUMBER)" tbl))
            (clutch-db-query conn (format "INSERT INTO %s VALUES (1)" tbl))
            (clutch-db-rollback conn)
            (let ((result (clutch-db-query
                           conn (format "SELECT COUNT(*) FROM %s" tbl))))
              (should (equal (caar (clutch-db-result-rows result)) "0"))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-toggle-auto-commit ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC set-auto-commit RPC should toggle between manual and auto modes."
  (clutch-db-test--with-oracle conn
    ;; Oracle starts in manual-commit mode
    (should (clutch-db-manual-commit-p conn))
    ;; Toggle to auto-commit
    (clutch-db-set-auto-commit conn t)
    (should-not (clutch-db-manual-commit-p conn))
    ;; Toggle back to manual-commit
    (clutch-db-set-auto-commit conn nil)
    (should (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-oracle-live-schema ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC schema introspection should list tables and columns."
  (clutch-db-test--with-oracle conn
    (let* ((tbl (clutch-db-test--live-name "CC_SCHEMA"))
           (drop-sql (format "DROP TABLE %s" tbl)))
      (unwind-protect
          (progn
            (ignore-errors (clutch-db-query conn drop-sql))
            (clutch-db-query conn
             (format "CREATE TABLE %s (id NUMBER PRIMARY KEY, name VARCHAR2(64))" tbl))
            (let ((tables (clutch-db-list-tables conn)))
              (should (member tbl tables)))
            (let ((cols (clutch-db-list-columns conn tbl)))
              (should (member "ID" cols))
              (should (member "NAME" cols))))
        (ignore-errors
          (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-row-identity-uses-unique-not-null ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC should use a non-null unique key when no primary key exists."
  (clutch-db-test--with-oracle conn
    (let* ((tbl (clutch-db-test--live-name "CC_UID"))
           (idx (clutch-db-test--live-name "CC_UID_UQ"))
           (drop-sql (format "DROP TABLE %s" tbl)))
      (unwind-protect
          (progn
            (ignore-errors (clutch-db-query conn drop-sql))
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (code VARCHAR2(32) NOT NULL, name VARCHAR2(64), CONSTRAINT %s UNIQUE (code))"
                     tbl idx))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn tbl))))
              (should (equal (plist-get candidate :kind) 'unique-key))
              (should (equal (plist-get candidate :name) idx))
              (should (equal (plist-get candidate :columns) '("CODE")))))
        (ignore-errors
          (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-row-identity-falls-back-to-rowid ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC should use ROWID to update a table without a logical key."
  (clutch-db-test--with-oracle conn
    (let* ((tbl (clutch-db-test--live-name "CC_ROWID"))
           (drop-sql (format "DROP TABLE %s" tbl)))
      (unwind-protect
          (progn
            (ignore-errors (clutch-db-query conn drop-sql))
            (clutch-db-query conn
                             (format "CREATE TABLE %s (name VARCHAR2(64))"
                                     tbl))
            (clutch-db-query conn
                             (format "INSERT INTO %s (name) VALUES ('before')"
                                     tbl))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn tbl))))
              (should (equal (plist-get candidate :kind) 'row-locator))
              (should (equal (plist-get candidate :name) "ROWID"))
              (should (equal (plist-get candidate :select-expressions)
                             '("ROWID")))
              (should (equal (plist-get candidate :where-sql) "ROWID = ?")))
            (let* ((rows (clutch-db-result-rows
                          (clutch-db-query
                           conn
                           (format "SELECT ROWID, name FROM %s" tbl))))
                   (rowid (caar rows))
                   (update-result
                    (clutch-db-execute-params
                     conn
                     (format "UPDATE %s SET name = ? WHERE ROWID = ?" tbl)
                     (list "after" rowid))))
              (should (equal (cadar rows) "before"))
              (should (equal (clutch-db-result-affected-rows update-result)
                             1))
              (should (equal (clutch-db-result-rows
                              (clutch-db-query
                               conn
                               (format "SELECT name FROM %s WHERE ROWID = '%s'"
                                       tbl rowid)))
                             '(("after"))))))
        (ignore-errors
          (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-cancel-keeps-connection-usable ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC cancel should interrupt the query and keep the session usable."
  (clutch-db-test--with-oracle conn
    (let ((request-id
           (clutch-jdbc--send
            "execute"
            `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
              (sql . "SELECT COUNT(*) FROM ALL_OBJECTS a, ALL_OBJECTS b")
              (fetch-size . ,clutch-jdbc-fetch-size))))
          cancel-result)
      (puthash request-id t clutch-jdbc--ignored-response-ids)
      (with-timeout
          (5 (ert-fail "Oracle live execute did not become cancellable"))
        (while (not (eq t (plist-get cancel-result :cancelled)))
          (setq cancel-result
                (clutch-jdbc--rpc
                 "cancel"
                 `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))
                 (clutch-jdbc--conn-rpc-timeout conn)))
          (unless (eq t (plist-get cancel-result :cancelled))
            (sleep-for 0.05))))
      (should (eq t (plist-get cancel-result :cancelled)))
      (let ((result (clutch-db-query conn "SELECT 42 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "42")))
      (should-not (gethash request-id clutch-jdbc--ignored-response-ids)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-wire-query-error-carries-diagnostics ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Wire-level Oracle JDBC errors should return structured diagnostics."
  (clutch-db-test--with-oracle conn
    (let* ((token (clutch-db-test--live-name "definitely_missing"))
           (request-id
            (clutch-jdbc--send
             "execute"
             `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
               (sql . ,(format "SELECT %s FROM DUAL" token))
               (fetch-size . ,clutch-jdbc-fetch-size))))
           (response (clutch-jdbc--recv-response
                      request-id
                      (clutch-jdbc--conn-rpc-timeout conn)
                      "execute")))
      (should-not (eq t (plist-get response :ok)))
      (let ((diag (plist-get response :diag)))
        (should (equal (plist-get diag :op) "execute"))
        (should (= (plist-get diag :request-id) request-id))
        (should (= (plist-get diag :conn-id) (clutch-jdbc-conn-conn-id conn)))
        (should (equal (plist-get diag :category) "query"))
        (should (string-match-p
                 (upcase token)
                 (upcase (plist-get diag :raw-message)))))
      (let ((result (clutch-db-query conn "SELECT 42 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "42"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-wire-query-error-carries-debug-payload ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Wire-level Oracle JDBC errors should carry opt-in backend debug payloads."
  (let ((clutch-debug-mode t))
    (clutch-db-test--with-oracle conn
      (let* ((token (clutch-db-test--live-name "definitely_missing"))
             (request-id
              (clutch-jdbc--send
               "execute"
               `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                 (sql . ,(format "SELECT %s FROM DUAL" token))
                 (fetch-size . ,clutch-jdbc-fetch-size))))
             (response (clutch-jdbc--recv-response
                        request-id
                        (clutch-jdbc--conn-rpc-timeout conn)
                        "execute")))
        (should-not (eq t (plist-get response :ok)))
        (let ((debug (plist-get response :debug)))
          (should (stringp (plist-get debug :stack-trace)))
          (should (string-match-p "java\\.sql\\."
                                  (plist-get debug :stack-trace)))
          (should (= (plist-get (plist-get debug :request-context) :fetch-size)
                     clutch-jdbc-fetch-size)))
        (let ((result (clutch-db-query conn "SELECT 44 AS answer FROM DUAL")))
          (should (equal (caar (clutch-db-result-rows result)) "44")))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-query-error-caches-diagnostics ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "High-level Oracle JDBC query errors should stay on the current connection."
  (let ((clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (clutch-db-test--with-oracle conn
      (let ((token (clutch-db-test--live-name "definitely_missing")))
        (condition-case err
            (progn
              (clutch-db-query conn (format "SELECT %s FROM DUAL" token))
              (should nil))
          (clutch-db-error
           (should (stringp (cadr err)))
           (let ((details (clutch-db-error-details conn)))
             (should (equal (plist-get (plist-get details :diag) :op) "execute"))
             (should (= (plist-get (plist-get details :diag) :conn-id)
                        (clutch-jdbc-conn-conn-id conn)))
             (should (equal (plist-get (plist-get details :diag) :category) "query"))
             (should (string-match-p
                      (upcase token)
                      (upcase (plist-get (plist-get details :diag) :raw-message))))))))
      (let ((result (clutch-db-query conn "SELECT 43 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "43"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-query-error-caches-debug-payload ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "High-level Oracle JDBC query errors should cache opt-in debug payloads."
  (let ((clutch-debug-mode t)
        (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (clutch-db-test--with-oracle conn
      (let ((token (clutch-db-test--live-name "definitely_missing")))
        (condition-case err
            (progn
              (clutch-db-query conn (format "SELECT %s FROM DUAL" token))
              (should nil))
          (clutch-db-error
           (should (stringp (cadr err)))
           (let ((debug (plist-get (clutch-db-error-details conn) :debug)))
             (should (stringp (plist-get debug :stack-trace)))
             (should (string-match-p "java\\.sql\\."
                                     (plist-get debug :stack-trace)))))))
      (let ((result (clutch-db-query conn "SELECT 45 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "45"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-wire-schema-switch-error-carries-generated-sql ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Wire-level Oracle schema-switch failures should expose generated SQL."
  (clutch-db-test--with-oracle conn
    (let* ((token (clutch-db-test--live-name "MISSING_SCHEMA"))
           (request-id
            (clutch-jdbc--send
             "set-current-schema"
             `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
               (schema . ,token))))
           (response (clutch-jdbc--recv-response
                      request-id
                      (clutch-jdbc--conn-rpc-timeout conn)
                      "set-current-schema")))
      (should-not (eq t (plist-get response :ok)))
      (let* ((diag (plist-get response :diag))
             (context (plist-get diag :context))
             (generated-sql (plist-get context :generated-sql)))
        (should (equal (plist-get diag :op) "set-current-schema"))
        (should (= (plist-get diag :request-id) request-id))
        (should (= (plist-get diag :conn-id) (clutch-jdbc-conn-conn-id conn)))
        (should (equal (plist-get diag :category) "metadata"))
        (should (equal (plist-get context :schema) token))
        (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" generated-sql))
        (should (string-match-p token generated-sql)))
      (let ((result (clutch-db-query conn "SELECT 42 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "42"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-schema-switch-error-caches-generated-sql ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "High-level Oracle schema-switch failures should stay on the current connection."
  (let ((clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (clutch-db-test--with-oracle conn
      (let ((token (clutch-db-test--live-name "MISSING_SCHEMA")))
        (condition-case err
            (progn
              (clutch-db-set-current-schema conn token)
              (should nil))
          (clutch-db-error
           (should (stringp (cadr err)))
           (let* ((details (clutch-db-error-details conn))
                  (diag (plist-get details :diag))
                  (context (plist-get diag :context))
                  (generated-sql (plist-get context :generated-sql)))
             (should (equal (plist-get diag :op) "set-current-schema"))
             (should (= (plist-get diag :conn-id)
                        (clutch-jdbc-conn-conn-id conn)))
             (should (equal (plist-get diag :category) "metadata"))
             (should (equal (plist-get context :schema) token))
             (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" generated-sql))
             (should (string-match-p token generated-sql))))))
      (let ((result (clutch-db-query conn "SELECT 43 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "43"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-low-priv-completion ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC low-privilege users should still get table completion and discovery."
  (if (null clutch-db-test-jdbc-oracle-password)
      (ert-skip "Set clutch-db-test-jdbc-oracle-password to enable Oracle live tests")
    (require 'clutch-db-jdbc)
    (let* ((admin (clutch-db-connect
                   'oracle
                   (list :host clutch-db-test-jdbc-oracle-host
                         :port clutch-db-test-jdbc-oracle-port
                         :user clutch-db-test-jdbc-oracle-user
                         :password clutch-db-test-jdbc-oracle-password
                         :database clutch-db-test-jdbc-oracle-service)))
           (user (clutch-db-test--live-name "CCLP"))
           (password "CcLowpriv123")
           (table-name (clutch-db-test--live-name "CC_LOWPRIV_TABLE")))
      (unwind-protect
          (progn
            (clutch-db-query
             admin
             (format "CREATE USER %s IDENTIFIED BY %s" user password))
            (clutch-db-query
             admin
             (format "GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO %s"
                     user))
            (let ((conn (clutch-db-connect
                         'oracle
                         (list :host clutch-db-test-jdbc-oracle-host
                               :port clutch-db-test-jdbc-oracle-port
                               :user user
                               :password password
                               :database clutch-db-test-jdbc-oracle-service))))
              (unwind-protect
                  (progn
                    (clutch-db-query
                     conn
                     (format "CREATE TABLE %s (id NUMBER PRIMARY KEY)" table-name))
                    (let ((tables (clutch-db-complete-tables conn "CC_LOW")))
                      (should (member table-name tables)))
                    (let ((entries (clutch-db-search-table-entries conn "CC_LOW")))
                      (should
                       (seq-some
                        (lambda (entry)
                          (and (equal (plist-get entry :name) table-name)
                               (equal (plist-get entry :schema) user)))
                        entries))))
                (clutch-db-disconnect conn))))
        (ignore-errors
          (clutch-db-query admin (format "DROP USER %s CASCADE" user)))
        (clutch-db-disconnect admin)))))

(ert-deftest clutch-db-test-jdbc-mssql-live-connect ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC connection should return a live conn."
  (clutch-db-test--with-mssql conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "SQL Server"))))

(ert-deftest clutch-db-test-jdbc-mssql-live-query ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC query should return one row for SELECT 1."
  (clutch-db-test--with-mssql conn
    (let ((result (clutch-db-query conn "SELECT 1 AS n")))
      (should (clutch-db-result-p result))
      (should (= (length (clutch-db-result-rows result)) 1))
      (should (equal (format "%s" (caar (clutch-db-result-rows result))) "1")))))

(ert-deftest clutch-db-test-jdbc-mssql-live-result-workflow ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC should handle real result workflows."
  (clutch-db-test--with-mssql conn
    (let* ((tbl (clutch-db-test--live-name "cc_mssql_flow"))
           (drop-sql (format "DROP TABLE IF EXISTS %s" tbl))
           (base-sql (format "SELECT id, name, score FROM %s ORDER BY id" tbl)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (id INT NOT NULL PRIMARY KEY, name NVARCHAR(32), score INT)"
                     tbl))
            (clutch-db-query
             conn
             (format "INSERT INTO %s (id, name, score) VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'cam', 30), (4, 'dan', 40), (5, 'eve', 50)"
                     tbl))
            (let* ((page-sql (clutch-db-build-paged-sql conn base-sql 0 2))
                   (rows (clutch-db-result-rows
                          (clutch-db-query conn page-sql))))
              (should (equal (mapcar (lambda (row) (format "%s" (car row))) rows)
                             '("1" "2"))))
            (let* ((sort-sql (clutch-db-build-paged-sql
                              conn base-sql 0 2 '("score" . "DESC")))
                   (rows (clutch-db-result-rows
                          (clutch-db-query conn sort-sql))))
              (should (equal (mapcar (lambda (row) (format "%s" (car row))) rows)
                             '("5" "4"))))
            (let* ((filter-sql
                    (clutch-db-apply-where
                     conn base-sql
                     (format "%s > 20" (clutch-db-escape-identifier conn "score"))))
                   (count-sql (clutch-db-build-count-sql conn filter-sql))
                   (count-rows (clutch-db-result-rows
                                (clutch-db-query conn count-sql))))
              (should (equal (format "%s" (caar count-rows)) "3")))
            (should (equal (clutch-db-primary-key-columns conn tbl) '("id")))
            (let ((update-result
                   (clutch-db-execute-params
                    conn
                    (format "UPDATE %s SET name = ? WHERE id = ?" tbl)
                    '("after" 3))))
              (should (= (clutch-db-result-affected-rows update-result) 1))
              (should (equal (clutch-db-result-rows
                              (clutch-db-query
                               conn
                               (format "SELECT name FROM %s WHERE id = 3" tbl)))
                             '(("after"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-jdbc-mssql-live-schema ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC schema introspection should return table entries."
  (clutch-db-test--with-mssql conn
    (let ((entries (clutch-db-list-table-entries conn)))
      (should (listp entries))
      (should (> (length entries) 0)))))

(ert-deftest clutch-db-test-sql-interface-mongodb-live-connect ()
  :tags '(:db-live :jdbc-live :sql-interface-mongodb-live)
  "MongoDB SQL Interface JDBC connection should return a live conn."
  (clutch-db-test--with-sql-interface-mongodb conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "MongoDB"))
    (should (eq (clutch-db-backend-key conn) 'mongodb))
    (should-not (clutch-db-manual-commit-supported-p conn))
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-sql-interface-mongodb-live-query ()
  :tags '(:db-live :jdbc-live :sql-interface-mongodb-live)
  "MongoDB SQL Interface should execute a read-only SQL Interface query."
  (clutch-db-test--with-sql-interface-mongodb conn
    (let* ((sql "SELECT * FROM [{'n': 1}]")
           (result (clutch-db-query conn sql))
           (rows (clutch-db-result-rows result)))
      (should (clutch-db-result-p result))
      (should (= (length rows) 1))
      (should (equal (format "%s" (caar rows)) "1"))
      (should (equal (clutch-db-build-paged-sql conn sql 1 1)
                     "SELECT * FROM [{'n': 1}] LIMIT 1 OFFSET 1")))))

(ert-deftest clutch-db-test-sql-interface-mongodb-live-schema ()
  :tags '(:db-live :jdbc-live :sql-interface-mongodb-live)
  "MongoDB SQL Interface metadata calls should return list-shaped results."
  (clutch-db-test--with-sql-interface-mongodb conn
    (should (listp (clutch-db-list-table-entries conn)))
    (should (listp (clutch-db-list-schemas conn)))))

(ert-deftest clutch-db-test-jdbc-clickhouse-live-connect ()
  :tags '(:db-live :jdbc-live :clickhouse-live)
  "ClickHouse JDBC connection should return a live conn."
  (clutch-db-test--with-clickhouse conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "ClickHouse"))))

(ert-deftest clutch-db-test-jdbc-clickhouse-live-result-workflow ()
  :tags '(:db-live :jdbc-live :clickhouse-live)
  "ClickHouse JDBC should handle real result workflows."
  (clutch-db-test--with-clickhouse conn
    (let* ((tbl (clutch-db-test--live-name "cc_ch_flow"))
           (drop-sql (format "DROP TABLE IF EXISTS %s" tbl))
           (base-sql (format "SELECT id, name, score FROM %s ORDER BY id" tbl)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (id Int32, name String, score Int32) ENGINE = Memory"
                     tbl))
            (clutch-db-query
             conn
             (format "INSERT INTO %s (id, name, score) VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'cam', 30), (4, 'dan', 40), (5, 'eve', 50)"
                     tbl))
            (let* ((page-sql (clutch-db-build-paged-sql conn base-sql 0 2))
                   (rows (clutch-db-result-rows
                          (clutch-db-query conn page-sql))))
              (should (equal (mapcar (lambda (row) (format "%s" (car row))) rows)
                             '("1" "2"))))
            (let* ((sort-sql (clutch-db-build-paged-sql
                              conn base-sql 0 2 '("score" . "DESC")))
                   (rows (clutch-db-result-rows
                          (clutch-db-query conn sort-sql))))
              (should (equal (mapcar (lambda (row) (format "%s" (car row))) rows)
                             '("5" "4"))))
            (let* ((filter-sql
                    (clutch-db-apply-where
                     conn base-sql
                     (format "%s > 20" (clutch-db-escape-identifier conn "score"))))
                   (count-sql (clutch-db-build-count-sql conn filter-sql))
                   (count-rows (clutch-db-result-rows
                                (clutch-db-query conn count-sql))))
              (should (equal (format "%s" (caar count-rows)) "3")))
            (let ((columns (clutch-db-list-columns conn tbl)))
              (should (member "id" columns))
              (should (member "name" columns))
              (should (member "score" columns))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-jdbc-clickhouse-live-schema ()
  :tags '(:db-live :jdbc-live :clickhouse-live)
  "ClickHouse JDBC schema introspection should return user table entries."
  (clutch-db-test--with-clickhouse conn
    (let* ((tbl (clutch-db-test--live-name "cc_ch_schema"))
           (drop-sql (format "DROP TABLE IF EXISTS %s" tbl)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (id Int32, name String) ENGINE = Memory"
                     tbl))
            (let ((entries (clutch-db-list-table-entries conn)))
              (should (listp entries))
              (should (cl-find tbl entries
                               :key (lambda (entry) (plist-get entry :name))
                               :test #'string=))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

;;;; Cross-backend consistency tests

(ert-deftest clutch-db-test-cross-type-categories ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends use consistent type categories."
  (clutch-db-test--require-cross-sql-live-backends)
  ;; Test numeric
  (clutch-db-test--with-local-mysql-tls
    (let ((mysql-conn (clutch-db-connect
                       'mysql
                       (clutch-db-test--mysql-live-params)))
          (pg-conn (clutch-db-connect
                    'pg
                    (clutch-db-test--pg-live-params))))
      (unwind-protect
          (progn
            ;; Both should return numeric type-category for integers
            (let* ((result (clutch-db-query mysql-conn "SELECT 42 AS n"))
                   (cols (clutch-db-result-columns result)))
              (should (eq (plist-get (car cols) :type-category) 'numeric)))
            (let* ((result (clutch-db-query pg-conn "SELECT 42 AS n"))
                   (cols (clutch-db-result-columns result)))
              (should (eq (plist-get (car cols) :type-category) 'numeric))))
        (when mysql-conn (clutch-db-disconnect mysql-conn))
        (when pg-conn (clutch-db-disconnect pg-conn))))))

(ert-deftest clutch-db-test-cross-null-handling ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends handle NULL values consistently."
  (clutch-db-test--require-cross-sql-live-backends)
  (clutch-db-test--with-local-mysql-tls
    (dolist (backend-spec (list (cons 'mysql
                                      (clutch-db-test--mysql-live-params))
                                (cons 'pg
                                      (clutch-db-test--pg-live-params))))
      (let ((conn (clutch-db-connect (car backend-spec) (cdr backend-spec))))
        (unwind-protect
            (let* ((result (clutch-db-query conn "SELECT NULL AS n"))
                   (row (car (clutch-db-result-rows result))))
              (should (null (car row))))
          (clutch-db-disconnect conn))))))

;;;; Unit tests — clutch-db-live-p (JDBC identity check)

(ert-deftest clutch-db-test-jdbc-live-p-variants ()
  "JDBC liveness should depend on current-agent identity and process state."
  (dolist (case '(("matching-live-process" fake-proc fake-proc t t)
                  ("dead-process" dead-proc dead-proc nil nil)
                  ("nil-agent-process" old-proc nil t nil)
                  ("mismatched-process" old-proc new-proc t nil)))
    (pcase-let ((`(,label ,conn-proc ,agent-proc ,livep ,expected) case))
      (ert-info ((format "live-p case: %s" label))
        (cl-letf (((symbol-function 'process-live-p) (lambda (_p) livep)))
          (let ((clutch-jdbc--agent-process agent-proc))
            (if expected
                (should (clutch-db-live-p
                         (make-clutch-jdbc-conn :process conn-proc :conn-id 1
                                                :params nil :busy nil)))
              (should-not (clutch-db-live-p
                           (make-clutch-jdbc-conn :process conn-proc :conn-id 1
                                                  :params nil :busy nil))))))))))

;;;; Unit tests — clutch-jdbc--recv-response timeout behaviour

(ert-deftest clutch-db-test-jdbc-recv-response-returns-matching ()
  "When a matching response is already queued, recv-response returns it immediately.
It does so without touching the agent process."
  (let ((clutch-jdbc--agent-process 'live-proc)
        (clutch-jdbc--response-queue
         (list '(:id 42 :ok t :result (:conn-id 1)))))
    (let ((result (clutch-jdbc--recv-response 42 10.0)))
      ;; Agent must NOT be killed.
      (should (eq clutch-jdbc--agent-process 'live-proc))
      (should (equal (plist-get result :id) 42)))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-kills-agent ()
  "When the RPC timeout fires, the agent process is killed and state is reset."
  (let (deleted-proc)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
              ((symbol-function 'delete-process)  (lambda (p) (setq deleted-proc p)))
              ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
      (let ((clutch-jdbc--agent-process 'fake-proc)
            (clutch-jdbc--response-queue '(stale)))
        ;; Timeout of 0.0 expires immediately.
        (should-error (clutch-jdbc--recv-response 9999 0.0) :type 'clutch-db-error)
        (should (eq deleted-proc 'fake-proc))
        (should (null clutch-jdbc--agent-process))
        (should (null clutch-jdbc--response-queue))))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-clears-async-callbacks ()
  "Sync timeout should clear pending async callbacks immediately."
  (let ((clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
        (cancelled nil))
    (puthash 77 (list :callback #'ignore :errback #'ignore :timer 'fake-timer)
             clutch-jdbc--async-callbacks)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
              ((symbol-function 'delete-process) #'ignore)
              ((symbol-function 'accept-process-output) (lambda (_p _s) nil))
              ((symbol-function 'cancel-timer)
               (lambda (timer)
                 (push timer cancelled))))
      (let ((clutch-jdbc--agent-process 'fake-proc)
            (clutch-jdbc--response-queue nil))
        (should-error (clutch-jdbc--recv-response 9999 0.0) :type 'clutch-db-error)
        (should-not (gethash 77 clutch-jdbc--async-callbacks))
        (should (equal cancelled '(fake-timer)))))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-agent-already-dead ()
  "When the agent already died, recv-response reports agent exit clearly."
  (let (deleted-proc)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) nil))
              ((symbol-function 'delete-process)  (lambda (p) (setq deleted-proc p)))
              ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
      (let ((clutch-jdbc--agent-process 'dead-proc)
            (clutch-jdbc--response-queue nil))
        (condition-case err
            (progn (clutch-jdbc--recv-response 9999 0.0) (should nil))
          (clutch-db-error
           (should (string-match-p "exited before replying" (cadr err)))))
        (should (null deleted-proc))
        (should (null clutch-jdbc--agent-process))))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-error-contains-connection-lost ()
  "A live-but-stuck agent still reports 'Connection lost' on timeout."
  (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
            ((symbol-function 'delete-process)  #'ignore)
            ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
    (let ((clutch-jdbc--agent-process 'fake-proc)
          (clutch-jdbc--response-queue nil))
      (condition-case err
          (progn (clutch-jdbc--recv-response 9999 0.0) (should nil))
        (clutch-db-error
         (should (string-match-p "Connection lost" (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-recv-response-connect-timeout-omits-reconnect-hint ()
  "Connect timeouts should not tell users to reconnect an unestablished session."
  (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
            ((symbol-function 'delete-process)  #'ignore)
            ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
    (let ((clutch-jdbc--agent-process 'fake-proc)
          (clutch-jdbc--response-queue nil))
      (condition-case err
          (progn (clutch-jdbc--recv-response 9999 0.0 "connect") (should nil))
        (clutch-db-error
         (should (string-match-p "Connection attempt timed out" (cadr err)))
         (should-not (string-match-p "reconnect with C-c C-e" (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-recv-response-agent-exit-reports-java-version-mismatch ()
  "An early agent exit with UnsupportedClassVersionError should report Java mismatch."
  (let ((stderr (get-buffer-create "*clutch-jdbc-agent-stderr*")))
    (unwind-protect
        (progn
          (with-current-buffer stderr
            (erase-buffer)
            (insert "Exception in thread \"main\" java.lang.UnsupportedClassVersionError: clutch/jdbc/Agent has been compiled by a more recent version of the Java Runtime\n"))
          (cl-letf (((symbol-function 'process-live-p) (lambda (_p) nil))
                    ((symbol-function 'delete-process) #'ignore)
                    ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
            (let ((clutch-jdbc--agent-process 'dead-proc)
                  (clutch-jdbc--response-queue nil)
                  (clutch-jdbc-agent-java-executable "java"))
              (condition-case err
                  (progn (clutch-jdbc--recv-response 9999 0.0) (should nil))
                (clutch-db-error
                 (should (string-match-p "requires a newer Java runtime" (cadr err)))
                 (should (string-match-p "`java'" (cadr err))))))))
      (kill-buffer stderr))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-points-to-debug-workflow-when-disabled ()
  "Connect errors should point users at the debug workflow when capture is off."
  (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
            ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 7))
            ((symbol-function 'clutch-jdbc--recv-response)
             (lambda (&rest _args)
               '(:ok nil
                 :error "diag-token-2038"
                 :diag (:category "connect")))))
    (condition-case err
        (progn
          (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
          (should nil))
      (clutch-db-error
       (should (string-match-p "diag-token-2038" (cadr err)))
       (should (string-match-p "clutch-debug-mode" (cadr err)))
       (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                               (cadr err)))))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-points-to-debug-buffer-when-enabled ()
  "Connect errors should point directly at the debug buffer when capture is on."
  (let ((clutch-debug-mode t))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 7))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 '(:ok nil
                   :error "diag-token-2038"
                   :diag (:category "connect")))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
            (should nil))
        (clutch-db-error
         (should (string-match-p "diag-token-2038" (cadr err)))
         (should-not (string-match-p "clutch-debug-mode" (cadr err)))
         (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                 (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-send-adds-debug-flag-when-debug-mode-enabled ()
  "JDBC requests should opt into backend debug payloads only in debug mode."
  (let ((clutch-jdbc--next-request-id 0)
        (clutch-debug-mode t)
        sent)
    (cl-letf (((symbol-function 'process-send-string)
               (lambda (_proc msg)
                 (setq sent msg))))
      (let ((clutch-jdbc--agent-process 'fake-proc))
        (clutch-jdbc--send "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb"))))
      (should (string-match-p "\"debug\":true" sent)))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-carries-structured-details ()
  "Connect errors should carry structured details in the condition data."
  (let ((diag '(:category "connect"
                :op "connect"
                :request-id 71
                :exception-class "java.sql.SQLNonTransientConnectionException"
                :sql-state "08071"
                :context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>"
                          :generated-sql "ALTER SESSION SET CURRENT_SCHEMA = \"REPORTING\""
                          :property-keys ("http_header_COOKIE" "socket_timeout"))
                :cause-chain ((:exception-class "java.sql.SQLNonTransientConnectionException"
                               :message "reason-71")
                              (:exception-class "java.net.ConnectException"
                               :message "root-71")))))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 71))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 `(:ok nil
                   :error "summary-71"
                   :diag ,diag))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
            (should nil))
        (clutch-db-error
         (should (string-match-p "summary-71" (cadr err)))
         (let* ((details (nth 2 err))
                (diag (plist-get details :diag))
                (context (plist-get diag :context)))
           (should (equal (plist-get details :backend) 'jdbc))
           (should (equal (plist-get details :summary) "summary-71"))
           (should (equal (plist-get diag :category) "connect"))
           (should (= (plist-get diag :request-id) 71))
           (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA"
                                   (plist-get context :generated-sql)))
           (should (equal (plist-get context :property-keys)
                          '("http_header_COOKIE" "socket_timeout")))
           (should (string-match-p "<redacted>"
                                   (plist-get context :redacted-url))))
         (should-not (string-match-p "cookie-secret-71"
                                     (prin1-to-string (nth 2 err)))))))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-carries-debug-payload ()
  "Structured JDBC details should preserve opt-in backend debug payloads."
  (let ((diag '(:category "connect"
                :op "connect"
                :request-id 72))
        (debug '(:thread "clutch-jdbc-request"
                 :request-context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>")
                 :stack-trace "java.sql.SQLNonTransientConnectionException: boom")))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 72))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 `(:ok nil
                   :error "summary-72"
                   :diag ,diag
                   :debug ,debug))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
            (should nil))
        (clutch-db-error
         (let ((details (nth 2 err)))
           (should (equal (plist-get details :summary) "summary-72"))
           (should (equal (plist-get (plist-get details :debug) :thread)
                          "clutch-jdbc-request"))
           (should (string-match-p "SQLNonTransientConnectionException"
                                   (plist-get (plist-get details :debug) :stack-trace)))))))))

(ert-deftest clutch-db-test-jdbc-rpc-on-conn-stores-structured-diagnostics-on-connection ()
  "Connection-scoped JDBC errors should stay on that connection."
  (let ((clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
        (conn (make-clutch-jdbc-conn :process 'proc :conn-id 11 :params '(:driver oracle)))
        (diag '(:category "query"
                :op "execute"
                :request-id 88
                :conn-id 11
                :raw-message "reason-88"
                :context (:generated-sql "SELECT * FROM hidden_table"))))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 88))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 `(:ok nil
                   :error "summary-88"
                   :diag ,diag))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc-on-conn conn "execute" '((conn-id . 11) (sql . "SELECT 1")))
            (should nil))
        (clutch-db-error
         (should (string-match-p "summary-88" (cadr err)))
         (let* ((details (clutch-db-error-details conn))
                (stored-diag (plist-get details :diag))
                (context (plist-get stored-diag :context)))
           (should (equal (plist-get details :summary) "summary-88"))
           (should (equal (plist-get stored-diag :op) "execute"))
           (should (= (plist-get stored-diag :conn-id) 11))
           (should (string-match-p "hidden_table"
                                   (plist-get context :generated-sql)))))))))

(ert-deftest clutch-db-test-jdbc-interrupt-cancel-success-returns-t ()
  "JDBC interrupt should return t only after a confirmed cancel acknowledgement."
  (let ((conn (make-clutch-jdbc-conn :conn-id 7
                                     :params '(:driver jdbc :rpc-timeout 12)))
        (clutch-jdbc--agent-process 'fake-proc)
        (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        (clutch-jdbc--response-queue '((:id 99 :ok t)))
        captured-op captured-params)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (op params)
                 (setq captured-op op
                       captured-params params)
                 99)))
      (should (clutch-db-interrupt-query conn))
      (should (equal captured-op "cancel"))
      (should (= (alist-get 'conn-id captured-params) 7))
      (should-not (gethash conn clutch-jdbc--busy-request-ids))
      (should (gethash 41 clutch-jdbc--ignored-response-ids)))))

(ert-deftest clutch-db-test-jdbc-interrupt-cancel-timeout-does-not-kill-agent ()
  "A slow cancel should degrade to nil without killing the shared JDBC agent."
  (let ((conn (make-clutch-jdbc-conn :conn-id 7
                                     :params '(:driver jdbc :rpc-timeout 12)))
        (clutch-jdbc-cancel-timeout-seconds 0.1)
        (clutch-jdbc--agent-process 'fake-proc)
        (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        (clutch-jdbc--response-queue nil)
        deleted-proc
        send-called)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (_op _params)
                 (setq send-called t)
                 99))
              ((symbol-function 'process-live-p)
               (lambda (_proc) t))
              ((symbol-function 'accept-process-output)
               (lambda (_proc _secs) nil))
              ((symbol-function 'delete-process)
               (lambda (proc)
                 (setq deleted-proc proc))))
      (should-not (clutch-db-interrupt-query conn))
      (should send-called)
      (should (eq clutch-jdbc--agent-process 'fake-proc))
      (should-not deleted-proc))))

(ert-deftest clutch-db-test-jdbc-interrupt-no-busy-request-returns-nil ()
  "JDBC interrupt should return nil and avoid RPC traffic when idle."
  (let ((conn (make-clutch-jdbc-conn :conn-id 7
                                     :params '(:driver jdbc :rpc-timeout 12)))
        (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        send-called)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (&rest _args)
                 (setq send-called t)
                 (error "Cancel should not be sent"))))
      (should-not (clutch-db-interrupt-query conn))
      (should-not send-called)
      (should (= (hash-table-count clutch-jdbc--ignored-response-ids) 0)))))

(ert-deftest clutch-db-test-jdbc-disconnect-timeout-does-not-kill-agent ()
  "A slow disconnect should clean local state without killing the shared agent."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 12)))
         (clutch-jdbc-disconnect-timeout-seconds 0.1)
         (clutch-jdbc--agent-process 'fake-proc)
         (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         (clutch-jdbc--response-queue nil)
         deleted-proc
         send-called)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (_op _params)
                 (setq send-called t)
                 99))
              ((symbol-function 'process-live-p)
               (lambda (_proc) t))
              ((symbol-function 'accept-process-output)
               (lambda (_proc _secs) nil))
              ((symbol-function 'delete-process)
               (lambda (proc)
                 (setq deleted-proc proc))))
      (clutch-db-disconnect conn)
      (should send-called)
      (should (eq clutch-jdbc--agent-process 'fake-proc))
      (should-not deleted-proc)
      (should-not (gethash conn clutch-jdbc--busy-request-ids))
      (should-not (gethash conn clutch-jdbc--error-details-by-conn))
      (should-not (gethash 7 clutch-jdbc--connections-by-id)))))

(ert-deftest clutch-db-test-jdbc-clear-error-details-forgets-conn-cache ()
  "Clearing JDBC diagnostics should remove the connection-scoped cache entry."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 12)))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
    (clutch-db-clear-error-details conn)
    (should-not (gethash conn clutch-jdbc--error-details-by-conn))))

(ert-deftest clutch-db-test-jdbc-disconnect-skips-rpc-when-agent-dead ()
  "Disconnect should skip the RPC when the shared JDBC agent is already dead."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 12)))
         (clutch-jdbc--agent-process nil)
         (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         send-called)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (&rest _args)
                 (setq send-called t)
                 (error "Disconnect RPC should not be sent"))))
      (clutch-db-disconnect conn)
      (should-not send-called)
      (should-not (gethash conn clutch-jdbc--busy-request-ids))
      (should-not (gethash conn clutch-jdbc--error-details-by-conn))
      (should-not (gethash 7 clutch-jdbc--connections-by-id)))))

(ert-deftest clutch-db-test-jdbc-agent-filter-surfaces-invalid-json-lines ()
  "Malformed agent output should surface as a protocol error."
  (let ((buf (generate-new-buffer " *clutch-jdbc-filter-test*"))
        (clutch-jdbc--response-queue nil))
    (unwind-protect
        (cl-letf (((symbol-function 'process-buffer) (lambda (_proc) buf))
                  ((symbol-function 'clutch-jdbc--dispatch-async-response)
                   (lambda (_parsed) nil)))
          (clutch-jdbc--agent-filter 'fake-proc
                                     "{\"id\":1,\"ok\":true}\nnot-json\n")
          (should (equal (car clutch-jdbc--response-queue)
                         '(:id 1 :ok t)))
          (should (plist-get (cadr clutch-jdbc--response-queue)
                             :protocol-error))
          (should-error (clutch-jdbc--recv-response 2 10.0)
                        :type 'clutch-db-error)
          (should-not clutch-jdbc--response-queue))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest clutch-db-test-jdbc-agent-filter-drops-ignored-response-ids ()
  "Interrupted JDBC request responses should be dropped instead of queued."
  (let ((buf (generate-new-buffer " *clutch-jdbc-filter-test*"))
        (clutch-jdbc--response-queue nil)
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql)))
    (puthash 41 t clutch-jdbc--ignored-response-ids)
    (unwind-protect
        (cl-letf (((symbol-function 'process-buffer) (lambda (_proc) buf))
                  ((symbol-function 'clutch-jdbc--dispatch-async-response)
                   (lambda (_parsed) nil)))
          (clutch-jdbc--agent-filter 'fake-proc
                                     "{\"id\":41,\"ok\":false,\"error\":\"Query cancelled\"}\n{\"id\":42,\"ok\":true}\n")
          (should-not (gethash 41 clutch-jdbc--ignored-response-ids))
          (should (equal clutch-jdbc--response-queue
                         '((:id 42 :ok t)))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))
;;; clutch-db-test.el ends here
