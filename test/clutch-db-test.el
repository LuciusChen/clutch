;;; clutch-db-test.el --- ERT tests for database backends -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
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
(require 'clutch-db-sqlite)
(require 'clutch-mongodb)
(require 'clutch-redis)
(require 'redis)
(require 'mongodb)

(eval-when-compile
  (require 'pg))

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
(defvar auth-sources)
(defvar auth-source-do-cache)
(defvar clutch-db-test--live-name-counter 0
  "Counter used to generate isolated live database object names.")
(declare-function clutch-db-mysql--type-category "clutch-db-mysql" (type character-set))
(declare-function clutch-db-mysql--convert-columns "clutch-db-mysql" (columns))
(declare-function clutch-db-mysql-connect "clutch-db-mysql" (params))
(declare-function clutch-db-pg--ctid-identity "clutch-db-pg" (conn table))
(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))
(declare-function clutch-db-pg--column-details-row
                  "clutch-db-pg" (row pk-cols fks))
(declare-function clutch-db-pg--convert-columns "clutch-db-pg" (columns &optional conn))
(declare-function clutch-db-pg--wrap-result "clutch-db-pg" (result))
(declare-function clutch-db-pg--rewrite-param-sql "clutch-db-pg" (sql))
(declare-function clutch-db-pg--typed-arguments "clutch-db-pg" (params))
(declare-function clutch-db-pg-connect "clutch-db-pg" (params))
(declare-function clutch-db-sqlite-connect "clutch-db-sqlite" (params))
(declare-function clutch--manual-backend-choices "clutch-connection" ())
(declare-function clutch--backend-display-name-from-params "clutch-connection" (params))
(declare-function clutch--build-conn "clutch-connection" (params))
(declare-function clutch--format-value "clutch-ui" (value))
(declare-function auth-source-forget-all-cached "auth-source" ())
(declare-function clutch-db-value-to-literal "clutch-backend"
                  (conn param &optional fallback-format-fn))
(declare-function make-mysql-conn "mysql" (&rest args))
(declare-function make-mysql-result "mysql" (&rest args))
(declare-function mysql-current-database "mysql" (conn))
(declare-function mysql-connection-id "mysql" (conn))
(declare-function make-pgcon "pg" (&rest args))
(declare-function make-pgresult "pg" (&rest args))
(declare-function pgcon-connect-plist "pg" (conn))
(declare-function pgcon-dbname "pg" (conn))
(declare-function pgcon-typname-by-oid "pg" (conn))
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
(defconst clutch-db-test--pg-oid-int4-array 1007)
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

(defmacro clutch-db-test--with-temp-dir (var prefix &rest body)
  "Bind VAR to a temporary directory named with PREFIX while running BODY."
  (declare (indent 2))
  `(let ((,var (make-temp-file ,prefix t)))
     (unwind-protect
         (progn ,@body)
       (delete-directory ,var t))))

(defun clutch-db-test--write-authinfo-profile (path profile fields)
  "Write a temporary authinfo PATH for PROFILE with alternating FIELDS."
  (with-temp-file path
    (insert "machine " profile)
    (cl-loop for (key value) on fields by #'cddr
             do (insert " " (format "%s" key) " " (format "%s" value)))
    (insert "\n")))

(defmacro clutch-db-test--with-authinfo-profile (profile fields &rest body)
  "Bind auth-source to a temporary authinfo PROFILE with FIELDS."
  (declare (indent 2))
  `(let ((authinfo-file (make-temp-file "clutch-authinfo-profile-")))
     (unwind-protect
         (let ((auth-sources (list authinfo-file))
               (auth-source-do-cache nil))
           (clutch-db-test--write-authinfo-profile
            authinfo-file ,profile ,fields)
           (auth-source-forget-all-cached)
           ,@body)
       (auth-source-forget-all-cached)
       (ignore-errors
         (delete-file authinfo-file)))))

(defmacro clutch-db-test--with-authinfo-profile-conn
    (conn-var profile-prefix fields params &rest body)
  "Bind CONN-VAR to a connection built from an authinfo profile.
PROFILE-PREFIX names the temporary profile, FIELDS are written to
authinfo, and PARAMS are explicit connection parameters."
  (declare (indent 4))
  (let ((profile-var (make-symbol "profile")))
    `(let ((,profile-var (format "clutch-live/%s"
                                  (make-temp-name ,profile-prefix))))
       (clutch-db-test--with-authinfo-profile
           ,profile-var ,fields
         (let ((,conn-var (clutch--build-conn
                           (append ,params
                                   (list :profile-entry ,profile-var)))))
           (unwind-protect
               (progn ,@body)
             (clutch-db-disconnect ,conn-var)))))))

(defmacro clutch-db-test--with-jdbc-temp-dir (var prefix &rest body)
  "Bind VAR and `clutch-jdbc-agent-dir' to a temporary directory."
  (declare (indent 2))
  `(clutch-db-test--with-temp-dir ,var ,prefix
     (let ((clutch-jdbc-agent-dir ,var))
       ,@body)))

(defmacro clutch-db-test--with-temp-sqlite (conn-var prefix &rest body)
  "Bind CONN-VAR to a temporary SQLite database named with PREFIX."
  (declare (indent 2))
  `(let* ((db-file (make-temp-file ,prefix nil ".db"))
          (,conn-var (clutch-db-sqlite-connect (list :database db-file))))
     (unwind-protect
         (progn ,@body)
       (clutch-db-disconnect ,conn-var)
       (ignore-errors (delete-file db-file)))))

;;;; Unit tests — connection parameter normalization

(ert-deftest clutch-db-test-normalize-connect-params-tls-options ()
  "Backend TLS options should normalize to adapter-native connection params."
  (dolist (case '((mysql-disabled mysql
                   (:host "127.0.0.1" :tls nil :ssl-mode off)
                   ((:clutch-tls-mode . disable) (:ssl-mode . disabled))
                   (:tls) nil)
                  (mysql-conflict mysql
                   (:host "127.0.0.1" :tls t :ssl-mode disabled)
                   nil nil clutch-db-error)
                  (pg-require pg
                   (:host "127.0.0.1" :tls t)
                   ((:clutch-tls-mode . require) (:sslmode . require))
                   (:tls) nil)
                  (pg-prefer pg
                   (:host "127.0.0.1" :sslmode "prefer")
                   ((:clutch-tls-mode . prefer) (:sslmode . prefer))
                   (:tls) nil)
                  (pg-unsupported pg
                   (:host "127.0.0.1" :sslmode verify-ca)
                   nil nil clutch-db-error)))
    (pcase-let ((`(,label ,backend ,input ,expected ,absent ,error-type) case))
      (ert-info ((format "case: %s" label))
        (if error-type
            (should-error (clutch-db--normalize-connect-params backend input)
                          :type error-type)
          (let ((params (clutch-db--normalize-connect-params backend input)))
            (dolist (pair expected)
              (should (eq (plist-get params (car pair)) (cdr pair))))
            (dolist (key absent)
              (should-not (plist-member params key)))))))))

(ert-deftest clutch-db-test-pg-param-rewrite-skips-quoted-identifiers ()
  "PostgreSQL parameter rewriting should ignore quoted identifier text."
  (require 'clutch-db-pg)
  (should (equal (clutch-db-pg--rewrite-param-sql
                  "SELECT \"?\", ?")
                 "SELECT \"?\", $1")))

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

(ert-deftest clutch-db-test-redis-query-mapping-contract ()
  :tags '(:smoke)
  "Redis query results should map command responses to result grids."
  (let ((conn (make-clutch-redis-conn :client 'redis-client)))
    (cl-letf (((symbol-function 'redis-command)
               (lambda (client command &rest args)
                 (should (eq client 'redis-client))
                 (pcase (cons command args)
                   (`("HGETALL" "user:1")
                    (mapcar (lambda (text)
                              (encode-coding-string text 'utf-8 t))
                            '("name" "Ada" "tier" "pro")))
                   (`("HRANDFIELD" "user:1" "2" "WITHVALUES")
                    (mapcar (lambda (text)
                              (encode-coding-string text 'utf-8 t))
                            '("name" "Ada" "tier" "pro")))
                   (`("ZRANGE" "leaders" "0" "-1")
                    (mapcar (lambda (text)
                              (encode-coding-string text 'utf-8 t))
                            '("ada" "grace")))
                   (`("ZRANGE" "leaders" "0" "-1" "WITHSCORES")
                    (mapcar (lambda (text)
                              (encode-coding-string text 'utf-8 t))
                            '("ada" "10" "grace" "8")))
                   (_ (ert-fail "Unexpected Redis command"))))))
      (dolist (case '(("HGETALL user:1"
                       ("field" "value")
                       (("name" "Ada") ("tier" "pro")))
                      ("ZRANGE leaders 0 -1"
                       ("index" "value")
                       ((0 "ada") (1 "grace")))
                      ("HRANDFIELD user:1 2 WITHVALUES"
                       ("field" "value")
                       (("name" "Ada") ("tier" "pro")))
                      ("ZRANGE leaders 0 -1 WITHSCORES"
                       ("member" "score")
                       (("ada" "10") ("grace" "8")))))
        (pcase-let ((`(,query ,expected-columns ,expected-rows) case))
          (ert-info ((format "query: %s" query))
            (let ((result (clutch-db-query conn query)))
              (should (equal (clutch-db-result-column-names
                              (clutch-db-result-columns result))
                             expected-columns))
              (should (equal (clutch-db-result-rows result)
                             expected-rows)))))))))

(ert-deftest clutch-db-test-redis-empty-scan-preserves-continuation-cursor ()
  "An empty nonterminal SCAN batch should still expose its next cursor."
  (let ((conn (make-clutch-redis-conn :client 'redis-client)))
    (cl-letf (((symbol-function 'redis-command)
               (lambda (&rest _args) '("7" nil))))
      (let ((result (clutch-db-query conn "SSCAN user:1 0 COUNT 25")))
        (should (equal (clutch-db-result-column-names
                        (clutch-db-result-columns result))
                       '("cursor" "value")))
        (should (equal (clutch-db-result-rows result)
                       '(("7" nil))))))))

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

(ert-deftest clutch-db-test-redis-browseable-snapshot-scans-once ()
  "Redis object discovery should not duplicate its initial bounded SCAN."
  (let ((conn (make-clutch-redis-conn :client (make-redis-conn)))
        calls)
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command cursor &rest args)
                 (push (list command cursor args) calls)
                 '("0" ("alpha")))))
      (should
       (equal (clutch-db-browseable-object-entries conn)
              '((:name "alpha" :schema "0" :type "KEY"))))
      (should (equal calls '(("SCAN" "0" ("COUNT" 1000))))))))

(ert-deftest clutch-db-test-redis-scan-keys-is-bounded-and-deduplicated ()
  "Redis discovery should stop at its cap and ignore repeated SCAN keys."
  (let ((conn (make-clutch-redis-conn :client 'redis-client))
        (clutch-redis-key-discovery-limit 2)
        calls)
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client _command cursor &rest _args)
                 (push cursor calls)
                 (pcase cursor
                   ("0" (list "7" '("alpha" "alpha")))
                   ("7" (list "9" '("beta" "gamma")))
                   (_ (ert-fail "Discovery continued past its configured cap")))))
              ((symbol-function 'message) #'ignore))
      (should (equal (clutch-db-list-tables conn) '("alpha" "beta")))
      (should (equal (nreverse calls) '("0" "7"))))))

(ert-deftest clutch-db-test-redis-scan-keys-bounds-empty-batches ()
  "Sparse prefix discovery should stop at its SCAN round-trip budget."
  (let ((conn (make-clutch-redis-conn :client 'redis-client))
        (clutch-redis--key-discovery-max-scan-batches 2)
        calls
        notice)
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command cursor &rest _args)
                 (pcase command
                   ("SCAN"
                    (push cursor calls)
                    (pcase cursor
                      ("0" '("7" nil))
                      ("7" '("9" nil))
                      (_ (ert-fail "Discovery exceeded its SCAN budget"))))
                   (_ (ert-fail "Unexpected Redis command")))))
              ((symbol-function 'message)
               (lambda (format-string &rest args)
                 (setq notice (apply #'format format-string args)))))
      (should-not (clutch-db-search-table-entries conn "rare:"))
      (should (equal (nreverse calls) '("0" "7")))
      (should (string-match-p "stopped after 2 SCAN batches" notice)))))

(ert-deftest clutch-db-test-redis-find-exact-key-without-scan ()
  "Exact Redis key lookup should not depend on bounded SCAN coverage."
  (let ((conn (make-clutch-redis-conn :client (make-redis-conn)))
        calls)
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command &rest args)
                 (push (cons command args) calls)
                 (pcase command
                   ("EXISTS" 1)
                   (_ (ert-fail "Exact lookup unexpectedly scanned keys"))))))
      (should
       (equal (clutch-db-find-table-entry conn "remote:key")
              '(:name "remote:key" :schema "0" :type "KEY")))
      (should (equal calls '(("EXISTS" "remote:key")))))))

(ert-deftest clutch-db-test-redis-find-missing-key-uses-one-command ()
  "Missing exact Redis keys should return nil after one EXISTS command."
  (let ((conn (make-clutch-redis-conn :client (make-redis-conn)))
        calls)
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command &rest args)
                 (push (cons command args) calls)
                 0)))
      (should-not (clutch-db-find-table-entry conn "missing:key"))
      (should (equal calls '(("EXISTS" "missing:key")))))))

(ert-deftest clutch-db-test-redis-prefix-search-keeps-sibling-keys ()
  "Redis prefix search should not collapse to an existing exact key."
  (let ((conn (make-clutch-redis-conn :client (make-redis-conn)))
        calls)
    (cl-letf (((symbol-function 'redis-command)
               (lambda (_client command &rest args)
                 (push (cons command args) calls)
                 (pcase command
                   ("SCAN" '("0" ("cache" "cache:1")))
                   (_ (ert-fail "Prefix search used a non-SCAN command"))))))
      (should
       (equal (clutch-db-search-table-entries conn "cache")
              '((:name "cache" :schema "0" :type "KEY")
                (:name "cache:1" :schema "0" :type "KEY"))))
      (should
       (equal calls '(("SCAN" "0" "MATCH" "cache*" "COUNT" 1000)))))))

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
  (let ((conn (make-clutch-redis-conn :client 'redis-client))
        (clutch-redis-browse-limit 25))
    (dolist (case '(("hash" . "HRANDFIELD \"user:1\" 25 WITHVALUES")
                    ("list" . "LRANGE \"user:1\" 0 24")
                    ("set" . "SRANDMEMBER \"user:1\" 25")
                    ("zset" . "ZRANGE \"user:1\" 0 24 WITHSCORES")
                    ("stream" . "XRANGE \"user:1\" - + COUNT 25")))
      (cl-letf (((symbol-function 'redis-command)
                 (lambda (_client command key)
                   (should (equal command "TYPE"))
                   (should (equal key "user:1"))
                   (encode-coding-string (car case) 'utf-8 t))))
        (should (equal (clutch-db-object-browse-query
                        conn '(:name "user:1" :type "KEY"))
                       (cdr case)))))))

(ert-deftest clutch-db-test-jdbc-fetch-all-contract ()
  "JDBC fetch-all should preserve rows and map RPC/query timeouts."
  (ert-info ("preserves batch order")
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
  (ert-info ("maps query timeout through fetch RPC")
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
        (should (= (alist-get 'query-timeout-seconds captured-params) 10)))))
  (ert-info ("rejects an invalid fetch-size before RPC")
    (let ((conn (make-clutch-jdbc-conn :params '(:rpc-timeout 9))))
      (cl-letf (((symbol-function 'clutch-jdbc--rpc-on-conn)
                 (lambda (&rest _args)
                   (ert-fail "Invalid fetch-size reached the agent"))))
        (dolist (clutch-jdbc-fetch-size '(0 10001 1.5))
          (should-error (clutch-jdbc--fetch-all conn 9)
                        :type 'user-error)
          (should-error (clutch-jdbc--execute-rpc
                         conn "execute" '((sql . "SELECT 1")))
                        :type 'user-error))))))

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

(ert-deftest clutch-db-test-jdbc-connect-timeout-contract ()
  "JDBC connect should keep explicit and default timeout phases separate."
  (ert-info ("explicit timeouts")
    (let ((clutch-jdbc-oracle-manual-commit t)
          (clutch-jdbc-validate-after-idle-seconds 123)
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
          (should (eq (alist-get 'auto-commit captured-params)
                      clutch-jdbc--json-false))
          (should (= (alist-get 'connect-timeout-seconds captured-params) 7))
          (should (= (alist-get 'network-timeout-seconds captured-params) 23))
          (should (= (alist-get 'validate-after-idle-seconds captured-params)
                     123))
          (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout) 41))
          (should (= (clutch-jdbc-conn-conn-id conn) 7))))))
  (ert-info ("default timeouts")
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
               '(:host "db" :port 1521 :database "svc"
                 :user "scott" :password "tiger")))
        (should (= captured-timeout 41))
        (should (equal (alist-get 'driver-class captured-params)
                       "oracle.jdbc.OracleDriver"))
        (should (= (alist-get 'connect-timeout-seconds captured-params) 10))
        (should (= (alist-get 'network-timeout-seconds captured-params) 30))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :connect-timeout)
                   10))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :read-idle-timeout)
                   30))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :query-timeout)
                   20))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout)
                   41)))))
  (ert-info ("invalid idle validation intervals")
    (dolist (value '(-1 1.5 "300"))
      (let ((clutch-jdbc-validate-after-idle-seconds value))
        (should-error
         (clutch-db-jdbc-connect
          'oracle
          '(:host "db" :port 1521 :database "svc"
            :user "scott" :password "tiger"))
         :type 'user-error)))))

(ert-deftest clutch-db-test-jdbc-connect-autocommit-contract ()
  "JDBC connect should send the expected default auto-commit flag."
  (dolist (case '((:label "sqlserver default"
                   :oracle-manual-commit t
                   :driver sqlserver
                   :params (:host "db" :port 1433 :database "app"
                            :user "sa" :password "secret")
                   :driver-class "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                   :auto-commit t)
                  (:label "oracle global override"
                   :oracle-manual-commit nil
                   :driver oracle
                   :params (:host "db" :port 1521 :database "svc"
                            :user "scott" :password "tiger")
                   :driver-class "oracle.jdbc.OracleDriver"
                   :auto-commit t)))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (let ((clutch-jdbc-oracle-manual-commit
             (plist-get case :oracle-manual-commit))
            captured-params)
        (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
                  ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
                  ((symbol-function 'clutch-jdbc--rpc)
                   (lambda (_op params &optional _timeout-seconds)
                     (setq captured-params params)
                     '(:conn-id 8))))
          (clutch-db-jdbc-connect
           (plist-get case :driver)
           (plist-get case :params))
          (should (equal (alist-get 'driver-class captured-params)
                         (plist-get case :driver-class)))
          (should (eq (alist-get 'auto-commit captured-params)
                      (plist-get case :auto-commit))))))))

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

(ert-deftest clutch-db-test-jdbc-query-timeout-contract ()
  "JDBC queries should send RPC timeout and the effective query timeout."
  (dolist (case '((:label "clamps past rpc margin"
                   :conn-id 4 :rpc-timeout 15 :query-timeout 16
                   :effective-query-timeout 10 :affected-rows 1
                   :sql "delete from t")
                  (:label "keeps timeout within margin"
                   :conn-id 5 :rpc-timeout 15 :query-timeout 8
                   :effective-query-timeout 8 :affected-rows 0
                   :sql "delete from t where 1=0")
                  (:label "clamps to rpc minus five"
                   :conn-id 6 :rpc-timeout 30 :query-timeout 30
                   :effective-query-timeout 25 :affected-rows 1
                   :sql "update t set x = 1")))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (let ((conn (make-clutch-jdbc-conn
                   :conn-id (plist-get case :conn-id)
                   :params (list :rpc-timeout (plist-get case :rpc-timeout)
                                 :query-timeout
                                 (plist-get case :query-timeout))))
            captured-op
            captured-params
            captured-timeout)
        (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
                  ((symbol-function 'clutch-jdbc--rpc-on-conn)
                   (lambda (_conn op params &optional timeout-seconds)
                     (setq captured-op op
                           captured-params params
                           captured-timeout timeout-seconds)
                     (list :type "dml"
                           :affected-rows
                           (plist-get case :affected-rows)))))
          (let ((result (clutch-db-query conn (plist-get case :sql))))
            (should (equal captured-op "execute"))
            (should (= captured-timeout (plist-get case :rpc-timeout)))
            (should (= (alist-get 'query-timeout-seconds captured-params)
                       (plist-get case :effective-query-timeout)))
            (should (= (clutch-db-result-affected-rows result)
                       (plist-get case :affected-rows)))))))))

(ert-deftest clutch-db-test-jdbc-manual-commit-p ()
  "JDBC manual-commit defaults should follow driver and global settings."
  (dolist (case '((oracle-default t (:driver oracle :user "scott") t)
                  (sqlserver-default t (:driver sqlserver :user "sa") nil)
                  (oracle-global-override nil (:driver oracle :user "scott") nil)))
    (pcase-let ((`(,label ,oracle-default ,params ,expected) case))
      (ert-info ((format "case: %s" label))
        (let ((clutch-jdbc-oracle-manual-commit oracle-default)
              (conn (make-clutch-jdbc-conn :params params)))
          (if expected
              (should (clutch-db-manual-commit-p conn))
            (should-not (clutch-db-manual-commit-p conn))))))))

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

(ert-deftest clutch-db-test-jdbc-transaction-rpcs ()
  "JDBC commit and rollback should send the expected transaction RPC."
  (dolist (case '((commit clutch-db-commit 17 12)
                  (rollback clutch-db-rollback 18 13)))
    (pcase-let ((`(,op ,fn ,conn-id ,timeout) case))
      (ert-info ((format "op: %s" op))
        (let ((conn (make-clutch-jdbc-conn
                     :conn-id conn-id
                     :params `(:driver oracle :rpc-timeout ,timeout)))
              captured-op captured-params captured-timeout)
          (cl-letf (((symbol-function 'clutch-jdbc--rpc)
                     (lambda (rpc-op params &optional timeout-seconds)
                       (setq captured-op rpc-op
                             captured-params params
                             captured-timeout timeout-seconds)
                       `(:conn-id ,conn-id))))
            (funcall fn conn)
            (should (equal captured-op (symbol-name op)))
            (should (= (alist-get 'conn-id captured-params) conn-id))
            (should (= captured-timeout timeout))))))))

(ert-deftest clutch-db-test-jdbc-set-auto-commit-fires-rpc-and-updates-params ()
  "JDBC auto-commit changes should update the remote session and local params."
  (let ((conn (make-clutch-jdbc-conn :conn-id 19
                                     :params '(:driver oracle :rpc-timeout 12 :manual-commit t)))
        calls)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (push (list op params timeout-seconds) calls)
                 `(:conn-id ,(alist-get 'conn-id params)
                   :auto-commit ,(alist-get 'auto-commit params)))))
      (clutch-db-set-auto-commit conn t)
      (pcase-let ((`(,op ,params ,timeout) (pop calls)))
        (should (equal op "set-auto-commit"))
        (should (= (alist-get 'conn-id params) 19))
        (should (eq (alist-get 'auto-commit params) t))
        (should (= timeout 12)))
      (should-not (plist-get (clutch-jdbc-conn-params conn) :manual-commit))
      (clutch-db-set-auto-commit conn nil)
      (pcase-let ((`(,op ,params ,timeout) (pop calls)))
        (should (equal op "set-auto-commit"))
        (should (= (alist-get 'conn-id params) 19))
        (should (eq (alist-get 'auto-commit params) clutch-jdbc--json-false))
        (should (= timeout 12)))
      (should (plist-get (clutch-jdbc-conn-params conn) :manual-commit)))))

(ert-deftest clutch-db-test-jdbc-execute-params-uses-agent-binding ()
  "JDBC parameter execution should bind values in the agent."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 23
               :params '(:driver sqlserver :rpc-timeout 12)))
        captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (actual-conn op params &optional _timeout)
                 (should (eq actual-conn conn))
                 (setq captured-op op
                       captured-params params)
                 '(:type "dml" :affected-rows 2))))
      (let ((result (clutch-db-execute-params
                     conn
                     "UPDATE dbo.orders SET note = ? WHERE id = ?"
                     (list (clutch-db-typed-param "中文" "NVARCHAR")
                           (clutch-db-typed-param 17 "INTEGER")))))
        (should (equal captured-op "execute-params"))
        (should (equal (alist-get 'sql captured-params)
                       "UPDATE dbo.orders SET note = ? WHERE id = ?"))
        (should (equal (alist-get 'values captured-params)
                       '("中文" 17)))
        (should (= (clutch-db-result-affected-rows result) 2))))))

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

(ert-deftest clutch-db-test-default-row-identity-contract ()
  "Default row identity should report no support but surface metadata errors."
  (should-not (clutch-db-row-identity-candidates '(:backend unsupported)
                                                 "demo"))
  (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
             (lambda (_conn _table)
               (signal 'clutch-db-error '("metadata failed")))))
    (should-error (clutch-db-row-identity-candidates '(:backend broken)
                                                    "demo")
                  :type 'clutch-db-error)))

(ert-deftest clutch-db-test-row-identity-adapters-surface-metadata-errors ()
  "Backend row identity metadata lookup failures should not become nil."
  (require 'clutch-db-mysql)
  (require 'clutch-db-pg)
  (require 'clutch-db-sqlite)
  (require 'clutch-db-jdbc)
  (let ((mysql-conn (make-mysql-conn :host "localhost" :database "test"))
        (pg-conn (clutch-db-test--make-pgcon :database "test"))
        (sqlite-conn (make-clutch-db-sqlite-conn :database "test.db"
                                                 :handle 'sqlite-handle))
        (jdbc-conn (make-clutch-jdbc-conn :conn-id 4
                                          :params '(:driver oracle))))
    (dolist (case `((mysql-query mysql-error ,mysql-conn)
                    (pg-exec pg-error ,pg-conn)
                    (sqlite-select sqlite-error ,sqlite-conn)))
      (pcase-let ((`(,fn ,err ,conn) case))
        (cl-letf (((symbol-function fn)
                   (lambda (&rest _)
                     (signal err '("metadata failed")))))
          (should-error (clutch-db-row-identity-candidates conn "demo")
                        :type 'clutch-db-error))))
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (_conn _table) nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (context table)
                 (should (eq context jdbc-conn))
                 (should (equal table "DEMO"))
                 '((:name "DEMO" :type "TABLE"))))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn _table)
                 (signal 'clutch-db-error '("jdbc metadata failed")))))
      (should-error (clutch-db-row-identity-candidates jdbc-conn "DEMO")
                    :type 'clutch-db-error))))

(ert-deftest clutch-db-test-optional-metadata-adapters-surface-errors ()
  "Optional native metadata adapters should not turn failures into nil."
  (require 'clutch-db-mysql)
  (require 'clutch-db-pg)
  (require 'clutch-db-sqlite)
  (dolist (case `((mysql
                   ,(make-mysql-conn :database "app")
                   mysql-query mysql-error
                   (:name "idx_orders" :type "INDEX"
                    :target-table "orders"))
                  (postgres
                   ,(clutch-db-test--make-pgcon :database "app")
                   pg-exec pg-error
                   (:name "idx_orders" :type "INDEX"))))
    (pcase-let ((`(,label ,conn ,query-fn ,error-type ,entry) case))
      (ert-info ((format "backend: %s" label))
        (cl-letf (((symbol-function query-fn)
                   (lambda (&rest _args)
                     (signal error-type '("metadata failed")))))
          (dolist (call (list
                         (lambda () (clutch-db-table-comment conn "orders"))
                         (lambda () (clutch-db-foreign-keys conn "orders"))
                         (lambda () (clutch-db-referencing-objects conn "orders"))
                         (lambda () (clutch-db-column-details conn "orders"))
                         (lambda () (clutch-db-object-details conn entry))))
            (should-error (funcall call) :type 'clutch-db-error))))))
  (let ((conn (make-clutch-db-sqlite-conn
               :database "app.db" :handle 'sqlite-handle)))
    (cl-letf (((symbol-function 'sqlite-select)
               (lambda (&rest _args)
                 (signal 'sqlite-error '("metadata failed")))))
      (should-error (clutch-db-foreign-keys conn "orders")
                    :type 'clutch-db-error)
      (should-error (clutch-db-column-details conn "orders")
                    :type 'clutch-db-error))))

(ert-deftest clutch-db-test-pg-query-io-inhibits-throw-on-input ()
  "PostgreSQL query paths should finish responses inside `while-no-input'."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        observed)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (context _sql)
                 (push throw-on-input observed)
                 (make-pgresult :connection context :status "SELECT 1")))
              ((symbol-function 'pg-exec-prepared)
               (lambda (context _sql _arguments &rest _options)
                 (push throw-on-input observed)
                 (make-pgresult :connection context :status "SELECT 1"))))
      (let ((throw-on-input 'completion-input))
        (clutch-db-query conn "SELECT 1")
        (clutch-db-execute-params conn "SELECT ?" '(1))))
    (should (equal observed '(nil nil)))))

(ert-deftest clutch-db-test-pg-foreign-keys-reject-malformed-response ()
  "PostgreSQL foreign-key metadata should reject contaminated result rows."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        observed)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (context _sql)
                 (setq observed throw-on-input)
                 (make-pgresult :connection context
                                :tuples '((90000556 "attname"))))))
      (let ((throw-on-input 'completion-input))
        (should-error (clutch-db-foreign-keys conn "task")
                      :type 'clutch-db-error)))
    (should-not observed)))

(defun clutch-db-test--assert-row-identity-skips-lower-priority
    (conn table pk-columns unique-fn locator-fn locator-value)
  "Assert CONN row identity stops after PK-COLUMNS for TABLE.
UNIQUE-FN and LOCATOR-FN are lower-priority candidate builders that should not
be called.  LOCATOR-VALUE is the value LOCATOR-FN would return if called."
  (let (unique-called locator-called)
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (context actual-table)
                 (should (eq context conn))
                 (should (equal actual-table table))
                 pk-columns))
              ((symbol-function unique-fn)
               (lambda (&rest _args)
                 (setq unique-called t)
                 '((:kind unique-key :name "uq_code" :columns ("code")))))
              ((symbol-function locator-fn)
               (lambda (&rest _args)
                 (setq locator-called t)
                 locator-value)))
      (should (equal (clutch-db-row-identity-candidates conn table)
                     `((:kind primary-key
                        :name "PRIMARY"
                        :columns ,pk-columns))))
      (should-not unique-called)
      (should-not locator-called))))

(ert-deftest clutch-db-test-mysql-row-identity-uses-unique-scan-without-primary-key ()
  "MySQL row identity should still use unique indexes when no primary key exists."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "localhost" :database "test")))
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (_context _table) nil))
              ((symbol-function 'clutch-db-mysql--unique-not-null-identities)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "demo"))
                 '((:kind unique-key :name "uq_code" :columns ("code"))))))
      (should (equal (clutch-db-row-identity-candidates conn "demo")
                     '((:kind unique-key :name "uq_code" :columns ("code"))))))))

(ert-deftest clutch-db-test-mysql-unique-identities-use-show-keys ()
  "MySQL unique row identity lookup should use scoped SHOW KEYS metadata."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "localhost" :database "test"))
        observed-sql)
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (context sql)
                 (should (eq context conn))
                 (setq observed-sql sql)
                 (make-mysql-result
                  :rows '(("demo" 0 "uq_pair" 2 "b" nil nil nil nil "" nil)
                          ("demo" 0 "uq_pair" 1 "a" nil nil nil nil "" nil)
                          ("demo" 0 "uq_nullable" 1 "maybe" nil nil nil nil "YES" nil))))))
      (should (equal (clutch-db-mysql--unique-not-null-identities conn "demo")
                     '((:kind unique-key :name "uq_pair" :columns ("a" "b")))))
      (should (string-prefix-p "SHOW KEYS FROM `demo`" observed-sql))
      (should-not (string-match-p "INFORMATION_SCHEMA" observed-sql)))))

(ert-deftest clutch-db-test-row-identity-skips-lower-priority-when-primary-key-exists ()
  "SQL row identity should not scan lower-priority candidates after PK."
  (require 'clutch-db-mysql)
  (require 'clutch-db-pg)
  (require 'clutch-db-sqlite)
  (require 'clutch-db-jdbc)
  (let ((conn (make-mysql-conn :host "localhost" :database "test"))
        unique-scan-called)
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "demo"))
                 '("id")))
              ((symbol-function 'clutch-db-mysql--unique-not-null-identities)
               (lambda (_context _table)
                 (setq unique-scan-called t)
                 '((:kind unique-key :name "uq_code" :columns ("code"))))))
      (should (equal (clutch-db-row-identity-candidates conn "demo")
                     '((:kind primary-key :name "PRIMARY" :columns ("id")))))
      (should-not unique-scan-called)))
  (dolist (case `((,(clutch-db-test--make-pgcon :database "test")
                   "demo" ("id")
                   clutch-db-pg--unique-not-null-identities
                   clutch-db-pg--ctid-identity
                   (:kind row-locator :name "ctid"))
                  (,(make-clutch-db-sqlite-conn :database "test.db")
                   "demo" ("id")
                   clutch-db-sqlite--unique-not-null-identities
                   clutch-db-sqlite--rowid-identity
                   (:kind row-locator :name "rowid"))
                  (,(make-clutch-jdbc-conn :conn-id 4
                                           :params '(:driver oracle))
                   "DEMO" ("ID")
                   clutch-jdbc--unique-not-null-identities
                   clutch-jdbc--rowid-identity
                   (:kind row-locator :name "ROWID"))))
    (pcase-let ((`(,conn ,table ,pk-columns ,unique-fn ,locator-fn
                   ,locator-value)
                 case))
      (cl-letf (((symbol-function 'clutch-db-search-table-entries)
                 (lambda (context actual-table)
                   (should (clutch-jdbc-conn-p context))
                   (should (equal actual-table table))
                   (list (list :name table :type "TABLE")))))
        (clutch-db-test--assert-row-identity-skips-lower-priority
         conn table pk-columns unique-fn locator-fn locator-value)))))

(ert-deftest clutch-db-test-sqlite-rowid-identity-in-memory ()
  "SQLite rowid tables should expose `rowid' as a row locator."
  (skip-unless (sqlite-available-p))
  (clutch-db-test--with-temp-sqlite conn "clutch-sqlite-rowid-"
    (clutch-db-query conn "CREATE TABLE demo (name TEXT)")
    (let ((candidate (car (clutch-db-row-identity-candidates conn "demo"))))
      (should (equal (plist-get candidate :kind) 'row-locator))
      (should (equal (plist-get candidate :name) "rowid"))
      (should (equal (plist-get candidate :select-expressions) '("rowid")))
      (should (equal (plist-get candidate :where-sql) "rowid = ?")))))

(ert-deftest clutch-db-test-sqlite-foreign-keys-async ()
  "SQLite foreign-key metadata should remain available through async dispatch."
  (skip-unless (sqlite-available-p))
  (clutch-db-test--with-temp-sqlite conn "clutch-sqlite-fk-"
    (clutch-db-query conn "CREATE TABLE accounts (id INTEGER PRIMARY KEY)")
    (clutch-db-query
     conn
     "CREATE TABLE users (account_id INTEGER REFERENCES accounts(id))")
    (let (timer-fn result)
      (cl-letf (((symbol-function 'run-with-idle-timer)
                 (lambda (_secs _repeat fn &rest _args)
                   (setq timer-fn fn)
                   'fake-timer)))
        (should (eq (clutch-db-foreign-keys-async
                     conn "users" (lambda (value) (setq result value)))
                    'fake-timer))
        (funcall timer-fn)
        (should (equal result
                       '(("account_id" :ref-table "accounts"
                          :ref-column "id"))))))))

(ert-deftest clutch-db-test-sqlite-memory-does-not-create-file ()
  "SQLite :memory: profiles should open an in-memory database."
  (skip-unless (sqlite-available-p))
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
  :tags '(:smoke)
  "SQLite DML with RETURNING should produce columns and rows."
  (skip-unless (sqlite-available-p))
  (clutch-db-test--with-temp-sqlite conn "clutch-sqlite-returning-"
    (clutch-db-query conn "CREATE TABLE demo (id INTEGER PRIMARY KEY, name TEXT)")
    (let ((result (clutch-db-query
                   conn
                   "INSERT INTO demo (name) VALUES ('a') RETURNING id, name")))
      (should (equal (mapcar (lambda (col) (plist-get col :name))
                             (clutch-db-result-columns result))
                     '("id" "name")))
      (should (equal (clutch-db-result-rows result) '((1 "a"))))
      (should-not (clutch-db-result-affected-rows result)))))

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
               (lambda (_conn _table) nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn prefix)
                 (should (equal prefix "DEMO"))
                 '((:name "DEMO" :type "TABLE"
                    :schema "CLUTCH" :source-schema "CLUTCH")))))
      (should (equal (clutch-db-row-identity-candidates conn "DEMO")
                     '((:kind row-locator
                        :name "ROWID"
                        :select-expressions ("ROWID")
                        :where-sql "ROWID = ?")))))))

(ert-deftest clutch-db-test-jdbc-oracle-row-identity-skips-dictionary-view ()
  "Oracle JDBC should not offer ROWID for dictionary views."
  (let ((conn (make-clutch-jdbc-conn :conn-id 4
                                     :params '(:driver oracle
                                               :schema "CLUTCH"))))
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (_conn _table) nil))
              ((symbol-function 'clutch-jdbc--unique-not-null-identities)
               (lambda (_conn _table) nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn prefix)
                 (should (equal prefix "ALL_TABLES"))
                 '((:name "ALL_TABLES" :type "PUBLIC SYNONYM"
                    :schema "SYS" :source-schema "PUBLIC")))))
      (should-not (clutch-db-row-identity-candidates conn "ALL_TABLES")))))

(ert-deftest clutch-db-test-jdbc-refresh-schema-async-returns-table-names ()
  "Async JDBC schema refresh should return only table names to its callback."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params `(:driver oracle :user "scott"
                                               :rpc-timeout ,clutch-jdbc-rpc-timeout-seconds)))
        callback-result)
    (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_conn) t))
              ((symbol-function 'clutch-jdbc--rpc-async)
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
                   :columns ("name" "type" "schema" "source_schema" "comment")
                   :rows (("ORDERS" "SYNONYM" "DATA_OWNER" "APP")
                          ("USERS" "TABLE" "APP" "APP" "用户")
                          ("USER_TABLES" "PUBLIC SYNONYM" "SYS" "PUBLIC"))
                   :done t))))
      (let ((entries (clutch-db-list-table-entries conn)))
        (should (equal entries
                       '((:name "ORDERS" :type "SYNONYM" :schema "DATA_OWNER" :source-schema "APP")
                         (:name "USERS" :type "TABLE" :schema "APP" :source-schema "APP" :comment "用户")
                         (:name "USER_TABLES" :type "PUBLIC SYNONYM" :schema "SYS" :source-schema "PUBLIC"))))))))

(ert-deftest clutch-db-test-jdbc-table-comment-async-uses-table-search-remarks ()
  "JDBC table-comment async should use table remarks surfaced by search-tables."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver generic
                                               :schema "APP"
                                               :rpc-timeout 7)))
        captured-op captured-params captured-timeout callback-result)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (op params callback &optional _errback timeout _conn)
                 (setq captured-op op)
                 (setq captured-params params)
                 (setq captured-timeout timeout)
                 (funcall callback
                          '(:tables ((:name "ORDERS" :type "TABLE"
                                       :schema "APP" :source-schema "APP"
                                       :comment "订单")
                                      (:name "ORDER_LOG" :type "TABLE"
                                       :schema "APP" :source-schema "APP"
                                       :comment "日志"))))
                 t)))
      (should (clutch-db-table-comment-async
               conn "ORDERS" (lambda (comment)
                               (setq callback-result comment))))
      (should (equal captured-op "search-tables"))
      (should (equal (alist-get 'prefix captured-params) "ORDERS"))
      (should (= captured-timeout 7))
      (should (equal callback-result "订单")))))

(ert-deftest clutch-db-test-jdbc-table-comment-uses-table-search-remarks ()
  "JDBC table-comment should use remarks surfaced by search-tables."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver generic
                                               :schema "APP"))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (should (equal op "search-tables"))
                 (should (equal (alist-get 'prefix params) "ORDERS"))
                 '(:tables ((:name "ORDERS" :type "TABLE"
                              :schema "APP" :source-schema "APP"
                              :comment "订单"))))))
      (should (equal (clutch-db-table-comment conn "ORDERS") "订单")))))

(ert-deftest clutch-db-test-jdbc-table-comment-skips-special-metadata-paths ()
  "JDBC table comments should not probe special metadata paths."
  (dolist (driver '(oracle clickhouse))
    (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                       :params `(:driver ,driver
                                                 :schema "APP")))
          rpc-called)
      (cl-letf (((symbol-function 'clutch-jdbc--rpc)
                 (lambda (&rest _args)
                   (setq rpc-called t)))
                ((symbol-function 'clutch-jdbc--rpc-async)
                 (lambda (&rest _args)
                   (setq rpc-called t)
                   t)))
        (should-not (clutch-db-table-comment conn "ORDERS"))
        (should-not (clutch-db-table-comment-async
                     conn "ORDERS" #'ignore))
        (should-not rpc-called)))))

(ert-deftest clutch-db-test-jdbc-refresh-schema-async-scheduling ()
  "Async JDBC schema refresh should respect connection timeout and idle delay."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "scott"
                                               :rpc-timeout 7)))
        captured-timeout)
    (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_conn) t))
              ((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (_op _params _callback &optional _errback timeout-seconds _conn)
                 (setq captured-timeout timeout-seconds)
                 42)))
      (should (clutch-db-refresh-schema-async conn #'ignore))
      (should (= captured-timeout 7))))
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :process 'fake-proc
                                     :params '(:driver oracle :user "scott"
                                               :rpc-timeout 7)))
        sent
        timer-fn
        timer-delay)
    (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_conn) t))
              ((symbol-function 'run-with-idle-timer)
               (lambda (secs _repeat fn &rest _args)
                 (setq timer-delay secs
                       timer-fn fn)
                 'fake-idle-timer))
              ((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (&rest _args)
                 (setq sent t)
                 42)))
      (should (clutch-db-refresh-schema-async conn #'ignore nil 0.75))
      (should (equal timer-delay 0.75))
      (should-not sent)
      (funcall timer-fn)
      (should sent))))

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
                    (foreign-keys . (("account_id" :ref-table "accounts"
                                      :ref-column "id")))
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
                     (lambda (context table &optional _schema)
                       (should (eq context conn))
                       (should (equal table "users"))
                       "Users table"))
                    ((symbol-function 'clutch-db-foreign-keys)
                     (lambda (context table)
                       (should (eq context conn))
                       (should (equal table "users"))
                       '(("account_id" :ref-table "accounts"
                          :ref-column "id"))))
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
                ('foreign-keys
                 (clutch-db-foreign-keys-async
                  conn "users" (lambda (value) (setq callback-result value))))
                ('list-objects
                 (clutch-db-list-objects-async
                  conn 'indexes (lambda (value) (setq callback-result value)))))
              'fake-timer))
            (should (equal scheduled '(0 nil)))
            (should (equal callback-result expected))))))))

(ert-deftest clutch-db-test-native-schema-refresh-accepts-idle-delay ()
  "Native schema refresh should honor the caller's idle delay."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                               :user "root" :database "mysql"))
        callback-result
        idle-timer)
    (cl-letf (((symbol-function 'run-with-idle-timer)
               (lambda (secs repeat fn &rest args)
                 (setq idle-timer (list secs repeat))
                 (apply fn args)
                 'fake-idle-timer))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-list-tables)
               (lambda (context)
                 (should (eq context conn))
                 '("users" "orders"))))
      (should (eq (clutch-db-refresh-schema-async
                   conn
                   (lambda (value) (setq callback-result value))
                   nil
                   0.75)
                  'fake-idle-timer))
      (should (equal idle-timer '(0.75 nil)))
      (should (equal callback-result '("users" "orders"))))))

(ert-deftest clutch-db-test-idle-metadata-call-reschedule-contract ()
  "Idle metadata calls should reschedule while background work must defer."
  (dolist (mode '(busy foreground-active))
    (ert-info ((format "mode: %s" mode))
      (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                              :user "postgres" :database "test"))
            (clutch-db--foreground-connections (make-hash-table :test 'eq))
            timers
            callback-result
            (query-count 0)
            (busy-states (if (eq mode 'busy) '(t nil) '(nil))))
        (when (eq mode 'foreground-active)
          (puthash conn t clutch-db--foreground-connections))
        (cl-letf (((symbol-function 'run-with-idle-timer)
                   (lambda (_secs _repeat fn &rest args)
                     (push (lambda () (apply fn args)) timers)
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
                     (cl-incf query-count)
                     '("id" "name"))))
          (should (eq (clutch-db--schedule-idle-metadata-call
                       conn
                       (lambda (columns)
                         (setq callback-result columns))
                       nil
                       #'clutch-db-list-columns
                       nil
                       "users")
                      'fake-timer))
          (should (= (length timers) 1))
          (funcall (pop timers))
          (should-not callback-result)
          (should (= query-count 0))
          (should (= (length timers) 1))
          (when (eq mode 'foreground-active)
            (remhash conn clutch-db--foreground-connections))
          (funcall (pop timers))
          (should (equal callback-result '("id" "name")))
          (should (= query-count 1)))))))

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

(ert-deftest clutch-db-test-jdbc-collect-table-entries-contract ()
  "JDBC table entry collection should handle direct and cursor responses."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott"))))
    (dolist (case
             '((:label "direct"
                :response (:tables ((:name "USERS" :type "TABLE" :schema "SCOTT")
                                    (:name "ORDERS" :type "TABLE" :schema "SCOTT")))
                :expected ((:name "USERS" :type "TABLE" :schema "SCOTT")
                           (:name "ORDERS" :type "TABLE" :schema "SCOTT")))
               (:label "legacy cursor"
                :response (:rows (("USERS" "TABLE" "SCOTT"))
                          :cursor-id 42
                          :done nil)
                :fetch-rows (("PRODUCTS" "TABLE" "SCOTT"))
                :fetch-cursor 42
                :expected ((:name "USERS" :type "TABLE" :schema "SCOTT"
                            :source-schema "SCOTT")
                           (:name "PRODUCTS" :type "TABLE" :schema "SCOTT"
                            :source-schema "SCOTT")))))
      (ert-info ((plist-get case :label))
        (let (fetch-cursor-id)
          (cl-letf (((symbol-function 'clutch-jdbc--fetch-all)
                     (lambda (_conn cursor-id)
                       (setq fetch-cursor-id cursor-id)
                       (plist-get case :fetch-rows))))
            (should (equal (clutch-jdbc--collect-table-entries
                            conn
                            (plist-get case :response))
                           (plist-get case :expected)))
            (should (equal fetch-cursor-id
                           (plist-get case :fetch-cursor)))))))))

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

(ert-deftest clutch-db-test-mongodb-endpoint-metadata-uses-effective-client-state ()
  "URL connections should expose normalized endpoint metadata from mongodb.el."
  (let ((conn (clutch-db-test--make-mongodb-conn "app" 'mongodb-client)))
    (cl-letf (((symbol-function 'mongodb-connection-host)
               (lambda (_client) "db.internal"))
              ((symbol-function 'mongodb-connection-port)
               (lambda (_client) 27018))
              ((symbol-function 'mongodb-connection-username)
               (lambda (_client) "reporter")))
      (should (equal (clutch-db-host conn) "db.internal"))
      (should (= (clutch-db-port conn) 27018))
      (should (equal (clutch-db-user conn) "reporter")))))

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
                 (not (eq symbol 'mongodb-connection-host))))
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
        (should (string-match-p "mongodb-connection-host"
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

(ert-deftest clutch-db-test-mongodb-connect-native-contract ()
  "Native MongoDB connect should pass params and use the client database."
  (dolist (case
           '((saved-params
              (:host "127.0.0.1"
               :port 27017
               :database "app"
               :auth-database "admin"
               :user "reporter"
               :password "s p"
               :tls t
               :props (("retryWrites" . "true")))
              "app"
              t)
             (url-database
              (:url "mongodb+srv://cluster.example.net/analytics?retryWrites=true")
              "analytics"
              nil)))
    (pcase-let ((`(,label ,params ,database ,check-captured) case))
      (ert-info ((format "case: %s" label))
        (let (captured-params)
          (cl-letf (((symbol-function 'mongodb-connect)
                     (lambda (params)
                       (setq captured-params params)
                       (make-mongodb-conn :database database :closed nil))))
            (let ((conn (clutch-mongodb-connect params)))
              (should (eq (clutch-db-backend-key conn) 'mongodb))
              (should (equal (clutch-db-display-name conn) "MongoDB"))
              (should (equal (clutch-db-database conn) database))
              (should (mongodb-conn-p (clutch-mongodb-conn-client conn)))
              (when check-captured
                (should (equal captured-params params))))))))))

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
  :tags '(:smoke)
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

(ert-deftest clutch-db-test-mongodb-eval-translates-find-helper-contract ()
  "Native MongoDB eval should translate find helpers to `mongodb-find'."
  (dolist (case
           (list
            (list :label "basic"
                  :query "db.users.find({active: true}, {name: 1}).limit(20)"
                  :return '((("_id" . "a") ("name" . "Ann")))
                  :limit 20)
            (list :label "implicit bounded limit"
                  :query "db.users.find({active: true}, {name: 1})"
                  :limit 1000)
            (list :label "cursor options"
                  :query (concat
                          "db.users.find({active: true}, {name: 1})"
                          ".sort({createdAt: -1})"
                          ".maxTimeMS(250)"
                          ".allowDiskUse(true)"
                          ".skip(5).limit(10)")
                  :limit 10
                  :skip 5
                  :sort '(("createdAt" . -1))
                  :options '(("maxTimeMS" . 250)
                             ("allowDiskUse" . t)))))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (let (captured)
        (cl-letf (((symbol-function 'mongodb-find)
                   (lambda (client database collection filter projection limit
                                 skip sort &optional options)
                     (setq captured
                           (list client database collection filter projection
                                 limit skip sort options))
                     (plist-get case :return))))
          (let ((value (clutch-mongodb--eval
                        (clutch-db-test--make-mongodb-conn "app" 'client)
                        (plist-get case :query))))
            (should (equal value (plist-get case :return)))))
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
          (should (= limit (plist-get case :limit)))
          (should (equal skip (plist-get case :skip)))
          (if-let* ((sort-pairs (plist-get case :sort)))
              (progn
                (should (mongodb-document-p sort))
                (should (equal (mongodb-document-pairs sort) sort-pairs)))
            (should-not sort))
          (if-let* ((expected-options (plist-get case :options)))
              (progn
                (should-not (assoc "sort" options))
                (dolist (option expected-options)
                  (should (equal (cdr (assoc (car option) options))
                                 (cdr option)))))
            (should-not options)))))))

(ert-deftest clutch-db-test-mongodb-find-limit-is-bounded ()
  "Native MongoDB find should reject unbounded and oversized limits."
  (let ((clutch-mongodb-find-result-limit 50))
    (dolist (query '("db.users.find({}).limit(0)"
                     "db.users.find({}).limit(51)"))
      (should-error
       (clutch-mongodb--eval
        (clutch-db-test--make-mongodb-conn "app" 'client)
        query)
       :type 'clutch-db-error))))

(ert-deftest clutch-db-test-mongodb-eval-validation-contract ()
  "Native MongoDB helper parsing should reject unsupported or invalid inputs."
  (let ((conn (clutch-db-test--make-mongodb-conn "app" 'client)))
    (cl-letf (((symbol-function 'mongodb-find)
               (lambda (&rest _) 'unexpected-success))
              ((symbol-function 'mongodb-count-documents)
               (lambda (&rest _) 'unexpected-success))
              ((symbol-function 'mongodb-update)
               (lambda (&rest _) 'unexpected-success)))
      (dolist (query '("db.users.find('name')"
                       "db.users.find({}, {}, {})"
                       "db.users.findOne({}).limit(1)"
                       "db.users.aggregate([]).sort({_id: 1})"
                       "db.users.countDocuments('name')"
                       "db.users.updateOne({}, 'name')"
                       "db.users.deleteOne({}).limit(1)"
                       "db.users.find({}).allowDiskUse('yes')"
                       "db.users.find({}).explain({mode: 'executionStats'})"
                       "db.users.deleteOne()"
                       "db.users.deleteOne('name')"
                       "db.users.deleteMany({})"
                       "db.users.insertOne('name')"
                       "db.users.insertMany({name: 'Ann'})"
                       "db.users.insertMany([1])"
                       "db.users.find({_id: NumberLong(7)})"
                       "db.getSiblingDB('admin').runCommand({ping: 1})"
                       "db.users.find({name: /ann/i})"))
        (ert-info ((format "query: %s" query))
          (should-error (clutch-mongodb--eval conn query)
                        :type 'clutch-db-error))))))

(ert-deftest clutch-db-test-mongodb-helper-chains-are-method-specific ()
  "MongoDB parsing should reject chains that execution would ignore."
  (dolist (query '("db.users.findOne({}).limit(1)"
                   "db.users.aggregate([]).sort({_id: 1})"
                   "db.users.deleteOne({}).limit(1)"))
    (ert-info ((format "query: %s" query))
      (should-error (clutch-mongodb--parse-db-call query)
                    :type 'clutch-db-error))))

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
               "{allowDiskUse: false})"
               ".allowDiskUse(true)"
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
        (should (= (cdr (assoc "maxTimeMS" pairs)) 500))))))

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

(ert-deftest clutch-db-test-mongodb-eval-translates-count-and-distinct ()
  "Native MongoDB eval should translate common scalar read helpers."
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
                 '("a" "b"))))
      (clutch-mongodb--eval
       (clutch-db-test--make-mongodb-conn "app" 'client)
       (concat
        "db.users.countDocuments({active: true}, {limit: 10});"
        "db.users.distinct('name', {active: true})")))
    (setq calls (nreverse calls))
    (should (= (length calls) 2))
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
    (pcase-let ((`(distinct ,_client ,_database ,_collection ,field
                            ,filter ,options)
                 (nth 1 calls)))
      (should (equal field "name"))
      (should (mongodb-document-p filter))
      (should (equal (mongodb-document-pairs filter)
                     '(("active" . t))))
      (should-not options))))

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
        "db.users.replaceOne({_id: 'b'}, {_id: 'b', name: 'Bob'})")))
    (setq calls (nreverse calls))
    (should (= (length calls) 2))
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
    (pcase-let ((`(,_client ,_database ,_collection ,filter ,replacement
                   ,multi ,options)
                 (cadr calls)))
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
  "Native MongoDB MQL parsing should preserve common query constructors."
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
        "_id: ObjectId('507f1f77bcf86cd799439011'), "
        "createdAt: ISODate('2024-01-02T03:04:05.678Z')"
        "})")))
    (should (mongodb-document-p captured-filter))
    (let* ((pairs (mongodb-document-pairs captured-filter))
           (id (cdr (assoc "_id" pairs)))
           (created-at (cdr (assoc "createdAt" pairs))))
      (should (mongodb-object-id-p id))
      (should (equal (mongodb-object-id-hex id)
                     "507f1f77bcf86cd799439011"))
      (should (mongodb-datetime-p created-at))
      (should (= (mongodb-datetime-millis created-at) 1704164645678)))))

(ert-deftest clutch-db-test-mongodb-eval-translates-db-helper-contract ()
  "Native MongoDB runCommand should call the public command API."
  (let (command-call)
    (cl-letf (((symbol-function 'mongodb-command)
               (lambda (client database command &optional _timeout)
                 (setq command-call (list client database command))
                 '(("ok" . 1)))))
      (let ((conn (clutch-db-test--make-mongodb-conn "app" 'client)))
        (should (equal (clutch-mongodb--eval conn "db.runCommand({ping: 1})")
                       '(("ok" . 1))))))
    (pcase-let ((`(,client ,database ,command) command-call))
      (should (eq client 'client))
      (should (equal database "app"))
      (should (mongodb-document-p command))
      (should (equal (mongodb-document-pairs command)
                     '(("ping" . 1)))))))

(ert-deftest clutch-db-test-mongodb-metadata-uses-public-client-api ()
  "Native MongoDB metadata should map databases and collections into Clutch objects."
  (let ((clutch-mongodb-schema-sample-size 2)
        (documents '((("_id" . (("$oid" . "64f")))
                      ("name" . "Ann")
                      ("score" . 10)
                      ("profile" . (("age" . 30))))
                     (("_id" . (("$oid" . "650")))
                      ("score" . "high")
                      ("active" . :false))))
        sample-collections)
    (cl-letf (((symbol-function 'mongodb-list-collections)
               (lambda (_client _database) '("users" "orders")))
              ((symbol-function 'mongodb-list-databases)
               (lambda (_client) '("app" "analytics")))
              ((symbol-function 'mongodb-find)
               (lambda (_client _database collection _filter _projection limit
                                 &rest _)
                 (push (list collection limit) sample-collections)
                 documents))
              ((symbol-function 'mongodb-list-collection-docs)
               (lambda (_client _database &optional _filter _options)
                 '((("name" . "users")
                    ("type" . "collection"))))))
      (let ((conn (clutch-db-test--make-mongodb-conn)))
        (should (equal (clutch-db-list-schemas conn)
                       '("analytics" "app")))
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
        (should (equal sample-collections
                       '(("users" 2) ("users" 2))))
        (should (string-match-p "\"users\""
                                (clutch-db-object-definition
                                 conn '(:name "users" :type "COLLECTION"))))))))

(ert-deftest clutch-db-test-mongodb-metadata-translates-client-errors ()
  "Native MongoDB metadata should translate client errors at the adapter boundary."
  (let ((conn (clutch-db-test--make-mongodb-conn)))
    (dolist (case '((list-tables . mongodb-list-collections)
                    (sample-documents . mongodb-find)
                    (collection-info . mongodb-list-collection-docs)))
      (pcase-let ((`(,operation . ,api) case))
        (ert-info ((symbol-name operation))
          (cl-letf (((symbol-function api)
                     (lambda (&rest _args)
                       (signal 'mongodb-error
                               (list (format "%s failed" operation))))))
            (let ((err
                   (should-error
                    (pcase operation
                      ('list-tables (clutch-db-list-tables conn))
                      ('sample-documents
                       (clutch-mongodb--sample-documents conn "users"))
                      ('collection-info
                       (clutch-mongodb--collection-info conn "users")))
                    :type 'clutch-db-error)))
              (should (equal (cadr err)
                             (format "%s failed" operation))))))))))

(ert-deftest clutch-db-test-mongodb-public-object-paths-translate-client-errors ()
  "Every public MongoDB object path should expose `clutch-db-error'."
  (let ((conn (clutch-db-test--make-mongodb-conn)))
    (dolist (case '((list-objects mongodb-list-indexes)
                    (object-details mongodb-list-indexes)
                    (object-action mongodb-aggregate)
                    (collection-profile mongodb-list-indexes)))
      (pcase-let ((`(,operation ,failing-api) case))
        (ert-info ((symbol-name operation))
          (cl-letf (((symbol-function 'mongodb-list-collections)
                     (lambda (&rest _args) '("users")))
                    ((symbol-function 'mongodb-find)
                     (lambda (&rest _args) nil))
                    ((symbol-function failing-api)
                     (lambda (&rest _args)
                       (signal 'mongodb-error
                               (list (format "%s failed" operation))))))
            (let ((err
                   (should-error
                    (pcase operation
                      ('list-objects
                       (clutch-db-list-objects conn 'indexes))
                      ('object-details
                       (clutch-db-object-details
                        conn '(:name "users_1" :type "INDEX"
                               :target-table "users")))
                      ('object-action
                       (clutch-db-object-action-metadata
                        conn '(:name "users" :type "COLLECTION")
                        'show-stats))
                      ('collection-profile
                       (clutch-db-collection-profile conn "users")))
                    :type 'clutch-db-error)))
              (should (equal (cadr err)
                             (format "%s failed" operation))))))))))

(ert-deftest clutch-db-test-mongodb-schema-sample-size-rejects-invalid ()
  "Native MongoDB metadata should reject invalid schema sample sizes."
  (let ((clutch-mongodb-schema-sample-size 0))
    (should-error (clutch-mongodb--schema-sample-limit)
                  :type 'clutch-db-error)))

(ert-deftest clutch-db-test-mongodb-schema-sampling-rejects-non-documents ()
  "Native MongoDB metadata should reject non-document sampling results."
  (cl-letf (((symbol-function 'mongodb-find)
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
        captured-filter)
    (cl-letf (((symbol-function 'mongodb-list-collection-docs)
               (lambda (_client _database &optional filter _options)
                 (setq captured-filter filter)
                 metadata)))
      (should
       (equal
        (clutch-db-object-action-metadata
         (clutch-db-test--make-mongodb-conn)
         '(:name "users" :type "COLLECTION")
         'show-validation)
        "{\"collection\":\"users\",\"configured\":true,\"validationAction\":\"warn\",\"validationLevel\":\"moderate\",\"validator\":{\"$jsonSchema\":{\"required\":[\"status\"]}}}"))
      (should (mongodb-document-p captured-filter))
      (should (equal (mongodb-document-elements captured-filter)
                     '(("name" . "users")))))))

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
    (cl-letf (((symbol-function 'mongodb-find)
               (lambda (_client database collection _filter _projection limit
                                 &rest _)
                 (should (equal database "app"))
                 (should (equal collection "users"))
                 (should (= limit 3))
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
  "Native MongoDB index insight should combine definitions and usage."
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
    (should (equal (clutch-db-update-namespace-params
                    conn '(:backend mongodb :database "app"))
                   '(:backend mongodb :database "analytics")))
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
  "ClickHouse column metadata should map foreign keys without adding scope."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default")))
        calls)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (push (cons op params) calls)
                 (pcase op
                   ("get-primary-keys" '(:primary-keys ("id")))
                   ("get-foreign-keys"
                    '(:foreign-keys ((:fk-column "user_id"
                                      :pk-table "users" :pk-column "id"))))
                   ("get-columns" `(:columns ((:name "id" :type "UInt64"
                                               :nullable ,clutch-jdbc--json-false)
                                              (:name "user_id" :type "UInt64"
                                               :nullable t))))
                   (_ (ert-fail (format "unexpected op: %s" op)))))))
      (should
       (equal (clutch-db-column-details conn "events")
              '((:name "id" :type "UInt64" :nullable nil
                 :primary-key t :foreign-key nil :comment nil)
                (:name "user_id" :type "UInt64" :nullable t
                 :primary-key nil
                 :foreign-key (:ref-table "users" :ref-column "id")
                 :comment nil))))
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
  (let ((buf (generate-new-buffer " *clutch-jdbc-async-timeout-test*"))
        (clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        (clutch-jdbc--response-queue nil)
        (clutch-jdbc-rpc-timeout-seconds 1)
        timeout-message
        timer-fn)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
                  ((symbol-function 'clutch-jdbc--send)
                   (lambda (_op _params) 77))
                  ((symbol-function 'process-buffer) (lambda (_proc) buf))
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
          (should-not (gethash 77 clutch-jdbc--async-callbacks))
          (should (gethash 77 clutch-jdbc--ignored-response-ids))
          (clutch-jdbc--agent-filter 'fake-proc "{\"id\":77,\"ok\":true}\n")
          (should-not clutch-jdbc--response-queue)
          (should-not (gethash 77 clutch-jdbc--ignored-response-ids)))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

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

(ert-deftest clutch-db-test-jdbc-dispatch-async-response-skips-disconnected-conn ()
  "A deferred JDBC callback should not run after its connection disconnects."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7 :params '(:driver jdbc)))
         (clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         timer-fn
         callback-called)
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (puthash 77 (list :callback (lambda (_result)
                                  (setq callback-called t))
                      :conn conn
                      :op 'get-tables)
             clutch-jdbc--async-callbacks)
    (cl-letf (((symbol-function 'run-at-time)
               (lambda (_secs _repeat fn &rest _args)
                 (setq timer-fn fn)
                 'fake-timer))
              ((symbol-function 'clutch-jdbc--agent-live-p) (lambda () nil)))
      (should (clutch-jdbc--dispatch-async-response
               '(:id 77 :ok t :result (:tables nil))))
      (clutch-db-disconnect conn)
      (funcall timer-fn)
      (should-not callback-called))))

(ert-deftest clutch-db-test-jdbc-validate-agent-jar-rejects-mismatch ()
  "JDBC agent startup should reject a jar with the wrong checksum."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-agent-"
    (let ((jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
          (clutch-jdbc-agent-version "0.1.2")
          (clutch-jdbc-agent-sha256 "deadbeef"))
      (with-temp-file jar
        (insert "not a release jar"))
      (should-error (clutch-jdbc--validate-agent-jar jar) :type 'user-error))))

(ert-deftest clutch-db-test-jdbc-ensure-agent-cleans-stale-jars ()
  "Ensuring the agent should keep only the current versioned jar."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-agent-"
    (let* ((clutch-jdbc-agent-version "0.1.2")
           (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
           (stale-a (expand-file-name "clutch-jdbc-agent-0.1.0.jar" tmpdir))
           (stale-b (expand-file-name "clutch-jdbc-agent-0.1.1.jar" tmpdir))
           (clutch-jdbc-agent-sha256 nil))
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
      (should-not (file-exists-p stale-b)))))

(ert-deftest clutch-db-test-jdbc-ensure-agent-allows-custom-jar-when-checksum-disabled ()
  "Checksum verification can be disabled for a local custom jar."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-agent-"
    (let ((clutch-jdbc-agent-version "0.1.2")
          (clutch-jdbc-agent-sha256 nil)
          (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir)))
      (with-temp-file jar
        (insert "custom build"))
      (should (clutch-jdbc--agent-jar-valid-p jar))
      (should (progn (clutch-jdbc--validate-agent-jar jar) t)))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-requires-agent-command ()
  "Missing JDBC agent should instruct the user to run the explicit install command."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-agent-"
    (let ((clutch-jdbc-agent-version "0.1.2"))
      (let ((err (should-error (clutch-jdbc--setup-prerequisites 'oracle)
                               :type 'user-error)))
        (should (string-match-p
                 "Run M-x clutch-jdbc-ensure-agent"
                 (cadr err)))))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-points-to-install-driver ()
  "Missing Maven drivers should point users at `clutch-jdbc-install-driver'."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-agent-"
    (let ((jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
          (clutch-jdbc-agent-version "0.1.2"))
      (make-directory (expand-file-name "drivers" tmpdir) t)
      (with-temp-file jar (insert "placeholder"))
      (cl-letf (((symbol-function 'clutch-jdbc--agent-jar-valid-p)
                 (lambda (_jar) t)))
        (let ((err (should-error
                    (clutch-jdbc--setup-prerequisites 'sqlserver)
                    :type 'user-error)))
          (should (string-match-p
                   "Run M-x clutch-jdbc-install-driver RET sqlserver"
                   (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-reports-manual-driver-url ()
  "Missing manual JDBC drivers should report the download URL and destination."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-agent-"
    (let* ((jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
           (clutch-jdbc-agent-version "0.1.2")
           (dest (expand-file-name "db2jcc4.jar" (expand-file-name "drivers" tmpdir))))
      (make-directory (expand-file-name "drivers" tmpdir) t)
      (with-temp-file jar (insert "placeholder"))
      (cl-letf (((symbol-function 'clutch-jdbc--agent-jar-valid-p)
                 (lambda (_jar) t)))
        (let ((err (should-error (clutch-jdbc--setup-prerequisites 'db2)
                                 :type 'user-error)))
          (should (string-match-p "requires manual download" (cadr err)))
          (should (string-match-p "ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads"
                                  (cadr err)))
          (should (string-match-p (regexp-quote dest) (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-install-driver-installs-oracle-i18n-companion ()
  "Installing Oracle JDBC should also install the orai18n companion jar."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-driver-"
    (let (downloaded)
      (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                 (lambda (_coords dest)
                   (push (file-name-nondirectory dest) downloaded)
                   (with-temp-file dest (insert "jar")))))
        (clutch-jdbc-install-driver 'oracle)
        (should (member "ojdbc8.jar" downloaded))
        (should (member "orai18n.jar" downloaded))))))

(ert-deftest clutch-db-test-jdbc-install-driver-removes-conflicting-oracle-jar ()
  "Installing an Oracle driver should remove the conflicting Oracle jar."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-driver-"
    (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
               (lambda (_coords dest)
                 (with-temp-file dest (insert "jar")))))
      (make-directory (expand-file-name "drivers" tmpdir) t)
      (with-temp-file (expand-file-name "drivers/ojdbc11.jar" tmpdir)
        (insert "jar"))
      (clutch-jdbc-install-driver 'oracle)
      (should (file-exists-p (expand-file-name "drivers/ojdbc8.jar" tmpdir)))
      (should-not (file-exists-p (expand-file-name "drivers/ojdbc11.jar" tmpdir))))))

(ert-deftest clutch-db-test-jdbc-install-driver-uses-maven-artifacts ()
  "Maven-backed JDBC drivers should request and install expected artifacts."
  (dolist (case '((sqlserver "drivers/mssql-jdbc.jar"
                    "com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre11" equal)
                  (duckdb "drivers/duckdb_jdbc.jar"
                   "org.duckdb:duckdb_jdbc:1.5.3.0" equal)
                  (mongodb "drivers/mongodb-jdbc.jar"
                   "org.mongodb:mongodb-jdbc:3.0.6:all" equal)
                  (redshift "drivers/redshift-jdbc42.jar"
                   "com.amazon.redshift:redshift-jdbc42:" prefix)))
    (pcase-let ((`(,driver ,jar-path ,expected-coords ,match) case))
      (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-driver-"
        (let (requested-coords)
          (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                     (lambda (coords dest)
                       (setq requested-coords coords)
                       (make-directory (file-name-directory dest) t)
                       (with-temp-file dest (insert "jar")))))
            (clutch-jdbc-install-driver driver)
            (pcase match
              ('equal (should (equal requested-coords expected-coords)))
              ('prefix (should (string-prefix-p expected-coords requested-coords))))
            (should (file-exists-p (expand-file-name jar-path tmpdir)))))))))

;;;; Unit tests — props normalization

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

(ert-deftest clutch-db-test-jdbc-normalize-row ()
  "JDBC row normalization should unwrap CLOB/BLOB previews and keep scalars."
  (dolist (case
           '((("text"
               (:__type "clob" :length 1000 :preview "hello clob")
               42)
              ("text" "hello clob" 42))
             (((:__type "clob" :length 0)) (nil))
             (((:__type "blob" :length 5 :text "hello")) ("hello"))
             ((1 "str" nil t) (1 "str" nil t))))
    (pcase-let ((`(,row ,expected) case))
      (should (equal (clutch-jdbc--normalize-row row) expected)))))

;;;; Unit tests — registered JDBC driver support

(ert-deftest clutch-db-test-jdbc-build-url-for-registered-drivers ()
  "Registered JDBC URL builders should apply default and explicit ports."
  (dolist (case '((redshift
                   (:host "cluster.us-east-1.redshift.amazonaws.com"
                    :database "mydb")
                   "jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/mydb")
                  (redshift
                   (:host "cluster.example.com" :port 5440
                    :database "analytics")
                   "jdbc:redshift://cluster.example.com:5440/analytics")
                  (clickhouse
                   (:host "ch.corp.com" :database "default")
                   "jdbc:clickhouse://ch.corp.com:8123/default")
                  (clickhouse
                   (:host "ch.corp.com" :port 8443 :database "analytics")
                   "jdbc:clickhouse://ch.corp.com:8443/analytics")))
    (pcase-let ((`(,driver ,params ,expected) case))
      (should (equal (clutch-jdbc--build-url driver params) expected)))))

(ert-deftest clutch-db-test-jdbc-oracle-keyword-probe-keeps-default-sql-product ()
  "Oracle identifier rendering should not change the global SQL dialect."
  (let ((default-product (default-value 'sql-product))
        (clutch-jdbc--oracle-display-keyword-cache
         (make-hash-table :test 'equal)))
    (should (clutch-jdbc--oracle-display-keyword-p "SELECT"))
    (should (eq (default-value 'sql-product) default-product))))

;;;; Unit tests — ClickHouse driver support

(ert-deftest clutch-db-test-jdbc-install-driver-installs-clickhouse ()
  "Installing ClickHouse JDBC should download the all-classifier artifact and companions."
  (clutch-db-test--with-jdbc-temp-dir tmpdir "clutch-jdbc-driver-"
    (let (all-coords)
      (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                 (lambda (coords dest)
                   (push coords all-coords)
                   (with-temp-file dest (insert "jar")))))
        (clutch-jdbc-install-driver 'clickhouse)
        (should (cl-some (lambda (c) (string-match-p ":all\\'" c)) all-coords))
        (should (file-exists-p (expand-file-name "drivers/clickhouse-jdbc.jar" tmpdir)))
        (should (file-exists-p (expand-file-name "drivers/slf4j-api.jar" tmpdir)))
        (should (file-exists-p (expand-file-name "drivers/slf4j-nop.jar" tmpdir)))))))

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

(ert-deftest clutch-db-test-jdbc-conn-schema ()
  "JDBC connection schema should follow driver defaults and explicit params."
  (dolist (case '((oracle-default (:driver oracle :user "app_user") "APP_USER")
                  (explicit-schema
                   (:driver oracle :user "app_user" :schema "REPORTING")
                   "REPORTING")
                  (non-oracle (:driver sqlserver :user "sa") nil)))
    (pcase-let ((`(,label ,params ,expected) case))
      (ert-info ((format "case: %s" label))
        (let ((conn (make-clutch-jdbc-conn :params params)))
          (should (equal (clutch-jdbc--conn-schema conn) expected)))))))

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

(ert-deftest clutch-db-test-jdbc-duckdb-switches-current-catalog-schema ()
  "DuckDB JDBC should switch a schema within the current catalog."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 8
               :params '(:driver jdbc
                         :url "jdbc:duckdb:/tmp/analytics.duckdb"
                         :rpc-timeout 9)))
        queries)
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn sql)
                 (push sql queries)
                 (cond
                  ((string-match-p "duckdb_schemas" sql)
                   (make-clutch-db-result
                    :rows '(("main") ("sales") ("odd.schema"))))
                  ((string-match-p "current_catalog" sql)
                   (make-clutch-db-result :rows '(("analytics" "main"))))
                  ((string-prefix-p "USE " sql)
                   (make-clutch-db-result :rows nil))
                  (t (ert-fail (format "Unexpected SQL: %s" sql)))))))
      (should
       (equal (clutch-db-list-schemas conn)
              '("main" "sales" "odd.schema")))
      (should (equal (clutch-db-current-schema conn) "main"))
      (should
       (equal (clutch-db-set-current-schema
               conn "odd.schema")
              "odd.schema"))
      (should (member "USE \"analytics\".\"odd.schema\"" queries))
      (should (equal (plist-get (clutch-jdbc-conn-params conn) :catalog)
                     "analytics"))
      (should (equal (plist-get (clutch-jdbc-conn-params conn) :schema)
                     "odd.schema"))
      (should-not
       (clutch-db-list-schemas
        (make-clutch-jdbc-conn
         :params '(:driver jdbc :url "jdbc:duckdb:")))))))

(ert-deftest clutch-db-test-jdbc-set-current-schema-contract ()
  "JDBC schema switching should support Oracle sessions and reject generic JDBC."
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
                     "ANALYTICS"))))
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
      (should (equal (mysql-current-database conn) "analytics"))
      (should (equal (clutch-db-update-namespace-params
                      conn '(:backend mysql :database "sales"))
                     '(:backend mysql :database "analytics"))))))

;;;; Unit tests — clutch-jdbc--apply-timeout-defaults

(ert-deftest clutch-db-test-jdbc-apply-timeout-defaults ()
  "Missing JDBC timeouts should be filled without overwriting explicit values."
  (let ((clutch-connect-timeout-seconds 10)
        (clutch-read-idle-timeout-seconds 20)
        (clutch-query-timeout-seconds 30)
        (clutch-jdbc-rpc-timeout-seconds 40))
    (dolist (case '((nil
                     (:connect-timeout 10 :read-idle-timeout 20
                      :query-timeout 30 :rpc-timeout 40))
                    ((:connect-timeout 99 :query-timeout 88)
                     (:connect-timeout 99 :read-idle-timeout 20
                      :query-timeout 88 :rpc-timeout 40))))
      (pcase-let* ((`(,params ,expected) case)
                   (result (clutch-jdbc--apply-timeout-defaults params)))
        (dolist (key '(:connect-timeout :read-idle-timeout
                       :query-timeout :rpc-timeout))
          (should (= (plist-get result key)
                     (plist-get expected key))))))))

;;;; Unit tests — backend registry

(ert-deftest clutch-db-test-backend-features ()
  :tags '(:smoke)
  "Test that backend features are correctly registered."
  (let ((mysql-features (alist-get 'mysql clutch-backend--registry))
        (pg-features (alist-get 'pg clutch-backend--registry))
        (sqlite-features (clutch-backend-feature 'sqlite))
        (jdbc-features (clutch-backend-feature 'jdbc))
        (clickhouse-features (clutch-backend-feature 'clickhouse))
        (mongodb-features (clutch-backend-feature 'mongodb))
        (redis-features (clutch-backend-feature 'redis)))
    ;; MySQL backend
    (should mysql-features)
    (should (eq (plist-get mysql-features :require) 'clutch-db-mysql))
    (should (eq (plist-get mysql-features :connect-fn) 'clutch-db-mysql-connect))
    (should (eq (plist-get mysql-features :normalize-fn)
                'clutch-db-mysql--normalize-connect-params))
    (should (equal (plist-get mysql-features :display-name) "MySQL"))
    (should (= (plist-get mysql-features :default-port) 3306))
    (should (eq (plist-get mysql-features :support-level) 'core))
    (should (eq (plist-get mysql-features :data-model) 'relational))
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
    (should (eq (plist-get pg-features :data-model) 'relational))
    (should (eq (plist-get pg-features :sql-product) 'postgres))
    (should (eq (plist-get sqlite-features :data-model) 'relational))
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
    (should (eq (plist-get mongodb-features :data-model) 'document))
    (should (clutch-backend-manual-choice-p 'mongodb))
    (should-not (clutch-backend-feature 'sql-interface-mongodb))
    (should (eq (plist-get redis-features :require) 'clutch-redis))
    (should (equal (plist-get redis-features :display-name) "Redis"))
    (should (= (plist-get redis-features :default-port) 6379))
    (should (eq (plist-get redis-features :support-level) 'basic))
    (should (eq (plist-get redis-features :data-model) 'key-value))
    (should (eq (clutch-backend-query-mode 'redis) #'clutch-redis-mode))))

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
      (should-not (clutch-db-sql-surface-p
                   'mongo-conn '(:backend mongodb :surface sql)))
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
  "Manual connect choices should include registered concrete backend metadata."
  (require 'clutch)
  (let ((choices (clutch--manual-backend-choices)))
    (should (member 'mysql choices))
    (should (member 'clickhouse choices))
    (should (member 'mongodb choices))
    (should-not (member 'sql-interface-mongodb choices))
    (should-not (member 'jdbc choices))
    (dolist (backend choices)
      (should (clutch-backend-display-name backend))
      (unless (eq backend 'sqlite)
        (should (numberp (clutch-backend-default-port backend)))))))

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
    (should (clutch-backend-jdbc-transport-p 'jdbc '(:backend jdbc)))
    (should (clutch-backend-jdbc-transport-p
             'clickhouse '(:backend clickhouse)))
    (should (eq (clutch-db-backend-key
                 (make-clutch-jdbc-conn :params '(:driver clickhouse)))
                'clickhouse))
    (should (equal (clutch--backend-display-name-from-params
                    '(:backend jdbc :display-name "KingbaseES"))
                   "KingbaseES"))
    (should (equal (clutch-db-display-name conn) "KingbaseES"))))

(ert-deftest clutch-db-test-build-conn-routes-generic-jdbc-through-jdbc-backend ()
  :tags '(:smoke)
  "The generic JDBC backend should pass :url through to `clutch-db-connect'."
  (require 'clutch)
  (let (captured-backend captured-params)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) "secret"))
              ((symbol-function 'clutch-db-connect)
               (lambda (backend params)
                 (setq captured-backend backend
                       captured-params params)
                 'fake-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
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
  (dolist (case `((,mysql-type-long 33 numeric)
                  (,mysql-type-float 33 numeric)
                  (,mysql-type-double 33 numeric)
                  (,mysql-type-decimal 33 numeric)
                  (,mysql-type-longlong 33 numeric)
                  (,mysql-type-date 33 date)
                  (,mysql-type-time 33 time)
                  (,mysql-type-datetime 33 datetime)
                  (,mysql-type-timestamp 33 datetime)
                  (,mysql-type-blob 63 blob)
                  (,mysql-type-blob 33 text)
                  (,mysql-type-json 63 json)
                  (9999 0 text)))
    (pcase-let ((`(,type ,charset ,expected) case))
      (should (eq (clutch-db-mysql--type-category type charset)
                  expected)))))

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
  (dolist (case `((,clutch-db-test--pg-oid-int4 numeric)
                  (,clutch-db-test--pg-oid-int8 numeric)
                  (,clutch-db-test--pg-oid-float8 numeric)
                  (,clutch-db-test--pg-oid-numeric numeric)
                  (,clutch-db-test--pg-oid-date date)
                  (,clutch-db-test--pg-oid-time time)
                  (,clutch-db-test--pg-oid-timestamp datetime)
                  (,clutch-db-test--pg-oid-timestamptz datetime)
                  (,clutch-db-test--pg-oid-bytea blob)
                  (,clutch-db-test--pg-oid-json json)
                  (,clutch-db-test--pg-oid-jsonb json)
                  (,clutch-db-pg--oid-bool text)
                  (999999 text)))
    (pcase-let ((`(,oid ,expected) case))
      (should (eq (clutch-db-pg--type-category oid) expected)))))

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

(ert-deftest clutch-db-test-pg-convert-columns-preserves-backend-type ()
  "PostgreSQL column conversion should keep backend type metadata."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test")))
    (puthash clutch-db-test--pg-oid-int4-array "_int4"
             (pgcon-typname-by-oid conn))
    (let ((converted
           (clutch-db-pg--convert-columns
            `(("precision" ,clutch-db-test--pg-oid-int4-array -1))
            conn)))
      (should (equal (plist-get (car converted) :name) "precision"))
      (should (equal (plist-get (car converted) :backend-type) "_int4")))))

(ert-deftest clutch-db-test-pg-array-params-use-postgresql-literals ()
  "PostgreSQL array params should render and execute as array literals."
  (require 'clutch-db-pg)
  (let* ((conn (clutch-db-test--make-pgcon :database "test"))
         (param (clutch-db-typed-param "[0,1,2]" "_int4")))
    (should (string-match-p
             (regexp-quote "{0,1,2}")
             (clutch-db-value-to-literal conn param)))
    (should (equal (clutch-db-pg--typed-arguments (list param))
                   '(("{0,1,2}" . nil))))
    (should (equal (clutch-db-pg--typed-arguments
                    (list (clutch-db-typed-param [0 1 2] "_int4")))
                   '(("{0,1,2}" . nil))))
    (let ((err (should-error
                (clutch-db-pg--typed-arguments
                 (list (clutch-db-typed-param "[0:2]={1,2,3}" "_int4")))
                :type 'user-error)))
      (should (string-match-p "explicit dimension bounds" (cadr err))))))

(ert-deftest clutch-db-test-pg-column-details-keep-array-display-type ()
  "PostgreSQL column details should keep display type while saving backend type."
  (require 'clutch-db-pg)
  (let ((detail (clutch-db-pg--column-details-row
                 '("precision" "ARRAY" "_int4" "YES" nil nil nil nil "NO" nil)
                 nil nil)))
    (should (equal (plist-get detail :type) "ARRAY"))
    (should (equal (plist-get detail :backend-type) "_int4"))))

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
  :tags '(:smoke)
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
  :tags '(:smoke)
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

(ert-deftest clutch-db-test-jdbc-build-paged-sql-dialects ()
  "JDBC pagination should follow dialect-specific offset syntax."
  (dolist (case
           (append
            '(("sqlserver explicit offset" (:driver sqlserver) "SELECT * FROM t"
               9 10 70 ("OFFSET 70 ROWS" "FETCH NEXT 10 ROWS ONLY") nil)
              ("oracle explicit offset" (:driver oracle) "SELECT * FROM t"
               9 10 70 ("ROWNUM <= 80" "rn > 70") nil)
              ("sqlserver preserves ORDER BY" (:driver sqlserver)
               "SELECT * FROM t ORDER BY created_at DESC" 0 10 nil
               ("ORDER BY created_at DESC"
                "OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY\\'")
               ("ORDER BY created_at DESC.*ORDER BY"))
              ("duckdb limit offset" (:url "jdbc:duckdb:/tmp/test.duckdb")
               "SELECT * FROM t ORDER BY created_at DESC" 0 10 nil
               ("ORDER BY created_at DESC LIMIT 10 OFFSET 0\\'")
               ("FETCH NEXT")))
            (mapcar
             (lambda (params)
               (list (format "limit offset %S" params) params
                     "SELECT * FROM t ORDER BY created_at DESC" 0 10 nil
                     '("ORDER BY created_at DESC LIMIT 10 OFFSET 0\\'")
                     '("FETCH NEXT")))
             '((:driver redshift)
               (:driver clickhouse)
               (:url "jdbc:redshift://cluster.example.com:5439/analytics")
               (:url "jdbc:clickhouse://ch.example.com:8123/default")))))
    (pcase-let ((`(,label ,params ,input-sql ,page ,page-size ,offset
                          ,matches ,not-matches)
                 case))
      (ert-info ((format "case: %s" label))
        (let* ((conn (make-clutch-jdbc-conn :params params))
             (sql (clutch-db-build-paged-sql
                   conn
                   input-sql
                   page
                   page-size
                   nil
                   offset)))
        (dolist (pattern matches)
          (should (string-match-p pattern sql)))
        (dolist (pattern not-matches)
          (should-not (string-match-p pattern sql))))))))

(ert-deftest clutch-db-test-jdbc-source-table-scope-follows-dialect-rules ()
  "JDBC source tables should preserve scope and dialect identifier rules."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle))))
    (should (equal (clutch-db--source-table-name conn "users") "USERS"))
    (should (equal (clutch-db--source-table-name conn "APP.users") "USERS"))
    (should (equal (clutch-db--source-table-name conn "\"MixedCase\"")
                   "MixedCase"))
    (should (equal (clutch-db--source-table-name conn "APP.\"MixedCase\"")
                   "MixedCase"))
    (should (equal (clutch-db--source-table-schema conn "app.users")
                   "APP"))
    (should (equal (clutch-db--source-table-schema
                    conn "\"App\".\"MixedCase\"")
                   "App")))
  (let ((conn (make-clutch-jdbc-conn :params '(:driver sqlserver))))
    (should (equal (clutch-db--source-table-name
                    conn "analytics.dbo.orders")
                   "orders"))
    (should (equal (clutch-db--source-table-schema
                    conn "analytics.dbo.orders")
                   "dbo"))
    (should (equal (clutch-db--source-table-catalog
                    conn "analytics.dbo.orders")
                   "analytics"))
    (should (equal (clutch-db--source-table-catalog
                    conn "[Sales DB].[reporting].[Order.Items]")
                   "Sales DB"))
    (should (equal (clutch-db--source-table-schema
                    conn "[Sales DB].[reporting].[Order.Items]")
                   "reporting"))
    (should (equal (clutch-db--source-table-name
                    conn "[Sales DB].[reporting].[Order.Items]")
                   "Order.Items"))))

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

(ert-deftest clutch-db-test-mysql-list-table-entries-carries-comments ()
  "MySQL table discovery should return comments with table entries."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :database "app"))
        captured-sql)
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 (make-mysql-result
                  :rows '(("orders" "BASE TABLE" "订单")
                          ("audit_log" "BASE TABLE" nil))))))
      (should
       (equal (clutch-db-list-table-entries conn)
              '((:name "orders" :type "TABLE" :schema "app"
                 :source-schema "app" :comment "订单")
                (:name "audit_log" :type "TABLE" :schema "app"
                 :source-schema "app" :comment nil))))
      (should (string-match-p "TABLE_COMMENT" captured-sql)))))

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

(ert-deftest clutch-db-test-adapter-disconnect-errors-surface ()
  "Native and JDBC adapter disconnect failures should remain visible."
  (require 'clutch-db-mysql)
  (require 'clutch-db-pg)
  (cl-letf (((symbol-function 'clutch-jdbc--agent-live-p) (lambda () t)))
    (dolist (case `((,(make-mysql-conn :host "localhost") mysql-disconnect mysql-error)
                    (,(clutch-db-test--make-pgcon :host "localhost") pg-disconnect pg-error)
                    (,(make-clutch-db-sqlite-conn :handle 'sqlite-handle) sqlite-close sqlite-error)
                    (,(make-clutch-jdbc-conn :conn-id 7) clutch-jdbc--send wrong-type-argument)))
      (pcase-let ((`(,conn ,disconnect-function ,error-type) case))
        (cl-letf (((symbol-function disconnect-function)
                   (lambda (&rest _args) (signal error-type '("close failed")))))
          (should-error (clutch-db-disconnect conn) :type error-type))))))

(ert-deftest clutch-db-test-pg-primary-keys-use-compatible-int2vector-ordering ()
  "PostgreSQL primary keys should preserve compatible int2vector order."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        sql)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn actual-sql)
                 (setq sql actual-sql)
                 (make-pgresult :tuples '(("tenant_id") ("id"))))))
      (should (equal (clutch-db-primary-key-columns conn "orders")
                     '("tenant_id" "id")))
      (should (string-search
               "generate_subscripts(i.indkey::smallint[], 1)" sql))
      (should (string-search "a.attnum = pk.key_array[pk.ord]" sql))
      (should (string-search "ORDER BY pk.ord" sql))
      (should-not (string-search "array_position" sql)))))

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

(ert-deftest clutch-db-test-pg-list-table-entries-carries-comments ()
  "PostgreSQL table discovery should return comments with table entries."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "app"))
        captured-sql)
    (cl-letf (((symbol-function 'clutch-db-current-schema)
               (lambda (_conn) "public"))
              ((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 (make-pgresult
                  :tuples '(("orders" "TABLE" "订单")
                            ("audit_log" "TABLE" nil))))))
      (should
       (equal (clutch-db-list-table-entries conn)
              '((:name "orders" :type "TABLE" :schema "public"
                 :source-schema "public" :comment "订单")
                (:name "audit_log" :type "TABLE" :schema "public"
                 :source-schema "public" :comment nil))))
      (should (string-match-p "obj_description" captured-sql)))))

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

(ert-deftest clutch-db-test-mysql-connect-wire-params ()
  "MySQL connect should pass only adapter-native params to `mysql-connect'."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((base '(:host "127.0.0.1"
                :port 3306
                :database "mysql"
                :user "root"
                :password "secret")))
    (dolist (case '((tls-disabled (:tls nil) nil nil
                     ((:ssl-mode . disabled))
                     (:clutch-tls-mode :tls))
                    (timeout-defaults nil 12 34
                     ((:connect-timeout . 12) (:read-idle-timeout . 34))
                     nil)
                    (pass-entry (:pass-entry "prod-db") nil nil
                     ((:password . "secret"))
                     (:pass-entry))))
      (pcase-let ((`(,label ,extra ,connect-timeout ,read-timeout
                            ,expected ,absent)
                   case))
        (ert-info ((format "case: %s" label))
          (let ((clutch-connect-timeout-seconds
                 (or connect-timeout clutch-connect-timeout-seconds))
                (clutch-read-idle-timeout-seconds
                 (or read-timeout clutch-read-idle-timeout-seconds))
                captured-args)
            (cl-letf (((symbol-function 'mysql-connect)
                       (lambda (&rest args)
                         (setq captured-args args)
                         (make-mysql-conn :host "127.0.0.1" :port 3306
                                          :user "root"
                                          :database "mysql-wire"))))
              (clutch-db-mysql-connect (append base extra))
              (dolist (pair expected)
                (should (equal (plist-get captured-args (car pair))
                               (cdr pair))))
              (dolist (key absent)
                (should-not (plist-member captured-args key))))))))))

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

(ert-deftest clutch-db-test-mysql-query-timeout-interrupts-and-keeps-connection ()
  "MySQL query timeout should cancel the server query when recovery succeeds."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                               :user "root" :database "test"))
        interrupted
        disconnected
        message)
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (_conn _sql)
                 (signal 'mysql-timeout
                         '("Timed out waiting for 4 bytes"))))
              ((symbol-function 'clutch-db-interrupt-query)
               (lambda (mysql-conn)
                 (should (eq mysql-conn conn))
                 (setq interrupted t)
                 t))
              ((symbol-function 'mysql-disconnect)
               (lambda (_conn)
                 (setq disconnected t))))
      (condition-case err
          (clutch-db-query conn "SELECT SLEEP(60)")
        (clutch-db-error
         (setq message (error-message-string err))))
      (should interrupted)
      (should-not disconnected)
      (should (string-match-p "restored MySQL connection" message)))))

(ert-deftest clutch-db-test-mysql-query-timeout-disconnects-when-recovery-fails ()
  "MySQL query timeout should close the connection when cancel recovery fails."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                               :user "root" :database "test"))
        interrupted
        disconnected
        message)
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (_conn _sql)
                 (signal 'mysql-timeout
                         '("Timed out waiting for 4 bytes"))))
              ((symbol-function 'clutch-db-interrupt-query)
               (lambda (mysql-conn)
                 (should (eq mysql-conn conn))
                 (setq interrupted t)
                 nil))
              ((symbol-function 'mysql-disconnect)
               (lambda (mysql-conn)
                 (should (eq mysql-conn conn))
                 (setq disconnected t))))
      (condition-case err
          (clutch-db-query conn "SELECT SLEEP(60)")
        (clutch-db-error
         (setq message (error-message-string err))))
      (should interrupted)
      (should disconnected)
      (should (string-match-p "timeout recovery failed" message)))))

(ert-deftest clutch-db-test-pg-interrupt-query-return-contract ()
  "PostgreSQL interrupt should return t for successful cancel and nil on pg errors."
  (require 'clutch-db-pg)
  (dolist (case '((success t)
                  (pg-error nil)))
    (pcase-let ((`(,label ,expected) case))
      (ert-info ((format "case: %s" label))
        (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432))
              called)
          (cl-letf (((symbol-function 'pg-cancel)
                     (lambda (pg-conn)
                       (setq called pg-conn)
                       (if expected
                           t
                         (signal 'pg-connection-error '("cancel failed"))))))
            (should (eq (clutch-db-interrupt-query conn) expected))
            (should (eq called conn))))))))

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

(ert-deftest clutch-db-test-mysql-live-authinfo-profile-connect ()
  :tags '(:db-live :mysql-live)
  "MySQL should connect through an authinfo `:profile-entry'."
  (if (not (clutch-db-test--mysql-live-configured-p))
      (ert-skip "Set clutch-db-test-mysql-password to enable MySQL live tests")
    (clutch-db-test--with-local-mysql-tls
      (clutch-db-test--with-authinfo-profile-conn
          conn "mysql-"
          (list "login" clutch-db-test-mysql-user
                "password" clutch-db-test-mysql-password
                "db-host" clutch-db-test-mysql-host
                "port" clutch-db-test-mysql-port
                "database" clutch-db-test-mysql-database)
          '(:backend mysql)
        (should (clutch-db-live-p conn))
        (should (equal (clutch-db-user conn)
                       clutch-db-test-mysql-user))
        (should (equal (clutch-db-database conn)
                       clutch-db-test-mysql-database))
        (clutch-db-test--assert-live-basic-query conn)))))

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

(ert-deftest clutch-db-test-mysql-live-timeout-recovers-connection ()
  :tags '(:db-live :mysql-live)
  "MySQL read timeout should resynchronize the session before reuse."
  (clutch-db-test--with-mysql conn
    (let ((clutch-db-mysql-cancel-timeout-seconds 2)
          (old-timeout (mysql-conn-read-idle-timeout conn))
          message)
      (unwind-protect
          (progn
            (clutch-db-mysql--set-read-idle-timeout conn 0.2)
            (condition-case err
                (clutch-db-query conn "SELECT SLEEP(5)")
              (clutch-db-error
               (setq message (error-message-string err))))
            (should (string-match-p "restored MySQL connection" message))
            (let ((result (clutch-db-query conn "SELECT 1 AS n")))
              (should (= (caar (clutch-db-result-rows result)) 1))))
        (clutch-db-mysql--set-read-idle-timeout conn old-timeout)))))

(ert-deftest clutch-db-test-mysql-live-schema ()
  :tags '(:db-live :mysql-live)
  "Test MySQL schema introspection and database switching."
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
      (should (string-match-p "CREATE\\( TABLE\\| .* VIEW\\)" ddl)))
    (let* ((original (clutch-db-current-schema conn))
           (schema (clutch-db-test--live-name "clutch_switch"))
           (quoted (clutch-db-escape-identifier conn schema)))
      (unwind-protect
          (progn
            (clutch-db-query conn (format "CREATE DATABASE %s" quoted))
            (should (member schema (clutch-db-list-schemas conn)))
            (clutch-db-set-current-schema conn schema)
            (should (equal (clutch-db-current-schema conn) schema))
            (clutch-db-query conn "CREATE TABLE namespace_probe (id INT)")
            (should (member "namespace_probe" (clutch-db-list-tables conn))))
        (ignore-errors (clutch-db-set-current-schema conn original))
        (ignore-errors
          (clutch-db-query conn (format "DROP DATABASE IF EXISTS %s" quoted)))))))

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

(ert-deftest clutch-db-test-pg-live-authinfo-profile-provides-backend ()
  :tags '(:db-live :pg-live)
  "PostgreSQL should connect when authinfo profile provides the backend."
  (if (not (clutch-db-test--pg-live-configured-p))
      (ert-skip "Set clutch-db-test-pg-password to enable PostgreSQL live tests")
    (clutch-db-test--with-authinfo-profile-conn
        conn "pg-"
        (list "backend" "pg"
              "login" clutch-db-test-pg-user
              "password" clutch-db-test-pg-password
              "db-host" clutch-db-test-pg-host
              "port" clutch-db-test-pg-port
              "database" clutch-db-test-pg-database)
        nil
      (should (clutch-db-live-p conn))
      (should (equal (clutch-db-user conn)
                     clutch-db-test-pg-user))
      (should (equal (clutch-db-database conn)
                     clutch-db-test-pg-database))
      (clutch-db-test--assert-live-basic-query conn))))

(ert-deftest clutch-db-test-pg-live-schema ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL schema introspection and switching."
  (clutch-db-test--with-pg conn
    (let* ((table (clutch-db-test--live-name "clutch_schema"))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (tenant_id SMALLINT, id SERIAL,
name TEXT, PRIMARY KEY (tenant_id, id))"
                     table))
            (let ((tables (clutch-db-list-tables conn)))
              (should (listp tables))
              (should (member table tables)))
            (let ((columns (clutch-db-list-columns conn table)))
              (should (listp columns))
              (should (member "tenant_id" columns))
              (should (member "id" columns))
              (should (member "name" columns)))
            (should (equal (clutch-db-primary-key-columns conn table)
                           '("tenant_id" "id")))
            (let ((ddl (clutch-db-object-definition
                        conn (list :name table :type "TABLE"))))
              (should (stringp ddl))
              (should (string-match-p "CREATE TABLE" ddl)))
            (let* ((original (clutch-db-current-schema conn))
                   (schema (clutch-db-test--live-name "clutch_switch"))
                   (quoted (clutch-db-escape-identifier conn schema)))
              (unwind-protect
                  (progn
                    (clutch-db-query conn (format "CREATE SCHEMA %s" quoted))
                    (should (member schema (clutch-db-list-schemas conn)))
                    (clutch-db-set-current-schema conn schema)
                    (should (equal (clutch-db-current-schema conn) schema))
                    (clutch-db-query conn
                                     "CREATE TABLE namespace_probe (id INT)")
                    (should (member "namespace_probe"
                                    (clutch-db-list-tables conn))))
                (ignore-errors (clutch-db-set-current-schema conn original))
                (ignore-errors
                  (clutch-db-query conn
                                   (format "DROP SCHEMA %s CASCADE" quoted))))))
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

(defun clutch-db-test--mongodb-live-drop-collection (conn collection)
  "Drop MongoDB COLLECTION through CONN, ignoring absence errors."
  (ignore-errors
    (mongodb-drop-collection
     (clutch-mongodb-conn-client conn)
     (clutch-mongodb-conn-database conn)
     collection)))

(defun clutch-db-test--mongodb-live-drop-database (conn database)
  "Drop MongoDB DATABASE through CONN, ignoring absence errors."
  (ignore-errors
    (mongodb-drop-database (clutch-mongodb-conn-client conn) database)))

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

(ert-deftest clutch-db-test-mongodb-live-authinfo-profile-connect ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB should connect through an authinfo `:profile-entry'."
  (if (not (and clutch-db-test-mongodb-live-enabled
                clutch-db-test-mongodb-host
                clutch-db-test-mongodb-port
                clutch-db-test-mongodb-database))
      (ert-skip "Set MongoDB host, port, and database to enable profile live tests")
    (clutch-db-test--with-authinfo-profile-conn
        conn "mongodb-"
        (append
         (list "backend" "mongodb"
               "db-host" clutch-db-test-mongodb-host
               "port" clutch-db-test-mongodb-port
               "database" clutch-db-test-mongodb-database)
         (when clutch-db-test-mongodb-user
           (list "login" clutch-db-test-mongodb-user))
         (when clutch-db-test-mongodb-password
           (list "password" clutch-db-test-mongodb-password))
         (when clutch-db-test-mongodb-auth-database
           (list "auth-database" clutch-db-test-mongodb-auth-database)))
        nil
      (should (clutch-db-live-p conn))
      (should (eq (clutch-db-backend-key conn) 'mongodb))
      (should (equal (clutch-db-display-name conn) "MongoDB"))
      (let* ((result (clutch-db-query
                      conn "db.runCommand({ping: 1})"))
             (columns (clutch-db-result-column-names
                       (clutch-db-result-columns result)))
             (ok-pos (cl-position "ok" columns :test #'equal)))
        (should ok-pos)
        (should (= (nth ok-pos (car (clutch-db-result-rows result)))
                   1.0))))))

(ert-deftest clutch-db-test-mongodb-live-query ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB should evaluate MQL helper commands and return document grids."
  (clutch-db-test--with-mongodb conn
    (let ((collection (clutch-db-test--mongodb-live-collection "query")))
      (unwind-protect
          (progn
            (clutch-db-test--mongodb-live-drop-collection conn collection)
            (let* ((result (clutch-db-query conn "db.runCommand({ping: 1})"))
                   (columns (clutch-db-result-column-names
                             (clutch-db-result-columns result)))
                   (ok-pos (cl-position "ok" columns :test #'equal)))
              (should ok-pos)
              (should (= (nth ok-pos (car (clutch-db-result-rows result)))
                         1.0)))
            (clutch-db-query
             conn
             (format
              (concat
               "db.getCollection(%S).insertMany(["
               "{_id: 'a', n: 1, s: 'hello', code: 'a);b]/c', "
               "createdAt: ISODate('2024-01-02T03:04:05.678Z')},"
               "{_id: 'b', n: 2, nested: {ok: true}}"
               "])")
              collection))
            (should (member collection (clutch-db-list-tables conn)))
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
                           "], {allowDiskUse: true})"
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
                           ".maxTimeMS(1000)"
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
               "db.getCollection(%S).replaceOne("
               "{_id: 'd'}, {_id: 'd', group: 'replaced', n: 9});"
               "db.getCollection(%S).insertOne({_id: 'e', n: 5});"
               "db.getCollection(%S).deleteOne({_id: 'e'})")
              collection collection collection collection collection))
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
                         '(("c" 3 "upd")
                           ("d" 9 "replaced"))))))
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
        (clutch-db-test--mongodb-live-drop-collection conn collection)))))

(ert-deftest clutch-db-test-mongodb-live-schema ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB metadata should expose databases, collections, and sampled keys."
  (clutch-db-test--with-mongodb conn
    (let* ((collection (clutch-db-test--mongodb-live-collection "schema"))
           (validation-collection (concat collection "_validation")))
      (unwind-protect
          (progn
            (clutch-db-test--mongodb-live-drop-collection conn collection)
            (clutch-db-test--mongodb-live-drop-collection
             conn validation-collection)
            (clutch-db-query
             conn
             (format
              (concat
               "db.getCollection(%S).insertOne({_id: 'sample-a', field: 1});"
               "db.getCollection(%S).insertOne({_id: 'sample-b', extra: true})")
              collection collection))
            (mongodb-create-index
             (clutch-mongodb-conn-client conn)
             (clutch-mongodb-conn-database conn)
             collection
             (mongodb-document '(("field" . 1)))
             (mongodb-document '(("name" . "field_idx"))))
            (clutch-db-query
             conn
             (format
              (concat
               "db.runCommand({create: %S, "
               "validator: {$jsonSchema: {bsonType: 'object', "
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
        (clutch-db-test--mongodb-live-drop-collection conn collection)
        (clutch-db-test--mongodb-live-drop-collection
         conn validation-collection)))))

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
        (clutch-db-test--mongodb-live-drop-database conn schema)
        (clutch-db-set-current-schema conn original)))))

(ert-deftest clutch-db-test-mongodb-live-error ()
  :tags '(:db-live :mongodb-live)
  "Native MongoDB query errors should signal `clutch-db-error'."
  (clutch-db-test--with-mongodb conn
    (should-error (clutch-db-query conn "db.getCollection(")
                  :type 'clutch-db-error)
    (let ((collection (clutch-db-test--mongodb-live-collection "error")))
      (unwind-protect
          (progn
            (clutch-db-test--mongodb-live-drop-collection conn collection)
            (clutch-db-query
             conn
             (format
              "db.getCollection(%S).insertOne({_id: 'dup'})"
              collection))
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
        (clutch-db-test--mongodb-live-drop-collection conn collection)))))

(ert-deftest clutch-db-test-redis-live-connect ()
  :tags '(:db-live :redis-live)
  "Redis connection should return a live key/value conn."
  (clutch-db-test--with-redis conn
    (should (clutch-db-live-p conn))
    (should (eq (clutch-db-backend-key conn) 'redis))
    (should (equal (clutch-db-display-name conn) "Redis"))
    (should (equal (clutch-db-current-schema conn)
                   (format "%s" clutch-db-test-redis-database)))))

(ert-deftest clutch-db-test-redis-live-authinfo-profile-connect ()
  :tags '(:db-live :redis-live)
  "Redis should connect through an authinfo `:profile-entry'."
  (if (not (clutch-db-test--redis-live-configured-p))
      (ert-skip "Set Redis host and port to enable profile live tests")
    (clutch-db-test--with-authinfo-profile-conn
        conn "redis-"
        (append
         (list "backend" "redis"
               "db-host" clutch-db-test-redis-host
               "port" clutch-db-test-redis-port
               "database" clutch-db-test-redis-database)
         (when clutch-db-test-redis-user
           (list "login" clutch-db-test-redis-user))
         (when clutch-db-test-redis-password
           (list "password" clutch-db-test-redis-password)))
        nil
      (should (clutch-db-live-p conn))
      (should (eq (clutch-db-backend-key conn) 'redis))
      (should (equal (clutch-db-current-schema conn)
                     (format "%s" clutch-db-test-redis-database))))))

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
              (let ((clutch-redis-browse-limit 1))
                (should (equal (clutch-db-object-browse-query conn entry)
                               (format "HRANDFIELD %S 1 WITHVALUES" hash-key)))
                (should (= (length
                            (clutch-db-result-rows
                             (clutch-db-query
                              conn (clutch-db-object-browse-query conn entry))))
                           1)))
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
  "Oracle JDBC should introspect and switch schemas on both sessions."
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
          (clutch-db-query conn drop-sql))))
    (let* ((original (clutch-db-current-schema conn))
           (schema (upcase (clutch-db-test--live-name "CC_SWITCH")))
           (quoted (clutch-db-escape-identifier conn schema)))
      (unwind-protect
          (progn
            (ignore-errors
              (clutch-db-query conn (format "DROP USER %s CASCADE" quoted)))
            (clutch-db-query
             conn
             (format "CREATE USER %s IDENTIFIED BY \"ClutchSwitch1\"" quoted))
            (should (member schema (clutch-db-list-schemas conn)))
            (clutch-db-set-current-schema conn schema)
            (should (equal (clutch-db-current-schema conn) schema))
            (clutch-db-query conn "CREATE TABLE namespace_probe (id NUMBER)")
            (should (member "NAMESPACE_PROBE" (clutch-db-list-tables conn)))
            (should
             (equal
              (caar
               (clutch-db-result-rows
                (clutch-db-query
                 conn
                 "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")))
              schema)))
        (ignore-errors (clutch-db-set-current-schema conn original))
        (ignore-errors
          (clutch-db-query conn (format "DROP USER %s CASCADE" quoted)))))))

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
  (dolist (case '(("matching-live-process" fake-proc fake-proc t t t)
                  ("disconnected-logical-connection" fake-proc fake-proc t nil nil)
                  ("dead-process" dead-proc dead-proc nil t nil)
                  ("nil-agent-process" old-proc nil t t nil)
                  ("mismatched-process" old-proc new-proc t t nil)))
    (pcase-let ((`(,label ,conn-proc ,agent-proc ,livep ,registered ,expected)
                 case))
      (ert-info ((format "live-p case: %s" label))
        (cl-letf (((symbol-function 'process-live-p) (lambda (_p) livep)))
          (let* ((clutch-jdbc--agent-process agent-proc)
                 (clutch-jdbc--connections-by-id
                  (make-hash-table :test 'eql))
                 (conn (make-clutch-jdbc-conn :process conn-proc :conn-id 1
                                              :params nil :busy nil)))
            (when registered
              (puthash 1 conn clutch-jdbc--connections-by-id))
            (if expected
                (should (clutch-db-live-p conn))
              (should-not (clutch-db-live-p conn)))))))))

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

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-cleans-state ()
  "When RPC timeout fires, agent and pending callback state are reset."
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
        (should (null clutch-jdbc--response-queue)))))
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

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-error-messages ()
  "Timeout errors should distinguish dead agents, lost sessions, and connects."
  (dolist (case
           '((:label "dead agent"
              :live nil
              :process dead-proc
              :must-match ("exited before replying")
              :deleted nil)
             (:label "lost session"
              :live t
              :process fake-proc
              :must-match ("Connection lost")
              :deleted fake-proc)
             (:label "connect timeout"
              :live t
              :process fake-proc
              :op "connect"
              :must-match ("Connection attempt timed out")
              :must-not-match ("reconnect with C-c C-e")
              :deleted fake-proc)))
    (ert-info ((format "recv-response timeout: %s" (plist-get case :label)))
      (let (deleted-proc)
        (cl-letf (((symbol-function 'process-live-p)
                   (lambda (_p) (plist-get case :live)))
                  ((symbol-function 'delete-process)
                   (lambda (p) (setq deleted-proc p)))
                  ((symbol-function 'accept-process-output)
                   (lambda (_p _s) nil)))
          (let ((clutch-jdbc--agent-process (plist-get case :process))
                (clutch-jdbc--response-queue nil))
            (condition-case err
                (progn
                  (if-let* ((op (plist-get case :op)))
                      (clutch-jdbc--recv-response 9999 0.0 op)
                    (clutch-jdbc--recv-response 9999 0.0))
                  (should nil))
              (clutch-db-error
               (dolist (pattern (plist-get case :must-match))
                 (should (string-match-p pattern (cadr err))))
               (dolist (pattern (plist-get case :must-not-match))
                 (should-not (string-match-p pattern (cadr err))))))
            (should (null clutch-jdbc--agent-process)))
          (should (eq deleted-proc (plist-get case :deleted)))
          )))))

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

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-debug-guidance ()
  "Connect errors should adapt their debug guidance to debug-mode state."
  (dolist (case '((:label "debug capture off"
                   :debug-mode nil
                   :mentions-enable t)
                  (:label "debug capture on"
                   :debug-mode t
                   :mentions-enable nil)))
    (ert-info ((format "connect error guidance: %s" (plist-get case :label)))
      (let ((clutch-debug-mode (plist-get case :debug-mode)))
        (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
                  ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 7))
                  ((symbol-function 'clutch-jdbc--recv-response)
                   (lambda (&rest _args)
                     '(:ok nil
                       :error "diag-token-2038"
                       :diag (:category "connect")))))
          (condition-case err
              (progn
                (clutch-jdbc--rpc
                 "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
                (should nil))
            (clutch-db-error
             (should (string-match-p "diag-token-2038" (cadr err)))
             (if (plist-get case :mentions-enable)
                 (should (string-match-p "clutch-debug-mode" (cadr err)))
               (should-not (string-match-p "clutch-debug-mode" (cadr err))))
             (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                     (cadr err))))))))))

(ert-deftest clutch-db-test-jdbc-rpc-uses-connection-timeout-by-default ()
  "Connection-scoped JDBC RPCs should inherit the connection RPC timeout."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 0.25)))
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         captured-timeout)
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 71))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (_id timeout &optional _op)
                 (setq captured-timeout timeout)
                 '(:ok t :result (:columns nil)))))
      (clutch-jdbc--rpc "get-columns" '((conn-id . 7) (table . "items")))
      (should (= captured-timeout 0.25))
      (clutch-jdbc--rpc "get-columns" '((conn-id . 7) (table . "items")) 0.1)
      (should (= captured-timeout 0.1)))))

(ert-deftest clutch-db-test-jdbc-send-encodes-debug-and-boolean-values ()
  "JDBC requests should encode debug and false as JSON booleans."
  (let ((clutch-jdbc--next-request-id 0)
        (clutch-debug-mode t)
        sent)
    (cl-letf (((symbol-function 'process-send-string)
               (lambda (_proc msg)
                 (setq sent msg))))
      (let ((clutch-jdbc--agent-process 'fake-proc))
        (clutch-jdbc--send "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb"))))
      (should (string-match-p "\"debug\":true" sent))
      (let ((clutch-debug-mode nil)
            (clutch-jdbc--agent-process 'fake-proc))
        (clutch-jdbc--send
         "set-auto-commit"
         `((conn-id . 9) (auto-commit . ,clutch-jdbc--json-false))))
      (should (string-match-p "\"auto-commit\":false" sent))
      (should-not (string-match-p "clutch-jdbc-json-false" sent)))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-details-contract ()
  "Connect errors should carry structured diagnostics and debug payloads."
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
                               :message "root-71"))))
        (debug '(:thread "clutch-jdbc-request"
                 :request-context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>")
                 :stack-trace "java.sql.SQLNonTransientConnectionException: boom")))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 71))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 `(:ok nil
                   :error "summary-71"
                   :diag ,diag
                   :debug ,debug))))
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
                                   (plist-get context :redacted-url)))
           (should (equal (plist-get (plist-get details :debug) :thread)
                          "clutch-jdbc-request"))
           (should (string-match-p
                    "SQLNonTransientConnectionException"
                    (plist-get (plist-get details :debug) :stack-trace))))
         (should-not (string-match-p "cookie-secret-71"
                                     (prin1-to-string (nth 2 err)))))))))

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

(ert-deftest clutch-db-test-jdbc-invalidated-error-retires-only-target-handle ()
  "An authoritative invalidation marker should retire only its JDBC handle."
  (let* ((conn (make-clutch-jdbc-conn
                :process 'fake-proc :conn-id 7 :params '(:driver oracle)))
         (other (make-clutch-jdbc-conn
                 :process 'fake-proc :conn-id 8 :params '(:driver oracle)))
         (clutch-jdbc--agent-process 'fake-proc)
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
         (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
         (clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
         (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
         (diag '(:category "query"
                 :op "execute"
                 :request-id 88
                 :conn-id 7
                 :connection-invalidated t
                 :exception-class "java.sql.SQLRecoverableException"
                 :sql-state "08000"
                 :vendor-code 17410
                 :raw-message "No more data to read from socket"))
         cancelled)
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (puthash 8 other clutch-jdbc--connections-by-id)
    (puthash conn 88 clutch-jdbc--busy-request-ids)
    (puthash other 89 clutch-jdbc--busy-request-ids)
    (puthash 90 (list :conn conn :timer 'target-timer)
             clutch-jdbc--async-callbacks)
    (puthash 91 (list :conn other :timer 'other-timer)
             clutch-jdbc--async-callbacks)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_proc) t))
              ((symbol-function 'cancel-timer)
               (lambda (timer) (push timer cancelled)))
              ((symbol-function 'clutch-jdbc--send)
               (lambda (&rest _args)
                 (ert-fail "Invalidated connection must not send disconnect"))))
      (let ((err
             (should-error
              (clutch-jdbc--response-result-or-signal
               conn "execute"
               `(:ok ,clutch-jdbc--json-false
                 :error "No more data to read from socket"
                 :diag ,diag))
              :type 'clutch-db-error)))
        (should (string-match-p "No more data" (cadr err))))
      (should-not (clutch-db-live-p conn))
      (should (clutch-db-live-p other))
      (should-not (gethash 7 clutch-jdbc--connections-by-id))
      (should (eq (gethash 8 clutch-jdbc--connections-by-id) other))
      (should-not (gethash conn clutch-jdbc--busy-request-ids))
      (should (= (gethash other clutch-jdbc--busy-request-ids) 89))
      (should-not (gethash 90 clutch-jdbc--async-callbacks))
      (should (gethash 91 clutch-jdbc--async-callbacks))
      (should (gethash 90 clutch-jdbc--ignored-response-ids))
      (should (equal cancelled '(target-timer)))
      (let* ((details (clutch-db-error-details conn))
             (stored-diag (plist-get details :diag)))
        (should (equal (plist-get stored-diag :raw-message)
                       "No more data to read from socket"))
        (should (eq (plist-get stored-diag :connection-invalidated) t)))
      (should (eq clutch-jdbc--agent-process 'fake-proc)))))

(ert-deftest clutch-db-test-jdbc-safe-retry-condition-requires-exact-protocol-facts ()
  "Only a pre-execution query invalidation should signal the retry condition."
  (dolist (case '(("execute" "execute" "query" 7 t t clutch-db-execution-not-started)
                  ("execute-params" "execute-params" "query" 7 t t clutch-db-execution-not-started)
                  ("execute" "execute" "query" 8 t t clutch-db-error)
                  ("execute" "execute" "query" 7 t nil clutch-db-error)
                  ("execute" "fetch" "query" 7 t t clutch-db-error)
                  ("execute" "execute" "metadata" 7 t t clutch-db-error)
                  ("fetch" "fetch" "fetch" 7 t t clutch-db-error)
                  ("execute" "execute" "query" 7 nil t clutch-db-error)))
    (pcase-let ((`(,op ,diag-op ,category ,diag-conn-id
                        ,invalidated ,not-started ,expected)
                 case))
      (let* ((conn (make-clutch-jdbc-conn
                    :process 'fake-proc :conn-id 7 :params '(:driver oracle)))
             (clutch-jdbc--agent-process 'fake-proc)
             (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
             (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
             (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
             (clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
             (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
             (diag `(:category ,category
                     :op ,diag-op
                     :conn-id ,diag-conn-id
                     :connection-invalidated ,invalidated
                     :execution-not-started ,not-started)))
        (puthash 7 conn clutch-jdbc--connections-by-id)
        (cl-letf (((symbol-function 'process-live-p) (lambda (_proc) t)))
          (let ((err
                 (should-error
                  (clutch-jdbc--response-result-or-signal
                   conn op
                   `(:ok ,clutch-jdbc--json-false
                     :error "idle validation failed"
                     :diag ,diag))
                  :type 'clutch-db-error)))
            (ert-info ((format "case: %S" case))
              (should (eq (car err) expected)))))))))

(ert-deftest clutch-db-test-jdbc-does-not-infer-invalidation-from-exception ()
  "Clutch should require the protocol marker instead of classifying errors."
  (let* ((conn (make-clutch-jdbc-conn
                :process 'fake-proc :conn-id 7 :params '(:driver oracle)))
         (clutch-jdbc--agent-process 'fake-proc)
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_proc) t)))
      (should-error
       (clutch-jdbc--response-result-or-signal
        conn "execute"
        `(:ok ,clutch-jdbc--json-false
          :error "No more data to read from socket"
          :diag (:category "query"
                 :op "execute"
                 :conn-id 7
                 :exception-class "java.sql.SQLRecoverableException"
                 :sql-state "08000"
                 :vendor-code 17410)))
       :type 'clutch-db-error)
      (should (clutch-db-live-p conn))
      (should (eq (gethash 7 clutch-jdbc--connections-by-id) conn)))))

(ert-deftest clutch-db-test-jdbc-interrupt-query-contract ()
  "JDBC interrupt should cancel only busy requests and preserve agent ownership."
  (let ((conn (make-clutch-jdbc-conn :conn-id 7
                                     :params '(:driver jdbc :rpc-timeout 12)))
        (clutch-jdbc--agent-process 'fake-proc)
        (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        (clutch-jdbc--response-queue
         '((:id 99 :ok t :result (:cancelled t :request-id 41))))
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
      (should (gethash 41 clutch-jdbc--ignored-response-ids))))
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
      (should-not deleted-proc)
      (should (gethash 99 clutch-jdbc--ignored-response-ids))))
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

(ert-deftest clutch-db-test-jdbc-interrupt-requires-confirmed-request ()
  "JDBC interrupt should reject unconfirmed or mismatched cancellation results."
  (dolist (result `((:cancelled ,clutch-jdbc--json-false :request-id 41)
                    (:cancelled t :request-id 42)
                    nil))
    (let ((conn (make-clutch-jdbc-conn :conn-id 7 :params '(:driver jdbc)))
          (clutch-jdbc--agent-process 'fake-proc)
          (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
          (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
          (clutch-jdbc--response-queue `((:id 99 :ok t :result ,result))))
      (puthash conn 41 clutch-jdbc--busy-request-ids)
      (cl-letf (((symbol-function 'clutch-jdbc--send)
                 (lambda (_op _params) 99)))
        (should-not (clutch-db-interrupt-query conn))))))

(ert-deftest clutch-db-test-jdbc-disconnect-cleans-state-contract ()
  "JDBC disconnect should clean local state without unsafe agent ownership changes."
  (dolist (case '((:label "slow live agent"
                   :agent-process fake-proc
                   :expect-send t
                   :expect-agent-process fake-proc)
                  (:label "dead agent"
                   :agent-process nil
                   :expect-send nil
                   :expect-agent-process nil)))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                          :params '(:driver jdbc :rpc-timeout 12)))
             (other-conn (make-clutch-jdbc-conn :conn-id 8
                                                :params '(:driver jdbc)))
             (clutch-jdbc-disconnect-timeout-seconds 0.1)
             (clutch-jdbc--agent-process (plist-get case :agent-process))
             (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
             (clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
             (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
             (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
             (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
             (clutch-jdbc--response-queue nil)
             deleted-proc
             send-called
             cancelled-timer)
        (puthash conn 41 clutch-jdbc--busy-request-ids)
        (puthash 43 (list :conn conn :timer 'metadata-timer)
                 clutch-jdbc--async-callbacks)
        (puthash 44 (list :conn other-conn :timer 'other-timer)
                 clutch-jdbc--async-callbacks)
        (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
        (puthash 7 conn clutch-jdbc--connections-by-id)
        (cl-letf (((symbol-function 'clutch-jdbc--send)
                   (lambda (&rest _args)
                     (setq send-called t)
                     99))
                  ((symbol-function 'process-live-p)
                   (lambda (_proc) t))
                  ((symbol-function 'accept-process-output)
                   (lambda (_proc _secs) nil))
                  ((symbol-function 'delete-process)
                   (lambda (proc)
                     (setq deleted-proc proc)))
                  ((symbol-function 'cancel-timer)
                   (lambda (timer)
                     (setq cancelled-timer timer))))
          (clutch-db-disconnect conn)
          (if (plist-get case :expect-send)
              (should send-called)
            (should-not send-called))
          (should (eq clutch-jdbc--agent-process
                      (plist-get case :expect-agent-process)))
          (should-not deleted-proc)
          (should-not (gethash conn clutch-jdbc--busy-request-ids))
          (should-not (gethash 43 clutch-jdbc--async-callbacks))
          (should (gethash 44 clutch-jdbc--async-callbacks))
          (should (eq cancelled-timer 'metadata-timer))
          (should (gethash 43 clutch-jdbc--ignored-response-ids))
          (should-not (gethash conn clutch-jdbc--error-details-by-conn))
          (should-not (gethash 7 clutch-jdbc--connections-by-id)))))))

(ert-deftest clutch-db-test-jdbc-clear-error-details-forgets-conn-cache ()
  "Clearing JDBC diagnostics should remove the connection-scoped cache entry."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 12)))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
    (clutch-db-clear-error-details conn)
    (should-not (gethash conn clutch-jdbc--error-details-by-conn))))

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

(ert-deftest clutch-db-test-jdbc-agent-filter-scans-fragments-incrementally ()
  "JDBC response fragments should not rescan bytes seen by earlier filter calls."
  (let ((buf (generate-new-buffer " *clutch-jdbc-filter-test*"))
        (clutch-jdbc--response-queue nil)
        (fragments '("{\"id\":" "17,\"ok\":" "true}" "\n"))
        scan-starts expected-starts record-scans)
    (unwind-protect
        (let ((original-search-forward (symbol-function 'search-forward)))
          (cl-letf (((symbol-function 'process-buffer) (lambda (_proc) buf))
                    ((symbol-function 'search-forward)
                     (lambda (string &rest args)
                       (when (and record-scans
                                  (eq (current-buffer) buf)
                                  (equal string "\n"))
                         (push (point) scan-starts))
                       (apply original-search-forward string args)))
                    ((symbol-function 'clutch-jdbc--dispatch-async-response)
                     (lambda (_parsed) nil)))
            (let ((next-start 1))
              (setq record-scans t)
              (dolist (fragment fragments)
                (push next-start expected-starts)
                (clutch-jdbc--agent-filter 'fake-proc fragment)
                (setq next-start (+ next-start (length fragment))))
              (push 1 expected-starts)
              (setq record-scans nil))
            (should (equal (nreverse scan-starts)
                           (nreverse expected-starts)))
            (clutch-jdbc--agent-filter
             'fake-proc "{\"id\":18,\"ok\":true}\n{\"id\":")
            (clutch-jdbc--agent-filter 'fake-proc "19,\"ok\":true}\n")
            (should (equal clutch-jdbc--response-queue
                           '((:id 17 :ok t) (:id 18 :ok t) (:id 19 :ok t))))
            (with-current-buffer buf
              (should (= (buffer-size) 0)))))
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
