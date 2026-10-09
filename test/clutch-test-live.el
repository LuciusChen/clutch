;;; clutch-test-live.el --- Live integration ERT tests -*- lexical-binding: t; -*-

;;; Commentary:

;; End-to-end live database workflow tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-test-backends))

(defvar mysql-tls-verify-server)
(defvar clutch-test-backend)
(defvar clutch-test-host)
(defvar clutch-test-port)
(defvar clutch-test-user)
(defvar clutch-test-password)
(defvar clutch-test-database)
(defvar clutch-test-url)
(defvar clutch-test-display-name)
(defvar clutch-test-props)
(defvar clutch-test-driver-class nil)
(defvar clutch--result-server-pageable)
(defvar clutch--result-server-rewritable)

;;;; Live integration tests

(defun clutch-test--live-connect-params ()
  "Return connection params for `clutch-test--with-conn'."
  (let ((params (if clutch-test-url
                    (list :url clutch-test-url)
                  (list :host clutch-test-host
                        :port clutch-test-port
                        :database clutch-test-database))))
    (when clutch-test-user
      (setq params (plist-put params :user clutch-test-user)))
    (when clutch-test-password
      (setq params (plist-put params :password clutch-test-password)))
    (when clutch-test-display-name
      (setq params (plist-put params :display-name clutch-test-display-name)))
    (when clutch-test-driver-class
      (setq params (plist-put params :driver-class clutch-test-driver-class)))
    (when clutch-test-props
      (setq params (plist-put params :props clutch-test-props)))
    params))

(ert-deftest clutch-test-live-connect-params-pass-driver-options ()
  "Generic JDBC live params should pass an optional driver class."
  :tags '(:clutch-live)
  (let ((clutch-test-url "jdbc:duckdb:/tmp/clutch-test.duckdb")
        (clutch-test-host nil)
        (clutch-test-port nil)
        (clutch-test-user nil)
        (clutch-test-password nil)
        (clutch-test-database nil)
        (clutch-test-display-name "DuckDB")
        (clutch-test-driver-class "org.duckdb.DuckDBDriver")
        (clutch-test-props nil))
    (let ((params (clutch-test--live-connect-params)))
      (should (equal (plist-get params :driver-class)
                     "org.duckdb.DuckDBDriver")))
    (let ((clutch-test-driver-class nil))
      (should-not
       (plist-member (clutch-test--live-connect-params) :driver-class)))))

(defun clutch-test--live-column-name (name)
  "Return NAME using the live backend's metadata identifier case."
  (if (clutch-test-live-backend-capability-p :uppercase-identifiers)
      (upcase name)
    name))

(ert-deftest clutch-test-live-column-name-follows-backend-metadata-case ()
  "Synthetic live rows should use the backend's metadata identifier case."
  :tags '(:clutch-live)
  (let ((clutch-test-backend 'oracle)
        (clutch-test-url nil))
    (should (equal (clutch-test--live-column-name "name") "NAME")))
  (let ((clutch-test-backend 'mysql)
        (clutch-test-url nil))
    (should (equal (clutch-test--live-column-name "name") "name"))))

(defun clutch-test--clickhouse-live-p ()
  "Return non-nil when live tests target ClickHouse."
  (clutch-test-live-backend-capability-p :clickhouse-engine))

(defun clutch-test--live-name-member-p (name names)
  "Return non-nil when NAME appears in NAMES, ignoring metadata case."
  (cl-find name names :test #'string-equal-ignore-case))

(defun clutch-test--updateable-live-backend-p ()
  "Return non-nil when generic live workflow SQL is valid for the backend."
  (clutch-test-live-backend-capability-p :updateable-workflow))

(defun clutch-test--result-live-backend-p ()
  "Return non-nil when result workflow SQL is valid for the backend."
  (clutch-test-live-backend-capability-p :result-workflow))

(defun clutch-test--live-supports-with-p (conn)
  "Return non-nil unless CONN is a MySQL server before 8.0, which has no WITH.
VERSION() gives MariaDB's own version, such as 10.11."
  (or (not (eq clutch-test-backend 'mysql))
      (>= (string-to-number
           (caar (clutch-db-result-rows
                  (clutch-db-query conn "SELECT VERSION()"))))
          8)))

(defun clutch-test--live-create-table-sql (table columns)
  "Return CREATE TABLE SQL for TABLE with COLUMNS.
COLUMNS entries have the shape (NAME KIND . ATTRS)."
  (format "CREATE TABLE %s (%s)%s"
          table
          (mapconcat
           (lambda (column)
             (pcase-let ((`(,name ,kind . ,attrs) column))
               (format "%s %s%s"
                       name
                       (pcase kind
                         ('int (if (clutch-test--clickhouse-live-p)
                                   "Int32" "INT"))
                         ('string (if (clutch-test--clickhouse-live-p)
                                      "String" "VARCHAR(64)"))
                         (_ (error "Unknown live test column kind: %S" kind)))
                       (if (and (memq 'primary attrs)
                                (not (clutch-test--clickhouse-live-p)))
                           " PRIMARY KEY"
                         ""))))
           columns
           ", ")
          (if (clutch-test--clickhouse-live-p) " ENGINE = Memory" "")))

(defun clutch-test--live-row-prefix-strings (row count)
  "Return the first COUNT values from ROW formatted as strings."
  (mapcar (lambda (value) (format "%s" value))
          (seq-take row count)))

(defun clutch-test--live-row-ids (rows)
  "Return the first-column identifiers from live ROWS as numbers."
  (mapcar (lambda (row) (string-to-number (format "%s" (car row))))
          rows))

(defmacro clutch-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live connection.
Skips if neither `clutch-test-password' nor `clutch-test-url' is set."
  (declare (indent 1))
  `(if (and (null clutch-test-password)
            (null clutch-test-url))
       (ert-skip "Set clutch-test-password or clutch-test-url to enable live tests")
     (let ((mysql-tls-verify-server nil))
       (let ((,var (clutch-db-connect
                    clutch-test-backend
                    (clutch-test--live-connect-params))))
         (unwind-protect
             (progn ,@body)
           (clutch-db-disconnect ,var))))))

(defmacro clutch-test--with-live-result-buffer (name &rest body)
  "Run BODY with live SELECT results isolated to result buffer NAME."
  (declare (indent 1) (debug (form body)))
  `(let ((clutch-test--result-name ,name))
     (cl-letf (((symbol-function 'clutch-result--buffer-name)
                (lambda () clutch-test--result-name)))
       (unwind-protect
           (progn ,@body)
         (when-let* ((buf (get-buffer clutch-test--result-name)))
           (kill-buffer buf))))))

(defun clutch-test--execute-live-select (conn sql)
  "Execute SQL through the result UI path for live connection CONN."
  (with-temp-buffer
    (let ((clutch-connection conn)
          (clutch--source-window (selected-window)))
      (clutch-test--execute-and-present sql conn))))

(ert-deftest clutch-test-live-clickhouse-converged-console-and-namespace-entrypoints ()
  "Unified console and namespace commands should work against ClickHouse."
  :tags '(:clutch-live)
  (unless (clutch-test--clickhouse-live-p)
    (ert-skip (clutch-test-capability-skip-message :clickhouse-engine)))
  (clutch-test--with-conn admin
    (let* ((database (format "clutch_switch_%d" (emacs-pid)))
           (params (append (list :backend 'clickhouse)
                           (clutch-test--live-connect-params)))
           (name (clutch--ad-hoc-console-name params))
           (clutch-console-directory (make-temp-file "clutch-console-" t))
           console-buffer)
      (unwind-protect
          (progn
            (clutch-db-query admin (format "CREATE DATABASE %s" database))
            (clutch-query-console (list :name name :params params))
            (setq console-buffer (current-buffer))
            (let ((old-connection clutch-connection)
                  (console-key (buffer-name console-buffer)))
              (cl-letf (((symbol-function 'completing-read)
                         (lambda (prompt collection &rest _args)
                           (should (equal prompt "Console: "))
                           (should (member console-key collection))
                           console-key)))
                (call-interactively #'clutch-query-console))
              (should (eq (current-buffer) console-buffer))
              (should (eq clutch-connection old-connection))
              (cl-letf (((symbol-function 'completing-read)
                         (lambda (prompt collection &rest _args)
                           (should (string-prefix-p
                                    "Switch schema/database" prompt))
                           (should (member database collection))
                           database)))
                (clutch-switch-schema))
              (should-not (eq clutch-connection old-connection))
              (should (equal (plist-get clutch--connection-params :database)
                             database))
              (should
               (equal
                (caar
                 (clutch-db-result-rows
                  (clutch-db-query clutch-connection
                                   "SELECT currentDatabase()")))
                database))))
        (when (buffer-live-p console-buffer)
          (kill-buffer console-buffer))
        (ignore-errors
          (clutch-db-query admin (format "DROP DATABASE IF EXISTS %s" database)))
        (delete-directory clutch-console-directory t)))))

(defun clutch-test--open-live-console (params)
  "Open the ad hoc query console for PARAMS, or reopen it, and return it."
  (clutch-query-console (list :name (clutch--ad-hoc-console-name params)
                              :params params))
  (current-buffer))

(defmacro clutch-test--with-live-console (params &rest body)
  "Run BODY in a query console opened on PARAMS, then close the console.
The console keeps its text in a temporary directory, its messages are
dropped, and closing it confirms nothing, as a test that failed can leave
uncommitted work behind."
  (declare (indent 1) (debug (form body)))
  (let ((buffer (make-symbol "buffer")))
    `(let ((clutch-console-directory (make-temp-file "clutch-console-" t))
           ,buffer)
       (unwind-protect
           (cl-letf (((symbol-function 'message) #'ignore))
             (setq ,buffer (clutch-test--open-live-console ,params))
             (with-current-buffer ,buffer ,@body))
         (when (buffer-live-p ,buffer)
           (cl-letf (((symbol-function 'yes-or-no-p) #'always))
             (kill-buffer ,buffer)))
         (delete-directory clutch-console-directory t)))))

(defun clutch-test--run-in-console (&rest statements)
  "Run each of STATEMENTS in the current console as typed, in turn."
  (dolist (sql statements)
    (clutch--execute sql)
    (clutch-test--await-queries)))

(defun clutch-test--end-console-session (admin)
  "End the console's server session from ADMIN and return its connection.
The console sees its connection closed before this returns.  The console
is on MySQL or PostgreSQL."
  (let* ((conn clutch-connection)
         (mysql (eq (clutch-db-backend-key conn) 'mysql))
         (id (clutch-test--server-session-id conn)))
    (clutch-db-query admin (format (if mysql
                                       "KILL %s"
                                     "SELECT pg_terminate_backend(%s)")
                                   id))
    (clutch-test--await (lambda () (not (clutch--connection-alive-p conn))))
    conn))

(defun clutch-test--server-session-id (conn)
  "Return the server's id of the session of MySQL or PostgreSQL CONN."
  (caar (clutch-db-result-rows
         (clutch-db-query conn (if (eq (clutch-db-backend-key conn) 'mysql)
                                   "SELECT CONNECTION_ID()"
                                 "SELECT pg_backend_pid()")))))

(defun clutch-test--server-session-count (admin id)
  "Return how many sessions with ID the server has, asked through ADMIN."
  (caar (clutch-db-result-rows
         (clutch-db-query
          admin
          (format (if (eq (clutch-db-backend-key admin) 'mysql)
                      "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = %s"
                    "SELECT count(*) FROM pg_stat_activity WHERE pid = %s")
                  id)))))

(defun clutch-test--await-session-end (admin id)
  "Wait until the server, asked through ADMIN, has no session with ID."
  (clutch-test--await
   (lambda () (equal (clutch-test--server-session-count admin id) 0))))

(ert-deftest clutch-test-live-console-follows-a-typed-namespace-switch ()
  "A console should follow a namespace switch typed into it.
It went on showing and loading the namespace it opened with, and its
parameters reconnected to that one.  PostgreSQL keeps the whole path.
Oracle and DuckDB list the tables of the schema they moved to, and
connecting with the console's parameters starts there."
  :tags '(:clutch-live)
  (unless (memq (clutch-test-live-backend-id) '(mysql pg oracle duckdb))
    (ert-skip "This regression covers MySQL USE, PostgreSQL SET search_path, Oracle ALTER SESSION and DuckDB USE"))
  (clutch-test--with-conn admin
    (let ((backend (clutch-test-live-backend-id))
          (schema (format "clutch_ns_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params))))
      (pcase-let ((`(,switch ,namespace ,key ,value ,check-sql)
                   (pcase backend
                     ('mysql
                      '("USE information_schema" "information_schema"
                        :database "information_schema" "SELECT DATABASE()"))
                     ('pg
                      (list (format "SET search_path TO %s, public" schema) schema
                            :search-path (format "%s, public" schema)
                            "SHOW search_path"))
                     ('oracle
                      (list (format "ALTER SESSION SET CURRENT_SCHEMA = %s" schema)
                            (upcase schema) :schema (upcase schema)
                            "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL"))
                     ('duckdb
                      (list (format "USE %s" schema) schema :schema schema
                            "SELECT current_schema()")))))
        (unwind-protect
            (progn
              (pcase backend
                ('pg (clutch-db-query admin (format "CREATE SCHEMA %s" schema)))
                ('oracle
                 (clutch-db-query
                  admin (format "CREATE USER %s IDENTIFIED BY \"Clutch_ns1\" QUOTA UNLIMITED ON users"
                                schema))
                 (clutch-db-query
                  admin (format "CREATE TABLE %s.only_here (id NUMBER)" schema)))
                ('duckdb
                 (clutch-db-query admin (format "CREATE SCHEMA %s" schema))
                 (clutch-db-query
                  admin (format "CREATE TABLE %s.only_here (id INTEGER)" schema))))
              (clutch-test--with-live-console params
                (when (eq backend 'oracle)
                  (clutch-test--run-in-console
                   (format "INSERT INTO %s.only_here VALUES (1)" schema))
                  (should (clutch--tx-dirty-p clutch-connection)))
                ;; Clutch asks before it runs an ALTER.
                (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                  (clutch-test--run-in-console switch))
                (should (equal (clutch-db-current-schema clutch-connection)
                               namespace))
                (should (equal (plist-get clutch--connection-params key) value))
                (when (memq backend '(oracle duckdb))
                  (should (clutch-test--live-name-member-p
                           "only_here" (clutch-db-list-tables clutch-connection))))
                (when (eq backend 'oracle)
                  ;; ALTER SESSION commits nothing, so the insert is still
                  ;; uncommitted work.
                  (should (clutch--tx-dirty-p clutch-connection))
                  (clutch-rollback)
                  (should (equal (format "%s"
                                         (caar (clutch-db-result-rows
                                                (clutch-db-query
                                                 admin (format "SELECT COUNT(*) FROM %s.only_here"
                                                               schema)))))
                                 "0")))
                (let ((reopened (clutch-db-connect clutch-test-backend
                                                   clutch--connection-params)))
                  (unwind-protect
                      (should (equal (caar (clutch-db-result-rows
                                            (clutch-db-query reopened check-sql)))
                                     value))
                    (clutch-db-disconnect reopened)))))
          (pcase backend
            ('pg
             (ignore-errors
               (clutch-db-query admin (format "DROP SCHEMA IF EXISTS %s" schema))))
            ('oracle
             (ignore-errors
               (clutch-db-query admin (format "DROP USER %s CASCADE" schema))))
            ('duckdb
             (ignore-errors
               (clutch-db-query
                admin (format "DROP SCHEMA IF EXISTS %s CASCADE" schema))))))))))

(defconst clutch-test--live-namespace-fixtures
  '((mysql "CREATE DATABASE %s" "DROP DATABASE IF EXISTS %s" "SELECT DATABASE()")
    (pg "CREATE SCHEMA %s" "DROP SCHEMA IF EXISTS %s CASCADE"
        "SELECT current_schemas(true)")
    (oracle "CREATE USER %s IDENTIFIED BY \"Clutch_ns1\"" "DROP USER %s CASCADE"
            "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
    (duckdb "CREATE SCHEMA %s" "DROP SCHEMA IF EXISTS %s CASCADE"
            "SELECT current_catalog(), current_schema(), current_setting('search_path')")
    (clickhouse "CREATE DATABASE %s" "DROP DATABASE IF EXISTS %s"
                "SELECT currentDatabase()"))
  "For each backend with a namespace switch: SQL that creates a namespace,
SQL that drops it, and a query that asks the server where a session is.")

(ert-deftest clutch-test-live-console-params-lead-back-to-a-switched-namespace ()
  "A connection with a console's parameters should start where the console is.
The automatic reconnect connects with them, so after `clutch-switch-schema'
a new connection must be where the server says the console is."
  :tags '(:clutch-live)
  (pcase-let ((`(,create ,drop ,where)
               (alist-get (clutch-test-live-backend-id)
                          clutch-test--live-namespace-fixtures)))
    (unless create
      (ert-skip "This backend has no namespace switch"))
    (clutch-test--with-conn admin
      (let ((name (funcall (if (eq clutch-test-backend 'oracle) #'upcase #'identity)
                           (format "clutch_ns_%d" (emacs-pid))))
            (params (append (list :backend clutch-test-backend)
                            (clutch-test--live-connect-params))))
        (unwind-protect
            (progn
              (clutch-db-query admin (format create name))
              (clutch-test--with-live-console params
                (cl-letf (((symbol-function 'completing-read) (lambda (&rest _) name))
                          ((symbol-function 'yes-or-no-p) #'always))
                  (clutch-switch-schema))
                (let ((here (clutch-db-result-rows (clutch-db-query clutch-connection where))))
                  (should (string-match-p (regexp-quote name) (format "%S" here)))
                  (let ((reopened (clutch-db-connect clutch-test-backend
                                                     clutch--connection-params)))
                    (unwind-protect
                        (should (equal (clutch-db-result-rows
                                        (clutch-db-query reopened where))
                                       here))
                      (clutch-db-disconnect reopened))))))
          (ignore-errors (clutch-db-query admin (format drop name))))))))

(ert-deftest clutch-test-live-clickhouse-url-console-lists-switches-and-reconnects ()
  "A ClickHouse console opened with a :url should stay in the database it shows.
Given only a :url, it listed the tables of `default'.  A switch
reconnected with the unchanged :url, so the server stayed in the old
database, and unqualified SQL ran there, while Clutch showed and listed
the new one.  The driver prefers a `database' property to the path."
  :tags '(:clutch-live)
  (unless (clutch-test--clickhouse-live-p)
    (ert-skip (clutch-test-capability-skip-message :clickhouse-engine)))
  (clutch-test--with-conn admin
    (let* ((a (format "clutch_url_a_%d" (emacs-pid)))
           (b (format "clutch_url_b_%d_+&" (emacs-pid)))
           (live (clutch-test--live-connect-params))
           (server (format "jdbc:clickhouse://%s:%d"
                           (plist-get live :host) (plist-get live :port))))
      (cl-flet ((current (conn)
                  (caar (clutch-db-result-rows
                         (clutch-db-query conn "SELECT currentDatabase()")))))
        (unwind-protect
            (progn
              (dolist (database (list a b))
                (clutch-db-query
                 admin (format "CREATE DATABASE %s"
                               (clutch-db-escape-identifier admin database))))
              (clutch-db-query
               admin (format "CREATE TABLE %s.only_in_a (id UInt8) ENGINE = Memory" a))
              (dolist (url (list (format "%s/%s" server a)
                                 (format "%s/default?database=%s" server a)
                                 (format "%s/%s" server
                                         (replace-regexp-in-string "_" "%5F" a))
                                 (format "%s/default?database=%s" server
                                         (replace-regexp-in-string "_" "%5F" a))))
                (ert-info (url)
                  (clutch-test--with-live-console
                      (list :backend 'clickhouse :url url
                            :user (plist-get live :user)
                            :password (plist-get live :password))
                    (should (member "only_in_a"
                                    (clutch-db-list-tables clutch-connection)))
                    (cl-letf (((symbol-function 'completing-read) (lambda (&rest _) b))
                              ((symbol-function 'yes-or-no-p) #'always))
                      (clutch-switch-schema))
                    (should (equal (current clutch-connection) b))
                    (should-not (member "only_in_a"
                                        (clutch-db-list-tables clutch-connection)))
                    (let ((reopened (clutch-db-connect 'clickhouse
                                                       clutch--connection-params)))
                      (unwind-protect
                          (should (equal (current reopened) b))
                        (clutch-db-disconnect reopened)))))))
          (dolist (database (list a b))
            (ignore-errors
              (clutch-db-query
               admin (format "DROP DATABASE IF EXISTS %s"
                             (clutch-db-escape-identifier admin database))))))))))

(ert-deftest clutch-test-live-duckdb-reconnect-refuses-an-unreachable-database ()
  "A DuckDB console that lost its session in a database a reconnect cannot reach
should say so instead of reconnecting.  A new connection opens the URL's
database file, so the automatic reconnect ran the next statement there,
or in a new, empty in-memory database, without a word."
  :tags '(:clutch-live :duckdb-live)
  (unless (eq (clutch-test-live-backend-id) 'duckdb)
    (ert-skip "Live backend is not DuckDB"))
  (let* ((attached (concat (make-temp-name
                            (expand-file-name "clutch-unreach-" temporary-file-directory))
                           ".duckdb"))
         (beside-memory (concat (make-temp-name
                                 (expand-file-name "clutch-unreach-" temporary-file-directory))
                                ".duckdb"))
         (alias (format "clutch_unreach_%d" (emacs-pid)))
         (params (append (list :backend clutch-test-backend)
                         (clutch-test--live-connect-params))))
    (unwind-protect
        (pcase-dolist (`(,label ,console-params ,setup ,namespace)
                       `(("attached" ,params
                          (,(format "ATTACH '%s' AS %s" attached alias)
                           ,(format "USE %s" alias))
                          ,(format "%s.main" alias))
                         ("in memory" ,(plist-put (copy-sequence params) :url "jdbc:duckdb:")
                          ("CREATE TABLE kept (v INTEGER)")
                          "memory.main")
                         ("attached to memory"
                          ,(plist-put (copy-sequence params) :url "jdbc:duckdb:")
                          (,(format "ATTACH '%s' AS %s" beside-memory alias)
                           ,(format "USE %s" alias))
                          ,(format "%s.main" alias))))
          (ert-info (label)
            (clutch-test--with-live-console console-params
              (apply #'clutch-test--run-in-console setup)
              (let ((lost clutch-connection))
                (clutch-db-disconnect lost)
                (should (string-match-p
                         (regexp-quote namespace)
                         (error-message-string
                          (should-error (clutch-test--run-in-console "SELECT 1")
                                        :type 'user-error))))
                (should (eq clutch-connection lost))))))
      (dolist (file (list attached (concat attached ".wal")
                          beside-memory (concat beside-memory ".wal")))
        (when (file-exists-p file)
          (delete-file file))))))

(ert-deftest clutch-test-live-duckdb-reconnect-stays-out-of-attached-databases ()
  "A DuckDB console moved into an attached database should keep its parameters.
A reconnect cannot return to an attached database, so the parameters
keep naming the database the URL opens, and connecting with them starts
there."
  :tags '(:clutch-live :duckdb-live)
  (unless (eq (clutch-test-live-backend-id) 'duckdb)
    (ert-skip "Live backend is not DuckDB"))
  (let ((attached (concat (make-temp-name
                           (expand-file-name "clutch-att-" temporary-file-directory))
                          ".duckdb"))
        (alias (format "clutch_att_%d" (emacs-pid)))
        (params (append (list :backend clutch-test-backend)
                        (clutch-test--live-connect-params))))
    (unwind-protect
        (clutch-test--with-live-console params
          (let ((before clutch--connection-params))
            (clutch-test--run-in-console
             (format "ATTACH '%s' AS %s" attached alias)
             (format "CREATE SCHEMA %s.side" alias)
             (format "USE %s.side" alias))
            (should (equal (caar (clutch-db-result-rows
                                  (clutch-db-query clutch-connection
                                                   "SELECT current_catalog()")))
                           alias))
            (should (equal clutch--connection-params before))
            (let* ((reopened (clutch-db-connect clutch-test-backend
                                                clutch--connection-params))
                   (home (unwind-protect
                             (caar (clutch-db-result-rows
                                    (clutch-db-query reopened
                                                     "SELECT current_catalog()")))
                           (clutch-db-disconnect reopened))))
              (should-not (equal home alias))
              (clutch-test--run-in-console (format "USE %s" home)
                                           (format "DETACH %s" alias)))))
      (dolist (file (list attached (concat attached ".wal")))
        (when (file-exists-p file)
          (delete-file file))))))

(ert-deftest clutch-test-live-duckdb-use-replaces-the-metadata-of-the-database-left ()
  "A DuckDB console moved into another database should offer that one's tables.
A USE of an attached database keeps the schema name main, and Clutch
compared only that name, so it kept the cached tables of the database
left.  A SET that moves nothing keeps the cached metadata."
  :tags '(:clutch-live :duckdb-live)
  (unless (eq (clutch-test-live-backend-id) 'duckdb)
    (ert-skip "Live backend is not DuckDB"))
  (let ((attached (concat (make-temp-name
                           (expand-file-name "clutch-meta-" temporary-file-directory))
                          ".duckdb"))
        (alias (format "clutch_meta_%d" (emacs-pid)))
        (left (format "clutch_left_%d" (emacs-pid)))
        (params (append (list :backend clutch-test-backend)
                        (clutch-test--live-connect-params))))
    (cl-flet ((cached-tables ()
                (sort (hash-table-keys (clutch--schema-for-connection))
                      #'string<)))
      (unwind-protect
          (clutch-test--with-live-console params
            (let ((home (caar (clutch-db-result-rows
                               (clutch-db-query clutch-connection
                                                "SELECT current_catalog()")))))
              (unwind-protect
                  (progn
                    (clutch-test--run-in-console
                     (format "CREATE TABLE %s (id INTEGER)" left)
                     (format "ATTACH '%s' AS %s" attached alias)
                     (format "CREATE TABLE %s.main.only_here (id INTEGER)" alias))
                    (clutch--refresh-schema-cache clutch-connection)
                    (should (member left (cached-tables)))
                    (clutch-test--run-in-console (format "USE %s" alias))
                    (should (equal (cached-tables) '("only_here")))
                    (let ((schema (clutch--schema-for-connection)))
                      (clutch-test--run-in-console "SET enable_progress_bar = false")
                      (should (eq (clutch--schema-for-connection) schema))))
                (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                  (clutch-test--run-in-console
                   (format "USE %s" (clutch-db-escape-identifier clutch-connection home))
                   (format "DETACH %s" alias)
                   (format "DROP TABLE IF EXISTS %s" left))))))
        (dolist (file (list attached (concat attached ".wal")))
          (when (file-exists-p file)
            (delete-file file)))))))

(ert-deftest clutch-test-live-oracle-reconnect-keeps-a-quoted-schema-name ()
  "A console moved to a quoted Oracle schema should reconnect into that schema.
The console recorded the schema as Oracle names it, but connecting with
its parameters upper-cased the name: a mixed-case one failed with
ORA-01435, and a lower-case one moved the session into the upper-case
schema of the same name when there was one."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'oracle)
    (ert-skip "This regression covers Oracle's quoted schema names"))
  (clutch-test--with-conn admin
    (let* ((mixed (format "Clutch_Mx_%d" (emacs-pid)))
           (lower (format "clutch_lc_%d" (emacs-pid)))
           (users (list mixed lower (upcase lower)))
           (params (append (list :backend 'oracle) (clutch-test--live-connect-params))))
      (unwind-protect
          (progn
            (dolist (user users)
              (clutch-db-query
               admin (format "CREATE USER \"%s\" IDENTIFIED BY \"Clutch_ns1\"" user)))
            (dolist (schema (list mixed lower))
              (ert-info (schema)
                (clutch-test--with-live-console params
                  ;; Clutch asks before it runs an ALTER.
                  (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                    (clutch-test--run-in-console
                     (format "ALTER SESSION SET CURRENT_SCHEMA = \"%s\"" schema)))
                  (should (equal (clutch-db-current-schema clutch-connection) schema))
                  (let ((reopened (clutch-db-connect 'oracle clutch--connection-params)))
                    (unwind-protect
                        (should (equal (caar (clutch-db-result-rows
                                              (clutch-db-query
                                               reopened
                                               "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")))
                                       schema))
                      (clutch-db-disconnect reopened)))))))
        (dolist (user users)
          (ignore-errors
            (clutch-db-query admin (format "DROP USER \"%s\" CASCADE" user))))))))

(ert-deftest clutch-test-live-mysql-console-follows-a-dropped-current-database ()
  "A MySQL console should have no database once its current one is dropped.
It went on showing the dropped database, and its automatic reconnect
asked for that database and failed.  Loading the tables of no database
must not fail either."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'mysql)
    (ert-skip "This regression covers MySQL's current database"))
  (clutch-test--with-conn admin
    (let ((database (format "clutch_drop_%d" (emacs-pid)))
          (params (append (list :backend 'mysql) (clutch-test--live-connect-params))))
      (cl-flet ((server-database ()
                  (caar (clutch-db-result-rows
                         (clutch-db-query clutch-connection "SELECT DATABASE()"))))
                (schema-state ()
                  (plist-get (clutch--schema-status-entry clutch-connection) :state)))
        (unwind-protect
            (progn
              (clutch-db-query admin (format "CREATE DATABASE %s" database))
              (clutch-test--with-live-console params
                (clutch-test--run-in-console (format "USE %s" database))
                (should (equal (clutch-db-current-schema clutch-connection) database))
                (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                  (clutch-test--run-in-console (format "DROP DATABASE %s" database)))
                (should-not (server-database))
                (should-not (clutch-db-current-schema clutch-connection))
                (should-not (plist-get clutch--connection-params :database))
                (clutch-test--await (lambda () (not (eq (schema-state) 'refreshing))))
                (should (eq (schema-state) 'ready))
                (let ((lost (clutch-test--end-console-session admin)))
                  (clutch-test--run-in-console "SELECT 1")
                  (should-not (eq clutch-connection lost))
                  (should-not (server-database)))))
          (ignore-errors
            (clutch-db-query admin (format "DROP DATABASE IF EXISTS %s" database))))))))

(ert-deftest clutch-test-live-pg-console-follows-the-server-search-path ()
  "A PostgreSQL console should follow the search_path that the server has.
A SET written with a quoted name or a comment was not followed, a
rollback that undid a SET left the console, and its reconnect, on the
schema the server had left, and a SET not yet committed went into the
reconnect parameters."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "This regression covers the PostgreSQL search_path"))
  (clutch-test--with-conn admin
    (let* ((schema (format "clutch_ns_back_%d" (emacs-pid)))
           (moved (format "%s, public" schema))
           (params (append (list :backend 'pg) (clutch-test--live-connect-params)))
           default-path)
      (cl-labels ((run (&rest statements)
                    (apply #'clutch-test--run-in-console statements))
                  (server-path ()
                    (caar (clutch-db-result-rows
                           (clutch-db-query clutch-connection "SHOW search_path"))))
                  (check (path namespace &optional reconnect-path)
                    (should (equal (server-path) path))
                    (should (equal (clutch-db-current-schema clutch-connection)
                                   namespace))
                    (should (equal (plist-get clutch--connection-params :search-path)
                                   (or reconnect-path path)))))
        (unwind-protect
            (progn
              (clutch-db-query admin (format "CREATE SCHEMA %s" schema))
              (clutch-test--with-live-console params
                (setq default-path (server-path))
                (run (format "SET \"search_path\" TO %s -- note" moved))
                (check moved schema)
                (run "RESET /* back */ search_path")
                (check default-path "public")
                (run "BEGIN" (format "SET search_path TO %s" moved) "ROLLBACK")
                (check default-path "public")
                (run "BEGIN" "SAVEPOINT s" (format "SET search_path TO %s" moved)
                     "ROLLBACK TO SAVEPOINT s" "COMMIT")
                (check default-path "public")
                (clutch-toggle-auto-commit)
                (run (format "SET search_path TO %s" moved))
                (check moved schema default-path)
                (clutch-rollback)
                (check default-path "public")
                (run (format "SET search_path TO %s" moved))
                (clutch-commit)
                (check moved schema)
                (run "RESET search_path")
                (check default-path "public" moved)
                (clutch-commit)
                (check default-path "public")
                (clutch-toggle-auto-commit)))
          (ignore-errors
            (clutch-db-query admin (format "DROP SCHEMA IF EXISTS %s" schema))))))))

(ert-deftest clutch-test-live-pg-reconnect-restores-only-a-kept-search-path ()
  "A PostgreSQL reconnect should restore the search_path the server kept.
A path set inside a transaction went into the reconnect parameters at
once, so a connection lost before the transaction ended came back on it,
though the server had rolled it back with the transaction."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "This regression covers the PostgreSQL search_path"))
  (clutch-test--with-conn admin
    (let* ((schema (format "clutch_ns_lost_%d" (emacs-pid)))
           (moved (format "%s, public" schema))
           (params (append (list :backend 'pg) (clutch-test--live-connect-params)))
           default-path)
      (cl-labels ((server-path ()
                    (caar (clutch-db-result-rows
                           (clutch-db-query clutch-connection "SHOW search_path"))))
                  (path-after-a-lost-connection ()
                    (let ((lost (clutch-test--end-console-session admin)))
                      (clutch-test--run-in-console "SELECT 1")
                      (should-not (eq clutch-connection lost))
                      (server-path))))
        (unwind-protect
            (progn
              (clutch-db-query admin (format "CREATE SCHEMA %s" schema))
              (clutch-test--with-live-console params
                (setq default-path (server-path))
                (clutch-test--run-in-console
                 "BEGIN" (format "SET LOCAL search_path TO %s" moved))
                (should (equal (clutch-db-current-schema clutch-connection) schema))
                (should (equal (path-after-a-lost-connection) default-path))
                (clutch-test--run-in-console
                 "BEGIN" (format "SET search_path TO %s" moved))
                (should (equal (path-after-a-lost-connection) default-path))
                (clutch-test--run-in-console
                 "BEGIN" (format "SET search_path TO %s" moved)
                 "COMMIT /* outer /* inner */ outer */ AND CHAIN -- last in its buffer")
                (should (equal (path-after-a-lost-connection) moved))))
          (ignore-errors
            (clutch-db-query admin (format "DROP SCHEMA IF EXISTS %s" schema))))))))

(ert-deftest clutch-test-live-pg-rollback-after-a-failed-commit-follows-the-path ()
  "A rollback after a failed PostgreSQL commit should show the server's path.
A COMMIT that fails rolls back the transaction, and a SET made in it, but
clutch-rollback then found nothing to end, and the console went on
showing the schema that SET had chosen."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "This regression covers the PostgreSQL search_path"))
  (clutch-test--with-conn admin
    (let ((schema (format "clutch_ns_failed_%d" (emacs-pid)))
          (parent (format "clutch_parent_%d" (emacs-pid)))
          (child (format "clutch_child_%d" (emacs-pid)))
          (params (append (list :backend 'pg) (clutch-test--live-connect-params))))
      (unwind-protect
          (progn
            (clutch-db-query admin (format "CREATE SCHEMA %s" schema))
            (clutch-db-query admin (format "CREATE TABLE public.%s (id int PRIMARY KEY)"
                                           parent))
            (clutch-db-query
             admin (format "CREATE TABLE public.%s (pid int REFERENCES public.%s %s)"
                           child parent "DEFERRABLE INITIALLY DEFERRED"))
            (clutch-test--with-live-console params
              (let ((default-path (caar (clutch-db-result-rows
                                         (clutch-db-query clutch-connection
                                                          "SHOW search_path")))))
                (clutch-toggle-auto-commit)
                (clutch-test--run-in-console
                 (format "SET search_path TO %s, public" schema)
                 (format "INSERT INTO public.%s VALUES (1)" child))
                (should (equal (clutch--shown-namespace) schema))
                (should-error (clutch-commit) :type 'user-error)
                (clutch-rollback)
                (should (equal (clutch--shown-namespace) "public"))
                (should (equal (plist-get clutch--connection-params :search-path)
                               default-path))
                (clutch-toggle-auto-commit))))
        (ignore-errors
          (clutch-db-query admin (format "DROP TABLE IF EXISTS public.%s, public.%s"
                                         child parent)))
        (ignore-errors
          (clutch-db-query admin (format "DROP SCHEMA IF EXISTS %s" schema)))))))

(ert-deftest clutch-test-live-reconnect-keeps-manual-commit-mode ()
  "An automatic reconnect should keep a console in Manual mode.
The console came back in Auto mode, so each statement after the
reconnect was committed on its own, past the reach of a rollback.
Reopening a console whose session was lost reconnects it too."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL manual commit"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_manual_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params))))
      (unwind-protect
          (progn
            (clutch-db-query admin (format "CREATE TABLE %s (id int)" table))
            (clutch-test--with-live-console params
              (clutch-toggle-auto-commit)
              (let ((lost (clutch-test--end-console-session admin)))
                (clutch-test--run-in-console "SELECT 1")
                (should-not (eq clutch-connection lost)))
              (should (clutch-db-manual-commit-p clutch-connection))
              (clutch-test--run-in-console (format "INSERT INTO %s VALUES (1)" table))
              (clutch-rollback)
              (should (equal (caar (clutch-db-result-rows
                                    (clutch-db-query
                                     admin (format "SELECT COUNT(*) FROM %s" table))))
                             0))
              (let ((console (current-buffer))
                    (lost (clutch-test--end-console-session admin)))
                (should (eq (clutch-test--open-live-console params) console))
                (should-not (eq clutch-connection lost))
                (should (clutch-db-manual-commit-p clutch-connection)))
              (clutch-toggle-auto-commit)))
        (ignore-errors
          (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table)))))))

(ert-deftest clutch-test-live-reopened-console-ends-the-lost-session ()
  "Reopening a console whose session was lost should end that session in full.
The console came back on a new connection, but its results stayed on the
dead one, its uncommitted work was not reported lost, and the old
transaction state stayed behind."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL consoles"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_reopen_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params)))
          (result-name "*clutch-test-reopen-result*"))
      (unwind-protect
          (progn
            (clutch-db-query admin (format "CREATE TABLE %s (id int)" table))
            (clutch-test--with-live-console params
              (let ((console (current-buffer)))
                (clutch-test--with-live-result-buffer result-name
                  (clutch-toggle-auto-commit)
                  (clutch-test--run-in-console
                   (format "INSERT INTO %s VALUES (1)" table)
                   (format "SELECT id FROM %s" table))
                  (with-current-buffer console
                    (should (clutch--tx-dirty-p clutch-connection))
                    (let ((lost (clutch-test--end-console-session admin))
                          messages)
                      (cl-letf (((symbol-function 'message)
                                 (lambda (format-string &rest args)
                                   (when format-string
                                     (push (apply #'format format-string args)
                                           messages)))))
                        (should (eq (clutch-test--open-live-console params) console)))
                      (should-not (eq clutch-connection lost))
                      (should (eq (buffer-local-value 'clutch-connection
                                                      (get-buffer result-name))
                                  clutch-connection))
                      (should-not (clutch--tx-state lost))
                      (should-not (clutch--tx-state clutch-connection))
                      (should (cl-some (lambda (text)
                                         (string-match-p
                                          "uncommitted changes were lost" text))
                                       messages))
                      (should (clutch-db-manual-commit-p clutch-connection))
                      (clutch-toggle-auto-commit)))))))
        (ignore-errors
          (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table)))))))

(ert-deftest clutch-test-live-connect-in-a-console-moves-its-session ()
  "`C-c C-e' in a console should move its whole session to a new connection.
After the session was lost, it bound only the console, and the console's
result reconnected to a session of its own; over a live session it left
the result with no connection, so refreshing it failed.  The new
connection started in Auto mode."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL consoles"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_connect_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params)))
          (result-name (format " *clutch-connect-result-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query admin (format "CREATE TABLE %s (id int PRIMARY KEY)" table))
            (clutch-db-query admin (format "INSERT INTO %s VALUES (1)" table))
            (clutch-test--with-live-console params
              (clutch-test--with-live-result-buffer result-name
                (clutch-toggle-auto-commit)
                (clutch-test--run-in-console (format "SELECT id FROM %s" table))
                (pcase-dolist (`(,label ,lose-session) '(("lost" t) ("live" nil)))
                  (ert-info (label)
                    (let ((old clutch-connection))
                      (when lose-session
                        (clutch-test--end-console-session admin))
                      (clutch-connect)
                      (should-not (eq clutch-connection old))
                      (should (clutch--connection-alive-p clutch-connection))
                      (should (clutch-db-manual-commit-p clutch-connection))
                      (let ((conn clutch-connection))
                        (with-current-buffer result-name
                          (should (eq clutch-connection conn))
                          (clutch-result-rerun)
                          (clutch-test--await-queries)
                          (should (eq clutch-connection conn)))))))
                (clutch-toggle-auto-commit))))
        (ignore-errors
          (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table)))))))

(ert-deftest clutch-test-live-connect-after-the-saved-entry-moved-leaves-the-results ()
  "`C-c C-e' should leave a console's results once its saved entry moved.
The entry led to another database or schema since the console connected,
and `C-c C-e' took the console's results there with their staged edits,
in the console's mode; with another server behind the entry, submitting
wrote to that server.  The console connects alone, in the mode the entry
starts in, and its results keep the old connection, or none."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL saved consoles"))
  (clutch-test--with-conn admin
    (let* ((mysql (eq clutch-test-backend 'mysql))
           (a (format "clutch_moved_a_%d" (emacs-pid)))
           (b (format "clutch_moved_b_%d" (emacs-pid)))
           (entry (lambda (namespace)
                    (append (list :backend clutch-test-backend
                                  (if mysql :database :schema) namespace)
                            (clutch-test--live-connect-params))))
           (clutch-console-directory (make-temp-file "clutch-console-" t))
           (result-name (format " *clutch-moved-result-%d*" (emacs-pid)))
           clutch-connection-alist console)
      (cl-flet ((value (namespace)
                  (caar (clutch-db-result-rows
                         (clutch-db-query
                          admin (format "SELECT v FROM %s.t WHERE id = 1" namespace))))))
        (unwind-protect
            (progn
              (dolist (namespace (list a b))
                (clutch-db-query admin (format (if mysql
                                                   "CREATE DATABASE %s"
                                                 "CREATE SCHEMA %s")
                                               namespace))
                (clutch-db-query
                 admin (format "CREATE TABLE %s.t (id int PRIMARY KEY, v varchar(20))"
                               namespace))
                (clutch-db-query
                 admin (format "INSERT INTO %s.t VALUES (1, 'orig')" namespace)))
              (pcase-dolist (`(,label ,lose-session) '(("live" nil) ("lost" t)))
                (ert-info (label)
                  (setq clutch-connection-alist (list (cons "moved" (funcall entry a))))
                  (cl-letf (((symbol-function 'message) #'ignore))
                    (clutch-query-console "moved")
                    (setq console (current-buffer))
                    (clutch-test--with-live-result-buffer result-name
                      (clutch-toggle-auto-commit)
                      (clutch-test--run-in-console "SELECT id, v FROM t")
                      (with-current-buffer result-name
                        (set-window-buffer (selected-window) (current-buffer))
                        (clutch--goto-cell 0 1)
                        (with-current-buffer (clutch-result-edit-cell)
                          (erase-buffer)
                          (insert "edited")
                          (clutch-result-edit-finish)))
                      (setq clutch-connection-alist
                            (list (cons "moved" (funcall entry b))))
                      (with-current-buffer console
                        (let ((old (if lose-session
                                       (clutch-test--end-console-session admin)
                                     clutch-connection)))
                          (clutch-connect)
                          (should (equal (clutch-db-current-schema clutch-connection) b))
                          (should-not (clutch-db-manual-commit-p clutch-connection))
                          (with-current-buffer result-name
                            (if lose-session
                                (should (eq clutch-connection old))
                              (should-not clutch-connection)
                              (should (string-match-p
                                       "Connection closed"
                                       (error-message-string
                                        (should-error (clutch-result-submit)
                                                      :type 'user-error))))))))
                      (should (equal (value a) "orig"))
                      (should (equal (value b) "orig"))))
                  (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                    (kill-buffer console)))))
          (when (buffer-live-p console)
            (cl-letf (((symbol-function 'yes-or-no-p) #'always))
              (kill-buffer console)))
          (dolist (namespace (list a b))
            (ignore-errors
              (clutch-db-query admin (format (if mysql
                                                 "DROP DATABASE IF EXISTS %s"
                                               "DROP SCHEMA IF EXISTS %s CASCADE")
                                             namespace))))
          (delete-directory clutch-console-directory t))))))

(defun clutch-test--result-action-after-session-loss (action)
  "Check that result ACTION recovers a lost session before contacting it."
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL session loss"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_result_recover_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params)))
          (result-name (format " *clutch-result-recover-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query admin
                             (format "CREATE TABLE %s (id int PRIMARY KEY, v varchar(20))"
                                     table))
            (clutch-db-query admin (format "INSERT INTO %s VALUES (1, 'orig')" table))
            (dolist (manual '(nil t))
              (ert-info ((format "%s, %s" action (if manual "Manual" "Auto")))
                (clutch-test--with-live-console params
                  (let ((console (current-buffer)))
                    (clutch-test--with-live-result-buffer result-name
                      (when manual (clutch-toggle-auto-commit))
                      (clutch-test--run-in-console (format "SELECT id, v FROM %s" table))
                      (with-current-buffer result-name
                        (pcase action
                          ('submit (clutch-test--xtdb-edit 0 "v" "edited"))
                          ('delete
                           (set-window-buffer (selected-window) (current-buffer))
                           (clutch--goto-cell 0 1)
                           (call-interactively #'clutch-result-delete-rows))))
                      (let ((lost (clutch-test--end-console-session admin)))
                        (with-current-buffer result-name
                          (set-window-buffer (selected-window) (current-buffer))
                          (clutch--goto-cell 0 1)
                          (pcase action
                            ('edit
                             (with-current-buffer (clutch-result-edit-cell)
                               (erase-buffer)
                               (insert "edited")
                               (clutch-result-edit-finish)))
                            ('copy
                             (let (kill-ring kill-ring-yank-pointer)
                               (clutch-result-copy 'update '((0) 1))
                               (should (string-match-p "UPDATE.*SET.*v.*orig"
                                                       (current-kill 0)))))
                            ((or 'submit 'delete) (clutch-test--xtdb-submit)))
                          (should-not (eq clutch-connection lost))
                          (should (eq clutch-connection
                                      (buffer-local-value 'clutch-connection console)))
                          (should (eq (not (null (clutch-db-manual-commit-p
                                                  clutch-connection)))
                                      manual))))
                      (with-current-buffer console
                        (when manual (clutch-rollback)))
                      (should
                       (equal (caar (clutch-db-result-rows
                                     (clutch-db-query
                                      admin (format "SELECT v FROM %s WHERE id=1" table))))
                              (cond (manual "orig")
                                    ((eq action 'submit) "edited")
                                    ((eq action 'delete) nil)
                                    (t "orig")))))))
                (clutch-db-query admin (format "DELETE FROM %s" table))
                (clutch-db-query admin (format "INSERT INTO %s VALUES (1, 'orig')" table)))))
        (ignore-errors
          (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table)))))))

(ert-deftest clutch-test-live-result-edit-recovers-a-lost-session ()
  "The first cell edit after session loss should recover its connection."
  :tags '(:clutch-live)
  (clutch-test--result-action-after-session-loss 'edit))

(ert-deftest clutch-test-live-result-copy-update-recovers-a-lost-session ()
  "The first UPDATE copy after session loss should recover its connection."
  :tags '(:clutch-live)
  (clutch-test--result-action-after-session-loss 'copy))

(ert-deftest clutch-test-live-result-submit-recovers-a-lost-session ()
  "Submitting staged edits after session loss should recover before its batch."
  :tags '(:clutch-live)
  (clutch-test--result-action-after-session-loss 'submit))

(ert-deftest clutch-test-live-result-delete-submit-recovers-a-lost-session ()
  "Submitting a staged delete after session loss should recover before its batch.
A delete loads no column metadata, so only the recovery before the batch
reconnects it."
  :tags '(:clutch-live)
  (clutch-test--result-action-after-session-loss 'delete))

(ert-deftest clutch-test-live-connect-outside-a-console-keeps-its-session ()
  "`C-c C-e' outside a console should connect a session picked again anew.
In a buffer with `clutch-mode' or the REPL, picking the connection the
session was on started a new session in Auto mode, though the buffer was
in Manual; over a live session the buffer's result lost its connection,
and over a lost one it reconnected to a session of its own.  Picking
another connection connects the buffer alone, in that connection's mode."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL"))
  (clutch-test--with-conn admin
    (let* ((mysql (eq clutch-test-backend 'mysql))
           (table (format "clutch_outside_%d" (emacs-pid)))
           (params (append (list :backend clutch-test-backend)
                           (clutch-test--live-connect-params)))
           (clutch-connection-alist
            (list (cons "same" params)
                  (cons "other" (append params '(:connect-timeout 7)))))
           (result-name (format " *clutch-outside-result-%d*" (emacs-pid)))
           (buffer (generate-new-buffer " *clutch-outside*"))
           repl)
      (cl-flet ((connect (name)
                  (cl-letf (((symbol-function 'completing-read)
                             (lambda (&rest _) name)))
                    (clutch-connect)))
                (manual ()
                  (unless (clutch-db-manual-commit-p clutch-connection)
                    (clutch-toggle-auto-commit))))
        (unwind-protect
            (progn
              (clutch-db-query
               admin (format "CREATE TABLE %s (id int PRIMARY KEY, v %s)"
                             table (if mysql "varchar(20)" "text")))
              (clutch-db-query admin (format "INSERT INTO %s VALUES (1, 'a')" table))
              (cl-letf (((symbol-function 'message) #'ignore))
                (with-current-buffer buffer
                  (clutch-mode)
                  (connect "same")
                  (clutch-test--with-live-result-buffer result-name
                    (pcase-dolist (`(,label ,lose-session) '(("live" nil) ("lost" t)))
                      (ert-info (label)
                        (clutch-test--run-in-console (format "SELECT id, v FROM %s" table))
                        (manual)
                        (let ((old (if lose-session
                                       (clutch-test--end-console-session admin)
                                     clutch-connection)))
                          (connect "same")
                          (should-not (eq clutch-connection old))
                          (should (clutch-db-manual-commit-p clutch-connection))
                          (let ((conn clutch-connection))
                            (with-current-buffer result-name
                              (should (eq clutch-connection conn))
                              (clutch-result-rerun)
                              (clutch-test--await-queries)
                              (should (eq clutch-connection conn)))))))
                    (clutch-rollback)
                    (connect "other")
                    (should-not (clutch-db-manual-commit-p clutch-connection))
                    (should-not (buffer-local-value 'clutch-connection
                                                    (get-buffer result-name)))))
                (setq repl (generate-new-buffer " *clutch-outside-repl*"))
                (with-current-buffer repl
                  (clutch-repl-mode)
                  (connect "same")
                  (manual)
                  (connect "same")
                  (should (clutch-db-manual-commit-p clutch-connection))
                  (clutch-toggle-auto-commit))))
          (dolist (b (list buffer repl))
            (when (buffer-live-p b)
              (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                (kill-buffer b))))
          (ignore-errors
            (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table))))))))

(ert-deftest clutch-test-live-connect-in-an-indirect-edit-keeps-its-console ()
  "`C-c C-e' in an indirect edit should move or leave its console's session.
Picking the connection the edit shares with its console disconnected the
console and connected the edit alone, in Auto mode, and so did picking
another.  The session now moves, console and all, in its mode, and
connecting the edit elsewhere leaves the console where it was.  Leaving
the edit then ends the session it connected, which stayed open on the
server, and an edit whose connection failed still leaves the console's."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL"))
  (clutch-test--with-conn admin
    (let* ((params (append (list :backend clutch-test-backend)
                           (clutch-test--live-connect-params)))
           (clutch-connection-alist
            (list (cons "same" params)
                  (cons "other" (append params '(:connect-timeout 7)))
                  (cons "broken" (plist-put (copy-sequence params) :port 1))))
           (clutch-console-directory (make-temp-file "clutch-console-" t))
           console indirect)
      (cl-flet ((connect (name)
                  (cl-letf (((symbol-function 'completing-read)
                             (lambda (&rest _) name)))
                    (clutch-connect))))
        (unwind-protect
            (cl-letf (((symbol-function 'message) #'ignore))
              (clutch-query-console "same")
              (setq console (current-buffer))
              (clutch-toggle-auto-commit)
              (insert "SELECT 1")
              (clutch-edit-indirect)
              (setq indirect (current-buffer))
              (let ((old clutch-connection))
                (connect "same")
                (should-not (eq clutch-connection old))
                (should (eq (buffer-local-value 'clutch-connection console)
                            clutch-connection))
                (should (clutch-db-manual-commit-p clutch-connection)))
              (let ((shared clutch-connection))
                (connect "other")
                (should-not (eq clutch-connection shared))
                (should (eq (buffer-local-value 'clutch-connection console) shared))
                (should (clutch--connection-alive-p shared))
                (let ((own (clutch-test--server-session-id clutch-connection)))
                  (clutch-indirect-abort)
                  (should-not (buffer-live-p indirect))
                  (clutch-test--await-session-end admin own))
                (should (clutch--connection-alive-p shared))
                (pop-to-buffer console)
                (clutch-edit-indirect)
                (setq indirect (current-buffer))
                (should-error (connect "broken"))
                (clutch-indirect-abort)
                (should (eq (buffer-local-value 'clutch-connection console) shared))
                (should (clutch--connection-alive-p shared))))
          (when (buffer-live-p indirect)
            (cl-letf (((symbol-function 'yes-or-no-p) #'always))
              (kill-buffer indirect)))
          (when (buffer-live-p console)
            (cl-letf (((symbol-function 'yes-or-no-p) #'always))
              (kill-buffer console)))
          (delete-directory clutch-console-directory t))))))

(ert-deftest clutch-test-live-indirect-edit-ends-a-session-of-its-own ()
  "`C-c C-e' in an indirect edit should end a session the edit connected.
Connecting the edit elsewhere a second time asked nothing and left the
session it had connected open on the server, its uncommitted work and
all.  A connection that fails, or the question answered no, leaves that
session to the edit."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL"))
  (clutch-test--with-conn admin
    (let* ((params (append (list :backend clutch-test-backend)
                           (clutch-test--live-connect-params)))
           (clutch-connection-alist
            (list (cons "same" params)
                  (cons "other" (append params '(:connect-timeout 7)))
                  (cons "third" (append params '(:connect-timeout 8)))
                  (cons "broken" (plist-put (copy-sequence params) :port 1))))
           (clutch-console-directory (make-temp-file "clutch-console-" t))
           console indirect)
      (cl-flet ((connect (name answer)
                  (let (asked)
                    (cl-letf (((symbol-function 'completing-read)
                               (lambda (&rest _) name))
                              ((symbol-function 'yes-or-no-p)
                               (lambda (prompt) (setq asked prompt) answer)))
                      (clutch-connect))
                    asked)))
        (unwind-protect
            (cl-letf (((symbol-function 'message) #'ignore))
              (clutch-query-console "same")
              (setq console (current-buffer))
              (insert "SELECT 1")
              (clutch-edit-indirect)
              (setq indirect (current-buffer))
              (connect "other" t)
              (clutch-toggle-auto-commit)
              (clutch-test--run-in-console
               "CREATE TEMPORARY TABLE clutch_own_session (v INT)"
               "INSERT INTO clutch_own_session VALUES (1)")
              (let* ((own clutch-connection)
                     (id (clutch-test--server-session-id own)))
                (should (clutch--tx-dirty-p own))
                (should-error (connect "broken" t))
                (should-error (connect "third" nil) :type 'user-error)
                (should (eq clutch-connection own))
                (should (clutch--tx-dirty-p own))
                (should (equal (clutch-test--server-session-count admin id) 1))
                (should (string-match-p "Uncommitted changes will be lost"
                                        (or (connect "third" t) "")))
                (should-not (eq clutch-connection own))
                (clutch-test--await-session-end admin id))
              (should (clutch--connection-alive-p
                       (buffer-local-value 'clutch-connection console))))
          (when (buffer-live-p indirect)
            (cl-letf (((symbol-function 'yes-or-no-p) #'always))
              (kill-buffer indirect)))
          (when (buffer-live-p console)
            (cl-letf (((symbol-function 'yes-or-no-p) #'always))
              (kill-buffer console)))
          (delete-directory clutch-console-directory t))))))

(ert-deftest clutch-test-live-indirect-edit-owns-a-session-quit-while-connecting ()
  "An indirect edit should own a connection bound before its setup was quit.
A quit while the new connection loaded its metadata left the edit holding
it as its console's session, with the console's target: picking that
connection again connected the edit alone, in Auto mode, and leaving the
edit left the connection open on the server."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL"))
  (clutch-test--with-conn admin
    (let* ((params (append (list :backend clutch-test-backend)
                           (clutch-test--live-connect-params)))
           (clutch-connection-alist
            (list (cons "same" params)
                  (cons "other" (append params '(:connect-timeout 7)))))
           (clutch-console-directory (make-temp-file "clutch-console-" t))
           console indirect)
      (cl-flet ((connect (name)
                  (cl-letf (((symbol-function 'completing-read)
                             (lambda (&rest _) name)))
                    (clutch-connect))))
        (unwind-protect
            (cl-letf (((symbol-function 'message) #'ignore))
              (clutch-query-console "same")
              (setq console (current-buffer))
              (insert "SELECT 1")
              (clutch-edit-indirect)
              (setq indirect (current-buffer))
              (let ((shared clutch-connection))
                ;; C-g while the new connection loads its metadata.
                (should (eq (condition-case nil
                                (cl-letf (((symbol-function 'clutch--prime-schema-cache)
                                           (lambda (_conn) (signal 'quit nil))))
                                  (connect "other"))
                              (quit 'quit))
                            'quit))
                (should-not (eq clutch-connection shared))
                (clutch-toggle-auto-commit)
                (let ((own clutch-connection))
                  (connect "other")
                  (should-not (eq clutch-connection own))
                  (should (clutch-db-manual-commit-p clutch-connection)))
                (let ((id (clutch-test--server-session-id clutch-connection)))
                  (clutch-indirect-abort)
                  (should-not (buffer-live-p indirect))
                  (clutch-test--await-session-end admin id))
                (should (clutch--connection-alive-p shared))))
          (dolist (buffer (list indirect console))
            (when (buffer-live-p buffer)
              (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                (kill-buffer buffer))))
          (delete-directory clutch-console-directory t))))))

(ert-deftest clutch-test-live-indirect-edit-runs-again-in-a-session-of-its-own ()
  "`C-c \='' should run SQL again in a session the indirect edit connected.
The edit ran its SQL in any other buffer that held its connection, which
after a first run was the edit's own result buffer: running again killed
the edit and sent the SQL there.  The edit is buried and runs it."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL"))
  (let* ((params (append (list :backend clutch-test-backend)
                         (clutch-test--live-connect-params)))
         (clutch-connection-alist
          (list (cons "same" params)
                (cons "other" (append params '(:connect-timeout 7)))))
         (clutch-console-directory (make-temp-file "clutch-console-" t))
         (result-name "*clutch-test-own-session-result*")
         console indirect)
    (clutch-test--with-live-result-buffer result-name
      (unwind-protect
          (cl-letf (((symbol-function 'message) #'ignore))
            (clutch-query-console "same")
            (setq console (current-buffer))
            (insert "SELECT 1")
            (clutch-edit-indirect)
            (setq indirect (current-buffer))
            (cl-letf (((symbol-function 'completing-read)
                       (lambda (&rest _) "other")))
              (clutch-connect))
            (let ((own clutch-connection))
              (dolist (sql '("SELECT 1 AS one" "SELECT 2 AS two"))
                (pop-to-buffer indirect)
                (erase-buffer)
                (insert sql)
                (clutch-indirect-execute)
                (clutch-test--await-queries)
                (should (buffer-live-p indirect))
                (should (eq (buffer-local-value 'clutch-connection indirect) own))
                (should (clutch--connection-alive-p own)))
              (should (equal (buffer-local-value 'clutch--result-columns
                                                 (get-buffer result-name))
                             '("two")))))
        (dolist (buffer (list indirect console))
          (when (buffer-live-p buffer)
            (cl-letf (((symbol-function 'yes-or-no-p) #'always))
              (kill-buffer buffer))))
        (delete-directory clutch-console-directory t)))))

(ert-deftest clutch-test-live-nested-indirect-edit-leaves-its-parent-session ()
  "An indirect edit opened from one with a session of its own borrows it.
Leaving the inner edit keeps that session.  Once the outer edit moved it
to a new connection, leaving the outer edit ends that connection, which
an edit that had connected left open on the server."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL"))
  (clutch-test--with-conn admin
    (let* ((params (append (list :backend clutch-test-backend)
                           (clutch-test--live-connect-params)))
           (clutch-connection-alist
            (list (cons "same" params)
                  (cons "other" (append params '(:connect-timeout 7)))))
           (clutch-console-directory (make-temp-file "clutch-console-" t))
           console outer inner)
      (cl-flet ((connect (name)
                  (cl-letf (((symbol-function 'completing-read)
                             (lambda (&rest _) name)))
                    (clutch-connect))))
        (unwind-protect
            (cl-letf (((symbol-function 'message) #'ignore))
              (clutch-query-console "same")
              (setq console (current-buffer))
              (insert "SELECT 1")
              (clutch-edit-indirect)
              (setq outer (current-buffer))
              (connect "other")
              (let ((own clutch-connection))
                (clutch-edit-indirect)
                (setq inner (current-buffer))
                (should (eq clutch-connection own))
                (clutch-indirect-abort)
                (should-not (buffer-live-p inner))
                (should (clutch--connection-alive-p own))
                (pop-to-buffer outer)
                (let ((id (clutch-test--server-session-id own)))
                  (connect "other")
                  (should-not (eq clutch-connection own))
                  (clutch-test--await-session-end admin id)))
              (let ((id (clutch-test--server-session-id clutch-connection)))
                (clutch-indirect-abort)
                (should-not (buffer-live-p outer))
                (clutch-test--await-session-end admin id))
              (should (clutch--connection-alive-p
                       (buffer-local-value 'clutch-connection console))))
          (dolist (buffer (list inner outer console))
            (when (buffer-live-p buffer)
              (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                (kill-buffer buffer))))
          (delete-directory clutch-console-directory t))))))

(ert-deftest clutch-test-live-picking-a-moved-console-returns-to-it ()
  "Picking a console that followed a namespace switch should return to it.
An ad hoc console that followed a typed USE or SET search_path was named
by the parameters that followed it, so picking it in `clutch-query-console'
opened a second console on a second connection.  Returning to it, live or
after its session was lost, replaced the parameters `C-c C-e' connects
with by those."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL USE and PostgreSQL SET search_path"))
  (clutch-test--with-conn admin
    (let* ((mysql (eq clutch-test-backend 'mysql))
           (namespace (format "clutch_pick_%d" (emacs-pid)))
           (params (append (list :backend clutch-test-backend)
                           (clutch-test--live-connect-params)))
           console picked)
      (unwind-protect
          (progn
            (clutch-db-query admin (format (if mysql
                                               "CREATE DATABASE %s"
                                             "CREATE SCHEMA %s")
                                           namespace))
            (clutch-test--with-live-console params
              (setq console (current-buffer))
              (let ((opened-with clutch--console-ad-hoc-params))
                (clutch-test--run-in-console
                 (format (if mysql "USE %s" "SET search_path TO %s") namespace))
                (should-not (equal clutch--connection-params opened-with))
                (pcase-dolist (`(,label ,lose-session) '(("live" nil) ("lost" t)))
                  (ert-info (label)
                    (let ((conn clutch-connection))
                      (when lose-session
                        (clutch-test--end-console-session admin))
                      (cl-letf (((symbol-function 'completing-read)
                                 (lambda (_prompt collection &rest _args)
                                   (should (member (buffer-name console) collection))
                                   (buffer-name console))))
                        (call-interactively #'clutch-query-console))
                      (setq picked (current-buffer))
                      (should (eq picked console))
                      (should (eq (not (eq clutch-connection conn)) lose-session))
                      (should (clutch--connection-alive-p clutch-connection))
                      (should (equal clutch--console-ad-hoc-params opened-with))))))))
        (when (and (buffer-live-p picked) (not (eq picked console)))
          (cl-letf (((symbol-function 'yes-or-no-p) #'always))
            (kill-buffer picked)))
        (ignore-errors
          (clutch-db-query admin (format (if mysql
                                             "DROP DATABASE IF EXISTS %s"
                                           "DROP SCHEMA IF EXISTS %s CASCADE")
                                         namespace)))))))

(ert-deftest clutch-test-live-indirect-edit-reconnects-its-session ()
  "An indirect edit opened outside Clutch should reconnect as its console does.
It held its console's connection but none of its parameters, so after a
typed USE or SET search_path it held only the namespace, and once the
session was lost, running SQL in it failed with \"Connection params
require :backend\"."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL USE and PostgreSQL SET search_path"))
  (clutch-test--with-conn admin
    (let* ((mysql (eq clutch-test-backend 'mysql))
           (namespace (format "clutch_indirect_%d" (emacs-pid)))
           (params (append (list :backend clutch-test-backend)
                           (clutch-test--live-connect-params)))
           (result-name (format " *clutch-indirect-result-%d*" (emacs-pid)))
           indirect)
      (unwind-protect
          (progn
            (clutch-db-query admin (format (if mysql
                                               "CREATE DATABASE %s"
                                             "CREATE SCHEMA %s")
                                           namespace))
            (clutch-test--with-live-console params
              (clutch-test--with-live-result-buffer result-name
               (let ((console (current-buffer)))
                (with-temp-buffer
                  (insert "SELECT 1")
                  (clutch-edit-indirect)
                  (setq indirect (current-buffer)))
                (with-current-buffer console
                  (clutch-test--run-in-console
                   (format (if mysql "USE %s" "SET search_path TO %s") namespace))
                  (let ((lost (clutch-test--end-console-session admin)))
                    (with-current-buffer indirect
                      (clutch-test--run-in-console "SELECT 1")
                      (should-not (eq clutch-connection lost))
                      (should (clutch--connection-alive-p clutch-connection))
                      (should (equal (clutch-db-current-schema clutch-connection)
                                     namespace)))
                    (should (eq clutch-connection
                                (buffer-local-value 'clutch-connection indirect)))))))))
        (when (buffer-live-p indirect)
          (kill-buffer indirect))
        (ignore-errors
          (clutch-db-query admin (format (if mysql
                                             "DROP DATABASE IF EXISTS %s"
                                           "DROP SCHEMA IF EXISTS %s CASCADE")
                                         namespace)))))))

(ert-deftest clutch-test-live-result-commands-after-the-session-ends ()
  "A result's commands should say so once its session has ended.
After `clutch-disconnect', refreshing, paging, counting, sorting and
filtering a result, editing a cell, showing a column's details,
previewing its SQL and copying rows as INSERT or UPDATE statements
failed with `cl-no-applicable-method'.  A result whose session was lost
still previews its SQL and copies rows as INSERT statements without
reconnecting."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL results"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_ended_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params)))
          (result-name (format " *clutch-ended-result-%d*" (emacs-pid))))
      (cl-flet ((call-in-result (command)
                  (with-current-buffer result-name
                    (set-window-buffer (selected-window) (current-buffer))
                    (clutch--goto-cell 0 1)
                    (cl-letf (((symbol-function 'completing-read)
                               (lambda (&rest _) "id = 1"))
                              ((symbol-function 'read-string)
                               (lambda (&rest _) "id = 1")))
                      (let ((value (call-interactively command)))
                        (when (and (bufferp value)
                                   (not (eq value (current-buffer))))
                          (kill-buffer value)))))))
        (unwind-protect
            (progn
              (clutch-db-query
               admin (format "CREATE TABLE %s (id int PRIMARY KEY, v varchar(20))" table))
              (clutch-db-query admin (format "INSERT INTO %s VALUES (1, 'a'), (2, 'b')"
                                             table))
              (clutch-test--with-live-console params
                (clutch-test--with-live-result-buffer result-name
                  (clutch-test--run-in-console (format "SELECT id, v FROM %s" table))
                  (let ((lost (clutch-test--end-console-session admin)))
                    (dolist (command '(clutch-result-copy-insert
                                       clutch-preview-execution-sql))
                      (ert-info ((format "lost: %s" command))
                        (call-in-result command)
                        (should (eq (buffer-local-value 'clutch-connection
                                                        (get-buffer result-name))
                                    lost)))))
                  (clutch-test--run-in-console (format "SELECT id, v FROM %s" table))
                  (clutch-disconnect)
                  (dolist (command '(clutch-result-rerun clutch-result-last-page
                                     clutch-result-count-total
                                     clutch-result-sort-by-column
                                     clutch-result-apply-filter clutch-result-edit-cell
                                     clutch-result-column-info
                                     clutch-preview-execution-sql
                                     clutch-result-copy-insert
                                     clutch-result-copy-update))
                    (ert-info ((format "ended: %s" command))
                      (should (string-match-p
                               "Connection closed"
                               (error-message-string
                                (should-error (call-in-result command)
                                              :type 'user-error)))))))))
          (when-let* ((preview (get-buffer "*clutch-preview*")))
            (kill-buffer preview))
          (ignore-errors
            (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table))))))))

(ert-deftest clutch-test-live-mysql-result-refuses-writes-after-a-schema-switch ()
  "A MySQL result should refuse to submit once the console switched database.
An edit staged in a result of database A, submitted after
`clutch-switch-schema' to B, updated the table of the same name in B.
Running the query again shows B's rows, which may then be edited."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'mysql)
    (ert-skip "This regression covers MySQL's current database"))
  (clutch-test--with-conn admin
    (let* ((a (format "clutch_result_a_%d" (emacs-pid)))
           (b (format "clutch_result_b_%d" (emacs-pid)))
           (params (append (list :backend 'mysql :database a)
                           (clutch-test--live-connect-params)))
           (result-name (format " *clutch-result-moved-%d*" (emacs-pid))))
      (cl-flet ((value (database)
                  (caar (clutch-db-result-rows
                         (clutch-db-query
                          admin (format "SELECT v FROM %s.t WHERE id = 1" database))))))
        (unwind-protect
            (progn
              (dolist (database (list a b))
                (clutch-db-query admin (format "CREATE DATABASE %s" database))
                (clutch-db-query
                 admin (format "CREATE TABLE %s.t (id int PRIMARY KEY, v varchar(20))"
                               database))
                (clutch-db-query
                 admin (format "INSERT INTO %s.t VALUES (1, 'orig')" database)))
              (clutch-test--with-live-console params
                (clutch-test--with-live-result-buffer result-name
                  (clutch-test--run-in-console "SELECT id, v FROM t")
                  (with-current-buffer result-name
                    (clutch-test--xtdb-edit 0 "v" "edited"))
                  (cl-letf (((symbol-function 'completing-read)
                             (lambda (&rest _) b)))
                    (clutch-switch-schema))
                  (with-current-buffer result-name
                    (should (string-match-p
                             "run the query again"
                             (error-message-string
                              (should-error (clutch-test--xtdb-submit)
                                            :type 'user-error))))
                    (should clutch--pending-edits)
                    (should-error (clutch-result-count-total) :type 'user-error)
                    (should (equal (list (value a) (value b)) '("orig" "orig")))
                    (cl-letf (((symbol-function 'yes-or-no-p) #'always))
                      (clutch-result-rerun)
                      (clutch-test--await-queries))
                    (clutch-test--xtdb-edit 0 "v" "edited")
                    (clutch-test--xtdb-submit)
                    (should (equal (list (value a) (value b)) '("orig" "edited")))))))
          (dolist (database (list a b))
            (ignore-errors
              (clutch-db-query admin (format "DROP DATABASE IF EXISTS %s" database)))))))))

(ert-deftest clutch-test-live-mysql-old-result-refuses-writes-after-a-typed-use ()
  "A MySQL result left behind by a typed USE should refuse to submit.
Result buffers are named by database, so after `USE b' the result of
database A stays in its own buffer, and an edit staged there and
submitted updated B's table of the same name."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'mysql)
    (ert-skip "This regression covers MySQL's current database"))
  (clutch-test--with-conn admin
    (let* ((a (format "clutch_typed_a_%d" (emacs-pid)))
           (b (format "clutch_typed_b_%d" (emacs-pid)))
           (params (append (list :backend 'mysql :database a)
                           (clutch-test--live-connect-params)))
           results)
      (cl-flet ((value (database)
                  (caar (clutch-db-result-rows
                         (clutch-db-query
                          admin (format "SELECT v FROM %s.t WHERE id = 1" database))))))
        (unwind-protect
            (progn
              (dolist (database (list a b))
                (clutch-db-query admin (format "CREATE DATABASE %s" database))
                (clutch-db-query
                 admin (format "CREATE TABLE %s.t (id int PRIMARY KEY, v varchar(20))"
                               database))
                (clutch-db-query
                 admin (format "INSERT INTO %s.t VALUES (1, 'orig')" database)))
              (clutch-test--with-live-console params
                (clutch-test--run-in-console "SELECT id, v FROM t")
                (push (get-buffer (clutch-result--buffer-name)) results)
                (clutch-test--run-in-console (format "USE %s" b))
                (push (get-buffer (clutch-result--buffer-name)) results)
                (should-not (eq (car results) (cadr results)))
                (with-current-buffer (cadr results)
                  (should-error (clutch-test--xtdb-edit 0 "v" "edited")
                                :type 'user-error))
                (should (equal (list (value a) (value b)) '("orig" "orig")))))
          (dolist (buffer results)
            (when (buffer-live-p buffer)
              (kill-buffer buffer)))
          (dolist (database (list a b))
            (ignore-errors
              (clutch-db-query admin (format "DROP DATABASE IF EXISTS %s" database)))))))))

(ert-deftest clutch-test-live-pg-result-refuses-writes-after-a-reconnect-moved-it ()
  "A PostgreSQL result should refuse to submit once a reconnect moved its path.
In Manual mode, with s1, s3 committed and s1, s2 set in the open
transaction, a result of t shows s2's row.  After the session was lost,
the next statement in the console reconnected on s1, s3, and the edit
staged in the result updated s3.t, though `current_schema()' was s1
throughout."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "This regression covers the PostgreSQL search_path"))
  (clutch-test--with-conn admin
    (let* ((s1 (format "clutch_path1_%d" (emacs-pid)))
           (s2 (format "clutch_path2_%d" (emacs-pid)))
           (s3 (format "clutch_path3_%d" (emacs-pid)))
           (params (append (list :backend 'pg) (clutch-test--live-connect-params)))
           (result-name (format " *clutch-result-path-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query admin (format "CREATE SCHEMA %s" s1))
            (dolist (schema (list s2 s3))
              (clutch-db-query admin (format "CREATE SCHEMA %s" schema))
              (clutch-db-query
               admin (format "CREATE TABLE %s.t (id int PRIMARY KEY, v text)" schema))
              (clutch-db-query
               admin (format "INSERT INTO %s.t VALUES (1, 'orig')" schema)))
            (dolist (action '(console submit edit copy))
              (clutch-test--with-live-console params
                (clutch-toggle-auto-commit)
                (clutch-test--run-in-console (format "SET search_path TO %s, %s" s1 s3))
                (clutch-commit)
                (clutch-test--run-in-console (format "SET search_path TO %s, %s" s1 s2))
                (clutch-test--with-live-result-buffer result-name
                  (clutch-test--run-in-console "SELECT id, v FROM t")
                  (with-current-buffer result-name
                    (clutch-test--xtdb-edit 0 "v" "edited"))
                  (let ((lost (clutch-test--end-console-session admin)))
                    (when (eq action 'console)
                      ;; The statement reconnects before it asks to discard
                      ;; the staged edit, which is kept.
                      (cl-letf (((symbol-function 'yes-or-no-p) #'ignore))
                        (should-error (clutch-test--run-in-console "SELECT 1")
                                      :type 'user-error)))
                    (with-current-buffer result-name
                      (ert-info ((format "first action: %s" action))
                        (should (string-match-p
                                 "run the query again"
                                 (error-message-string
                                  (should-error
                                   (pcase action
                                     ('edit
                                      (clutch--goto-cell 0 1)
                                      (clutch-result-edit-cell))
                                     ('copy (clutch-result-copy 'update '((0) 1)))
                                     (_ (clutch-test--xtdb-submit)))
                                   :type 'user-error)))))
                      (should clutch--pending-edits)
                      (should-not (eq clutch-connection lost))))
                  (dolist (schema (list s2 s3))
                    (should (equal (caar (clutch-db-result-rows
                                          (clutch-db-query
                                           clutch-connection
                                           (format "SELECT v FROM %s.t WHERE id = 1"
                                                   schema))))
                                   "orig"))))
                (clutch-rollback)
                (clutch-toggle-auto-commit))))
        (dolist (schema (list s1 s2 s3))
          (ignore-errors
            (clutch-db-query admin (format "DROP SCHEMA IF EXISTS %s CASCADE" schema))))))))

(ert-deftest clutch-test-live-duckdb-namespace-entrypoint ()
  "The public command should switch DuckDB schemas in the current catalog."
  :tags '(:clutch-live :duckdb-live)
  (unless (eq (clutch-test-live-backend-id) 'duckdb)
    (ert-skip "Live backend is not DuckDB"))
  (let* ((params (append
                  (list :backend 'jdbc
                        :driver-class "org.duckdb.DuckDBDriver"
                        :display-name "DuckDB")
                  (clutch-test--live-connect-params)))
         (conn (clutch-db-connect 'jdbc params))
         original original-catalog)
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (setq-local clutch-connection conn
                      clutch--connection-params params)
          (setq original (clutch-db-current-schema conn)
                original-catalog
                (caar (clutch-db-result-rows
                       (clutch-db-query
                        conn "SELECT current_catalog(), current_schema()"))))
          (should (member original (clutch-db-list-schemas conn)))
          (clutch-db-query conn "CREATE SCHEMA \"odd.schema\"")
          (unwind-protect
              (progn
                (cl-letf (((symbol-function 'completing-read)
                           (lambda (prompt collection &rest _args)
                             (should (string-prefix-p
                                      "Switch schema/database" prompt))
                             (should (member "odd.schema" collection))
                             "odd.schema")))
                  (clutch-switch-schema))
                (should (eq clutch-connection conn))
                (should (equal (plist-get clutch--connection-params :catalog)
                               original-catalog))
                (should (equal (plist-get clutch--connection-params :schema)
                               "odd.schema"))
                (clutch-db-query conn
                                 "CREATE TABLE namespace_probe (id INTEGER)")
                (should
                 (cl-find "namespace_probe"
                          (clutch-db-list-table-entries conn)
                          :key (lambda (entry) (plist-get entry :name))
                          :test #'string=)))
            (when (clutch-db-live-p conn)
              (ignore-errors (clutch-db-set-current-schema conn original))
              (ignore-errors
                (clutch-db-query conn "DROP SCHEMA \"odd.schema\" CASCADE")))))
      (ignore-errors (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-live-schema-introspection ()
  "Test schema introspection functions."
  :tags '(:clutch-live)
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_schema_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string)))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
	      (clutch-db-query conn create-sql)
	      (let ((tables (clutch-db-list-tables conn)))
	        (should (listp tables))
	        (should (clutch-test--live-name-member-p table tables)))
	      (let ((columns (clutch-db-list-columns conn table)))
	        (should (listp columns))
	        (should (clutch-test--live-name-member-p "id" columns))
	        (should (clutch-test--live-name-member-p "name" columns)))
	      (let ((pk-cols (clutch-db-primary-key-columns conn table)))
	        (unless (clutch-test--clickhouse-live-p)
	          (should (equal (mapcar #'downcase pk-cols) '("id"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-completion-does-not-cache-unknown-table ()
  "Deferred native completion should not cache a parsed nonexistent table."
  :tags '(:clutch-live :native-columns-live)
  (clutch-test--with-conn conn
    (unless (memq clutch-test-backend '(pg mysql))
      (ert-skip "Live backend is not a deferred native SQL adapter"))
    (should (clutch-db-completion-deferred-columns-p conn))
    (clutch-test--with-isolated-metadata-caches
      (let ((schema (make-hash-table :test 'equal))
            (table (format "clutch_missing_%d" (emacs-pid)))
            (missing (make-symbol "missing"))
            scheduled)
        (dolist (known-table (clutch-db-list-tables conn))
          (puthash known-table nil schema))
        (puthash conn schema clutch--schema-cache)
        (puthash conn (list :state 'ready) clutch--schema-status-cache)
        (cl-letf (((symbol-function 'run-with-idle-timer)
                   (lambda (_secs _repeat fn &rest args)
                     (setq scheduled (cons fn args))
                     'fake-timer)))
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection conn)
            (insert (format "select * from %s where nam" table))
            (goto-char (point-min))
            (search-forward "nam")
            (completion-at-point)))
        (when scheduled
          (apply (car scheduled) (cdr scheduled)))
        (should-not scheduled)
        (should (eq (gethash table schema missing) missing))))))

(ert-deftest clutch-test-live-object-describe-uses-real-table-and-index-metadata ()
  "Object describe should render real table/index metadata from the backend."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :object-describe)
    (ert-skip (clutch-test-capability-skip-message :object-describe)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_obj_desc_%d" (emacs-pid)))
           (index (format "idx_clutch_obj_desc_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (index-sql
            (format "CREATE INDEX %s ON %s (name)" index table)))
      (let ((clutch--object-cache (make-hash-table :test 'eq))
            (clutch--object-warmup-timers (make-hash-table :test 'eq))
            (clutch--object-warmup-generations (make-hash-table :test 'eq))
            (clutch--table-metadata-cache (make-hash-table :test 'eq)))
        (unwind-protect
            (progn
              (clutch-db-query conn drop-sql)
              (clutch-db-query conn create-sql)
              (clutch-db-query conn index-sql)
              (let* ((table-entry
                      (cl-find table
                               (clutch-db-list-table-entries conn)
                               :key (lambda (entry) (plist-get entry :name))
                               :test #'string=))
                     (_warmed-indexes
                      (clutch--object-type-entries conn "INDEX" t))
                     (index-entry
                      (cl-find index
                               (clutch-db-list-objects conn 'indexes)
                               :key (lambda (entry) (plist-get entry :name))
                               :test #'string=)))
                (should table-entry)
                (should index-entry)
                (let ((text (clutch--object-describe-text conn table-entry)))
                  (should (string-match-p (regexp-quote table) text))
                  (should (string-match-p "^Columns (2)$" text))
                  (should (string-match-p "^  id\\_>" text))
                  (should (string-match-p "^  name\\_>" text))
                  (should (string-match-p (regexp-quote index) text)))
                (let ((text (clutch--object-describe-text conn index-entry)))
                  (should (string-match-p (regexp-quote index) text))
                  (should (string-match-p "^Columns (1)$" text))
                  (should (string-match-p "^  name\\_>" text)))))
          (ignore-errors (clutch-db-query conn drop-sql)))))))

(ert-deftest clutch-test-live-paged-sql-building ()
  "Test paged SQL query building."
  :tags '(:clutch-live)
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_paged_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (insert-sql
            (format "INSERT INTO %s (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
                    table))
           (base-sql (format "SELECT id, name FROM %s" table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
	      (let* ((paged (clutch-db-build-paged-sql conn base-sql 0 2))
	             (rows (clutch-db-result-rows
	                    (clutch-db-query conn paged))))
	        (let ((paged-upper (upcase paged)))
	          (should (or (string-match-p "LIMIT" paged-upper)
	                      (string-match-p "OFFSET" paged-upper)
	                      (string-match-p "ROWNUM" paged-upper)
	                      (string-match-p "FETCH" paged-upper))))
	        (should (= (length rows) 2)))
	      (let* ((sort-column
                      (if (clutch-test-live-backend-capability-p
                           :uppercase-identifiers)
                          "ID"
                        "id"))
	             (paged (clutch-db-build-paged-sql
	                     conn base-sql 0 2 (cons sort-column "DESC")))
	             (rows (clutch-db-result-rows
	                    (clutch-db-query conn paged))))
	        (should (string-match-p "ORDER BY" paged))
	        (should (equal (mapcar (lambda (row)
	                                 (list (format "%s" (car row)) (cadr row)))
	                               rows)
	                       '(("3" "c") ("2" "b"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-query-export-to-file ()
  "Export a selected SELECT or CTE directly without displaying a result grid."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This direct-export fixture uses native MySQL/PostgreSQL SQL"))
  (clutch-test--with-conn conn
    (let ((table (format "clutch_query_export_%d" (emacs-pid)))
          (path (make-temp-file "clutch-query-export-live-")))
      (unwind-protect
          (progn
            (clutch-db-query conn (format "CREATE TABLE %s(id INT PRIMARY KEY, body VARCHAR(64))" table))
            (clutch-db-query conn (format "INSERT INTO %s VALUES (1, '中文,a'), (2, 'b'), (3, '')" table))
            (dolist (sql (append (list (format "SELECT id, body FROM %s ORDER BY id" table))
                                 (when (clutch-test--live-supports-with-p conn)
                                   (list (format "WITH c AS (SELECT * FROM %s) SELECT * FROM c ORDER BY id" table)))))
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn)
                (insert "SELECT 'outside';\n")
                (let ((beg (point))
                      (clutch-export-page-size 2))
                  (insert sql)
                  (set-mark beg)
                  (setq mark-active t transient-mark-mode t)
                  (write-region "original" nil path nil 'silent)
                  (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                            ((symbol-function 'yes-or-no-p) (lambda (&rest _) t))
                            ((symbol-function 'clutch-result--display-select)
                             (lambda (&rest _) (ert-fail "Export displayed a grid"))))
                    (clutch-test--with-minibuffer-answers '("csv" "utf-8")
                      (call-interactively #'clutch-export-query))
                    (should (gethash conn clutch--running-queries))
                    (should (equal (with-temp-buffer (insert-file-contents path)
                                                     (buffer-string)) "original"))
                    (clutch-test--await-queries))
                  (should-not clutch--last-result-buffer)
                  (should-not clutch--execution-start-time)
                  (should-not (clutch-db--foreground-busy-p conn))
                  (should (equal (with-temp-buffer (insert-file-contents path)
                                                   (buffer-string))
                                 "id,body\n1,\"中文,a\"\n2,b\n3,\n"))))))
        (clutch-db-query conn (format "DROP TABLE IF EXISTS %s" table))
        (delete-file path)))))

(defun clutch-test--query-export-type-cases ()
  "Return (NAME TYPE SQL CSV TSV) cases for a wide live export fixture.
Expected cells are literal export text, independent of the formatter."
  (let* ((oracle (eq clutch-test-backend 'oracle))
         (mssql (eq clutch-test-backend 'sqlserver))
         (mysql (eq clutch-test-backend 'mysql))
         (pg (eq clutch-test-backend 'pg))
         (varchar (cond (oracle "VARCHAR2(128)") (mssql "NVARCHAR(128)")
                        (t "VARCHAR(128)")))
         ;; Oracle LOBs create storage indexes; keep the heap fixture index-free.
         (text (cond (oracle "VARCHAR2(1024)") (mssql "NVARCHAR(MAX)") (t "TEXT")))
         (prefix (if mssql "N" "")))
    `((small ,(if oracle "NUMBER(5)" "SMALLINT") "-123" "-123" "-123")
      (integer ,(if oracle "NUMBER(10)" "INT") "123456" "123456" "123456")
      (big ,(if oracle "NUMBER(19)" "BIGINT") "9007199254740993"
           "9007199254740993" "9007199254740993")
      (decimal "DECIMAL(20,6)" "12345678901234.125000"
               ,(if oracle "12345678901234.125" "12345678901234.125000")
               ,(if oracle "12345678901234.125" "12345678901234.125000"))
      (floating ,(cond (oracle "BINARY_DOUBLE") (mssql "FLOAT")
                       (pg "DOUBLE PRECISION") (t "DOUBLE")) "1.25" "1.25" "1.25")
      (truth ,(cond (oracle "NUMBER(1)") (mssql "BIT") (t "BOOLEAN"))
             ,(if (or pg (eq clutch-test-backend 'jdbc)) "TRUE" "1")
             ,(if (or mysql oracle) "1" "true")
             ,(if (or mysql oracle) "1" "true"))
      (falsity ,(cond (oracle "NUMBER(1)") (mssql "BIT") (t "BOOLEAN"))
               ,(if (or pg (eq clutch-test-backend 'jdbc)) "FALSE" "0")
               ,(if (or mysql oracle) "0" "false")
               ,(if (or mysql oracle) "0" "false"))
      (day "DATE" ,(if oracle "DATE '2024-02-29'" "'2024-02-29'")
           ,(if oracle "2024-02-29 00:00:00" "2024-02-29")
           ,(if oracle "2024-02-29 00:00:00" "2024-02-29"))
      (clock ,(if oracle "VARCHAR2(8)" "TIME") "'12:34:56'" "12:34:56" "12:34:56")
      (stamp ,(cond (mysql "DATETIME") (mssql "DATETIME2(0)") (t "TIMESTAMP"))
             ,(if oracle "TIMESTAMP '2024-02-29 12:34:56'" "'2024-02-29 12:34:56'")
             "2024-02-29 12:34:56" "2024-02-29 12:34:56")
      (fixed "CHAR(3)" "'abc'" "abc" "abc")
      (unicode ,varchar ,(concat prefix "'中文🙂'") "中文🙂" "中文🙂")
      (quoted ,varchar ,(concat prefix "'a,b\"c'") "\"a,b\"\"c\"" "\"a,b\"\"c\"")
      (lines ,text ,(concat prefix "'line1\nline2'") "\"line1\nline2\"" "\"line1\nline2\"")
      (binary ,(cond (pg "BYTEA") (oracle "RAW(16)")
                     (mssql "VARBINARY(16)") (t "BLOB"))
              ,(cond (pg "decode('616263', 'hex')")
                     (oracle "HEXTORAW('616263')")
                     (mysql "0x616263") (mssql "0x616263")
                     (t "from_hex('616263')")) "616263" "616263")
      (json ,(cond (pg "JSONB") (mysql "JSON") (t text)) "'{\"ok\":true}'"
            "\"{\"\"ok\"\":true}\"" "\"{\"\"ok\"\":true}\"")
      (uuid ,(cond (pg "UUID") (mssql "UNIQUEIDENTIFIER") (t varchar))
            "'00000000-0000-0000-0000-000000000123'"
            "00000000-0000-0000-0000-000000000123"
            "00000000-0000-0000-0000-000000000123")
      (unsigned "DECIMAL(20,0)" "18446744073709551615"
                "18446744073709551615" "18446744073709551615")
      (null ,varchar "NULL" "NULL" "NULL")
      (empty ,varchar "''" ,(if oracle "NULL" "") ,(if oracle "NULL" "")))))

(ert-deftest clutch-test-live-query-export-wide-table-without-identity ()
  "Export 101 columns and 602 rows of varied types without a key or index."
  :tags '(:clutch-live)
  (unless (or (memq clutch-test-backend '(mysql pg oracle sqlserver))
              (and (eq clutch-test-backend 'jdbc) clutch-test-url
                   (string-prefix-p "jdbc:duckdb:" clutch-test-url)))
    (ert-skip "The wide fixture targets MySQL, PostgreSQL, Oracle, SQL Server and DuckDB"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_export_wide_%d" (emacs-pid)))
           (path (make-temp-file "clutch-export-wide-"))
           (cases (clutch-test--query-export-type-cases))
           (fields (cl-loop for group below 5 append
                            (cl-loop for case in cases collect
                                     (cons (format "c_%s_%d" (car case) group)
                                           (cdr case)))))
           (literals (mapconcat (lambda (field) (nth 2 field)) fields ", "))
           (select
            (format "SELECT seq_id, %s FROM %s ORDER BY seq_id"
                    (mapconcat
                     (lambda (field)
                       (let ((name (car field)))
                         (if (string-prefix-p "c_binary_" name)
                             (format "%s AS %s"
                                     (pcase clutch-test-backend
                                       ('pg (format "encode(%s, 'hex')" name))
                                       ('oracle (format "RAWTOHEX(%s)" name))
                                       ('sqlserver (format "CONVERT(VARCHAR(32), %s, 2)" name))
                                       (_ (format "HEX(%s)" name)))
                                     name)
                           name))) fields ", ")
                    table))
           (insert-sql
            (if (eq clutch-test-backend 'sqlserver)
                (format "WITH r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 601) INSERT INTO %s SELECT n, %s FROM r OPTION (MAXRECURSION 0)" table literals)
              (let ((row-source
                     (pcase clutch-test-backend
                       ('oracle "SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 601")
                       ('mysql
                        (let ((digits (mapconcat (lambda (n) (format "SELECT %d n" n))
                                                 (number-sequence 0 9) " UNION ALL ")))
                          (format "SELECT 1+a.n+10*b.n+100*c.n n FROM (%s) a CROSS JOIN (%s) b CROSS JOIN (%s) c WHERE a.n+10*b.n+100*c.n < 601"
                                  digits digits digits)))
                       (_ "SELECT generate_series n FROM generate_series(1,601)"))))
                (format "INSERT INTO %s SELECT n, %s FROM (%s) r" table literals row-source))))
           created)
      (unwind-protect
          (progn
            (clutch-db-query
             conn (format "CREATE TABLE %s (seq_id INT, %s)" table
                          (mapconcat (lambda (field) (format "%s %s" (car field) (nth 1 field)))
                                     fields ", ")))
            (setq created t)
            (clutch-db-query conn insert-sql)
            (clutch-db-query conn (format "INSERT INTO %s VALUES (602, %s)" table
                                          (mapconcat (lambda (_) "NULL") fields ", ")))
            (should-not (clutch-db-primary-key-columns conn table))
            (let* ((index-sql
                    (pcase clutch-test-backend
                      ('pg (format "SELECT COUNT(*) FROM pg_indexes WHERE schemaname=current_schema() AND tablename='%s'" table))
                      ('mysql (format "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='%s'" table))
                      ('oracle (format "SELECT COUNT(*) FROM user_indexes WHERE table_name=UPPER('%s')" table))
                      ('sqlserver (format "SELECT COUNT(*) FROM sys.indexes WHERE object_id=OBJECT_ID('%s') AND index_id>0" table))
                      (_ (format "SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name='%s'" table))))
                   (count (caar (clutch-db-result-rows (clutch-db-query conn index-sql)))))
              (should (= (string-to-number (format "%s" count)) 0)))
            (dolist (kind '(csv tsv))
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn)
                (insert select)
                (let ((clutch-result-max-rows 500)
                      (clutch-export-page-size 200)
                      (clutch-jdbc-fetch-size 50))
                  (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                            ((symbol-function 'yes-or-no-p) (lambda (&rest _) t))
                            ((symbol-function 'clutch-db-primary-key-columns)
                             (lambda (&rest _) (ert-fail "Export loaded row identity")))
                            ((symbol-function 'clutch-result--display-select)
                             (lambda (&rest _) (ert-fail "Export displayed a grid"))))
                    (clutch-test--with-minibuffer-answers (list (symbol-name kind) "utf-8-bom")
                      (call-interactively #'clutch-export-query))
                    (clutch-test--await-queries))
                  (let* ((delimiter (if (eq kind 'csv) "," "\t"))
                         (cells (mapconcat (lambda (field) (nth (if (eq kind 'csv) 3 4) field))
                                           fields delimiter))
                         (header (mapconcat #'clutch-test--live-column-name
                                            (cons "seq_id" (mapcar #'car fields)) delimiter))
                         (expected (concat header "\n"
                                           (mapconcat (lambda (id) (format "%d%s%s\n" id delimiter cells))
                                                      (number-sequence 1 601) "")
                                           "602" delimiter
                                           (mapconcat (lambda (_) "NULL") fields delimiter) "\n")))
                    (let* ((actual (with-temp-buffer (insert-file-contents path) (buffer-string)))
                           (comparison (compare-strings expected nil nil actual nil nil)))
                      (unless (eq comparison t)
                        (let ((offset (1- (abs comparison))))
                          (ert-fail
                           (format "%s mismatch at %d: expected %S, got %S"
                                   kind offset
                                   (substring expected offset (min (length expected) (+ offset 100)))
                                   (substring actual offset (min (length actual) (+ offset 100)))))))))
                  (should-not clutch--last-result-buffer)
                  (should-not (clutch-db--foreground-busy-p conn)))))
            (message "Wide export verified on %s: 101 columns, 602 rows, no primary key, no indexes, CSV and TSV"
                     clutch-test-backend))
        (when created
          (clutch-db-query conn (format "DROP TABLE %s" table)))
        (delete-file path)))))

(ert-deftest clutch-test-live-query-export-refuses-unavailable-binary ()
  "Reject JDBC binary metadata, while exporting a complete XML BLOB value."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(oracle sqlserver))
    (ert-skip "This fixture tests JDBC's metadata-only binary values"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_export_blob_%d" (emacs-pid)))
           (oracle (eq clutch-test-backend 'oracle))
           (dir (make-temp-file "clutch-export-blob-" t))
           (path (expand-file-name "out.csv" dir))
           created)
      (unwind-protect
          (progn
            (clutch-db-query conn (format "CREATE TABLE %s (id INT, body %s)" table
                                          (if oracle "RAW(16)" "VARBINARY(16)")))
            (setq created t)
            (clutch-db-query conn (format "INSERT INTO %s VALUES (1, %s)" table
                                          (if oracle "HEXTORAW('3c723e6f6b3c2f723e')"
                                            "0x3c723e6f6b3c2f723e")))
            (clutch-db-query conn (format "INSERT INTO %s VALUES (2, %s)" table
                                          (if oracle "HEXTORAW('616263')" "0x616263")))
            (clutch-db-commit conn)
            (with-temp-buffer
              (clutch-mode)
              (setq-local clutch-connection conn)
              (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                        ((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
                (insert (format "SELECT id, body FROM %s WHERE id=1" table))
                (clutch-test--with-minibuffer-answers '("csv" "utf-8")
                  (call-interactively #'clutch-export-query))
                (clutch-test--await-queries)
                (should (equal (with-temp-buffer (insert-file-contents path) (buffer-string))
                               (if oracle "ID,BODY\n1,<r>ok</r>\n" "id,body\n1,<r>ok</r>\n")))
                (write-region "original" nil path nil 'silent)
                (erase-buffer)
                (insert (format "SELECT id, body FROM %s WHERE id=2" table))
                (clutch-test--with-minibuffer-answers '("csv" "utf-8")
                  (call-interactively #'clutch-export-query))
                (clutch-test--await-queries)))
            (should (equal (with-temp-buffer (insert-file-contents path) (buffer-string)) "original"))
            (should (equal (directory-files dir nil "\\`[^.]") '("out.csv")))
            (should-not (clutch-db--foreground-busy-p conn)))
        (when created
          (clutch-db-query conn (format "DROP TABLE %s" table))
          (clutch-db-commit conn))
        (delete-directory dir t)))))

(ert-deftest clutch-test-live-query-export-refuses-incomplete-clob ()
  "Export a complete Oracle CLOB, but never replace it with a partial one."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'oracle)
    (ert-skip "This fixture tests Oracle JDBC CLOB previews"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_export_clob_%d" (emacs-pid)))
           (dir (make-temp-file "clutch-export-clob-" t))
           (path (expand-file-name "out.csv" dir))
           (sql (format "SELECT id, body FROM %s ORDER BY id" table))
           created messages)
      (unwind-protect
          (progn
            (clutch-db-query conn (format "CREATE TABLE %s (id INT, body CLOB)" table))
            (setq created t)
            (clutch-db-query conn (format "INSERT INTO %s VALUES (1, '中文短CLOB')" table))
            (clutch-db-query conn (format "INSERT INTO %s VALUES (2, RPAD('x', 1024, 'x'))" table))
            (clutch-db-commit conn)
            (should (clutch-db-value-preview-p
                     (cadr (cadr (clutch-db-result-rows (clutch-db-query conn sql))))))
            (write-region "original" nil path nil 'silent)
            (with-temp-buffer
              (clutch-mode)
              (setq-local clutch-connection conn)
              (insert (format "SELECT id, body FROM %s WHERE id=1" table))
              (let ((clutch-export-page-size 1))
                (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                          ((symbol-function 'yes-or-no-p) (lambda (&rest _) t))
                          ((symbol-function 'message)
                           (lambda (fmt &rest args)
                             (when fmt (push (apply #'format-message fmt args) messages)))))
                  (clutch-test--with-minibuffer-answers '("csv" "utf-8")
                    (call-interactively #'clutch-export-query))
                  (clutch-test--await-queries)
                  (should (equal (with-temp-buffer (insert-file-contents path) (buffer-string))
                                 "ID,BODY\n1,中文短CLOB\n"))
                  (write-region "original" nil path nil 'silent)
                  (erase-buffer)
                  (insert sql)
                  (clutch-test--with-minibuffer-answers '("csv" "utf-8")
                    (call-interactively #'clutch-export-query))
                  (clutch-test--await-queries))))
            (should (cl-some (lambda (msg) (string-match-p "only a preview" msg)) messages))
            (should (equal (with-temp-buffer (insert-file-contents path) (buffer-string)) "original"))
            (should (equal (directory-files dir nil "\\`[^.]") '("out.csv")))
            (should-not (clutch-db--foreground-busy-p conn)))
        (when created
          (clutch-db-query conn (format "DROP TABLE %s" table))
          (clutch-db-commit conn))
        (delete-directory dir t)))))

(ert-deftest clutch-test-live-result-filter-sort-page-count-export-workflow ()
  "Result buffer workflows should run real backend queries end-to-end."
  :tags '(:clutch-live)
  (unless (clutch-test--result-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :result-workflow)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_result_flow_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string) (score int))))
           (insert-sql
            (format
             "INSERT INTO %s (id, name, score) VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'cam', 30), (4, 'dan', 40), (5, 'eve', 50)"
             table))
           (select-sqls
            (list (format "SELECT id, name, score FROM %s ORDER BY id" table)
                  (format "WITH r AS (SELECT id, name, score FROM %s) SELECT id, name, score FROM r ORDER BY id"
                          table)))
           (result-name (format " *clutch-flow-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (dolist (select-sql (if (clutch-test--live-supports-with-p conn)
                                    select-sqls
                                  (butlast select-sqls)))
              (ert-info (select-sql)
                (clutch-test--with-live-result-buffer result-name
                  (let ((clutch-result-max-rows 2))
                    (clutch-test--execute-live-select conn select-sql))
                  (with-current-buffer result-name
                    (set-window-buffer (selected-window) (current-buffer))
                    (setq-local clutch-result-max-rows 2)
                    (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                   '(1 2)))
                    (should (string-match-p "ann" (buffer-string)))
                    (should clutch--page-has-more)
                    (cl-letf (((symbol-function 'message) #'ignore))
                      (clutch-result-count-total)
                      (clutch-test--await-queries)
                      (should (= clutch--page-total-rows 5))
                      (clutch-result-last-page)
                      (clutch-test--await-queries)
                      (should (= clutch--page-current 2))
                      (should (= clutch--page-offset 3))
                      (should-not clutch--page-has-more)
                      (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                     '(4 5)))
                      (should (string-match-p "eve" (buffer-string)))
                      (let ((summary clutch--footer-base-string))
                        (dolist (pattern '("eve" "missing" ""))
                          (cl-letf (((symbol-function 'read-string)
                                     (lambda (&rest _) pattern)))
                            (progn (call-interactively (key-binding (kbd "/")))
                               (clutch-test--await-queries)))
                          (should (equal summary clutch--footer-base-string))
                          (pcase pattern
                            ("eve"
                             (should (equal (clutch-test--live-row-ids
                                             (clutch--result-display-rows))
                                            '(5)))
                             (should (string-match-p
                                      "1/2 page matches"
                                      (clutch--footer-mode-line-display))))
                            ("missing"
                             (should-not (clutch--result-display-rows))
                             (should (string-match-p "No matches on this page"
                                                     (buffer-string))))
                            (""
                             (should (= 2 (length (clutch--result-display-rows))))
                             (should-not (string-match-p "No matches"
                                                         (buffer-string)))))))
                      (let ((score-column
                             (cl-find "score" clutch--result-columns
                                      :test #'string-equal-ignore-case)))
                        (should score-column)
                        (clutch-result--sort score-column t)
                        (clutch-test--await-queries))
                      (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                     '(5 4)))
                      (should (string-match-p "dan" (buffer-string)))
                      (clutch-result-next-page)
                      (clutch-test--await-queries)
                      (should (= clutch--page-current 1))
                      (should clutch--page-has-more)
                      (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                     '(3 2)))
                      (let ((score-column
                             (cl-find "score" clutch--result-columns
                                      :test #'string-equal-ignore-case)))
                        (should score-column)
                        (clutch-test--with-minibuffer-answers
                            (list score-column "> 20")
                          (clutch-result-apply-filter)
                          (clutch-test--await-queries))
                        (should (equal clutch--where-filter
                                       (format "%s > 20"
                                               (clutch-db-escape-identifier
                                                conn score-column)))))
                      (should (= clutch--page-current 0))
                      (should (equal (sort (clutch-test--live-row-ids
                                            clutch--result-rows)
                                           #'<)
                                     '(3 4)))
                      (clutch-result-count-total)
                      (clutch-test--await-queries)
                      (should (= clutch--page-total-rows 3))
                      (cl-letf (((symbol-function 'read-string)
                                 (lambda (&rest _) "missing")))
                        (progn (call-interactively (key-binding (kbd "/")))
                               (clutch-test--await-queries)))
                      (should-not (clutch--result-display-rows))
                      (let (rows)
                        (clutch-result--collect-all-export-rows
                         (lambda (all) (setq rows all)))
                        (clutch-test--await-queries)
                        (should (equal (sort (clutch-test--live-row-ids rows) #'<)
                                       '(3 4 5))))
                      ;; Pressing W again changes the condition, and an empty
                      ;; condition clears the filter.
                      (let ((score-column
                             (cl-find "score" clutch--result-columns
                                      :test #'string-equal-ignore-case)))
                        (clutch-test--with-minibuffer-answers
                            (list score-column "> 40")
                          (clutch-result-apply-filter)
                          (clutch-test--await-queries))
                        (should (equal (clutch-test--live-row-ids
                                        clutch--result-rows)
                                       '(5)))
                        (clutch-test--with-minibuffer-answers
                            (list score-column "")
                          (clutch-result-apply-filter)
                          (clutch-test--await-queries))
                        (should-not clutch--where-filter)
                        (should (memq 1 (clutch-test--live-row-ids
                                         clutch--result-rows))))))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-mysql-limited-join-duplicate-columns-executes-flat ()
  "MySQL limited JOIN results with duplicate column names should not be wrapped."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :duplicate-column-join)
    (ert-skip (clutch-test-capability-skip-message :duplicate-column-join)))
  (clutch-test--with-conn conn
    (let* ((table-a (format "clutch_dup_a_%d" (emacs-pid)))
           (table-b (format "clutch_dup_b_%d" (emacs-pid)))
           (drop-a (format "DROP TABLE IF EXISTS %s" table-a))
           (drop-b (format "DROP TABLE IF EXISTS %s" table-b))
           (create-a
            (format "CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(64))"
                    table-a))
           (create-b
            (format "CREATE TABLE %s (id INT PRIMARY KEY, label VARCHAR(64))"
                    table-b))
           (insert-a
            (format "INSERT INTO %s (id, name) VALUES (1, 'ann'), (2, 'bob')"
                    table-a))
           (insert-b
            (format "INSERT INTO %s (id, label) VALUES (1, 'a1'), (2, 'b2')"
                    table-b))
           (select-sql
            (format "SELECT a.*, b.* FROM %s AS a JOIN %s AS b ON a.id = b.id LIMIT 10"
                    table-a table-b))
           (result-name (format " *clutch-dup-columns-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-b)
            (clutch-db-query conn drop-a)
            (clutch-db-query conn create-a)
            (clutch-db-query conn create-b)
            (clutch-db-query conn insert-a)
            (clutch-db-query conn insert-b)
            (clutch-test--with-live-result-buffer result-name
              (let ((clutch-result-max-rows 1))
                (clutch-test--execute-live-select conn select-sql))
              (with-current-buffer result-name
                (should (equal clutch--result-columns
                               '("id" "name" "id" "label")))
                (should (= (length clutch--result-rows) 2))
                (should-not clutch--page-has-more)
                (should-not clutch--result-server-pageable)
                (should-not clutch--result-server-rewritable)
                (should-not clutch--result-source-table))))
        (ignore-errors (clutch-db-query conn drop-b))
        (ignore-errors (clutch-db-query conn drop-a))))))

(ert-deftest clutch-test-live-jdbc-refused-cancel-stops-batch-and-export ()
  "A finished JDBC statement keeps its outcome but starts no further SQL.
Cancel just before Clutch handles the real reply, when the JDBC backend
has no active request to cancel.  No cancellation result is mocked."
  :tags '(:clutch-live)
  (unless (memq (clutch-test-live-backend-id) '(oracle sqlserver clickhouse duckdb))
    (ert-skip "Requires a JDBC SQL backend"))
  (clutch-test--with-conn conn
    (let* ((dir (make-temp-file "clutch-live-refused-cancel-" t))
           (path (expand-file-name "out.csv" dir))
           (from (if (eq clutch-test-backend 'oracle) " FROM dual" ""))
           (first (concat "SELECT 1 AS id" from))
           (second (concat "SELECT 2 AS id" from))
           (third (concat "SELECT 3 AS id" from)))
      (unwind-protect
          (dolist (workflow '(batch export))
            (ert-info ((symbol-name workflow))
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn)
                (insert (if (eq workflow 'batch)
                            (concat first "; " second)
                          (concat first " UNION ALL " second " UNION ALL " third
                                  " ORDER BY id")))
                (write-region "original" nil path nil 'silent)
                (let ((finish (symbol-function 'clutch--finish-db-query))
                      (query (symbol-function 'clutch--run-db-query-async))
                      (interrupt (symbol-function 'clutch-db-interrupt-query))
                      (source (current-buffer))
                      (clutch-export-page-size 1)
                      (cancel-result 'not-called)
                      cancelled completed reply-error dispatched)
                  (cl-letf (((symbol-function 'clutch--finish-db-query)
                             (lambda (reply-conn sql callback result error)
                               (when (and (eq reply-conn conn) (not cancelled))
                                 (setq cancelled t completed result reply-error error)
                                 (with-current-buffer source
                                   (call-interactively #'clutch-cancel-query-or-quit)))
                               (funcall finish reply-conn sql callback result error)))
                            ((symbol-function 'clutch--run-db-query-async)
                             (lambda (query-conn sql region callback)
                               (push sql dispatched)
                               (funcall query query-conn sql region callback)))
                            ((symbol-function 'clutch-db-interrupt-query)
                             (lambda (cancel-conn)
                               (setq cancel-result (funcall interrupt cancel-conn))))
                            ((symbol-function 'read-file-name) (lambda (&rest _) path))
                            ((symbol-function 'yes-or-no-p) (lambda (&rest _) t))
                            ((symbol-function 'clutch--present-statement-outcome) #'ignore))
                    (if (eq workflow 'batch)
                        (call-interactively #'clutch-execute-buffer)
                      (clutch-test--with-minibuffer-answers '("csv" "utf-8")
                        (call-interactively #'clutch-export-query)))
                    (clutch-test--await-queries))
                  (should cancelled)
                  (should-not cancel-result)
                  (should-not reply-error)
                  (should (equal (format "%s" (caar (clutch-db-result-rows completed))) "1"))
                  (should (= (length dispatched) 1))
                  (should-not (clutch-db--foreground-busy-p conn))
                  (should (clutch-db-live-p conn))
                  (should (equal (with-temp-buffer (insert-file-contents path) (buffer-string))
                                 "original"))
                  (should (equal (directory-files dir nil "\\`[^.]") '("out.csv")))))))
        (delete-directory dir t)))))

(ert-deftest clutch-test-live-long-statement-runs-in-background-and-cancels ()
  "A long statement should leave Emacs free and stop when C-g cancels it."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :async-cancel)
    (ert-skip (clutch-test-capability-skip-message :async-cancel)))
  (clutch-test--with-conn conn
    (let ((sleep-sql (plist-get (clutch-test-live-backend-descriptor)
                                :sleep-sql))
          (start (float-time))
          timer-fired shown)
      (with-temp-buffer
        (setq-local clutch-connection conn)
        (cl-letf (((symbol-function 'clutch--show-execution-error)
                   (lambda (_buffer _conn _sql err &rest _args)
                     (setq shown err)
                     "cancelled")))
          (clutch--execute (format sleep-sql 30))
          (should-not shown)
          (should (gethash conn clutch--running-queries))
          (run-at-time 0.2 nil (lambda () (setq timer-fired t)))
          (clutch-test--await (lambda () timer-fired))
          (should (gethash conn clutch--running-queries))
          (clutch-cancel-query-or-quit)
          (clutch-test--await (lambda () shown))
          (should (eq (car shown) 'clutch-db-error))
          (should (< (- (float-time) start) 20))
          (should-not (gethash conn clutch--running-queries))))
      (clutch-db-query conn (format sleep-sql 0))
      (should (clutch-db-live-p conn)))))

(ert-deftest clutch-test-live-disconnect-ends-running-statement ()
  "Disconnecting during a statement should return at once and end it once.
The server may still finish the statement, so its outcome is unknown, which
the echo area says; the buffer has left the connection, so no error page is
drawn."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :async-cancel)
    (ert-skip (clutch-test-capability-skip-message :async-cancel)))
  (clutch-test--with-conn conn
    (let ((sleep-sql (plist-get (clutch-test-live-backend-descriptor)
                                :sleep-sql))
          (start (float-time))
          shown reported)
      (with-temp-buffer
        (setq-local clutch-connection conn)
        (cl-letf (((symbol-function 'clutch--show-execution-error)
                   (lambda (&rest _) (setq shown t) "failed"))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (let ((text (apply #'format format-string args)))
                       (when (string-match-p "outcome is unknown" text)
                         (push text reported)))))
                  ((symbol-function 'clutch--confirm-session-close) #'ignore))
          (clutch--execute (format sleep-sql 30))
          (should (gethash conn clutch--running-queries))
          (clutch-disconnect)
          (should (< (- (float-time) start) 3))
          (clutch-test--await (lambda () reported))
          (sleep-for 0.2)
          (ert-run-idle-timers)
          (should (= (length reported) 1))
          (should-not shown)
          (should-not (gethash conn clutch--running-queries)))))))

(ert-deftest clutch-test-live-pg-ctid-edit-via-execute-select-persists ()
  "PostgreSQL no-key edit should work through SELECT row identity injection."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :ctid-row-identity)
    (ert-skip (clutch-test-capability-skip-message :ctid-row-identity)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_ctid_edit_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql (format "CREATE TABLE %s (name TEXT)" table))
           (insert-sql (format "INSERT INTO %s (name) VALUES ('before')" table))
           (select-sql (format "SELECT name FROM %s" table))
           (result-name (format " *clutch-ctid-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should (equal (plist-get clutch--row-identity :kind)
                               'row-locator))
                (should (equal (plist-get clutch--row-identity :name) "ctid"))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t)))
                  (let ((row (car clutch--result-rows)))
                    (clutch-result--apply-edit
                     0 0 "after"
                     (list
                      :identity (clutch-db-row-identity-values
                                 row clutch--row-identity)
                      :original (car row)
                      :original-state (cons nil (car row)))))
                  (should clutch--pending-edits)
                  (progn (clutch-result-submit) (clutch-test--await-queries))
                  (should-not clutch--pending-edits)
                  (should (equal (caar clutch--result-rows) "after")))))
            (let ((rows (clutch-db-result-rows
                         (clutch-db-query
                          conn
                          (format "SELECT name FROM %s" table)))))
              (should (equal rows '(("after"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-pg-qualified-table-changes-itself ()
  "A result of a table in another schema should be edited by its own key.
public has a table of the same name keyed by another column, and the
schema is not on the search path.  Unquoted names fold to lower case and
quoted ones keep their case, as PostgreSQL reads them."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "Live backend is not PostgreSQL"))
  (clutch-test--with-conn conn
    (let* ((schema (format "clutch_qual_%d" (emacs-pid)))
           (quoted (format "\"Qual_%d\"" (emacs-pid)))
           (table (format "people_%d" (emacs-pid)))
           (result-name (format " *clutch-pg-qualified-%d*" (emacs-pid))))
      (cl-flet ((rows (sql)
                  (clutch-db-result-rows (clutch-db-query conn sql)))
                (edit-first-row (column value)
                  (let ((row (car clutch--result-rows))
                        (cidx (cl-position column clutch--result-columns
                                           :test #'string=)))
                    (clutch-result--apply-edit
                     0 cidx value
                     (list :identity (clutch-db-row-identity-values
                                      row clutch--row-identity)
                           :original (nth cidx row)
                           :original-state (cons nil (nth cidx row))))
                    (cl-letf (((symbol-function 'yes-or-no-p)
                               (lambda (&rest _) t)))
                      (clutch-result-submit)
                      (clutch-test--await-queries)))))
        (unwind-protect
            (progn
              (dolist (sql (list (format "CREATE TABLE public.%s (id text PRIMARY KEY)" table)
                                 (format "CREATE SCHEMA %s" schema)
                                 (format "CREATE TABLE %s.%s (pk int PRIMARY KEY, id text, nick text)"
                                         schema table)
                                 (format "INSERT INTO %s.%s VALUES (1, 'same', 'a'), (2, 'same', 'b')"
                                         schema table)
                                 (format "CREATE SCHEMA %s" quoted)
                                 (format "CREATE TABLE %s.\"People\" (pk int PRIMARY KEY, name text)"
                                         quoted)
                                 (format "INSERT INTO %s.\"People\" VALUES (1, 'Cy')" quoted)))
                (clutch-db-query conn sql))
              (clutch-test--with-live-result-buffer result-name
                (clutch-test--execute-live-select
                 conn (format "SELECT * FROM %s.%s ORDER BY pk" (upcase schema) table))
                (with-current-buffer result-name
                  (should (equal (plist-get clutch--row-identity :columns) '("pk")))
                  (edit-first-row "nick" "A"))
                (clutch-test--execute-live-select
                 conn (format "SELECT * FROM %s.\"People\"" quoted))
                (with-current-buffer result-name
                  (should (equal (plist-get clutch--row-identity :columns) '("pk")))
                  (edit-first-row "name" "Cz")))
              (should (equal (rows (format "SELECT pk, nick FROM %s.%s ORDER BY pk"
                                           schema table))
                             '((1 "A") (2 "b"))))
              (should (equal (rows (format "SELECT name FROM %s.\"People\"" quoted))
                             '(("Cz"))))
              (should-not (rows (format "SELECT * FROM public.%s" table))))
          (dolist (sql (list (format "DROP SCHEMA IF EXISTS %s CASCADE" schema)
                             (format "DROP SCHEMA IF EXISTS %s CASCADE" quoted)
                             (format "DROP TABLE IF EXISTS public.%s" table)))
            (ignore-errors (clutch-db-query conn sql))))))))

(ert-deftest clutch-test-live-pg-result-follows-keys-in-its-schema ()
  "Following a foreign key should open the parent in the child's schema.
The query either qualifies the child or finds it through the search path,
whose first schema, public, has a parent table of the same name."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "Live backend is not PostgreSQL"))
  (clutch-test--with-conn conn
    (let* ((schema (format "clutch_fk_%d" (emacs-pid)))
           (parents (format "parents_%d" (emacs-pid)))
           (result-name (format " *clutch-pg-fk-%d*" (emacs-pid))))
      (cl-flet ((follow (query)
                  (let (followed)
                    (clutch-test--with-live-result-buffer result-name
                      (clutch-test--execute-live-select conn query)
                      (with-current-buffer result-name
                        (clutch-test--await (lambda () (assq 1 clutch--fk-info)))
                        (cl-letf (((symbol-function 'clutch--execute)
                                   (lambda (sql &rest _) (setq followed sql))))
                          (clutch-record--follow-fk
                           (cdr (assq 1 clutch--fk-info)) 1 (current-buffer)))))
                    (clutch-db-result-rows (clutch-db-query conn followed)))))
        (unwind-protect
            (progn
              (dolist (sql (list (format "CREATE TABLE public.%s (id int PRIMARY KEY, label text)"
                                         parents)
                                 (format "INSERT INTO public.%s VALUES (1, 'public')" parents)
                                 (format "CREATE SCHEMA %s" schema)
                                 (format "CREATE TABLE %s.%s (id int PRIMARY KEY, label text)"
                                         schema parents)
                                 (format "INSERT INTO %s.%s VALUES (1, 'own')" schema parents)
                                 (format "CREATE TABLE %s.children (id int PRIMARY KEY, parent_id int REFERENCES %s.%s (id))"
                                         schema schema parents)
                                 (format "INSERT INTO %s.children VALUES (1, 1)" schema)))
                (clutch-db-query conn sql))
              (should (equal (follow (format "SELECT * FROM %s.children" schema))
                             '((1 "own"))))
              (clutch-db-query conn (format "SET search_path TO public, %s" schema))
              (should (equal (follow "SELECT * FROM children") '((1 "own")))))
          (dolist (sql (list (format "DROP SCHEMA IF EXISTS %s CASCADE" schema)
                             (format "DROP TABLE IF EXISTS public.%s" parents)))
            (ignore-errors (clutch-db-query conn sql))))))))

(ert-deftest clutch-test-live-pg-ctid-aggregate-select-skips-row-identity-injection ()
  "PostgreSQL no-key aggregate SELECT should not receive CTID injection."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :ctid-row-identity)
    (ert-skip (clutch-test-capability-skip-message :ctid-row-identity)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_ctid_count_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql (format "CREATE TABLE %s (name TEXT)" table))
           (insert-sql
            (format "INSERT INTO %s (name) VALUES ('a'), ('b')" table))
           (select-sql (format "SELECT count(1) FROM %s" table))
           (result-name (format " *clutch-ctid-count-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (let* ((result (clutch-test--execute-live-select conn select-sql))
                     (rows (clutch-db-result-rows result)))
                (should (equal (format "%s" (caar rows)) "2")))
              (with-current-buffer result-name
                (should (string-match-p "2" (buffer-string))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-aggregate-select-skips-row-identity-injection ()
  "Aggregate SELECT execution should not inject row identity into live SQL."
  :tags '(:clutch-live)
  (unless (clutch-test--result-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :result-workflow)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_issue12_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (insert-sql
            (format "INSERT INTO %s (id, name) VALUES (1, 'a'), (2, 'b')"
                    table))
           (select-sql (format "SELECT count(1) FROM %s" table))
           (result-name (format " *clutch-count-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (let* ((result (clutch-test--execute-live-select conn select-sql))
                     (rows (clutch-db-result-rows result)))
                (should (equal (format "%s" (caar rows)) "2")))
              (with-current-buffer result-name
                (should (string-match-p "2" (buffer-string))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-data-modifying-cte-runs-once ()
  "A SELECT over a data-modifying CTE should run once and dirty Manual mode.
A quote inside the CTE's quoted name must not hide the modification."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :data-modifying-cte)
    (ert-skip (clutch-test-capability-skip-message :data-modifying-cte)))
  (clutch-test--with-conn conn
    (dolist (name '("i" "\"it's i\""))
      (ert-info (name)
        (let* ((table (format "clutch_cte_write_%d" (emacs-pid)))
               (drop-sql (format "DROP TABLE IF EXISTS %s" table))
               (count-sql (format "SELECT count(*) FROM %s" table))
               (cte-sql
                (format "WITH %s AS (INSERT INTO %s SELECT g FROM generate_series(1, 5) g RETURNING id) SELECT * FROM %s"
                        name table name))
               (result-name (format " *clutch-cte-write-live-%d*" (emacs-pid))))
          (cl-flet ((row-count ()
                      (string-to-number
                       (format "%s" (caar (clutch-db-result-rows
                                           (clutch-db-query conn count-sql)))))))
            (unwind-protect
                (progn
                  (clutch-db-query conn drop-sql)
                  (clutch-db-query conn (format "CREATE TABLE %s (id int)" table))
                  (clutch-test--with-live-result-buffer result-name
                    (let ((clutch-result-max-rows 2))
                      (clutch-test--execute-live-select conn cte-sql))
                    (with-current-buffer result-name
                      (should (= (length clutch--result-rows) 5))
                      (should-error (clutch-result-next-page) :type 'user-error)))
                  (should (= (row-count) 5))
                  (clutch-db-set-auto-commit conn nil)
                  (clutch-test--with-live-result-buffer result-name
                    (clutch-test--execute-live-select conn cte-sql))
                  (should (clutch--tx-dirty-p conn))
                  (clutch-db-rollback conn)
                  (clutch--clear-tx-state conn)
                  (should (= (row-count) 5)))
              (ignore-errors
                (when (clutch-db-manual-commit-p conn)
                  (clutch-db-rollback conn)
                  (clutch--clear-tx-state conn)
                  (clutch-db-set-auto-commit conn t)))
              (ignore-errors (clutch-db-query conn drop-sql)))))))))

(ert-deftest clutch-test-live-select-into-copies-every-row ()
  "SELECT INTO should copy every row as written and dirty Manual mode."
  :tags '(:clutch-live)
  (unless (clutch-test-live-backend-capability-p :select-into)
    (ert-skip (clutch-test-capability-skip-message :select-into)))
  (clutch-test--with-conn conn
    (let* ((source (format "clutch_into_src_%d" (emacs-pid)))
           (copies (mapcar (lambda (n) (format "clutch_into_copy%d_%d" n (emacs-pid)))
                           '(1 2 3)))
           (result-name (format " *clutch-select-into-live-%d*" (emacs-pid))))
      (cl-flet ((drop-all ()
                  (dolist (table (cons source copies))
                    (ignore-errors
                      (clutch-db-query conn (format "DROP TABLE IF EXISTS %s" table)))))
                (row-count (table)
                  (string-to-number
                   (format "%s" (caar (clutch-db-result-rows
                                       (clutch-db-query
                                        conn (format "SELECT COUNT(*) FROM %s" table))))))))
        (unwind-protect
            (progn
              (drop-all)
              (clutch-db-query conn (clutch-test--live-create-table-sql
                                     source '((id int primary) (name string))))
              (clutch-db-query
               conn (format "INSERT INTO %s (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"
                            source))
              ;; A single-table SELECT drew row identity injection, and a
              ;; join a pagination tail.
              (clutch-test--with-live-result-buffer result-name
                (let ((clutch-result-max-rows 2))
                  (clutch-test--execute-live-select
                   conn (format "SELECT * INTO %s FROM %s" (nth 0 copies) source))
                  (clutch-test--execute-live-select
                   conn (format "SELECT s.id, s.name INTO %s FROM %s s JOIN %s k ON k.id = s.id"
                                (nth 1 copies) source source))))
              (should (= (row-count (nth 0 copies)) 5))
              (should (= (row-count (nth 1 copies)) 5))
              (clutch-db-set-auto-commit conn nil)
              (clutch-test--with-live-result-buffer result-name
                (clutch-test--execute-live-select
                 conn (format "SELECT * INTO %s FROM %s" (nth 2 copies) source)))
              (should (clutch--tx-dirty-p conn))
              (clutch-db-rollback conn)
              (clutch--clear-tx-state conn)
              (clutch-db-set-auto-commit conn t))
          (ignore-errors
            (when (clutch-db-manual-commit-p conn)
              (clutch-db-rollback conn)
              (clutch--clear-tx-state conn)
              (clutch-db-set-auto-commit conn t)))
          (drop-all))))))

(ert-deftest clutch-test-live-edit-field-and-submit-persists ()
  "Edit through a real SELECT result and submit the persisted row change."
  :tags '(:clutch-live)
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :updateable-workflow)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_edit_submit_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (insert-sql
             (format "INSERT INTO %s (id, name) VALUES (1, 'before')" table))
           (select-sql
            (concat (and (eq (clutch-db-backend-key conn) 'pg)
                         "-- row identity comment regression\n")
                    (format "SELECT id, name FROM %s ORDER BY id" table)))
           (result-name (format " *clutch-edit-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should (equal (clutch-test--live-row-prefix-strings
                                (car clutch--result-rows) 2)
                               '("1" "before")))
                (should (equal (mapcar #'downcase
                                       (plist-get clutch--row-identity :columns))
                               '("id")))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t)))
                  (let ((row (car clutch--result-rows)))
                    (clutch-result--apply-edit
                     0 1 "after"
                     (list
                      :identity (clutch-db-row-identity-values
                                 row clutch--row-identity)
                      :original (nth 1 row)
                      :original-state (cons nil (nth 1 row)))))
                  (should clutch--pending-edits)
                  (progn (clutch-result-submit) (clutch-test--await-queries))
                  (should-not clutch--pending-edits)
                  (should (equal (clutch-test--live-row-prefix-strings
                                  (car clutch--result-rows) 2)
                                 '("1" "after"))))))
            (let* ((res (clutch-db-query conn select-sql))
                   (rows (clutch-db-result-rows res)))
              (should (equal (mapcar (lambda (row)
                                       (clutch-test--live-row-prefix-strings
                                        row 2))
                                     rows)
                             '(("1" "after"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-cte-result-edit-changes-one-base-row ()
  "Editing a CTE result should change only the base table row it shows."
  :tags '(:clutch-live)
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :updateable-workflow)))
  (clutch-test--with-conn conn
    (unless (clutch-test--live-supports-with-p conn)
      (ert-skip "MySQL before 8.0 has no WITH clause"))
    (let* ((table (format "clutch_cte_edit_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (rows-sql (format "SELECT id, name, team FROM %s ORDER BY id" table))
           (result-name (format " *clutch-cte-edit-live-%d*" (emacs-pid))))
      (cl-flet ((table-rows ()
                  (mapcar (lambda (row)
                            (clutch-test--live-row-prefix-strings row 3))
                          (clutch-db-result-rows
                           (clutch-db-query conn rows-sql)))))
        (unwind-protect
            (progn
              (clutch-db-query conn drop-sql)
              (clutch-db-query conn (clutch-test--live-create-table-sql
                                     table '((id int primary) (name string)
                                             (team string))))
              (clutch-db-query
               conn (format "INSERT INTO %s (id, name, team) VALUES (1, 'alpha', 'a'), (2, 'alpha', 'b')"
                            table))
              ;; None of the queries projects the key, and both rows share a name.
              (cl-loop
               for (select-sql value)
               in `((,(format "WITH c (n, t) AS (SELECT name, team FROM %s) SELECT n, t FROM c ORDER BY t"
                              table)
                     "gamma")
                    (,(format "WITH c AS (SELECT name AS n, team AS t FROM %s), d AS (SELECT * FROM c) SELECT x.n, x.t FROM d x ORDER BY x.t"
                              table)
                     "delta")
                    ;; A `*' over the table, next to which the innermost
                    ;; SELECT adds the hidden identity.
                    (,(format "WITH c AS (SELECT * FROM %s) SELECT name, team FROM c ORDER BY team"
                              table)
                     "epsilon"))
               do (ert-info (select-sql)
                    (clutch-test--with-live-result-buffer result-name
                      (clutch-test--execute-live-select conn select-sql)
                      (with-current-buffer result-name
                        (should (string-equal-ignore-case
                                 clutch--result-source-table table))
                        (let ((row (car clutch--result-rows)))
                          (clutch-result--apply-edit
                           0 0 value
                           (list :identity (clutch-db-row-identity-values
                                            row clutch--row-identity)
                                 :original (car row)
                                 :original-state (cons nil (car row)))))
                        (cl-letf (((symbol-function 'yes-or-no-p)
                                   (lambda (&rest _) t)))
                          (clutch-result-submit)
                          (clutch-test--await-queries))
                        (should-not clutch--pending-edits)))
                    (should (equal (table-rows)
                                   `(("1" ,value "a") ("2" "alpha" "b")))))))
          (ignore-errors (clutch-db-query conn drop-sql)))))))

(ert-deftest clutch-test-live-cte-result-delete-and-insert-reach-base-table ()
  "Deleting and inserting through a CTE result should change its base table."
  :tags '(:clutch-live)
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :updateable-workflow)))
  (clutch-test--with-conn conn
    (unless (clutch-test--live-supports-with-p conn)
      (ert-skip "MySQL before 8.0 has no WITH clause"))
    (let* ((table (format "clutch_cte_delete_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (rows-sql (format "SELECT id, name, team FROM %s ORDER BY id" table))
           (select-sql
            (format "WITH c (n, t) AS (SELECT name, team FROM %s) SELECT n, t FROM c ORDER BY t"
                    table))
           (result-name (format " *clutch-cte-delete-live-%d*" (emacs-pid))))
      (cl-flet ((table-rows ()
                  (mapcar (lambda (row)
                            (clutch-test--live-row-prefix-strings row 3))
                          (clutch-db-result-rows
                           (clutch-db-query conn rows-sql)))))
        (unwind-protect
            (progn
              (clutch-db-query conn drop-sql)
              (clutch-db-query conn (clutch-test--live-create-table-sql
                                     table '((id int primary) (name string)
                                             (team string))))
              (clutch-db-query
               conn (format "INSERT INTO %s (id, name, team) VALUES (1, 'alpha', 'a'), (2, 'alpha', 'b')"
                            table))
              (clutch-test--with-live-result-buffer result-name
                (clutch-test--execute-live-select conn select-sql)
                (with-current-buffer result-name
                  (set-window-buffer (selected-window) (current-buffer))
                  (cl-letf (((symbol-function 'yes-or-no-p)
                             (lambda (&rest _) t)))
                    ;; The second row shown shares its name with the first.
                    (goto-char (aref clutch--row-start-positions 1))
                    (clutch-result-delete-rows)
                    (clutch-result-submit)
                    (clutch-test--await-queries)
                    (should (equal (table-rows) '(("1" "alpha" "a"))))
                    (setq-local clutch--pending-inserts
                                `(((,(clutch-test--live-column-name "id") . "3")
                                   (,(clutch-test--live-column-name "name") . "omega")
                                   (,(clutch-test--live-column-name "team") . "c"))))
                    (clutch-result-submit)
                    (clutch-test--await-queries)
                    (should (equal (table-rows)
                                   '(("1" "alpha" "a") ("3" "omega" "c"))))))))
          (ignore-errors (clutch-db-query conn drop-sql)))))))

(ert-deftest clutch-test-live-autocommit-staged-batch-is-atomic ()
  "Auto mode should commit or roll back a real staged batch as one submission."
  :tags '(:clutch-live)
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :updateable-workflow)))
  (clutch-test--with-conn conn
    (when (clutch-db-manual-commit-p conn)
      (ert-skip "This regression requires an auto-commit connection"))
    (let* ((table (format "clutch_auto_batch_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (select-sql (format "SELECT id, name FROM %s ORDER BY id" table))
           (result-name (format " *clutch-auto-batch-live-%d*" (emacs-pid)))
           (id-column (clutch-test--live-column-name "id"))
           (name-column (clutch-test--live-column-name "name")))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (setq-local
                 clutch--pending-inserts
                 `(((,id-column . "1") (,name-column . "one"))
                   ((,id-column . "2") (,name-column . "two"))))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (progn (clutch-result-submit) (clutch-test--await-queries)))
                (should-not clutch--pending-inserts)
                (should-not (clutch-db-manual-commit-p conn))
                (setq-local
                 clutch--pending-inserts
                 `(((,id-column . "3") (,name-column . "three"))
                   ((,id-column . "1") (,name-column . "duplicate"))))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (should-error (progn (clutch-result-submit) (clutch-test--await-queries)) :type 'user-error))
                (should (= (length clutch--pending-inserts) 2))
                (should-not (clutch-db-manual-commit-p conn))))
            (should
             (equal
              (mapcar
               (lambda (row)
                 (clutch-test--live-row-prefix-strings row 2))
               (clutch-db-result-rows (clutch-db-query conn select-sql)))
              '(("1" "one") ("2" "two")))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-manual-staged-batch-rolls-back-to-savepoint ()
  "A failed Manual submission should preserve earlier work and undo its own prefix."
  :tags '(:clutch-live)
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :updateable-workflow)))
  (unless (clutch-test-live-backend-capability-p :manual-savepoint)
    (ert-skip (clutch-test-capability-skip-message :manual-savepoint)))
  (clutch-test--with-conn conn
    (unless (clutch-db-manual-commit-supported-p conn)
      (ert-skip "This regression requires manual-commit support"))
    (let* ((table (format "clutch_manual_batch_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (select-sql (format "SELECT id, name FROM %s ORDER BY id" table))
           (insert-sql (format "INSERT INTO %s (id, name) VALUES (?, ?)" table))
           (result-name (format " *clutch-manual-batch-live-%d*" (emacs-pid)))
           (id-column (clutch-test--live-column-name "id"))
           (name-column (clutch-test--live-column-name "name")))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-set-auto-commit conn nil)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (clutch--run-db-query conn insert-sql '(1 "earlier"))
              (with-current-buffer result-name
                (setq-local
                 clutch--pending-inserts
                 `(((,id-column . "2") (,name-column . "batch-prefix"))
                   ((,id-column . "1") (,name-column . "duplicate"))))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (should-error (progn (clutch-result-submit) (clutch-test--await-queries)) :type 'user-error))
                (should (= (length clutch--pending-inserts) 2))
                (should (clutch--tx-dirty-p conn))
                (should
                 (equal
                  (mapcar
                   (lambda (row)
                     (clutch-test--live-row-prefix-strings row 2))
                   (clutch-db-result-rows (clutch-db-query conn select-sql)))
                  '(("1" "earlier"))))
                (setq-local
                 clutch--pending-inserts
                 `(((,id-column . "2") (,name-column . "batch-prefix"))
                   ((,id-column . "3") (,name-column . "fixed"))))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (progn (clutch-result-submit) (clutch-test--await-queries)))
                (should-not clutch--pending-inserts)
                (should
                 (equal
                  (mapcar
                   (lambda (row)
                     (clutch-test--live-row-prefix-strings row 2))
                   (clutch-db-result-rows (clutch-db-query conn select-sql)))
                  '(("1" "earlier")
                    ("2" "batch-prefix")
                    ("3" "fixed"))))))
            (clutch-db-rollback conn)
            (clutch--clear-tx-state conn)
            (should-not
             (clutch-db-result-rows (clutch-db-query conn select-sql)))
            (clutch-db-set-auto-commit conn t))
        (ignore-errors
          (when (clutch-db-manual-commit-p conn)
            (clutch-db-rollback conn)
            (clutch--clear-tx-state conn)
            (clutch-db-set-auto-commit conn t)))
          (clutch-db-query conn drop-sql)))))

(ert-deftest clutch-test-live-rollback-to-a-savepoint-keeps-the-transaction-dirty ()
  "A rollback to a savepoint should leave the work before it known as uncommitted.
Clutch took it for a rollback of the whole transaction, so a disconnect then
lost that work without asking.  The savepoint is named chain, a word that a
whole rollback can also end with."
  :tags '(:clutch-live)
  (pcase-let ((`(,save ,rollback)
               (pcase clutch-test-backend
                 ((or 'pg 'mysql 'oracle)
                  '("SAVEPOINT chain" "ROLLBACK TO SAVEPOINT chain"))
                 ('sqlserver
                  '("SAVE TRANSACTION chain" "ROLLBACK TRANSACTION chain")))))
    (unless save
      (ert-skip "This regression needs a backend whose savepoint syntax it knows"))
    (clutch-test--with-conn conn
      (unless (clutch-db-manual-commit-supported-p conn)
        (ert-skip "This regression requires manual-commit support"))
      (let* ((table (format "clutch_savepoint_%d" (emacs-pid)))
             (drop-sql (format "DROP TABLE IF EXISTS %s" table))
             (insert-sql (format "INSERT INTO %s (id, name) VALUES (?, ?)" table)))
        (unwind-protect
            (progn
              (clutch-db-query conn drop-sql)
              (clutch-db-query conn (clutch-test--live-create-table-sql
                                     table '((id int primary) (name string))))
              (clutch-db-set-auto-commit conn nil)
              (clutch--run-db-query conn insert-sql '(1 "before"))
              (clutch--run-db-query conn save)
              (clutch--run-db-query conn insert-sql '(2 "after"))
              (clutch--run-db-query conn rollback)
              (should (clutch--tx-dirty-p conn))
              (should (equal (clutch-test--live-row-ids
                              (clutch-db-result-rows
                               (clutch-db-query
                                conn (format "SELECT id FROM %s" table))))
                             '(1))))
          (ignore-errors
            (when (clutch-db-manual-commit-p conn)
              (clutch-db-rollback conn)
              (clutch--clear-tx-state conn)
              (clutch-db-set-auto-commit conn t)))
          (clutch-db-query conn drop-sql))))))

(defun clutch-test--live-close-prompts (close)
  "Call CLOSE, declining every confirmation, and return the prompts it asked."
  (let (prompts)
    (cl-letf (((symbol-function 'yes-or-no-p)
               (lambda (prompt) (push prompt prompts) nil)))
      (condition-case nil
          (funcall close)
        (user-error nil)))
    prompts))

(ert-deftest clutch-test-live-auto-mode-transaction-begun-with-sql-is-tracked ()
  "Work in a transaction begun with SQL in Auto mode should be known.
Clutch recorded nothing in Auto mode, so after a typed BEGIN and INSERT,
disconnecting or killing the console dropped the insert without asking.
The server's report of an open transaction now marks it, a rollback to
a savepoint keeps it, and a typed COMMIT clears it.  Switching to Manual
mode keeps such a transaction open for `clutch-commit' to end."
  :tags '(:clutch-live)
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL and PostgreSQL transaction status"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_auto_tx_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params))))
      (cl-flet ((rows ()
                  (caar (clutch-db-result-rows
                         (clutch-db-query
                          admin (format "SELECT COUNT(*) FROM %s" table))))))
        (unwind-protect
            (progn
              (clutch-db-query admin (format "CREATE TABLE %s (id int)" table))
              (clutch-test--with-live-console params
                (should-not (clutch-db-manual-commit-p clutch-connection))
                (clutch-test--run-in-console
                 (if (eq clutch-test-backend 'mysql) "START TRANSACTION" "BEGIN")
                 (format "INSERT INTO %s VALUES (1)" table))
                (should (eq (plist-get clutch--connection-render-state
                                       :transaction-state)
                            'auto-dirty))
                (should (clutch-test--live-close-prompts #'clutch-disconnect))
                (should (clutch--connection-alive-p clutch-connection))
                (let ((console (current-buffer)))
                  (should (clutch-test--live-close-prompts
                           (lambda () (kill-buffer console))))
                  (should (buffer-live-p console)))
                (clutch-test--run-in-console
                 "SAVEPOINT s" (format "INSERT INTO %s VALUES (2)" table)
                 "ROLLBACK TO SAVEPOINT s")
                (should (clutch-test--live-close-prompts #'clutch-disconnect))
                (should (= (rows) 0))
                (clutch-test--run-in-console "COMMIT")
                (should-not (clutch--tx-state clutch-connection))
                (should (eq (plist-get clutch--connection-render-state
                                       :transaction-state)
                            'auto))
                (should (= (rows) 1))
                (clutch-test--run-in-console
                 (if (eq clutch-test-backend 'mysql) "START TRANSACTION" "BEGIN")
                 (format "INSERT INTO %s VALUES (3)" table))
                (clutch-toggle-auto-commit)
                (should (eq (plist-get clutch--connection-render-state
                                       :transaction-state)
                            'dirty))
                (should (= (rows) 1))
                (clutch-commit)
                (should (= (rows) 2))
                (clutch-toggle-auto-commit)))
          (ignore-errors
            (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table))))))))

(ert-deftest clutch-test-live-mysql-ddl-that-commits-nothing-keeps-work-known ()
  "A MySQL CREATE TEMPORARY TABLE should keep the work before it known.
Clutch took every DDL for one that commits, as most MySQL DDL does, so in
Manual mode a temporary table after an INSERT cleared the insert, and a
disconnect then dropped it without asking.  A CREATE TABLE commits it."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'mysql)
    (ert-skip "This regression covers MySQL's implicit commits"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_tmp_tx_%d" (emacs-pid)))
          (params (append (list :backend 'mysql) (clutch-test--live-connect-params))))
      (cl-flet ((rows ()
                  (caar (clutch-db-result-rows
                         (clutch-db-query
                          admin (format "SELECT COUNT(*) FROM %s" table))))))
        (unwind-protect
            (progn
              (clutch-db-query admin (format "CREATE TABLE %s (id int)" table))
              (clutch-test--with-live-console params
                (clutch-toggle-auto-commit)
                (clutch-test--run-in-console
                 (format "INSERT INTO %s VALUES (1)" table)
                 (format "CREATE TEMPORARY TABLE %s_tmp (id int)" table))
                (should (clutch--tx-dirty-p clutch-connection))
                (should (clutch-test--live-close-prompts #'clutch-disconnect))
                (should (= (rows) 0))
                (clutch-test--run-in-console
                 (format "CREATE TABLE %s_made (id int)" table))
                (should-not (clutch--tx-state clutch-connection))
                (should (= (rows) 1))
                (clutch-toggle-auto-commit)))
          (ignore-errors
            (clutch-db-query admin (format "DROP TABLE IF EXISTS %s, %s_made"
                                           table table))))))))

(ert-deftest clutch-test-live-insert-and-delete-submit-persists ()
  "Submitted insert and delete staging should persist on a real backend."
  :tags '(:clutch-live)
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :updateable-workflow)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_insert_delete_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (select-sql (format "SELECT id, name FROM %s ORDER BY id" table))
           (result-name (format " *clutch-insert-delete-live-%d*" (emacs-pid)))
           insert-buf)
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should-not clutch--result-rows)
                (cl-letf (((symbol-function 'pop-to-buffer)
                           (lambda (buf &rest _args)
                             (setq insert-buf buf)
                             buf)))
                  (clutch-result-insert-row)))
              (with-current-buffer insert-buf
                (goto-char (point-min))
                (should (re-search-forward "^id.*: " nil t))
                (insert "1")
                (goto-char (point-min))
                (should (re-search-forward "^name.*: " nil t))
                (insert "ann")
                (cl-letf (((symbol-function 'quit-window) #'ignore)
                          ((symbol-function 'message) #'ignore))
                  (clutch-result-insert-stage)))
              (with-current-buffer result-name
                (let ((insert (car clutch--pending-inserts)))
                  (should (equal (cdr (assoc-string "id" insert t)) "1"))
                  (should (equal (cdr (assoc-string "name" insert t)) "ann")))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (progn (clutch-result-submit) (clutch-test--await-queries)))
                (should-not clutch--pending-inserts))
              (should (equal (mapcar (lambda (row)
                                       (clutch-test--live-row-prefix-strings
                                        row 2))
                                     (clutch-db-result-rows
                                      (clutch-db-query conn select-sql)))
                             '(("1" "ann"))))
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should (equal (clutch-test--live-row-prefix-strings
                                (car clutch--result-rows) 2)
                               '("1" "ann")))
                (cl-letf (((symbol-function 'clutch--selected-row-indices)
                           (lambda () '(0)))
                          ((symbol-function 'message) #'ignore))
                  (clutch-result-delete-rows))
                (should (equal (mapcar (lambda (identity)
                                         (format "%s" (aref identity 0)))
                                       clutch--pending-deletes)
                               '("1")))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (progn (clutch-result-submit) (clutch-test--await-queries)))
                (should-not clutch--pending-deletes)))
            (should-not (clutch-db-result-rows
                         (clutch-db-query conn select-sql))))
        (when (buffer-live-p insert-buf)
          (kill-buffer insert-buf))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-pg-repl-batch-keeps-its-last-transaction-dirty ()
  "A REPL batch ending with an uncommitted write must ask before closing.
COMMIT at the start of the input cleared the work of its later BEGIN
and INSERT, despite the server reporting that transaction still open."
  :tags '(:clutch-live)
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "This regression covers PostgreSQL's multi-statement REPL"))
  (clutch-test--with-conn admin
    (let ((table (format "clutch_repl_tx_%d" (emacs-pid)))
          (params (append '(:backend pg) (clutch-test--live-connect-params))))
      (unwind-protect
          (progn
            (clutch-db-query admin (format "CREATE TABLE %s (id int)" table))
            (clutch-test--with-live-console params
              (let ((conn clutch-connection))
                (clutch-repl-mode)
                (setq-local clutch-connection conn
                            clutch--connection-params params))
              (clutch-repl--input-sender
               nil (format "COMMIT; BEGIN; INSERT INTO %s VALUES (1);" table))
              (clutch-test--await-queries)
              (should (clutch--tx-dirty-p clutch-connection))
              (should (clutch-test--live-close-prompts #'clutch-disconnect))
              (should (clutch--connection-alive-p clutch-connection))
              (should (= (caar (clutch-db-result-rows
                               (clutch-db-query admin
                                                (format "SELECT COUNT(*) FROM %s" table))))
                         0))
              (clutch-repl--input-sender nil "COMMIT;")
              (clutch-test--await-queries)
              (should-not (clutch--tx-state clutch-connection))
              (should (= (caar (clutch-db-result-rows
                               (clutch-db-query admin
                                                (format "SELECT COUNT(*) FROM %s" table))))
                         1))))
        (ignore-errors (clutch-db-query admin (format "DROP TABLE IF EXISTS %s" table)))))))

;;;; XTDB

(defvar clutch-test--xtdb-table-counter 0
  "Number of tables created by XTDB live tests in this Emacs.")

(defun clutch-test--xtdb-table (prefix)
  "Return a new table name starting with PREFIX.
XTDB has no DROP TABLE, and a table keeps its column types after its
rows are erased, so each test writes to tables of its own."
  (format "clutch_%s_%d_%d" prefix (emacs-pid)
          (cl-incf clutch-test--xtdb-table-counter)))

(defun clutch-test--xtdb-rows (conn sql)
  "Return the rows of SQL on CONN."
  (clutch-db-result-rows (clutch-db-query conn sql)))

(defun clutch-test--xtdb-column-type (conn table column)
  "Return XTDB's own type of COLUMN in TABLE on CONN."
  (caar (clutch-test--xtdb-rows
         conn
         (format "SELECT data_type FROM information_schema.columns WHERE table_name = '%s' AND column_name = '%s'"
                 table column))))

(defun clutch-test--xtdb-edit (ridx column value)
  "Stage VALUE for COLUMN of row RIDX through an edit buffer.
The current buffer is the result."
  (set-window-buffer (selected-window) (current-buffer))
  (clutch--goto-cell ridx (cl-position column clutch--result-columns
                                       :test #'string=))
  (with-current-buffer (clutch-result-edit-cell)
    (erase-buffer)
    (insert value)
    (clutch-result-edit-finish)))

(defun clutch-test--xtdb-stage-insert (fields)
  "Stage a row of FIELDS, an alist of columns and values, from the insert form.
The current buffer is the result."
  (set-window-buffer (selected-window) (current-buffer))
  (clutch-result-insert-row)
  (with-current-buffer (window-buffer (selected-window))
    (pcase-dolist (`(,name . ,value) fields)
      (clutch-test--set-insert-field-value name value))
    (clutch-result-insert-stage)))

(defun clutch-test--xtdb-submit ()
  "Submit the staged changes of the current result."
  (set-window-buffer (selected-window) (current-buffer))
  (cl-letf (((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
    (clutch-result-submit)
    (clutch-test--await-queries)))

(defun clutch-test--xtdb-execute (conn sql)
  "Run SQL on CONN as a command does and return its confirmation prompts."
  (let (prompts)
    (cl-letf (((symbol-function 'yes-or-no-p)
               (lambda (prompt) (push prompt prompts) t)))
      (with-temp-buffer
        (let ((clutch-connection conn)
              (clutch--source-window (selected-window))
              (clutch-high-risk-query-confirmation 'yes-or-no))
          (clutch--execute sql)
          (clutch-test--await-queries))))
    prompts))

(ert-deftest clutch-test-live-xtdb-first-write-transaction-defers-metadata ()
  "A cold console's catalog refresh must leave its write transaction usable."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn admin
    (let ((table (clutch-test--xtdb-table "cold_tx"))
          (params (append '(:backend xtdb) (clutch-test--live-connect-params))))
      (clutch-test--with-live-console params
        (clutch-test--run-in-console "BEGIN READ WRITE")
        (ert-run-idle-timers)
        (should (eq (pgsql-transaction-status
                     (clutch-db-pg--connection-client clutch-connection))
                    'in-transaction))
        (clutch-test--run-in-console
         (format "INSERT INTO %s (_id, name) VALUES ('a', 'kept')" table)
         "COMMIT")
        (should (equal (clutch-test--xtdb-rows admin
                                             (format "SELECT name FROM %s" table))
                       '(("kept"))))
        (ert-run-idle-timers)
        (clutch-test--await
         (lambda () (eq (plist-get (clutch--schema-status-entry clutch-connection) :state)
                        'ready)))))))

(ert-deftest clutch-test-live-xtdb-manual-read-does-not-open-a-read-only-transaction ()
  "Manual reads must not prevent the following DML from opening its transaction.
An ASSERT opens one as a write does, so it still guards the writes after it."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn admin
    (let ((table (clutch-test--xtdb-table "read_then_write"))
          (params (append '(:backend xtdb) (clutch-test--live-connect-params))))
      (clutch-test--with-live-console params
        (clutch-toggle-auto-commit)
        (clutch-test--run-in-console "SELECT 1")
        (clutch-test--run-in-console
         (format "INSERT INTO %s (_id, name) VALUES ('a', 'kept')" table))
        (should (clutch--tx-dirty-p clutch-connection))
        (should-not (clutch-test--xtdb-rows admin (format "SELECT name FROM %s" table)))
        (clutch-commit)
        (should (equal (clutch-test--xtdb-rows admin (format "SELECT name FROM %s" table))
                       '(("kept"))))
        (clutch-test--run-in-console
         (format "INSERT INTO %s (_id, name) VALUES ('b', 'discarded')" table))
        (clutch-rollback)
        (should (equal (clutch-test--xtdb-rows admin
                                             (format "SELECT name FROM %s ORDER BY _id" table))
                       '(("kept"))))
        ;; An ASSERT opens the transaction too, so it guards the write after it.
        (clutch-test--run-in-console
         "ASSERT 1 = 2"
         (format "INSERT INTO %s (_id, name) VALUES ('c', 'guarded')" table))
        (should-error (clutch-commit) :type 'user-error)
        (clutch-rollback)
        (should (equal (clutch-test--xtdb-rows admin
                                             (format "SELECT name FROM %s ORDER BY _id" table))
                       '(("kept"))))))))

(ert-deftest clutch-test-live-xtdb-reads-its-own-catalog ()
  "XTDB should connect as its own backend and read its catalog as XTDB has it."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((table (clutch-test--xtdb-table "meta")))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, age, name) VALUES ('a', 1, 'x')" table))
      (should (eq (clutch-db-backend-key conn) 'xtdb))
      (should (member table (clutch-db-list-tables conn)))
      (should-not (clutch-db-list-schemas conn))
      (dolist (category '(indexes sequences procedures functions triggers))
        (should-not (clutch-db-list-objects conn category)))
      (should (equal (clutch-db-primary-key-columns conn table) '("_id")))
      (cl-flet ((detail (name key)
                  (plist-get (cl-find name (clutch-db-column-details conn table)
                                      :key (lambda (d) (plist-get d :name))
                                      :test #'equal)
                             key)))
        (should (equal (detail "age" :backend-type) "int8"))
        (should (equal (detail "name" :backend-type) "text"))
        (should-not (detail "_id" :nullable))
        (should (detail "_valid_from" :generated))
        (should (detail "_system_from" :generated)))))
  (let ((err (should-error
              (clutch-db-connect 'xtdb (append '(:schema "public")
                                               (clutch-test--live-connect-params))))))
    (should (string-match-p "cannot switch its current schema"
                            (error-message-string err)))))

(ert-deftest clutch-test-live-xtdb-staged-changes-keep-column-types ()
  "An insert, an edit and a deletion should keep their columns' types.
XTDB stores a value with the type it is sent as."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((table (clutch-test--xtdb-table "staff"))
          (result-name (format " *clutch-xtdb-staff-%d*" (emacs-pid))))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, age, name) VALUES ('a1', 12, 'Ann'), ('a2', 15, 'Max')"
                    table))
      (clutch-test--with-live-result-buffer result-name
        (clutch-test--execute-live-select
         conn (format "SELECT * FROM %s ORDER BY _id" table))
        (with-current-buffer result-name
          (should (eq (plist-get clutch--row-identity :kind) 'primary-key))
          (clutch-test--xtdb-stage-insert
           '(("_id" . "a3") ("age" . "17") ("name" . "Annie")))
          (clutch-test--xtdb-edit 0 "name" "Ann2")
          (clutch--goto-cell 1 0)
          (clutch-result-delete-rows)
          (clutch-test--xtdb-submit)))
      (should (equal (clutch-test--xtdb-rows
                      conn (format "SELECT _id, age, name FROM %s ORDER BY _id" table))
                     '(("a1" 12 "Ann2") ("a3" 17 "Annie"))))
      (should (equal (clutch-test--xtdb-column-type conn table "age") ":i64"))
      (should (equal (clutch-test--xtdb-column-type conn table "name") ":utf8")))))

(ert-deftest clutch-test-live-xtdb-manual-mode-refuses-staged-changes ()
  "Staged changes should need Auto mode, since XTDB has no savepoints."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((table (clutch-test--xtdb-table "manual"))
          (result-name (format " *clutch-xtdb-manual-%d*" (emacs-pid))))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, name) VALUES ('a1', 'Ann')" table))
      (clutch-db-set-auto-commit conn nil)
      (clutch-test--with-live-result-buffer result-name
        (clutch-test--execute-live-select conn (format "SELECT * FROM %s" table))
        (with-current-buffer result-name
          (clutch-test--xtdb-edit 0 "name" "Ann3")
          (should (string-match-p
                   "no savepoints"
                   (error-message-string
                    (should-error (clutch-test--xtdb-submit)
                                  :type 'user-error))))))
      (clutch-db-set-auto-commit conn t)
      (should (equal (clutch-test--xtdb-rows conn (format "SELECT name FROM %s" table))
                     '(("Ann")))))))

(ert-deftest clutch-test-live-xtdb-transaction-state-follows-the-server ()
  "XTDB's report of an open transaction should mark uncommitted work.
An INSERT in Manual mode, or after BEGIN READ WRITE in Auto mode, leaves
work that a disconnect asks about; a commit clears it, and a SELECT in
Manual mode, which runs outside a transaction, marks nothing."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (let ((table (clutch-test--xtdb-table "tx"))
        (params (append (list :backend 'xtdb) (clutch-test--live-connect-params))))
    (cl-flet ((state () (plist-get clutch--connection-render-state
                                   :transaction-state)))
      (clutch-test--with-live-console params
        (clutch-test--run-in-console
         (format "INSERT INTO %s (_id, name) VALUES ('a0', 'Al')" table))
        (should (eq (state) 'auto))
        (clutch-test--run-in-console
         "BEGIN READ WRITE"
         (format "INSERT INTO %s (_id, name) VALUES ('a1', 'Ann')" table))
        (should (eq (state) 'auto-dirty))
        (clutch-test--run-in-console "COMMIT")
        (should (eq (state) 'auto))
        (clutch-test--with-conn other
          (should (equal (clutch-test--xtdb-rows
                          other (format "SELECT _id FROM %s ORDER BY _id" table))
                         '(("a0") ("a1")))))
        (clutch-toggle-auto-commit)
        (clutch-test--run-in-console
         (format "INSERT INTO %s (_id, name) VALUES ('a2', 'Bo')" table))
        (should (eq (state) 'dirty))
        (should (clutch-test--live-close-prompts #'clutch-disconnect))
        (clutch-commit)
        (should (eq (state) 'manual))
        (clutch-test--run-in-console (format "SELECT * FROM %s" table))
        (should (eq (state) 'manual))
        (clutch-rollback)
        (clutch-toggle-auto-commit)))))

(ert-deftest clutch-test-live-xtdb-time-and-union-columns ()
  "A time column should take times, and a union column should refuse a value.
XTDB reports both as json, and stores a JSON string as a string."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((rota (clutch-test--xtdb-table "rota"))
          (mixed (clutch-test--xtdb-table "mixed"))
          (result-name (format " *clutch-xtdb-time-%d*" (emacs-pid))))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, starts) VALUES ('r1', TIME '09:30:00')" rota))
      (clutch-db-query conn (format "INSERT INTO %s (_id, v) VALUES ('m1', 1)" mixed))
      (clutch-db-query conn (format "INSERT INTO %s (_id, v) VALUES ('m2', 'one')" mixed))
      (clutch-test--with-live-result-buffer result-name
        (clutch-test--execute-live-select
         conn (format "SELECT * FROM %s ORDER BY _id" rota))
        (with-current-buffer result-name
          (clutch-test--xtdb-stage-insert '(("_id" . "r2") ("starts" . "11:00:00")))
          (clutch-test--xtdb-submit)
          ;; A time can be given with or without seconds.
          (dolist (value '("10:15:00" "10:20"))
            (clutch-test--xtdb-edit 0 "starts" value)
            (clutch-test--xtdb-submit))
          (clutch-test--xtdb-edit 1 "starts" "\"10:45\"")
          (should-error (clutch-test--xtdb-submit)))
        (clutch-test--execute-live-select
         conn (format "SELECT * FROM %s ORDER BY _id" mixed))
        (with-current-buffer result-name
          (clutch-test--xtdb-edit 0 "v" "2")
          (should-error (clutch-test--xtdb-submit))))
      (should (equal (clutch-test--xtdb-rows
                      conn (format "SELECT _id, starts FROM %s ORDER BY _id" rota))
                     '(("r1" "10:20") ("r2" "11:00"))))
      (should (equal (clutch-test--xtdb-column-type conn rota "starts")
                     "[:time-local :nano]"))
      (should (equal (clutch-test--xtdb-rows
                      conn (format "SELECT _id, v FROM %s ORDER BY _id" mixed))
                     '(("m1" 1) ("m2" "one")))))))

(ert-deftest clutch-test-live-xtdb-number-union-columns-take-each-value ()
  "A column of integers and fractions should take either through edits.
Each value goes as a member type that holds it, so the column's union does not
grow, also after it gains a NULL; a value that is no number is refused."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((table (clutch-test--xtdb-table "score"))
          (result-name (format " *clutch-xtdb-score-%d*" (emacs-pid))))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, n) VALUES ('s1', 1), ('s2', 2.5)" table))
      (clutch-test--with-live-result-buffer result-name
        (clutch-test--execute-live-select
         conn (format "SELECT * FROM %s ORDER BY _id" table))
        (with-current-buffer result-name
          (clutch-test--xtdb-edit 0 "n" "7")
          (clutch-test--xtdb-edit 1 "n" "3.5")
          (clutch-test--xtdb-submit)
          (clutch-test--xtdb-stage-insert '(("_id" . "s3") ("n" . "4")))
          (clutch-test--xtdb-submit)
          (clutch--goto-cell 1 (cl-position "n" clutch--result-columns
                                            :test #'string=))
          (with-current-buffer (clutch-result-edit-cell)
            (clutch-result-edit-set-null)
            (clutch-result-edit-finish))
          (clutch-test--xtdb-submit)
          (clutch-test--xtdb-edit 0 "n" "8.25")
          (clutch-test--xtdb-submit)
          (clutch-test--xtdb-edit 2 "n" "x")
          (should-error (clutch-test--xtdb-submit))))
      (should (equal (clutch-test--xtdb-rows
                      conn (format "SELECT _id, n FROM %s ORDER BY _id" table))
                     '(("s1" 8.25) ("s2" nil) ("s3" 4))))
      (should (equal (clutch-test--xtdb-column-type conn table "n")
                     "[:union :i64 :f64 [:? :null]]")))))

(ert-deftest clutch-test-live-xtdb-number-union-values-keep-every-digit ()
  "A number should go as a member of its column that holds it whole.
A long fraction takes the decimal member before the float, and an integer
out of the integer member's range takes the float."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((exact (clutch-test--xtdb-table "exact"))
          (small (clutch-test--xtdb-table "small"))
          (result-name (format " *clutch-xtdb-fit-%d*" (emacs-pid)))
          (digits "0.12345678901234567890123456789"))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, n) VALUES ('e1', 1.5::decimal), ('e2', 2.5)"
                    exact))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, n) VALUES ('m1', 1::smallint), ('m2', 2.5::real)"
                    small))
      (clutch-test--with-live-result-buffer result-name
        (clutch-test--execute-live-select
         conn (format "SELECT * FROM %s ORDER BY _id" exact))
        (with-current-buffer result-name
          (clutch-test--xtdb-edit 0 "n" digits)
          (clutch-test--xtdb-submit))
        (clutch-test--execute-live-select
         conn (format "SELECT * FROM %s ORDER BY _id" small))
        (with-current-buffer result-name
          (clutch-test--xtdb-edit 0 "n" "32768")
          (clutch-test--xtdb-submit)))
      (should (equal (clutch-test--xtdb-rows
                      conn (format "SELECT CAST(n AS VARCHAR) FROM %s WHERE _id = 'e1'"
                                   exact))
                     (list (list digits))))
      (should (equal (clutch-test--xtdb-rows
                      conn (format "SELECT CAST(n AS VARCHAR) FROM %s WHERE _id = 'm1'"
                                   small))
                     '(("32768.0")))))))

(ert-deftest clutch-test-live-xtdb-timestamptz-keeps-the-time-shown ()
  "A timestamptz should be written as the time shown, with Emacs's offset.
The insert form should set a row's valid time through _valid_from."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (unwind-protect
      (progn
        (set-time-zone-rule "Asia/Shanghai")
        (clutch-test--with-conn conn
          (let ((events (clutch-test--xtdb-table "events"))
                (staff (clutch-test--xtdb-table "valid"))
                (result-name (format " *clutch-xtdb-tz-%d*" (emacs-pid))))
            (clutch-db-query
             conn (format "INSERT INTO %s (_id, tz) VALUES ('e1', TIMESTAMP '2026-01-02T11:04:05+08:00')"
                          events))
            (clutch-db-query
             conn (format "INSERT INTO %s (_id, name) VALUES ('a1', 'Ann')" staff))
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select
               conn (format "SELECT * FROM %s ORDER BY _id" events))
              (with-current-buffer result-name
                (clutch-test--xtdb-edit 0 "tz" "2026-01-02 11:04:06")
                (clutch-test--xtdb-stage-insert
                 '(("_id" . "e2") ("tz" . "2026-05-01 12:00:00")))
                (clutch-test--xtdb-submit))
              (clutch-test--execute-live-select
               conn (format "SELECT *, _valid_from FROM %s ORDER BY _id" staff))
              (with-current-buffer result-name
                (clutch-test--xtdb-stage-insert
                 '(("_id" . "a9") ("name" . "Vic")
                   ("_valid_from" . "2020-05-01 00:00:00")))
                (clutch-test--xtdb-submit)))
            (should (equal (clutch-test--xtdb-rows
                            conn (format "SELECT _id, CAST(tz AS VARCHAR) FROM %s ORDER BY _id"
                                         events))
                           '(("e1" "2026-01-02T11:04:06+08:00")
                             ("e2" "2026-05-01T12:00+08:00"))))
            (should (equal (clutch-test--xtdb-column-type conn events "tz")
                           "[:timestamp-tz :micro \"+08:00\"]"))
            (should (equal (clutch-test--xtdb-rows
                            conn (format "SELECT CAST(_valid_from AS VARCHAR) FROM %s WHERE _id = 'a9'"
                                         staff))
                           '(("2020-04-30T16:00Z[UTC]")))))))
    (set-time-zone-rule (getenv "TZ"))))

(ert-deftest clutch-test-live-xtdb-history-results-are-read-only ()
  "A row of a query of past versions should refuse edits; a current one not.
Its _id names the current version, which an edit would change."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((table (clutch-test--xtdb-table "history"))
          (result-name (format " *clutch-xtdb-history-%d*" (emacs-pid))))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, name) VALUES ('one', 'OLD')" table))
      (clutch-db-query
       conn (format "UPDATE %s SET name = 'NEW' WHERE _id = 'one'" table))
      (clutch-test--with-live-result-buffer result-name
        (pcase-dolist
            (`(,label ,sql ,column)
             `(("plain"
                ,(format "SELECT _id, name FROM %s FOR SYSTEM_TIME ALL WHERE name = 'OLD'" table)
                "name")
               ("cte"
                ,(format "WITH h AS (SELECT _id, name FROM %s FOR SYSTEM_TIME ALL) SELECT * FROM h WHERE name = 'OLD'" table)
                "name")
               ("setting"
                ,(format "SETTING DEFAULT VALID_TIME TO ALL SELECT _id, name FROM %s WHERE name = 'OLD'" table)
                "name")
               ("block comment"
                ,(format "SELECT _id, name\nFROM %s FOR /* history */ SYSTEM_TIME ALL\nWHERE name = 'OLD' LIMIT 10" table)
                "name")
               ("line comment"
                ,(format "SELECT _id, name\nFROM %s FOR -- history\nSYSTEM_TIME ALL\nWHERE name = 'OLD' LIMIT 10" table)
                "name")
               ("quoted alias"
                ,(format "SELECT _id, name AS \"customer's name\"\nFROM %s FOR SYSTEM_TIME ALL\nWHERE name = 'OLD' LIMIT 10" table)
                "customer's name")))
          (ert-info (label)
            (clutch-test--execute-live-select conn sql)
            (with-current-buffer result-name
              (should (equal (nth (cl-position column clutch--result-columns
                                               :test #'string=)
                                  (car clutch--result-rows))
                             "OLD"))
              (should-not clutch--row-identity)
              (should-error (clutch-test--xtdb-edit 0 column "EDITED_OLD")
                            :type 'user-error))))
        (should (equal (clutch-test--xtdb-rows
                        conn (format "SELECT name FROM %s FOR SYSTEM_TIME ALL ORDER BY name"
                                     table))
                       '(("NEW") ("OLD"))))
        (clutch-test--execute-live-select
         conn (format "SELECT _id, name AS \"FOR SYSTEM_TIME ALL\" FROM %s" table))
        (with-current-buffer result-name
          (should (eq (plist-get clutch--row-identity :kind) 'primary-key))
          (clutch-test--xtdb-edit 0 "FOR SYSTEM_TIME ALL" "EDITED")
          (clutch-test--xtdb-submit)))
      (should (equal (clutch-test--xtdb-rows conn (format "SELECT name FROM %s" table))
                     '(("EDITED")))))))

(ert-deftest clutch-test-live-xtdb-erase-asks-once-and-dirties-manual-mode ()
  "ERASE should ask once, as a DELETE does, and dirty Manual mode."
  :tags '(:xtdb-live)
  (unless (eq clutch-test-backend 'xtdb)
    (ert-skip "Live backend is not XTDB"))
  (clutch-test--with-conn conn
    (let ((auto (clutch-test--xtdb-table "erase"))
          (manual (clutch-test--xtdb-table "erase_manual"))
          (result-name (format " *clutch-xtdb-erase-%d*" (emacs-pid))))
      (clutch-db-query
       conn (format "INSERT INTO %s (_id, v) VALUES ('e1', 1), ('e2', 2)" auto))
      (clutch-db-query conn (format "INSERT INTO %s (_id, v) VALUES ('m1', 1)" manual))
      (clutch-test--with-live-result-buffer result-name
        (let ((prompts (clutch-test--xtdb-execute
                        conn (format "ERASE FROM %s WHERE _id = 'e1'" auto))))
          (should (= (length prompts) 1))
          (should (string-prefix-p "Execute destructive query?" (car prompts))))
        (let ((prompts (clutch-test--xtdb-execute
                        conn (format "ERASE FROM %s WHERE true" auto))))
          (should (= (length prompts) 1))
          (should (string-prefix-p "Execute high-risk query (WHERE is always true)?"
                                   (car prompts))))
        (should-not (clutch-test--xtdb-rows
                     conn (format "SELECT _id FROM %s FOR SYSTEM_TIME ALL" auto)))
        (clutch-db-set-auto-commit conn nil)
        (clutch-test--xtdb-execute
         conn (format "ERASE FROM %s WHERE _id = 'm1'" manual))
        (should (clutch--tx-dirty-p conn))
        (clutch-db-rollback conn)
        (clutch--clear-tx-state conn)
        (clutch-db-set-auto-commit conn t))
      (should (equal (clutch-test--xtdb-rows conn (format "SELECT _id FROM %s" manual))
                     '(("m1")))))))

(provide 'clutch-test-live)

;;; clutch-test-live.el ends here
