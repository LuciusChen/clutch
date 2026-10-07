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
  :tags '(:clutch-live)
  "Generic JDBC live params should pass an optional driver class."
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
  :tags '(:clutch-live)
  "Synthetic live rows should use the backend's metadata identifier case."
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
  :tags '(:clutch-live)
  "Unified console and namespace commands should work against ClickHouse."
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
         (id (caar (clutch-db-result-rows
                    (clutch-db-query conn (if mysql
                                              "SELECT CONNECTION_ID()"
                                            "SELECT pg_backend_pid()"))))))
    (clutch-db-query admin (format (if mysql
                                       "KILL %s"
                                     "SELECT pg_terminate_backend(%s)")
                                   id))
    (clutch-test--await (lambda () (not (clutch--connection-alive-p conn))))
    conn))

(ert-deftest clutch-test-live-console-follows-a-typed-namespace-switch ()
  :tags '(:clutch-live)
  "A console should follow a namespace switch typed into it.
It went on showing and loading the namespace it opened with, and its
parameters reconnected to that one.  PostgreSQL keeps the whole path."
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "This regression covers MySQL USE and PostgreSQL SET search_path"))
  (clutch-test--with-conn admin
    (let ((schema (format "clutch_ns_%d" (emacs-pid)))
          (params (append (list :backend clutch-test-backend)
                          (clutch-test--live-connect-params))))
      (pcase-let ((`(,switch ,namespace ,key ,value ,check-sql)
                   (if (eq clutch-test-backend 'mysql)
                       '("USE information_schema" "information_schema"
                         :database "information_schema" "SELECT DATABASE()")
                     (list (format "SET search_path TO %s, public" schema) schema
                           :search-path (format "%s, public" schema)
                           "SHOW search_path"))))
        (unwind-protect
            (progn
              (when (eq clutch-test-backend 'pg)
                (clutch-db-query admin (format "CREATE SCHEMA %s" schema)))
              (clutch-test--with-live-console params
                (clutch-test--run-in-console switch)
                (should (equal (clutch-db-current-schema clutch-connection)
                               namespace))
                (should (equal (plist-get clutch--connection-params key) value))
                (let ((reopened (clutch-db-connect clutch-test-backend
                                                   clutch--connection-params)))
                  (unwind-protect
                      (should (equal (caar (clutch-db-result-rows
                                            (clutch-db-query reopened check-sql)))
                                     value))
                    (clutch-db-disconnect reopened)))))
          (when (eq clutch-test-backend 'pg)
            (ignore-errors
              (clutch-db-query admin (format "DROP SCHEMA IF EXISTS %s" schema)))))))))

(ert-deftest clutch-test-live-mysql-console-follows-a-dropped-current-database ()
  :tags '(:clutch-live)
  "A MySQL console should have no database once its current one is dropped.
It went on showing the dropped database, and its automatic reconnect
asked for that database and failed.  Loading the tables of no database
must not fail either."
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
  :tags '(:clutch-live)
  "A PostgreSQL console should follow the search_path that the server has.
A SET written with a quoted name or a comment was not followed, a
rollback that undid a SET left the console, and its reconnect, on the
schema the server had left, and a SET not yet committed went into the
reconnect parameters."
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
  :tags '(:clutch-live)
  "A PostgreSQL reconnect should restore the search_path the server kept.
A path set inside a transaction went into the reconnect parameters at
once, so a connection lost before the transaction ended came back on it,
though the server had rolled it back with the transaction."
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
  :tags '(:clutch-live)
  "A rollback after a failed PostgreSQL commit should show the server's path.
A COMMIT that fails rolls back the transaction, and a SET made in it, but
clutch-rollback then found nothing to end, and the console went on
showing the schema that SET had chosen."
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
  :tags '(:clutch-live)
  "An automatic reconnect should keep a console in Manual mode.
The console came back in Auto mode, so each statement after the
reconnect was committed on its own, past the reach of a rollback.
Reopening a console whose session was lost reconnects it too."
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

(ert-deftest clutch-test-live-duckdb-namespace-entrypoint ()
  :tags '(:clutch-live :duckdb-live)
  "The public command should switch DuckDB schemas in the current catalog."
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
  :tags '(:clutch-live)
  "Test schema introspection functions."
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
  :tags '(:clutch-live :native-columns-live)
  "Deferred native completion should not cache a parsed nonexistent table."
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
  :tags '(:clutch-live)
  "Object describe should render real table/index metadata from the backend."
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
  :tags '(:clutch-live)
  "Test paged SQL query building."
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

(ert-deftest clutch-test-live-result-filter-sort-page-count-export-workflow ()
  :tags '(:clutch-live)
  "Result buffer workflows should run real backend queries end-to-end."
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
  :tags '(:clutch-live)
  "MySQL limited JOIN results with duplicate column names should not be wrapped."
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

(ert-deftest clutch-test-live-long-statement-runs-in-background-and-cancels ()
  :tags '(:clutch-live)
  "A long statement should leave Emacs free and stop when C-g cancels it."
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
  :tags '(:clutch-live)
  "Disconnecting during a statement should return at once and end it once.
The server may still finish the statement, so its outcome is unknown, which
the echo area says; the buffer has left the connection, so no error page is
drawn."
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
  :tags '(:clutch-live)
  "PostgreSQL no-key edit should work through SELECT row identity injection."
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
  :tags '(:clutch-live)
  "A result of a table in another schema should be edited by its own key.
public has a table of the same name keyed by another column, and the
schema is not on the search path.  Unquoted names fold to lower case and
quoted ones keep their case, as PostgreSQL reads them."
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
  :tags '(:clutch-live)
  "Following a foreign key should open the parent in the child's schema.
The query either qualifies the child or finds it through the search path,
whose first schema, public, has a parent table of the same name."
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
  :tags '(:clutch-live)
  "PostgreSQL no-key aggregate SELECT should not receive CTID injection."
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
  :tags '(:clutch-live)
  "Aggregate SELECT execution should not inject row identity into live SQL."
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
  :tags '(:clutch-live)
  "A SELECT over a data-modifying CTE should run once and dirty Manual mode.
A quote inside the CTE's quoted name must not hide the modification."
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
  :tags '(:clutch-live)
  "SELECT INTO should copy every row as written and dirty Manual mode."
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
  :tags '(:clutch-live)
  "Edit through a real SELECT result and submit the persisted row change."
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
  :tags '(:clutch-live)
  "Editing a CTE result should change only the base table row it shows."
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
  :tags '(:clutch-live)
  "Deleting and inserting through a CTE result should change its base table."
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
  :tags '(:clutch-live)
  "Auto mode should commit or roll back a real staged batch as one submission."
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
  :tags '(:clutch-live)
  "A failed Manual submission should preserve earlier work and undo its own prefix."
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
  :tags '(:clutch-live)
  "A rollback to a savepoint should leave the work before it known as uncommitted.
Clutch took it for a rollback of the whole transaction, so a disconnect then
lost that work without asking.  The savepoint is named chain, a word that a
whole rollback can also end with."
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

(ert-deftest clutch-test-live-insert-and-delete-submit-persists ()
  :tags '(:clutch-live)
  "Submitted insert and delete staging should persist on a real backend."
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

(ert-deftest clutch-test-live-xtdb-reads-its-own-catalog ()
  :tags '(:xtdb-live)
  "XTDB should connect as its own backend and read its catalog as XTDB has it."
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
  :tags '(:xtdb-live)
  "An insert, an edit and a deletion should keep their columns' types.
XTDB stores a value with the type it is sent as."
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
  :tags '(:xtdb-live)
  "Staged changes should need Auto mode, since XTDB has no savepoints."
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

(ert-deftest clutch-test-live-xtdb-time-and-union-columns ()
  :tags '(:xtdb-live)
  "A time column should take times, and a union column should refuse a value.
XTDB reports both as json, and stores a JSON string as a string."
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
  :tags '(:xtdb-live)
  "A column of integers and fractions should take either through edits.
Each value goes as a member type that holds it, so the column's union does not
grow, also after it gains a NULL; a value that is no number is refused."
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
  :tags '(:xtdb-live)
  "A number should go as a member of its column that holds it whole.
A long fraction takes the decimal member before the float, and an integer
out of the integer member's range takes the float."
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
  :tags '(:xtdb-live)
  "A timestamptz should be written as the time shown, with Emacs's offset.
The insert form should set a row's valid time through _valid_from."
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
  :tags '(:xtdb-live)
  "A row of a query of past versions should refuse edits; a current one not.
Its _id names the current version, which an edit would change."
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
  :tags '(:xtdb-live)
  "ERASE should ask once, as a DELETE does, and dirty Manual mode."
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
