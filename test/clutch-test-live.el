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
                      (should (= clutch--page-total-rows 5))
                      (clutch-result-last-page)
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
                        (clutch-result--sort score-column t))
                      (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                     '(5 4)))
                      (should (string-match-p "dan" (buffer-string)))
                      (clutch-result-next-page)
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
                      (should (= clutch--page-total-rows 3))
                      (cl-letf (((symbol-function 'read-string)
                                 (lambda (&rest _) "missing")))
                        (progn (call-interactively (key-binding (kbd "/")))
                               (clutch-test--await-queries)))
                      (should-not (clutch--result-display-rows))
                      (let ((rows (clutch-result--collect-all-export-rows)))
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
  "Disconnecting during a statement should return at once and end it once."
  (unless (clutch-test-live-backend-capability-p :async-cancel)
    (ert-skip (clutch-test-capability-skip-message :async-cancel)))
  (clutch-test--with-conn conn
    (let ((sleep-sql (plist-get (clutch-test-live-backend-descriptor)
                                :sleep-sql))
          (start (float-time))
          shown)
      (with-temp-buffer
        (setq-local clutch-connection conn)
        (cl-letf (((symbol-function 'clutch--show-execution-error)
                   (lambda (_buffer _conn _sql err &rest _args)
                     (push err shown)
                     "failed"))
                  ((symbol-function 'clutch--confirm-session-close) #'ignore))
          (clutch--execute (format sleep-sql 30))
          (should (gethash conn clutch--running-queries))
          (clutch-disconnect)
          (should (< (- (float-time) start) 3))
          (clutch-test--await (lambda () shown))
          (sleep-for 0.2)
          (ert-run-idle-timers)
          (should (= (length shown) 1))
          (should (eq (caar shown) 'clutch-db-error))
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
          (clutch--execute sql conn)
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
