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
    (when clutch-test-props
      (setq params (plist-put params :props clutch-test-props)))
    params))

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
      (clutch--execute-select sql conn))))

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
      (let ((clutch--object-cache (make-hash-table :test 'equal))
            (clutch--object-warmup-timers (make-hash-table :test 'equal))
            (clutch--object-warmup-generations (make-hash-table :test 'equal))
            (clutch--table-metadata-cache (make-hash-table :test 'equal)))
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
           (select-sql (format "SELECT id, name, score FROM %s ORDER BY id" table))
           (result-name (format " *clutch-flow-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
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
                    (cl-letf (((symbol-function 'completing-read)
                               (lambda (&rest _args) score-column))
                              ((symbol-function 'read-string)
                               (lambda (&rest _args) "> 20")))
                      (clutch-result-apply-filter))
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
                  (let ((rows (clutch-result--collect-all-export-rows)))
                    (should (equal (sort (clutch-test--live-row-ids rows) #'<)
                                   '(3 4 5))))))))
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
                  (clutch-result-commit)
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

(ert-deftest clutch-test-live-edit-field-and-commit-persists ()
  :tags '(:clutch-live)
  "Edit through a real SELECT result and commit the persisted row change."
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip (clutch-test-capability-skip-message :updateable-workflow)))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_edit_commit_%d" (emacs-pid)))
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
                  (clutch-result-commit)
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

(ert-deftest clutch-test-live-insert-and-delete-commit-persists ()
  :tags '(:clutch-live)
  "Insert and delete staging should persist through real backend commits."
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
                  (clutch-result-insert-commit)))
              (with-current-buffer result-name
                (let ((insert (car clutch--pending-inserts)))
                  (should (equal (cdr (assoc-string "id" insert t)) "1"))
                  (should (equal (cdr (assoc-string "name" insert t)) "ann")))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (clutch-result-commit))
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
                  (clutch-result-commit))
                (should-not clutch--pending-deletes)))
            (should-not (clutch-db-result-rows
                         (clutch-db-query conn select-sql))))
        (when (buffer-live-p insert-buf)
          (kill-buffer insert-buf))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(provide 'clutch-test-live)

;;; clutch-test-live.el ends here
