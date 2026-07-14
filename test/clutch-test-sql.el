;;; clutch-test-sql.el --- SQL intelligence ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; SQL parsing, rewrite, completion, eldoc, and xref tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; SQL test helpers

(defmacro clutch-test-sql--with-xref-buffer (spec &rest body)
  "Run BODY in a temporary clutch SQL buffer for xref tests.
SPEC is a plist.  Supported keys are :sql, :pre-needle, :needle, :offset,
:schema, :aliases, :tables, and :connection-alive."
  (declare (indent 1) (debug (sexp body)))
  `(let* ((spec ,spec)
          (schema (plist-get spec :schema))
          (aliases (plist-get spec :aliases))
          (tables (plist-get spec :tables))
          (has-live-stub (plist-member spec :connection-alive))
          (live-p (plist-get spec :connection-alive))
          (orig-alive (symbol-function 'clutch--connection-alive-p)))
     (with-temp-buffer
       (clutch-mode)
       (setq-local clutch-connection 'fake-conn)
       (insert (plist-get spec :sql))
       (goto-char (point-min))
       (when-let* ((pre-needle (plist-get spec :pre-needle)))
         (search-forward pre-needle))
       (search-forward (plist-get spec :needle))
       (goto-char (+ (match-beginning 0) (or (plist-get spec :offset) 0)))
       (cl-letf (((symbol-function 'clutch--schema-for-connection)
                  (lambda (&optional _conn) schema))
                 ((symbol-function 'clutch--tables-in-query-cache-entry)
                  (lambda (_schema)
                    (when (or aliases tables)
                      (list :beg (point-min) :end (point-max)
                            :statement-aliases aliases
                            :statement-tables tables))))
                 ((symbol-function 'clutch--connection-alive-p)
                  (lambda (conn)
                    (if has-live-stub live-p (funcall orig-alive conn)))))
         ,@body))))

(defun clutch-test-sql--xref-definition-position (identifier)
  "Return the first xref definition buffer position for IDENTIFIER."
  (let* ((defs (xref-backend-definitions 'clutch identifier))
         (loc (xref-item-location (car defs))))
    (xref-buffer-location-position loc)))

;;;; SQL parsing — query classification

(ert-deftest clutch-test-query-classification-contract ()
  "SQL query classifiers should distinguish result, schema, and destructive SQL."
  (dolist (case '((schema-affecting
                   clutch-db-sql-schema-affecting-p
                   ("CREATE TABLE t (id INT)"
                    "alter table t add column x int"
                    "DROP VIEW v")
                   ("DELETE FROM t" "SELECT * FROM t"))
                  (destructive
                   clutch-db-sql-destructive-p
                   ("DROP TABLE users"
                    "TRUNCATE users"
                    "DELETE FROM users"
                    "delete from users where id=1"
                    "-- cleanup\nDROP TABLE users")
                   ("SELECT * FROM users" "UPDATE users SET name='x'"))
                  (select
                   clutch-db-sql-select-query-p
                   ("SELECT * FROM users"
                    "select id from users"
                    "  SELECT * FROM t"
                    "WITH cte AS (SELECT 1) SELECT * FROM cte"
                    "-- get users\nSELECT * FROM users"
                    "/* all */\nSELECT * FROM users"
                    "-- a\n-- b\nSELECT 1"
                    "SHOW TABLES"
                    "DESCRIBE users"
                    "EXPLAIN SELECT * FROM t")
                   ("WITH cte AS (SELECT 1) UPDATE users SET active = 1"
                    "WITH deleted AS (DELETE FROM users RETURNING id) DELETE FROM audit"
                    "INSERT INTO users VALUES (1)"
                    "UPDATE users SET name='x'"))))
    (pcase-let ((`(,label ,predicate ,matching ,rejected) case))
      (ert-info ((format "classifier: %s" label))
        (dolist (sql matching)
          (should (funcall predicate sql)))
        (dolist (sql rejected)
          (should-not (funcall predicate sql)))))))

(ert-deftest clutch-test-risky-dml-reason ()
  "Risky DML should detect UPDATE/DELETE without effective WHERE."
  (dolist (sql '("UPDATE users SET name='x'"
                 "DELETE FROM users"
                 "WITH x AS (SELECT 1) UPDATE users SET name='x'"
                 "UPDATE users SET name='x' WHERE 1=1"
                 "DELETE FROM users WHERE 1=1"
                 "UPDATE users SET name='x' WHERE 1 = 1"
                 "DELETE FROM users WHERE (1=1)"
                 "UPDATE users SET name='x' WHERE true"
                 "DELETE FROM users WHERE TRUE"
                 "UPDATE users SET name='x' WHERE 1"
                 "UPDATE users SET name='x' WHERE 2=2"
                 "UPDATE users SET name='x' WHERE 1=1 RETURNING id"
                 "DELETE FROM users WHERE 1=1 -- all rows"
                 "UPDATE users SET name='x' WHERE 1=1 OR id=5"
                 "UPDATE users SET name='x' WHERE id=5 OR 1=1"
                 "UPDATE users SET name='x' WHERE (id=5 OR 1=1)"
                 "UPDATE users SET name='x' WHERE 1=1 AND TRUE"))
    (should (clutch--risky-dml-reason sql)))
  (dolist (sql '("UPDATE users SET name='x' WHERE id=1"
                 "DELETE FROM users WHERE id=1"
                 "WITH x AS (SELECT 1) UPDATE users SET name='x' WHERE id=1"
                 "UPDATE users SET name='x' WHERE 1=1 AND id=5"
                 "UPDATE users SET name='x' WHERE (id=5 OR 1=1) AND status='active'"
                 "UPDATE users SET name='x' WHERE note='1=1'"
                 "DELETE FROM users WHERE status='active'"
                 "WITH x AS (SELECT 1) SELECT * FROM x"
                 "SELECT * FROM users"))
    (should-not (clutch--risky-dml-reason sql))))

;;;; SQL parsing — statement bounds

(ert-deftest clutch-test-statement-bounds ()
  "Statement bounds should handle semicolon-delimited SQL edge cases."
  (dolist (case '(("blank lines"
                   "SELECT *\nFROM users\n\nWHERE id = 1"
                   "users"
                   "SELECT *\nFROM users\n\nWHERE id = 1")
                  ("middle statement"
                   "SELECT 1;\nSELECT 2;\nSELECT 3"
                   "SELECT 2"
                   "SELECT 2")
                  ("first statement"
                   "SELECT 1;\nSELECT 2"
                   "SELECT 1"
                   "SELECT 1")
                  ("semicolon in string"
                   "SELECT 'a;b' AS v;"
                   "SELECT"
                   "SELECT 'a;b' AS v")
                  ("semicolon in comment"
                   "SELECT 1 -- foo;\nFROM t;"
                   "SELECT"
                   "SELECT 1 -- foo;\nFROM t")
                  ("semicolon in backtick identifier"
                   "SELECT `a;b` FROM t;\nSELECT 2"
                   "FROM"
                   "SELECT `a;b` FROM t")
                  ("semicolon in bracket identifier"
                   "SELECT [a;b] FROM t;\nSELECT 2"
                   "FROM"
                   "SELECT [a;b] FROM t")
                  ("semicolon after escaped double quote"
                   "SELECT \"a\"\";b\" FROM t;\nSELECT 2"
                   "FROM"
                   "SELECT \"a\"\";b\" FROM t")
                  ("semicolon after escaped backtick"
                   "SELECT `a``;b` FROM t;\nSELECT 2"
                   "FROM"
                   "SELECT `a``;b` FROM t")
                  ("semicolon after escaped bracket"
                   "SELECT [a]];b] FROM t;\nSELECT 2"
                   "FROM"
                   "SELECT [a]];b] FROM t")))
    (pcase-let ((`(,label ,sql ,point-token ,expected) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (insert sql)
          (goto-char (point-min))
          (search-forward point-token)
          (let ((bounds (clutch--statement-bounds-at-point)))
            (should (equal (string-trim
                            (buffer-substring-no-properties
                             (car bounds) (cdr bounds)))
                           expected))))))))

(ert-deftest clutch-test-sql-context-statement-bounds ()
  "SQL context bounds should share semicolon-aware statement parsing."
  (dolist (case '(("semicolon in string"
                   "SELECT 'a;b' AS semi;\nSELECT 2"
                   "semi"
                   "SELECT 'a;b' AS semi")
                  ("semicolon in comment"
                   "SELECT 1 -- ignored;\nFROM users;\nSELECT 2"
                   "users"
                   "SELECT 1 -- ignored;\nFROM users")
                  ("blank-line fallback"
                   "SELECT 1\n\nSELECT 2"
                   "2"
                   "SELECT 2")))
    (pcase-let ((`(,label ,sql ,point-token ,expected) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (insert sql)
          (goto-char (point-min))
          (search-forward point-token)
          (pcase-let ((`(,beg . ,end) (clutch--statement-bounds)))
            (should (equal (string-trim
                            (buffer-substring-no-properties beg end))
                           expected))))))))

(ert-deftest clutch-test-execute-dwim-prefers-semicolon-statement-bounds ()
  :tags '(:smoke)
  "DWIM execution should prefer semicolon-delimited statement bounds."
  (with-temp-buffer
    (insert "INSERT INTO demo(note) VALUES (E'first line\n\nthird line');\n\nSELECT 2")
    (goto-char (point-min))
    (search-forward "third")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (sql beg end &optional _conn)
                   (setq captured
                         (list sql
                               (string-trim
                                (buffer-substring-no-properties beg end)))))))
        (clutch-execute-dwim (point) (point))
        (should (equal captured
                       '("INSERT INTO demo(note) VALUES (E'first line\n\nthird line')"
                         "INSERT INTO demo(note) VALUES (E'first line\n\nthird line')")))))))

(ert-deftest clutch-test-execute-dwim-drops-comment-only-prefix-paragraphs ()
  "DWIM execution should not send detached comment dividers before SQL at point."
  (with-temp-buffer
    (insert (concat
             "INSERT INTO audit_log VALUES (1);\n\n"
             "-------------------------------------------------\n\n"
             "-- update the subject name\n"
             "UPDATE cap_sale_subject SET subject_name = 'new' WHERE subject_id = 4;"))
    (search-backward "UPDATE cap_sale_subject")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (sql beg end &optional _conn)
                   (setq captured
                         (list sql
                               (string-trim
                                (buffer-substring-no-properties beg end)))))))
        (clutch-execute-dwim (point) (point))
        (should (equal captured
                       '("-- update the subject name\nUPDATE cap_sale_subject SET subject_name = 'new' WHERE subject_id = 4"
                         "-- update the subject name\nUPDATE cap_sale_subject SET subject_name = 'new' WHERE subject_id = 4")))))))

(ert-deftest clutch-test-execute-dwim-at-semicolon-edge ()
  "DWIM execution should handle semicolon-edge cursor positions."
  (with-temp-buffer
    (insert "SELECT 1;\n\nSELECT 2;")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (sql beg end &optional _conn)
                   (setq captured
                         (list sql
                               (string-trim
                                (buffer-substring-no-properties beg end)))))))
        (goto-char (point-min))
        (search-forward "1")
        (clutch-execute-dwim (point) (point))
        (should (equal captured '("SELECT 1" "SELECT 1")))
        (forward-char 1)
        (clutch-execute-dwim (point) (point))
        (should (equal captured '("SELECT 1" "SELECT 1")))
        (forward-char 1)
        (should-error (clutch-execute-dwim (point) (point))
                      :type 'user-error)
        (search-forward "SELECT 2")
        (goto-char (match-beginning 0))
        (clutch-execute-dwim (point) (point))
        (should (equal captured '("SELECT 2" "SELECT 2")))))))

(ert-deftest clutch-test-execute-dwim-falls-back-to-query-bounds-without-semicolons ()
  "DWIM execution should keep blank-line query parsing when no top-level semicolon exists."
  (with-temp-buffer
    (insert "SELECT 1\n\nSELECT 2")
    (goto-char (point-max))
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (sql beg end &optional _conn)
                   (setq captured
                         (list sql
                               (string-trim
                                (buffer-substring-no-properties beg end)))))))
        (clutch-execute-dwim (point) (point))
        (should (equal captured '("SELECT 2" "SELECT 2")))))))

(ert-deftest clutch-test-execute-buffer-splits-multiple-statements ()
  "Buffer execution should split semicolon-delimited statements."
  (with-temp-buffer
    (let ((sql "INSERT INTO demo VALUES (1);\nINSERT INTO demo VALUES (2);")
          executed ensured single-call)
      (insert sql)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)))
                ((symbol-function 'clutch--execute-statements)
                 (lambda (stmts)
                   (setq executed stmts)))
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (&rest _args)
                   (setq single-call t))))
        (clutch-execute-buffer)
        (should ensured)
        (should (equal (mapcar #'car executed)
                       '("INSERT INTO demo VALUES (1)"
                         "INSERT INTO demo VALUES (2)")))
        (should-not single-call)))))

(ert-deftest clutch-test-execute-buffer-rejects-empty-statements ()
  "A buffer containing only delimiters should report that it has no SQL."
  (with-temp-buffer
    (insert " ; \n ;; ")
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore))
      (should-error (clutch-execute-buffer) :type 'user-error))))

(ert-deftest clutch-test-execute-region-marks-each-successful-statement ()
  "Region execution should move the fringe marker statement by statement."
  (with-temp-buffer
    (insert "  INSERT INTO demo VALUES (1);\n\n"
            "  UPDATE demo SET seen = 1 WHERE id = 1;\n"
            "  UPDATE demo SET seen = 0 WHERE id = 2;")
    (let ((clutch-connection 'fake-conn)
          (original-marker (symbol-function 'clutch--mark-executed-sql-region))
          ensured
          starts
          marks)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql)
                   (push (list sql (overlayp clutch--executed-sql-overlay))
                         starts)
                   'ok))
                ((symbol-function 'clutch--mark-executed-sql-region)
                 (lambda (beg end)
                   (push (string-trim
                          (buffer-substring-no-properties beg end))
                         marks)
                   (funcall original-marker beg end)))
                ((symbol-function 'message) #'ignore))
        (clutch-execute-region (point-min) (point-max)))
      (should ensured)
      (should (equal (mapcar #'car (reverse starts))
                     '("INSERT INTO demo VALUES (1)"
                       "UPDATE demo SET seen = 1 WHERE id = 1"
                       "UPDATE demo SET seen = 0 WHERE id = 2")))
      (should (equal (mapcar #'cadr (reverse starts))
                     '(nil nil nil)))
      (should (equal (reverse marks)
                     '("INSERT INTO demo VALUES (1)"
                       "UPDATE demo SET seen = 1 WHERE id = 1"
                       "UPDATE demo SET seen = 0 WHERE id = 2")))
      (should (overlayp clutch--executed-sql-overlay))
      (should (= (overlay-start clutch--executed-sql-overlay)
                 (save-excursion
                   (goto-char (point-max))
                   (line-beginning-position)))))))

(ert-deftest clutch-test-execute-statements-marks-final-select ()
  "A final SELECT in a statement batch should keep its source bounds."
  (let (final-select)
    (cl-letf (((symbol-function 'clutch--run-db-query) (lambda (&rest _) 'ok))
              ((symbol-function 'clutch--execute-and-mark)
               (lambda (sql beg end &optional _conn)
                 (setq final-select (list sql beg end))))
              ((symbol-function 'message) #'ignore))
      (clutch--execute-statements
       '(("UPDATE demo SET seen = 1 WHERE id = 1" 1 25)
         ("SELECT * FROM demo" 27 45))))
    (should (equal final-select '("SELECT * FROM demo" 27 45)))))

;;;; SQL parsing — table and alias extraction

(ert-deftest clutch-test-tables-in-buffer-caches-until-buffer-changes ()
  "Table lookup in the buffer should reuse cached results until text changes."
  (with-temp-buffer
    (insert "SELECT * FROM users")
    (let ((schema (make-hash-table :test 'equal))
          first-cache)
      (puthash "users" t schema)
      (puthash "posts" t schema)
      (should (equal (clutch--tables-in-buffer schema) '("users")))
      (setq first-cache clutch--tables-in-buffer-cache)
      (should (equal (clutch--tables-in-buffer schema) '("users")))
      (should (eq clutch--tables-in-buffer-cache first-cache))
      (goto-char (point-max))
      (insert " JOIN posts")
      (should (equal (clutch--tables-in-buffer schema) '("users" "posts")))
      (should-not (eq clutch--tables-in-buffer-cache first-cache)))))

(ert-deftest clutch-test-tables-in-buffer-fallback-contract ()
  "Whole-buffer table fallback should avoid substring leaks and honor quoting."
  (dolist (case '(("identifier scan"
                   "SELECT * FROM app.Users JOIN `order_items` oi ON oi.user_id = Users.id"
                   ("users" "order_items" "app.Users" "items")
                   ("app.Users" "order_items" "users"))
                  ("quoted non-identifier"
                   "SELECT * FROM `order-items`"
                   ("order-items" "items")
                   ("order-items"))))
    (pcase-let ((`(,label ,sql ,schema-tables ,expected) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (insert sql)
          (let ((schema (make-hash-table :test 'equal)))
            (dolist (table schema-tables)
              (puthash table t schema))
            (should (equal (sort (clutch--tables-in-buffer schema)
                                 #'string<)
                           expected))))))))

(ert-deftest clutch-test-tables-in-query-caches-within-statement ()
  "Statement table lookup should reuse cached results until statement or text changes."
  (with-temp-buffer
    (insert "SELECT * FROM users JOIN posts ON users.id = posts.user_id;\n\nSELECT * FROM logs")
    (let ((schema (make-hash-table :test 'equal))
          first-cache second-cache)
      (puthash "users" t schema)
      (puthash "posts" t schema)
      (puthash "logs" t schema)
      (goto-char (point-min))
      (should (equal (sort (copy-sequence (clutch--tables-in-query schema)) #'string<)
                     '("posts" "users")))
      (setq first-cache clutch--tables-in-query-cache)
      (search-forward "users.id")
      (should (equal (sort (copy-sequence (clutch--tables-in-query schema)) #'string<)
                     '("posts" "users")))
      (should (eq clutch--tables-in-query-cache first-cache))
      (goto-char (point-max))
      (should (equal (clutch--tables-in-query schema) '("logs")))
      (setq second-cache clutch--tables-in-query-cache)
      (should-not (eq second-cache first-cache))
      (goto-char (point-max))
      (insert " WHERE level = 'error'")
      (should (equal (clutch--tables-in-query schema) '("logs")))
      (should-not (eq clutch--tables-in-query-cache second-cache)))))

(ert-deftest clutch-test-table-scan-does-not-consume-join-as-previous-table-alias ()
  "JOIN should not be consumed as the previous table's optional alias.
Otherwise the scanner skips past the JOIN token and misses the joined table."
  (with-temp-buffer
    (insert "select p.title from users join posts p on users.id = p.user_id")
    (let ((entry (clutch--compute-tables-in-query-cache nil)))
      (should (equal (plist-get entry :statement-tables)
                     '("users" "posts")))
      (should (equal (plist-get entry :statement-aliases)
                     '(("p" . "posts")))))))

(ert-deftest clutch-test-union-all-alias-scoped-to-branch ()
  "Repeated aliases in UNION ALL branches should resolve branch-locally."
  (dolist (case
           '(("top-level"
              "select t.id from users t\nunion all\nselect t.ti from posts t")
             ("inside subquery"
              "select data_view.id from (\n  select t.id from users t\n  union all\n  select t.ti from posts t\n) data_view")))
    (pcase-let ((`(,label ,sql) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (insert sql)
          (goto-char (point-min))
          (search-forward "t.ti")
          (let ((schema (make-hash-table :test 'equal))
                (clutch-connection 'fake))
            (puthash "users" '("id" "name") schema)
            (puthash "posts" '("title" "body") schema)
            (cl-letf (((symbol-function 'clutch--schema-for-connection)
                       (lambda () schema))
                      ((symbol-function 'clutch-db-busy-p)
                       (lambda (_conn) nil))
                      ((symbol-function 'clutch-db-completion-sync-columns-p)
                       (lambda (_conn) t))
                      ((symbol-function 'clutch--ensure-columns-async)
                       (lambda (&rest _args)
                         (ert-fail "should not queue async loads when branch columns are cached"))))
              (let* ((capf (clutch-completion-at-point))
                     (candidates (clutch-test--completion-candidates capf)))
                (should capf)
                (should (member "title" candidates))
                (should-not (member "name" candidates))))))))))

(ert-deftest clutch-test-extract-tables-and-aliases-contract ()
  "Table extraction should preserve tables, aliases, and scan bounds."
  (let* ((sql "SELECT a.id FROM users a JOIN orders b ON a.id = b.uid JOIN users c")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql)))
         (tables (car result))
         (aliases (cdr result)))
    (should (equal tables '("users" "orders" "users")))
    (should (equal aliases '(("a" . "users") ("b" . "orders") ("c" . "users"))))
    (should (equal (delete-dups (copy-sequence tables)) '("users" "orders"))))
  (dolist (case '(("schema-qualified without alias"
                   "SELECT * FROM test.users"
                   ("users") nil)
                  ("string literal FROM"
                   "select 'from users' from orders o"
                   ("orders") (("o" . "orders")))
                  ("quoted identifiers"
                   "select * from `order_items` oi join \"users\" u"
                   ("order_items" "users")
                   (("oi" . "order_items") ("u" . "users")))
                  ("multiline from"
                   "SELECT
    case_code
FROM
    section9_cases_wide
ORDER BY id"
                   ("section9_cases_wide") nil)))
    (pcase-let ((`(,label ,sql ,tables ,aliases) case))
      (ert-info ((format "case: %s" label))
        (let ((result (clutch--extract-tables-and-aliases
                       sql 0 (length sql))))
          (should (equal (car result) tables))
          (should (equal (cdr result) aliases))))))
  (let* ((sql "SELECT u.id FROM users u UNION ALL SELECT p.id FROM posts p")
         (end (string-match "UNION" sql))
         (real-string-match (symbol-function 'string-match))
         (calls 0))
    (cl-letf (((symbol-function 'string-match)
               (lambda (&rest args)
                 (when (> (cl-incf calls) 10)
                   (ert-fail "table extraction repeated an out-of-range match"))
                 (apply real-string-match args))))
      (should (equal (clutch--extract-tables-and-aliases sql 0 end)
                     '(("users") ("u" . "users")))))))

(ert-deftest clutch-test-statement-table-identifiers-in-sql-covers-common-dml-and-ddl ()
  "SQL-string table scanning should cover agent-context DML and DDL inputs."
  (should (equal (clutch--statement-table-identifiers-in-sql
                  "SELECT 'FROM ignored' FROM public.users u JOIN orders o ON o.user_id = u.id")
                 '("users" "orders")))
  (should (equal (clutch--statement-table-identifiers-in-sql
                  "DELETE FROM logs WHERE id = 1; ALTER TABLE users ADD COLUMN flag int;")
                 '("logs" "users")))
  (should (equal (clutch--statement-table-identifiers-in-sql
                  "CREATE UNIQUE INDEX orders_user_idx ON sales.orders (user_id);")
                 '("orders")))
  (should (equal (clutch--statement-table-identifiers-in-sql
                  "CREATE INDEX orders_user_idx\n  ON sales.orders (user_id);")
                 '("orders"))))

(ert-deftest clutch-test-source-table-detection-uses-top-level-from ()
  "Result source-table detection should ignore literals and preserve table names."
  (should (equal (clutch-db-sql-source-table
                  "SELECT 'from ignored' AS label FROM public.users u")
                 "users"))
  (should (equal (clutch-db-sql-source-table
                  "-- from ignored\nSELECT * FROM `sales`.`orders`")
                 "orders"))
  (should (equal (clutch-db-sql-source-table
                  "SELECT * FROM \"HR\".\"EMPLOYEES\"")
                 "EMPLOYEES")))

(ert-deftest clutch-test-simple-source-table-rejects-ambiguous-relations ()
  "Simple source-table detection should only accept direct single-table SELECTs."
  (should (equal (clutch-db-sql-source-table
                  "SELECT * FROM public.users u WHERE active = 1" t)
                 "users"))
  (should-not (clutch-db-sql-source-table
               "WITH x AS (SELECT * FROM users) SELECT * FROM x" t))
  (should-not (clutch-db-sql-source-table
               "SELECT * FROM users JOIN posts ON posts.user_id = users.id" t))
  (should-not (clutch-db-sql-source-table
               "SELECT * FROM users, posts" t))
  (should-not (clutch-db-sql-source-table
               "SELECT * FROM users UNION ALL SELECT * FROM posts" t))
  (should-not (clutch-db-sql-source-table
               "SELECT * FROM (SELECT * FROM users) u" t)))

;;;; SQL parsing — string and comment awareness

(ert-deftest clutch-test-skip-literal-or-comment ()
  "Skip SQL literals and comments while leaving quoted identifiers alone."
  (dolist (case '(("single quote" "'hello'" 0 7)
                  ("escaped quote" "'it''s'" 0 7)
                  ("unterminated quote" "'hello" 0 6)
                  ("plain sql" "SELECT" 0 nil)
                  ("line comment" "-- comment\ncode" 0 11)
                  ("line comment at eof" "-- comment" 0 10)
                  ("block comment" "/* block */" 0 11)
                  ("unterminated block" "/* open" 0 7)
                  ("double-quoted identifier" "\"User Table\"" 0 nil)
                  ("backtick identifier" "`user_table`" 0 nil)))
    (pcase-let ((`(,label ,sql ,pos ,expected) case))
      (ert-info ((format "case: %s" label))
        (should (equal (clutch-db-sql-skip-literal-or-comment sql pos)
                       expected))))))

(ert-deftest clutch-test-substitute-params-skips-quoted-identifiers ()
  "Parameter substitution should only consume executable placeholders."
  (should
   (equal
    (clutch-db-substitute-params
     "SELECT \"?\", `?`, [?], ?"
     '(42)
     #'number-to-string)
    "SELECT \"?\", `?`, [?], 42")))

(ert-deftest clutch-test-mask-literal-or-comment ()
  "Mask string literals and comments but preserve identifiers."
  (let ((masked (clutch-db-sql-mask-literal-or-comment
                 "select 'union all' from t -- limit")))
    (should (= (length masked) (length "select 'union all' from t -- limit")))
    ;; String content masked.
    (should (string-match-p "select '         ' from t" masked))
    ;; Comment masked.
    (should-not (string-match-p "limit" masked))
    ;; Delimiters preserved.
    (should (string-match-p "from t" masked)))
  ;; Double-quoted identifiers are NOT masked.
  (let ((masked (clutch-db-sql-mask-literal-or-comment
                 "select * from \"User Table\" u")))
    (should (string-match-p "\"User Table\"" masked)))
  ;; Multibyte string content must not trigger aset errors.
  (let* ((sql "select case when '全提' then '即存' end from t")
         (masked (clutch-db-sql-mask-literal-or-comment sql)))
    (should (= (length masked) (length sql)))
    (should (string-match-p "from t" masked))
    (should-not (string-match-p "全提" masked))
    (should-not (string-match-p "即存" masked))))

(ert-deftest clutch-test-find-top-level-clause-skips-literals-and-comments ()
  "Top-level clause search should ignore text hidden inside literals/comments."
  (dolist (case '(("string only" "select 'limit 10' from t" nil)
                  ("block comment then real clause"
                   "select /* limit */ from t limit 5" t)
                  ("line comment then real clause"
                   "select -- limit\nfrom t limit 5" t)))
    (pcase-let ((`(,label ,sql ,expected) case))
      (ert-info ((format "case: %s" label))
        (let ((found (clutch-db-sql-find-top-level-clause sql "LIMIT")))
          (if expected
              (should found)
            (should-not found)))))))

;;;; SQL parsing — WHERE and COUNT rewriting

(ert-deftest clutch-test-ensure-where-guard-blocks-missing-where ()
  "Generated DML statements must contain top-level WHERE."
  (should-error
   (clutch-result--ensure-where-guard '("UPDATE t SET x=1") "UPDATE")
   :type 'user-error)
  (should-error
   (clutch-result--ensure-where-guard '("DELETE FROM t") "DELETE")
   :type 'user-error)
  (should (null (clutch-result--ensure-where-guard
                 '("UPDATE t SET x=1 WHERE id=1" "DELETE FROM t WHERE id=1")
                 "UPDATE"))))

(ert-deftest clutch-test-apply-where-rewrites-selects ()
  "WHERE rewrite should wrap supported SELECT shapes without changing bounds."
  (dolist (case
           (list
            (list :label "simple"
                  :sql "SELECT * FROM t"
                  :filter "id = 1"
                  :equal "SELECT * FROM (SELECT * FROM t) AS _clutch_filter WHERE id = 1")
            (list :label "existing WHERE"
                  :sql "SELECT * FROM t WHERE x > 0"
                  :filter "id = 1"
                  :matches '("FROM (SELECT \\* FROM t WHERE x > 0)"
                             "WHERE id = 1\\'"))
            (list :label "CTE"
                  :sql "WITH x AS (SELECT id FROM t) SELECT * FROM x"
                  :filter "id > 10"
                  :matches '("^SELECT \\* FROM (WITH x AS"
                             "WHERE id > 10\\'"))
            (list :label "UNION"
                  :sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b)"
                  :filter "id > 10"
                  :matches '("^SELECT \\* FROM (.*UNION ALL.*) AS _clutch_filter"
                             "WHERE id > 10\\'"))
            (list :label "comments and semicolon"
                  :sql "-- head comment\n/* block */\nSELECT id FROM t;"
                  :filter "id > 10"
                  :prefix "SELECT * FROM (SELECT id FROM t) AS _clutch_filter WHERE id > 10"
                  :not-matches '(";\\s-*) AS _clutch_filter"))
            (list :label "unbounded ORDER BY"
                  :sql "SELECT id, name FROM users ORDER BY created_at DESC"
                  :filter "id > 10"
                  :equal "SELECT * FROM (SELECT id, name FROM users) AS _clutch_filter WHERE id > 10")
            (list :label "bounded ORDER BY"
                  :sql "SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20"
                  :filter "id > 10"
                  :equal "SELECT * FROM (SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20) AS _clutch_filter WHERE id > 10")))
    (ert-info ((format "apply WHERE: %s" (plist-get case :label)))
      (let ((result (clutch-db-apply-where
                     'fake-conn
                     (plist-get case :sql)
                     (plist-get case :filter))))
        (when (plist-get case :equal)
          (should (equal result (plist-get case :equal))))
        (when (plist-get case :prefix)
          (should (string-prefix-p (plist-get case :prefix) result)))
        (dolist (pattern (plist-get case :matches))
          (should (string-match-p pattern result)))
        (dolist (pattern (plist-get case :not-matches))
          (should-not (string-match-p pattern result)))))))

(ert-deftest clutch-test-count-filtered-ordered-query-strips-inner-order-by ()
  "COUNT over filtered SQL should not keep an invalid inner ORDER BY."
  (let* ((filtered (clutch-db-apply-where
                    'fake-conn
                    "SELECT id, name FROM users ORDER BY created_at DESC"
                    "id > 10"))
         (result (clutch-db-build-count-sql 'fake-conn filtered)))
    (should (string-prefix-p
             "SELECT COUNT(*) FROM (SELECT * FROM (SELECT id, name FROM users)"
             result))
    (should-not (string-match-p "ORDER BY created_at" result))))

(ert-deftest clutch-test-build-count-sql-rewrites-selects ()
  "Count SQL should wrap supported SELECT shapes while preserving bounds."
  (dolist (case
           (list
            (list :label "limit offset"
                  :sql "SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20"
                  :matches '("^SELECT COUNT(\\*) FROM (SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20) AS _clutch_count\\'"))
            (list :label "CTE"
                  :sql "WITH x AS (SELECT id FROM t ORDER BY id) SELECT * FROM x ORDER BY id"
                  :matches '("^SELECT COUNT(\\*) FROM (WITH x AS"
                             ") AS _clutch_count\\'")
                  :not-matches '("ORDER BY id\\s-*) AS _clutch_count\\'"))
            (list :label "DISTINCT"
                  :sql "SELECT DISTINCT user_id FROM visits ORDER BY user_id"
                  :matches '("^SELECT COUNT(\\*) FROM (SELECT DISTINCT user_id FROM visits) AS _clutch_count\\'"))
            (list :label "inner ORDER BY"
                  :sql "SELECT * FROM (SELECT id FROM t ORDER BY created_at DESC) s ORDER BY id"
                  :matches '("SELECT id FROM t ORDER BY created_at DESC"
                             "AS _clutch_count\\'")
                  :not-matches '("ORDER BY id\\s-*) AS _clutch_count\\'"))
            (list :label "UNION top ORDER BY"
                  :sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b) ORDER BY id"
                  :matches '("UNION ALL")
                  :not-matches '("ORDER BY id\\s-*) AS _clutch_count\\'"))
            (list :label "UNION limit offset"
                  :sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b) LIMIT 50 OFFSET 100"
                  :matches '("UNION ALL"
                             "LIMIT 50\\s-+OFFSET 100\\s-*) AS _clutch_count\\'"))
            (list :label "window ORDER BY"
                  :sql "SELECT row_number() OVER (ORDER BY created_at DESC) AS rn FROM t ORDER BY rn LIMIT 5"
                  :matches '("OVER (ORDER BY created_at DESC)"
                             "ORDER BY rn\\s-+LIMIT 5\\s-*) AS _clutch_count\\'"))
            (list :label "trailing semicolon"
                  :sql "SELECT * FROM users;"
                  :matches '("SELECT \\* FROM users")
                  :not-matches '(";\\s-*) AS _clutch_count\\'"))
            (list :label "leading comments"
                  :sql "-- comment\n/* block */\nSELECT id FROM t ORDER BY id"
                  :prefix "SELECT COUNT(*) FROM (SELECT id FROM t)"
                  :not-matches '("ORDER BY id\\s-*) AS _clutch_count\\'"))))
    (ert-info ((format "COUNT rewrite: %s" (plist-get case :label)))
      (let ((result (clutch-db-build-count-sql 'fake-conn
                                                (plist-get case :sql))))
        (when (plist-get case :prefix)
          (should (string-prefix-p (plist-get case :prefix) result)))
        (dolist (pattern (plist-get case :matches))
          (should (string-match-p pattern result)))
        (dolist (pattern (plist-get case :not-matches))
          (should-not (string-match-p pattern result)))))))

(ert-deftest clutch-test-oracle-derived-table-alias-omits-as ()
  "Oracle SQL rewrites should not use AS before derived-table aliases."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver oracle))))
    (should (equal (clutch-db-build-count-sql
                    clutch-connection
                    "SELECT id FROM t FETCH FIRST 50 ROWS ONLY")
                   "SELECT COUNT(*) FROM (SELECT id FROM t FETCH FIRST 50 ROWS ONLY) clutch_count"))
    (should (equal (clutch-db-apply-where clutch-connection
                                          "SELECT * FROM t"
                                          "id = 1")
                   "SELECT * FROM (SELECT * FROM t) clutch_filter WHERE id = 1"))))

;;;; SQL parsing — LIMIT detection and paging SQL

(ert-deftest clutch-test-db-sql-has-top-level-limit-p ()
  "Test LIMIT clause detection."
  (should (clutch-db-sql-has-top-level-limit-p "SELECT * FROM t LIMIT 10"))
  (should (clutch-db-sql-has-top-level-limit-p "select * from t limit 10"))
  (should (clutch-db-sql-has-top-level-limit-p
           "SELECT * FROM t WHERE x=1 LIMIT 5 OFFSET 10"))
  (should (clutch-db-sql-has-top-level-limit-p
           "(SELECT id FROM a) UNION ALL (SELECT id FROM b) LIMIT 20"))
  (should-not (clutch-db-sql-has-top-level-limit-p
               "SELECT * FROM (SELECT * FROM t LIMIT 5) AS s"))
  (should-not (clutch-db-sql-has-top-level-limit-p
               "(SELECT id FROM a LIMIT 1) UNION ALL (SELECT id FROM b)"))
  (should-not (clutch-db-sql-has-top-level-limit-p
               "WITH x AS (SELECT * FROM t LIMIT 3) SELECT * FROM x"))
  (should-not (clutch-db-sql-has-top-level-limit-p "SELECT * FROM t"))
  (should-not (clutch-db-sql-has-top-level-limit-p
               "SELECT * FROM t WHERE limitation = 1")))


;;;; Completion — SQL keywords

(ert-deftest clutch-test-sql-keyword-completion-contract ()
  "Keyword CAPF should expose candidates, casing, phrases, and exit behavior."
  (dolist (case '(("lower prefix" "sel" ("SELECT"))
                  ("blank" " " nil)))
    (pcase-let ((`(,label ,input ,expected) case))
      (ert-info ((format "basic: %s" label))
        (with-temp-buffer
          (insert input)
          (let ((result (clutch-sql-keyword-completion-at-point)))
            (if expected
                (let ((candidates (clutch-test--completion-candidates result)))
                  (should result)
                  (dolist (candidate expected)
                    (should (member candidate candidates))))
              (should-not result)))))))
  (dolist (case '(("zero-arg function" "cur" "CURRENT_DATABASE()")
                  ("keyword prefix" "sele" "SELECT")))
    (pcase-let ((`(,label ,input ,expected) case))
      (ert-info ((format "installed CAPF: %s" label))
        (with-temp-buffer
          (insert input)
          (let (captured)
            (setq-local completion-at-point-functions nil)
            (clutch--install-completion-capfs)
            (cl-letf ((completion-in-region-function
                       (lambda (start end collection &optional predicate)
                         (setq captured
                               (all-completions
                                (buffer-substring-no-properties start end)
                                collection predicate))
                         t)))
              (should (completion-at-point))
              (should (member expected captured))))))))
  (let ((clutch-sql-completion-case-style 'lower))
    (dolist (case '(("sel" "select" "SELECT")
                    ("gro" "group by" "GROUP BY")))
      (pcase-let ((`(,input ,expected ,rejected) case))
        (ert-info ((format "lowercase style: %s" input))
          (with-temp-buffer
            (insert input)
            (let* ((result (clutch-sql-keyword-completion-at-point))
                   (candidates (clutch-test--completion-candidates result)))
              (should result)
              (should (member expected candidates))
              (should-not (member rejected candidates))))))))
  (dolist (case '(("gro" ("GROUP BY") ("GROUP"))
                  ("ord" ("ORDER BY") ("ORDER"))
                  ("par" ("PARTITION BY") ("PARTITION"))
                  ("pri" ("PRIMARY KEY") ("PRIMARY"))
                  ("for" ("FOREIGN KEY") ("FOREIGN"))
                  ("inn" ("INNER" "INNER JOIN") nil)
                  ("lef" ("LEFT" "LEFT JOIN") nil)
                  ("rig" ("RIGHT" "RIGHT JOIN") nil)
                  ("cro" ("CROSS" "CROSS JOIN") nil)
                  ("uni" ("UNION" "UNION ALL") nil)
                  ("is" ("IS" "IS NULL" "IS NOT NULL") nil)
                  ("not" ("NOT" "NOT NULL") nil)))
    (pcase-let ((`(,prefix ,expected ,rejected) case))
      (ert-info ((format "phrase: %s" prefix))
        (with-temp-buffer
          (insert prefix)
          (let* ((result (clutch-sql-keyword-completion-at-point))
                 (candidates (clutch-test--completion-candidates result)))
            (dolist (candidate expected)
              (should (member candidate candidates)))
            (dolist (candidate rejected)
              (should-not (member candidate candidates))))))))
  (dolist (case '(("plain keyword" "FROM" "FROM" "FROM ")
                  ("function call" "cur" "CURRENT_DATABASE()" "cur")))
    (pcase-let ((`(,label ,input ,accepted ,expected) case))
      (ert-info ((format "exit function: %s" label))
        (with-temp-buffer
          (insert input)
          (let* ((capf (clutch-sql-keyword-completion-at-point))
                 (exit-fn (plist-get (cdddr capf) :exit-function)))
            (funcall exit-fn accepted 'exact)
            (should (equal (buffer-string) expected))))))))

;;;; Completion — identifiers and columns

(ert-deftest clutch-test-completion-at-point-keyword-phrase-exit-inserts-space ()
  "Connection-aware completion should space accepted keyword phrases."
  (with-temp-buffer
    (insert "select * from users gro")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "users" '("id") schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t)))
        (let* ((capf (clutch-completion-at-point))
               (exit-fn (plist-get (cdddr capf) :exit-function)))
          (delete-region (car capf) (cadr capf))
          (insert "GROUP BY")
          (funcall exit-fn "GROUP BY" 'finished)
          (should (equal (buffer-string)
                         "select * from users GROUP BY ")))))))

(ert-deftest clutch-test-completion-at-point-uses-current-columns-at-empty-sql-slots ()
  "CAPF should complete visible columns at empty SQL expression positions."
  (dolist (sql '("select | from users u join orders o on u.id = o.user_id"
                 "select * from users u join orders o on u.id = o.user_id where |"
                 "select * from users u join orders o on u.id = o.user_id group by |"
                 "select * from users u join orders o on u.id = o.user_id order by |"
                 "select * from users u join orders o on |"))
    (with-temp-buffer
      (clutch-mode)
      (insert sql)
      (goto-char (point-min))
      (search-forward "|")
      (delete-char -1)
      (let ((schema (make-hash-table :test 'equal))
            (clutch-connection 'fake)
            captured
            annotation-function)
        (puthash "users" '("id" "name") schema)
        (puthash "orders" '("id" "user_id") schema)
        (cl-letf (((symbol-function 'clutch--schema-for-connection)
                   (lambda (&optional _conn) schema))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-completion-sync-columns-p)
                   (lambda (_conn) t))
                  (completion-in-region-function
                   (lambda (_start _end collection &optional predicate)
                     (setq captured (all-completions "" collection predicate)
                           annotation-function
                           (plist-get completion-extra-properties
                                     :annotation-function))
                     t)))
          (let* ((capf (clutch-completion-at-point))
                 (props (nthcdr 3 capf))
                 (annotation-function (plist-get props :annotation-function)))
            (should capf)
            (should (equal (all-completions "" (nth 2 capf)
                                            (plist-get props :predicate))
                           '("u.id" "u.name" "o.id" "o.user_id")))
            (should annotation-function)
            (should (equal (substring-no-properties
                            (funcall annotation-function "u.id"))
                           "  u (users)")))
          (let ((command (local-key-binding (kbd "C-c TAB"))))
            (let ((this-command command))
              (call-interactively command)))
          (should (equal captured '("u.id" "u.name" "o.id" "o.user_id")))
          (should annotation-function)
          (should (equal (substring-no-properties
                          (funcall annotation-function "u.id"))
                         "  u (users)")))))))

(ert-deftest clutch-test-complete-at-point-keeps-empty-column-completion-active ()
  "Manual empty-column completion should keep its completion region valid."
  (with-temp-buffer
    (clutch-mode)
    (insert "select  from users")
    (goto-char (point-min))
    (search-forward "select ")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          completion-predicate)
      (puthash "users" '("id" "name") schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                (completion-in-region-function
                 (lambda (_start _end _collection &optional _predicate)
                   (setq completion-predicate
                         completion-in-region-mode-predicate)
                   t)))
        (call-interactively #'clutch-complete-at-point)
        (should completion-predicate)
        (should (funcall completion-predicate))))))

(ert-deftest clutch-test-insert-completion-at-point-uses-enum-candidates ()
  "Insert buffer completion should return enum candidates for the current field."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")) insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "is_ship_blocked" "owner")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "is_ship_blocked" :type-category numeric)
                                                     (:name "owner" :type-category text))
                        clutch--result-source-table "shipping_incidents"))
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity" :type "enum('low','medium','high','critical')")
                               (list :name "is_ship_blocked" :type "tinyint(1)")
                               (list :name "owner" :type "varchar(255)"))))
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args) (setq insert-buf buf) buf)))
              (clutch-result-insert--open-buffer
               "shipping_incidents" result-buf '(("owner" . "alice")))
              (with-current-buffer insert-buf
              (clutch-test--goto-insert-field-value "severity" t)
              (pcase-let ((`(,beg ,end ,candidates . ,_)
                           (clutch-result-insert-completion-at-point)))
                (should (= beg end))
                (should (equal candidates '("low" "medium" "high" "critical"))))
              (clutch-test--goto-insert-field-value "is_ship_blocked" t)
              (pcase-let ((`(,_beg ,_end ,candidates . ,_)
                           (clutch-result-insert-completion-at-point)))
                (should (equal candidates '("0" "1"))))
              (clutch-test--goto-insert-field-value "owner" t)
              (should-not (clutch-result-insert-completion-at-point)))))
      (when (buffer-live-p result-buf) (kill-buffer result-buf))
      (when (buffer-live-p insert-buf) (kill-buffer insert-buf)))))

(ert-deftest clutch-test-insert-complete-field-falls-back-to-completing-read ()
  "Insert completion command should fall back when CAPF does not handle it."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (insert-buf (generate-new-buffer "*clutch-insert-temp*"))
        completion-called)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity")
                        clutch--result-column-defs '((:name "severity" :type-category text))))
          (cl-letf (((symbol-function 'completion-at-point) (lambda () nil))
                    ((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity"
                                     :type "enum('low','medium','high','critical')"))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) (setq insert-buf buf) buf))
                      ((symbol-function 'completing-read)
                       (lambda (&rest _args)
                         (setq completion-called t)
                         "critical")))
            (with-current-buffer result-buf
              (setq-local clutch--result-source-table "shipping_incidents"))
            (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
            (with-current-buffer insert-buf
              (clutch-test--goto-insert-field-value "severity")
              (clutch-result-insert-complete-field)
              (should completion-called)
              (should (equal
                       (plist-get (clutch-result-insert--field-state "severity")
                                  :value)
                       "critical")))))
      (kill-buffer result-buf)
      (kill-buffer insert-buf))))

(ert-deftest clutch-test-complete-field-propagates-capf-errors ()
  "Edit and insert completion commands should not swallow CAPF failures."
  (dolist (case '((edit clutch-result-edit-complete-field)
                  (insert clutch-result-insert-complete-field)))
    (pcase-let ((`(,mode ,command) case))
      (ert-info ((format "mode: %s" mode))
        (with-temp-buffer
          (pcase mode
            ('edit
             (setq-local clutch-result-edit--column-name "severity")
             (clutch--result-edit-mode 1))
            ('insert
             (insert "severity: \n")
             (clutch--result-insert-major-mode)
             (clutch-test--goto-insert-field-value "severity")))
          (cl-letf (((symbol-function 'completion-at-point)
                     (lambda ()
                       (error "CAPF boom"))))
            (should-error (funcall command) :type 'error)))))))

(ert-deftest clutch-test-completion-capf-real-sqlite-workflow ()
  "Exercise the installed SQL CAPFs against real SQLite metadata."
  (skip-unless (and (fboundp 'sqlite-available-p) (sqlite-available-p)))
  (clutch-test--with-isolated-metadata-caches
    (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
           (schema (make-hash-table :test 'equal)))
      (unwind-protect
          (progn
            (dolist (ddl '("create table users (id integer, name text)"
                           "create table posts (id integer, title text)"
                           "create table orders (order_id integer, user_id integer)"
                           "create table user_roles (id integer)"
                           "create table xaccounts (id integer)"
                           "create table APP_CONFIG (CONFIG_ID integer, CONFIG_NAME text)"))
              (clutch-db-query conn ddl))
            (dolist (table '("users" "posts" "orders" "user_roles"
                             "xaccounts" "APP_CONFIG"))
              (puthash table (clutch-db-list-columns conn table) schema))
            (dolist (table '("orders" "xaccounts"))
              (puthash table nil schema))
            (puthash conn schema clutch--schema-cache)
            (puthash conn '(:state ready) clutch--schema-status-cache)
            (dolist (case
                     '((short "x" nil preserve (:exact ("xaccounts")) nil)
                       (from "select * from or" nil preserve
                             (:exact ("orders")) nil)
                       (qualified
                        "select * from users u join posts p on u."
                        nil preserve (:exact ("id" "name")) t)
                       (keyword "sele from users" "sele" preserve
                                (:members ("SELECT")) nil)
                       (cache-miss
                        "select ord from users join orders on users.id = orders.user_id"
                        "ord" preserve (:members ("ORDER BY" "order_id")) nil)
                       (lowercase "select con from APP_CONFIG" "con" lower
                                  (:members ("concat" "constraint" "config_id" "config_name")
                                   :absent ("CONFIG_ID" "CONFIG_NAME"))
                                  nil)))
              (pcase-let ((`(,label ,sql ,needle ,style ,expected ,company) case))
                (ert-info ((format "case: %s" label))
                  (with-temp-buffer
                    (clutch-mode)
                    (setq-local clutch-connection conn)
                    (insert sql)
                    (if needle
                        (progn
                          (goto-char (point-min))
                          (search-forward needle))
                      (goto-char (point-max)))
                    (let* ((clutch-sql-completion-case-style style)
                           (captured nil)
                           (completion-in-region-function
                            (lambda (start end collection &optional predicate)
                              (setq captured
                                    (all-completions
                                     (buffer-substring-no-properties start end)
                                     collection predicate))
                              t)))
                      (should (completion-at-point))
                      (if-let* ((exact (plist-get expected :exact)))
                          (should (equal captured exact))
                        (dolist (candidate (plist-get expected :members))
                          (should (member candidate captured))))
                      (dolist (candidate (plist-get expected :absent))
                        (should-not (member candidate captured)))
                      (when company
                        (should
                         (eq (plist-get (nthcdr 3 (clutch-completion-at-point))
                                        :company-prefix-length)
                             t))))))))
            (should-not (gethash "xaccounts" schema))
            (should (equal (gethash "orders" schema) '("order_id" "user_id"))))
        (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-completion-backend-source-effect-matrix ()
  "Keep cached, direct-table, and direct-column completion sources distinct."
  (clutch-test--with-isolated-metadata-caches
    (let* ((conn (make-clutch-jdbc-conn :driver 'oracle))
           (schema (make-hash-table :test 'equal))
           table-rpc-result table-rpc-prefixes column-rpc-calls)
      (puthash conn schema clutch--schema-cache)
      (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-complete-tables)
                 (lambda (_conn prefix)
                   (when (eq table-rpc-result 'forbidden)
                     (ert-fail "ready cache hit must avoid table RPC"))
                   (push prefix table-rpc-prefixes)
                   table-rpc-result))
                ((symbol-function 'clutch-db-complete-columns)
                 (lambda (_conn table prefix)
                   (push (cons table prefix) column-rpc-calls)
                   (pcase table
                     ("APP_CONFIG" '("CONFIG_ID" "CONFIG_NAME"))
                     ("APP_LOG" '("PAYLOAD")))))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (ert-fail "sync-disabled completion must not ensure columns"))))
              (dolist (case
                       '((ready-hit "select * from us" "USERS" ready forbidden
                                    ("USERS") nil)
                         (ready-miss "select * from or" "AUDIT_LOG" ready
                                     ("ORDERS") ("ORDERS") "or")
                         (stale-cache "select * from us" "USERS_OLD" stale
                                      ("USERS_NEW")
                                      ("USERS_NEW" "USERS_OLD") "us")))
                (pcase-let ((`(,label ,sql ,cached-table ,state ,rpc-result
                                      ,expected ,rpc-prefix) case))
                  (ert-info ((format "case: %s" label))
                    (clrhash schema)
                    (puthash cached-table nil schema)
                    (puthash conn (list :state state) clutch--schema-status-cache)
                    (setq table-rpc-result rpc-result
                          table-rpc-prefixes nil)
                    (with-temp-buffer
                      (clutch-mode)
                      (setq-local clutch-connection conn)
                      (insert sql)
                      (goto-char (point-max))
                      (let* ((capf (clutch-completion-at-point))
                             (candidates (clutch-test--completion-candidates capf)))
                        (should capf)
                        (should (equal candidates expected))
                        (if rpc-prefix
                            (should (equal table-rpc-prefixes
                                           (list rpc-prefix)))
                          (should-not table-rpc-prefixes)))))))
              (clrhash schema)
              (puthash "APP_CONFIG" nil schema)
              (puthash "APP_LOG" nil schema)
              (puthash conn '(:state ready) clutch--schema-status-cache)
              (setq table-rpc-result 'forbidden
                    column-rpc-calls nil)
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn)
                (insert "select u.con from APP_CONFIG u join APP_LOG p on u.id = p.config_id")
                (goto-char (point-min))
                (search-forward "con")
                (let* ((capf (clutch-completion-at-point))
                       (candidates (clutch-test--completion-candidates capf)))
                  (should capf)
                  (dolist (candidate '("CONFIG_ID" "CONFIG_NAME"))
                    (should (member candidate candidates)))
                  (dolist (candidate '("PAYLOAD" "APP_CONFIG" "APP_LOG"))
                    (should-not (member candidate candidates)))
                  (should (equal column-rpc-calls
                                 '(("APP_CONFIG" . "con"))))))))))

(ert-deftest clutch-test-completion-oracle-i18n-fails-soft-once ()
  "Oracle i18n completion errors should warn once and remain non-fatal."
  (clutch-test--with-isolated-metadata-caches
    (let ((conn (make-clutch-db-sqlite-conn))
          (clutch--oracle-i18n-warning-shown nil)
          warned)
      (with-temp-buffer
        (clutch-mode)
        (setq-local clutch-connection conn)
        (insert "select * from app_")
        (goto-char (point-max))
        (cl-letf (((symbol-function 'clutch-db-complete-tables)
                   (lambda (_conn _prefix)
                     (signal
                      'clutch-db-error
                      '("Non supported character set (add orai18n.jar in your classpath): ZHS16GBK"))))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (setq warned (apply #'format format-string args)))))
          (should-not (clutch-completion-at-point))
          (should (string-match-p "orai18n.jar" warned))
          (setq warned nil)
          (should-not (clutch-completion-at-point))
          (should-not warned))))))

;;;; Eldoc — schema and column info

(ert-deftest clutch-test-eldoc-metadata-plan-contract ()
  "Eldoc metadata policy should be a pure table of explicit steps."
  (dolist (case
           '((table-cached "users" nil nil nil (("users" "id" "name"))
                           ((table-summary "users" ("id" "name"))))
             (table-warmup "users" nil nil t (("users"))
                           ((queue-column "users") (table-summary "users" nil)))
             (short "i" nil ("users") t (("users")) ((skip)))
             (large "id" nil ("a" "b" "c" "d") t nil ((skip)))
             (qualified "id" "users" ("a" "b" "c" "d") t (("users"))
                        ((sync-column "users")))
             (cached "ID" nil ("users") nil (("users" "id" "name"))
                     ((cached-column "users" ("id" "name"))))
             (sync "id" nil ("users") t (("users"))
                   ((sync-column "users")))
             (sync-disabled "id" nil ("users") nil (("users"))
                            ((skip "users")))))
    (pcase-let ((`(,label ,sym ,qualified ,tables ,sync ,entries ,expected) case))
      (ert-info ((format "case: %s" label))
        (let ((schema (make-hash-table :test 'equal)))
          (dolist (entry entries)
            (puthash (car entry) (cdr entry) schema))
          (should (equal (clutch--eldoc-metadata-plan
                          schema sym qualified tables sync)
                         expected)))))))

(ert-deftest clutch-test-eldoc-effect-and-public-entry-contract ()
  "Eldoc should execute metadata plans through its real SQL entry."
  (let (schema sync busy loads called ensure-error column-type comment
               comment-cached queued-columns queued-comments)
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema))
              ((symbol-function 'clutch-db-busy-p) (lambda (_conn) busy))
              ((symbol-function 'clutch-db-live-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-completion-sync-columns-p)
               (lambda (_conn) sync))
              ((symbol-function 'clutch--ensure-columns)
               (lambda (_conn _schema table)
                 (setq called table)
                 (if ensure-error
                     (signal 'clutch-db-error '("column load failed"))
                   (cdr (assoc table loads)))))
              ((symbol-function 'clutch--ensure-columns-async)
               (lambda (_conn _schema table) (push table queued-columns)))
              ((symbol-function 'clutch--cached-table-comment)
               (lambda (&rest _args) comment))
              ((symbol-function 'clutch--table-comment-cached-p)
               (lambda (&rest _args) comment-cached))
              ((symbol-function 'clutch--ensure-table-comment-async)
               (lambda (_conn table) (push table queued-comments)))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "testdb"))
              ((symbol-function 'clutch--eldoc-column-string)
               (lambda (_conn table column)
                 (format "%s.%s %s" table column (or column-type "bigint")))))
      (setq schema (make-hash-table :test 'equal)
            sync t comment-cached nil)
      (puthash "users" nil schema)
      (let ((doc (clutch--eldoc-schema-string 'fake schema "users")))
        (should (string-match-p "users" doc))
        (should (equal queued-columns '("users")))
        (should (equal queued-comments '("users"))))
      (dolist (case
               (list
                (list "SELECT * FROM test.users;" "test.users" 0
                      '(("users" "id")) nil nil nil '("users" "1 col"))
                (list "SELECT ID FROM users;" "ID" 0
                      '(("users" "id")) nil nil "users.id bigint" nil)
                (list (concat "SELECT d.id FROM accounts a JOIN customers c "
                              "ON c.account_id=a.id JOIN invoices i ON "
                              "i.customer_id=c.id JOIN orders_large d ON "
                              "d.invoice_id=i.id")
                      "d.id" 2
                      '(("accounts") ("customers") ("invoices") ("orders_large"))
                      t '(("orders_large" "id" "invoice_id"))
                      "orders_large.id bigint" nil)
                (list "SELECT\n case_code, operative_name\nFROM\n section9_cases_wide"
                      "case_code" 0 '(("section9_cases_wide")) t
                      '(("section9_cases_wide" "case_code" "operative_name"))
                      "section9_cases_wide.case_code text" nil)))
        (pcase-let ((`(,sql ,needle ,offset ,entries ,case-sync ,case-loads
                            ,expected ,matches) case))
          (setq schema (make-hash-table :test 'equal)
                sync case-sync loads case-loads called nil busy nil
                column-type (and (string-match-p "section9" sql) "text")
                comment-cached t)
          (dolist (entry entries)
            (puthash (car entry) (cdr entry) schema))
          (with-temp-buffer
            (clutch-mode)
            (insert sql)
            (goto-char (point-min))
            (search-forward needle)
            (goto-char (+ (match-beginning 0) offset))
            (setq-local clutch-connection 'fake)
            (let ((doc (clutch--eldoc-function)))
              (if expected
                  (should (equal doc expected))
                (dolist (pattern matches) (should (string-match-p pattern doc))))))))
      (setq schema (make-hash-table :test 'equal) sync t busy t called nil)
      (puthash "users" nil schema)
      (with-temp-buffer
        (insert "SELECT id FROM users")
        (goto-char 8)
        (setq-local clutch-connection 'fake)
        (should-not (clutch--eldoc-function))
        (should-not called))
      (setq busy nil ensure-error t)
      (should-error (clutch--eldoc-schema-string 'fake schema "id" "users")
                    :type 'clutch-db-error))))

;;;; Xref — alias jump

(ert-deftest clutch-test-xref-alias-jump-contract ()
  "Alias xref should jump to the scoped source alias definition."
  (dolist (case
           (list
            (list :label "where alias"
                  :sql "SELECT u.* FROM users u WHERE u.id = 1"
                  :needle "u.id"
                  :schema 'fake-schema
                  :aliases '(("u" . "users"))
                  :tables '("users")
                  :id "u"
                  :before "users "
                  :char ?u)
            (list :label "qualified field"
                  :sql "SELECT u.name FROM users u"
                  :needle "u.name"
                  :offset 2
                  :schema 'fake-schema
                  :aliases '(("u" . "users"))
                  :tables '("users")
                  :id "u"
                  :before "users "
                  :char ?u)
            (list :label "join alias"
                  :sql "SELECT o.total FROM users u JOIN orders o ON o.uid = u.id"
                  :needle "o.total"
                  :schema 'fake-schema
                  :aliases '(("u" . "users") ("o" . "orders"))
                  :tables '("users" "orders")
                  :id "o"
                  :before "orders "
                  :char ?o)
            (list :label "union branch with cache"
                  :sql "SELECT a.id FROM users a\nUNION ALL\nSELECT a.id FROM orders a"
                  :pre-needle "UNION ALL\n"
                  :needle "a.id"
                  :schema 'fake-schema
                  :aliases '(("a" . "users") ("a" . "orders"))
                  :tables '("users" "orders")
                  :id "a"
                  :before "orders "
                  :char ?a)
            (list :label "alias inside function call"
                  :sql (concat "SELECT SUM(opd.goods_num) FROM\n"
                               "    order_plan_detail opd WHERE opd.id = 1")
                  :needle "SUM(opd"
                  :offset 4
                  :id "opd"
                  :text "opd"
                  :before "order_plan_detail ")
            (list :label "multiline FROM"
                  :sql (concat
                        "SELECT\n"
                        "    opd.goods_num\n"
                        "FROM\n"
                        "    order_plan_detail opd\n"
                        "    LEFT JOIN order_plan op ON op.plan_id = opd.plan_id\n"
                        "WHERE\n"
                        "    opd.plan_id = 1")
                  :needle "opd.plan_id"
                  :id "opd"
                  :text "opd"
                  :before "order_plan_detail "
                  :char ?o)
            (list :label "no schema cache"
                  :sql "SELECT u.name FROM users u WHERE u.id = 1"
                  :needle "u.id"
                  :connection-alive nil
                  :id "u"
                  :before "users "
                  :char ?u)
            (list :label "no schema cache union branch"
                  :sql "SELECT a.id FROM users a\nUNION ALL\nSELECT a.id FROM orders a"
                  :pre-needle "UNION ALL\n"
                  :needle "a.id"
                  :id "a"
                  :before "orders "
                  :char ?a)
            (list :label "quoted alias"
                  :sql "SELECT \"u\".name FROM users \"u\" WHERE \"u\".id = 1"
                  :needle "\"u\".id"
                  :offset 1
                  :id "u"
                  :char ?\")
            (list :label "quoted multiword alias"
                  :sql (concat
                        "SELECT \"User Name\".id FROM users \"User Name\" "
                        "WHERE \"User Name\".id = 1")
                  :needle ".id"
                  :offset -2
                  :id "User Name"
                  :char ?\")))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (clutch-test-sql--with-xref-buffer case
        (let* ((id (xref-backend-identifier-at-point 'clutch))
               (pos (clutch-test-sql--xref-definition-position id)))
          (should (equal id (plist-get case :id)))
          (when-let* ((char (plist-get case :char)))
            (should (eq (char-after pos) char)))
          (when-let* ((text (plist-get case :text)))
            (should (equal (buffer-substring-no-properties
                            pos (+ pos (length text)))
                           text)))
          (when-let* ((before (plist-get case :before)))
            (goto-char pos)
            (should (looking-back before
                                  (max (point-min) (- pos 40))))))))))

(ert-deftest clutch-test-xref-non-alias-contract ()
  "Xref should not treat source tables, strings, comments, or fields as aliases."
  (dolist (case
           (list
            (list :label "field name"
                  :sql "SELECT name FROM users u"
                  :needle "name"
                  :schema 'fake-schema
                  :aliases '(("u" . "users"))
                  :tables '("users")
                  :id "name"
                  :no-definitions t)
            (list :label "schema-qualified table"
                  :sql "SELECT * FROM app_schema.order_items"
                  :needle "order_items"
                  :id "order_items"
                  :error "source table")
            (list :label "source table"
                  :sql "SELECT * FROM customer_accounts"
                  :needle "customer_accounts"
                  :find "customer_accounts"
                  :error "source table")
            (list :label "comment"
                  :sql "SELECT * FROM users u -- use u.id here\nWHERE u.id = 1"
                  :needle "u.id here"
                  :no-definitions t)
            (list :label "string"
                  :sql "SELECT 'u.id' AS note FROM users u WHERE u.id = 1"
                  :needle "u.id'"
                  :no-definitions t)))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (clutch-test-sql--with-xref-buffer case
        (let ((id (or (plist-get case :find)
                      (xref-backend-identifier-at-point 'clutch))))
          (when-let* ((expected-id (plist-get case :id)))
            (should (equal id expected-id)))
          (if-let* ((error-pattern (plist-get case :error)))
              (let ((err (if (plist-get case :find)
                             (should-error (xref-find-definitions id)
                                           :type 'user-error)
                           (should-error
                            (xref-backend-definitions 'clutch id)
                            :type 'user-error))))
                (should (string-match-p error-pattern
                                        (error-message-string err))))
            (should-not (xref-backend-definitions 'clutch id))))))))

(provide 'clutch-test-sql)

;;; clutch-test-sql.el ends here
