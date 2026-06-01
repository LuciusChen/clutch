;;; clutch-test-sql.el --- SQL intelligence ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; SQL parsing, rewrite, completion, eldoc, and xref tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common))

;;;; SQL parsing — query classification

(ert-deftest clutch-test-schema-affecting-query-p ()
  "DDL-like statements should mark schema stale."
  (should (clutch-db-sql-schema-affecting-p "CREATE TABLE t (id INT)"))
  (should (clutch-db-sql-schema-affecting-p "alter table t add column x int"))
  (should (clutch-db-sql-schema-affecting-p "DROP VIEW v"))
  (should-not (clutch-db-sql-schema-affecting-p "DELETE FROM t"))
  (should-not (clutch-db-sql-schema-affecting-p "SELECT * FROM t")))

(ert-deftest clutch-test-destructive-query-p ()
  "Test destructive query detection."
  (should (clutch-db-sql-destructive-p "DROP TABLE users"))
  (should (clutch-db-sql-destructive-p "TRUNCATE users"))
  (should (clutch-db-sql-destructive-p "DELETE FROM users"))
  (should (clutch-db-sql-destructive-p "delete from users where id=1"))
  ;; With leading comment
  (should (clutch-db-sql-destructive-p "-- cleanup\nDROP TABLE users"))
  (should-not (clutch-db-sql-destructive-p "SELECT * FROM users"))
  (should-not (clutch-db-sql-destructive-p "UPDATE users SET name='x'")))

(ert-deftest clutch-test-risky-dml-p ()
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
    (should (clutch--risky-dml-p sql)))
  (dolist (sql '("UPDATE users SET name='x' WHERE id=1"
                 "DELETE FROM users WHERE id=1"
                 "WITH x AS (SELECT 1) UPDATE users SET name='x' WHERE id=1"
                 "UPDATE users SET name='x' WHERE 1=1 AND id=5"
                 "UPDATE users SET name='x' WHERE (id=5 OR 1=1) AND status='active'"
                 "UPDATE users SET name='x' WHERE note='1=1'"
                 "DELETE FROM users WHERE status='active'"
                 "WITH x AS (SELECT 1) SELECT * FROM x"
                 "SELECT * FROM users"))
    (should-not (clutch--risky-dml-p sql))))

(ert-deftest clutch-test-select-query-p ()
  "Test SELECT query detection."
  (should (clutch-db-sql-select-query-p "SELECT * FROM users"))
  (should (clutch-db-sql-select-query-p "select id from users"))
  (should (clutch-db-sql-select-query-p "  SELECT * FROM t"))
  (should (clutch-db-sql-select-query-p "WITH cte AS (SELECT 1) SELECT * FROM cte"))
  ;; With leading comments — previously broke SELECT detection
  (should (clutch-db-sql-select-query-p "-- get users\nSELECT * FROM users"))
  (should (clutch-db-sql-select-query-p "/* all */\nSELECT * FROM users"))
  (should (clutch-db-sql-select-query-p "-- a\n-- b\nSELECT 1"))
  ;; Result-set introspection commands also route through the SELECT/result path.
  (should (clutch-db-sql-select-query-p "SHOW TABLES"))
  (should (clutch-db-sql-select-query-p "DESCRIBE users"))
  (should (clutch-db-sql-select-query-p "EXPLAIN SELECT * FROM t"))
  (should-not (clutch-db-sql-select-query-p "INSERT INTO users VALUES (1)"))
  (should-not (clutch-db-sql-select-query-p "UPDATE users SET name='x'")))

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
                   "SELECT 1 -- foo;\nFROM t")))
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
  "The same alias in different UNION ALL branches should resolve branch-locally.
It should not always resolve to the first occurrence."
  ;; Cursor in the second branch: t. should complete with posts columns.
  (with-temp-buffer
    (insert "select t.id from users t\nunion all\nselect t.ti from posts t")
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
          (should-not (member "name" candidates)))))))

(ert-deftest clutch-test-union-all-alias-scoped-inside-subquery ()
  "UNION ALL inside a subquery should still scope aliases per branch."
  ;; Outer: select ... from (...) data_view
  ;; Inner: two branches with same alias t for different tables.
  (with-temp-buffer
    (insert (concat "select data_view.id from (\n"
                    "  select t.id from users t\n"
                    "  union all\n"
                    "  select t.ti from posts t\n"
                    ") data_view"))
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
          (should-not (member "name" candidates)))))))

(ert-deftest clutch-test-extract-tables-and-aliases ()
  "Clutch--extract-tables-and-aliases should preserve table and alias order.
It should retain duplicates for callers that deduplicate later."
  (let* ((sql "SELECT a.id FROM users a JOIN orders b ON a.id = b.uid JOIN users c")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql)))
         (tables (car result))
         (aliases (cdr result)))
    ;; Tables in order, including duplicate "users".
    (should (equal tables '("users" "orders" "users")))
    ;; Aliases in order.
    (should (equal aliases '(("a" . "users") ("b" . "orders") ("c" . "users"))))
    ;; delete-dups (as used by the cache) removes the second "users".
    (should (equal (delete-dups (copy-sequence tables)) '("users" "orders")))))

(ert-deftest clutch-test-extract-tables-and-aliases-handles-schema-qualified-table-without-alias ()
  "Schema-qualified tables without aliases should still parse cleanly."
  (let* ((sql "SELECT * FROM test.users")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql))))
    (should (equal (car result) '("users")))
    (should-not (cdr result))))

(ert-deftest clutch-test-extract-tables-and-aliases-handles-multiline-from-clause ()
  "Table extraction should survive multiline FROM clauses with indentation."
  (with-temp-buffer
    (clutch-mode)
    (let* ((sql "SELECT
    case_code
FROM
    section9_cases_wide
ORDER BY id")
           (result (clutch--extract-tables-and-aliases sql 0 (length sql))))
      (should (equal (car result) '("section9_cases_wide")))
      (should-not (cdr result)))))

(ert-deftest clutch-test-statement-table-identifiers-handle-multiline-from-clause ()
  "Statement table scanning should survive multiline FROM clauses."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT
    case_code
FROM
    section9_cases_wide
ORDER BY id")
    (should (equal (clutch--statement-table-identifiers)
                   '("section9_cases_wide")))))

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

(ert-deftest clutch-test-source-table-token-preserves-quoting ()
  "Source-table token detection should preserve identifier quoting."
  (should (equal (clutch-db-sql--source-table-token
                  "SELECT * FROM \"HR\".\"MixedCase\"")
                 "\"HR\".\"MixedCase\""))
  (should (equal (clutch-db-sql--source-table-token
                  "SELECT * FROM public.users u" t)
                 "public.users")))

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

(ert-deftest clutch-test-normalize-table-token-strips-quotes-from-schema-qualified ()
  "Quoted schema-qualified names like \"HR\".\"EMPLOYEES\" should normalize cleanly.
They should become a bare table name."
  (should (equal (clutch--normalize-statement-table-token "\"HR\".\"EMPLOYEES\"")
                 "EMPLOYEES"))
  (should (equal (clutch--normalize-statement-table-token "`db`.`table`")
                 "table"))
  (should (equal (clutch--normalize-statement-table-token "schema.table")
                 "table"))
  (should (equal (clutch--normalize-statement-table-token "\"EMPLOYEES\"")
                 "EMPLOYEES"))
  (should (equal (clutch--normalize-statement-table-token "users")
                 "users")))

;;;; SQL parsing — string and comment awareness

(ert-deftest clutch-test-strip-leading-comments ()
  "Test stripping leading SQL comments."
  (should (equal (clutch-db-sql-strip-leading-comments "SELECT 1") "SELECT 1"))
  (should (equal (clutch-db-sql-strip-leading-comments "  SELECT 1") "SELECT 1"))
  ;; Single-line comment
  (should (equal (clutch-db-sql-strip-leading-comments "-- hello\nSELECT 1")
                 "SELECT 1"))
  ;; Multiple single-line comments
  (should (equal (clutch-db-sql-strip-leading-comments "-- a\n-- b\nSELECT 1")
                 "SELECT 1"))
  ;; Multi-line comment
  (should (equal (clutch-db-sql-strip-leading-comments "/* foo */SELECT 1")
                 "SELECT 1"))
  ;; Mixed
  (should (equal (clutch-db-sql-strip-leading-comments "/* foo */\n-- bar\nSELECT 1")
                 "SELECT 1"))
  ;; Only comments
  (should (equal (clutch-db-sql-strip-leading-comments "-- nothing") "")))

(ert-deftest clutch-test-skip-literal-or-comment-single-quote ()
  "Skip a single-quoted string literal."
  (should (= (clutch-db-sql-skip-literal-or-comment "'hello'" 0) 7))
  ;; Escaped quote via ''
  (should (= (clutch-db-sql-skip-literal-or-comment "'it''s'" 0) 7))
  ;; Unterminated — return length.
  (should (= (clutch-db-sql-skip-literal-or-comment "'hello" 0) 6))
  ;; Not at a literal — return nil.
  (should-not (clutch-db-sql-skip-literal-or-comment "SELECT" 0)))

(ert-deftest clutch-test-skip-literal-or-comment-line-comment ()
  "Skip a -- line comment."
  (should (= (clutch-db-sql-skip-literal-or-comment "-- comment\ncode" 0) 11))
  ;; Comment at end of string (no newline).
  (should (= (clutch-db-sql-skip-literal-or-comment "-- comment" 0) 10)))

(ert-deftest clutch-test-skip-literal-or-comment-block-comment ()
  "Skip a /* block comment */."
  (should (= (clutch-db-sql-skip-literal-or-comment "/* block */" 0) 11))
  ;; Unterminated.
  (should (= (clutch-db-sql-skip-literal-or-comment "/* open" 0) 7)))

(ert-deftest clutch-test-skip-literal-or-comment-ignores-double-quote ()
  "Double-quoted identifiers are NOT literals — should return nil."
  (should-not (clutch-db-sql-skip-literal-or-comment "\"User Table\"" 0)))

(ert-deftest clutch-test-skip-literal-or-comment-ignores-backtick ()
  "Backtick identifiers are NOT literals — should return nil."
  (should-not (clutch-db-sql-skip-literal-or-comment "`user_table`" 0)))

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

(ert-deftest clutch-test-find-top-level-clause-skips-string ()
  "LIMIT inside a string literal should not be found."
  (should-not (clutch-db-sql-find-top-level-clause
               "select 'limit 10' from t" "LIMIT")))

(ert-deftest clutch-test-find-top-level-clause-skips-comment ()
  "LIMIT inside a comment should not be found, but a real one should."
  (should (clutch-db-sql-find-top-level-clause
           "select /* limit */ from t limit 5" "LIMIT"))
  (should (clutch-db-sql-find-top-level-clause
           "select -- limit\nfrom t limit 5" "LIMIT")))

(ert-deftest clutch-test-extract-tables-skips-string-literal ()
  "FROM inside a string literal should not produce a table."
  (let* ((sql "select 'from users' from orders o")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql))))
    (should (equal (car result) '("orders")))
    (should (equal (cdr result) '(("o" . "orders"))))))

(ert-deftest clutch-test-extract-tables-preserves-quoted-identifier ()
  "Backtick-quoted identifiers should still be extracted.
Double-quoted multi-word identifiers are a pre-existing regex limitation."
  (let* ((sql "select * from `order_items` oi join \"users\" u")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql)))
         (tables (car result))
         (aliases (cdr result)))
    (should (member "order_items" tables))
    (should (member "users" tables))
    (should (assoc "oi" aliases))
    (should (assoc "u" aliases))))

(ert-deftest clutch-test-union-branch-skips-string-union ()
  "UNION inside a string should not split branches."
  (let* ((sql "select 'union all' from a union all select * from b")
         ;; Point in the first branch (before the real UNION).
         (range (clutch--union-branch-range sql 5)))
    ;; The range should NOT start after the string — only split at real UNION.
    (should (= (car range) 0))
    (should (< (car range) 26))))

(ert-deftest clutch-test-innermost-paren-skips-string-parens ()
  "Parentheses inside string literals should not affect depth tracking."
  (let* ((sql "select (a, ')', b) from t")
         (range (clutch--innermost-paren-range sql 9)))
    ;; Point at 'a' inside the real parens — range should be (8 . 17).
    ;; ( is at 7, ) is at 17; result is (1+ open) . close-pos.
    (should (= (car range) 8))
    (should (= (cdr range) 17))))

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

(ert-deftest clutch-test-apply-where ()
  "Test WHERE filter application to SQL."
  ;; Simple case wraps query and applies outer WHERE
  (should (string-match-p
           "SELECT \\* FROM (SELECT \\* FROM t) AS _clutch_filter WHERE id = 1"
           (clutch-db-apply-where 'fake-conn "SELECT * FROM t" "id = 1")))
  ;; Query with existing WHERE is wrapped safely
  (let ((result (clutch-db-apply-where
                 'fake-conn "SELECT * FROM t WHERE x > 0" "id = 1")))
    (should (string-match-p "FROM (SELECT \\* FROM t WHERE x > 0)" result))
    (should (string-match-p "WHERE id = 1\\'" result))))

(ert-deftest clutch-test-apply-where-with-cte ()
  "Test WHERE filter wrapping for CTE SQL."
  (let* ((sql "WITH x AS (SELECT id FROM t) SELECT * FROM x")
         (result (clutch-db-apply-where 'fake-conn sql "id > 10")))
    (should (string-match-p "^SELECT \\* FROM (WITH x AS" result))
    (should (string-match-p "WHERE id > 10\\'" result))))

(ert-deftest clutch-test-apply-where-with-union ()
  "Test WHERE filter wrapping for UNION SQL."
  (let* ((sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b)")
         (result (clutch-db-apply-where 'fake-conn sql "id > 10")))
    (should (string-match-p "^SELECT \\* FROM (.*UNION ALL.*) AS _clutch_filter" result))
    (should (string-match-p "WHERE id > 10\\'" result))))

(ert-deftest clutch-test-apply-where-normalizes-comments-and-semicolon ()
  "WHERE rewrite should strip leading comments and trailing semicolons."
  (let* ((sql "-- head comment\n/* block */\nSELECT id FROM t;")
         (result (clutch-db-apply-where 'fake-conn sql "id > 10")))
    (should (string-prefix-p
             "SELECT * FROM (SELECT id FROM t) AS _clutch_filter WHERE id > 10"
             result))
    (should-not (string-match-p ";\\s-*) AS _clutch_filter" result))))

(ert-deftest clutch-test-apply-where-preserves-top-level-order-by ()
  "WHERE rewrite should preserve the user's visible result ordering."
  (let* ((sql "SELECT id, name FROM users ORDER BY created_at DESC")
         (result (clutch-db-apply-where 'fake-conn sql "id > 10")))
    (should (equal result
                   "SELECT * FROM (SELECT id, name FROM users ORDER BY created_at DESC) AS _clutch_filter WHERE id > 10"))))

(ert-deftest clutch-test-apply-where-preserves-limited-result-set ()
  "WHERE rewrite should preserve LIMIT/OFFSET result-set boundaries."
  (let* ((sql "SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20")
         (result (clutch-db-apply-where 'fake-conn sql "id > 10")))
    (should (equal result
                   "SELECT * FROM (SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20) AS _clutch_filter WHERE id > 10"))))

(ert-deftest clutch-test-build-count-sql-preserves-top-level-limit-offset ()
  "Count SQL should count the current limited result set."
  (let ((result (clutch-db-build-count-sql
                 'fake-conn
                 "SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20")))
    (should (string-match-p
             "^SELECT COUNT(\\*) FROM (SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20) AS _clutch_count\\'"
             result))))

(ert-deftest clutch-test-build-count-sql-with-cte ()
  "Count SQL should wrap CTE query safely."
  (let* ((sql "WITH x AS (SELECT id FROM t ORDER BY id) SELECT * FROM x ORDER BY id")
         (result (clutch-db-build-count-sql 'fake-conn sql)))
    (should (string-match-p "^SELECT COUNT(\\*) FROM (WITH x AS" result))
    (should (string-match-p ") AS _clutch_count\\'" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-with-distinct ()
  "Count SQL should preserve DISTINCT semantics via derived-table wrapping."
  (let* ((sql "SELECT DISTINCT user_id FROM visits ORDER BY user_id")
         (result (clutch-db-build-count-sql 'fake-conn sql)))
    (should (string-match-p
             "^SELECT COUNT(\\*) FROM (SELECT DISTINCT user_id FROM visits) AS _clutch_count\\'"
             result))))

(ert-deftest clutch-test-build-count-sql-keeps-inner-order-by ()
  "Count SQL should not remove ORDER BY inside nested subqueries."
  (let* ((sql "SELECT * FROM (SELECT id FROM t ORDER BY created_at DESC) s ORDER BY id")
         (result (clutch-db-build-count-sql 'fake-conn sql)))
    (should (string-match-p "SELECT id FROM t ORDER BY created_at DESC" result))
    (should (string-match-p "AS _clutch_count\\'" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-with-union-top-order ()
  "Count SQL should drop only top-level ORDER BY for UNION queries."
  (let* ((sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b) ORDER BY id")
         (result (clutch-db-build-count-sql 'fake-conn sql)))
    (should (string-match-p "UNION ALL" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-with-union-limit-offset ()
  "Count SQL should preserve top-level LIMIT/OFFSET on UNION queries."
  (let* ((sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b) LIMIT 50 OFFSET 100")
         (result (clutch-db-build-count-sql 'fake-conn sql)))
    (should (string-match-p "UNION ALL" result))
    (should (string-match-p "LIMIT 50\\s-+OFFSET 100\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-keeps-window-order-by ()
  "Count SQL should keep ORDER BY inside window OVER clauses."
  (let* ((sql "SELECT row_number() OVER (ORDER BY created_at DESC) AS rn FROM t ORDER BY rn LIMIT 5")
         (result (clutch-db-build-count-sql 'fake-conn sql)))
    (should (string-match-p "OVER (ORDER BY created_at DESC)" result))
    (should (string-match-p "ORDER BY rn\\s-+LIMIT 5\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-strips-trailing-semicolon ()
  "Count SQL should normalize trailing semicolons."
  (let ((result (clutch-db-build-count-sql 'fake-conn "SELECT * FROM users;")))
    (should-not (string-match-p ";\\s-*) AS _clutch_count\\'" result))
    (should (string-match-p "SELECT \\* FROM users" result))))

(ert-deftest clutch-test-build-count-sql-strips-leading-comments ()
  "Count SQL should ignore leading SQL comments."
  (let* ((sql "-- comment\n/* block */\nSELECT id FROM t ORDER BY id")
         (result (clutch-db-build-count-sql 'fake-conn sql)))
    (should (string-prefix-p "SELECT COUNT(*) FROM (SELECT id FROM t)" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-omits-as-for-oracle-derived-table ()
  "Oracle count rewrite should not use AS before a derived-table alias."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver oracle))))
    (should (equal (clutch-db-build-count-sql
                    clutch-connection
                    "SELECT id FROM t FETCH FIRST 50 ROWS ONLY")
                   "SELECT COUNT(*) FROM (SELECT id FROM t FETCH FIRST 50 ROWS ONLY) clutch_count"))))

(ert-deftest clutch-test-apply-where-omits-as-for-oracle-derived-table ()
  "Oracle WHERE rewrite should not use AS before a derived-table alias."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver oracle))))
    (should (equal (clutch-db-apply-where clutch-connection "SELECT * FROM t" "id = 1")
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

(ert-deftest clutch-test-sql-keyword-completion-case-insensitive ()
  "Test case-insensitive matching (input \"sel\" matches \"SELECT\")."
  (with-temp-buffer
    (insert "sel")
    (let* ((result (clutch-sql-keyword-completion-at-point))
           (candidates (clutch-test--completion-candidates result)))
      (should result)
      (should (member "SELECT" candidates)))))

(ert-deftest clutch-test-sql-keyword-completion-includes-zero-arg-functions ()
  "Installed keyword CAPF should include zero-argument SQL functions."
  (with-temp-buffer
    (insert "cur")
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
        (should (member "CURRENT_DATABASE()" captured))))))

(ert-deftest clutch-test-sql-keyword-completion-chain-case-insensitive ()
  "The installed CAPF chain should complete lowercase keyword prefixes."
  (with-temp-buffer
    (insert "sele")
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
        (should (member "SELECT" captured))))))

(ert-deftest clutch-test-sql-keyword-completion-no-prefix ()
  "Test that keyword capf returns nil with no word at point."
  (with-temp-buffer
    (insert " ")
    (let ((result (clutch-sql-keyword-completion-at-point)))
      (should-not result))))

(ert-deftest clutch-test-sql-keyword-completion-honors-lowercase-style ()
  "Keyword completion should honor `clutch-sql-completion-case-style'."
  (let ((clutch-sql-completion-case-style 'lower))
    (with-temp-buffer
      (insert "sel")
      (let* ((result (clutch-sql-keyword-completion-at-point))
             (candidates (clutch-test--completion-candidates result)))
        (should result)
        (should (member "select" candidates))
        (should-not (member "SELECT" candidates))))
    (with-temp-buffer
      (insert "gro")
      (let* ((result (clutch-sql-keyword-completion-at-point))
             (candidates (clutch-test--completion-candidates result)))
        (should result)
        (should (member "group by" candidates))
        (should-not (member "GROUP BY" candidates))))))

(ert-deftest clutch-test-sql-keyword-completion-uses-complete-clause-phrases ()
  "Keyword completion should offer complete multi-word clause heads."
  (dolist (case '(("gro" "GROUP BY" "GROUP")
                  ("ord" "ORDER BY" "ORDER")
                  ("par" "PARTITION BY" "PARTITION")
                  ("pri" "PRIMARY KEY" "PRIMARY")
                  ("for" "FOREIGN KEY" "FOREIGN")))
    (pcase-let ((`(,prefix ,phrase ,first-token) case))
      (with-temp-buffer
        (insert prefix)
        (let* ((result (clutch-sql-keyword-completion-at-point))
               (candidates (clutch-test--completion-candidates result)))
          (should (member phrase candidates))
          (should-not (member first-token candidates)))))))

(ert-deftest clutch-test-sql-keyword-completion-keeps-additive-phrase-bases ()
  "Additive keyword phrases should not hide valid first-token completions."
  (dolist (case '(("inn" "INNER" "INNER JOIN")
                  ("lef" "LEFT" "LEFT JOIN")
                  ("rig" "RIGHT" "RIGHT JOIN")
                  ("cro" "CROSS" "CROSS JOIN")
                  ("uni" "UNION" "UNION ALL")
                  ("is" "IS" "IS NULL" "IS NOT NULL")
                  ("not" "NOT" "NOT NULL")))
    (pcase-let ((`(,prefix . ,expected) case))
      (with-temp-buffer
        (insert prefix)
        (let* ((result (clutch-sql-keyword-completion-at-point))
               (candidates (clutch-test--completion-candidates result)))
          (dolist (candidate expected)
            (should (member candidate candidates))))))))

(ert-deftest clutch-test-keyword-capf-exit-function-inserts-space-on-exact ()
  "Keyword CAPF exit-function should insert a trailing space for status `exact'."
  (with-temp-buffer
    (insert "FROM")
    (let* ((capf (clutch-sql-keyword-completion-at-point))
           (exit-fn (plist-get (cdddr capf) :exit-function)))
      (funcall exit-fn "FROM" 'exact)
      (should (equal (buffer-string) "FROM ")))))

(ert-deftest clutch-test-keyword-capf-exit-function-skips-space-after-call ()
  "Keyword CAPF exit-function should not add a space after function calls."
  (with-temp-buffer
    (insert "cur")
    (let* ((capf (clutch-sql-keyword-completion-at-point))
           (exit-fn (plist-get (cdddr capf) :exit-function)))
      (funcall exit-fn "CURRENT_DATABASE()" 'exact)
      (should (equal (buffer-string) "cur")))))

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
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "is_ship_blocked" "owner")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "is_ship_blocked" :type-category numeric)
                                                     (:name "owner" :type-category text))))
          (with-temp-buffer
            (insert "severity: \nis_ship_blocked: \nowner: alice\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity" :type "enum('low','medium','high','critical')")
                               (list :name "is_ship_blocked" :type "tinyint(1)")
                               (list :name "owner" :type "varchar(255)")))))
              (goto-char (point-min))
              (goto-char (clutch-result-insert--current-field-value-position))
              (pcase-let ((`(,beg ,end ,candidates . ,_)
                           (clutch-result-insert-completion-at-point)))
                (should (= beg end))
                (should (equal candidates '("low" "medium" "high" "critical"))))
              (forward-line 1)
              (goto-char (clutch-result-insert--current-field-value-position))
              (pcase-let ((`(,_beg ,_end ,candidates . ,_)
                           (clutch-result-insert-completion-at-point)))
                (should (equal candidates '("0" "1"))))
              (forward-line 1)
              (goto-char (clutch-result-insert--current-field-value-position))
              (should-not (clutch-result-insert-completion-at-point)))))
      (kill-buffer result-buf))))

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
          (with-current-buffer insert-buf
            (erase-buffer)
            (insert "severity: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'completion-at-point) (lambda () nil))
                      ((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity"
                                     :type "enum('low','medium','high','critical')"))))
                      ((symbol-function 'completing-read)
                       (lambda (&rest _args)
                         (setq completion-called t)
                         "critical")))
              (goto-char (point-min))
              (goto-char (clutch-result-insert--current-field-value-start))
              (clutch-result-insert-complete-field)
              (should completion-called)
              (should (equal (buffer-string) "severity: critical\n")))))
      (kill-buffer result-buf)
      (kill-buffer insert-buf))))

(ert-deftest clutch-test-edit-complete-field-propagates-capf-errors ()
  "Edit completion should not swallow `completion-at-point' failures."
  (with-temp-buffer
    (setq-local clutch-result-edit--column-name "severity")
    (clutch--result-edit-mode 1)
    (cl-letf (((symbol-function 'completion-at-point)
               (lambda ()
                 (error "CAPF boom"))))
      (should-error (clutch-result-edit-complete-field) :type 'error))))

(ert-deftest clutch-test-insert-complete-field-propagates-capf-errors ()
  "Insert completion should not swallow `completion-at-point' failures."
  (with-temp-buffer
    (insert "severity: \n")
    (clutch-result-insert-mode 1)
    (cl-letf (((symbol-function 'completion-at-point)
               (lambda ()
                 (error "CAPF boom"))))
      (goto-char (point-min))
      (goto-char (clutch-result-insert--current-field-value-start))
      (should-error (clutch-result-insert-complete-field) :type 'error))))

(ert-deftest clutch-test-completion-at-point-keeps-short-prefix-table-only ()
  "Non-keyword short prefixes should not load columns eagerly."
  (with-temp-buffer
    (insert "x")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          called)
      (puthash "xaccounts" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (setq called t)
                   '("id"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should (member "xaccounts" candidates))
          (should-not called))))))

(ert-deftest clutch-test-completion-at-point-uses-cached-columns-for-small-statement-scope ()
  "Identifier completion should reuse cached columns and sync-load cache misses."
  (with-temp-buffer
    (insert "us")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          loaded)
      (puthash "users" '("id" "name") schema)
      (puthash "orders" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users" "orders")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq loaded table)
                   '("order_id" "user_id")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "sync completion should not queue async column loads"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf "")))
          (should capf)
          (should (equal loaded "orders"))
          (should (member "id" candidates))
          (should (member "order_id" candidates))
          (should (member "users" candidates)))))))

(ert-deftest clutch-test-completion-at-point-keeps-statement-scope-candidates-tight ()
  "Statement-scoped column completion should not leak unrelated schema tables."
  (with-temp-buffer
    (insert "select us from users join posts on users.id = posts.user_id")
    (goto-char (point-min))
    (search-forward "us")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "users" nil schema)
      (puthash "posts" nil schema)
      (puthash "logs" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users" "posts")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "should not queue async loads when statement columns are cached"))))
        (puthash "users" '("user_id" "user_name") schema)
        (puthash "posts" '("post_id" "user_id") schema)
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf "")))
          (should capf)
          (should (member "users" candidates))
          (should (member "posts" candidates))
          (should (member "user_id" candidates))
          (should-not (member "logs" candidates)))))))

(ert-deftest clutch-test-completion-at-point-sync-loads-columns-for-small-statement-scope ()
  "Native statement-scoped completion should sync-load columns on cache miss."
  (with-temp-buffer
    (insert "us")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          loaded)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq loaded table)
                   '("id" "name")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "sync completion should not queue async column loads"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf "")))
          (should capf)
          (should (equal loaded "users"))
          (should (member "id" candidates))
          (should (member "users" candidates)))))))

(ert-deftest clutch-test-completion-at-point-uses-qualified-table-for-cached-column-loading ()
  "Qualified completion should only use cached columns for the referenced table."
  (dolist (case '(("select * from users u join posts p on u." "u.")
                  ("select users. from users" "users.")))
    (pcase-let ((`(,sql ,marker) case))
      (with-temp-buffer
        (clutch-mode)
        (insert sql)
        (goto-char (point-min))
        (search-forward marker)
        (let ((schema (make-hash-table :test 'equal))
              (clutch-connection 'fake)
              captured)
          (puthash "users" '("id" "name") schema)
          (puthash "posts" '("title" "body") schema)
          (push (lambda ()
                  (list (point) (point) '("generic") :exclusive 'no))
                completion-at-point-functions)
          (run-hooks 'corfu-mode-hook)
          (cl-letf (((symbol-function 'clutch--schema-for-connection)
                     (lambda () schema))
                    ((symbol-function 'clutch-db-busy-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'clutch-db-completion-sync-columns-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch--ensure-columns-async)
                     (lambda (&rest _args)
                       (ert-fail "should not queue async loads when target is cached")))
                    (completion-in-region-function
                     (lambda (start end collection &optional predicate)
                       (setq captured
                             (all-completions
                              (buffer-substring-no-properties start end)
                              collection predicate))
                       t)))
            (let ((command (local-key-binding (kbd "TAB"))))
              (let ((this-command command))
                (call-interactively command)))
            (should (equal captured '("id" "name")))
            (should-not (member "generic" captured))
            (should-not (member "title" captured))
            (should (eq (plist-get (nthcdr 3 (clutch-completion-at-point))
                                   :company-prefix-length)
                        t))))))))

(ert-deftest clutch-test-completion-at-point-uses-direct-table-candidates-without-schema-cache ()
  "Direct backend table completion should work without a schema cache."
  (with-temp-buffer
    (insert "select * from zj_")
    (goto-char (point-max))
    (let ((clutch-connection 'fake))
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () nil))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-complete-tables)
                 (lambda (_conn prefix)
                   (should (equal prefix "zj_"))
                   '("ZJ_NCBUSINESSDATA" "ZJ_SYS_PARA"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should (member "ZJ_NCBUSINESSDATA" candidates))
          (should (member "ZJ_SYS_PARA" candidates)))))))

(ert-deftest clutch-test-completion-at-point-uses-ready-schema-table-candidates-before-direct-rpc ()
  "Table completion should use ready schema matches before backend completion."
  (with-temp-buffer
    (insert "select * from us")
    (goto-char (point-max))
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "USERS" nil schema)
      (puthash "ORDERS" nil schema)
      (puthash "USER_ROLES" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch--schema-status-entry)
                 (lambda (_conn) '(:state ready)))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-complete-tables)
                 (lambda (&rest _)
                   (ert-fail "ready schema matches should avoid direct RPC"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should (equal (sort (copy-sequence candidates) #'string<)
                         '("USERS" "USER_ROLES"))))))))

(ert-deftest clutch-test-completion-at-point-falls-back-to-direct-rpc-when-ready-schema-has-no-match ()
  "Ready schema cache misses should fall back to backend table completion."
  (with-temp-buffer
    (insert "select * from or")
    (goto-char (point-max))
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          rpc-called)
      (puthash "AUDIT_LOG" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch--schema-status-entry)
                 (lambda (_conn) '(:state ready)))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-complete-tables)
                 (lambda (_conn prefix)
                   (setq rpc-called t)
                   (should (equal prefix "or"))
                   '("ORDERS"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should rpc-called)
          (should (equal candidates '("ORDERS"))))))))

(ert-deftest clutch-test-completion-at-point-falls-back-to-direct-rpc-when-schema-is-stale ()
  "Stale schema cache should not block backend table completion."
  (with-temp-buffer
    (insert "select * from us")
    (goto-char (point-max))
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          rpc-called)
      (puthash "USERS_OLD" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch--schema-status-entry)
                 (lambda (_conn) '(:state stale)))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-complete-tables)
                 (lambda (_conn prefix)
                   (setq rpc-called t)
                   (should (equal prefix "us"))
                   '("USERS_NEW"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should rpc-called)
          (should (member "USERS_NEW" candidates)))))))

(ert-deftest clutch-test-completion-at-point-prefers-table-candidates-in-from-context ()
  "FROM/JOIN table completion should not be shadowed by SQL keyword completion."
  (with-temp-buffer
    (insert "select * from or")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          captured)
      (puthash "ORDERS" nil schema)
      (setq-local completion-at-point-functions nil)
      (clutch--install-completion-capfs)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch--schema-status-entry)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                (completion-in-region-function
                 (lambda (start end collection &optional predicate)
                   (setq captured
                         (list :input (buffer-substring-no-properties start end)
                               :candidates (all-completions
                                            (buffer-substring-no-properties start end)
                                            collection predicate)))
                   t)))
        (should (completion-at-point))
        (should (equal (plist-get captured :input) "or"))
        (should (equal (plist-get captured :candidates) '("ORDERS")))))))

(ert-deftest clutch-test-completion-at-point-defers-to-keywords-for-keyword-prefixes ()
  "Statement-start keyword prefixes should not be shadowed by schema tables."
  (with-temp-buffer
    (insert "sele")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "SELECT_LOG" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil)))
        (should-not (clutch-completion-at-point))
        (let* ((capf (clutch-sql-keyword-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should (member "SELECT" candidates))
          (should-not (member "SELECT_LOG" candidates)))))))

(ert-deftest clutch-test-completion-at-point-chain-keeps-keywords-with-statement-context ()
  "The installed CAPF chain should still offer keywords with statement tables."
  (with-temp-buffer
    (insert "sele from users")
    (goto-char (point-min))
    (search-forward "sele")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          captured)
      (puthash "users" '("id" "name") schema)
      (setq-local completion-at-point-functions nil)
      (clutch--install-completion-capfs)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                (completion-in-region-function
                 (lambda (start end collection &optional predicate)
                   (setq captured
                         (all-completions
                          (buffer-substring-no-properties start end)
                          collection predicate))
                   t)))
        (should (completion-at-point))
        (should (member "SELECT" captured))))))

(ert-deftest clutch-test-completion-at-point-uses-direct-column-candidates-when-sync-loads-disabled ()
  "Direct backend column completion should avoid synchronous ensure-columns."
  (with-temp-buffer
    (insert "select pa from ZJ_SYS_PARA")
    (goto-char (point-min))
    (search-forward "pa")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "ZJ_SYS_PARA" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (error "Should not synchronously load columns")))
                ((symbol-function 'clutch-db-complete-columns)
                 (lambda (_conn table prefix)
                   (should (equal table "ZJ_SYS_PARA"))
                   (should (equal prefix "pa"))
                   '("PARA_ID" "PARA_NAME"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should (member "PARA_ID" candidates))
          (should (member "PARA_NAME" candidates)))))))

(ert-deftest clutch-test-completion-at-point-uses-alias-qualified-table-for-direct-column-loading ()
  "Alias-qualified completion should only query direct columns for the referenced table."
  (with-temp-buffer
    (insert "select u.pa from ZJ_SYS_PARA u join ZJ_LOG p on u.id = p.para_id")
    (goto-char (point-min))
    (search-forward "pa")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          seen)
      (puthash "ZJ_SYS_PARA" nil schema)
      (puthash "ZJ_LOG" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (error "Should not synchronously load columns")))
                ((symbol-function 'clutch-db-complete-columns)
                 (lambda (_conn table prefix)
                   (push table seen)
                   (should (equal prefix "pa"))
                   (pcase table
                     ("ZJ_SYS_PARA" '("PARA_ID" "PARA_NAME"))
                     ("ZJ_LOG" '("PAYLOAD"))
                     (_ nil)))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (clutch-test--completion-candidates capf)))
          (should capf)
          (should (equal seen '("ZJ_SYS_PARA")))
          (should (member "PARA_ID" candidates))
          (should (member "PARA_NAME" candidates))
          (should-not (member "PAYLOAD" candidates))
          (should-not (member "ZJ_SYS_PARA" candidates))
          (should-not (member "ZJ_LOG" candidates)))))))

(ert-deftest clutch-test-completion-at-point-lowercases-identifiers-when-configured ()
  "Identifier completion should honor lowercase case style."
  (let ((clutch-sql-completion-case-style 'lower))
    (with-temp-buffer
      (insert "select pa from ZJ_SYS_PARA")
      (goto-char (point-min))
      (search-forward "pa")
      (let ((schema (make-hash-table :test 'equal))
            (clutch-connection 'fake))
        (puthash "ZJ_SYS_PARA" nil schema)
        (cl-letf (((symbol-function 'clutch--schema-for-connection)
                   (lambda () schema))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch--tables-in-current-statement)
                   (lambda (_schema) '("ZJ_SYS_PARA")))
                  ((symbol-function 'clutch-db-completion-sync-columns-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-complete-columns)
                   (lambda (_conn table prefix)
                     (should (equal table "ZJ_SYS_PARA"))
                     (should (equal prefix "pa"))
                     '("PARA_ID" "PARA_NAME"))))
          (let* ((capf (clutch-completion-at-point))
                 (candidates (clutch-test--completion-candidates capf "")))
            (should capf)
            (should (member "zj_sys_para" candidates))
            (should (member "para_id" candidates))
            (should-not (member "ZJ_SYS_PARA" candidates))))))))

(ert-deftest clutch-test-completion-at-point-swallows-oracle-i18n-completion-errors ()
  "Oracle completion should fail soft when orai18n.jar is missing."
  (let ((clutch--oracle-i18n-warning-shown nil)
        warned)
    (with-temp-buffer
      (insert "select * from zj_")
      (goto-char (point-max))
      (let ((clutch-connection 'fake))
        (cl-letf (((symbol-function 'clutch--schema-for-connection)
                   (lambda () nil))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-complete-tables)
                   (lambda (_conn _prefix)
                     (signal 'clutch-db-error
                             '("Non supported character set (add orai18n.jar in your classpath): ZHS16GBK"))))
                  ((symbol-function 'message)
                   (lambda (fmt &rest args)
                     (setq warned (apply #'format fmt args)))))
          (should-not (clutch-completion-at-point))
          (should (string-match-p "orai18n.jar" warned))
          (setq warned nil)
          (should-not (clutch-completion-at-point))
          (should-not warned))))))

;;;; Eldoc — schema and column info

(ert-deftest clutch-test-eldoc-schema-string-skips-large-statement-scope ()
  "Eldoc should not synchronously load columns across too many tables."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal))
          called)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("a" "b" "c" "d")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (setq called t)
                   nil)))
        (should-not (clutch--eldoc-schema-string 'fake schema "id"))
        (should-not called)))))

(ert-deftest clutch-test-eldoc-schema-string-uses-current-statement-columns ()
  "Eldoc should resolve column info from cached current-statement tables."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal)))
      (puthash "users" '("id" "name") schema)
      (cl-letf (((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-schema-string 'fake schema "id")
                       "users.id bigint"))))))

(ert-deftest clutch-test-eldoc-schema-string-sync-loads-current-statement-columns ()
  "Native backends should sync-load current statement columns for eldoc."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal))
          called)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq called table)
                   '("id" "name")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "eldoc should not fall back to async-only column loading here")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-schema-string 'fake schema "id")
                       "users.id bigint"))
        (should (equal called "users"))))))

(ert-deftest clutch-test-eldoc-schema-string-skips-sync-load-when-connection-busy ()
  "Eldoc should not synchronously load columns while CONN is busy.
Instead it should queue async warmup and return nil until metadata is ready."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal))
          async-called)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (signal 'clutch-db-error
                           '("sync column load should not run while busy"))))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (_conn _schema table)
                   (setq async-called table)
                   t)))
        (should-not (clutch--eldoc-schema-string 'fake schema "id"))
        (should (equal async-called "users"))))))

(ert-deftest clutch-test-eldoc-schema-string-uses-cached-columns-when-sync-loads-disabled ()
  "Eldoc should not synchronously load columns when backend disables it."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "ZJ_SYS_PARA" '("PARA_ID" "PARA_NAME") schema)
    (cl-letf (((symbol-function 'clutch-db-completion-sync-columns-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--cached-table-comment)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--table-comment-cached-p)
               (lambda (&rest _args) t))
              ((symbol-function 'clutch--ensure-table-comment-async)
               #'ignore)
              ((symbol-function 'clutch-db-database)
               (lambda (_conn) "ORCL")))
      (should (string-match-p "ZJ_SYS_PARA"
                              (clutch--eldoc-schema-string 'fake schema "ZJ_SYS_PARA")))
      (should (string-match-p "2 cols"
                              (clutch--eldoc-schema-string 'fake schema "ZJ_SYS_PARA"))))))

(ert-deftest clutch-test-eldoc-on-schema-qualifier-resolves-table-name ()
  "Eldoc on the schema part of schema.table should resolve to the table."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT * FROM test.users;")
    (goto-char (point-min))
    (search-forward "test.users")
    (goto-char (match-beginning 0))
    (let ((schema (make-hash-table :test 'equal)))
      (puthash "users" '("id") schema)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--cached-columns)
                 (lambda (_schema _table) '("id")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (let ((eldoc (clutch--eldoc-function)))
          (should (stringp eldoc))
          (should (string-match-p "users" eldoc))
          (should (string-match-p "1 col" eldoc)))))))

(ert-deftest clutch-test-eldoc-on-uppercase-column-uses-cached-lowercase-metadata ()
  "Uppercase SQL identifiers should still match lowercase cached columns."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT ID FROM users;")
    (goto-char (point-min))
    (search-forward "ID")
    (goto-char (match-beginning 0))
    (let ((schema (make-hash-table :test 'equal)))
      (puthash "users" '("id") schema)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-function)
                       "users.id bigint"))))))

(ert-deftest clutch-test-eldoc-qualified-column-bypasses-statement-table-limit ()
  "Alias-qualified field eldoc should not be blocked by large join scopes."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT d.id
FROM accounts a
JOIN customers c ON c.account_id = a.id
JOIN invoices i ON i.customer_id = c.id
JOIN orders_large d ON d.invoice_id = i.id;")
    (goto-char (point-min))
    (search-forward "d.id")
    (goto-char (+ (match-beginning 0) 2))
    (let ((schema (make-hash-table :test 'equal))
          called)
      (dolist (table '("accounts" "customers" "invoices" "orders_large"))
        (puthash table nil schema))
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema)
                   '("accounts" "customers" "invoices" "orders_large")))
                ((symbol-function 'clutch--table-aliases-in-current-statement)
                 (lambda (_schema)
                   '(("a" . "accounts")
                     ("c" . "customers")
                     ("i" . "invoices")
                     ("d" . "orders_large"))))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq called table)
                   (should (equal table "orders_large"))
                   '("id" "invoice_id")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-function)
                       "orders_large.id bigint"))
        (should (equal called "orders_large"))))))

(ert-deftest clutch-test-eldoc-field-resolves-multiline-from-table ()
  "Field eldoc should work when FROM and the table name are on separate lines."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT
    case_code, operative_name
FROM
    section9_cases_wide
ORDER BY id")
    (goto-char (point-min))
    (search-forward "case_code")
    (goto-char (match-beginning 0))
    (let ((schema (make-hash-table :test 'equal))
          called)
      (puthash "section9_cases_wide" nil schema)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq called table)
                   '("case_code" "operative_name")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s text" table col-name))))
        (should (equal (clutch--eldoc-function)
                       "section9_cases_wide.case_code text"))
        (should (equal called "section9_cases_wide"))))))

(ert-deftest clutch-test-eldoc-schema-string-queues-background-metadata-on-cache-miss ()
  "Eldoc should queue background metadata when a table cache entry is empty."
  (let ((schema (make-hash-table :test 'equal))
        queued-columns queued-comments)
    (puthash "users" nil schema)
    (cl-letf (((symbol-function 'clutch--cached-table-comment)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--table-comment-cached-p)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--ensure-columns-async)
               (lambda (_conn _schema table)
                 (push table queued-columns)
                 t))
              ((symbol-function 'clutch--ensure-table-comment-async)
               (lambda (_conn table)
                 (push table queued-comments)
                 t))
              ((symbol-function 'clutch-db-database)
               (lambda (_conn) "appdb")))
      (let ((doc (clutch--eldoc-schema-string 'fake schema "users")))
        (should (stringp doc))
        (should (equal queued-columns '("users")))
        (should (equal queued-comments '("users")))))))

;;;; Xref — alias jump

(ert-deftest clutch-test-xref-alias-jump-basic ()
  "Alias `u' in a WHERE clause should jump to its FROM-clause definition."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT u.* FROM users u WHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users"))
                       :statement-tables '("users")))))
      ;; Move to the `u' in `u.id'
      (goto-char (point-min))
      (search-forward "u.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          ;; Verify the character at pos is `u' after `users '
          (should (eq (char-after pos) ?u))
          (goto-char pos)
          (should (looking-back "users " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-jump-qualified ()
  "Qualified reference `u.name' should resolve qualifier `u' as the alias."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT u.name FROM users u")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users"))
                       :statement-tables '("users")))))
      ;; Move to `name' in `u.name'
      (goto-char (point-min))
      (search-forward "u.name")
      (goto-char (+ (match-beginning 0) 2)) ;; on `name'
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?u))
          ;; `u' after `users ' in FROM
          (goto-char pos)
          (should (looking-back "users " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-join ()
  "Alias `o' should jump to the JOIN definition, not the FROM alias."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT o.total FROM users u JOIN orders o ON o.uid = u.id")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users") ("o" . "orders"))
                       :statement-tables '("users" "orders")))))
      ;; Move to `o' in `o.total'
      (goto-char (point-min))
      (search-forward "o.total")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "o"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?o))
          (goto-char pos)
          (should (looking-back "orders " (- pos 10))))))))

(ert-deftest clutch-test-xref-non-alias-returns-no-definitions ()
  "A non-alias symbol should return the symbol but no definitions."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT name FROM users u")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users"))
                       :statement-tables '("users")))))
      ;; Move to `name' (not an alias, not qualified)
      (goto-char (point-min))
      (search-forward "name")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "name"))
        (should-not (xref-backend-definitions 'clutch id))))))

(ert-deftest clutch-test-xref-alias-union-scoped ()
  "In a UNION query, alias jump should target the current branch's definition."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT a.id FROM users a\nUNION ALL\nSELECT a.id FROM orders a")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("a" . "users") ("a" . "orders"))
                       :statement-tables '("users" "orders")))))
      ;; Move to `a' in second branch (after UNION ALL)
      (goto-char (point-min))
      (search-forward "UNION ALL\n")
      (search-forward "a.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "a"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?a))
          ;; Should be in the second branch (after "orders ")
          (goto-char pos)
          (should (looking-back "orders " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-inside-parens ()
  "An alias inside SUM(...) or CASE should still find the FROM definition."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT SUM(fopd.goods_num) FROM\n    ffp_order_plan_detail fopd WHERE fopd.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      ;; cursor on fopd inside SUM(...)
      (goto-char (point-min))
      (search-forward "SUM(fopd")
      (goto-char (- (point) 4)) ;; on `fopd' inside parens
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "fopd"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (equal (buffer-substring-no-properties pos (+ pos 4)) "fopd"))
          (goto-char pos)
          (should (looking-back "ffp_order_plan_detail " (- pos 30))))))))

(ert-deftest clutch-test-xref-alias-multiline-from ()
  "Alias lookup should work when FROM and table name are on separate lines."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT\n    fopd.goods_num\nFROM\n    ffp_order_plan_detail fopd\n    LEFT JOIN ffp_order_plan fop ON fop.plan_id = fopd.plan_id\nWHERE\n    fopd.plan_id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      ;; Move to `fopd' in WHERE clause
      (goto-char (point-min))
      (search-forward "fopd.plan_id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "fopd"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?f))
          (should (equal (buffer-substring-no-properties pos (+ pos 4)) "fopd"))
          (goto-char pos)
          (should (looking-back "ffp_order_plan_detail " (- pos 30))))))))

(ert-deftest clutch-test-xref-alias-no-schema-cache ()
  "Alias lookup should work even when the schema cache is not warmed."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT u.name FROM users u WHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) nil)))
      (goto-char (point-min))
      (search-forward "u.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?u))
          (goto-char pos)
          (should (looking-back "users " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-no-schema-union-scoped ()
  "Without schema cache, alias resolution should still scope to UNION branch."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT a.id FROM users a\nUNION ALL\nSELECT a.id FROM orders a")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      ;; cursor in second branch
      (goto-char (point-min))
      (search-forward "UNION ALL\n")
      (search-forward "a.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "a"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?a))
          (goto-char pos)
          (should (looking-back "orders " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-quoted ()
  "Alias lookup should handle quoted alias identifiers like `\"u\"'."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT \"u\".name FROM users \"u\" WHERE \"u\".id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward "\"u\".id")
      (goto-char (+ (match-beginning 0) 1)) ;; on `u' inside quotes
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          ;; Should land on the `"' of `"u"' in FROM clause
          (should (eq (char-after pos) ?\")))))))

(ert-deftest clutch-test-xref-ignores-alias-in-comment ()
  "Alias lookup should not jump on alias-like text inside a comment."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT * FROM users u -- use u.id here\nWHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward "u.id here")
      (goto-char (match-beginning 0))
      (should-not
       (xref-backend-definitions
        'clutch
        (xref-backend-identifier-at-point 'clutch))))))

(ert-deftest clutch-test-xref-ignores-alias-in-string ()
  "Alias lookup should not jump on alias-like text inside a string."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT 'u.id' AS note FROM users u WHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward "u.id'")
      (goto-char (match-beginning 0))
      (should-not
       (xref-backend-definitions
        'clutch
        (xref-backend-identifier-at-point 'clutch))))))

(ert-deftest clutch-test-xref-alias-quoted-multiword ()
  "Alias lookup should handle double-quoted multi-word identifiers."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT \"User Name\".id FROM users \"User Name\" WHERE \"User Name\".id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward ".id")
      (backward-char 2)
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "User Name"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?\")))))))

(provide 'clutch-test-sql)

;;; clutch-test-sql.el ends here
