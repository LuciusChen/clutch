;;; clutch-test-query.el --- Query execution ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Query execution, cancellation, reconnects, batches and error handling
;; tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; Execute — query execution and error handling

(ert-deftest clutch-test-execute-only-paginates-select-statements ()
  "Result-set commands must not inherit SELECT pagination rewrites."
  (dolist (case
           '(("SELECT * FROM users" t)
             ("-- rows\nWITH active AS (SELECT * FROM users) SELECT * FROM active" t)
             ("SHOW INDEX FROM users WHERE Key_name = 'users_name_key'" nil)
             ("DESCRIBE users" nil)
             ("DESC users" nil)
             ("EXPLAIN SELECT * FROM users" nil)
             ("PRAGMA table_info(users)" nil)
             ("VALUES (1)" nil)
             ("INSERT INTO users (name) VALUES ('Ada') RETURNING id" nil)
             ("WITH i AS (INSERT INTO users (name) VALUES ('Ada') RETURNING id) SELECT * FROM i" nil)
             ("SELECT * INTO users_copy FROM users" nil)
             ("SELECT * FROM FINAL TABLE (INSERT INTO users (name) VALUES ('Ada'))" nil)
             ("CALL list_users()" nil)))
    (pcase-let ((`(,sql ,pageable) case))
      (ert-info ((format "sql: %s" sql))
        (let (executed-sql identity-prepared)
          (cl-letf (((symbol-function 'clutch--prepare-row-identity-query)
                     (lambda (_connection statement)
                       (setq identity-prepared t)
                       (list :sql statement)))
                    ((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_connection statement _page-num _page-size
                              &optional _order-by _page-offset)
                       (concat statement " /* paged */")))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_connection statement)
                       (setq executed-sql statement)
                       (make-clutch-db-result
                        :columns '((:name "value"))
                        :rows '((1))))))
            (let ((outcome
                   (clutch-test--await-outcome
                    (lambda (k)
                      (clutch--execute-statement-attempt
                       sql 'fake-conn t nil nil k)))))
              (should (plist-get outcome :result-query-p))
              (should (eq (plist-get outcome :server-pageable) pageable))
              (should (eq (and identity-prepared t) pageable))
              (should (equal executed-sql
                             (if pageable
                                 (concat sql " /* paged */")
                               sql))))))))))

(ert-deftest clutch-test-result-filter-page-count-export-real-sqlite-workflow ()
  "Public result commands preserve one filtered SQLite workflow end to end."
  (let ((clutch-result-max-rows 2) (kill-ring nil) (kill-ring-yank-pointer nil))
    (clutch-test--with-sqlite-result (conn result)
        (list "CREATE TABLE metrics (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
              (concat "INSERT INTO metrics (id, name, score) VALUES "
                      "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30), "
                      "(4, 'four', 40), (5, 'five', 50)"))
        "SELECT name, score FROM metrics ORDER BY id"
      (should (derived-mode-p 'clutch-result-mode))
      (should (memq 'clutch--header-line-display
                    (flatten-tree header-line-format)))
      (should (equal (clutch--column-names-for-indices
                      (clutch--visible-columns))
                     '("name" "score")))
      (should (equal (plist-get clutch--row-identity :indices) '(2)))
      (should (equal (mapcar (lambda (row) (cl-subseq row 0 2))
                             clutch--result-rows)
                     '(("one" 10) ("two" 20))))
      (dolist (pattern '("two" "missing" ""))
        (cl-letf (((symbol-function 'read-string)
                   (lambda (&rest _) pattern)))
          (call-interactively (key-binding (kbd "/"))))
        (should (string-match-p (regexp-quote "1-2 of 3+ rows")
                                clutch--footer-base-string))
        (should (eq (and clutch--page-has-more t) t))
        (when (equal pattern "two")
          (should (string-match-p
                   "1/2 page matches" (clutch--footer-mode-line-display))))
        (when (equal pattern "missing")
          (should (string-match-p "No matches on this page" (buffer-string))))
        (when (equal pattern "two")
          (clutch-result-copy-tsv)
          (should (string-match-p "two" (current-kill 0 t)))))
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) "score"))
                ((symbol-function 'read-string)
                 (lambda (&rest _args) "> 20")))
        (clutch-result-apply-filter)
        (should (memq 'clutch--header-line-display
                      (flatten-tree header-line-format)))
        (should (equal clutch--where-filter "\"score\" > 20"))
        (should (string-match-p "WHERE \"score\" > 20"
                                clutch--last-query))
        (should (equal (plist-get clutch--row-identity :indices) '(2)))
        (should (equal clutch--result-rows
                       '(("three" 30 3) ("four" 40 4))))
        (clutch-result-count-total)
        (should (= clutch--page-total-rows 3))
        (clutch-result-next-page)
        (should (= clutch--page-current 1))
        (should-not clutch--page-has-more)
        (should (equal (plist-get clutch--row-identity :indices) '(2)))
        (should (equal clutch--result-rows '(("five" 50 5))))
        (cl-letf (((symbol-function 'read-string)
                   (lambda (&rest _) "not-a-row")))
          (call-interactively #'clutch-result-filter))
        (should-not (clutch--result-display-rows))
        (let ((suffix
               (clutch-test--transient-suffix-for-key
                'clutch-result-export "t")))
          (should suffix)
          (cl-letf (((symbol-function 'transient-args)
                     (lambda (_prefix) nil)))
            (funcall (oref suffix command))))
        (let ((tsv (current-kill 0 t)))
          (should (equal tsv
                         "name\tscore\nthree\t30\nfour\t40\nfive\t50\n"))
          (should-not
           (string-match-p "one\\|two\\|clutch__rid\\|id\t" tsv)))))))

(ert-deftest clutch-test-cte-result-rewrites-real-sqlite-workflow ()
  "A simple query over a CTE should count, sort and filter on the server."
  (let ((clutch-result-max-rows 2))
    (clutch-test--with-sqlite-result (conn result)
        (list "CREATE TABLE metrics (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
              (concat "INSERT INTO metrics (id, name, score) VALUES "
                      "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30), "
                      "(4, 'four', 40), (5, 'five', 50)"))
        "WITH m AS (SELECT name, score FROM metrics) SELECT name, score FROM m ORDER BY score"
      (should clutch--result-server-rewritable)
      (should (equal clutch--result-source-table "metrics"))
      (should (equal clutch--result-rows '(("one" 10 1) ("two" 20 2))))
      (clutch-result-count-total)
      (should (= clutch--page-total-rows 5))
      (clutch-result--sort "score" t)
      (should (equal clutch--result-rows '(("five" 50 5) ("four" 40 4))))
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) "score"))
                ((symbol-function 'read-string)
                 (lambda (&rest _args) "< 40")))
        (clutch-result-apply-filter))
      (should (string-prefix-p "WITH m AS" clutch--last-query))
      (clutch-result-count-total)
      (should (= clutch--page-total-rows 3)))))

(ert-deftest clutch-test-cte-result-edits-base-table-real-sqlite-workflow ()
  "Edits of a CTE result should reach the one base table row they show."
  (clutch-test--with-sqlite-result (conn result)
      '("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, team TEXT)"
        "INSERT INTO people (id, name, team) VALUES (1, 'alpha', 'a'), (2, 'alpha', 'b'), (3, 'beta', 'b')")
      ;; The key is not projected, and names repeat across rows.
      "WITH c (n, t) AS (SELECT name, team FROM people) SELECT n, t FROM c ORDER BY t, n"
    (cl-flet ((table-rows ()
                (clutch-db-result-rows
                 (clutch-db-query
                  conn "SELECT id, name, team FROM people ORDER BY id")))
              (edit-first-name (value)
                (let ((row (car clutch--result-rows)))
                  (clutch-result--apply-edit
                   0 0 value
                   (list :identity (clutch-db-row-identity-values
                                    row clutch--row-identity)
                         :original (car row)
                         :original-state (cons nil (car row)))))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t)))
                  (clutch-result-submit))))
      (should (equal clutch--result-source-table "people"))
      (should (equal (clutch--insert-target-table) "people"))
      (edit-first-name "gamma")
      (should (equal (table-rows)
                     '((1 "gamma" "a") (2 "alpha" "b") (3 "beta" "b"))))
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) "t"))
                ((symbol-function 'read-string)
                 (lambda (&rest _args) "= 'b'")))
        (clutch-result-apply-filter))
      (should (plist-get clutch--row-identity :indices))
      (should (equal (mapcar (lambda (row) (cl-subseq row 0 2))
                             clutch--result-rows)
                     '(("alpha" "b") ("beta" "b"))))
      (edit-first-name "delta")
      (should (equal (table-rows)
                     '((1 "gamma" "a") (2 "delta" "b") (3 "beta" "b")))))))

(ert-deftest clutch-test-cte-result-deletes-and-inserts-real-sqlite-workflow ()
  "Deletes and inserts through a CTE result should reach its base table."
  (clutch-test--with-sqlite-result (conn result)
      '("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, team TEXT)"
        "INSERT INTO people (id, name, team) VALUES (1, 'alpha', 'a'), (2, 'alpha', 'b')")
      ;; The key is not projected, and names repeat across rows.
      "WITH c (n, t) AS (SELECT name, team FROM people) SELECT n, t FROM c ORDER BY t"
    (cl-flet ((table-rows ()
                (clutch-db-result-rows
                 (clutch-db-query
                  conn "SELECT id, name, team FROM people ORDER BY id"))))
      (cl-letf (((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
        ;; The second row shown shares its name with the first.
        (goto-char (aref clutch--row-start-positions 1))
        (clutch-result-delete-rows)
        (clutch-result-submit)
        (should (equal (table-rows) '((1 "alpha" "a"))))
        (setq-local clutch--pending-inserts
                    '((("id" . "3") ("name" . "omega") ("team" . "c"))))
        (clutch-result-submit)
        (should (equal (table-rows)
                       '((1 "alpha" "a") (3 "omega" "c"))))))))

(ert-deftest clutch-test-cte-result-edits-shown-row-past-identity-like-columns ()
  "An edit through a CTE should change the row shown whatever columns exist.
Outer SELECTs pass the identity on by name, so a column named like it must
not take its place, whether the query names it, the table has it, the
table sits in another schema, or the column is generated."
  (dolist (case
           '(("query alias"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)"
               "INSERT INTO people VALUES (1, 'alpha'), (2, 'beta')")
              "WITH c AS (SELECT name, 2 AS clutch__rid_0 FROM people WHERE id = 1) SELECT name FROM c"
              "people")
             ("table column"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, clutch__rid_0 INTEGER)"
               "INSERT INTO people VALUES (1, 'alpha', 2), (2, 'beta', 1)")
              "WITH c AS (SELECT * FROM people WHERE id = 1) SELECT name FROM c"
              "people")
             ("table in another schema"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)"
               "INSERT INTO people VALUES (1, 'alpha'), (2, 'beta')"
               "ATTACH DATABASE ':memory:' AS aux"
               "CREATE TABLE aux.people (id INTEGER PRIMARY KEY, name TEXT, clutch__rid_0 INTEGER)"
               "INSERT INTO aux.people VALUES (1, 'alpha', 2), (2, 'beta', 1)")
              "WITH c AS (SELECT * FROM aux.people WHERE id = 1) SELECT name FROM c"
              "aux.people")
             ("generated column"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, clutch__rid_0 INTEGER GENERATED ALWAYS AS (3 - id) VIRTUAL)"
               "INSERT INTO people (id, name) VALUES (1, 'alpha'), (2, 'beta')")
              "WITH c AS (SELECT * FROM people WHERE id = 1) SELECT name FROM c"
              "people")))
    (pcase-let ((`(,label ,setup ,sql ,table) case))
      (ert-info (label)
        (clutch-test--with-sqlite-result (conn result) setup sql
          (should (equal (mapcar #'car clutch--result-rows) '("alpha")))
          (let ((row (car clutch--result-rows)))
            (clutch-result--apply-edit
             0 0 "edited"
             (list :identity (clutch-db-row-identity-values
                              row clutch--row-identity)
                   :original (car row)
                   :original-state (cons nil (car row)))))
          (cl-letf (((symbol-function 'yes-or-no-p)
                     (lambda (&rest _) t)))
            (clutch-result-submit))
          (should (equal (clutch-db-result-rows
                          (clutch-db-query
                           conn (format "SELECT id, name FROM %s ORDER BY id"
                                        table)))
                         '((1 "edited") (2 "beta")))))))))

(ert-deftest clutch-test-insert-export-names-source-columns-real-sqlite ()
  "INSERT export should name the table's columns, not the result's aliases."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-insert-export-source*"))
         (clutch--execution-refresh-timer nil))
    (unwind-protect
        (save-window-excursion
          (clutch-db-query conn "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
          (clutch-db-query conn "INSERT INTO people (id, name) VALUES (1, 'alpha')")
          (set-window-buffer (selected-window) source)
          (dolist (sql '("SELECT id AS k, name AS v FROM people"
                         "WITH c (k, v) AS (SELECT id, name FROM people) SELECT * FROM c"))
            (ert-info (sql)
              (let (result)
                (with-current-buffer source
                  (clutch-mode)
                  (setq-local clutch-connection conn
                              clutch--connection-params
                              '(:backend sqlite :database ":memory:"))
                  (erase-buffer)
                  (insert sql)
                  (clutch-execute-buffer)
                  (setq result clutch--last-result-buffer))
                (with-current-buffer result
                  (should (equal (clutch-result--build-insert-statements-for-rows
                                  clutch--result-rows (clutch--visible-columns))
                                 '("INSERT INTO people (\"id\", \"name\") VALUES (1, 'alpha');")))
                  (kill-buffer result))))))
      (clutch--execution-refresh-stop)
      (when (buffer-live-p source)
        (kill-buffer source))
      (when (clutch-db-live-p conn)
        (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-column-sizing-bounds-long-value-work ()
  "Column sizing should stop measuring once the display cap is reached."
  (let ((measure (symbol-function 'string-width))
        (long (make-string 1048576 ?x))
        (clutch-column-width-max 30))
    (cl-letf (((symbol-function 'string-width)
               (lambda (text &rest args)
                 (should (< (length text) 128))
                 (apply measure text args))))
      (should (equal (clutch--compute-column-widths
                      '("value") (make-list 50 (list long))
                      '((:name "value" :type-category text)))
                     [30])))))

(ert-deftest clutch-test-capped-width-preserves-display-width ()
  "Capped measurement should preserve Unicode, controls and compositions."
  (with-temp-buffer
    (dolist (text (list "" "abc" "中文" "á🙂" "a\tb\nc"
                        (concat (make-string 80 ?\u0301) "x")
                        (compose-string (make-string 80 ?x) 0 80 ?z)))
      (dotimes (limit 40)
        (should (= (clutch--capped-string-width text limit)
                   (min limit (string-width text))))))
    (setq-local buffer-display-table (make-display-table))
    (aset buffer-display-table ?x [?a ?b ?c])
    (should (= (clutch--capped-string-width "xxxx" 8) 8))))

(ert-deftest clutch-test-cell-prefix-preserves-isolated-mark-width ()
  "Prefix scanning must honor the buffer's isolated combining-mark width."
  (let ((measure (symbol-function 'string-width)))
    (dolist (isolated-width '(0 1))
      (cl-letf (((symbol-function 'string-width)
                 (lambda (text &rest args)
                   (if (equal text "́") isolated-width
                     (apply measure text args)))))
        (should (equal (clutch--cell-visible-prefix "á" 1)
                       (if (zerop isolated-width) '("á") '("…" . t))))))))

(defun clutch-test--check-file-export (suffix &optional target-suffix)
  "Check encoding, batching and destination preservation with SUFFIX.
When TARGET-SUFFIX is non-nil, export through a link to that suffix."
  (let* ((dir (make-temp-file "clutch-export-test-" t))
         (path (expand-file-name (concat "out.csv" suffix) dir))
         (target (when target-suffix (concat "target.csv" target-suffix)))
         (rows '((1 "中文,a") (2 "x\ny") (3 "z\uFEFF😀"))))
    (unwind-protect
        (with-temp-buffer
          (auto-compression-mode 1)
          (when target (make-symbolic-link target path))
          (setq-local clutch--result-columns '("id" "name"))
          (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                    ((symbol-function 'clutch-result--collect-all-export-rows)
                     (lambda (_on-rows) (ert-fail "File export collected all rows"))))
            (dolist (coding '(utf-8-with-signature utf-8 gb18030
                                                   utf-16 utf-16-dos utf-16le utf-16be
                                                   utf-16le-with-signature
                                                   utf-16be-with-signature-dos))
              (cl-letf (((symbol-function 'clutch--read-delimited-export-coding-system)
                         (lambda (_) coding))
                        ((symbol-function 'clutch-result--map-export-batches)
                         (lambda (function done)
                           (funcall function (seq-take rows 2))
                           (funcall function (last rows))
                           (funcall done nil))))
                (clutch--export-result 'csv 'file))
              (unless (string-empty-p suffix)
                (should (equal (with-temp-buffer
                                 (set-buffer-multibyte nil)
                                 (insert-file-contents-literally path nil 0 2)
                                 (buffer-string))
                               (unibyte-string #x1f #x8b))))
              (should (equal (with-temp-buffer
                               (set-buffer-multibyte nil)
                               (let ((coding-system-for-read 'no-conversion))
                                 (insert-file-contents path))
                               (buffer-string))
                             (encode-coding-string (clutch--export-csv-content rows)
                                                   coding))))
            (let ((before (with-temp-buffer
                            (insert-file-contents-literally path)
                            (buffer-string))))
              ;; A failure reported to DONE, as a failed page is, returns.
              (dolist (exit '(error quit incomplete failed))
                (cl-letf (((symbol-function 'clutch--read-delimited-export-coding-system)
                           (lambda (_) 'utf-8))
                          ((symbol-function 'clutch-result--map-export-batches)
                           (lambda (function done)
                             (funcall function (list (car rows)))
                             (pcase exit
                               ('error (error "Second page failed"))
                               ('quit (signal 'quit nil))
                               ('incomplete
                                (funcall function
                                         (list (list 2 (make-clutch-db-value-preview
                                                        :type 'clob :length 100
                                                        :text "preview")))))
                               ('failed (funcall done '(error "Second page failed")))))))
                  (should (eq (condition-case err
                                  (progn (clutch--export-result 'csv 'file) 'failed)
                                (error (car err)) (quit 'quit))
                              (pcase exit ('incomplete 'user-error) (_ exit))))
                  (should (equal before (with-temp-buffer
                                          (insert-file-contents-literally path)
                                          (buffer-string)))))))
            (when target (should (equal (file-symlink-p path) target)))
            (should (equal (directory-files dir nil "\\`[^.]")
                           (append (list (file-name-nondirectory path))
                                   (when target (list target)))))))
      (delete-directory dir t))))

(ert-deftest clutch-test-streamed-export-preserves-content-and-file ()
  "Check ordinary-file encoding, batching and failure preservation."
  (clutch-test--check-file-export ""))

(ert-deftest clutch-test-file-export-preserves-compression ()
  "Check gzip encoding, batching and failure preservation."
  (skip-unless (executable-find "gzip"))
  (clutch-test--check-file-export ".gz"))

(ert-deftest clutch-test-export-symlink-keeps-selected-filename-handler ()
  "The selected name controls compression even across differently named links."
  (skip-unless (executable-find "gzip"))
  (clutch-test--check-file-export ".gz" "")
  (clutch-test--check-file-export "" ".gz"))

(ert-deftest clutch-test-export-handler-writes-once-and-preserves-failures ()
  "A non-appending handler sees complete bytes and cannot damage the target."
  (let* ((dir (make-temp-file "clutch-export-handler-" t))
         (path (expand-file-name "out.clutch-test" dir))
         (rows '(("中文,a") ("last")))
         (calls 0)
         outcome handler)
    (setq handler
          (lambda (operation &rest args)
            (let ((inhibit-file-name-handlers
                   (cons handler (and (eq inhibit-file-name-operation operation)
                                      inhibit-file-name-handlers)))
                  (inhibit-file-name-operation operation))
              (if (not (eq operation 'write-region))
                  (apply operation args)
                (cl-incf calls)
                (should-not (nth 3 args))
                (let ((bytes (if (stringp (car args)) (car args)
                               (buffer-substring-no-properties
                                (car args) (cadr args)))))
                  (write-region (concat "encoded:" bytes) nil
                                (nth 2 args) nil 'silent))
                (pcase outcome
                  ('file-error (signal 'file-error '("Transform failed")))
                  ('quit (signal 'quit nil)))))))
    (unwind-protect
        (with-temp-buffer
          (setq-local clutch--result-columns '("value"))
          (dolist (result '(success file-error quit))
            (setq outcome result calls 0)
            (write-region "old\n" nil path nil 'silent)
            (set-file-modes path #o640)
            (let ((file-name-handler-alist
                   (cons (cons "\\.clutch-test\\'" handler)
                         file-name-handler-alist)))
              (cl-letf (((symbol-function 'clutch-result--map-export-batches)
                         (lambda (function done)
                           (funcall function (list (car rows)))
                           (funcall function (cdr rows))
                           (funcall done nil))))
                (should (eq (condition-case err
                                (progn
                                  (clutch-result--write-export-file
                                   'csv (cdr (assq 'csv clutch--result-export-kinds))
                                   path #'ignore :coding 'utf-8-with-signature)
                                  'success)
                              (file-error (car err))
                              (quit 'quit))
                            result))))
            (should (= calls 1))
            (should (equal (with-temp-buffer
                             (set-buffer-multibyte nil)
                             (insert-file-contents-literally path)
                             (buffer-string))
                           (if (eq result 'success)
                               (concat "encoded:"
                                       (encode-coding-string
                                        (clutch--export-csv-content rows)
                                        'utf-8-with-signature))
                             "old\n")))
            (should (= (file-modes path) #o640))
            (should (equal (directory-files dir nil "\\`[^.]")
                           '("out.clutch-test")))))
      (delete-directory dir t))))

(ert-deftest clutch-test-streamed-export-preserves-symlinks ()
  "Export follows relative link chains and preserves their targets on failure."
  (dolist (existing '(t nil))
    (dolist (outcome '(success error quit incomplete))
      (let* ((dir (make-temp-file "clutch-export-link-" t))
             (target-dir (expand-file-name "data" dir))
             (target (expand-file-name "real.csv" target-dir))
             (link (expand-file-name "export.csv" dir))
             (middle (expand-file-name "current.csv" dir)))
        (unwind-protect
            (with-temp-buffer
              (make-directory target-dir)
              (when existing
                (write-region "old\n" nil target nil 'silent)
                (set-file-modes target #o640))
              (make-symbolic-link "data/real.csv" middle)
              (make-symbolic-link "current.csv" link)
              (setq-local clutch--result-columns '("value"))
              (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) link))
                        ((symbol-function 'clutch--read-delimited-export-coding-system)
                         (lambda (_) 'utf-8))
                        ((symbol-function 'clutch-result--map-export-batches)
                         (lambda (function done)
                           (funcall function '(("new")))
                           (pcase outcome
                             ('error (error "Second page failed"))
                             ('quit (signal 'quit nil))
                             ('incomplete
                              (funcall function
                                       (list (list (make-clutch-db-value-preview
                                                    :type 'clob :length 1000
                                                    :text "preview")))))
                             (_ (funcall function '(("last")))
                                (funcall done nil))))))
                (should (eq (condition-case err
                                (progn (clutch--export-result 'csv 'file) 'success)
                              (error (car err))
                              (quit 'quit))
                            (if (eq outcome 'incomplete) 'user-error outcome))))
              (should (equal (file-symlink-p link) "current.csv"))
              (should (equal (file-symlink-p middle) "data/real.csv"))
              (if (or existing (eq outcome 'success))
                  (should (equal (with-temp-buffer
                                   (insert-file-contents target)
                                   (buffer-string))
                                 (if (eq outcome 'success)
                                     "value\nnew\nlast\n"
                                   "old\n")))
                (should-not (file-exists-p target)))
              (when existing
                (should (= (file-modes target) #o640)))
              (should (equal (directory-files dir nil "\\`[^.]")
                             '("current.csv" "data" "export.csv")))
              (should (equal (directory-files target-dir nil "\\`[^.]")
                             (when (or existing (eq outcome 'success))
                               '("real.csv")))))
          (delete-directory dir t))))))

(ert-deftest clutch-test-view-metadata-only-binary-as-preview ()
  "View a length-only JDBC BLOB as unavailable content with its actual size."
  (let ((value (car (clutch-jdbc--normalize-row '((:__type "blob" :length 3))))))
    (should (equal (string-trim-right
                    (plist-get (clutch--view-spec value '(:type-category blob)) :content))
                   "<BLOB preview; 3 total>"))))

(ert-deftest clutch-test-export-rejects-metadata-only-binary-values ()
  "Binary length metadata is not complete data; decoded text remains usable."
  (let ((metadata (car (clutch-jdbc--normalize-row '((:__type "blob" :length 3))))))
    (clutch-test--with-result-state (:columns '("body") :column-defs '((:name "body")))
      (should-error (clutch--export-csv-content (list (list metadata))) :type 'user-error)
      (let ((value (car (clutch-jdbc--normalize-row
                         '((:__type "blob" :length 4 :text "<r/>" :encoding "utf-8"))))))
        (should (equal (clutch--export-csv-content (list (list value))) "body\n<r/>\n"))))))

(ert-deftest clutch-test-query-export-content-and-bounds ()
  "Direct export writes CSV/TSV without a grid, respecting SQL bounds."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (path (make-temp-file "clutch-query-export-")))
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE items(id INTEGER PRIMARY KEY, body TEXT)")
          (clutch-db-query conn "INSERT INTO items VALUES (1, '中文,a'), (2, 'a\"b'), (3, NULL), (4, ''), (5, 'last')")
          (dolist (case '(("SELECT id, body FROM items ORDER BY id" "csv" "utf-8-bom"
                           "id,body\n1,\"中文,a\"\n2,\"a\"\"b\"\n3,\n4,\"\"\n5,last\n" 3)
                          ("SELECT id, body FROM items ORDER BY id LIMIT 1 OFFSET 1" "tsv" "utf-8"
                           "id\tbody\n2\t\"a\"\"b\"\n" 1)
                          ("WITH c AS (SELECT * FROM items) SELECT id, body FROM c WHERE 0" "csv" "utf-8"
                           "id,body\n" 1)))
            (pcase-let ((`(,sql ,format ,encoding ,expected ,query-count) case))
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn)
                (insert "SELECT 'outside';\n" sql "; -- trailing comment\n")
                (goto-char (+ (point-min) (length "SELECT 'outside';\n") 1))
                (let ((clutch-result-max-rows 4)
                      (clutch-export-page-size 2)
                      (query (symbol-function 'clutch-db-query))
                      executed)
                  (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                            ((symbol-function 'yes-or-no-p) (lambda (&rest _) t))
                            ((symbol-function 'clutch-db-query)
                             (lambda (connection sql)
                               (push sql executed)
                               (funcall query connection sql)))
                            ((symbol-function 'clutch-result--display-select)
                             (lambda (&rest _) (ert-fail "Export displayed a grid")))
                            ((symbol-function 'clutch-result--check-pending-changes)
                             (lambda () (ert-fail "Export would discard staged edits"))))
                    (clutch-test--with-minibuffer-answers (list format encoding)
                      (call-interactively #'clutch-export-query)))
                  (should (= (length executed) query-count))
                  (should-not clutch--last-result-buffer)
                  (should-not (clutch-db--foreground-busy-p conn))
                  (should (equal (with-temp-buffer
                                   (insert-file-contents path)
                                   (buffer-string))
                                 expected))
                  (let ((bytes (with-temp-buffer
                                 (set-buffer-multibyte nil)
                                 (insert-file-contents-literally path)
                                 (buffer-string))))
                    (should (eq (string-prefix-p (unibyte-string #xef #xbb #xbf) bytes)
                                (equal encoding "utf-8-bom")))
                    (when (equal encoding "utf-8-bom")
                      (should-not (string-match-p (unibyte-string #xef #xbb #xbf)
                                                  (substring bytes 3))))))))))
      (delete-file path)
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-export-default-settings ()
  "Accept configured defaults in query and result file exports."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
         (dir (make-temp-file "clutch-export-defaults-" t))
         (source-dir (expand-file-name "source/" dir))
         (export-dir (expand-file-name "exports/" dir)))
    (unwind-protect
        (progn
          (make-directory source-dir)
          (make-directory export-dir)
          (dolist (custom '(nil t))
            (ert-info ((if custom "configured defaults" "original defaults"))
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn
                            default-directory source-dir)
                (insert "SELECT 7 AS id, '中文' AS name, NULL AS missing, '' AS empty, 'NULL' AS marker")
                (let* ((clutch-export-default-format (if custom 'tsv 'csv))
                       (clutch-export-null-value-text (if custom "NULL" ""))
                       (clutch-csv-export-default-coding-system
                        (if custom 'utf-8 'utf-8-with-signature))
                       (clutch-export-default-directory (and custom export-dir))
                       (clutch-export-default-file-name (and custom "report.data"))
                       (path (expand-file-name (if custom "report.data" "export.csv")
                                               (if custom export-dir source-dir)))
                       (expected
                        (if custom
                            "id\tname\tmissing\tempty\tmarker\n7\t中文\tNULL\t\t\"NULL\"\n"
                          "id,name,missing,empty,marker\n7,中文,,\"\",NULL\n")))
                  (cl-letf (((symbol-function 'read-file-name)
                             (lambda (_prompt directory _default _mustmatch initial)
                               (expand-file-name initial (or directory default-directory)))))
                    (dolist (surface '(query result))
                      (if (eq surface 'query)
                          (clutch-test--with-minibuffer-answers '("" "")
                            (call-interactively #'clutch-export-query))
                        (setq-local clutch--result-columns
                                    '("id" "name" "missing" "empty" "marker")
                                    clutch--result-rows '((7 "中文" nil "" "NULL")))
                        (clutch-test--with-minibuffer-answers '("")
                          (clutch--export-result clutch-export-default-format 'file)))
                      (should (equal (with-temp-buffer
                                       (set-buffer-multibyte nil)
                                       (insert-file-contents-literally path)
                                       (buffer-string))
                                     (encode-coding-string
                                      expected clutch-csv-export-default-coding-system)))
                      (delete-file path)))
                  ;; The configured name is for CSV and TSV; SQL keeps its own.
                  (let (offered)
                    (cl-letf (((symbol-function 'read-file-name)
                               (lambda (_prompt directory _default _mustmatch initial)
                                 (setq offered (list directory initial))
                                 (expand-file-name initial directory)))
                              ((symbol-function 'clutch-result--write-export-file)
                               #'ignore)
                              ((symbol-function 'message) #'ignore))
                      (clutch--export-result 'insert 'file))
                    (should (equal offered
                                   (list (and custom export-dir) "export.sql")))))))))
      (clutch-db-disconnect conn)
      (delete-directory dir t))))

(ert-deftest clutch-test-query-export-rejects-unsupported-statements ()
  "Invalid exports and declined overwrites start no query or file writes.
An export is also refused before its prompts while a query runs on the
connection."
  (dolist (sql '("SELECT 1; SELECT 2" "DELETE FROM t" "SELECT * INTO copy FROM t"
                 "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d" "-- comment"))
    (with-temp-buffer
      (insert sql)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda () (ert-fail "Invalid SQL reached the connection"))))
        (should-error (clutch-export-query (point-min) (point-max)) :type 'user-error))))
  (with-temp-buffer
    (insert "SELECT 1")
    (setq-local clutch-connection 'fake-conn)
    (let ((clutch--running-queries (make-hash-table :test 'eq)))
      (puthash 'fake-conn (list :buffer (current-buffer)) clutch--running-queries)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _) (ert-fail "A running query reached the prompts"))))
        (should (equal (cadr (should-error (clutch-export-query (point-min) (point-max))
                                           :type 'user-error))
                       "A query is running on this connection; C-g cancels it")))))
  (let ((path (make-temp-file "clutch-export-declined-")))
    (unwind-protect
        (with-temp-buffer
          (insert "SELECT 1")
          (setq-local clutch-connection 'fake-conn)
          (write-region "original" nil path nil 'silent)
          (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                    ((symbol-function 'clutch-db-sql-surface-p) (lambda (_conn _params) t))
                    ((symbol-function 'read-file-name) (lambda (&rest _) path))
                    ((symbol-function 'yes-or-no-p) (lambda (&rest _) nil))
                    ((symbol-function 'clutch-db-query-async)
                     (lambda (&rest _) (ert-fail "Declined export started SQL"))))
            (clutch-test--with-minibuffer-answers '("csv" "utf-8")
              (should-error (clutch-export-query (point-min) (point-max)) :type 'user-error)))
          (should (equal (with-temp-buffer (insert-file-contents path) (buffer-string))
                         "original")))
      (delete-file path))))

(ert-deftest clutch-test-query-export-async-lifecycle ()
  "Direct export stays atomic on failure, cancel, kill, move and handler quit."
  (dolist (outcome '(success page-error cancelled cancel-refused cancel-error
                             killed moved quit incomplete))
    (ert-info ((symbol-name outcome))
      (let* ((dir (make-temp-file "clutch-query-export-async-" t))
             (path (expand-file-name "out.tsv" dir))
             (source (generate-new-buffer " *clutch-query-export*"))
             completions)
        (unwind-protect
            (clutch-test--with-async-statements finishes
              (write-region "original" nil path nil 'silent)
              (with-current-buffer source
                (insert "SELECT id FROM t")
                (setq-local clutch-connection 'async-conn)
                (let ((clutch-export-page-size 2))
                  (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                            ((symbol-function 'clutch-db-sql-surface-p) (lambda (_conn _params) t))
                            ((symbol-function 'read-file-name) (lambda (&rest _) path))
                            ((symbol-function 'yes-or-no-p) (lambda (&rest _) t))
                            ((symbol-function 'clutch-db-build-paged-sql)
                             (lambda (_conn _sql page-num &rest _)
                               (format "SELECT id FROM t PAGE %d" page-num)))
                            ((symbol-function 'message)
                             (lambda (fmt &rest args)
                               (when (and fmt (string-prefix-p "Exported " fmt))
                                 (push (apply #'format-message fmt args)
                                       completions)))))
                    (clutch-test--with-minibuffer-answers '("tsv" "utf-8")
                      (clutch-export-query (point-min) (point-max)))
                    (funcall (cdar finishes)
                             (make-clutch-db-result :columns '((:name "id"))
                                                    :rows '((1) (2))) nil)
                    (ert-run-idle-timers)
                    (should (equal (caar finishes) "SELECT id FROM t PAGE 1"))
                    (should (equal (with-temp-buffer (insert-file-contents path)
                                                     (buffer-string)) "original"))
                    (pcase outcome
                      ((or 'cancelled 'cancel-refused 'cancel-error)
                       (cl-letf (((symbol-function 'clutch-db-interrupt-query)
                                  (lambda (_)
                                    (pcase outcome
                                      ('cancelled t)
                                      ('cancel-error
                                       (signal 'clutch-db-error '("Cancel refused")))))))
                         (clutch-cancel-query-or-quit)))
                      ('killed (kill-buffer source))
                      ('moved (setq-local clutch-connection 'another-conn)))
                    (let ((write (symbol-function 'write-region)))
                      (cl-letf (((symbol-function 'write-region)
                                 (lambda (&rest args)
                                   (if (eq outcome 'quit)
                                       (signal 'quit nil)
                                     (apply write args)))))
                        (if (eq outcome 'page-error)
                            (funcall (cdar finishes) nil '(clutch-db-error "query failed"))
                          (funcall (cdar finishes)
                                   (make-clutch-db-result
                                    ;; Oracle's later pages append an RN column.
                                    :columns '((:name "id") (:name "RN"))
                                    :rows (if (eq outcome 'incomplete)
                                              (list (list (make-clutch-db-value-preview
                                                           :type 'clob :length 100
                                                           :text "preview")))
                                            (if (memq outcome '(cancelled cancel-refused cancel-error))
                                                '((3 99) (4 100))
                                              '((3 99))))) nil))
                        (should-not
                         (condition-case nil
                             (progn (ert-run-idle-timers) nil)
                           (quit t))))))))
              (should (= (length finishes) 2))
              (should-not (clutch-db--foreground-busy-p 'async-conn))
              (should-not (gethash 'async-conn clutch--running-queries))
              (should (equal completions
                             (when (eq outcome 'success)
                               (list (format "Exported 3 rows to %s (utf-8)" path)))))
              (should (equal (with-temp-buffer (insert-file-contents path) (buffer-string))
                             (if (eq outcome 'success) "id\n1\n2\n3\n" "original")))
              (should (equal (directory-files dir nil "\\`[^.]") '("out.tsv"))))
          (when (buffer-live-p source) (kill-buffer source))
          (delete-directory dir t))))))

(ert-deftest clutch-test-query-export-streams-when-the-backend-can ()
  "Direct export streams formatted text when the backend offers it.
It sends no paged query, writes each part in the chosen encoding and
reports the rows the backend counted, and a failed or cancelled stream
leaves the destination and no temporary file behind."
  (dolist (outcome '(success error cancelled))
    (ert-info ((symbol-name outcome))
      (let* ((dir (make-temp-file "clutch-query-export-stream-" t))
             (path (expand-file-name "out.tsv" dir))
             (source (generate-new-buffer " *clutch-query-export*"))
             stream interrupted messages)
        (unwind-protect
            (clutch-test--with-async-statements finishes
              (write-region "original" nil path nil 'silent)
              (with-current-buffer source
                (insert "SELECT id, name FROM t")
                (setq-local clutch-connection 'async-conn)
                (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                          ((symbol-function 'clutch-db-sql-surface-p)
                           (lambda (_conn _params) t))
                          ((symbol-function 'read-file-name) (lambda (&rest _) path))
                          ((symbol-function 'yes-or-no-p) (lambda (&rest _) t))
                          ((symbol-function 'clutch-db-delimited-export-async)
                           (lambda (&rest arguments) (setq stream arguments) t))
                          ((symbol-function 'clutch-db-interrupt-query)
                           (lambda (_conn) (setq interrupted t)))
                          ((symbol-function 'message)
                           (lambda (format &rest arguments)
                             (push (apply #'format format arguments) messages))))
                  (clutch-test--with-minibuffer-answers '("tsv" "utf-8-bom")
                    (clutch-export-query (point-min) (point-max)))
                  (pcase-let ((`(,conn ,sql ,delimiter ,header ,null-text
                                       ,function ,callback)
                               stream))
                    (should (equal (list conn sql delimiter header null-text)
                                   (list 'async-conn "SELECT id, name FROM t" ?\t t
                                         clutch-export-null-value-text)))
                    (funcall function "id\tname\n1\ta\n" 1)
                    (funcall function "2\t中文\n" 1)
                    (pcase outcome
                      ('success
                       (funcall callback (make-clutch-db-result :affected-rows 2) nil))
                      ('error
                       (funcall callback nil '(clutch-db-error "disk full")))
                      ('cancelled
                       (clutch-cancel-query-or-quit)
                       (should interrupted)
                       (funcall callback nil
                                '(clutch-db-error "canceling statement"))))
                    (ert-run-idle-timers))))
              (should-not finishes)
              (should-not (clutch-db--foreground-busy-p 'async-conn))
              (should-not (gethash 'async-conn clutch--running-queries))
              (should (equal (with-temp-buffer
                               (set-buffer-multibyte nil)
                               (insert-file-contents-literally path)
                               (buffer-string))
                             (if (eq outcome 'success)
                                 (encode-coding-string "id\tname\n1\ta\n2\t中文\n"
                                                       'utf-8-with-signature)
                               "original")))
              (should (equal (directory-files dir nil "\\`[^.]") '("out.tsv")))
              (when (eq outcome 'success)
                (should (string-prefix-p "Exported 2 rows to" (car messages)))))
          (when (buffer-live-p source) (kill-buffer source))
          (delete-directory dir t))))))

(ert-deftest clutch-test-file-export-pages-through-sqlite ()
  "Paged file output should equal complete formatting for every SQL format."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (path (make-temp-file "clutch-export-pages-")))
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE items(id INTEGER PRIMARY KEY, body TEXT)")
          (clutch-db-query conn "INSERT INTO items VALUES (1, '中文,a'), (2, 'x'), (3, NULL), (4, ''), (5, 'z')")
          (clutch-test--with-result-state
              (:connection conn :base-query "SELECT id, body FROM items ORDER BY id"
               :last-query "SELECT id, body FROM items ORDER BY id"
               :server-pageable t :columns '("id" "body")
               :column-defs '((:name "id" :type-category numeric)
                              (:name "body" :type-category text)))
            (setq-local clutch--result-source-table "items"
                        clutch--row-identity
                        (clutch-test--primary-row-identity "items" '("id") '(0)))
            (let ((clutch-result-max-rows 4)
                  (clutch-export-page-size 2)
                  (rows (clutch-db-result-rows
                         (clutch-db-query conn clutch--base-query))))
              (dolist (kind '(csv tsv insert update))
                (dolist (omit-header '(nil t))
                  (let* ((spec (cdr (assq kind clutch--result-export-kinds)))
                         (content (plist-get spec :content))
                         (expected (if (memq kind '(csv tsv))
                                       (funcall content rows omit-header)
                                     (funcall content rows)))
                         (calls 0)
                         (query (symbol-function 'clutch-db-query))
                         executed
                         (format-batch (symbol-function content)))
                    (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                              ((symbol-function 'clutch--read-delimited-export-coding-system)
                               (lambda (_) 'utf-8))
                              ((symbol-function 'clutch-db-query)
                               (lambda (connection sql)
                                 (push sql executed)
                                 (funcall query connection sql)))
                              ((symbol-function content)
                               (lambda (batch &rest args)
                                 (should (<= (length batch) 2))
                                 (cl-incf calls)
                                 (apply format-batch batch args))))
                      (clutch--export-result kind 'file omit-header))
                    (should (= calls 3))
                    (should (= (length executed) 3))
                    (should (equal expected (with-temp-buffer
                                              (insert-file-contents path)
                                              (buffer-string))))))))))
      (delete-file path)
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-export-page-size-must-be-positive ()
  "Invalid page sizes fail both export paths before querying or writing data."
  (let ((path (make-temp-file "clutch-export-invalid-size-")))
    (unwind-protect
        (dolist (size '(0 -1 1.5 nil))
          (let ((clutch-export-page-size size))
            (write-region "original" nil path nil 'silent)
            (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                      ((symbol-function 'clutch-db-sql-surface-p)
                       (lambda (_conn _params) t))
                      ((symbol-function 'read-file-name) (lambda (&rest _) path))
                      ((symbol-function 'clutch-db-query-async)
                       (lambda (&rest _) (ert-fail "Invalid size started SQL"))))
              (with-temp-buffer
                (setq-local clutch-connection 'fake-conn)
                (insert "SELECT id FROM t")
                (clutch-test--with-minibuffer-answers '("csv" "utf-8")
                  (should-error (clutch-export-query (point-min) (point-max))
                                :type 'user-error)))
              (clutch-test--with-result-state
                  (:base-query "SELECT id FROM t" :server-pageable t)
                (should-error
                 (clutch-result--map-export-batches #'ignore #'ignore)
                 :type 'user-error)))
            (should (equal (with-temp-buffer
                             (insert-file-contents path) (buffer-string))
                           "original"))))
      (delete-file path))))

(ert-deftest clutch-test-export-ends-once-when-its-failure-cannot-be-shown ()
  "An export should end once even when showing a page's failure fails.
Its connection is released and its completion runs once, as a batch ends
when its continuation fails."
  (with-temp-buffer
    (clutch-test--with-async-statements finishes
      (clutch-test--init-result-state
       (list :columns '("id") :rows '((1)) :connection 'async-conn
             :base-query "SELECT id FROM t" :server-pageable t
             :result-max-rows 2))
      (let (completions)
        (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                   (lambda (_conn _sql page-num &rest _)
                     (format "SELECT id FROM t PAGE %d" page-num)))
                  ((symbol-function 'clutch--present-statement-outcome)
                   (lambda (&rest _) (error "Display failed")))
                  ((symbol-function 'message) #'ignore))
          (clutch-result--export-pages "SELECT id FROM t" "SELECT id FROM t" 2
                                       #'ignore
                                       (lambda (err) (push err completions)))
          (funcall (cdar finishes) nil '(clutch-db-error "connection reset"))
          (ignore-errors (ert-run-idle-timers))
          (should (= (length completions) 1))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-export-stops-when-its-result-loses-its-connection ()
  "An export whose result lost its connection should stop without showing the page.
The disconnect that cleared the result's connection also closed it, and the
page in flight then failed; showing that failure would put an error page in
a buffer that holds no connection, or holds another one."
  (clutch-test--with-result-state
      (:columns '("id") :rows '((1)) :connection 'async-conn
       :base-query "SELECT id FROM t" :server-pageable t :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (let (shown completions)
        (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                   (lambda (_conn _sql page-num &rest _)
                     (format "SELECT id FROM t PAGE %d" page-num)))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (not (eq conn 'async-conn))))
                  ((symbol-function 'clutch--show-execution-error)
                   (lambda (&rest _) (setq shown t) "failed"))
                  ((symbol-function 'message) #'ignore))
          (clutch-result--export-pages "SELECT id FROM t" "SELECT id FROM t" 2
                                       #'ignore
                                       (lambda (err) (push err completions)))
          ;; A disconnect elsewhere clears the result's connection.
          (with-temp-buffer
            (clutch--invalidate-derived-buffers 'async-conn))
          (funcall (cdar finishes) nil
                   '(clutch-db-error
                     "Disconnected while the statement ran; its outcome is unknown"))
          (ert-run-idle-timers)
          (should-not shown)
          (should (= (length completions) 1))
          (should (string-match-p "connection changed"
                                  (error-message-string (car completions))))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-export-stops-when-its-result-moves-during-a-synchronous-page ()
  "An export should stop when its result loses its connection during a page.
A backend that waits on the network synchronously runs timers while a page
runs, and a disconnect from one of them clears the result's connection."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (run-db-query (symbol-function 'clutch--run-db-query))
        pages completions)
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE n(i INTEGER PRIMARY KEY)")
          (clutch-db-query conn "INSERT INTO n VALUES (1), (2), (3), (4)")
          (clutch-test--with-result-state
              (:connection conn :base-query "SELECT i FROM n ORDER BY i"
               :last-query "SELECT i FROM n ORDER BY i"
               :server-pageable t :columns '("i"))
            (cl-letf (((symbol-function 'clutch--run-db-query)
                       (lambda (&rest args)
                         (push (nth 1 args) pages)
                         (prog1 (apply run-db-query args)
                           ;; A timer that runs while the page waits
                           ;; disconnects the result's connection.
                           (with-temp-buffer
                             (clutch--invalidate-derived-buffers conn)))))
                      ((symbol-function 'message) #'ignore))
              (clutch-result--export-pages
               "SELECT i FROM n ORDER BY i" "SELECT i FROM n ORDER BY i" 2
               #'ignore (lambda (err) (push err completions)))))
          (should (= (length pages) 1))
          (should (= (length completions) 1))
          (should (string-match-p "connection changed"
                                  (error-message-string (car completions)))))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-file-export-of-many-pages-keeps-a-flat-stack ()
  "An export should fetch many synchronous pages without nesting them.
SQLite runs each page before its callback returns, which a recursive page
loop turns into a stack as deep as the pages are many."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (path (make-temp-file "clutch-export-many-"))
        exported)
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE n(i INTEGER PRIMARY KEY)")
          (clutch-db-query
           conn "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 800) INSERT INTO n SELECT i FROM c")
          (clutch-test--with-result-state
              (:connection conn :base-query "SELECT i FROM n ORDER BY i"
               :last-query "SELECT i FROM n ORDER BY i"
               :server-pageable t :columns '("i"))
            ;; Far fewer frames than pages: a page that nested the next
            ;; would run out of depth long before the last.
            (let ((clutch-export-page-size 1)
                  (max-lisp-eval-depth 1000))
              (cl-letf (((symbol-function 'message) #'ignore))
                (clutch-result--write-export-file
                 'csv (cdr (assq 'csv clutch--result-export-kinds)) path
                 (lambda (row-count) (setq exported row-count))
                 :coding 'utf-8))))
          (should (eql exported 800)))
      (delete-file path)
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-collect-all-export-rows-contract ()
  "Export row collection should page, reuse local rows, and reconnect when needed."
  (dolist (case '(("plain limit"
                   "SELECT id FROM t LIMIT 2" nil ((1) (2)) 100)
                  ("sorted limit"
                   "SELECT id FROM t LIMIT 3" ("id" . "DESC")
                   ((3) (2) (1)) 2)))
    (pcase-let ((`(,label ,query ,order-by ,rows ,max-rows) case))
      (ert-info ((format "nonpageable: %s" label))
        (clutch-test--with-result-state
            (:base-query query
             :last-query query
             :order-by order-by
             :server-pageable nil
             :rows rows)
          (let ((clutch-result-max-rows max-rows)
                (queries 0)
                paginated)
            (cl-letf (((symbol-function 'clutch--connection-alive-p)
                       (lambda (_conn) t))
                      ((symbol-function 'clutch-db-build-paged-sql)
                       (lambda (&rest _args)
                         (setq paginated t)
                         "unexpected page query"))
                      ((symbol-function 'clutch-db-query)
                       (lambda (_conn _sql)
                         (cl-incf queries)
                         (make-clutch-db-result :rows rows))))
              (let (collected)
                (clutch-result--collect-all-export-rows
                 (lambda (all) (setq collected all)))
                (should (equal collected rows)))
              (should (= queries 0))
              (should-not paginated)))))))
  (ert-info ("reconnect before querying")
    (clutch-test--with-result-state
        (:connection 'stale-conn
         :base-query "SELECT id FROM t LIMIT 1"
         :last-query "SELECT id FROM t LIMIT 1"
         :server-pageable t)
      (let (ensured captured-conn)
        (cl-letf (((symbol-function 'clutch--ensure-connection)
                   (lambda ()
                     (setq ensured t)
                     (setq-local clutch-connection 'new-conn)))
                  ((symbol-function 'clutch-db-query)
                   (lambda (conn _sql)
                     (setq captured-conn conn)
                     (make-clutch-db-result :rows '((1))))))
          (let (collected)
            (clutch-result--collect-all-export-rows
             (lambda (all) (setq collected all)))
            (should (equal collected '((1)))))
          (should ensured)
          (should (eq captured-conn 'new-conn)))))))

(ert-deftest clutch-test-execute-select-detects-primary-key-before-first-render ()
  "Primary-key identity should be ready before the first result render."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (clutch--source-window (selected-window))
        (result-name "*clutch-test-result*")
        (captured-identity :unset))
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql _page-num _page-size
                              &optional _order-by _page-offset)
                 sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id)))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id") (:name "name") (:name "clutch__rid_0"))
                  :rows '((1 "a" 1))))))
      (clutch-test--with-result-buffer
          (result-name (lambda (&rest _args)
                         (setq captured-identity clutch--row-identity)))
        (clutch-test--execute-and-present "SELECT * FROM users" 'fake-conn)
        (should (eq (plist-get captured-identity :kind) 'primary-key))
        (should (equal (plist-get captured-identity :source-indices) '(0)))
        (with-current-buffer result-name
          (should clutch--result-server-pageable)
          (should clutch--result-server-rewritable))))))

(ert-deftest clutch-test-execute-select-fetches-one-row-lookahead ()
  "Initial SELECT execution should trim lookahead rows before rendering."
  (let ((clutch--source-window (selected-window))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
        (result-name "*clutch-test-result*")
        (clutch-result-max-rows 2)
        captured-page-size
        captured-offset)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql _page-num page-size &optional _order-by page-offset)
                 (setq captured-page-size page-size
                       captured-offset page-offset)
                 sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id"))
                  :rows '((1) (2) (3))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch-test--execute-and-present "SELECT id FROM users" 'fake-conn)
        (should (= captured-page-size 3))
        (should (= captured-offset 0))
        (with-current-buffer result-name
          (should (equal clutch--result-rows '((1) (2))))
          (should clutch--page-has-more)
          (should (= clutch--page-offset 0)))))))

(ert-deftest clutch-test-execute-select-page-tailed-queries-stay-flat ()
  "Page-tailed SELECT shapes should execute directly without wrapper paging."
  (dolist (case `((offset
                   "SELECT id FROM users OFFSET 20"
                   ((:name "id"))
                   ((1))
                   nil)
                  (complex-limit
                   "SELECT c.*, cc.* FROM table_a AS c JOIN table_b AS cc ON c.id = cc.id LIMIT 10"
                   ((:name "id") (:name "name") (:name "id"))
                   ((1 "a" 1) (2 "b" 2))
                   ((1 "a" 1) (2 "b" 2)))
                  (duplicate-label-limit
                   "SELECT id AS dup, name AS dup FROM users LIMIT 10"
                   ((:name "dup") (:name "dup"))
                   ((1 "a"))
                   nil)))
    (pcase-let ((`(,label ,sql ,columns ,rows ,expected-rows) case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--source-window (selected-window))
              (clutch--row-identity-cache (make-hash-table :test 'eq))
              (result-name "*clutch-test-result*")
              captured-sql)
          (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (&rest _args)
                       (error "Should not wrap page-tailed query results")))
                    ((symbol-function 'clutch-db-row-identity-candidates)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn query)
                       (setq captured-sql query)
                       (make-clutch-db-result
                        :columns columns
                        :rows rows))))
            (clutch-test--with-result-buffer (result-name)
              (clutch-test--execute-and-present sql 'fake-conn)
              (should (equal captured-sql sql))
              (with-current-buffer result-name
                (should-not clutch--result-server-pageable)
                (should-not clutch--result-server-rewritable)
                (should-not clutch--page-has-more)
                (when (eq label 'complex-limit)
                  (should-not clutch--result-source-table))
                (when expected-rows
                  (should (equal clutch--result-rows expected-rows)))))))))))

(ert-deftest clutch-test-execute-simple-limit-select-retains-edit-source ()
  "A simple SELECT with LIMIT should retain its staged-edit source table."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (clutch--source-window (selected-window))
        (result-name "*clutch-test-result*")
        (sql "SELECT id, name FROM users LIMIT 10")
        captured-sql)
    (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id)))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn query)
                 (setq captured-sql query)
                 (make-clutch-db-result
                  :columns '((:name "id")
                             (:name "name")
                             (:name "clutch__rid_0"))
                  :rows '((1 "alice" 1))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch-test--execute-and-present sql 'fake-conn)
        (should (string-match-p "LIMIT 10\\'" captured-sql))
        (with-current-buffer result-name
          (should-not clutch--result-server-pageable)
          (should-not clutch--result-server-rewritable)
          (should (equal clutch--result-source-table "users"))
          (should (equal (clutch--result-source-table-or-user-error "edit cell")
                         "users"))
          (should (eq (plist-get clutch--row-identity :kind)
                      'primary-key)))))))

(ert-deftest clutch-test-execute-select-duplicate-labels-are-not-rewritable ()
  "Duplicate result labels should remain pageable but not relation-rewritable."
  (let ((clutch--source-window (selected-window))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
        (result-name "*clutch-test-result*")
        (sql "SELECT id AS dup, name AS dup FROM users")
        captured-build-sql)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn base-sql _page-num _page-size
                              &optional _order-by _page-offset)
                 (setq captured-build-sql base-sql)
                 base-sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn _query)
                 (make-clutch-db-result
                  :columns '((:name "dup") (:name "dup"))
                  :rows '((1 "a"))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch-test--execute-and-present sql 'fake-conn)
        (should (equal captured-build-sql sql))
        (with-current-buffer result-name
          (should clutch--result-server-pageable)
          (should-not clutch--result-server-rewritable))))))

(ert-deftest clutch-test-execute-select-honors-result-context-overrides ()
  "Internal filter SQL should keep the verified relation source capabilities."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (clutch--source-window (selected-window))
        (result-context
         '(:server-pageable t
           :row-identity-prep
           (:sql "SELECT id, name, `id` AS `clutch__rid_0` FROM users")))
        (result-name "*clutch-test-result*")
        (sql "SELECT * FROM (SELECT id, name FROM users) AS _clutch_filter WHERE id > 1")
        captured-base-sql)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn base-sql _page-num _page-size
                              &optional _order-by _page-offset)
                 (setq captured-base-sql base-sql)
                 base-sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _args)
                 (error "Should use context row identity prep")))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn _query)
                 (make-clutch-db-result
                  :columns '((:name "id") (:name "name"))
                  :rows '((2 "bob"))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch-test--execute-and-present sql 'fake-conn result-context)
        (should (string-match-p "`id` AS `clutch__rid_0`"
                                captured-base-sql))))))

(ert-deftest clutch-test-high-risk-query-typed-confirmation-contract ()
  "Typed high-risk confirmation should proceed only for the exact token YES."
  (dolist (case '(("NO" cancel)
                  ("YES" accept)))
    (pcase-let ((`(,answer ,expected) case))
      (ert-info ((format "answer: %s" answer))
        (let ((clutch-high-risk-query-confirmation 'typed))
          (cl-letf (((symbol-function 'clutch--high-risk-query-reason)
                     (lambda (_sql) "no WHERE"))
                    ((symbol-function 'read-string)
                     (lambda (&rest _args) answer)))
            (if (eq expected 'accept)
                (should (clutch--confirm-high-risk-query
                         "UPDATE users SET x=1"))
              (should-error (clutch--confirm-high-risk-query
                             "UPDATE users SET x=1")
                            :type 'user-error))))))))

(ert-deftest clutch-test-truncate-requires-typed-confirmation-by-default ()
  "TRUNCATE should require one typed confirmation by default."
  (should (eq (default-value 'clutch-high-risk-query-confirmation) 'typed))
  (let (typed-prompts)
    (cl-letf (((symbol-function 'read-string)
               (lambda (prompt &rest _args)
                 (push prompt typed-prompts)
                 "YES"))
              ((symbol-function 'yes-or-no-p)
               (lambda (&rest _args)
                 (ert-fail "TRUNCATE used a simple confirmation"))))
      (should-not (clutch--confirm-query-execution
                   "TRUNCATE TABLE users"))
      (should (= (length typed-prompts) 1)))))

(ert-deftest clutch-test-high-risk-query-confirmation-is-customizable ()
  "High-risk confirmation should support simple and disabled policies."
  (dolist (policy '(yes-or-no nil))
    (let ((clutch-high-risk-query-confirmation policy)
          (simple-prompts 0))
      (cl-letf (((symbol-function 'read-string)
                 (lambda (&rest _args)
                   (ert-fail "Non-typed policy requested typed confirmation")))
                ((symbol-function 'yes-or-no-p)
                 (lambda (&rest _args)
                   (cl-incf simple-prompts)
                   t)))
        (should-not (clutch--confirm-query-execution
                     "TRUNCATE TABLE users"))
        (should (= simple-prompts (if policy 1 0)))))))

(ert-deftest clutch-test-confirmation-covers-statements-in-with-clause ()
  "A DELETE in or after a WITH clause should ask as a plain DELETE does."
  (dolist (case '(("WITH d AS (DELETE FROM users RETURNING id) SELECT * FROM d"
                   typed)
                  ("WITH d AS (DELETE FROM users WHERE id = 1 RETURNING id) SELECT * FROM d"
                   simple)
                  ("WITH old AS (SELECT 1) DELETE FROM users WHERE id = 1"
                   simple)
                  ("WITH i AS (INSERT INTO users VALUES (1) RETURNING id) SELECT * FROM i"
                   nil)))
    (pcase-let ((`(,sql ,expected) case))
      (ert-info ((format "sql: %s" sql))
        (let ((clutch-high-risk-query-confirmation 'typed)
              prompts)
          (cl-letf (((symbol-function 'read-string)
                     (lambda (&rest _args) (push 'typed prompts) "YES"))
                    ((symbol-function 'yes-or-no-p)
                     (lambda (&rest _args) (push 'simple prompts) t)))
            (clutch--confirm-query-execution sql)
            (should (equal prompts (and expected (list expected))))))))))

(ert-deftest clutch-test-preview-execution-sql-uses-result-pending-batch ()
  "Preview in result mode should show generated SQL for staged result changes."
  (with-temp-buffer
    (let (captured)
      (setq-local clutch-connection 'fake-conn
                  clutch--result-source-table "users"
                  clutch--result-columns '("id" "name" "note")
                  clutch--result-column-defs
                  '((:name "id" :source-column "id")
                    (:name "name" :source-column "name")
                    (:name "note" :source-column "note"))
                  clutch--row-identity
                  (clutch-test--primary-row-identity "users" '("id") '(0))
                  clutch--pending-inserts
                  '((("id" . "3") ("name" . "cat") ("note" . "NULL")))
                  clutch--pending-edits
                  (list (cons (cons (vector 1) 1) "lynx"))
                  clutch--pending-deletes
                  (list (vector 2)))
      (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
                ((symbol-function 'derived-mode-p) (lambda (&rest _modes) t))
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (&rest _)
                   '((:name "id") (:name "name") (:name "note"))))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn name) (format "`%s`" name)))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql &optional _product) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       (mapconcat #'identity
                                  '("INSERT INTO `users` (`id`, `name`, `note`) VALUES ('3', 'cat', 'NULL');"
                                    "UPDATE `users` SET `name` = 'lynx' WHERE `id` = 1;"
                                    "DELETE FROM `users` WHERE `id` = 2;")
                                  "\n")))))))

(ert-deftest clutch-test-result-effective-query-applies-where-filter ()
  "Result workflows should reuse the filtered SQL, not just display the filter."
  :tags '(:smoke)
  (with-temp-buffer
    (setq-local clutch--base-query "SELECT * FROM t"
                clutch--last-query "SELECT * FROM t WHERE id > 0"
                clutch--where-filter "id = 1")
    (cl-letf (((symbol-function 'clutch-db-apply-where)
               (lambda (_conn sql filter)
                 (format "FILTER[%s]{%s}" filter sql))))
      (should (equal (clutch-result--effective-query)
                     "FILTER[id = 1]{SELECT * FROM t}")))))

(ert-deftest clutch-test-execute-page-fetches-lookahead-and-trims-visible-rows ()
  "Paging should fetch one extra row to distinguish exact last pages."
  (with-temp-buffer
    (let (captured-page-size captured-offset)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT id FROM t"
                  clutch--result-server-pageable t
                  clutch-result-max-rows 2)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql _page-num page-size _order-by
                         &optional page-offset)
                   (setq captured-page-size page-size
                         captured-offset page-offset)
                   "SELECT id FROM t LIMIT 3 OFFSET 2"))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result
                    :columns '((:name "id"))
                    :rows '((3) (4) (5)))))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result--execute-page 1)
        (should (= captured-page-size 3))
        (should (= captured-offset 2))
        (should (equal clutch--result-rows '((3) (4))))
        (should clutch--page-has-more)
        (should (= clutch--page-offset 2))
        (should (= clutch--page-current 1))))))

(ert-deftest clutch-test-execute-page-reapplies-active-local-sort ()
  "Paging should apply an active local sort to each newly loaded page."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--base-query "SELECT id, score FROM complex_result"
                clutch--result-server-pageable t
                clutch--result-server-rewritable nil
                clutch--result-columns '("score" "score")
                clutch--result-column-defs '((:name "score") (:name "score"))
                clutch--sort-column "score"
                clutch--sort-descending nil
                clutch--local-sort-column-index 1
                clutch--order-by nil
                clutch-result-max-rows 3)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-build-paged-sql)
               (lambda (&rest _args) "SELECT page"))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id") (:name "score"))
                  :rows '((4 30) (5 10) (6 20)))))
              ((symbol-function 'clutch--refresh-display) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-result--execute-page 1)
      (should (equal (mapcar #'car clutch--result-rows) '(5 6 4)))
      (should (= clutch--local-sort-column-index 1))
      (should (equal clutch--local-sort-original-rows
                     '((4 30) (5 10) (6 20)))))))

(ert-deftest clutch-test-count-total-errors-for-nonrewritable-query-result ()
  "COUNT should not wrap arbitrary query results in a derived table."
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable nil
                clutch-connection 'fake-conn
                clutch--base-query "SELECT a.*, b.* FROM a JOIN b ON a.id = b.id LIMIT 10"
                clutch--last-query clutch--base-query)
    (let (queried)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (&rest _args) (error "Should not build COUNT SQL")))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (&rest _args) (setq queried t))))
        (let ((err (should-error (clutch-result-count-total)
                                 :type 'user-error)))
          (should (string-match-p "Server-side count"
                                  (error-message-string err))))
        (should-not queried)))))

(ert-deftest clutch-test-result-query-commands-ensure-connection-before-query ()
  "Result query commands should reconnect before querying."
  (dolist (command '(page count))
    (ert-info ((format "command: %s" command))
      (with-temp-buffer
        (let (ensured captured-conn)
          (setq-local clutch-connection 'stale-conn
                      clutch--base-query "SELECT * FROM t"
                      clutch--result-server-pageable t
                      clutch--result-server-rewritable t
                      clutch-result-max-rows 100)
          (cl-letf (((symbol-function 'clutch--ensure-connection)
                     (lambda ()
                       (setq ensured t)
                       (setq-local clutch-connection 'new-conn)))
                    ((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_conn _sql _page-num _page-size
                              &optional _order-by _page-offset)
                       "SELECT * FROM paged"))
                    ((symbol-function 'clutch-db-build-count-sql)
                     (lambda (_conn _sql) "SELECT COUNT(*)"))
                    ((symbol-function 'clutch-db-query)
                     (lambda (conn _sql)
                       (setq captured-conn conn)
                       (make-clutch-db-result :columns nil :rows '((3)))))
                    ((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                    ((symbol-function 'message) #'ignore))
            (pcase command
              ('page (clutch-result--execute-page 0))
              ('count
               (clutch-result-count-total)
               (should (= clutch--page-total-rows 3))))
            (should ensured)
            (should (eq captured-conn 'new-conn))))))))

(ert-deftest clutch-test-execute-page-remembers-error-details-and-debug-event ()
  "Paging failures should populate `current-buffer' error details and trace.
The page stays, and the message says so."
  (with-temp-buffer
    (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
          (clutch-debug-mode t)
          (raw-message "Connection refused (host=db.example.com, port=3306)")
          messages)
      (clutch--clear-debug-capture)
      (setq-local clutch-connection conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--result-server-pageable t
                  clutch--result-rows '((1))
                  clutch-result-max-rows 100)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages)))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql _page-num _page-size
                              &optional _order-by _page-offset)
                   "SELECT * FROM t LIMIT 100 OFFSET 0"))
                ((symbol-function 'clutch-db-backend-key)
                 (lambda (_conn) 'pg))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (signal 'clutch-db-error (list raw-message)))))
        (let ((display-summary
               (condition-case err
                   (signal 'clutch-db-error (list raw-message))
                 (clutch-db-error
                  (clutch--humanize-db-error (error-message-string err))))))
          (clutch-result--execute-page 0)
          (should (equal clutch--result-rows '((1))))
          (should (string-suffix-p "(result unchanged)" (car messages)))
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (debug-text (clutch-test--debug-buffer-string)))
            (should details)
            (should (eq (plist-get details :backend) 'pg))
            (should (equal (plist-get details :summary)
                           (clutch--humanize-db-error raw-message)))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get (plist-get diag :context) :sql)
                           "SELECT * FROM t"))
            (should (string-match-p "Phase: error" debug-text))
            (should (string-match-p
                     (regexp-quote display-summary) debug-text))))))))

(ert-deftest clutch-test-execute-dml-skips-debug-backend-lookup-when-disabled ()
  "DML execution should not consult debug-only backend state when debug is off."
  (with-temp-buffer
    (let ((clutch-debug-mode nil)
          rendered)
      (cl-letf (((symbol-function 'clutch-db-backend-key)
                 (lambda (_conn)
                   (error "Debug-disabled path should not resolve backend key")))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'clutch-db-sql-schema-affecting-p)
                 (lambda (_sql) nil))
                ((symbol-function 'clutch-result--display)
                 (lambda (result sql _elapsed)
                   (setq rendered (list result sql)))))
        (should (clutch-test--execute-and-present
                 "UPDATE demo SET enabled = 1 WHERE id = 1" 'fake-conn))
        (should (equal (cadr rendered)
                       "UPDATE demo SET enabled = 1 WHERE id = 1"))
        (should (= (clutch-db-result-affected-rows (car rendered)) 1))))))

(ert-deftest clutch-test-result-sql-commands-use-effective-filtered-query ()
  "Result SQL commands should consume the active filtered query."
  (dolist (command '(rerun preview))
    (ert-info ((format "command: %s" command))
      (with-temp-buffer
        (clutch-result-mode)
        (let (captured)
          (setq-local clutch-connection 'fake-conn
                      clutch--base-query "SELECT * FROM t"
                      clutch--where-filter "id = 1")
          (cl-letf (((symbol-function 'clutch-db-apply-where)
                     (lambda (_conn sql filter)
                       (format "FILTER[%s]{%s}" filter sql)))
                    ((symbol-function 'clutch--execute)
                     (lambda (sql &optional context)
                       (setq captured (list sql context))))
                    ((symbol-function 'clutch--preview-sql-buffer)
                     (lambda (sql &optional _product)
                       (setq captured sql))))
            (pcase command
              ('rerun
               (clutch-result-rerun)
               (should (equal (car captured)
                              "FILTER[id = 1]{SELECT * FROM t}"))
               ;; The filter goes with it, so the new result keeps it.
               (should (equal (plist-get (nth 1 captured) :where-filter)
                              "id = 1")))
              ('preview
               (clutch-preview-execution-sql)
               (should (equal captured
                              "FILTER[id = 1]{SELECT * FROM t}"))))))))))

(ert-deftest clutch-test-preview-execution-sql-prefers-semicolon-statement-bounds-in-sql-buffer ()
  "Preview should mirror DWIM statement bounds for semicolon-delimited SQL buffers."
  (with-temp-buffer
    (insert "INSERT INTO demo(note) VALUES (E'first line\n\nthird line');\n\nSELECT 2")
    (goto-char (point-min))
    (search-forward "third")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql &optional _product) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       "INSERT INTO demo(note) VALUES (E'first line\n\nthird line')"))))))

(ert-deftest clutch-test-preview-leaves-the-result-buffer-selected ()
  "\\`C-c C-c' after \\`C-c C-p' should submit from the result buffer.
The preview is in `sql-mode', where \\`C-c C-c' sends to an SQL process."
  (let ((result (generate-new-buffer " *clutch-preview-focus*")))
    (unwind-protect
        (save-window-excursion
          (delete-other-windows)
          (split-window)
          (switch-to-buffer result)
          (clutch-result-mode)
          (cl-letf (((symbol-function 'clutch-result--preview-execution-sql)
                     (lambda () "UPDATE t SET a = 1 WHERE id = 1;")))
            (call-interactively (key-binding (kbd "C-c C-p"))))
          (should (eq (window-buffer (selected-window)) result))
          (should (eq (key-binding (kbd "C-c C-c")) #'clutch-result-submit))
          (should (get-buffer-window "*clutch-preview*")))
      (kill-buffer result)
      (when-let* ((preview (get-buffer "*clutch-preview*")))
        (kill-buffer preview)))))

(ert-deftest clutch-test-preview-sql-buffer-uses-local-connection-product ()
  "SQL previews should use their source dialect without changing the default."
  (let ((default-product (default-value 'sql-product))
        preview-buffer)
    (unwind-protect
        (cl-letf (((symbol-function 'display-buffer)
                   (lambda (buf &rest _args)
                     (setq preview-buffer buf)
                     nil)))
          (clutch--preview-sql-buffer
           "SELECT NVL(name, 0) FROM DUAL" 'oracle)
          (with-current-buffer preview-buffer
            (font-lock-ensure)
            (should (local-variable-p 'sql-product))
            (should (eq sql-product 'oracle))
            (goto-char (point-min))
            (search-forward "NVL")
            (should (get-text-property (match-beginning 0) 'face)))
          (should (eq (default-value 'sql-product) default-product)))
      (when (buffer-live-p preview-buffer)
        (kill-buffer preview-buffer)))))

(ert-deftest clutch-test-statement-breaks-respect-mysql-backslash-escapes ()
  "A MySQL escaped quote must not end the literal and split the statement.
Splitting there would send the fragment before the semicolon as a whole
statement."
  (let* ((sql "UPDATE t SET note = 'it\\'s here; keep' WHERE id = 1; SELECT 2;")
         (mysql (clutch-db-sql-dialect 'mysql))
         (inside (string-search "; keep" sql)))
    ;; Only the two real terminators, both outside the literal.
    (should (equal (clutch-db-sql-statement-breaks sql mysql)
                   (list (string-search ";" sql (string-search "id = 1" sql))
                         (1- (length sql)))))
    ;; Without the dialect the escaped quote is read as the literal's end, so
    ;; the semicolon inside the value is taken for a statement terminator.
    (should (member inside (clutch-db-sql-statement-breaks sql)))
    (should-not (member inside (clutch-db-sql-statement-breaks sql mysql)))
    ;; A trailing backslash keeps its standard meaning where the dialect has
    ;; no backslash escape, so the literal still ends at the closing quote.
    (should (= (length (clutch-db-sql-statement-breaks
                        "SELECT 'a\\'; SELECT 2;"))
               2))
    (should (equal (clutch-db-sql-mask-literal-or-comment
                    "SELECT 'a\\'b' FROM t" mysql)
                   "SELECT '    ' FROM t"))))

(ert-deftest clutch-test-sql-dialect-rules ()
  "Dialect lookup should only claim rules a product actually has."
  (should (equal (clutch-db-sql-dialect 'postgres) '(:dollar-quotes t)))
  (should (equal (clutch-db-sql-dialect 'mysql) '(:backslash-escapes t)))
  (dolist (product '(sqlite oracle ms db2 nil))
    (should-not (clutch-db-sql-dialect product))))

(ert-deftest clutch-test-statement-breaks-ignore-postgresql-dollar-quotes ()
  "Dollar-quoted function bodies should remain one executable statement."
  (dolist (case '(("$$" . "PERFORM 1; PERFORM 2;")
                  ("$body$" . "SELECT ';'; RETURN;")))
    (let* ((pg (clutch-db-sql-dialect 'postgres))
           (delimiter (car case))
           (body (cdr case))
           (sql (format (concat "CREATE FUNCTION f() RETURNS void AS %s%s%s "
                                "LANGUAGE plpgsql; SELECT 2;")
                        delimiter body delimiter))
           (body-open (string-search delimiter sql))
           (body-close (+ (string-search delimiter sql
                                          (+ body-open (length delimiter)))
                          (length delimiter)))
           (breaks (clutch-db-sql-statement-breaks sql pg)))
      (ert-info ((format "delimiter: %s" delimiter))
        (should (= (length breaks) 2))
        (should (cl-every (lambda (offset) (>= offset body-close)) breaks)))))
  (should (= (length (clutch-db-sql-statement-breaks
                      "SELECT $1; SELECT 2;" (clutch-db-sql-dialect 'postgres)))
             2))
  (should (= (length (clutch-db-sql-statement-breaks
                      "SELECT $tag$; SELECT 2;"))
             2))
  (should (= (length (clutch-db-sql-statement-breaks
                      "SELECT foo$tag$; SELECT 2;"
                      (clutch-db-sql-dialect 'postgres)))
             2))
  (string-match "needle" "needle")
  (let ((saved-match-data (match-data)))
    (clutch-db-sql-statement-breaks
     (concat "SELECT " (mapconcat #'identity
                                  (make-list 4000 "$1") ",") ";")
     (clutch-db-sql-dialect 'postgres))
    (should (equal (match-data) saved-match-data))))

(ert-deftest clutch-test-postgresql-statement-bounds-enable-dollar-quotes ()
  "Query bounds should enable dollar quotes only for PostgreSQL products."
  (let ((sql (concat "CREATE FUNCTION f() RETURNS void AS $$"
                     "PERFORM 1; PERFORM 2;"
                     "$$ LANGUAGE plpgsql; SELECT 2;")))
    (with-temp-buffer
      (insert sql)
      (search-backward "PERFORM 1")
      (let ((clutch--conn-sql-product 'postgres))
        (pcase-let ((`(,beg . ,end) (clutch--statement-bounds-at-point)))
          (should (equal (string-trim
                          (buffer-substring-no-properties beg end))
                         (substring sql 0 (string-search "; SELECT" sql)))))))))

(ert-deftest clutch-test-execute-params-fallback-renders-sql-before-query ()
  "Fallback parameter execution should render SQL via escape helpers."
  (let (captured-sql)
    (cl-letf (((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn value)
                 (format "'%s'" value)))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 'ok)))
      (should (eq (clutch-db-execute-params
                   'fake-conn
                   "UPDATE demo SET name = ?, age = ? WHERE note IS ?"
                   '("alice" 7 nil))
                  'ok))
      (should (equal captured-sql
                     "UPDATE demo SET name = 'alice', age = 7 WHERE note IS NULL")))))

(ert-deftest clutch-test-execute-params-fallback-json-error-surfaces ()
  "Fallback parameter execution should signal when JSON parameter serialization fails."
  (let ((payload (make-hash-table :test 'equal))
        query-called)
    (puthash "key" "value" payload)
    (cl-letf (((symbol-function 'json-serialize)
               (lambda (_value)
                 (signal 'wrong-type-argument '("json serialization failed"))))
              ((symbol-function 'clutch-db-query)
               (lambda (&rest _args)
                 (setq query-called t)
                 'unexpected)))
      (let ((err (should-error
                  (clutch-db-execute-params
                   'fake-conn
                   "INSERT INTO demo(payload) VALUES (?)"
                   (list payload))
                  :type 'clutch-db-error)))
        (should-not query-called)
        (should (string-match-p
                 "Cannot serialize parameter value as JSON"
                 (cadr err)))))))

(ert-deftest clutch-test-execute-statements-remembers-error-details ()
  "Batch statement failures should store details for early and final errors."
  (dolist (case '((("INSERT INTO first VALUES (1)"
                    "INSERT INTO second VALUES (2)")
                   "INSERT INTO first VALUES (1)"
                   1)
                  (("INSERT INTO ok_rows VALUES (1)"
                    "INSERT INTO broken_rows VALUES (2)")
                   "INSERT INTO broken_rows VALUES (2)"
                   2)))
    (pcase-let ((`(,stmts ,broken-sql ,statement-index) case))
      (with-temp-buffer
        (let ((clutch-debug-mode t)
              (conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
              (raw-message "Connection refused (host=db.example.com, port=3306)")
              displayed)
          (clutch--clear-debug-capture)
          (setq-local clutch-connection conn)
          (cl-letf (((symbol-function 'clutch-db-backend-key)
                     (lambda (_conn) 'mysql))
                    ((symbol-function 'clutch-result--display-error)
                     (lambda (_conn sql summary message &optional _elapsed hint)
                       (setq displayed (list sql summary message hint))
                       (current-buffer)))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn sql)
                       (if (equal sql broken-sql)
                           (signal 'clutch-db-error (list raw-message))
                         (make-clutch-db-result :affected-rows 1)))))
            (let* ((display-parts (clutch--humanize-db-error-parts raw-message))
                   (result-summary (plist-get display-parts :summary))
                   (result-hint (plist-get display-parts :hint))
                   (display-summary
                    (condition-case err
                        (signal 'clutch-db-error (list raw-message))
                      (clutch-db-error
                       (clutch--humanize-db-error (error-message-string err)))))
                   (signaled (should-error (clutch--execute-statements stmts)
                                           :type 'user-error)))
              (should (eq clutch-connection conn))
              (should (equal (cadr signaled)
                             (format "Statement %d failed: %s"
                                     statement-index
                                     (clutch--debug-workflow-message
                                      display-summary))))
              (should (equal displayed
                             (list broken-sql
                                   result-summary
                                   raw-message
                                   result-hint)))
              (let* ((details clutch--buffer-error-details)
                     (diag (plist-get details :diag))
                     (debug-text (clutch-test--debug-buffer-string)))
                (should details)
                (should (eq (plist-get details :backend) 'mysql))
                (should (equal (plist-get diag :raw-message) raw-message))
                (should (equal (plist-get (plist-get diag :context) :sql)
                               broken-sql))
                (should (string-match-p "Phase: error" debug-text))
                (should (string-match-p
                         (regexp-quote display-summary) debug-text))))))))))

(ert-deftest clutch-test-execution-error-renders-result-without-message ()
  "Single-statement execution errors should not duplicate details in messages."
  (with-temp-buffer
    (insert "SELECT * FROM missing_users")
    (let ((raw-message "Table 'demo.missing_users' doesn't exist")
          err displayed messages)
      (condition-case caught
          (signal 'clutch-db-error (list raw-message))
        (clutch-db-error
         (setq err caught)))
      (cl-letf (((symbol-function 'clutch-result--display-error)
                 (lambda (_conn sql summary message &optional elapsed hint)
                   (setq displayed (list sql summary message elapsed hint))
                   (current-buffer)))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (push (apply #'format fmt args) messages))))
        (clutch--present-statement-outcome
         "SELECT * FROM missing_users" 'fake-conn
         (list :error err :elapsed 0.012 :source-buffer (current-buffer))
         (cons (point-min) (point-max)))
        (should displayed)
        (should (equal (car displayed) "SELECT * FROM missing_users"))
        (should-not messages)
        (should (overlayp clutch--executed-sql-overlay))
        (should (string-match-p
                 "Last failed SQL"
                 (overlay-get clutch--executed-sql-overlay 'help-echo)))
        (let* ((before (overlay-get clutch--executed-sql-overlay 'before-string))
               (display (get-text-property 0 'display before)))
          (if (display-graphic-p)
              (should (equal display
                             '(left-fringe clutch-executed-sql-dot
                                           clutch-failed-sql-marker-face)))
            (should (equal (car display) '(margin left-margin)))
            (should (equal (cadr display) "●"))
            (should (eq (get-text-property 0 'face (cadr display))
                        'clutch-failed-sql-marker-face))))
        (should (eq clutch--last-result-buffer (current-buffer)))))))

(ert-deftest clutch-test-display-error-result-renders-result-buffer ()
  "SQL errors should render in the result buffer without source overlays."
  (let ((source (generate-new-buffer " *clutch-error-source*"))
        shown result-buf)
    (unwind-protect
        (with-current-buffer source
          (setq-local clutch-connection 'fake-conn
                      clutch--connection-params '(:backend oracle :database "db")
                      clutch--conn-sql-product 'oracle)
          (insert "SELECT missing_col FROM dual")
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'clutch-result--show-buffer)
                     (lambda (buf) (setq shown buf) buf)))
            (setq result-buf
                  (clutch-result--display-error
                   clutch-connection
                   (buffer-string)
                   "ORA-00904: invalid column name"
                   "ORA-00904: \"MISSING_COL\": invalid identifier"
                   0.012
                   "invalid column name")))
          (should (eq shown result-buf))
          (should-not (overlays-in (point-min) (point-max)))
          (with-current-buffer result-buf
            (should (derived-mode-p 'clutch-result-mode))
            (should (equal clutch--connection-params
                           '(:backend oracle :database "db")))
            (should-not truncate-lines)
            (should word-wrap)
            (let ((text (buffer-string)))
              (should-not (string-match-p "\\`ERROR\n" text))
              (should (string-match-p "Hint: invalid column name" text))
              (should (string-match-p "invalid column name" text))
              (should (string-match-p "ORA-00904" text))
              (should-not (string-match-p "Details" text))
              (should-not (string-match-p "SELECT missing_col FROM dual" text))
              (should (string-match-p "Failed in" text)))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-execute-and-mark-skips-success-overlay-on-error ()
  "Failed execution should not mark SQL as successfully executed."
  (with-temp-buffer
    (insert "SELECT bad_col FROM dual")
    (let ((marked nil))
      (cl-letf (((symbol-function 'clutch--execute) (lambda (&rest _) nil))
                ((symbol-function 'clutch--mark-executed-sql-region)
                 (lambda (&rest _) (setq marked t))))
        (clutch--execute-and-mark (buffer-string) (point-min) (point-max))
        (should-not marked)))))

(ert-deftest clutch-test-executed-sql-overlay-marks-statement-start-line ()
  "Executed SQL should use a start-line marker instead of a body highlight."
  (with-temp-buffer
    (insert "  SELECT 1;\n  SELECT 2;")
    (clutch--mark-executed-sql-region (point-min) (point-max))
    (should (overlayp clutch--executed-sql-overlay))
    (should (= (overlay-start clutch--executed-sql-overlay) (point-min)))
    (let* ((before (overlay-get clutch--executed-sql-overlay 'before-string))
           (display (get-text-property 0 'display before)))
      (if (display-graphic-p)
          (should (equal display
                         '(left-fringe clutch-executed-sql-dot
                                       clutch-executed-sql-marker-face)))
         (should (equal (car display) '(margin left-margin)))
         (should (equal (cadr display) "●"))
         (should (eq (get-text-property 0 'face (cadr display))
                    'clutch-executed-sql-marker-face))))
    (should-not (overlay-get clutch--executed-sql-overlay 'modification-hooks))))

(ert-deftest clutch-test-execute-quit-distinguishes-confirmation-from-query ()
  "Only a quit during the database call should retire the connection."
  (with-temp-buffer
    (let ((buf (current-buffer))
          (disconnected nil)
          confirmation-quit
          phase
          refresh-started
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          (clutch-connection 'fake-conn)
          (clutch--execution-start-time nil))
      (puthash clutch-connection 'dirty clutch--tx-state-cache)
      (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch--execution-refresh-start)
                 (lambda () (setq refresh-started t)))
                ((symbol-function 'clutch--update-mode-line)
                 (lambda (&optional _execution-only) nil))
                ((symbol-function 'clutch--confirm-query-execution)
                 (lambda (_sql)
                   (when (eq phase 'confirm) (signal 'quit nil))))
                ((symbol-function 'clutch-db-result-query-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--prepare-row-identity-query)
                 (lambda (&rest _args)
                   (should refresh-started)
                   (signal 'quit nil)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query) (lambda (_conn) nil))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (setq phase 'confirm)
        (condition-case nil
            (clutch--execute "SELECT 1")
          (quit (setq confirmation-quit t)))
        (should confirmation-quit)
        (should-not disconnected)
        (should (eq clutch-connection 'fake-conn))
        (setq phase 'query)
        (let ((error (should-error
                      (clutch--execute "SELECT 1")
                      :type 'user-error)))
          (should (equal (cadr error)
                         clutch--transaction-outcome-unknown-message)))
        (should disconnected)
        (with-current-buffer buf
          (should (eq clutch-connection 'fake-conn)))
        ;; Retirement keeps the dirty flag as lost-transaction evidence;
        ;; the next transaction command consumes it and refuses to run.
        (should (gethash 'fake-conn clutch--tx-state-cache))
        (should-not clutch--execution-start-time)))))

(ert-deftest clutch-test-execute-quit-prefers-backend-interrupt-over-disconnect ()
  "Quit should keep the session when a backend interrupt succeeds."
  (with-temp-buffer
    (let* ((buf (current-buffer))
           (conn 'fake-conn)
           (interrupted nil)
           (disconnected nil)
           (clutch--tx-state-cache (make-hash-table :test 'eq))
           (clutch-connection conn)
           (clutch--execution-start-time nil))
      (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch--update-mode-line)
                 (lambda (&optional _execution-only) nil))
                ((symbol-function 'clutch--execute-statement)
                 (lambda (_sql connection &rest _args)
                   (clutch--handle-query-quit connection)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query)
                 (lambda (_conn)
                   (setq interrupted t)
                   t))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch--execute "SELECT pg_sleep(10)")
                      :type 'user-error)
        (should interrupted)
        (should-not disconnected)
        (with-current-buffer buf
          (should (eq clutch-connection conn)))
        (should-not clutch--execution-start-time)))))

(ert-deftest clutch-test-execute-db-error-preserves-dead-reconnect-anchor ()
  "Query errors should retain a dead connection for the next reconnect."
  (with-temp-buffer
    (let* ((conn 'fake-conn)
           (clutch-connection conn)
           (clutch--tx-state-cache (make-hash-table :test 'eq))
           (clutch--execution-start-time nil)
           (displayed-error nil)
           (error-context nil)
           (preserved nil)
           (details-cleared nil)
           (executions 0)
           (mode-line-updates 0))
      (puthash conn 'dirty clutch--tx-state-cache)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-sql-destructive-p)
                 (lambda (_sql) nil))
                ((symbol-function 'clutch-db-result-query-p)
                 (lambda (_conn _sql) t))
                ((symbol-function 'clutch-db-query-result-context)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--prepare-row-identity-query)
                 (lambda (&rest _args)
                   (list :sql "SELECT SLEEP(60)")))
                ((symbol-function 'clutch-db-sql-has-top-level-row-limit-p)
                 (lambda (_sql) t))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (&rest _args)
                   (setq executions (1+ executions))
                   (signal 'clutch-db-error '("query timed out"))))
                ((symbol-function 'clutch--show-execution-error)
                 (lambda (_source _conn _sql err &optional _elapsed context _region)
                   (setq displayed-error (error-message-string err)
                         error-context context)))
                ((symbol-function 'clutch--preserve-dead-connection-for-reconnect)
                 (lambda (connection)
                   (setq preserved connection)))
                ((symbol-function 'clutch-db-clear-error-details)
                 (lambda (connection)
                   (setq details-cleared connection)))
                ((symbol-function 'clutch--update-mode-line)
                 (lambda (&optional _execution-only)
                   (setq mode-line-updates (1+ mode-line-updates)))))
        (clutch--execute "SELECT SLEEP(60)")
        (should (= executions 1))
        (should (string-match-p "query timed out" displayed-error))
        (should (eq (plist-get error-context :transaction-outcome) 'unknown))
        (should (eq clutch-connection conn))
        (should (eq preserved conn))
        (should (eq details-cleared conn))
        (should-not clutch--execution-start-time)
        (should (> mode-line-updates 0))))))

(ert-deftest clutch-test-replacement-connection-keeps-the-commit-mode ()
  "A connection that replaces another should keep its commit mode.
A console in Manual mode came back in Auto mode after the automatic
reconnect, so each statement after it was committed on its own.  A new
connection that cannot take the mode is discarded, and one that has no
Manual mode is left as it is."
  (clutch-test--with-isolated-metadata-caches
    (let ((old-conn (list 'old-connection))
          (new-conn (list 'new-connection))
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          modes set discarded)
      (cl-letf (((symbol-function 'clutch--build-conn) (lambda (_params) new-conn))
                ((symbol-function 'clutch-db-manual-commit-supported-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-p)
                 (lambda (conn) (alist-get conn modes)))
                ((symbol-function 'clutch-db-set-auto-commit)
                 (lambda (conn auto-commit)
                   (push (list conn auto-commit) set)
                   (setf (alist-get conn modes) (not auto-commit))))
                ((symbol-function 'clutch--discard-unbound-connection)
                 (lambda (conn) (push conn discarded))))
        (cl-flet ((replace (old-manual new-manual)
                    (setq modes (list (cons old-conn old-manual)
                                      (cons new-conn new-manual))
                          set nil)
                    (should (eq (clutch--build-replacement-conn old-conn nil)
                                new-conn))
                    set))
          (should (equal (replace t nil) (list (list new-conn nil))))
          ;; Oracle starts in manual mode, which a session may have left.
          (should (equal (replace nil t) (list (list new-conn t))))
          (should-not (replace t t))
          (should-not (replace nil nil))
          (cl-letf (((symbol-function 'clutch-db-manual-commit-supported-p)
                     (lambda (conn) (eq conn old-conn))))
            (should-not (replace t nil)))
          (cl-letf (((symbol-function 'clutch-db-manual-commit-supported-p)
                     (lambda (conn) (eq conn new-conn))))
            (should (equal (replace t t) (list (list new-conn t))))))
        (dolist (failure '((clutch-db-error "Lost connection") (quit)))
          (setq modes (list (cons old-conn t) (cons new-conn nil))
                discarded nil)
          (cl-letf (((symbol-function 'clutch-db-set-auto-commit)
                     (lambda (_conn _auto-commit)
                       (signal (car failure) (cdr failure)))))
            (should (eq (car (condition-case err
                                 (clutch--build-replacement-conn old-conn nil)
                               ((error quit) err)))
                        (car failure))))
          (should (equal discarded (list new-conn))))
        ;; A schema switch that reconnects goes through it too.
        (setq modes (list (cons old-conn t) (cons new-conn nil))
              set nil)
        (cl-letf (((symbol-function 'clutch--connection-alive-p) #'ignore)
                  ((symbol-function 'clutch--require-live-connection) #'ignore)
                  ((symbol-function 'clutch--finalize-rebound-connection) #'ignore))
          (clutch--replace-connection old-conn nil 'postgres))
        (should (equal set (list (list new-conn nil))))))))

(ert-deftest clutch-test-dead-query-reconnects-on-next-command-without-replay ()
  "A dead query should preserve its session anchor until the next command."
  (clutch-test--with-isolated-metadata-caches
    (let* ((old-conn (list 'old-connection))
           (new-conn (list 'new-connection))
           (params '(:backend oracle :database "ORCL"))
           (source (generate-new-buffer " *clutch-reconnect-source*"))
           (attached (generate-new-buffer " *clutch-reconnect-attached*"))
           (clutch--tx-state-cache (make-hash-table :test 'eq))
           (clutch--problem-records-by-conn (make-hash-table :test 'eq))
           (clutch--schema-cache-updated-hook
            '(clutch--handle-schema-cache-updated))
           (clutch--metadata-state-changed-hook
            '(clutch--refresh-schema-status-ui))
           (old-live t)
           allow-revert
           (builds 0)
           (reverts 0)
           executions)
      (unwind-protect
          (progn
            (dolist (buffer (list source attached))
              (with-current-buffer buffer
                (setq-local clutch-connection old-conn
                            clutch--connection-params params
                            clutch--conn-sql-product 'oracle)))
            (with-current-buffer source
              (setq-local clutch--query-buffer-local-p t))
            (with-current-buffer attached
              (setq-local revert-buffer-function
                          (lambda (&rest _args)
                            (unless allow-revert
                              (ert-fail "Dead-session cleanup must not revert"))
                            (cl-incf reverts))))
            (cl-letf (((symbol-function 'clutch--connection-alive-p)
                       (lambda (connection)
                         (if (eq connection old-conn)
                             old-live
                           (eq connection new-conn))))
                      ((symbol-function 'clutch--build-conn)
                       (lambda (reconnect-params)
                         (should (equal reconnect-params params))
                         (cl-incf builds)
                         new-conn))
                      ((symbol-function 'clutch--execute-statement)
                       (lambda (sql connection _present-result-p _region k
                                    &rest _args)
                         (push (list sql connection) executions)
                         (funcall
                          k
                          (if (eq connection old-conn)
                              (progn
                                (setq old-live nil)
                                (list :error '(clutch-db-error "socket lost")
                                      :source-buffer source))
                            (list :result (make-clutch-db-result :affected-rows 1)
                                  :result-query-p nil
                                  :source-buffer source)))))
                      ((symbol-function 'clutch--show-execution-error)
                       (lambda (&rest _args) "socket lost"))
                      ((symbol-function 'clutch-result--display) #'ignore)
                      ((symbol-function 'clutch-db-clear-error-details) #'ignore)
                      ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                      ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                      ((symbol-function 'clutch--refresh-connection-render-state)
                       #'ignore)
                      ((symbol-function 'clutch--execution-refresh-start) #'ignore)
                      ((symbol-function 'clutch--update-mode-line) #'ignore)
                      ((symbol-function 'redisplay) #'ignore)
                      ((symbol-function 'clutch--connection-key)
                       (lambda (_connection) "oracle@test"))
                      ((symbol-function 'message) #'ignore))
              (with-current-buffer source
                (clutch--execute "SELECT once")
                (should (= (length executions) 1))
                (should (= builds 0))
                (should (= reverts 0))
                (should (eq clutch-connection old-conn))
                (setq allow-revert t)
                (clutch--execute "SELECT next")))
            (should (= builds 1))
            (should (= reverts 1))
            (should (equal (nreverse executions)
                           `(("SELECT once" ,old-conn)
                             ("SELECT next" ,new-conn))))
            (dolist (buffer (list source attached))
              (should (eq (buffer-local-value 'clutch-connection buffer)
                          new-conn))))
        (dolist (buffer (list source attached))
          (when (buffer-live-p buffer)
            (kill-buffer buffer)))))))

(ert-deftest clutch-test-execute-retries-only-safe-clean-preflight-failures ()
  "Retry once only when JDBC proves execution did not start and tx is clean.
A transaction begun with BEGIN in Auto mode holds uncommitted work too,
and a namespace a new connection cannot return to is not retried."
  (dolist (case '((auto nil nil 2 1 new-conn nil)
                  (manual-clean t nil 2 1 new-conn nil)
                  (manual-dirty t t 1 0 old-conn clutch-db-execution-not-started)
                  (auto-dirty nil t 1 0 old-conn clutch-db-execution-not-started)
                  (unreachable nil nil 1 0 old-conn clutch-db-execution-not-started)
                  (ambiguous-first-failure nil nil 1 0 old-conn clutch-db-error)
                  (second-failure nil nil 2 1 new-conn clutch-db-error)))
    (pcase-let ((`(,label ,manual ,dirty ,expected-runs ,expected-reconnects
                         ,expected-connection ,expected-error)
                 case))
      (with-temp-buffer
        (let ((clutch-connection 'old-conn)
              (clutch--tx-state-cache (make-hash-table :test 'eq))
              (old-live t)
              (runs 0)
              (confirmations 0)
              (reconnects 0)
              (clutch-db--foreground-connections
               (make-hash-table :test 'eq)))
          (when dirty
            (puthash 'old-conn 'dirty clutch--tx-state-cache))
          (cl-letf (((symbol-function 'clutch--confirm-query-execution)
                     (lambda (_sql) (cl-incf confirmations)))
                    ((symbol-function 'clutch-db-result-query-p)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'clutch-db-unreachable-namespace)
                     (lambda (_conn) (and (eq label 'unreachable) "att.main")))
                    ((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_conn) manual))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn)
                       (if (eq conn 'old-conn) old-live t)))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (conn _sql)
                       (should (clutch-db--foreground-busy-p conn))
                       (cl-incf runs)
                       (cond
                        ((eq conn 'old-conn)
                         (setq old-live nil)
                         (signal (if (eq label 'ambiguous-first-failure)
                                     'clutch-db-error
                                   'clutch-db-execution-not-started)
                                 '("idle validation failed")))
                        ((eq label 'second-failure)
                         (signal 'clutch-db-error '("second attempt failed")))
                        (t
                         (make-clutch-db-result :affected-rows 1)))))
                    ((symbol-function 'clutch--try-reconnect)
                     (lambda ()
                       (cl-incf reconnects)
                       (setq clutch-connection 'new-conn))))
            (let ((outcome
                   (clutch-test--await-outcome
                    (lambda (k)
                      (clutch--execute-statement
                       "SELECT side_effect_free" 'old-conn nil nil k)))))
              (ert-info ((format "case: %s" label))
                (should (= runs expected-runs))
                ;; Callers confirm once; neither attempt asks again.
                (should (= confirmations 0))
                (should (= reconnects expected-reconnects))
                (should (eq (plist-get outcome :connection)
                            expected-connection))
                (should (eq (car-safe (plist-get outcome :error))
                            expected-error))))))))))

(ert-deftest clutch-test-batch-continues-on-the-reconnected-connection ()
  "Statements after an idle reconnect in a batch should use the new connection."
  (with-temp-buffer
    (let ((clutch-connection 'old-conn)
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          (clutch-db--foreground-connections (make-hash-table :test 'eq))
          (old-live t)
          executions)
      (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-result-query-p) #'ignore)
                ((symbol-function 'clutch-db-manual-commit-p) #'ignore)
                ((symbol-function 'clutch--forget-row-identities) #'ignore)
                ((symbol-function 'clutch--note-schema-affecting-query) #'ignore)
                ((symbol-function 'clutch--execution-refresh-start) #'ignore)
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'message) #'ignore)
                ((symbol-function 'clutch--connection-key)
                 (lambda (conn) (symbol-name conn)))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn) (if (eq conn 'old-conn) old-live t)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (conn sql &rest _args)
                   (push (list sql conn) executions)
                   (if (eq conn 'old-conn)
                       (progn
                         (setq old-live nil)
                         (signal 'clutch-db-execution-not-started
                                 '("idle validation failed")))
                     (make-clutch-db-result :affected-rows 1))))
                ((symbol-function 'clutch--try-reconnect)
                 (lambda ()
                   (setq clutch-connection 'new-conn))))
        (clutch--execute-statements '("UPDATE a SET n = 1" "UPDATE b SET n = 2"))
        (should (equal (nreverse executions)
                       '(("UPDATE a SET n = 1" old-conn)
                         ("UPDATE a SET n = 1" new-conn)
                         ("UPDATE b SET n = 2" new-conn))))))))

(ert-deftest clutch-test-batch-stops-when-its-buffer-moves-during-a-synchronous-statement ()
  "A batch should stop when its buffer moves while a statement runs synchronously.
A backend that waits on the network synchronously runs timers, and one of
them can move the buffer to another connection before the reply arrives."
  (with-temp-buffer
    (let ((clutch-connection 'conn-a)
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          (clutch-db--foreground-connections (make-hash-table :test 'eq))
          (source (current-buffer))
          executions messages)
      (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-result-query-p) #'ignore)
                ((symbol-function 'clutch-db-manual-commit-p) #'ignore)
                ((symbol-function 'clutch--forget-row-identities) #'ignore)
                ((symbol-function 'clutch--note-schema-affecting-query) #'ignore)
                ((symbol-function 'clutch--execution-refresh-start) #'ignore)
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch--connection-key)
                 (lambda (conn) (symbol-name conn)))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (conn sql &rest _args)
                   (push (list sql conn) executions)
                   ;; A timer that runs while the statement waits moves the buffer.
                   (with-current-buffer source
                     (setq clutch-connection 'conn-b))
                   (make-clutch-db-result :affected-rows 1))))
        (clutch--execute-statements '("UPDATE a SET n = 1" "UPDATE b SET n = 2"))
        (should (equal executions '(("UPDATE a SET n = 1" conn-a))))
        (should (member "1 statement executed, then stopped: the connection changed"
                        messages))
        (should-not (clutch-db--foreground-busy-p 'conn-a))))))

(ert-deftest clutch-test-batch-stops-when-its-buffer-switches-connection ()
  "A batch should stop rather than follow its buffer to another connection."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let (sent messages)
        (cl-letf (((symbol-function 'clutch-db-query-async)
                   (lambda (conn sql callback)
                     (push (cons conn sql) sent)
                     (push (cons sql callback) finishes)
                     t))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute-statements '("UPDATE t SET n = 1" "UPDATE t SET n = 2"))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (setq-local clutch-connection 'conn-b)
          (ert-run-idle-timers)
          (should (equal sent '((conn-a . "UPDATE t SET n = 1"))))
          (should (member "1 statement executed, then stopped: the connection changed"
                          messages))
          (should-not (clutch-db--foreground-busy-p 'conn-a)))))))

(ert-deftest clutch-test-batch-reports-a-statement-that-failed-after-its-buffer-moved ()
  "A batch statement that fails after its buffer moved should only be reported.
The disconnect that moved the buffer closed the connection, so the statement
in flight failed with an unknown outcome; drawing that put an error page in
a result buffer of another connection, or of none.  A statement that
succeeds after its buffer moved leaves the marker of one the buffer runs."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let ((a-alive t) shown messages)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn) (if conn (symbol-name conn) "none")))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (or a-alive (not (eq conn 'conn-a)))))
                  ((symbol-function 'clutch--show-execution-error)
                   (lambda (&rest _) (setq shown t) "failed"))
                  ((symbol-function 'clutch--retire-query-connection) #'ignore)
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute-statements '("UPDATE t SET n = 1" "UPDATE t SET n = 2"))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          ;; Statement 2 runs; a disconnect then clears the buffer's connection.
          (setq a-alive nil)
          (setq-local clutch-connection nil)
          (funcall (cdar finishes) nil
                   '(clutch-db-error
                     "Disconnected while the statement ran; its outcome is unknown"))
          (ert-run-idle-timers)
          (should-not shown)
          (should (cl-some
                   (lambda (text)
                     (string-match-p
                      "\\`1 statement executed, then stopped: the connection changed; statement 2: .*outcome is unknown"
                      text))
                   messages))
          (should-not (clutch-db--foreground-busy-p 'conn-a))))))
  (with-temp-buffer
    (insert "UPDATE a SET n = 1;\nUPDATE b SET n = 2;\nSELECT 3;")
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (conn) (if conn (symbol-name conn) "none")))
                ((symbol-function 'message) #'ignore))
        (clutch--execute-statements
         (clutch--split-statement-specs (buffer-substring 1 41) 1))
        (setq-local clutch-connection 'conn-b)
        (clutch--execute-and-mark "SELECT 3;" 41 (point-max))
        ;; conn-a's first statement succeeds while SELECT 3 runs on conn-b.
        (funcall (cdr (car (last finishes)))
                 (make-clutch-db-result :affected-rows 1) nil)
        (ert-run-idle-timers)
        (should (string-prefix-p
                 "Running"
                 (overlay-get clutch--executed-sql-overlay 'help-echo)))))))

(ert-deftest clutch-test-idle-retry-runs-only-on-its-reconnected-connection ()
  "An idle retry should run only on the connection its reconnect built.
There it plans row identity anew rather than reuse the old connection's
plan.  After a reply that came once the statement started, a reconnect or
a retry that fails before it runs ends the statement with its error.  A
buffer that the reconnect's wait killed or moved elsewhere is not retried
on, and a statement it started elsewhere keeps its marker."
  (with-temp-buffer
    (let ((clutch-connection 'old-conn)
          (old-live t)
          executions prepared-connections)
      (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
                ((symbol-function 'clutch-db-result-query-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch-db-query-result-context)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--prepare-row-identity-query)
                 (lambda (connection _sql)
                   (push connection prepared-connections)
                   '(:sql "fresh-plan")))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (connection)
                   (if (eq connection 'old-conn) old-live t)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (connection sql)
                   (push (list connection sql) executions)
                   (if (eq connection 'old-conn)
                       (progn
                         (setq old-live nil)
                         (signal 'clutch-db-execution-not-started
                                 '("idle validation failed")))
                     (make-clutch-db-result :columns ["id"] :rows '((1))))))
                ((symbol-function 'clutch--try-reconnect)
                 (lambda ()
                   (setq clutch-connection 'new-conn))))
        (let ((outcome
               (clutch-test--await-outcome
                (lambda (k)
                  (clutch--execute-statement
                   "SELECT * FROM items" 'old-conn t nil k
                   '(:row-identity-prep (:sql "stale-plan")
                     :server-pageable nil))))))
          (should (eq (plist-get outcome :connection) 'new-conn))
          (should (equal (nreverse executions)
                         '((old-conn "stale-plan")
                           (new-conn "fresh-plan"))))
          (should (equal prepared-connections '(new-conn)))))))
  (dolist (failing '(reconnect retry))
    (with-temp-buffer
      (insert "SELECT 1")
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (let ((alive t) shown)
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (or alive (not (eq conn 'async-conn)))))
                    ((symbol-function 'clutch--try-reconnect)
                     (lambda ()
                       (when (eq failing 'reconnect)
                         (signal 'clutch-db-error '("Connection refused")))
                       (setq-local clutch-connection 'new-conn)))
                    ((symbol-function 'clutch-db-query-async)
                     (lambda (conn sql callback)
                       (when (eq conn 'new-conn)
                         (error "Dispatch failed"))
                       (push (cons sql callback) finishes)
                       t))
                    ((symbol-function 'clutch-result--display-error)
                     (lambda (&rest args) (setq shown args) nil)))
            (clutch--execute-and-mark (buffer-string) (point-min) (point-max))
            (setq alive nil)
            (funcall (cdar finishes) nil
                     '(clutch-db-execution-not-started "connection invalidated"))
            ;; The reply is handled in a timer, outside any command.
            (let ((debug-on-error nil))
              (ert-run-idle-timers))
            (should-not (clutch-db--foreground-busy-p 'async-conn))
            (should-not (clutch-db--foreground-busy-p 'new-conn))
            (should-not clutch--execution-start-time)
            (should (string-prefix-p
                     "Last failed"
                     (overlay-get clutch--executed-sql-overlay 'help-echo)))
            (should (string-match-p (if (eq failing 'reconnect)
                                        "Connection refused"
                                      "Dispatch failed")
                                    (format "%S" shown))))))))
  (pcase-dolist (`(,reconnects ,starts) '((nil t) (t t) (t nil)))
    (with-temp-buffer
      (insert "SELECT 1;\nSELECT 2;")
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (let ((source (current-buffer)) (alive t) shown)
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (or alive (not (eq conn 'async-conn)))))
                    ((symbol-function 'clutch--connection-key) #'symbol-name)
                    ((symbol-function 'clutch--try-reconnect)
                     ;; The reconnect's wait runs a timer that moves the
                     ;; buffer to conn-b, and maybe starts SELECT 2 there.
                     (lambda ()
                       (let (moved)
                         (run-with-timer
                          0 nil (lambda ()
                                  (with-current-buffer source
                                    (setq-local clutch-connection 'conn-b)
                                    (when starts
                                      (clutch--execute-and-mark "SELECT 2;" 11 20)))
                                  (setq moved t)))
                         (while (not moved)
                           (accept-process-output nil 0.01)))
                       (if reconnects
                           'new-conn
                         (signal 'clutch-db-error '("Reconnect failed")))))
                    ((symbol-function 'clutch-result--display-error)
                     (lambda (&rest args) (setq shown args) nil)))
            (clutch--execute-and-mark "SELECT 1;" 1 10)
            (setq alive nil)
            (funcall (cdar finishes) nil
                     '(clutch-db-execution-not-started "connection invalidated"))
            (let ((debug-on-error nil))
              (ert-run-idle-timers))
            (should-not shown)
            (should (= (length finishes) (if starts 2 1)))
            (should-not (clutch-db--foreground-busy-p 'async-conn))
            (should (string-prefix-p
                     (if starts "Running" "Last failed")
                     (overlay-get clutch--executed-sql-overlay 'help-echo))))))))
  (let ((source (generate-new-buffer " *clutch-retry-killed*")))
    (with-current-buffer source
      (insert "SELECT 1;")
      (setq-local clutch-connection 'async-conn))
    (clutch-test--with-async-statements finishes
      (let ((alive t))
        (cl-letf (((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (or alive (not (eq conn 'async-conn)))))
                  ((symbol-function 'clutch--try-reconnect)
                   ;; The reconnect's wait runs a timer that kills the buffer.
                   (lambda ()
                     (run-with-timer 0 nil #'kill-buffer source)
                     (while (buffer-live-p source)
                       (accept-process-output nil 0.01))
                     t)))
          (with-current-buffer source
            (clutch--execute-and-mark "SELECT 1;" 1 10))
          (setq alive nil)
          (funcall (cdar finishes) nil
                   '(clutch-db-execution-not-started "connection invalidated"))
          (let ((debug-on-error nil))
            (ert-run-idle-timers))
          (should (= (length finishes) 1))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(defmacro clutch-test--with-async-statements (finishes-var &rest body)
  "Run BODY with statements finishing only when callbacks in FINISHES-VAR run.
Each started statement pushes (SQL . CALLBACK) onto FINISHES-VAR."
  (declare (indent 1) (debug (symbolp body)))
  `(let ((clutch-db--foreground-connections (make-hash-table :test 'eq))
         (clutch--running-queries (make-hash-table :test 'eq))
         (,finishes-var nil))
     (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
               ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
               ((symbol-function 'clutch--update-mode-line) #'ignore)
               ((symbol-function 'clutch--execution-refresh-start) #'ignore)
               ((symbol-function 'clutch--confirm-query-execution) #'ignore)
               ((symbol-function 'clutch-db-result-query-p) #'ignore)
               ((symbol-function 'clutch-db-manual-commit-p) #'ignore)
               ((symbol-function 'clutch--tx-uncertain-p) #'ignore)
               ((symbol-function 'clutch--record-tx-state-after-query) #'ignore)
               ((symbol-function 'clutch--note-schema-affecting-query) #'ignore)
               ((symbol-function 'clutch-db-query-async)
                (lambda (_conn sql callback)
                  (push (cons sql callback) ,finishes-var)
                  t)))
       ,@body)))

(ert-deftest clutch-test-result-context-interruption-keeps-the-successful-reply ()
  "A failed context lookup must not lose a successful asynchronous query.
An interrupted lookup ran before the activity guard, leaving its timer
and foreground reservation behind instead of presenting the result."
  (dolist (failure '(clutch-db-error quit))
    (with-temp-buffer
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (let (presented)
          (cl-letf (((symbol-function 'clutch-db-resolution-context)
                     (lambda (_conn) (signal failure '("Context unavailable"))))
                    ((symbol-function 'clutch--present-statement-outcome)
                     (lambda (_sql _conn outcome &rest _)
                       (setq presented outcome))))
            (clutch--execute "SELECT 1")
            (funcall (cdar finishes)
                     (make-clutch-db-result :columns '((:name "value"))
                                            :rows '((1))) nil)
            (condition-case nil (ert-run-idle-timers) (quit nil))
            (should presented)
            (should (equal (clutch-db-result-rows (plist-get presented :result))
                           '((1))))
            (should (eq (plist-get presented :resolution-context) 'unknown))
            (should-not clutch--execution-start-time)
            (should (zerop (hash-table-count clutch-db--foreground-connections)))
            (should (zerop (hash-table-count clutch--running-queries)))))))))

(ert-deftest clutch-test-quit-in-the-command-loop-cancels-the-running-query ()
  "A quit that reaches the command loop comes back as \\`C-g'.
On MS-Windows a \\`C-g' typed while Emacs is busy quits whatever runs
next, such as redisplay, instead of being read as a key.  While the
buffer's query runs, the quit is delivered as the key again, whose
command cancels the query once.  Another error, a buffer without a
running query, a buffer where \\`C-g' runs another command and a query
already being cancelled get no key."
  (should (advice-function-member-p #'clutch--cancel-running-query-on-quit
                                    (default-value 'command-error-function)))
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements _finishes
      ;; Only this handler runs, not others on the global value, such as
      ;; Transient's while a menu is open.
      (let ((command-error-function #'ignore)
            unread-command-events interrupts)
        (add-function :after command-error-function
                      #'clutch--cancel-running-query-on-quit)
        (cl-letf (((symbol-function 'clutch-db-interrupt-query)
                   (lambda (conn) (push conn interrupts) t)))
          (clutch--run-db-query-async 'async-conn "SELECT 1" nil #'ignore)
          (funcall command-error-function '(error "Other") "" nil)
          (with-temp-buffer
            (funcall command-error-function '(quit) "" nil))
          (with-temp-buffer
            (setq-local clutch-connection 'other-conn)
            (clutch--run-db-query-async 'other-conn "SELECT 2" nil #'ignore)
            (funcall command-error-function '(quit) "" nil))
          (should-not unread-command-events)
          (funcall command-error-function '(quit) "" nil)
          (should (equal unread-command-events '(?\C-g)))
          ;; The command loop reads it as the key.
          (call-interactively
           (key-binding (vector (pop unread-command-events))))
          (should (equal interrupts '(async-conn)))
          (funcall command-error-function '(quit) "" nil)
          (should-not unread-command-events))))))

(ert-deftest clutch-test-indirect-execute-runs-in-a-buffer-holding-its-connection ()
  "SQL from an indirect edit should run in a buffer that holds its connection.
Closing the edit shows a buffer of another kind, such as source code, which
holds no connection for the statement to belong to."
  (let ((console (generate-new-buffer " *clutch-test-console*"))
        (code (generate-new-buffer " *clutch-test-code*"))
        (indirect (generate-new-buffer " *clutch-test-indirect*"))
        ran-in)
    (unwind-protect
        (progn
          (with-current-buffer console
            (setq-local clutch-connection 'indirect-conn))
          (with-current-buffer indirect
            (clutch--indirect-mode 1)
            (setq-local clutch-connection 'indirect-conn)
            (insert "SELECT 1"))
          (switch-to-buffer code)
          (switch-to-buffer indirect)
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch--execute)
                     (lambda (sql &rest _)
                       (setq ran-in (list (current-buffer) sql)))))
            (with-current-buffer indirect
              (clutch-indirect-execute)))
          (should-not (buffer-live-p indirect))
          (should (equal ran-in (list console "SELECT 1"))))
      (dolist (buffer (list console code indirect))
        (when (buffer-live-p buffer)
          (kill-buffer buffer))))))

(ert-deftest clutch-test-indirect-execute-runs-in-the-edit-holding-its-connection-alone ()
  "SQL from an indirect edit that alone holds its connection should run there.
Connecting in the edit leaves no other buffer to run the SQL in.  The edit is
buried instead of killed, since its statement's reply needs it."
  (let ((code (generate-new-buffer " *clutch-test-code*"))
        (indirect (generate-new-buffer " *clutch-test-indirect*"))
        ran-in)
    (unwind-protect
        (progn
          (with-current-buffer indirect
            (setq-local clutch-connection 'edit-conn)
            (insert "SELECT 41"))
          (switch-to-buffer code)
          (switch-to-buffer indirect)
          (cl-letf (((symbol-function 'clutch--execute)
                     (lambda (sql &rest _)
                       (setq ran-in (list (current-buffer) sql)))))
            (with-current-buffer indirect
              (clutch-indirect-execute)))
          (should (buffer-live-p indirect))
          (should (equal ran-in (list indirect "SELECT 41"))))
      (dolist (buffer (list code indirect))
        (when (buffer-live-p buffer)
          (kill-buffer buffer))))))

(ert-deftest clutch-test-indirect-execute-runs-in-a-session-of-its-own ()
  "SQL from an indirect edit should run in a session the edit connected.
The edit ran it in any other buffer that held the connection, such as the
edit's own result buffer, and killed the edit.  Once that session is
gone, the edit runs SQL where a connection is, as it did before."
  (let ((console (generate-new-buffer " *clutch-test-console*"))
        (result (generate-new-buffer " *clutch-test-result*"))
        (code (generate-new-buffer " *clutch-test-code*"))
        (indirect (generate-new-buffer " *clutch-test-indirect*"))
        ran-in)
    (unwind-protect
        (progn
          (with-current-buffer console
            (setq-local clutch-connection 'console-conn))
          (with-current-buffer result
            (setq-local clutch-connection 'own-conn))
          (with-current-buffer indirect
            (clutch--indirect-mode 1)
            (setq-local clutch-connection 'own-conn
                        clutch--connected-here t)
            (insert "SELECT 1"))
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch--find-connection)
                     (lambda () 'console-conn))
                    ((symbol-function 'clutch--execute)
                     (lambda (sql &rest _)
                       (setq ran-in (list (current-buffer) sql)))))
            (switch-to-buffer code)
            (switch-to-buffer indirect)
            (clutch-indirect-execute)
            (should (buffer-live-p indirect))
            (should (equal ran-in (list indirect "SELECT 1")))
            (with-current-buffer indirect
              (setq-local clutch-connection nil))
            (with-current-buffer result
              (setq-local clutch-connection nil))
            (switch-to-buffer indirect)
            (clutch-indirect-execute)
            (should-not (buffer-live-p indirect))
            (should (equal ran-in (list console "SELECT 1")))))
      (dolist (buffer (list console result code indirect))
        (when (buffer-live-p buffer)
          (kill-buffer buffer))))))

(ert-deftest clutch-test-edit-indirect-holds-the-params-of-its-connection ()
  "An indirect edit opened outside Clutch should hold its connection's params.
It held a console's connection but none of its parameters, so once the
console followed a namespace switch it held only that namespace, and
reconnecting from it failed with \"Connection params require :backend\"."
  (let ((console (generate-new-buffer " *clutch-test-console*"))
        (params '(:backend mysql :host "db" :database "app"))
        indirect)
    (unwind-protect
        (progn
          (with-current-buffer console
            (setq-local clutch-connection 'live-conn
                        clutch--connection-params params
                        clutch--conn-sql-product 'mysql
                        clutch--session-target 'console-target))
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (eq conn 'live-conn)))
                    ((symbol-function 'clutch--update-mode-line) #'ignore)
                    ((symbol-function 'message) #'ignore))
            (with-temp-buffer
              (insert "SELECT 1")
              (clutch-edit-indirect)
              (setq indirect (current-buffer))))
          (with-current-buffer indirect
            (should (eq clutch-connection 'live-conn))
            (should (equal clutch--connection-params params))
            (should (eq clutch--conn-sql-product 'mysql))
            (should (eq clutch--session-target 'console-target))))
      (dolist (buffer (list console indirect))
        (when (buffer-live-p buffer)
          (kill-buffer buffer))))))

(ert-deftest clutch-test-statement-reply-after-its-buffer-moved-is-only-reported ()
  "A statement's reply after its buffer left its connection should only be reported.
Drawing it put the old connection's error page in the result buffer that the
buffer's new connection names, and bound that buffer to the old connection.
Nor does it mark its statement over one the buffer now runs."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let ((a-alive t) rendered messages)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn) (if conn (symbol-name conn) "none")))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (or a-alive (not (eq conn 'conn-a)))))
                  ((symbol-function 'clutch--retire-query-connection) #'ignore)
                  ((symbol-function 'clutch-result--display-error)
                   (lambda (&rest _) (setq rendered t) nil))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute "UPDATE t SET n = 1 WHERE id = 1")
          ;; Disconnected and connected elsewhere while the reply waits.
          (setq a-alive nil)
          (setq-local clutch-connection 'conn-b)
          (funcall (cdar finishes) nil
                   '(clutch-db-error
                     "Disconnected while the statement ran; its outcome is unknown"))
          (ert-run-idle-timers)
          (should-not rendered)
          (should (cl-some (lambda (text) (string-match-p "outcome is unknown" text))
                           messages))
          (should-not (clutch-db--foreground-busy-p 'conn-a))))))
  (with-temp-buffer
    (insert "SELECT 1;\nSELECT 2;")
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (conn) (if conn (symbol-name conn) "none")))
                ((symbol-function 'message) #'ignore))
        (clutch--execute-and-mark "SELECT 1;" 1 10)
        (setq-local clutch-connection 'conn-b)
        (clutch--execute-and-mark "SELECT 2;" 11 20)
        ;; conn-a answers while the buffer runs SELECT 2 on conn-b.
        (funcall (cdr (car (last finishes)))
                 (make-clutch-db-result :affected-rows 1) nil)
        (ert-run-idle-timers)
        (should (string-prefix-p
                 "Running"
                 (overlay-get clutch--executed-sql-overlay 'help-echo)))
        (should (equal (buffer-substring-no-properties
                        (overlay-start clutch--executed-sql-overlay)
                        (overlay-end clutch--executed-sql-overlay))
                       "SELECT 2;"))))))

(ert-deftest clutch-test-redis-select-moves-the-session-to-its-database ()
  "A Redis SELECT should move the session to the database it selects.
A reconnect selected the database the connection was opened with, and the
cached keys and their metadata stayed that database's."
  (require 'redis)
  (require 'clutch-redis)
  (with-temp-buffer
    ;; The client reports the database that the SELECT moves it to, as
    ;; redis.el does once the server accepts it.
    (let ((conn (make-clutch-redis-conn :client (make-redis-conn :database "1")))
          primed)
      (setq-local clutch-connection conn)
      (setq-local clutch--connection-params '(:backend redis :database 0))
      (clutch-test--with-async-statements finishes
        (let ((clutch--schema-cache (make-hash-table :test 'eq)))
          (puthash conn 'keys-of-database-0 clutch--schema-cache)
          (cl-letf (((symbol-function 'clutch-result--display-select) #'ignore)
                    ((symbol-function 'clutch--prime-schema-cache)
                     (lambda (connection) (setq primed connection))))
            (clutch--execute "SELECT 1")
            (funcall (cdar finishes)
                     (make-clutch-db-result :columns '((:name "value"))
                                            :rows '(("OK")))
                     nil)
            (ert-run-idle-timers)
            (should (equal (plist-get clutch--connection-params :database) "1"))
            (should-not (gethash conn clutch--schema-cache))
            (should (eq primed conn))))))))

(ert-deftest clutch-test-namespace-follow-up-keeps-metadata-when-nothing-moved ()
  "Following a possible switch should replace metadata only when it moved.
A PostgreSQL COMMIT or ROLLBACK is followed in case it undid a search_path,
and replacing the metadata after every one reloaded the schema for nothing,
also after the first one on a console whose parameters had no path yet.
The parameters take the namespace either way."
  (with-temp-buffer
    (let ((namespace "public") (cleared 0) (primed 0))
      (setq-local clutch-connection 'ns-conn)
      (setq-local clutch--connection-params '(:backend pg))
      (cl-letf (((symbol-function 'clutch-db-update-namespace-params)
                 (lambda (_conn params)
                   (plist-put (copy-sequence params) :search-path namespace)))
                ((symbol-function 'clutch-db-current-schema)
                 (lambda (_conn) namespace))
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn) (cl-incf cleared)))
                ((symbol-function 'clutch--prime-schema-cache)
                 (lambda (_conn) (cl-incf primed))))
        (clutch--note-namespace-switch 'ns-conn '("public" nil))
        (should (equal (plist-get clutch--connection-params :search-path)
                       "public"))
        (should (= cleared 0))
        (should (= primed 0))
        (setq namespace "alt")
        (clutch--note-namespace-switch 'ns-conn '("public" nil))
        (should (equal (plist-get clutch--connection-params :search-path) "alt"))
        (should (= cleared 1))
        (should (= primed 1))
        ;; A connection lost right after its statement cannot answer.
        (cl-letf (((symbol-function 'clutch-db-update-namespace-params)
                   (lambda (_conn _params)
                     (signal 'clutch-db-error '("connection lost"))))
                  ((symbol-function 'message) #'ignore))
          (clutch--note-namespace-switch 'ns-conn '("alt" nil)))
        (should (equal (plist-get clutch--connection-params :search-path)
                       "alt"))))))

(ert-deftest clutch-test-namespace-follow-up-replaces-metadata-of-another-catalog ()
  "Following a possible switch should replace metadata read in another catalog.
A DuckDB USE of another database can keep a schema named main, and
comparing the schema alone kept the cached tables of the database left."
  (with-temp-buffer
    (let ((scope '((catalog . "home") (schema . "main"))) (cleared 0) (primed 0))
      (setq-local clutch-connection 'ns-conn)
      (setq-local clutch--connection-params '(:backend jdbc))
      (setq-local clutch--connection-render-state '(:namespace "main"))
      (cl-letf (((symbol-function 'clutch-db-update-namespace-params)
                 (lambda (_conn params) params))
                ((symbol-function 'clutch-db-current-schema)
                 (lambda (_conn) "main"))
                ((symbol-function 'clutch-db-metadata-scope)
                 (lambda (_conn) scope))
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn) (cl-incf cleared)))
                ((symbol-function 'clutch--prime-schema-cache)
                 (lambda (_conn) (cl-incf primed))))
        (let ((before (clutch--namespace-before 'ns-conn)))
          (clutch--note-namespace-switch 'ns-conn before)
          (should (= cleared 0))
          (should (= primed 0))
          (setq scope '((catalog . "att") (schema . "main")))
          (clutch--note-namespace-switch 'ns-conn before)
          (should (= cleared 1))
          (should (= primed 1)))))))

(ert-deftest clutch-test-transaction-commands-follow-the-namespace-back ()
  "Ending a transaction from a command should follow the namespace back.
PostgreSQL undoes a search_path set inside a transaction that rolls back,
but the console stayed on it after `clutch-rollback', and after a commit or
a switch to auto-commit that ended a SET LOCAL.  The follow-up runs once
the transaction has ended, against the namespace shown before."
  (with-temp-buffer
    (setq-local clutch-connection 'tx-conn)
    (setq-local clutch--connection-render-state '(:namespace "alt"))
    (let (events)
      (cl-letf (((symbol-function 'clutch--ensure-transaction-connection) #'ignore)
                ((symbol-function 'clutch-db-manual-commit-supported-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
                ((symbol-function 'clutch--tx-uncertain-p) #'ignore)
                ((symbol-function 'clutch--tx-unresolved-p) #'ignore)
                ((symbol-function 'clutch-db-rollback)
                 (lambda (_conn) (push 'rollback events)))
                ((symbol-function 'clutch-db-commit)
                 (lambda (_conn) (push 'commit events) nil))
                ((symbol-function 'clutch-db-set-auto-commit)
                 (lambda (_conn _auto) (push 'auto-commit events)))
                ((symbol-function 'clutch--mark-dml-results-rolled-back) #'ignore)
                ((symbol-function 'clutch--mark-dml-results-committed) #'ignore)
                ((symbol-function 'clutch--clear-tx-state) #'ignore)
                ((symbol-function 'message) #'ignore)
                ((symbol-function 'clutch-db-namespace-switch-p)
                 (lambda (_conn sql) (push sql events) t))
                ((symbol-function 'clutch--note-namespace-switch)
                 (lambda (_conn before) (push (list 'follow before) events))))
        (clutch-rollback)
        (clutch-commit)
        (clutch-toggle-auto-commit)
        (should (equal (nreverse events)
                       '(rollback "ROLLBACK" (follow ("alt" nil))
                         commit "COMMIT" (follow ("alt" nil))
                         auto-commit "COMMIT" (follow ("alt" nil)))))))))

(ert-deftest clutch-test-repl-reply-after-the-repl-moved-draws-no-result ()
  "A REPL statement's reply after the REPL left its connection draws no result.
Showing the SELECT put the old connection's rows in the result buffer that
the REPL's new connection names, bound to the old connection."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let (displayed output)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn) (if conn (symbol-name conn) "none")))
                  ((symbol-function 'clutch-result--display-select)
                   (lambda (&rest _) (setq displayed t)))
                  ((symbol-function 'clutch-repl--output)
                   (lambda (text) (push text output)))
                  ((symbol-function 'message) #'ignore))
          (clutch-repl--execute-and-print "SELECT 1")
          (setq-local clutch-connection 'conn-b)
          (funcall (cdar finishes)
                   (make-clutch-db-result :columns '((:name "1")) :rows '((1)))
                   nil)
          (ert-run-idle-timers)
          (should-not displayed)
          (should (string-match-p "not shown" (car output))))))))

(ert-deftest clutch-test-async-execute-presents-after-completion ()
  "An asynchronous statement should hold its connection until it finishes.
Its marker shows that it ran even when presenting its result fails."
  (with-temp-buffer
    (insert "UPDATE t SET n = 1 WHERE id = 1")
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (let (displayed)
        (cl-letf (((symbol-function 'clutch-result--display)
                   (lambda (result _sql _elapsed) (setq displayed result))))
          (clutch--execute-and-mark (buffer-string) (point-min) (point-max))
          (should (equal (mapcar #'car finishes) (list (buffer-string))))
          (should (clutch-db--foreground-busy-p 'async-conn))
          (should (string-prefix-p
                   "Running"
                   (overlay-get clutch--executed-sql-overlay 'help-echo)))
          (should (eq (overlay-get clutch--executed-sql-overlay 'face)
                      'clutch-running-sql-face))
          (should (= (overlay-start clutch--executed-sql-overlay) (point-min)))
          (should (= (overlay-end clutch--executed-sql-overlay) (point-max)))
          (should-error (clutch--execute "SELECT 2") :type 'user-error)
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (should-not displayed)
          (ert-run-idle-timers)
          (should (= (clutch-db-result-affected-rows displayed) 1))
          (should-not (gethash 'async-conn clutch--running-queries))
          (should-not (clutch-db--foreground-busy-p 'async-conn))
          (should-not clutch--execution-start-time)
          (should (string-prefix-p
                   "Last executed"
                   (overlay-get clutch--executed-sql-overlay 'help-echo)))
          (should-not (overlay-get clutch--executed-sql-overlay 'face))
          (should (= (overlay-start clutch--executed-sql-overlay)
                     (overlay-end clutch--executed-sql-overlay)))))))
  (with-temp-buffer
    (insert "UPDATE t SET n = 1 WHERE id = 1")
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch-result--display)
                 (lambda (&rest _) (error "Presenting failed"))))
        (clutch--execute-and-mark (buffer-string) (point-min) (point-max))
        (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
        ;; The error then ends the timer, as it would outside a test.
        (let ((debug-on-error nil))
          (ert-run-idle-timers))
        (should (string-prefix-p
                 "Last executed"
                 (overlay-get clutch--executed-sql-overlay 'help-echo)))))))

(ert-deftest clutch-test-page-load-keeps-result-until-it-succeeds ()
  "A page load should leave the page, its sort and staging until it succeeds.
It runs without blocking, staging waits for it, and a failure leaves the
result as it was.  So does \[clutch-cancel-query-or-quit], also when the
backend refuses the cancel or fails to send it and the page then arrives."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows '((1 "a") (2 "b"))
       :connection 'async-conn
       :base-query "SELECT id, name FROM t"
       :server-pageable t
       :server-rewritable t)
    (clutch-test--with-async-statements finishes
      (let (messages)
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-sql-surface-p) (lambda (&rest _) t))
                  ((symbol-function 'clutch-db-build-paged-sql)
                   (lambda (&rest _) "SELECT id, name FROM t ORDER BY name DESC"))
                  ((symbol-function 'clutch--refresh-display) #'ignore)
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch-result--sort "name" t)
          (should (= (length finishes) 1))
          (should-not clutch--sort-column)
          (should-not clutch--order-by)
          (should (equal clutch--result-rows '((1 "a") (2 "b"))))
          (should (string-match-p
                   "A query is running"
                   (error-message-string
                    (should-error (clutch-edit--require-sql-staged-mutation
                                   "Edit / re-edit")
                                  :type 'user-error))))
          ;; An edit opened before the page load cannot be staged either.
          (should (string-match-p
                   "A query is running"
                   (error-message-string
                    (should-error (clutch-result--apply-edit
                                   0 1 "z" (list :identity [1] :original "a"
                                                 :original-state (cons nil "a")))
                                  :type 'user-error))))
          (funcall (cdar finishes) nil '(clutch-db-error "relation does not exist"))
          (ert-run-idle-timers)
          (should-not clutch--sort-column)
          (should-not clutch--order-by)
          (should (equal clutch--result-rows '((1 "a") (2 "b"))))
          (should (string-suffix-p "(result unchanged)" (car messages)))
          (clutch-result--sort "name" t)
          (funcall (cdar finishes)
                   (make-clutch-db-result :columns clutch--result-column-defs
                                          :rows '((2 "b") (1 "a")))
                   nil)
          (ert-run-idle-timers)
          (should (equal clutch--sort-column "name"))
          (should (equal clutch--order-by '("name" . "DESC")))
          (should (equal clutch--result-rows '((2 "b") (1 "a"))))
          (should (equal (car messages) "Sorted by name DESC"))
          (dolist (interrupt (list #'ignore
                                   (lambda (_conn)
                                     (signal 'clutch-db-error '("cancel failed")))))
            (cl-letf (((symbol-function 'clutch-db-interrupt-query) interrupt))
              (clutch-result--sort "name" nil)
              (clutch-cancel-query-or-quit)
              (funcall (cdar finishes)
                       (make-clutch-db-result :columns clutch--result-column-defs
                                              :rows '((1 "a") (2 "b")))
                       nil)
              (ert-run-idle-timers)
              (should (equal clutch--order-by '("name" . "DESC")))
              (should (equal clutch--result-rows '((2 "b") (1 "a"))))
              (should (string-suffix-p "(result unchanged)" (car messages))))))))))

(ert-deftest clutch-test-page-load-leaves-a-result-of-another-connection ()
  "A page that arrives once its buffer shows another connection's result is dropped.
Showing it there would pair one database's rows with another's connection,
and an edit would then change the other database by the first one's keys.
A failure is dropped too: its error page would replace that result and bind
the buffer to the closed connection."
  (dolist (reply (list (list (make-clutch-db-result :rows '((3) (4))) nil)
                       (list nil '(clutch-db-error "connection closed"))))
    (clutch-test--with-result-state
        (:columns '("id") :rows '((1) (2)) :connection 'async-conn
         :base-query "SELECT id FROM t" :server-pageable t :result-max-rows 2)
      (clutch-test--with-async-statements finishes
        (let (shown)
          (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                    ((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (&rest _) "SELECT id FROM t PAGE 1"))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (not (eq conn 'async-conn))))
                    ((symbol-function 'clutch--show-execution-error)
                     (lambda (&rest _) (setq shown t) "failed"))
                    ((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'message) #'ignore))
            (clutch-result--execute-page 1)
            (apply (cdar finishes) reply)
            ;; Before the reply is handled, the buffer shows another result.
            (setq-local clutch-connection 'other-conn
                        clutch--result-rows '((10) (20)))
            (ert-run-idle-timers)
            (should-not shown)
            (should (equal clutch--result-rows '((10) (20))))
            (should (eq clutch-connection 'other-conn))))))))

(ert-deftest clutch-test-query-activity-reply-needs-its-buffer-on-its-connection ()
  "A reply should reach its handler only while its buffer holds its connection.
That is the connection the reply came from, which an idle reconnect puts in
the buffer in place of the reserved one.  A buffer that holds another
connection, or none, ends the activity and calls MOVED; a killed buffer ends
it and says so; a handler that exits nonlocally ends it too."
  (let ((clutch-db--foreground-connections (make-hash-table :test 'eq))
        messages)
    (cl-letf (((symbol-function 'clutch--update-mode-line) #'ignore)
              ((symbol-function 'clutch--execution-refresh-start) #'ignore)
              ((symbol-function 'message)
               (lambda (format-string &rest args)
                 (push (apply #'format format-string args) messages))))
      (cl-flet ((reply (buffer-holds reply-from &optional kill handle)
                  (let ((buffer (generate-new-buffer " *clutch-reply*"))
                        activity events)
                    (with-current-buffer buffer
                      (setq-local clutch-connection 'old-conn)
                      (setq activity (clutch--begin-query-activity 'old-conn))
                      (setq-local clutch-connection buffer-holds))
                    (when kill
                      (kill-buffer buffer))
                    (condition-case nil
                        (clutch--query-activity-reply
                         activity reply-from
                         (or handle (lambda () (push 'handled events)))
                         :moved (lambda () (push 'moved events)))
                      (error (push 'signalled events)))
                    (when (buffer-live-p buffer)
                      (kill-buffer buffer))
                    (list (nreverse events)
                          (and (plist-get activity :ended) t)))))
        (should (equal (reply 'old-conn 'old-conn) '((handled) nil)))
        (should (equal (reply 'new-conn 'new-conn) '((handled) nil)))
        (should (equal (reply 'other-conn 'old-conn) '((moved) t)))
        (should (equal (reply nil 'old-conn) '((moved) t)))
        (should (equal (reply 'old-conn 'old-conn t) '(nil t)))
        (should (member "Query finished after its buffer was killed" messages))
        (should (equal (reply 'old-conn 'old-conn nil
                              (lambda () (error "Boom")))
                       '((signalled) t)))))))

(ert-deftest clutch-test-query-activity-end-keeps-a-later-ones-time ()
  "Ending a query activity should leave the time of a later one in its buffer.
A page load that ends after its buffer moved to another connection and
started loading a page there cleared the time of that running load."
  (clutch-test--with-result-state
      (:columns '("id") :rows '((1) (2)) :connection 'async-conn
       :base-query "SELECT id FROM t" :server-pageable t :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (&rest _) "SELECT id FROM t PAGE 1"))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result--execute-page 1)
        (funcall (cdar finishes) nil '(clutch-db-error "connection closed"))
        (setq-local clutch-connection 'other-conn)
        (clutch-result--execute-page 1)
        (ert-run-idle-timers)
        (should (gethash 'other-conn clutch--running-queries))
        (should clutch--execution-start-time)
        (funcall (cdar finishes)
                 (make-clutch-db-result :columns clutch--result-column-defs
                                        :rows '((3) (4)))
                 nil)
        (ert-run-idle-timers)
        (should-not clutch--execution-start-time)))))

(ert-deftest clutch-test-last-page-stops-when-its-count-is-cancelled ()
  "C-g during the count of a last-page move should stop the page load.
The cancel may meet a count that has already finished; the move stops
there, and the result keeps its rows and its unknown total."
  (clutch-test--with-result-state
      (:columns '("id") :rows '((1) (2)) :connection 'async-conn
       :base-query "SELECT id FROM t" :server-pageable t :server-rewritable t
       :page-total-rows nil :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (&rest _) "SELECT count(*) FROM t"))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql page-num &rest _)
                   (format "SELECT id FROM t PAGE %d" page-num)))
                ((symbol-function 'clutch-db-interrupt-query) (lambda (_conn) t))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-last-page)
        (clutch-cancel-query-or-quit)
        (funcall (cdar finishes) (make-clutch-db-result :rows '((5))) nil)
        (ert-run-idle-timers)
        (should (equal (mapcar #'car finishes) '("SELECT count(*) FROM t")))
        (should-not clutch--page-total-rows)
        (should (equal clutch--result-rows '((1) (2))))
        (should-not (clutch-db--foreground-busy-p 'async-conn))))))

(ert-deftest clutch-test-last-page-counts-rows-first-without-blocking ()
  "The last page of an uncounted result should load once the count arrives."
  (clutch-test--with-result-state
      (:columns '("id")
       :rows '((1) (2))
       :connection 'async-conn
       :base-query "SELECT id FROM t"
       :server-pageable t
       :server-rewritable t
       :page-total-rows nil
       :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (&rest _) "SELECT count(*) FROM t"))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql page-num &rest _)
                   (format "SELECT id FROM t PAGE %d" page-num)))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-last-page)
        (should (equal (mapcar #'car finishes) '("SELECT count(*) FROM t")))
        (funcall (cdar finishes) (make-clutch-db-result :rows '((5))) nil)
        (ert-run-idle-timers)
        (should (= clutch--page-total-rows 5))
        (should (equal (mapcar #'car finishes)
                       '("SELECT id FROM t PAGE 2" "SELECT count(*) FROM t")))
        ;; The page load counts its own time once the count's has ended.
        (should clutch--execution-start-time)))))

(ert-deftest clutch-test-async-execute-drops-outcome-of-killed-buffer ()
  "A statement finishing after its buffer is killed should only be reported."
  (let ((buffer (generate-new-buffer " *clutch-async-source*"))
        messages displayed)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch-result--display)
                 (lambda (&rest _args) (setq displayed t)))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages))))
        (with-current-buffer buffer
          (setq-local clutch-connection 'async-conn)
          (clutch--execute "UPDATE t SET n = 1 WHERE id = 1"))
        (kill-buffer buffer)
        (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
        (ert-run-idle-timers)
        (should-not displayed)
        (should (member "Query finished after its buffer was killed" messages))
        (should-not (gethash 'async-conn clutch--running-queries))
        (should-not (clutch-db--foreground-busy-p 'async-conn))))))

(ert-deftest clutch-test-async-markers-follow-edits-while-running ()
  "Status markers should stay on their statement while the buffer is edited.
While it runs or is being cancelled, its background covers just its text.
A statement deleted while it runs leaves no marker behind."
  (cl-flet ((marker-line ()
              (save-excursion
                (goto-char (overlay-start clutch--executed-sql-overlay))
                (buffer-substring-no-properties
                 (line-beginning-position) (line-end-position))))
            (marked-text ()
              (buffer-substring-no-properties
               (overlay-start clutch--executed-sql-overlay)
               (overlay-end clutch--executed-sql-overlay))))
    (with-temp-buffer
      (insert "SELECT 1;\nUPDATE t SET n = 1;\n")
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'clutch-result--display) #'ignore)
                  ((symbol-function 'clutch-db-interrupt-query) (lambda (_conn) t)))
          (clutch--execute-and-mark "UPDATE t SET n = 1;" 11 (point-max))
          (goto-char (point-min))
          (insert "-- typed while it runs\n")
          (save-excursion
            (goto-char (overlay-start clutch--executed-sql-overlay))
            (insert "-- typed at its start\n"))
          (should (equal (marked-text) "UPDATE t SET n = 1;"))
          (clutch-cancel-query-or-quit)
          (should (equal (marker-line) "UPDATE t SET n = 1;"))
          (should (equal (marked-text) "UPDATE t SET n = 1;"))
          (should (eq (overlay-get clutch--executed-sql-overlay 'face)
                      'clutch-cancelling-sql-face))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (marker-line) "UPDATE t SET n = 1;")))))
    (with-temp-buffer
      (insert "SELECT 1;  \nUPDATE t SET n = 1;\n")
      (setq-local clutch-connection 'async-conn)
      (goto-char (point-min))
      (search-forward "UPDATE")
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'clutch-result--display) #'ignore))
          (clutch-execute-dwim (point) (point))
          (goto-char (point-min))
          (end-of-line)
          (insert "-- typed while it runs")
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (marker-line) "UPDATE t SET n = 1;")))))
    (with-temp-buffer
      (insert "UPDATE a SET n = 1;\nUPDATE b SET n = 2;\n")
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'clutch-result--display) #'ignore)
                  ((symbol-function 'message) #'ignore))
          (clutch--execute-statements
           (clutch--split-statement-specs (buffer-string) (point-min)))
          (goto-char (point-min))
          (insert "-- typed while the first one runs\n")
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (marker-line) "UPDATE b SET n = 2;")))))
    (with-temp-buffer
      (insert "SELECT 1;\nUPDATE t SET n = 1;\n")
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'clutch-result--display) #'ignore))
          (clutch--execute-and-mark "UPDATE t SET n = 1;" 11 (point-max))
          (delete-region 11 (point-max))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should-not clutch--executed-sql-overlay))))))

(ert-deftest clutch-test-execute-at-point-flashes-the-statement ()
  "The statement picked at point should flash first.
SQLite runs it synchronously, which blocks pulse's timers and redisplay,
so the flash is drawn before it runs.  Point stays and no region becomes
active.  The flash marks the picked statement before any confirmation,
so a declined statement has flashed too.  SQL run without a source
region, as from the REPL, does not flash, and
`clutch-pulse-statement-at-point' set to nil turns the flash off."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
         events
         (record-query (lambda (_conn sql) (push (cons 'query sql) events))))
    ;; Emacs 32 replaces a generic's lazy dispatcher on its first call,
    ;; which drops a `cl-letf' wrapper but keeps advice.
    (advice-add 'clutch-db-query :before record-query)
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (setq-local clutch-connection conn)
          (insert "SELECT 1 AS a;\nSELECT 2 AS b;\n")
          (goto-char (point-min))
          (search-forward "SELECT 2")
          (cl-letf (((symbol-function 'pulse-momentary-highlight-region)
                     (lambda (beg end &rest _)
                       (push (cons 'pulse (buffer-substring-no-properties beg end))
                             events)))
                    ((symbol-function 'redisplay)
                     (lambda (&rest _) (push 'redisplay events)))
                    ((symbol-function 'clutch-result--display) #'ignore))
            (let ((point (point)))
              (call-interactively #'clutch-execute-dwim)
              (should (= (point) point))
              (should-not (region-active-p)))
            (let ((sent (seq-drop-while (lambda (event)
                                          (not (eq (car-safe event) 'pulse)))
                                        (reverse events))))
              (should (equal (seq-take sent 2) '((pulse . "SELECT 2 AS b") redisplay)))
              (should (string-prefix-p "SELECT 2 AS b" (cdr (assq 'query sent)))))
            (setq events nil)
            (clutch--execute "SELECT 3")
            (should-not (assq 'pulse events))
            (setq events nil)
            (let ((clutch-pulse-statement-at-point nil))
              (call-interactively #'clutch-execute-dwim))
            (should (string-prefix-p "SELECT 2 AS b" (cdr (assq 'query events))))
            (should-not (assq 'pulse events))
            (setq events nil)
            (goto-char (point-max))
            (insert "DELETE FROM t WHERE id = 1;")
            (cl-letf (((symbol-function 'yes-or-no-p) #'ignore))
              (should-error (call-interactively #'clutch-execute-dwim)
                            :type 'user-error))
            (should (equal (assq 'pulse events)
                           '(pulse . "DELETE FROM t WHERE id = 1")))
            (should-not (assq 'query events))))
      (advice-remove 'clutch-db-query record-query)
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-execute-chosen-text-does-not-flash ()
  "A region, the buffer and each statement of a batch should not flash.
The user chose that text, so it needs no flash to show what runs."
  (with-temp-buffer
    (insert "UPDATE a SET n = 1;\nUPDATE b SET n = 2;\n")
    (setq-local clutch-connection 'async-conn)
    (let ((transient-mark-mode t)
          flashes)
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'pulse-momentary-highlight-region)
                   (lambda (beg end &rest _)
                     (push (buffer-substring-no-properties beg end) flashes)))
                  ((symbol-function 'clutch-result--display) #'ignore)
                  ((symbol-function 'message) #'ignore))
          (cl-flet ((finish ()
                      (funcall (cdar finishes)
                               (make-clutch-db-result :affected-rows 1) nil)
                      (ert-run-idle-timers)))
            (clutch-execute-buffer)
            (finish)
            (finish)
            (should (equal (mapcar #'car finishes)
                           '("UPDATE b SET n = 2" "UPDATE a SET n = 1")))
            (goto-char (point-min))
            (set-mark (line-end-position))
            (call-interactively #'clutch-execute-dwim)
            (finish)
            (should (equal (caar finishes) "UPDATE a SET n = 1"))
            (should (= (length finishes) 3))
            (should-not flashes)))))))

(ert-deftest clutch-test-async-statements-run-one-after-another ()
  "A batch should send each statement after the previous one finishes."
  (with-temp-buffer
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (let (messages)
        (cl-letf (((symbol-function 'clutch--show-execution-error)
                   (lambda (&rest _args) "duplicate key"))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute-statements
           '("INSERT INTO t VALUES (1)"
             "INSERT INTO t VALUES (2)"
             "INSERT INTO t VALUES (3)"))
          (should (equal (mapcar #'car finishes) '("INSERT INTO t VALUES (1)")))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (mapcar #'car finishes)
                         '("INSERT INTO t VALUES (2)"
                           "INSERT INTO t VALUES (1)")))
          (funcall (cdar finishes) nil '(clutch-db-error "duplicate key"))
          (ert-run-idle-timers)
          (should (= (length finishes) 2))
          (should (cl-find-if (lambda (text)
                                (string-prefix-p
                                 "Statement 2 failed: duplicate key" text))
                              messages))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-async-batch-stops-when-cancel-meets-a-finished-statement ()
  "C-g should stop a batch even when its statement finishes first.
A statement's result can arrive before the cancel; that statement keeps
its outcome and the next one does not run, even if cancellation fails."
  (dolist (cancel-result '(t nil error))
    (ert-info ((format "Cancel result: %S" cancel-result))
      (with-temp-buffer
        (clutch-mode)
        (setq-local clutch-connection 'async-conn)
        (insert "UPDATE t SET n = 1 WHERE id = 1; DELETE FROM t WHERE id = 2;")
        (clutch-test--with-async-statements finishes
          (let (messages recorded)
            (cl-letf (((symbol-function 'clutch-db-interrupt-query)
                       (lambda (_conn)
                         (if (eq cancel-result 'error)
                             (signal 'clutch-db-error '("Cancel refused"))
                           cancel-result)))
                      ((symbol-function 'clutch--record-tx-state-after-query)
                       (lambda (_conn sql) (push sql recorded)))
                      ((symbol-function 'message)
                       (lambda (format-string &rest args)
                         (push (apply #'format format-string args) messages))))
              (call-interactively #'clutch-execute-buffer)
              (call-interactively #'clutch-cancel-query-or-quit)
              (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
              (ert-run-idle-timers)
              (should (equal (mapcar #'car finishes)
                             '("UPDATE t SET n = 1 WHERE id = 1")))
              (should (equal recorded '("UPDATE t SET n = 1 WHERE id = 1")))
              (should (member "1 statement executed, then cancelled" messages))
              (should-not (gethash 'async-conn clutch--running-queries))
              (should-not (clutch-db--foreground-busy-p 'async-conn)))))))))

(ert-deftest clutch-test-async-batch-releases-its-connection-on-a-quit ()
  "A quit while a batch starts its next statement should end the batch.
Otherwise the connection stays reserved and the mode line keeps counting."
  (with-temp-buffer
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (let ((execute (symbol-function 'clutch--execute-statement))
            (calls 0))
        (cl-letf (((symbol-function 'message) #'ignore)
                  ((symbol-function 'clutch--execute-statement)
                   (lambda (&rest args)
                     (if (= (cl-incf calls) 2)
                         (signal 'quit nil)
                       (apply execute args)))))
          (clutch--execute-statements
           '("UPDATE t SET n = 1 WHERE id = 1" "UPDATE t SET n = 2 WHERE id = 2"))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          ;; ERT does not fail a test that quits, so take the quit here.
          (condition-case nil (ert-run-idle-timers) (quit nil))
          (should (= calls 2))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-async-batch-failure-after-buffer-kill-says-nothing-odd ()
  "A batch statement that fails after its buffer is killed should not say nil."
  (let ((source (generate-new-buffer " *clutch-batch-killed*"))
        messages)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages))))
        (with-current-buffer source
          (setq-local clutch-connection 'async-conn)
          (clutch--execute-statements
           '("UPDATE t SET n = 1 WHERE id = 1" "UPDATE t SET n = 2 WHERE id = 2")))
        (kill-buffer source)
        (funcall (cdar finishes) nil '(clutch-db-error "connection reset"))
        (ert-run-idle-timers)
        (should-not (member "nil" messages))
        (should-not (clutch-db--foreground-busy-p 'async-conn))))))

(ert-deftest clutch-test-cancel-command-cancels-a-running-query-or-quits ()
  "C-g should ask once to cancel the running query and otherwise quit."
  (dolist (accepted '(nil t))
    (with-temp-buffer
      (setq-local clutch-connection 'async-conn)
      (let ((clutch--running-queries (make-hash-table :test 'eq))
            interrupts)
        (cl-letf (((symbol-function 'clutch--update-mode-line) #'ignore)
                  ((symbol-function 'message) #'ignore)
                  ((symbol-function 'clutch-db-interrupt-query)
                   (lambda (conn) (push conn interrupts) accepted)))
          (cl-flet ((press ()
                      (condition-case nil
                          (progn (clutch-cancel-query-or-quit) 'cancelled)
                        (quit 'quit))))
            (should (eq (press) 'quit))
            (puthash 'async-conn
                     (list :buffer (current-buffer) :region nil :cancelling nil)
                     clutch--running-queries)
            (should (eq (press) 'cancelled))
            (should (plist-get (gethash 'async-conn clutch--running-queries)
                               :cancelling))
            (should (eq (press) 'quit))
            (should (equal interrupts '(async-conn)))))))))

(ert-deftest clutch-test-present-outcome-uses-executing-connection ()
  "Presentation should use the connection that produced the outcome."
  (let ((result (make-clutch-db-result :columns ["id"] :rows '((1))))
        displayed-connection)
    (cl-letf (((symbol-function 'clutch-result--display-select)
               (lambda (connection &rest _args)
                 (setq displayed-connection connection))))
      (clutch--present-statement-outcome
       "SELECT 1" 'old-conn
       (list :connection 'new-conn
             :result result
             :result-query-p t
             :source-buffer (current-buffer)))
      (should (eq displayed-connection 'new-conn)))))

(ert-deftest clutch-test-handle-query-quit-remembers-interrupt-error-details-and-debug-event ()
  "Interrupt RPC failures should record details and retain reconnect anchors."
  (with-temp-buffer
    (let* ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
           (clutch-debug-mode t)
           (clutch-connection conn)
           (raw-message "Connection refused (host=db.example.com, port=3306)")
           (captured-message nil)
           (disconnected nil)
           (live t)
           (record (generate-new-buffer " *clutch-abandoned-record*")))
      (unwind-protect
          (progn
            (clutch--clear-debug-capture)
            (with-current-buffer record
              (clutch-record-mode)
              (setq-local clutch-connection conn
                          clutch--connection-render-state
                          '(:connected-p t)))
            (cl-letf (((symbol-function 'clutch-db-backend-key)
                      (lambda (_conn) 'pg))
                      ((symbol-function 'clutch--connection-alive-p)
                       (lambda (_conn) live))
                      ((symbol-function 'clutch-db-interrupt-query)
                       (lambda (_conn)
                         (signal 'clutch-db-error (list raw-message))))
                      ((symbol-function 'clutch-db-disconnect)
                       (lambda (_conn)
                         (setq disconnected t
                               live nil)))
                      ((symbol-function 'message)
                       (lambda (fmt &rest args)
                         (setq captured-message (apply #'format fmt args)))))
              (should-error (clutch--handle-query-quit clutch-connection)
                            :type 'clutch-query-interrupted)
              (let* ((summary (clutch--humanize-db-error raw-message))
                     (message-summary
                      (condition-case err
                          (signal 'clutch-db-error (list raw-message))
                        (clutch-db-error
                         (clutch--humanize-db-error
                          (error-message-string err)))))
                     (details clutch--buffer-error-details)
                     (diag (plist-get details :diag))
                     (context (plist-get diag :context))
                     (debug-text (clutch-test--debug-buffer-string)))
                (should disconnected)
                (should details)
                (should (eq (plist-get details :backend) 'pg))
                (should (equal (plist-get details :summary) summary))
                (should (equal (plist-get diag :raw-message) raw-message))
                (should (plist-member context :sql))
                (should-not (plist-get context :sql))
                (should
                 (equal captured-message
                        (format
                         "Interrupt failed: %s"
                         (clutch--debug-workflow-message message-summary))))
                (dolist (expected
                         `(,(concat "Operation: cancel\nPhase: error")
                           ,(concat "Summary: " message-summary)
                           "Operation: interrupt\nPhase: disconnect"))
                  (should (string-match-p (regexp-quote expected) debug-text)))
                (with-current-buffer record
                  (should (eq clutch-connection conn))
                  (should
                   (string-match-p
                    "DISCONNECTED"
                    (substring-no-properties
                     (clutch--header-with-disconnect-badge "Record"))))))))
        (when (buffer-live-p record)
          (kill-buffer record))))))

(ert-deftest clutch-test-execute-uses-backend-result-query-p ()
  "Execute should let the backend classify non-SQL result-set queries."
  (let (captured outcome)
    (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
              ((symbol-function 'clutch-db-result-query-p)
               (lambda (conn sql)
                 (setq captured (list conn sql))
                 t))
              ((symbol-function 'clutch--run-db-query)
               (lambda (&rest _) (make-clutch-db-result))))
      (setq outcome
            (clutch-test--await-outcome
             (lambda (k)
               (clutch--execute-statement
                "db.users.find()" 'document-conn nil nil k))))
      (should (equal captured '(document-conn "db.users.find()")))
      (should (plist-get outcome :result-query-p)))))

(ert-deftest clutch-test-execute-statements-confirms-each-nonselect ()
  "Batch execution should apply destructive and high-risk guards."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (let (queries risky-sqls yes-prompts)
      (cl-letf (((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql)
                   (push sql queries)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'clutch--confirm-high-risk-query)
                 (lambda (sql)
                   (push sql risky-sqls)
                   (string-prefix-p "UPDATE" sql)))
                ((symbol-function 'clutch-db-sql-schema-affecting-p)
                 (lambda (_sql) nil))
                ((symbol-function 'yes-or-no-p)
                 (lambda (_prompt)
                   (setq yes-prompts (1+ (or yes-prompts 0)))
                   t))
                ((symbol-function 'message) #'ignore))
        (clutch--execute-statements
         '("DROP TABLE users" "UPDATE users SET admin = 1"))
        (should (equal (nreverse queries)
                       '("DROP TABLE users" "UPDATE users SET admin = 1")))
        (should (equal (nreverse risky-sqls)
                       '("DROP TABLE users" "UPDATE users SET admin = 1")))
        (should (= yes-prompts 1))))))

(ert-deftest clutch-test-execute-statements-refreshes-schema-after-ddl ()
  "Batch DDL execution should refresh or invalidate schema metadata."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (let (refreshed)
      (cl-letf (((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :affected-rows 0)))
                ((symbol-function 'clutch-db-sql-schema-affecting-p)
                 (lambda (_sql) t))
                ((symbol-function 'clutch-db-eager-schema-refresh-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--refresh-schema-cache-async)
                 (lambda (conn) (setq refreshed conn)))
                ((symbol-function 'clutch--confirm-high-risk-query)
                 #'ignore)
                ((symbol-function 'yes-or-no-p) (lambda (_prompt) t))
                ((symbol-function 'message) #'ignore))
        (clutch--execute-statements '("CREATE TABLE users (id INT)"))
        (should (eq refreshed 'fake-conn))))))

(provide 'clutch-test-query)

;;; clutch-test-query.el ends here
