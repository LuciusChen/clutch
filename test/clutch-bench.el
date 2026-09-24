;;; clutch-bench.el --- Performance benchmark suite -*- lexical-binding: t; -*-

;; Copyright (C) 2026 Free Software Foundation, Inc.

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; URL: https://github.com/LuciusChen/clutch

;; This file is part of clutch.

;;; Commentary:

;; Run live backend benchmarks against local container databases.
;;
;; Example:
;;   Emacs -Q --batch \
;;     --eval '(setq load-prefer-newer t)' \
;;     -L . -L test \
;;     -l test/clutch-bench.el \
;;     -f clutch-bench-run-all
;;
;; `clutch-bench-run-sql-context' needs no database: it times statement,
;; eldoc and completion analysis on a long pasted SQL script.

;;; Code:

(require 'cl-lib)
(require 'benchmark)
(require 'clutch)
(require 'clutch-backend)

(defvar clutch-bench-iterations 30
  "Number of iterations per query benchmark.")

(defvar clutch-bench-mysql-params
  '(:host "127.0.0.1" :port 3306 :user "root" :password "test" :database "mysql")
  "MySQL benchmark connection parameters.
Uses the built-in `mysql` database so the benchmark does not imply a
project-shipped demo schema.")

(defvar clutch-bench-pg-params
  '(:host "127.0.0.1" :port 5432 :user "postgres" :password "test" :database "postgres")
  "PostgreSQL benchmark connection parameters.")

(defvar clutch-bench-ui-rows 1000
  "Number of synthetic result rows for UI render benchmarks.")

(defvar clutch-bench-ui-columns 120
  "Number of synthetic result columns for UI render benchmarks.")

(defvar clutch-bench-ui-full-render-iterations 3
  "Number of full result redraw iterations for UI benchmarks.")

(defvar clutch-bench-ui-row-refresh-iterations 100
  "Number of row-local refresh iterations for UI benchmarks.")

(defconst clutch-bench--mysql-medium-sql
  "WITH RECURSIVE seq AS (
     SELECT 1 AS n
     UNION ALL
     SELECT n + 1 FROM seq WHERE n < 500
   )
   SELECT n, CONCAT('user_', n) AS name, ROUND(n * 1.23, 2) AS score
   FROM seq"
  "Medium-size MySQL query used for benchmark.")

(defconst clutch-bench--pg-medium-sql
  "SELECT g AS n,
          CONCAT('user_', g::text) AS name,
          ROUND((g * 1.23)::numeric, 2) AS score
   FROM generate_series(1, 500) AS g"
  "Medium-size PostgreSQL query used for benchmark.")

(defun clutch-bench--ms (seconds)
  "Convert SECONDS to milliseconds."
  (* 1000.0 seconds))

(defun clutch-bench--format-ms (seconds)
  "Format SECONDS as milliseconds with 2 decimal places."
  (format "%.2fms" (clutch-bench--ms seconds)))

(defun clutch-bench--sample-query (conn sql iterations)
  "Run SQL on CONN for ITERATIONS and return timing statistics."
  (let ((times nil)
        (rows 0)
        (gc-start gcs-done))
    (dotimes (_ iterations)
      (let ((start (float-time)))
        (let ((result (clutch-db-query conn sql)))
          (setq rows (length (clutch-db-result-rows result))))
        (push (- (float-time) start) times)))
    (let* ((sorted (sort times #'<))
           (sum (cl-reduce #'+ sorted))
           (avg (/ sum iterations))
           (min (car sorted))
           (max (car (last sorted)))
           (p95 (nth (max 0 (1- (ceiling (* iterations 0.95)))) sorted))
           (gc-delta (- gcs-done gc-start)))
      (list :avg avg :min min :max max :p95 p95 :rows rows :gcs gc-delta))))

(defun clutch-bench--one-backend (backend params medium-sql)
  "Run connection and query benchmarks for BACKEND using PARAMS and MEDIUM-SQL."
  (condition-case err
      (let* ((connect-start (float-time))
             (conn (clutch-db-connect backend params))
             (connect-elapsed (- (float-time) connect-start))
             (select1 (clutch-bench--sample-query conn "SELECT 1" clutch-bench-iterations))
             (medium (clutch-bench--sample-query conn medium-sql clutch-bench-iterations)))
        (unwind-protect
            (list :backend backend
                  :connect connect-elapsed
                  :select1 select1
                  :medium medium)
          (ignore-errors (clutch-db-disconnect conn))))
    (error
     (list :backend backend :error (error-message-string err)))))

(defun clutch-bench--print-query-line (label stats)
  "Print one LABEL line from STATS."
  (princ (format "  %-8s avg=%s p95=%s min=%s max=%s rows=%d gcs=%d\n"
                 label
                 (clutch-bench--format-ms (plist-get stats :avg))
                 (clutch-bench--format-ms (plist-get stats :p95))
                 (clutch-bench--format-ms (plist-get stats :min))
                 (clutch-bench--format-ms (plist-get stats :max))
                 (plist-get stats :rows)
                 (plist-get stats :gcs))))

(defun clutch-bench--print-result (result)
  "Print benchmark RESULT."
  (princ (format "\n[%s]\n" (upcase (symbol-name (plist-get result :backend)))))
  (if-let* ((err (plist-get result :error)))
      (princ (format "  ERROR: %s\n" err))
    (princ (format "  connect  %s\n"
                   (clutch-bench--format-ms (plist-get result :connect))))
    (clutch-bench--print-query-line "select1" (plist-get result :select1))
    (clutch-bench--print-query-line "medium" (plist-get result :medium))))

(defun clutch-bench--ui-columns ()
  "Return synthetic UI benchmark column names."
  (cl-loop for i below clutch-bench-ui-columns
           collect (format "c%03d" i)))

(defun clutch-bench--ui-column-defs (columns)
  "Return synthetic column defs for COLUMNS."
  (mapcar (lambda (column)
            (list :name column
                  :type-category
                  (if (string-suffix-p "0" column) 'numeric 'text)))
          columns))

(defun clutch-bench--ui-rows ()
  "Return synthetic UI benchmark rows."
  (cl-loop for r below clutch-bench-ui-rows
           collect
           (cl-loop for c below clutch-bench-ui-columns
                    collect (format "r%d-c%d-value" r c))))

(defun clutch-bench--setup-ui-buffer ()
  "Install synthetic result state in the current buffer."
  (let* ((columns (clutch-bench--ui-columns))
         (defs (clutch-bench--ui-column-defs columns))
         (rows (clutch-bench--ui-rows)))
    (clutch-result-mode)
    (setq-local clutch-connection nil
                clutch--connection-params nil
                clutch--conn-sql-product nil
                clutch--result-columns columns
                clutch--result-column-defs defs
                clutch--result-rows rows
                clutch--filtered-rows nil
                clutch--filter-pattern nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--fk-info nil
                clutch--active-edit-cell nil
                clutch--row-identity nil
                clutch--row-identity-status nil
                clutch--result-source-table nil
                clutch--page-current 0
                clutch--page-offset 0
                clutch--page-total-rows nil
                clutch--page-has-more nil
                clutch--query-elapsed nil
                clutch--where-filter nil
                clutch--aggregate-summary nil
                clutch-result-max-rows clutch-bench-ui-rows
                clutch--column-widths
                (clutch--compute-column-widths columns rows defs))))

(defun clutch-bench--print-ui-line (label stats iterations)
  "Print UI benchmark LABEL from STATS over ITERATIONS."
  (let ((avg (/ (car stats) (float iterations))))
    (princ (format "  %-12s total=%s avg=%s gcs=%d gc-time=%s\n"
                   label
                   (clutch-bench--format-ms (car stats))
                   (clutch-bench--format-ms avg)
                   (cadr stats)
                   (clutch-bench--format-ms (caddr stats))))))

;;;###autoload
(defun clutch-bench-run-ui ()
  "Run synthetic result-buffer UI benchmarks."
  (interactive)
  (princ (format "clutch UI benchmark (%d rows x %d columns)\n"
                 clutch-bench-ui-rows clutch-bench-ui-columns))
  (princ (format-time-string "timestamp: %Y-%m-%d %H:%M:%S %z\n"))
  (with-temp-buffer
    (clutch-bench--setup-ui-buffer)
    (let ((full (benchmark-run clutch-bench-ui-full-render-iterations
                  (clutch--render-result)))
          row
          footer)
      (clutch--render-result)
      (setq row (benchmark-run clutch-bench-ui-row-refresh-iterations
                  (clutch--replace-row-at-index
                   (/ clutch-bench-ui-rows 2))))
      (setq footer (benchmark-run clutch-bench-ui-row-refresh-iterations
                     (clutch--refresh-footer-line)))
      (clutch-bench--print-ui-line
       "full-render" full clutch-bench-ui-full-render-iterations)
      (clutch-bench--print-ui-line
       "row-refresh" row clutch-bench-ui-row-refresh-iterations)
      (clutch-bench--print-ui-line
       "footer" footer clutch-bench-ui-row-refresh-iterations)))
  (princ "\nDone.\n"))

;;;; SQL context benchmark (long pasted SQL)

(defvar clutch-bench-sql-context-bytes (* 1024 1024)
  "Size of the synthetic SQL script for context benchmarks, in bytes.")

(defun clutch-bench--sql-script (bytes)
  "Return a semicolon-delimited SQL script of at least BYTES bytes.
Every statement carries strings, comments and parentheses holding
semicolons, so the scan cannot take shortcuts."
  (with-temp-buffer
    (let ((i 0))
      (while (< (buffer-size) bytes)
        (insert (format (concat
                         "SELECT u.id, u.name, 'it''s; ok' AS note -- x;\n"
                         "FROM users u\n"
                         "JOIN orders o ON (o.user_id = u.id AND o.n IN (1, (2)))\n"
                         "WHERE u.created_at > '2024-01-01' /* y; */ AND o.id = %d;\n\n")
                        i))
        (cl-incf i)))
    (buffer-string)))

(defun clutch-bench--sql-context-sample (label setup body)
  "Print LABEL with the time of BODY in a buffer built by SETUP."
  (let ((buf (funcall setup)))
    (unwind-protect
        (with-current-buffer buf
          (garbage-collect)
          (let ((start (float-time)))
            (funcall body)
            (princ (format "  %-28s %s\n" label
                           (clutch-bench--format-ms (- (float-time) start))))))
      (with-current-buffer buf
        ;; The stub connection has nothing to disconnect on kill.
        (setq-local clutch-connection nil)
        (kill-buffer buf)))))

;;;###autoload
(defun clutch-bench-run-sql-context ()
  "Benchmark statement, eldoc and completion analysis on a long SQL script.
Runs without a database: the connection is a stub whose schema knows the
tables the script mentions.  Each sample uses a fresh buffer so the first
request after a paste and the request after the next keystroke are both
measured."
  (interactive)
  (let* ((text (clutch-bench--sql-script clutch-bench-sql-context-bytes))
         (schema (make-hash-table :test 'equal))
         (fresh (lambda ()
                  (let ((buf (generate-new-buffer " *clutch-bench-sql*")))
                    (with-current-buffer buf
                      (clutch-mode)
                      (setq-local clutch-connection 'clutch-bench-stub)
                      (insert text)
                      (goto-char (point-max))
                      (syntax-propertize (point-max)))
                    buf))))
    (puthash "users" '("id" "name" "created_at") schema)
    (puthash "orders" '("id" "user_id" "n") schema)
    (princ (format "clutch SQL context benchmark (%d bytes, %d statements)\n"
                   (length text)
                   (length (clutch-db-sql-statement-breaks text))))
    (princ (format-time-string "timestamp: %Y-%m-%d %H:%M:%S %z\n"))
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema))
              ((symbol-function 'clutch-db-busy-p) (lambda (_conn) nil))
              ((symbol-function 'clutch-db-live-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-completion-sync-columns-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-completion-deferred-columns-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-connection-sql-dialect)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "bench"))
              ((symbol-function 'clutch--cached-table-comment)
               (lambda (&rest _) nil))
              ((symbol-function 'clutch--table-comment-cached-p)
               (lambda (&rest _) t))
              ((symbol-function 'clutch--ensure-columns)
               (lambda (_conn schema table) (gethash table schema))))
      (clutch-bench--sql-context-sample
       "statement-breaks" fresh
       (lambda () (clutch-db-sql-statement-breaks text)))
      (clutch-bench--sql-context-sample
       "eldoc after paste" fresh
       (lambda () (insert "SELECT id") (clutch--eldoc-function)))
      (clutch-bench--sql-context-sample
       "completion after paste" fresh
       (lambda () (insert "sel") (clutch-completion-at-point)))
      (clutch-bench--sql-context-sample
       "completion next keystroke"
       (lambda ()
         (let ((buf (funcall fresh)))
           (with-current-buffer buf
             (insert "sel")
             (clutch-completion-at-point)
             (insert "e"))
           buf))
       (lambda () (clutch-completion-at-point)))
      (clutch-bench--sql-context-sample
       "execute-buffer split" fresh
       (lambda () (clutch--split-statement-specs text 1)))))
  (princ "\nDone.\n"))

;;;###autoload
(defun clutch-bench-run-all ()
  "Run all live benchmarks and print a compact summary."
  (interactive)
  (princ (format "clutch benchmark (%d iterations/query)\n" clutch-bench-iterations))
  (princ (format-time-string "timestamp: %Y-%m-%d %H:%M:%S %z\n"))
  (let ((results (list (clutch-bench--one-backend 'mysql
                                                  clutch-bench-mysql-params
                                                  clutch-bench--mysql-medium-sql)
                       (clutch-bench--one-backend 'pg
                                                  clutch-bench-pg-params
                                                  clutch-bench--pg-medium-sql))))
    (dolist (result results)
      (clutch-bench--print-result result))
    (princ "\nDone.\n")))

(provide 'clutch-bench)
;;; clutch-bench.el ends here
