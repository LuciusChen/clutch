;;; clutch-export.el --- Result copy and export workflow -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; Internal result copy and export helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'clutch-query)
(require 'clutch-ui)
(require 'subr-x)

(defcustom clutch-csv-export-default-coding-system 'utf-8-with-signature
  "Default coding system when exporting CSV files."
  :type '(choice (const :tag "UTF-8 (with BOM)" utf-8-with-signature)
                 (const :tag "UTF-8" utf-8)
                 (const :tag "GBK" gbk)
                 (coding-system :tag "Other coding system"))
  :group 'clutch)

(defvar clutch-connection)
(defvar clutch-result-max-rows)
(defvar clutch--base-query)
(defvar clutch--order-by)
(defvar clutch--result-columns)
(defvar clutch--result-rows)

(declare-function clutch--build-paged-sql "clutch-query"
                  (base-sql page-num page-size &optional order-by page-offset))
(declare-function clutch--ensure-connection "clutch-connection" ())
(declare-function clutch--format-value "clutch-ui" (val))
(declare-function clutch--insert-target-table "clutch" ())
(declare-function clutch--message-count "clutch-ui" (value))
(declare-function clutch--message-keyword "clutch-ui" (value))
(declare-function clutch--prepare-row-identity-query "clutch-query" (conn sql))
(declare-function clutch--run-db-query "clutch-connection" (conn sql &optional params))
(declare-function clutch--sql-has-limit-p "clutch-query" (sql))
(declare-function clutch--visible-columns "clutch-ui" ())
(declare-function clutch-result--build-insert-statements "clutch"
                  (indices col-indices table))
(declare-function clutch-result--build-insert-statements-for-rows "clutch"
                  (rows col-indices table))
(declare-function clutch-result--build-update-statements-for-rows "clutch"
                  (rows col-indices op))
(declare-function clutch-result--cell-at-point "clutch-edit" ())
(declare-function clutch-result--effective-query "clutch-query" ())
(declare-function clutch-result--region-rectangle-indices "clutch" ())
(declare-function clutch-result--selected-row-indices "clutch" ())

(defconst clutch--result-export-formats
  '(("csv-copy" :kind csv :destination clipboard)
    ("csv-file" :kind csv :destination file)
    ("insert-copy" :kind insert :destination clipboard)
    ("insert-file" :kind insert :destination file)
    ("update-copy" :kind update :destination clipboard)
    ("update-file" :kind update :destination file))
  "Available result export choices and their execution targets.")

(defconst clutch--result-export-kinds
  '((csv . (:content clutch--export-csv-content
            :file-prompt "Export CSV to file: "
            :default-file "export.csv"
            :copy-message "Copied %d row%s as CSV"
            :file-message "Exported %d row%s to %s (%s)"
            :file-coding csv))
    (insert . (:content clutch--export-insert-content
               :file-prompt "Export SQL to file: "
               :default-file "export.sql"
               :copy-message "Copied %d row%s as INSERT SQL"
               :file-message "Exported %d row%s as INSERT SQL to %s"))
    (update . (:content clutch--export-update-content
               :file-prompt "Export SQL to file: "
               :default-file "export.sql"
               :copy-message "Copied %d row%s as UPDATE SQL"
               :file-message "Exported %d row%s as UPDATE SQL to %s")))
  "Result export behavior keyed by logical export kind.")

(defun clutch-result--copy-selection-indices (&optional rect)
  "Return row and column indices for result copy commands.
RECT, when non-nil, has priority.  Otherwise active regions are treated as a
rectangle and inactive regions fall back to the current cell."
  (let ((rect (or rect
                  (if (use-region-p)
                      (clutch-result--region-rectangle-indices)
                    (pcase-let ((`(,ridx ,cidx ,_v)
                                 (or (clutch-result--cell-at-point)
                                     (clutch--user-error "No cell at point"))))
                      (cons (list ridx) (list cidx)))))))
    (cons (or (car-safe rect)
              (clutch-result--selected-row-indices)
              (clutch--user-error "No row at point"))
          (or (cdr-safe rect)
              (clutch--visible-columns)))))

(defun clutch-result--copy-rows-as-insert (&optional rect)
  "Copy row(s) as INSERT statement(s) to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell."
  (let* ((selection (clutch-result--copy-selection-indices rect))
         (indices (car selection))
         (col-indices (cdr selection))
         (table (clutch--insert-target-table))
         (stmts (clutch-result--build-insert-statements indices col-indices table)))
    (kill-new (mapconcat #'identity stmts "\n"))
    (deactivate-mark)
    (message "Copied %s %s statement%s (%s col%s)"
             (clutch--message-count (length stmts))
             (clutch--message-keyword "INSERT")
             (if (= (length stmts) 1) "" "s")
             (clutch--message-count (length col-indices))
             (if (= (length col-indices) 1) "" "s"))))

(defun clutch-result--copy-rows-as-update (&optional rect)
  "Copy row(s) as UPDATE statement(s) to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell."
  (let* ((selection (clutch-result--copy-selection-indices rect))
         (indices (car selection))
         (col-indices (cdr selection))
         (rows (mapcar (lambda (ridx) (nth ridx clutch--result-rows)) indices))
         (stmts (clutch-result--build-update-statements-for-rows
                 rows col-indices "copy UPDATE SQL")))
    (kill-new (mapconcat #'identity stmts "\n"))
    (deactivate-mark)
    (message "Copied %s %s statement%s (%s col%s)"
             (clutch--message-count (length stmts))
             (clutch--message-keyword "UPDATE")
             (if (= (length stmts) 1) "" "s")
             (clutch--message-count (length col-indices))
             (if (= (length col-indices) 1) "" "s"))))

(defun clutch--csv-escape (val)
  "Return CSV-escaped string for VAL."
  (let ((s (clutch--format-value val)))
    (if (string-match-p "[,\"\n]" s)
        (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" s))
      s)))

(defun clutch--csv-lines-for-rows (rows col-indices)
  "Return CSV lines for ROWS using COL-INDICES."
  (let ((col-names (mapcar (lambda (i) (nth i clutch--result-columns))
                           col-indices)))
    (cons (mapconcat #'identity col-names ",")
          (cl-loop for row in rows
                   for vals = (mapcar (lambda (i) (nth i row)) col-indices)
                   collect (mapconcat #'clutch--csv-escape vals ",")))))

(defun clutch-result--build-csv-lines (indices col-indices)
  "Return CSV lines for INDICES rows using COL-INDICES columns."
  (clutch--csv-lines-for-rows
   (mapcar (lambda (ridx) (nth ridx clutch--result-rows)) indices)
   col-indices))

(defun clutch-result--copy-rows-as-csv (&optional rect)
  "Copy row(s) as CSV to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell.
Includes a header row with column names."
  (let* ((selection (clutch-result--copy-selection-indices rect))
         (indices (car selection))
         (col-indices (cdr selection))
         (lines (clutch-result--build-csv-lines indices col-indices)))
    (kill-new (mapconcat #'identity lines "\n"))
    (deactivate-mark)
    (message "Copied %s row%s as %s (%s col%s)"
             (clutch--message-count (length indices))
             (if (= (length indices) 1) "" "s")
             (clutch--message-keyword "CSV")
             (clutch--message-count (length col-indices))
             (if (= (length col-indices) 1) "" "s"))))

;;;###autoload
(defun clutch-result-export ()
  "Export the current result.
Prompts for format:
- csv-copy: all rows to clipboard as CSV text
- csv-file: all rows to CSV file
- insert-copy: all rows to clipboard as INSERT statements
- insert-file: all rows to a .sql file as INSERT statements
- update-copy: all rows to clipboard as UPDATE statements
- update-file: all rows to a .sql file as UPDATE statements."
  (interactive)
  (let* ((choice (completing-read
                  "Export format: "
                  (mapcar #'car clutch--result-export-formats)
                  nil t))
         (format (cdr (assoc choice clutch--result-export-formats))))
    (unless format
      (clutch--user-error "Unsupported export format: %s" choice))
    (clutch--export-result format)))

(defun clutch--export-csv-content (rows)
  "Return CSV export text for ROWS using current visible result columns."
  (let* ((lines (clutch--csv-lines-for-rows rows (clutch--visible-columns)))
         (body (mapconcat #'identity (cdr lines) "\n")))
    (if (string-empty-p body)
        (concat (car lines) "\n")
      (concat (car lines) "\n" body "\n"))))

(defun clutch--csv-export-coding-choices ()
  "Return alist of CSV export coding labels to coding systems."
  (let ((pairs '(("utf-8-bom" . utf-8-with-signature)
                 ("utf-8" . utf-8)
                 ("gbk" . gbk)
                 ("cp936" . cp936))))
    (cl-loop for (label . coding) in pairs
             when (coding-system-p coding)
             collect (cons label coding))))

(defun clutch--read-csv-export-coding-system ()
  "Read coding system for CSV file export."
  (let* ((choices (clutch--csv-export-coding-choices))
         (default (if (coding-system-p clutch-csv-export-default-coding-system)
                      clutch-csv-export-default-coding-system
                    'utf-8-with-signature))
         (default-label (car (rassoc default choices)))
         (label (completing-read
                 (format "CSV encoding (default %s): "
                         (or default-label (symbol-name default)))
                 (mapcar #'car choices) nil t nil nil default-label)))
    (or (cdr (assoc label choices)) default)))

(defun clutch-result--collect-all-export-rows ()
  "Return all rows for current result by auto-paging when needed."
  (clutch--ensure-connection)
  (let ((effective-sql (clutch-result--effective-query)))
    (if (null effective-sql)
        clutch--result-rows
      (let* ((row-identity-prep
              (clutch--prepare-row-identity-query clutch-connection effective-sql))
             (identity-sql (plist-get row-identity-prep :sql)))
        (if (or (null clutch--base-query)
                (clutch--sql-has-limit-p effective-sql))
            (clutch-db-result-rows
             (clutch--run-db-query clutch-connection identity-sql))
          (cl-loop with page-size = clutch-result-max-rows
                   for page-num from 0
                   for paged-sql = (clutch--build-paged-sql
                                    identity-sql page-num page-size
                                    clutch--order-by)
                   for result = (clutch--run-db-query
                                  clutch-connection paged-sql)
                   for batch = (clutch-db-result-rows result)
                   append batch into rows
                   until (< (length batch) page-size)
                   finally return rows))))))

(defun clutch--export-insert-content (rows)
  "Return INSERT statement export text for ROWS using current result metadata."
  (let* ((table (clutch--insert-target-table))
         (col-indices (clutch--visible-columns))
         (stmts (clutch-result--build-insert-statements-for-rows
                 rows col-indices table)))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-update-content (rows)
  "Return UPDATE statement export text for ROWS using current result metadata."
  (let* ((col-indices (clutch--visible-columns))
         (stmts (clutch-result--build-update-statements-for-rows
                 rows col-indices "export UPDATE SQL")))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-result (format)
  "Execute result export described by FORMAT."
  (let* ((kind (plist-get format :kind))
         (destination (plist-get format :destination))
         (spec (or (cdr (assq kind clutch--result-export-kinds))
                   (clutch--user-error "Unsupported export kind: %s" kind)))
         (rows (clutch-result--collect-all-export-rows))
         (coding (when (and (eq destination 'file)
                            (eq (plist-get spec :file-coding) 'csv))
                   (clutch--read-csv-export-coding-system)))
         (text (funcall (plist-get spec :content) rows))
         (row-count (length rows))
         (row-suffix (if (= (length rows) 1) "" "s")))
    (pcase destination
      ('clipboard
       (kill-new text)
       (message (plist-get spec :copy-message) row-count row-suffix))
      ('file
       (let* ((path (read-file-name (plist-get spec :file-prompt)
                                    nil nil nil
                                    (plist-get spec :default-file)))
              (coding-system-for-write (or coding coding-system-for-write)))
         (with-temp-buffer
           (insert text)
           (write-region (point-min) (point-max) path nil 'silent))
         (apply #'message (plist-get spec :file-message)
                (append (list row-count row-suffix path)
                        (when coding (list coding))))))
      (_
       (clutch--user-error "Unsupported export destination: %s" destination)))))

(provide 'clutch-export)

;;; clutch-export.el ends here
