;;; clutch-query.el --- SQL execution and result workflow -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;; This file is part of clutch.

;; clutch is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; clutch is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with clutch.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Query console, SQL literal conversion, pagination, query execution engine,
;; error humanization, error details, and indirect edit buffers for clutch.
;;
;; This module is required by `clutch.el' — do not require `clutch' here.

;;; Code:

(require 'clutch-db)
(require 'clutch-connection)
(require 'cl-lib)

;; Forward declarations — variables defined in clutch.el
(defvar clutch-connection)
(defvar clutch--buffer-error-details)
(defvar clutch--executing-p)
(defvar clutch--conn-sql-product)
(defvar clutch--last-query)
(defvar clutch--last-result-buffer)
(defvar clutch--base-query)
(defvar clutch--connection-params)
(defvar clutch--console-name)
(defvar clutch--console-storage-name)
(defvar clutch--console-ad-hoc-params)
(defvar clutch--source-window)
(defvar clutch--executing-sql-start)
(defvar clutch--executing-sql-end)
(defvar clutch-debug-mode nil)
(defvar clutch-debug-buffer-name "*clutch-debug*")
(defvar clutch-connection-alist nil)
(defvar clutch-result-window-height 0.33)
(defvar clutch-result-max-rows 500)
(defvar clutch-console-yank-cleanup t)

;; Forward declarations — variables defined in clutch-ui / clutch-edit
(defvar clutch--aggregate-summary)
(defvar clutch--cached-pk-indices)
(defvar clutch--column-widths)
(defvar clutch--filter-pattern)
(defvar clutch--filtered-rows)
(defvar clutch--fk-info)
(defvar clutch--marked-rows)
(defvar clutch--order-by)
(defvar clutch--page-current)
(defvar clutch--page-has-more)
(defvar clutch--page-offset)
(defvar clutch--page-total-rows)
(defvar clutch--pending-deletes)
(defvar clutch--pending-edits)
(defvar clutch--pending-inserts)
(defvar clutch--query-elapsed)
(defvar clutch--row-identity)
(defvar clutch--result-column-defs)
(defvar clutch--result-columns)
(defvar clutch--result-source-table)
(defvar clutch--result-rows)
(defvar clutch--sort-column)
(defvar clutch--sort-descending)
(defvar clutch--where-filter)

;; Forward declarations — functions defined in clutch.el
(declare-function clutch--console-buffer-base-name "clutch" (name))
(declare-function clutch--debug-sql-preview "clutch-debug" (sql))
(declare-function clutch--remember-debug-event "clutch-debug" (&rest event))
(declare-function clutch--remember-problem-record "clutch-debug" (&rest args))
(declare-function clutch--find-console-buffer "clutch" (name &optional storage-name))
(declare-function clutch--update-console-buffer-name "clutch" ())
(declare-function clutch--console-persistence-name "clutch" (name &optional params))
(declare-function clutch--console-file "clutch" (name))
(declare-function clutch--effective-sql-product "clutch" (params))
(declare-function clutch--set-schema-status "clutch-schema" (conn state &optional table-count error-message))
(declare-function clutch--sql-rewrite "clutch" (sql op &optional arg))

;; Forward declarations — functions from clutch-connection.el
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch--ensure-clutch-loaded "clutch-connection" ())
(declare-function clutch--connection-oracle-jdbc-p "clutch-connection" (conn))
(declare-function clutch--clear-tx-dirty "clutch-connection" (conn))
(declare-function clutch--run-db-query "clutch-connection" (conn sql))
(declare-function clutch--connection-alive-p "clutch-connection" (conn))
(declare-function clutch--bind-connection-context "clutch-connection" (conn &optional params product))
(declare-function clutch--activate-current-buffer-connection "clutch-connection" (conn params &optional product))
(declare-function clutch--ensure-connection "clutch-connection" ())
(declare-function clutch--backend-key-from-conn "clutch-connection" (conn))
(declare-function clutch--spinner-start "clutch-connection" ())
(declare-function clutch--update-mode-line "clutch-connection" ())
(declare-function clutch--build-conn "clutch-connection" (params))
(declare-function clutch--saved-connection-params "clutch-connection" (name))
(declare-function clutch--read-manual-connection-params "clutch-connection" (&optional sqlite-file))
(declare-function clutch--read-sqlite-file-params "clutch-connection" ())
(declare-function clutch--normalize-sqlite-database-file "clutch-connection" (file))
(declare-function clutch--backend-display-name-from-params "clutch-connection" (params))
(declare-function clutch--connection-candidates-affixation "clutch-connection" (candidates))
(declare-function clutch--prepare-connection-origin-params
                  "clutch-connection" (params &optional source-default-directory))
;; clutch-result-mode and clutch-mode are defined in clutch.el which requires
;; clutch-query.el — we cannot require clutch here.
(declare-function clutch-result-mode "clutch" ())
(declare-function clutch-mode "clutch" ())

;; Forward declarations — functions from clutch-ui / clutch-edit
(declare-function clutch--mark-executed-sql-region "clutch-ui" (beg end))
(declare-function clutch--mark-failed-sql-region "clutch-ui" (beg end &optional message))
(declare-function clutch--compute-column-widths "clutch-ui" (col-names rows column-defs &optional max-width))
(declare-function clutch--refresh-display "clutch-ui" ())
(declare-function clutch--display-select-result "clutch-ui" (col-names rows columns))
(declare-function clutch--display-result "clutch-ui" (result sql elapsed))
(declare-function clutch--display-error-result "clutch-ui" (connection sql summary message &optional elapsed hint))
(declare-function clutch--format-elapsed "clutch-ui" (seconds))
(declare-function clutch--format-value "clutch-ui" (value))
(declare-function clutch--message-count "clutch-ui" (value))
(declare-function clutch--message-keyword "clutch-ui" (value))
(declare-function clutch--message-literal "clutch-ui" (value))
(declare-function clutch--load-fk-info "clutch-edit" ())
(declare-function clutch-result--pending-sql-content "clutch-edit" (&optional stmts))
(declare-function clutch--schema-for-connection "clutch-schema" (&optional conn))
(declare-function clutch--schema-status-entry "clutch-schema" (conn))
(declare-function clutch--refresh-schema-cache-async "clutch-schema" (conn))

;; Forward declarations — functions from clutch-db
(declare-function clutch-db-interrupt-query "clutch-db" (conn))

;;;; Query console

(defun clutch--console-window-for (buf)
  "Return the best window to display BUF.
Priority: (1) window already showing BUF; (2) any visible clutch
console window; (3) nil, meaning use the selected window."
  (or (get-buffer-window buf)
      (cl-find-if (lambda (w)
                    (string-prefix-p "*clutch: "
                                     (buffer-name (window-buffer w))))
                  (window-list))))

(defun clutch--sqlite-file-console-target (&optional params)
  "Return an ad hoc SQLite query console target for PARAMS."
  (let ((params (or params (clutch--read-sqlite-file-params))))
    (list :name (clutch--ad-hoc-console-name params)
          :params params
          :ad-hoc t)))

(defun clutch--ad-hoc-console-name (params)
  "Return a display name for an ad hoc console using PARAMS."
  (if (eq (plist-get params :backend) 'sqlite)
      (format "SQLite: %s" (abbreviate-file-name (plist-get params :database)))
    (let* ((backend (or (clutch--backend-display-name-from-params params)
                        (format "%s" (plist-get params :backend))))
           (user (or (plist-get params :user) "?"))
           (host (or (plist-get params :host) "?"))
           (port (plist-get params :port))
           (database (or (plist-get params :database)
                         (plist-get params :sid))))
      (format "%s: %s@%s%s%s"
              backend
              user
              host
              (if port (format ":%s" port) "")
              (if database (format "/%s" database) "")))))

(defun clutch--ad-hoc-console-target (&optional params)
  "Return an ad hoc query console target for PARAMS."
  (let ((params (or params (clutch--read-manual-connection-params t))))
    (list :name (clutch--ad-hoc-console-name params)
          :params params
          :ad-hoc t)))

(defun clutch--read-query-console-choice (names)
  "Read a query console choice from NAMES; no match means new."
  (let ((read-choice
         (lambda ()
           (let ((completion-extra-properties
                  '(:affixation-function clutch--connection-candidates-affixation)))
             (completing-read "Console: "
                              names nil nil nil nil "")))))
    (if (boundp 'vertico-preselect)
        (cl-progv '(vertico-preselect) '(prompt)
          (funcall read-choice))
      (funcall read-choice))))

(defun clutch--read-query-console-target ()
  "Read a saved connection name or an ad hoc connection target."
  (clutch--ensure-clutch-loaded)
  (let* ((names (mapcar #'car clutch-connection-alist))
         (choice (if names
                     (clutch--read-query-console-choice names)
                   "")))
    (cond
     ((string= choice "")
      (clutch--ad-hoc-console-target))
     ((member choice names)
      choice)
     (t
      (clutch--ad-hoc-console-target)))))

(defun clutch--console-yank-cleanup ()
  "Clean whitespace in the just-pasted region of a query console.
Only runs when `clutch-console-yank-cleanup' is non-nil, the current
buffer is a query console, and the last command was a yank variant."
  (when (and clutch-console-yank-cleanup
             clutch--console-name
             (memq this-command '(yank yank-pop clipboard-yank)))
    (let ((beg (region-beginning))
          (end (region-end)))
      (when (< beg end)
        (whitespace-cleanup-region beg end)))))

(defun clutch--open-query-console
    (name params &optional ad-hoc-params source-default-directory)
  "Open or switch to query console NAME using PARAMS.
AD-HOC-PARAMS, when non-nil, are stored for console-local reconnects.
SOURCE-DEFAULT-DIRECTORY is the buffer directory that initiated the command."
  (let* ((params (clutch--prepare-connection-origin-params
                  params source-default-directory))
         (ad-hoc-params (and ad-hoc-params params))
         (product (clutch--effective-sql-product params))
         (storage-name (clutch--console-persistence-name name params))
         (existing (clutch--find-console-buffer name storage-name)))
    (if (and existing
             (buffer-local-value 'clutch-connection existing)
             (clutch--connection-alive-p
              (buffer-local-value 'clutch-connection existing)))
        (progn
          (with-current-buffer existing
            (let ((existing-params (or clutch--connection-params params)))
              (setq-local clutch--console-name name)
              (setq-local clutch--console-storage-name storage-name)
              (setq-local clutch--console-ad-hoc-params
                          (and ad-hoc-params existing-params))
              (clutch--bind-connection-context
               clutch-connection existing-params product))
            (clutch--update-console-buffer-name))
          (select-window
           (or (clutch--console-window-for existing) (selected-window)))
          (switch-to-buffer existing))
      (let* ((conn (if (and existing
                            (buffer-local-value 'clutch-connection existing)
                            (not (clutch--connection-alive-p
                                  (buffer-local-value 'clutch-connection existing))))
                       (with-current-buffer existing
                         (clutch--build-conn params))
                     (clutch--build-conn params)))
             (buf (or existing
                      (get-buffer-create (clutch--console-buffer-base-name name))))
             (is-new (zerop (buffer-size buf))))
        (select-window (or (clutch--console-window-for buf) (selected-window)))
        (switch-to-buffer buf)
        (unless (derived-mode-p 'clutch-mode)
          (clutch-mode))
        (setq-local clutch--console-name name)
        (setq-local clutch--console-storage-name storage-name)
        (setq-local clutch--console-ad-hoc-params ad-hoc-params)
        (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
        (when is-new
          (let* ((coding-system-for-read 'utf-8)
                 (file (clutch--console-file storage-name))
                 (legacy-file (clutch--console-file name))
                 (read-file (if (or (file-readable-p file)
                                    (equal file legacy-file))
                                file
                              legacy-file)))
            (when (file-readable-p read-file)
              (insert-file-contents read-file))))
        (clutch--activate-current-buffer-connection conn params product)
        (clutch--update-console-buffer-name)))))

;;;###autoload
(defun clutch-query-sqlite-file (file)
  "Open a query console for SQLite database FILE."
  (interactive
   (list (plist-get (clutch--read-sqlite-file-params) :database)))
  (let ((source-default-directory default-directory)
        (params (list :backend 'sqlite
                      :database (clutch--normalize-sqlite-database-file file))))
    (pcase-let ((`(:name ,name :params ,target-params :ad-hoc t)
                 (clutch--sqlite-file-console-target params)))
      (clutch--open-query-console
       name target-params target-params source-default-directory))))

;;;###autoload
(defun clutch-query-console (target)
  "Open or switch to a query console for TARGET.
Creates a dedicated buffer *clutch: TARGET* with `clutch-mode' enabled
and connects automatically if not already connected.
Repeated calls with the same saved connection or ad hoc target switch to the
existing buffer.
When called from outside a clutch buffer, reuses any visible clutch
window rather than replacing the current window."
  (interactive (list (clutch--read-query-console-target)))
  (let ((source-default-directory default-directory))
    (if (stringp target)
        (let ((params (or (clutch--saved-connection-params target)
                          (clutch--user-error "No saved connection named %s" target))))
          (clutch--open-query-console
           target params nil source-default-directory))
      (let ((name (plist-get target :name))
            (params (plist-get target :params)))
        (clutch--open-query-console
         name params params source-default-directory)))))

;;;###autoload
(defun clutch-switch-console ()
  "Switch to an open clutch query console using `completing-read'."
  (interactive)
  (let ((consoles (cl-loop for buf in (buffer-list)
                            when (string-prefix-p "*clutch: " (buffer-name buf))
                            collect (buffer-name buf))))
    (if consoles
        (switch-to-buffer (completing-read "Switch to console: " consoles nil t))
      (clutch--user-error "No clutch consoles open.  Use M-x clutch-query-console"))))

;;;; Value conversion

(defun clutch--column-names (columns)
  "Extract column names from COLUMNS as a list of strings.
Handles the case where the driver returns non-string names
\(e.g., SELECT 1 produces an integer column name)."
  (mapcar (lambda (c)
            (let ((name (plist-get c :name)))
              (if (stringp name) name (format "%s" name))))
          columns))

(defun clutch--value-to-literal (val)
  "Convert Elisp VAL to a SQL literal string.
nil → \"NULL\", numbers unquoted, strings escaped."
  (cond
   ((null val) "NULL")
   ((numberp val) (number-to-string val))
   ((stringp val) (clutch-db-escape-literal clutch-connection val))
   (t (clutch-db-escape-literal clutch-connection
                                   (clutch--format-value val)))))

(defun clutch--json-false-value-p (val)
  "Return non-nil when VAL represents a parsed JSON false sentinel."
  (or (eq val :false)
      ;; JDBC uses a private false sentinel so it can distinguish JSON false
      ;; from SQL NULL when decoding metadata payloads.
      (and (symbolp val)
           (string= (symbol-name val) "clutch-jdbc-json-false"))))

(defun clutch--json-value-to-string (val)
  "Convert VAL to valid JSON text suitable for JSON editing and viewing."
  (cond
   ((null val)
    "null")
   ((and (stringp val)
         (fboundp 'json-serialize)
         (fboundp 'json-parse-string))
    (condition-case nil
        (clutch--json-serialize-text (json-parse-string val))
      (error (clutch--json-serialize-text val))))
   ((clutch--json-false-value-p val)
    "false")
   ((and (fboundp 'json-serialize)
         (or (numberp val)
             (eq val t)
             (hash-table-p val)
             (vectorp val)
             (listp val)))
    (condition-case nil
        (clutch--json-serialize-text
         (if (clutch--json-false-value-p val) :false val))
      (error (clutch--format-value val))))
   (t (clutch--format-value val))))

;;;; Result source detection

(defun clutch-result--table-from-sql (sql)
  "Extract the first table name from the FROM clause of SQL.
Handles backtick, double-quote, and unquoted identifiers, with an
optional schema prefix (schema.table).  Returns a string or nil."
  (let ((case-fold-search t))
    (cond
     ;; backtick-quoted: FROM `schema`.`table` or FROM `table`
     ((string-match "\\bFROM\\s-+\\(?:`[^`]+`\\.\\)?`\\([^`]+\\)`" sql)
      (match-string 1 sql))
     ;; double-quoted: FROM \"schema\".\"table\" or FROM \"table\"
     ((string-match "\\bFROM\\s-+\\(?:\"[^\"]+\"\\.\\)?\"\\([^\"]+\\)\"" sql)
      (match-string 1 sql))
     ;; unquoted (including CJK): FROM schema.table or FROM table
     ((string-match "\\bFROM\\s-+\\(?:[^[:space:],();.]+\\.\\)?\\([^[:space:],();]+\\)" sql)
      (match-string 1 sql)))))

(defun clutch-result--detect-table ()
  "Try to detect the source table from the last query.
Returns table name string or nil."
  (when clutch--last-query
    (clutch-result--table-from-sql clutch--last-query)))

(defun clutch-result--source-table ()
  "Return the current result source table from state or SQL detection."
  (or clutch--result-source-table
      (clutch-result--detect-table)))

(defun clutch--result-source-table-or-user-error (op)
  "Return source table for current result, or signal user-error for OP."
  (or (clutch-result--source-table)
      (clutch--user-error "Cannot %s: source table cannot be detected (multi-table or derived query)"
                  op)))

(defconst clutch--row-identity-hidden-prefix "clutch__rid_"
  "Prefix used for hidden row identity result columns.")

(defun clutch--row-identity-hidden-aliases (count)
  "Return COUNT hidden row identity column aliases."
  (cl-loop for i below count
           collect (format "%s%d" clutch--row-identity-hidden-prefix i)))

(defun clutch--row-identity-key-expressions (conn candidate)
  "Return SELECT expressions for key CANDIDATE on CONN."
  (mapcar (lambda (column)
            (clutch-db-escape-identifier conn column))
          (plist-get candidate :columns)))

(defun clutch--row-identity-select-expressions (conn candidate)
  "Return hidden SELECT expressions for CANDIDATE on CONN."
  (or (plist-get candidate :select-expressions)
      (clutch--row-identity-key-expressions conn candidate)))

(defun clutch--row-identity-top-level-comma-p (sql start end)
  "Return non-nil when SQL has a top-level comma between START and END."
  (let ((pos start)
        (depth 0)
        found)
    (while (and (< pos end) (not found))
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (setq pos (min skip end))
        (let ((ch (aref sql pos)))
          (cond
           ((= ch ?\() (cl-incf depth) (cl-incf pos))
           ((= ch ?\)) (cl-decf depth) (cl-incf pos))
           ((and (= depth 0) (= ch ?,)) (setq found t))
           (t (cl-incf pos))))))
    found))

(defun clutch--row-identity-single-from-table-p (sql)
  "Return non-nil when SQL has a single top-level FROM table segment."
  (when-let* ((from-pos (clutch-db-sql-find-top-level-clause sql "FROM")))
    (pcase-let ((`(,start ,end)
                 (clutch--row-identity-from-body-range sql from-pos)))
      (let ((from-segment (string-trim-left (substring sql start end))))
        (and (not (string-prefix-p "(" from-segment))
             (not (clutch--row-identity-top-level-comma-p sql start end)))))))

(defun clutch--row-identity-direct-select-p (sql)
  "Return non-nil when SQL starts with a direct SELECT."
  (let ((case-fold-search t))
    (string-match-p "\\`\\s-*select\\b" sql)))

(defconst clutch--row-identity-aggregate-functions
  '("ARRAY_AGG" "AVG" "BIT_AND" "BIT_OR" "BIT_XOR" "BOOL_AND" "BOOL_OR"
    "COLLECT" "CORR" "COUNT" "COVAR_POP" "COVAR_SAMP" "EVERY"
    "GROUP_CONCAT" "JSON_AGG" "JSON_OBJECT_AGG" "JSONB_AGG"
    "JSONB_OBJECT_AGG" "LISTAGG" "MAX" "MEDIAN" "MIN"
    "PERCENTILE_CONT" "PERCENTILE_DISC" "REGR_COUNT" "STDDEV"
    "STDDEV_POP" "STDDEV_SAMP" "STRING_AGG" "SUM" "VAR_POP"
    "VAR_SAMP" "VARIANCE" "XMLAGG")
  "Aggregate function names that make row identity injection unsafe.")

(defun clutch--row-identity-ident-char-p (char)
  "Return non-nil when CHAR can be part of a SQL identifier."
  (and char
       (or (and (>= char ?A) (<= char ?Z))
           (and (>= char ?a) (<= char ?z))
           (and (>= char ?0) (<= char ?9))
           (= char ?_)
           (= char ?$))))

(defun clutch--row-identity-skip-space (sql pos)
  "Return position after whitespace in SQL starting at POS."
  (let ((len (length sql)))
    (while (and (< pos len)
                (memq (aref sql pos) '(?\s ?\t ?\r ?\n)))
      (cl-incf pos))
    pos))

(defun clutch--row-identity-looking-at-keyword-p (sql pos keyword)
  "Return non-nil when SQL at POS starts with KEYWORD as a token."
  (let* ((len (length sql))
         (end (+ pos (length keyword)))
         (case-fold-search t))
    (and (<= end len)
         (string= (upcase (substring sql pos end)) keyword)
         (not (clutch--row-identity-ident-char-p
               (and (< end len) (aref sql end)))))))

(defun clutch--row-identity-matching-paren-pos (sql open-pos)
  "Return the matching close-paren position for OPEN-POS in SQL, or nil."
  (let ((pos open-pos)
        (len (length sql))
        (depth 0)
        close)
    (while (and (< pos len) (not close))
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (setq pos skip)
        (let ((char (aref sql pos)))
          (cond
           ((= char ?\()
            (cl-incf depth)
            (cl-incf pos))
           ((= char ?\))
            (cl-decf depth)
            (if (= depth 0)
                (setq close pos)
              (cl-incf pos)))
           (t
            (cl-incf pos))))))
    close))

(defun clutch--row-identity-window-aggregate-call-p (sql open-pos)
  "Return non-nil when SQL has a window aggregate call at OPEN-POS."
  (when-let* ((close-pos (clutch--row-identity-matching-paren-pos sql open-pos)))
    (let ((pos (clutch--row-identity-skip-space sql (1+ close-pos))))
      (when (clutch--row-identity-looking-at-keyword-p sql pos "FILTER")
        (setq pos (clutch--row-identity-skip-space
                   sql (+ pos (length "FILTER"))))
        (when (and (< pos (length sql))
                   (= (aref sql pos) ?\())
          (when-let* ((filter-close
                       (clutch--row-identity-matching-paren-pos sql pos)))
            (setq pos (clutch--row-identity-skip-space
                       sql (1+ filter-close))))))
      (clutch--row-identity-looking-at-keyword-p sql pos "OVER"))))

(defun clutch--row-identity-select-list-has-aggregate-p (sql)
  "Return non-nil when SQL's outer SELECT list contains a non-window aggregate."
  (let ((case-fold-search t))
    (when (string-match "\\`\\s-*select\\b" sql)
      (let* ((start (match-end 0))
             (end (clutch-db-sql-find-top-level-clause sql "FROM")))
        (when end
          (let ((select-list (substring sql start end))
                (pos 0)
                (len (- end start)))
            (catch 'aggregate
              (while (< pos len)
                (if-let* ((skip (clutch-db-sql-skip-literal-or-comment
                                 select-list pos)))
                    (setq pos skip)
                  (let ((char (aref select-list pos)))
                    (cond
                     ((= char ?\()
                      (let ((inner (clutch--row-identity-skip-space
                                    select-list (1+ pos))))
                        (if (or (clutch--row-identity-looking-at-keyword-p
                                 select-list inner "SELECT")
                                (clutch--row-identity-looking-at-keyword-p
                                 select-list inner "WITH"))
                            (setq pos (if-let* ((close (clutch--row-identity-matching-paren-pos
                                                        select-list pos)))
                                          (1+ close)
                                        len))
                          (cl-incf pos))))
                     ((clutch--row-identity-ident-char-p char)
                      (let ((ident-start pos))
                        (while (and (< pos len)
                                    (clutch--row-identity-ident-char-p
                                     (aref select-list pos)))
                          (cl-incf pos))
                        (let* ((ident (upcase (substring select-list ident-start pos)))
                               (call-pos (clutch--row-identity-skip-space
                                          select-list pos)))
                          (when (and (member ident clutch--row-identity-aggregate-functions)
                                     (< call-pos len)
                                     (= (aref select-list call-pos) ?\()
                                     (not (clutch--row-identity-window-aggregate-call-p
                                           select-list call-pos)))
                            (throw 'aggregate t)))))
                     (t
                      (cl-incf pos))))))
              nil)))))))

(defun clutch--row-identity-augmentable-sql-p (sql table)
  "Return non-nil when SQL for TABLE may receive hidden identity columns."
  (and table
       (clutch--row-identity-direct-select-p sql)
       (clutch--row-identity-single-from-table-p sql)
       (not (clutch--row-identity-select-list-has-aggregate-p sql))
       (not (clutch-db-sql-find-top-level-clause sql "DISTINCT"))
       (not (clutch-db-sql-find-top-level-clause sql "GROUP"))
       (not (clutch-db-sql-find-top-level-clause sql "HAVING"))
       (not (clutch-db-sql-find-top-level-clause sql "UNION"))
       (not (clutch-db-sql-find-top-level-clause sql "INTERSECT"))
       (not (clutch-db-sql-find-top-level-clause sql "EXCEPT"))
       (not (clutch-db-sql-find-top-level-clause sql "JOIN"))))

(defun clutch--row-identity-from-body-range (sql from-pos)
  "Return `(START END)' for the top-level FROM body in SQL after FROM-POS."
  (let ((start (+ from-pos 4)))
    (list start
          (or (cl-loop for clause in '("WHERE" "GROUP" "HAVING" "ORDER\\s-+BY"
                                       "LIMIT" "OFFSET" "FETCH" "UNION"
                                       "INTERSECT" "EXCEPT")
                       for pos = (clutch-db-sql-find-top-level-clause
                                  sql clause start)
                       when pos minimize pos)
              (length sql)))))

(defun clutch--row-identity-from-body-parts (body)
  "Return `(TABLE ALIAS)' from simple FROM BODY."
  (let ((case-fold-search t)
        (pattern (concat "\\`\\s-*"
                         "\\(\\(?:\\(?:\"[^\"]+\"\\.\\)?\"[^\"]+\"\\|[^[:space:]]+\\)\\)"
                         "\\(?:\\s-+\\(?:AS\\s-+\\)?\\(\"[^\"]+\"\\|[^[:space:]]+\\)\\)?"
                         "\\s-*\\'")))
    (and (string-match pattern body)
         (list (match-string 1 body)
               (match-string 2 body)))))

(defun clutch--row-identity-table-qualifier (table)
  "Return the exposed table qualifier from TABLE."
  (cond
   ((string-match "\\.\\(\"[^\"]+\"\\)\\'" table)
    (match-string 1 table))
   ((string-match "\\.\\([^.\"]+\\)\\'" table)
    (match-string 1 table))
   (t table)))

(defun clutch--row-identity-oracle-star-rewrite (sql from-pos)
  "Return Oracle-safe SQL and star qualifier for SELECT * SQL before FROM-POS.
The return value is `(SQL QUALIFIER)'.  QUALIFIER is nil when SQL did not need
rewriting."
  (let ((select-list (string-trim (substring sql 0 from-pos))))
    (when (string-match-p "\\`\\s-*SELECT\\s-+\\*\\s-*\\'" select-list)
      (pcase-let* ((`(,body-start ,body-end)
                    (clutch--row-identity-from-body-range sql from-pos))
                   (body (substring sql body-start body-end))
                   (parts (clutch--row-identity-from-body-parts body)))
        (when parts
          (let* ((table (car parts))
                 (alias (cadr parts))
                 (qualifier (or alias
                                (clutch--row-identity-table-qualifier table))))
            (list sql (format "%s.*" qualifier))))))))

(defun clutch--row-identity-inject-select-list (conn sql expressions aliases)
  "Return SQL with hidden identity EXPRESSIONS inserted using ALIASES.
CONN supplies identifier escaping for the hidden aliases."
  (let ((sql (string-trim-right
              (replace-regexp-in-string ";\\s-*\\'" "" sql))))
    (if-let* ((from-pos (clutch-db-sql-find-top-level-clause sql "FROM")))
      (let* ((oracle-star
              (and (clutch--connection-oracle-jdbc-p conn)
                   (clutch--row-identity-oracle-star-rewrite sql from-pos)))
             (effective-sql (or (car oracle-star) sql))
             (select-head
              (if-let* ((qualified-star (cadr oracle-star)))
                  (format "SELECT %s" qualified-star)
                (string-trim-right (substring effective-sql 0 from-pos))))
             (hidden (mapconcat
                      #'identity
                      (cl-mapcar
                       (lambda (expr alias)
                         (format "%s AS %s"
                                 expr
                                 (clutch-db-escape-identifier conn alias)))
                       expressions aliases)
                      ", ")))
        (concat select-head
                ", " hidden " "
                (string-trim-left (substring effective-sql from-pos))))
      sql)))

(defun clutch--schema-cache-contains-table-p (schema table)
  "Return non-nil when SCHEMA contains TABLE, case-insensitively."
  (and (hash-table-p schema)
       (cl-loop for cached being the hash-keys of schema
                thereis (and (stringp cached)
                             (string-equal (downcase cached)
                                           (downcase table))))))

(defun clutch--schema-cache-definitely-misses-table-p (conn table)
  "Return non-nil when CONN's ready schema cache does not contain TABLE."
  (condition-case nil
      (when (and conn
                 (stringp table)
                 (fboundp 'clutch--schema-status-entry)
                 (fboundp 'clutch--schema-for-connection))
        (let ((entry (clutch--schema-status-entry conn)))
          (when (eq (plist-get entry :state) 'ready)
            (let ((schema (clutch--schema-for-connection conn)))
              (and schema
                   (not (clutch--schema-cache-contains-table-p
                         schema table)))))))
    (error nil)))

(defun clutch--prepare-row-identity-query (conn sql)
  "Return a row identity preparation plist for executing SQL on CONN.
The returned plist contains :sql, :table, :candidate, :hidden-aliases, and
:augmented.  If no identity candidate is available, :sql is the original SQL."
  (let* ((table (clutch-result--table-from-sql sql))
         (candidates (and table
                          (not (clutch--schema-cache-definitely-misses-table-p
                                conn table))
                          (condition-case nil
                              (clutch-db-row-identity-candidates conn table)
                            (clutch-db-error nil))))
         (candidate (car candidates))
         (expressions (and candidate
                           (clutch--row-identity-select-expressions
                            conn candidate)))
         (aliases (and expressions
                       (clutch--row-identity-hidden-aliases
                        (length expressions))))
         (augment-p (and candidate expressions
                         (clutch--row-identity-augmentable-sql-p sql table))))
    (list :sql (if augment-p
                   (clutch--row-identity-inject-select-list
                    conn sql expressions aliases)
                 sql)
          :table table
          :candidate candidate
          :candidates candidates
          :hidden-aliases (and augment-p aliases)
          :augmented (and augment-p t))))

(defun clutch--row-identity-column-indices (columns names)
  "Return column indices in COLUMNS for NAMES, or nil if any is absent."
  (let ((col-names (clutch--column-names columns))
        indices)
    (catch 'missing
      (dolist (name names (nreverse indices))
        (if-let* ((idx (cl-position name col-names :test #'string=)))
            (push idx indices)
          (throw 'missing nil))))))

(defun clutch--finalize-row-identity (prep columns)
  "Return finalized row identity metadata for PREP and result COLUMNS."
  (when-let* ((candidate (plist-get prep :candidate)))
    (let* ((aliases (plist-get prep :hidden-aliases))
           (indices (if aliases
                        (clutch--row-identity-column-indices columns aliases)
                      (clutch--row-identity-column-indices
                       columns (plist-get candidate :columns))))
           (source-indices
            (and (eq (plist-get candidate :kind) 'primary-key)
                 (clutch--row-identity-column-indices
                  columns (plist-get candidate :columns)))))
      (when indices
        (append
         (list :table (plist-get prep :table)
               :indices indices
               :source-indices source-indices
               :hidden-aliases aliases)
         candidate)))))

(defun clutch--apply-row-identity-column-metadata (columns prep)
  "Return COLUMNS with hidden identity aliases from PREP marked hidden."
  (let ((aliases (plist-get prep :hidden-aliases)))
    (if (not aliases)
        columns
      (mapcar
       (lambda (column)
         (let ((name (plist-get column :name)))
           (if (member name aliases)
               (plist-put (copy-sequence column) :hidden t)
             column)))
       columns))))

;;;; Result display

(defun clutch--result-window ()
  "Return the window currently showing a clutch result buffer, or nil.
Searches all windows on the current frame."
  (cl-find-if (lambda (w)
                (string-prefix-p "*clutch-result:"
                                 (buffer-name (window-buffer w))))
              (window-list nil 'no-minibuf)))

(defun clutch--show-result-buffer (buf)
  "Display BUF in the result window slot.
Reuses the existing result window when one is visible, replacing its
buffer in place.  Creates a new window below `clutch--source-window'
when no result window exists yet."
  (let ((result-win (clutch--result-window)))
    (if result-win
        (progn
          (set-window-buffer result-win buf)
          (select-window result-win))
      (pop-to-buffer buf `(display-buffer-in-direction
                           (window . ,(or clutch--source-window
                                          (selected-window)))
                           (direction . below)
                           (window-height . ,clutch-result-window-height))))))

(defun clutch--result-buffer-name ()
  "Return the result buffer name based on current connection.
Uses the full connection key so each console gets its own result buffer."
  (if (clutch--connection-alive-p clutch-connection)
      (format "*clutch-result: %s*" (clutch--connection-key clutch-connection))
    "*clutch-result: results*"))

;;;; SQL pagination helpers

(defun clutch--sql-has-limit-p (sql)
  "Return non-nil if SQL has a top-level LIMIT clause."
  (clutch-db-sql-has-top-level-limit-p sql))

(defun clutch--sql-has-page-tail-p (sql)
  "Return non-nil if SQL has a top-level LIMIT or OFFSET clause."
  (or (clutch-db-sql-has-top-level-limit-p sql)
      (clutch-db-sql-has-top-level-offset-p sql)))

(defun clutch--sql-derived-table-alias (alias &optional conn)
  "Return a derived-table alias clause for ALIAS on CONN.
Oracle does not accept AS before a subquery alias."
  (if (clutch--connection-oracle-jdbc-p (or conn clutch-connection))
      (replace-regexp-in-string "\\`_+" "" alias)
    (format "AS %s" alias)))

(defun clutch--build-paged-sql (base-sql page-num page-size
                                         &optional order-by page-offset)
  "Build a paged SQL query wrapping BASE-SQL.
PAGE-NUM is 0-based, PAGE-SIZE is the row limit.
ORDER-BY is a cons (COL-NAME . DIRECTION) or nil.
PAGE-OFFSET, when non-nil, overrides the offset derived from PAGE-NUM.
If BASE-SQL already has a top-level LIMIT/OFFSET, pagination and sorting
operate on that result set via an outer wrapper query."
  (let ((effective-base
         (if (clutch--sql-has-page-tail-p base-sql)
             (format "SELECT * FROM (%s) %s"
                     (string-trim-right
                      (replace-regexp-in-string ";\\s-*\\'" "" base-sql))
                     (clutch--sql-derived-table-alias "_clutch_page"))
           base-sql)))
    (clutch-db-build-paged-sql clutch-connection effective-base
                               page-num page-size order-by page-offset)))

(defun clutch--page-current-from-offset (page-offset page-size)
  "Return the zero-based logical page number for PAGE-OFFSET and PAGE-SIZE."
  (if (and page-size (> page-size 0))
      (floor (max 0 page-offset) page-size)
    0))

(defun clutch--split-page-lookahead-rows (rows page-size)
  "Return (VISIBLE-ROWS . HAS-MORE) from ROWS after PAGE-SIZE lookahead trimming."
  (let ((has-more (> (length rows) page-size)))
    (cons (if has-more
              (cl-subseq rows 0 page-size)
            rows)
          has-more)))

(defun clutch--install-result-page-state
    (columns rows elapsed page-num &optional row-identity-prep
             page-offset page-has-more)
  "Install buffer-local state for a rendered result page.
COLUMNS, ROWS, ELAPSED, and PAGE-NUM describe the page.  ROW-IDENTITY-PREP
describes hidden row identity columns, PAGE-OFFSET overrides the derived SQL
offset, and PAGE-HAS-MORE records one-row lookahead.  Return column names."
  (let* ((column-defs (clutch--apply-row-identity-column-metadata
                       columns row-identity-prep))
         (row-identity (clutch--finalize-row-identity
                        row-identity-prep column-defs))
         (column-names (clutch--column-names column-defs))
         (cached-pk-indices
          (and (eq (plist-get row-identity :kind) 'primary-key)
               (or (plist-get row-identity :source-indices)
                   (plist-get row-identity :indices))))
         (offset (or page-offset (* page-num clutch-result-max-rows))))
    (setq-local clutch--result-columns column-names
                clutch--result-column-defs column-defs
                clutch--row-identity row-identity
                clutch--result-rows rows
                clutch--page-current page-num
                clutch--page-offset offset
                clutch--page-has-more page-has-more
                clutch--cached-pk-indices cached-pk-indices
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--query-elapsed elapsed
                clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--column-widths
                (clutch--compute-column-widths column-names rows column-defs))
    column-names))

(defun clutch--update-page-state (columns rows elapsed page-num
                                          &optional row-identity-prep
                                          page-offset page-has-more)
  "Update buffer-local state for a new page of results.
COLUMNS, ROWS, ELAPSED, and PAGE-NUM describe the new page.
ROW-IDENTITY-PREP describes any hidden row identity columns in COLUMNS.
PAGE-OFFSET is the zero-based row offset for ROWS, and PAGE-HAS-MORE records
whether a lookahead row proved there are later rows."
  (clutch--install-result-page-state
   columns rows elapsed page-num row-identity-prep page-offset page-has-more))

(defun clutch--execute-page (page-num &optional page-offset)
  "Execute the query for PAGE-NUM and refresh the result buffer display.
Uses the current effective result SQL, including active WHERE filters.
PAGE-OFFSET, when non-nil, overrides PAGE-NUM for last-window pagination.
Signals an error if pagination is not available."
  (let* ((source-buffer (current-buffer))
         (effective-sql (clutch-result--effective-query))
         (page-size clutch-result-max-rows)
         (offset (or page-offset (* page-num page-size)))
         (fetch-size (1+ page-size)))
    (unless effective-sql
      (clutch--user-error "Pagination not available for this query"))
    (clutch--ensure-connection)
    (when (and (or clutch--pending-edits
                   clutch--pending-deletes
                   clutch--pending-inserts)
               (not (yes-or-no-p "Discard staged changes and change page? ")))
      (clutch--user-error "Page change cancelled"))
    (let* ((row-identity-prep
            (clutch--prepare-row-identity-query clutch-connection effective-sql))
           (identity-sql (plist-get row-identity-prep :sql))
           (paged-sql (clutch--build-paged-sql
                       identity-sql page-num fetch-size clutch--order-by
                       offset))
           (start (float-time))
           (result (condition-case err
                       (clutch--run-db-query clutch-connection paged-sql)
                     (clutch-db-error
                      (let* ((failure
                              (clutch--remember-execute-error
                               source-buffer
                               clutch-connection
                               effective-sql
                               err
                               (list :page-num page-num
                                     :page-offset offset
                                     :paged-sql
                                     (clutch--debug-sql-preview paged-sql))))
                             (summary (cdr failure)))
                        (clutch--user-error "%s" (clutch--debug-workflow-message summary))))))
           (elapsed (- (float-time) start))
           (page (clutch--split-page-lookahead-rows
                  (clutch-db-result-rows result) page-size))
           (rows (car page))
           (has-more (cdr page)))
      (clutch--update-page-state
       (clutch-db-result-columns result) rows elapsed
       page-num
       row-identity-prep offset has-more)
      (clutch--refresh-display)
      (message "Rows %s loaded (%s, %s row%s)"
               (clutch--message-count
                (format "%d-%d" (if rows (1+ offset) 0)
                        (+ offset (length rows))))
               (clutch--message-literal (clutch--format-elapsed elapsed))
               (clutch--message-count (length rows))
               (if (= (length rows) 1) "" "s")))))

(defun clutch--execute-page-at-offset (page-offset &optional page-num)
  "Execute the result page whose first row starts at PAGE-OFFSET.
PAGE-NUM records the logical page index for navigation when provided."
  (clutch--execute-page
   (or page-num
       (clutch--page-current-from-offset page-offset clutch-result-max-rows))
   page-offset))

;;;; Query execution engine

(defun clutch--strip-leading-comments (sql)
  "Strip leading SQL comments and whitespace from SQL.
Handles single-line (--) and multi-line (/* */) comments."
  (let ((s (string-trim-left sql)))
    (while (or (string-prefix-p "--" s)
               (string-prefix-p "/*" s))
      (setq s (string-trim-left
               (cond
                ((string-prefix-p "--" s)
                 (if-let* ((nl (string-search "\n" s)))
                     (substring s (1+ nl))
                   ""))
                ((string-prefix-p "/*" s)
                 (if-let* ((end (string-search "*/" s)))
                     (substring s (+ end 2))
                   ""))))))
    s))

(defun clutch--destructive-query-p (sql)
  "Return non-nil if SQL is a destructive operation.
Leading SQL comments are stripped before checking."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (string-match-p "\\`\\(?:DELETE\\|DROP\\|TRUNCATE\\|ALTER\\)\\b"
                    (upcase trimmed))))

(defun clutch--schema-affecting-query-p (sql)
  "Return non-nil if SQL is likely to invalidate cached schema."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (string-match-p "\\`\\(?:CREATE\\|ALTER\\|DROP\\|TRUNCATE\\|RENAME\\)\\b"
                    (upcase trimmed))))

(defun clutch--sql-normalize-for-rewrite (sql)
  "Return SQL trimmed for rewrite operations."
  (string-trim-right
   (replace-regexp-in-string ";\\s-*\\'" ""
                             (clutch--strip-leading-comments sql))))

(defun clutch--sql-main-op-keyword (sql)
  "Return main top-level operation keyword for SQL, or nil."
  (let* ((normalized (clutch--sql-normalize-for-rewrite sql))
         (candidates
          (cl-loop for kw in '("UPDATE" "DELETE" "SELECT" "INSERT" "REPLACE" "MERGE")
                   for pos = (clutch-db-sql-find-top-level-clause normalized kw)
                   when pos collect (cons kw pos)))
         (first (car (sort candidates (lambda (a b) (< (cdr a) (cdr b)))))))
    (car-safe first)))

(defun clutch--risky-dml-p (sql)
  "Return non-nil when SQL is a top-level UPDATE/DELETE statement without WHERE."
  (let ((normalized (clutch--sql-normalize-for-rewrite sql))
        (main-op (clutch--sql-main-op-keyword sql)))
    (and (member main-op '("UPDATE" "DELETE"))
         (not (clutch-db-sql-find-top-level-clause normalized "WHERE")))))

(defun clutch--require-risky-dml-confirmation (sql)
  "Require explicit typed confirmation for risky DML SQL."
  (when (clutch--risky-dml-p sql)
    (let ((token (read-string "High-risk DML (no WHERE). Type YES to continue: ")))
      (unless (string= token "YES")
        (clutch--user-error "Query cancelled")))))

(defun clutch--select-query-p (sql)
  "Return non-nil if SQL returns a result set.
Matches SELECT, WITH, and introspection keywords (DESCRIBE, DESC,
SHOW, EXPLAIN) that also return tabular results.
Leading SQL comments are stripped before checking."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (string-match-p
     "\\`\\(?:SELECT\\|WITH\\|DESCRIBE\\|DESC\\|SHOW\\|EXPLAIN\\)\\b"
     (upcase trimmed))))

(defun clutch--init-result-state (conn sql columns rows elapsed
                                       &optional row-identity-prep
                                       page-offset page-has-more)
  "Initialize buffer-local state for a fresh query result.
CONN is the connection, SQL the original query, COLUMNS and ROWS
the result data, ELAPSED the query time.  ROW-IDENTITY-PREP describes any
hidden row identity columns in COLUMNS.  PAGE-OFFSET is the zero-based row
offset for ROWS, and PAGE-HAS-MORE records one-row lookahead.  Returns column
names."
  (setq-local clutch--last-query sql
              clutch--base-query sql
              clutch-connection conn
              clutch--sort-column nil
              clutch--sort-descending nil
              clutch--page-total-rows nil
              clutch--order-by nil
              clutch--aggregate-summary nil)
  (clutch--install-result-page-state
   columns rows elapsed 0 row-identity-prep page-offset page-has-more))

(defun clutch--query-debug-summary (result)
  "Return a compact summary string for RESULT."
  (if-let* ((rows (clutch-db-result-rows result)))
      (format "Returned %d row(s)" (length rows))
    (format "Affected %s row(s)"
            (or (clutch-db-result-affected-rows result) 0))))

(defun clutch--execute-source-buffer ()
  "Return the source BUFFER for the current execution."
  (if (window-live-p clutch--source-window)
      (window-buffer clutch--source-window)
    (current-buffer)))

(defun clutch--remember-execute-debug-event (connection phase sql
                                                        &optional buffer summary
                                                        elapsed context)
  "Record an execute debug event.
CONNECTION, PHASE, SQL, BUFFER, SUMMARY, ELAPSED, and CONTEXT describe it."
  (when clutch-debug-mode
    (apply #'clutch--remember-debug-event
           (append (when buffer (list :buffer buffer))
                   (list :connection connection
                         :op "execute"
                         :phase phase
                         :backend (clutch--backend-key-from-conn connection)
                         :sql sql)
                   (when summary (list :summary summary))
                   (when elapsed (list :elapsed elapsed))
                   (when context (list :context context))))))

(defun clutch--abort-execution-on-db-error (source-buffer connection sql err
                                                        &optional elapsed)
  "Record ERR for SQL on CONNECTION and abort execution from SOURCE-BUFFER.
ELAPSED, when non-nil, is the failed execution duration in seconds."
  (let* ((failure (clutch--remember-execute-error
                   source-buffer connection sql err))
         (message (car failure))
         (summary (cdr failure))
         (display (clutch--humanize-db-error-parts message))
         (display-summary (or (plist-get display :summary) summary))
         (hint (plist-get display :hint)))
    (when (and (buffer-live-p source-buffer)
               clutch--executing-sql-start
               clutch--executing-sql-end)
      (with-current-buffer source-buffer
        (clutch--mark-failed-sql-region
         clutch--executing-sql-start clutch--executing-sql-end
         display-summary)))
    (let ((buf (clutch--display-error-result
                connection sql display-summary message elapsed hint)))
      (when (and (buffer-live-p source-buffer)
                 (buffer-live-p buf))
        (with-current-buffer source-buffer
          (setq-local clutch--last-result-buffer buf))))
    (throw 'clutch--execution-aborted nil)))

(defun clutch--execute-select (sql connection)
  "Execute a SELECT SQL query with pagination on CONNECTION.
Returns the query result."
  (let* ((page-size clutch-result-max-rows)
         (fetch-size (1+ page-size))
         (row-identity-prep
          (clutch--prepare-row-identity-query connection sql))
         (identity-sql (plist-get row-identity-prep :sql))
         (paged-sql (clutch-db-build-paged-sql
                     connection identity-sql 0 fetch-size nil 0))
         (start (float-time))
         (source-buffer (clutch--execute-source-buffer))
         (_debug-start
          (clutch--remember-execute-debug-event
           connection "start" sql source-buffer nil nil
           (list :paged-sql (clutch--debug-sql-preview paged-sql))))
         (result (condition-case err
                     (clutch--run-db-query connection paged-sql)
                   (clutch-db-error
                    (clutch--abort-execution-on-db-error
                     source-buffer connection sql err
                     (- (float-time) start)))))
         (elapsed (- (float-time) start))
         (buf (get-buffer-create (clutch--result-buffer-name)))
         (columns (clutch--apply-row-identity-column-metadata
                   (clutch-db-result-columns result) row-identity-prep))
         (page (clutch--split-page-lookahead-rows
                (clutch-db-result-rows result) page-size))
         (rows (car page))
         (has-more (cdr page)))
    (clutch--remember-execute-debug-event
     connection "success" sql source-buffer
     (clutch--query-debug-summary result) elapsed)
    (with-current-buffer buf
      (clutch-result-mode)
      (let ((col-names (clutch--init-result-state
                        connection sql columns rows elapsed
                        row-identity-prep 0 has-more)))
        (clutch--load-fk-info)
        (when col-names
          (clutch--display-select-result col-names rows columns))))
    (when (buffer-live-p source-buffer)
      (with-current-buffer source-buffer
        (setq-local clutch--last-result-buffer buf)))
    (clutch--show-result-buffer buf)
    result))

(defun clutch--execute-dml (sql connection)
  "Execute a DML SQL query on CONNECTION and display results.
Returns the query result."
  (setq clutch--last-query sql)
  (let* ((start (float-time))
         (_debug-start
          (clutch--remember-execute-debug-event connection "start" sql))
         (result (condition-case err
                     (clutch--run-db-query connection sql)
                   (clutch-db-error
                    (clutch--abort-execution-on-db-error
                     (clutch--execute-source-buffer) connection sql err
                     (- (float-time) start)))))
         (elapsed (- (float-time) start)))
    (clutch--remember-execute-debug-event
     connection "success" sql nil (clutch--query-debug-summary result) elapsed)
    (when (clutch--schema-affecting-query-p sql)
      (if (clutch-db-eager-schema-refresh-p connection)
          (clutch--set-schema-status connection 'stale)
        (clutch--refresh-schema-cache-async connection)))
    (clutch--display-result result sql elapsed)
    result))

(defun clutch-result--effective-query ()
  "Return the effective SQL for the current result workflow.
Includes the active WHERE filter when one is applied."
  (let ((base (or clutch--base-query clutch--last-query)))
    (if (and base clutch--where-filter clutch--base-query)
        (clutch--apply-where base clutch--where-filter)
      base)))

(defun clutch--apply-where (sql filter)
  "Apply WHERE FILTER to SQL query string.
Wraps SQL as a derived table and applies FILTER in an outer WHERE.
This avoids brittle clause injection for CTE/UNION/subquery-heavy SQL."
  (clutch--sql-rewrite sql 'where filter))

(defun clutch--check-pending-changes ()
  "Prompt to discard staged changes in the result buffer, if any.
Signals `user-error' if the user declines."
  (when-let* ((result-buf (get-buffer (clutch--result-buffer-name))))
    (with-current-buffer result-buf
      (when (and (or clutch--pending-edits
                     clutch--pending-deletes
                     clutch--pending-inserts)
                 (not (yes-or-no-p "Discard staged changes and re-run query? ")))
        (clutch--user-error "Execution cancelled")))))

(defun clutch--abandon-query-connection (connection)
  "Drop CONNECTION after an unrecoverable query interruption."
  (when (clutch--connection-alive-p connection)
    (clutch-db-disconnect connection))
  (clutch--clear-tx-dirty connection)
  (when (eq connection clutch-connection)
    (setq clutch-connection nil)))

(defun clutch--handle-query-quit (connection)
  "Convert a raw quit on CONNECTION into an interrupt or disconnect."
  (let* ((source-buffer (current-buffer))
         (interrupted
          (condition-case err
              (and (clutch--connection-alive-p connection)
                   (clutch-db-interrupt-query connection))
            (clutch-db-error
             (let* ((msg (error-message-string err))
                    (summary (clutch--humanize-db-error msg)))
               (clutch--remember-buffer-query-error-details
                source-buffer connection nil err)
               (when clutch-debug-mode
                 (clutch--remember-debug-event
                  :buffer source-buffer
                  :connection connection
                  :op "cancel"
                  :phase "error"
                  :backend (clutch--backend-key-from-conn connection)
                  :summary summary))
               (message "Interrupt failed: %s"
                        (clutch--debug-workflow-message summary))
               nil)))))
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :connection connection
       :op "interrupt"
       :phase (if interrupted "success" "disconnect")
       :backend (clutch--backend-key-from-conn connection)
       :summary (if interrupted
                    "Interrupted running query without disconnecting"
                  "Interrupt recovery failed; connection abandoned")))
    (unless interrupted
      (clutch--abandon-query-connection connection))
    (signal 'clutch-query-interrupted nil)))

(defun clutch--execute (sql &optional conn)
  "Execute SQL on CONN (or current buffer connection).
Times execution and displays results.
For SELECT queries, applies pagination (LIMIT/OFFSET).
Prompts for confirmation on destructive operations."
  (clutch--ensure-connection)
  (setq-local clutch--buffer-error-details nil)
  (clutch--check-pending-changes)
  (let ((connection (or conn clutch-connection))
        (source-win (selected-window)))
    (when (clutch--destructive-query-p sql)
      (unless (yes-or-no-p
               (format "Execute destructive query?\n  %s\n\nProceed? "
                       (truncate-string-to-width (string-trim sql) 80)))
        (clutch--user-error "Query cancelled")))
    (clutch--require-risky-dml-confirmation sql)
    (setq clutch--executing-p t)
    (clutch--spinner-start)
    (clutch--update-mode-line)
    (redisplay t)
    (unwind-protect
        (condition-case nil
            (catch 'clutch--execution-aborted
              (let ((clutch--source-window source-win))
                (if (clutch--select-query-p sql)
                    (clutch--execute-select sql connection)
                  (clutch--execute-dml sql connection))))
          (quit
           (clutch--handle-query-quit connection)))
      (when (window-live-p source-win)
        (select-window source-win))
      (setq clutch--executing-p nil)
      (clutch--update-mode-line))))

(defun clutch--trim-sql-bounds (beg end)
  "Return (BEG . END) trimmed to non-whitespace between BEG and END."
  (save-excursion
    (goto-char beg)
    (skip-chars-forward " \t\r\n" end)
    (let ((tbeg (point)))
      (goto-char end)
      (skip-chars-backward " \t\r\n" beg)
      (let ((tend (point)))
        (when (< tbeg tend)
          (cons tbeg tend))))))

;;;; Error humanization

(defconst clutch--db-error-hints
  '(;; ClickHouse
    ("Lightweight updates are not supported"
     . "enable lightweight update: ALTER TABLE ... MODIFY SETTING enable_block_number_column = 1")
    ("Lightweight deletes? \\(?:is\\|are\\) not supported"
     . "enable lightweight delete: SET allow_experimental_lightweight_delete = 1")
    ;; Oracle
    ("No suitable driver found for jdbc:oracle:"
     . "Oracle JDBC driver not installed; run M-x clutch-jdbc-install-driver RET oracle")
    ("ORA-00942" . "table or view does not exist; check name and privileges")
    ("ORA-01031" . "insufficient privileges")
    ("ORA-00904" . "invalid column name")
    ;; MySQL
    ("Access denied for user" . "wrong username or password")
    ("Unknown column" . "column does not exist; check spelling")
    ;; PostgreSQL
    ("relation .* does not exist" . "table does not exist; check schema and name")
    ("permission denied" . "insufficient privileges")
    ;; General
    ("Connection refused" . "cannot connect; check host and port")
    ("connect timed out\\|connection timeout" . "connection timed out; check network and firewall"))
  "Alist of (REGEX . HINT) for known database error patterns.")

(defun clutch--humanize-db-error-parts (msg)
  "Return structured human-facing parts for database error MSG.
The returned plist contains :summary and, when available, :hint."
  (let ((cleaned (or msg ""))
        (case-fold-search t))
    ;; Strip "Database error: " prefix
    (setq cleaned (replace-regexp-in-string
                   "\\`Database error: " "" cleaned))
    ;; Strip ClickHouse queryId suffix: (queryId= uuid-value)
    (setq cleaned (replace-regexp-in-string
                   "[ \t]*(queryId=[^)]*)" "" cleaned))
    ;; Strip ClickHouse version suffix: (version N.N.N (official build))
    (setq cleaned (replace-regexp-in-string
                   "[ \t]*(version [^)]*([^)]*)[^)]*)" "" cleaned))
    ;; Strip Java stack trace (from first "at " frame onward)
    (when (string-match "\n[ \t]*at " cleaned)
      (setq cleaned (substring cleaned 0 (match-beginning 0))))
    ;; Normalize whitespace
    (setq cleaned (string-trim
                   (replace-regexp-in-string "[[:space:]\n\r]+" " " cleaned)))
    (list :summary cleaned
          :hint (cl-loop for (pattern . h) in clutch--db-error-hints
                         when (string-match-p pattern cleaned)
                         return h))))

(defun clutch--humanize-db-error (msg)
  "Return a user-friendly version of database error MSG.
Strips internal noise (queryId, version, stack traces) and appends
actionable hints for known error patterns."
  (let* ((parts (clutch--humanize-db-error-parts msg))
         (summary (plist-get parts :summary))
         (hint (plist-get parts :hint)))
    (if hint
        (format "%s [%s]" summary hint)
      summary)))

(defun clutch--debug-workflow-message (message)
  "Return MESSAGE annotated with the single debug-buffer workflow."
  (if (or (not message)
          (string-match-p (regexp-quote clutch-debug-buffer-name) message)
          (string-match-p
           "Run M-x clutch-jdbc-\\(ensure-agent\\|install-driver\\)" message))
      message
    (if clutch-debug-mode
        (format "%s See %s for details." message clutch-debug-buffer-name)
      (format "%s Enable clutch-debug-mode, reproduce the failure, then inspect %s."
              message clutch-debug-buffer-name))))

(defun clutch--make-buffer-query-error-details (connection sql err)
  "Return structured error details for a failed SQL execution on CONNECTION.
SQL is the user-visible statement.  ERR is the original signaled condition."
  (let* ((message (or (cadr err) (error-message-string err)))
         (details (copy-tree (nth 2 err)))
         (diag (copy-tree (plist-get details :diag)))
         (context (copy-tree (plist-get diag :context))))
    (unless details
      (setq details (list :summary (clutch--humanize-db-error message))))
    (when-let* ((backend (and connection
                              (condition-case nil
                                  (clutch--backend-key-from-conn connection)
                                (error nil)))))
      (unless (plist-member details :backend)
        (setq details (plist-put details :backend backend))))
    (unless (plist-get details :summary)
      (setq details (plist-put details :summary
                               (clutch--humanize-db-error message))))
    (unless diag
      (setq diag (list :raw-message message)))
    (unless (plist-get diag :raw-message)
      (setq diag (plist-put diag :raw-message message)))
    (unless (or (plist-get context :generated-sql)
                (plist-get context :sql))
      (setq context (plist-put context :sql sql)))
    (setq diag (plist-put diag :context context))
    (plist-put details :diag diag)))

(defun clutch--remember-buffer-query-error-details (buffer connection sql err)
  "Store the failed SQL execution details for BUFFER.
CONNECTION, SQL and ERR describe the failed query."
  (when (buffer-live-p buffer)
    (clutch--remember-problem-record
     :buffer buffer
     :connection connection
     :problem (clutch--make-buffer-query-error-details connection sql err))))

(defun clutch--remember-execute-error (buffer connection sql err &optional context)
  "Capture a failed execute path for BUFFER on CONNECTION.
SQL is the user-visible statement and ERR is the original condition.
Optional CONTEXT is merged into the debug event.  Return
`(MESSAGE . SUMMARY)' for the failure."
  (let* ((formatted (error-message-string err))
         (message (if (stringp (cadr err)) (cadr err) formatted))
         (summary (clutch--humanize-db-error formatted)))
    (clutch--remember-buffer-query-error-details buffer connection sql err)
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :buffer buffer
       :connection connection
       :op "execute"
       :phase "error"
       :backend (clutch--backend-key-from-conn connection)
       :summary summary
       :sql sql
       :context context))
    (cons message summary)))

(defun clutch--execute-and-mark (sql beg end &optional conn)
  "Execute SQL on CONN and mark BEG..END on success."
  (pcase-let* ((`(,trim-beg . ,trim-end)
                 (or (clutch--trim-sql-bounds beg end)
                     (cons beg end))))
    (when (let ((clutch--executing-sql-start trim-beg)
                (clutch--executing-sql-end trim-end))
            (clutch--execute sql conn))
      (clutch--mark-executed-sql-region beg end))))

;;;; Query-at-point detection

(defun clutch--query-bounds-at-point ()
  "Return the SQL statement bounds around point as (BEG . END)."
  (let ((delimiter "\\(;\\|^[[:space:]]*$\\)"))
    (cons (save-excursion
            (if (re-search-backward delimiter nil t)
                (match-end 0)
              (point-min)))
          (save-excursion
            (if (re-search-forward delimiter nil t)
                (match-beginning 0)
              (point-max))))))

(defun clutch--statement-delimited-buffer-p ()
  "Return non-nil when the current buffer contains a top-level semicolon.
Semicolons inside strings, line comments, and block comments do not count."
  (let* ((text (buffer-substring-no-properties (point-min) (point-max)))
         (len (length text))
         (in-string nil)
         (i 0)
         found)
    (while (and (< i len) (not found))
      (let ((ch (aref text i)))
        (cond
         (in-string
          (when (= ch in-string)
            (setq in-string nil)))
         ((= ch ?')  (setq in-string ?'))
         ((= ch ?\") (setq in-string ?\"))
         ((and (= ch ?-) (< (1+ i) len) (= (aref text (1+ i)) ?-))
          (while (and (< i len) (/= (aref text i) ?\n))
            (cl-incf i)))
         ((and (= ch ?/) (< (1+ i) len) (= (aref text (1+ i)) ?*))
          (cl-incf i 2)
          (while (and (< (1+ i) len)
                      (not (and (= (aref text i) ?*)
                                (= (aref text (1+ i)) ?/))))
            (cl-incf i))
          (cl-incf i))
         ((= ch ?\;)
          (setq found t))))
      (cl-incf i))
    found))

(defun clutch--statement-effective-offset (text offset)
  "Return insertion OFFSET in TEXT for semicolon-edge statement selection.
When point is on or immediately after a semicolon, treat it as belonging to the
preceding statement."
  (let ((len (length text)))
    (cond
     ((and (< offset len)
           (= (aref text offset) ?\;))
      offset)
     ((and (> offset 0)
           (= (aref text (1- offset)) ?\;))
      (1- offset))
     (t offset))))

(defun clutch--preview-sql-buffer (sql)
  "Display SQL in the *clutch-preview* buffer."
  (let ((buf (get-buffer-create "*clutch-preview*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert sql)
        (goto-char (point-min))
        (if (derived-mode-p 'sql-mode)
            (setq buffer-read-only t)
          (when (fboundp 'sql-mode)
            (sql-mode))
          (setq buffer-read-only t))))
    (pop-to-buffer buf)))

;;;###autoload
(defun clutch-preview-execution-sql ()
  "Preview the execution payload for the current workflow."
  (interactive)
  (let ((sql
         (cond
          ((derived-mode-p 'clutch-result-mode)
           (if (or clutch--pending-inserts
                   clutch--pending-edits
                   clutch--pending-deletes)
               (string-trim-right (clutch-result--pending-sql-content))
             (clutch-result--effective-query)))
          ((use-region-p)
           (string-trim (buffer-substring-no-properties
                         (region-beginning) (region-end))))
          (t
           (pcase-let* ((`(,beg . ,end) (clutch--dwim-bounds-at-point)))
             (string-trim (buffer-substring-no-properties beg end)))))))
    (when (or (null sql) (string-empty-p sql))
      (clutch--user-error "No SQL to preview"))
    (clutch--preview-sql-buffer sql)))

;;;; Interactive commands

;;;###autoload
(defun clutch-execute-query-at-point ()
  "Execute the SQL query at point."
  (interactive)
  (pcase-let* ((`(,beg . ,end) (clutch--query-bounds-at-point))
               (sql (string-trim (buffer-substring-no-properties beg end))))
    (when (string-empty-p sql)
      (clutch--user-error "No query at point"))
    (clutch--ensure-connection)
    (clutch--execute-and-mark sql beg end)))

(defun clutch--statement-bounds-at-point ()
  "Return the SQL statement bounds using only semicolons as delimiters.
Unlike `clutch--query-bounds-at-point', blank lines are ignored so that
long statements spanning multiple paragraphs are captured whole.
Semicolons inside strings, line comments, and block comments are skipped."
  (let* ((text (buffer-substring-no-properties (point-min) (point-max)))
         (offset (- (point) (point-min)))
         (len (length text))
         (breaks nil)                    ; list of semicolon positions
         (in-string nil)
         (i 0))
    ;; Collect all top-level semicolon positions using the same parser
    ;; as `clutch--split-statements'.
    (while (< i len)
      (let ((ch (aref text i)))
        (cond
         (in-string
          (when (= ch in-string) (setq in-string nil)))
         ((= ch ?')  (setq in-string ?'))
         ((= ch ?\") (setq in-string ?\"))
         ((and (= ch ?-) (< (1+ i) len) (= (aref text (1+ i)) ?-))
          (while (and (< i len) (/= (aref text i) ?\n)) (cl-incf i)))
         ((and (= ch ?/) (< (1+ i) len) (= (aref text (1+ i)) ?*))
          (cl-incf i 2)
          (while (and (< (1+ i) len)
                      (not (and (= (aref text i) ?*) (= (aref text (1+ i)) ?/))))
            (cl-incf i))
          (cl-incf i))
         ((= ch ?\;) (push i breaks))))
      (cl-incf i))
    (setq breaks (nreverse breaks))
    ;; Find the enclosing semicolons around point.
    (let* ((beg (point-min))
           (end (point-max))
           (effective-offset (clutch--statement-effective-offset text offset))
           (semicolon-edge (or (/= effective-offset offset)
                               (and (< offset len)
                                    (= (aref text offset) ?\;)))))
      (dolist (b breaks)
        (if (< b effective-offset)
            (setq beg (+ (point-min) b 1))
          (when (= end (point-max))
            (setq end (+ (point-min) b)))))
      (if (or semicolon-edge
              (when-let* ((trimmed (clutch--trim-sql-bounds beg end)))
                (>= (point) (car trimmed))))
          (cons beg end)
        (cons (point) (point))))))

(defun clutch--dwim-bounds-at-point ()
  "Return point-local SQL bounds for DWIM execute and preview workflows.
Prefer semicolon-delimited statement bounds when the current buffer
contains any top-level semicolon; otherwise fall back to query-at-point
bounds, which also split on blank lines."
  (if (clutch--statement-delimited-buffer-p)
      (clutch--statement-bounds-at-point)
    (clutch--query-bounds-at-point)))

;;;###autoload
(defun clutch-execute-statement-at-point ()
  "Execute the SQL statement at point, delimited only by semicolons.
Blank lines inside the statement are preserved."
  (interactive)
  (pcase-let* ((`(,beg . ,end) (clutch--statement-bounds-at-point))
               (sql (string-trim (buffer-substring-no-properties beg end))))
    (when (string-empty-p sql)
      (clutch--user-error "No statement at point"))
    (clutch--ensure-connection)
    (clutch--execute-and-mark sql beg end)))

(defun clutch--split-statements (sql)
  "Split SQL into individual statements on unquoted semicolons.
Skips semicolons inside single-quoted strings, -- line comments,
and /* */ block comments."
  (let ((stmts nil) (start 0) (in-string nil) (i 0) (len (length sql)))
    (while (< i len)
      (let ((ch (aref sql i)))
        (cond
         (in-string
          (when (= ch in-string)
            (setq in-string nil)))
         ((= ch ?')  (setq in-string ?'))
         ((= ch ?\") (setq in-string ?\"))
         ((and (= ch ?-) (< (1+ i) len) (= (aref sql (1+ i)) ?-))
          (while (and (< i len) (/= (aref sql i) ?\n)) (cl-incf i)))
         ((and (= ch ?/) (< (1+ i) len) (= (aref sql (1+ i)) ?*))
          (cl-incf i 2)
          (while (and (< (1+ i) len)
                      (not (and (= (aref sql i) ?*) (= (aref sql (1+ i)) ?/))))
            (cl-incf i))
          (cl-incf i))
         ((= ch ?\;)
          (let ((stmt (string-trim (substring sql start i))))
            (unless (string-empty-p stmt) (push stmt stmts)))
          (setq start (1+ i)))))
      (cl-incf i))
    (let ((tail (string-trim (substring sql start))))
      (unless (string-empty-p tail) (push tail stmts)))
    (nreverse stmts)))

(defun clutch--execute-statements (stmts)
  "Execute STMTS sequentially.
DML/DDL statements run silently; the final SELECT (if any) opens a
result buffer.  Stops and reports on the first error."
  (let* ((last (car (last stmts)))
         (before-last (butlast stmts))
         (done 0)
         (source-buffer (current-buffer)))
    (cl-labels
        ((signal-statement-error (err stmt)
           (let* ((failure
                  (clutch--remember-execute-error
                   source-buffer clutch-connection stmt err
                   (list :statement-index (1+ done))))
                  (message (car failure))
                  (summary (cdr failure))
                  (display (clutch--humanize-db-error-parts message))
                  (display-summary (or (plist-get display :summary)
                                       summary))
                  (hint (plist-get display :hint))
                  (buf (clutch--display-error-result
                        clutch-connection stmt display-summary message nil
                        hint)))
             (when (and (buffer-live-p source-buffer)
                        clutch--executing-sql-start
                        clutch--executing-sql-end)
               (with-current-buffer source-buffer
                 (clutch--mark-failed-sql-region
                  clutch--executing-sql-start clutch--executing-sql-end
                  display-summary)))
             (when (and (buffer-live-p source-buffer)
                        (buffer-live-p buf))
               (with-current-buffer source-buffer
                 (setq-local clutch--last-result-buffer buf)))
             (clutch--user-error "Statement %d failed: %s"
                         (1+ done)
                         (clutch--debug-workflow-message summary)))))
      (dolist (stmt before-last)
        (condition-case err
            (progn (clutch--run-db-query clutch-connection stmt) (cl-incf done))
          (quit
           (clutch--handle-query-quit clutch-connection))
          (clutch-db-error
           (signal-statement-error err stmt))))
      (if (clutch--select-query-p last)
          (progn
            (when (> done 0)
              (message "%s statement%s %s"
                       (clutch--message-count done)
                       (if (= done 1) "" "s")
                       (clutch--message-keyword "executed")))
            (clutch--execute last))
        (condition-case err
            (progn (clutch--run-db-query clutch-connection last) (cl-incf done)
                   (message "%s statement%s %s"
                            (clutch--message-count done)
                            (if (= done 1) "" "s")
                            (clutch--message-keyword "executed")))
          (quit
           (clutch--handle-query-quit clutch-connection))
          (clutch-db-error
           (signal-statement-error err last)))))))

;;;###autoload
(defun clutch-execute-dwim (beg end)
  "Execute SQL from BEG to END using the most useful local boundary.
With an active region, execute that region.  Otherwise, prefer the
semicolon-delimited statement at point when the current buffer contains
top-level semicolons; fall back to the current query-at-point when it does
not.  When the region contains multiple semicolon-separated statements, they
are executed sequentially."
  (interactive
   (if (use-region-p)
       (list (region-beginning) (region-end))
     (list (point) (point))))
  (clutch--ensure-connection)
  (if (use-region-p)
      (clutch--execute-sql-range beg end "region")
    (pcase-let* ((`(,qb . ,qe) (clutch--dwim-bounds-at-point))
                 (sql (string-trim (buffer-substring-no-properties qb qe))))
      (when (string-empty-p sql)
        (clutch--user-error "No SQL at point"))
      (clutch--execute-and-mark sql qb qe))))

(defun clutch--execute-sql-range (beg end scope)
  "Execute trimmed SQL between BEG and END for SCOPE.
Semicolon-delimited multi-statement ranges run sequentially."
  (let* ((sql (string-trim (buffer-substring-no-properties beg end)))
         (stmts (clutch--split-statements sql)))
    (when (string-empty-p sql)
      (clutch--user-error "No SQL in %s" scope))
    (if (cdr stmts)
        (clutch--execute-statements stmts)
      (clutch--execute-and-mark sql beg end))))

;;;###autoload
(defun clutch-execute-region (beg end)
  "Execute SQL in the region from BEG to END.
Semicolon-delimited multi-statement regions run sequentially."
  (interactive "r")
  (clutch--ensure-connection)
  (clutch--execute-sql-range beg end "region"))

;;;###autoload
(defun clutch-execute-buffer ()
  "Execute SQL in the current buffer.
Semicolon-delimited multi-statement buffers run sequentially."
  (interactive)
  (clutch--ensure-connection)
  (clutch--execute-sql-range (point-min) (point-max) "buffer"))

(defun clutch--find-connection ()
  "Find a live database connection from any clutch-mode buffer.
Returns the connection or nil."
  (cl-loop for buf in (buffer-list)
           for conn = (buffer-local-value 'clutch-connection buf)
           when (clutch--connection-alive-p conn)
           return conn))

;;;###autoload
(defun clutch-execute (sql)
  "Execute SQL from any buffer.
With an active region, execute the region.  Otherwise execute the
current line.  Uses the connection from any clutch-mode buffer."
  (interactive
   (list (string-trim
          (if (use-region-p)
              (buffer-substring-no-properties (region-beginning) (region-end))
            (buffer-substring-no-properties
             (line-beginning-position) (line-end-position))))))
  (when (string-empty-p sql)
    (clutch--user-error "No SQL to execute"))
  (let* ((conn (or clutch-connection
                   (clutch--find-connection)
                   (clutch--user-error "No active connection.  Use M-x clutch-mode then C-c C-e to connect")))
         (beg (if (use-region-p) (region-beginning) (line-beginning-position)))
         (end (if (use-region-p) (region-end) (line-end-position))))
    (clutch--execute-and-mark sql beg end conn)))

;;;; Indirect edit buffer

(defun clutch--string-at-point ()
  "Return the string literal content at point, or nil.
Uses `syntax-ppss' to detect string boundaries, so it works in
any mode that has a proper syntax table (Java, Kotlin, Python,
Go, Ruby, etc.)."
  (let ((ppss (syntax-ppss)))
    (when (nth 3 ppss)
      (let ((str-start (nth 8 ppss)))
        (save-excursion
          (goto-char str-start)
          (forward-sexp 1)
          (buffer-substring-no-properties (1+ str-start) (1- (point))))))))

(defvar clutch--indirect-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c '") #'clutch-indirect-execute)
    (define-key map (kbd "C-c C-k") #'clutch-indirect-abort)
    map)
  "Keymap for `clutch--indirect-mode'.")

(define-minor-mode clutch--indirect-mode
  "Minor mode active in indirect SQL edit buffers.
\\<clutch--indirect-mode-map>
Key bindings:
  \\[clutch-indirect-execute]	Execute and close
  \\[clutch-indirect-abort]	Abort and close"
  :lighter " Indirect")

;;;###autoload
(defun clutch-indirect-execute ()
  "Execute the SQL in the indirect buffer, then close it."
  (interactive)
  (let ((sql (string-trim
              (buffer-substring-no-properties (point-min) (point-max))))
        (conn (or clutch-connection
                  (clutch--find-connection))))
    (when (string-empty-p sql)
      (clutch--user-error "No SQL to execute"))
    (unless conn
      (clutch--user-error "No active connection"))
    (quit-window 'kill)
    ;; `quit-window' kills the indirect buffer, leaving the Lisp execution
    ;; context in a dead buffer.  Any subsequent `with-current-buffer' call
    ;; would fail when `save-current-buffer' tries to restore that dead buffer.
    ;; Explicitly switch to the live buffer now selected after the kill.
    (with-current-buffer (window-buffer (selected-window))
      (clutch--execute sql conn))))

;;;###autoload
(defun clutch-indirect-abort ()
  "Abort the indirect edit buffer."
  (interactive)
  (quit-window 'kill))

(defun clutch--extract-indirect-sql-text ()
  "Return SQL text to populate an indirect edit buffer.
Uses region if active, string literal at point if inside one,
or the current line otherwise."
  (string-trim
   (cond
    ((use-region-p)
     (buffer-substring-no-properties (region-beginning) (region-end)))
    ((clutch--string-at-point))
    (t
     (buffer-substring-no-properties
      (line-beginning-position) (line-end-position))))))

;;;###autoload
(defun clutch-edit-indirect ()
  "Open an indirect `clutch-mode' buffer with SQL extracted from context.
With an active region, use the region.  When point is inside a
string literal (DAO code, etc.), extract the string content.
Otherwise use the current line.

The indirect buffer inherits the connection from any live
`clutch-mode' buffer.  Edit the SQL freely, then press
\\<clutch--indirect-mode-map>\\[clutch-indirect-execute] \
to execute or \\[clutch-indirect-abort] to abort."
  (interactive)
  (let* ((text (clutch--extract-indirect-sql-text))
         (conn (or (bound-and-true-p clutch-connection)
                   (clutch--find-connection)))
         (params clutch--connection-params)
         (product clutch--conn-sql-product)
         (buf  (generate-new-buffer "*clutch: indirect*")))
    (pop-to-buffer buf)
    (clutch-mode)
    (when conn
      (clutch--bind-connection-context conn params product)
      (clutch--update-mode-line))
    (clutch--indirect-mode 1)
    (insert text)
    (goto-char (point-min))
    (message "Edit SQL, then C-c ' to execute, C-c C-k to abort")))

(provide 'clutch-query)
;;; clutch-query.el ends here
