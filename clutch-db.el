;;; clutch-db.el --- Database backend protocol facade -*- lexical-binding: t; -*-

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

;; Backend-agnostic database interface for clutch.
;;
;; Defines a generic API via `cl-defgeneric' that database backends
;; (MySQL, PostgreSQL, etc.) implement via `cl-defmethod'.
;; Also owns backend-neutral SQL helpers and database error normalization.
;;
;; Each backend provides a connection struct and methods dispatching
;; on that struct type.  clutch.el calls only these generics,
;; never backend-specific functions directly.

;;; Code:

(require 'cl-lib)
(require 'subr-x)

;;;; Error types

(define-error 'clutch-db-error "Database error")
(define-error 'clutch-query-interrupted "Query interrupted" 'user-error)

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
    (setq cleaned (replace-regexp-in-string
                   "\\`Database error: " "" cleaned))
    (setq cleaned (replace-regexp-in-string
                   "[ \t]*(queryId=[^)]*)" "" cleaned))
    (setq cleaned (replace-regexp-in-string
                   "[ \t]*(version [^)]*([^)]*)[^)]*)" "" cleaned))
    (when (string-match "\n[ \t]*at " cleaned)
      (setq cleaned (substring cleaned 0 (match-beginning 0))))
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

(defmacro clutch-db--translate-library-error (condition &rest body)
  "Run BODY and translate backend CONDITION errors to `clutch-db-error'."
  (declare (indent 1) (debug t))
  `(condition-case err
       (progn ,@body)
     (,condition
      (signal 'clutch-db-error
              (list (error-message-string err))))))

(defun clutch-db--normalize-symbol-option (value)
  "Return VALUE normalized to a lowercase symbol, or nil when absent."
  (cond
   ((null value) nil)
   ((symbolp value)
    (intern (downcase (symbol-name value))))
   ((stringp value)
    (intern (downcase value)))
   (t value)))

(defun clutch-db--reject-removed-connect-params (params)
  "Signal for removed connection PARAMS and return PARAMS otherwise."
  (when (plist-member params :read-timeout)
    (user-error "Connection parameter :read-timeout was removed; use :read-idle-timeout"))
  params)

(defun clutch-db--apply-connect-defaults (params defaults)
  "Return PARAMS with connection DEFAULTS filled in.
DEFAULTS is an alist of (KEY . VALUE).  Existing non-nil values in PARAMS win."
  (cl-loop with normalized = (copy-sequence params)
           for (key . value) in defaults
           do (setq normalized
                    (plist-put normalized key
                               (or (plist-get normalized key) value)))
           finally return normalized))

(defun clutch--json-serialize-text (value &optional context)
  "Return VALUE serialized as normal Emacs JSON text.
`json-serialize' returns a unibyte UTF-8 string.  Decode it back to a
regular multibyte Emacs string so non-ASCII JSON content remains readable.
When CONTEXT is non-nil, use it in the raised `clutch-db-error' message."
  (condition-case err
      (let ((json (json-serialize value)))
        (if (multibyte-string-p json)
            json
          (decode-coding-string json 'utf-8 t)))
    (error
     (signal 'clutch-db-error
             (list (format "Cannot serialize %s as JSON: %s"
                           (or context "value")
                           (error-message-string err)))))))

(defun clutch-db--normalize-mysql-ssl-mode (ssl-mode)
  "Return canonical MySQL SSL-MODE, or signal `clutch-db-error'."
  (pcase (clutch-db--normalize-symbol-option ssl-mode)
    ('nil nil)
    ((or 'disabled 'off) 'disabled)
    (_
     (signal 'clutch-db-error
             (list (format "Unsupported MySQL :ssl-mode %S (supported: disabled)" ssl-mode))))))

(defun clutch-db--normalize-pg-sslmode (sslmode)
  "Return canonical PostgreSQL SSLMODE, or signal `clutch-db-error'."
  (pcase (clutch-db--normalize-symbol-option sslmode)
    ('nil nil)
    ((or 'disable 'prefer 'require 'verify-full)
     (clutch-db--normalize-symbol-option sslmode))
    (_
     (signal 'clutch-db-error
             (list (format
                    "Unsupported PostgreSQL :sslmode %S (supported: disable, prefer, require, verify-full)"
                    sslmode))))))

(defun clutch-db--normalize-mysql-connect-params (params)
  "Return PARAMS normalized for the MySQL backend."
  (let* ((params (copy-sequence params))
         (tls-specified-p (plist-member params :tls))
         (tls (plist-get params :tls))
         (ssl-mode (clutch-db--normalize-mysql-ssl-mode
                    (plist-get params :ssl-mode)))
         (tls-mode (cond
                    (ssl-mode 'disable)
                    (tls-specified-p (if tls 'require 'disable))
                    (t 'default))))
    (when ssl-mode
      (setq params (plist-put params :ssl-mode ssl-mode)))
    (when (and ssl-mode tls-specified-p tls)
      (signal 'clutch-db-error
              (list "Conflicting MySQL TLS options: :tls t cannot be combined with :ssl-mode disabled")))
    (setq params (plist-put params :clutch-tls-mode tls-mode))
    (when (and tls-specified-p (null tls))
      ;; Canonicalize the generic plaintext shortcut to MySQL's explicit name.
      (setq params (plist-put params :ssl-mode 'disabled))
      (cl-remf params :tls))
    params))

(defun clutch-db--normalize-pg-connect-params (params)
  "Return PARAMS normalized for the PostgreSQL backend."
  (let* ((params (copy-sequence params))
         (tls-specified-p (plist-member params :tls))
         (tls (plist-get params :tls))
         (sslmode (clutch-db--normalize-pg-sslmode
                   (plist-get params :sslmode)))
         (tls-mode (pcase sslmode
                     ((or 'disable 'prefer) sslmode)
                     ((or 'require 'verify-full) 'require)
                     (_ (if tls-specified-p
                            (if tls 'require 'disable)
                          'default)))))
    (pcase sslmode
      ('disable
       (when (and tls-specified-p tls)
         (signal 'clutch-db-error
                 (list "Conflicting PostgreSQL TLS options: :tls t cannot be combined with :sslmode disable"))))
      ('prefer
       (when tls-specified-p
         (signal 'clutch-db-error
                 (list "Conflicting PostgreSQL TLS options: :sslmode prefer cannot be combined with :tls"))))
      ((or 'require 'verify-full)
       (when (and tls-specified-p (null tls))
         (signal 'clutch-db-error
                 (list (format "Conflicting PostgreSQL TLS options: :tls nil cannot be combined with :sslmode %s"
                               sslmode))))))
    (setq params (plist-put params :clutch-tls-mode tls-mode))
    (cond
     (sslmode
      (setq params (plist-put params :sslmode sslmode))
      (when tls-specified-p
        (cl-remf params :tls)))
     (tls-specified-p
      ;; Canonicalize the generic boolean shortcut to PostgreSQL's official name.
      (setq params (plist-put params :sslmode (if tls 'require 'disable)))
      (cl-remf params :tls)))
    params))

(defun clutch-db--normalize-connect-params (backend params)
  "Return connection PARAMS normalized for BACKEND."
  (setq params (clutch-db--reject-removed-connect-params params))
  (pcase backend
    ('mysql (clutch-db--normalize-mysql-connect-params params))
    ('pg (clutch-db--normalize-pg-connect-params params))
    (_ params)))

;;;; Result struct

(cl-defstruct clutch-db-result
  "A database query result.
CONNECTION is the backend connection object.
COLUMNS is a list of plists (:name STR :type-category SYM) where
:type-category is one of: numeric, blob, json, text, date, time,
datetime, other.
ROWS is a list of lists (one per row).
AFFECTED-ROWS, LAST-INSERT-ID, and WARNINGS are for DML results."
  connection columns rows affected-rows last-insert-id warnings)

(defun clutch-db-result-column-names (columns)
  "Return column names from result COLUMNS as strings."
  (mapcar (lambda (column)
            (let ((name (plist-get column :name)))
              (if (stringp name) name (format "%s" name))))
          columns))

(defun clutch-db-row-identity-values (row row-identity)
  "Return a vector of identity values from ROW using ROW-IDENTITY."
  (vconcat (mapcar (lambda (i) (nth i row))
                   (plist-get row-identity :indices))))

(defvar clutch-db--foreground-connections (make-hash-table :test 'eq)
  "Connections currently reserved by foreground Clutch commands.
Values are nesting counts.")

(defun clutch-db--foreground-busy-p (conn)
  "Return non-nil when CONN is reserved by foreground Clutch work."
  (and conn (gethash conn clutch-db--foreground-connections)))

(defmacro clutch-db-with-foreground-connection (conn &rest body)
  "Run BODY while marking CONN reserved for foreground work."
  (declare (indent 1) (debug t))
  (let ((conn-var (make-symbol "conn")))
    `(let ((,conn-var ,conn))
       (when ,conn-var
         (puthash ,conn-var
                  (1+ (or (gethash ,conn-var clutch-db--foreground-connections) 0))
                  clutch-db--foreground-connections))
       (unwind-protect
           (progn ,@body)
         (when ,conn-var
           (let ((count (1- (or (gethash ,conn-var clutch-db--foreground-connections) 1))))
             (if (> count 0)
                 (puthash ,conn-var count clutch-db--foreground-connections)
               (remhash ,conn-var clutch-db--foreground-connections))))))))

;;;; SQL helpers (literal-or-comment awareness)

(defun clutch-db-sql-strip-leading-comments (sql)
  "Strip leading SQL comments and whitespace from SQL."
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

(defun clutch-db-sql-normalize (sql)
  "Return SQL trimmed for clause-aware analysis and rewrite."
  (string-trim-right
   (replace-regexp-in-string
    ";\\s-*\\'" "" (clutch-db-sql-strip-leading-comments sql))))

(defun clutch-db-sql-skip-literal-or-comment (sql pos)
  "If POS in SQL starts a string literal or comment, return position past it.
Handles single-quoted strings (with '' escape), -- line comments, and
/* block comments */.  Double-quoted identifiers and backticks are NOT
treated as literals.  Returns nil when POS is at normal code."
  (let ((len (length sql))
        (ch (and (< pos (length sql)) (aref sql pos))))
    (pcase ch
      (?\' ;; Single-quoted string: scan for unescaped closing quote.
       (cl-loop for i from (1+ pos) below len
                when (= (aref sql i) ?\')
                do (if (and (< (1+ i) len) (= (aref sql (1+ i)) ?\'))
                       (cl-incf i)          ; '' escape, skip pair
                     (cl-return (1+ i)))    ; past closing quote
                finally return len))
      (?-  ;; Possible -- line comment.
       (if (and (< (1+ pos) len) (= (aref sql (1+ pos)) ?-))
           (or (cl-loop for i from (+ pos 2) below len
                        when (= (aref sql i) ?\n) return (1+ i))
               len)
         nil))
      (?/  ;; Possible /* block comment */.
       (if (and (< (1+ pos) len) (= (aref sql (1+ pos)) ?*))
           (let ((end (cl-loop for i from (+ pos 2) below (1- len)
                               when (and (= (aref sql i) ?*)
                                         (= (aref sql (1+ i)) ?/))
                               return (+ i 2))))
             (or end len))
         nil)))))

(defun clutch-db-sql-mask-literal-or-comment (sql)
  "Return a string the same length as SQL with literals/comments blanked.
Single-quoted content (between the quotes) and comment text become spaces.
Quote delimiters are preserved.  Double-quoted identifiers and backticks
are left intact.  Safe for multibyte strings (avoids `aset')."
  (let ((pieces nil)
        (copy-from 0)
        (pos 0)
        (len (length sql)))
    (while (< pos len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (if (= (aref sql pos) ?\')
              ;; String literal: preserve quote delimiters, blank content.
              (let* ((has-close (and (> skip (1+ pos))
                                    (= (aref sql (1- skip)) ?\')))
                     (content-end (if has-close (1- skip) skip)))
                (push (substring sql copy-from (1+ pos)) pieces)
                (push (make-string (max 0 (- content-end (1+ pos))) ?\s) pieces)
                (when has-close (push "'" pieces))
                (setq copy-from skip pos skip))
            ;; Comment: blank entirely.
            (push (substring sql copy-from pos) pieces)
            (push (make-string (- skip pos) ?\s) pieces)
            (setq copy-from skip pos skip))
        (cl-incf pos)))
    (push (substring sql copy-from) pieces)
    (apply #'concat (nreverse pieces))))

(defun clutch-db-sql-scan-code (sql start end fn)
  "Scan SQL code characters from START to END, skipping strings/comments.
FN is called with (POS CHAR DEPTH), where DEPTH is the parenthesis depth before
CHAR is applied.  When FN returns non-nil, stop and return that value."
  (let ((pos (or start 0))
        (end (or end (length sql)))
        (depth 0)
        result)
    (while (and (< pos end) (not result))
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (setq pos (min skip end))
        (let ((ch (aref sql pos)))
          (setq result (funcall fn pos ch depth))
          (unless result
            (cond
             ((= ch ?\() (cl-incf depth))
             ((= ch ?\)) (setq depth (max 0 (1- depth)))))
            (cl-incf pos)))))
    result))

(defun clutch-db-sql-matching-paren-position (sql open-pos)
  "Return the matching close-paren position for OPEN-POS in SQL, or nil."
  (clutch-db-sql-scan-code
   sql open-pos nil
   (lambda (pos ch depth)
     (and (= ch ?\))
          (= depth 1)
          pos))))

;;;; SQL helpers (statement boundaries)

(defun clutch-db-sql-statement-breaks (sql)
  "Return zero-based offsets of top-level semicolons in SQL.
Semicolons inside strings, line comments, and block comments do not count."
  (let ((breaks nil)
        (in-string nil)
        (i 0)
        (len (length sql)))
    (while (< i len)
      (let ((ch (aref sql i)))
        (cond
         (in-string
          (when (= ch in-string)
            (setq in-string nil)))
         ((= ch ?')  (setq in-string ?'))
         ((= ch ?\") (setq in-string ?\"))
         ((and (= ch ?-) (< (1+ i) len) (= (aref sql (1+ i)) ?-))
          (while (and (< i len) (/= (aref sql i) ?\n))
            (cl-incf i)))
         ((and (= ch ?/) (< (1+ i) len) (= (aref sql (1+ i)) ?*))
          (cl-incf i 2)
          (while (and (< (1+ i) len)
                      (not (and (= (aref sql i) ?*)
                                (= (aref sql (1+ i)) ?/))))
            (cl-incf i))
          (cl-incf i))
         ((= ch ?\;)
          (push i breaks))))
      (cl-incf i))
    (nreverse breaks)))

(defun clutch-db-sql-statement-effective-offset (text offset)
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

(defun clutch-db-sql-semicolon-statement-bounds (text offset)
  "Return zero-based statement bounds around OFFSET in TEXT.
Top-level semicolons delimit statements.  Semicolons inside strings and
comments are ignored."
  (let ((beg 0)
        (end (length text))
        (effective-offset
         (clutch-db-sql-statement-effective-offset text offset)))
    (dolist (break (clutch-db-sql-statement-breaks text))
      (if (< break effective-offset)
          (setq beg (1+ break))
        (when (= end (length text))
          (setq end break))))
    (cons beg end)))

(defun clutch-db-sql--trim-bounds (text beg end)
  "Return non-whitespace bounds in TEXT between BEG and END, or nil."
  (while (and (< beg end)
              (memq (aref text beg) '(?\s ?\t ?\r ?\n)))
    (cl-incf beg))
  (while (and (< beg end)
              (memq (aref text (1- end)) '(?\s ?\t ?\r ?\n)))
    (cl-decf end))
  (when (< beg end)
    (cons beg end)))

(defun clutch-db-sql-semicolon-statement-bounds-at-offset
    (text offset &optional strict-leading-space)
  "Return zero-based semicolon statement bounds around OFFSET in TEXT.
When STRICT-LEADING-SPACE is non-nil and OFFSET is before the trimmed
statement body, return an empty range at OFFSET.  This lets execute-at-point
avoid running the previous statement from blank space between semicolon
delimited statements."
  (let* ((bounds (clutch-db-sql-semicolon-statement-bounds text offset))
         (effective-offset (clutch-db-sql-statement-effective-offset text offset))
         (semicolon-edge (or (/= effective-offset offset)
                             (and (< offset (length text))
                                  (= (aref text offset) ?\;)))))
    (if (and strict-leading-space
             (not semicolon-edge)
             (not (when-let* ((trimmed (clutch-db-sql--trim-bounds
                                        text (car bounds) (cdr bounds))))
                    (>= offset (car trimmed)))))
        (cons offset offset)
      bounds)))

(defun clutch-db-sql-blank-line-statement-bounds (text offset)
  "Return zero-based blank-line-delimited statement bounds around OFFSET."
  (let ((len (length text))
        (beg 0)
        (end nil)
        (pos 0))
    (while (< pos len)
      (let* ((line-start pos)
             (newline (string-search "\n" text pos))
             (line-end (or newline len))
             (blank-p (string-match-p
                       "\\`[ \t\r]*\\'"
                       (substring text line-start line-end))))
        (cond
         ((and blank-p (<= line-end offset))
          (setq beg (if newline (1+ line-end) line-end)))
         ((and blank-p (not end) (> line-start offset))
          (setq end line-start)))
        (setq pos (if newline (1+ line-end) len))))
    (cons beg (or end len))))

(defun clutch-db-sql-context-statement-bounds (text offset)
  "Return statement bounds for SQL context features in TEXT at OFFSET.
Use semicolon-aware bounds when TEXT has top-level semicolons; otherwise fall
back to blank-line paragraph bounds."
  (if (clutch-db-sql-statement-breaks text)
      (clutch-db-sql-semicolon-statement-bounds text offset)
    (clutch-db-sql-blank-line-statement-bounds text offset)))

;;;; SQL helpers (top-level clause detection)

(defun clutch-db-sql--top-level-clause-match (sql start patterns)
  "Return (POS . PATTERN) for the first top-level PATTERNS match in SQL.
START is the initial search offset.  PATTERNS are case-insensitive regex
fragments matched with word boundaries."
  (let ((case-fold-search t)
        (matchers (mapcar (lambda (pattern)
                            (cons pattern (format "\\b%s\\b" pattern)))
                          patterns)))
    (clutch-db-sql-scan-code
     sql start nil
     (lambda (pos _ch depth)
       (and (zerop depth)
            (catch 'match
              (dolist (matcher matchers)
                (when (and (string-match (cdr matcher) sql pos)
                           (= (match-beginning 0) pos))
                  (throw 'match (cons pos (car matcher)))))
              nil))))))

(defun clutch-db-sql-find-top-level-clause (sql pattern &optional start)
  "Return start position of top-level PATTERN in SQL, or nil.
PATTERN is matched case-insensitively with word boundaries.
START defaults to 0."
  (car (clutch-db-sql--top-level-clause-match
        sql (or start 0) (list pattern))))

(defun clutch-db-sql-has-top-level-clause-p (sql pattern &optional start)
  "Return non-nil when SQL has top-level PATTERN starting at START."
  (clutch-db-sql-find-top-level-clause sql pattern start))

(defun clutch-db-sql-has-top-level-limit-p (sql)
  "Return non-nil when SQL has a top-level LIMIT clause."
  (clutch-db-sql-has-top-level-clause-p sql "LIMIT"))

(defun clutch-db-sql-has-top-level-offset-p (sql)
  "Return non-nil when SQL has a top-level OFFSET clause."
  (clutch-db-sql-has-top-level-clause-p sql "OFFSET"))

(defun clutch-db-sql-starts-with-keyword-p (sql keywords)
  "Return non-nil when SQL starts with one of KEYWORDS."
  (let ((trimmed (clutch-db-sql-strip-leading-comments sql)))
    (string-match-p (concat "\\`" (regexp-opt keywords) "\\b")
                    (upcase trimmed))))

(defun clutch-db-sql-leading-keyword (sql)
  "Return the leading SQL keyword for SQL, or nil."
  (let ((trimmed (clutch-db-sql-strip-leading-comments sql)))
    (when (string-match "\\`\\([[:alpha:]]+\\)" trimmed)
      (upcase (match-string 1 trimmed)))))

(defun clutch-db-sql-main-op-keyword (sql)
  "Return main top-level operation keyword for SQL, or nil."
  (let* ((normalized (clutch-db-sql-normalize sql))
         (match (clutch-db-sql--top-level-clause-match
                 normalized 0
                 '("UPDATE" "DELETE" "SELECT" "INSERT" "REPLACE" "MERGE"))))
    (cdr match)))

(defun clutch-db-sql-top-level-comma-p (sql start end)
  "Return non-nil when SQL has a top-level comma between START and END."
  (clutch-db-sql-scan-code
   sql start end
   (lambda (_pos ch depth)
     (and (zerop depth) (= ch ?,)))))

(defun clutch-db-sql-next-top-level-clause-position (sql start patterns)
  "Return earliest top-level clause match in SQL after START for PATTERNS.
PATTERNS is a list of case-insensitive regex fragments passed to
`clutch-db-sql-find-top-level-clause'."
  (car (clutch-db-sql--top-level-clause-match sql start patterns)))

(defun clutch-db-sql-from-body-range (sql from-pos)
  "Return `(START END)' for the top-level FROM body in SQL after FROM-POS."
  (let ((start (+ from-pos 4)))
    (list start
          (or (clutch-db-sql-next-top-level-clause-position
               sql start
               '("WHERE" "GROUP\\s-+BY" "HAVING" "ORDER\\s-+BY"
                 "LIMIT" "OFFSET" "FETCH" "FOR" "UNION" "INTERSECT"
                 "EXCEPT"))
              (length sql)))))

(defconst clutch-db-sql--table-token-pattern
  (concat "\\(?:`[^`]+`\\."
          "\\)?`[^`]+`"
          "\\|\\(?:\"[^\"]+\"\\.\\)?\"[^\"]+\""
          "\\|\\(?:\\[[^]]+\\]\\.\\)?\\[[^]]+\\]"
          "\\|[^[:space:],();]+")
  "SQL table token pattern accepted by source-table helpers.")

(defun clutch-db-sql-from-body-parts (body)
  "Return `(TABLE ALIAS)' from simple FROM BODY."
  (let ((case-fold-search t)
        (pattern (concat "\\`\\s-*\\("
                         clutch-db-sql--table-token-pattern
                         "\\)"
                         "\\(?:\\s-+\\(?:AS\\s-+\\)?"
                         "\\(\"[^\"]+\"\\|`[^`]+`\\|\\[[^]]+\\]\\|[^[:space:]]+\\)"
                         "\\)?\\s-*\\'")))
    (and (string-match pattern body)
         (list (match-string 1 body)
               (match-string 2 body)))))

(defun clutch-db-sql-table-qualifier (table)
  "Return the exposed table qualifier from TABLE."
  (cond
   ((string-match "\\.\\(\"[^\"]+\"\\|`[^`]+`\\|\\[[^]]+\\]\\)\\'" table)
    (match-string 1 table))
   ((string-match "\\.\\([^.\"]+\\)\\'" table)
    (match-string 1 table))
   (t table)))

(defun clutch-db-sql-table-name (table)
  "Return the unquoted table name represented by TABLE."
  (let ((name (clutch-db-sql-table-qualifier table)))
    (cond
     ((string-match "\\`\"\\([^\"]+\\)\"\\'" name)
      (match-string 1 name))
     ((string-match "\\``\\([^`]+\\)`\\'" name)
      (match-string 1 name))
     ((string-match "\\`\\[\\([^]]+\\)\\]\\'" name)
      (match-string 1 name))
     (t name))))

(defun clutch-db-sql--source-table-token (sql &optional simple-only)
  "Return the top-level source table token for SQL, or nil.
When SIMPLE-ONLY is non-nil, require a direct single-table SELECT.  Derived
tables, joins, comma joins, CTEs, UNION/INTERSECT/EXCEPT, and other ambiguous
relations return nil.  The returned token preserves identifier quoting."
  (let* ((normalized (clutch-db-sql-normalize sql))
         (from-pos (clutch-db-sql-find-top-level-clause normalized "FROM")))
    (when from-pos
      (pcase-let* ((`(,start ,end)
                    (clutch-db-sql-from-body-range normalized from-pos))
                   (body (string-trim (substring normalized start end))))
        (when (or (not simple-only)
                  (and (clutch-db-sql-starts-with-keyword-p normalized '("SELECT"))
                       (not (string-prefix-p "(" body))
                       (not (clutch-db-sql-top-level-comma-p normalized start end))
                       (not (clutch-db-sql-next-top-level-clause-position
                             normalized 0 '("UNION" "INTERSECT" "EXCEPT")))
                       (not (let ((join-pos
                                   (clutch-db-sql-find-top-level-clause
                                    normalized "JOIN" start)))
                              (and join-pos (< join-pos end))))))
          (if simple-only
              (car (clutch-db-sql-from-body-parts body))
            (when (string-match (concat "\\`\\s-*\\("
                                        clutch-db-sql--table-token-pattern
                                        "\\)")
                                body)
              (match-string 1 body))))))))

(defun clutch-db-sql-source-table (sql &optional simple-only)
  "Return the top-level source table name for SQL, or nil.
When SIMPLE-ONLY is non-nil, require a direct single-table SELECT.  Derived
tables, joins, comma joins, CTEs, UNION/INTERSECT/EXCEPT, and other ambiguous
relations return nil."
  (when-let* ((token (clutch-db-sql--source-table-token sql simple-only)))
    (clutch-db-sql-table-name token)))

(defun clutch-db-sql-destructive-p (sql)
  "Return non-nil if SQL is a destructive operation."
  (clutch-db-sql-starts-with-keyword-p
   sql '("DELETE" "DROP" "TRUNCATE" "ALTER")))

(defun clutch-db-sql-schema-affecting-p (sql)
  "Return non-nil if SQL is likely to invalidate cached schema."
  (clutch-db-sql-starts-with-keyword-p
   sql '("CREATE" "ALTER" "DROP" "TRUNCATE" "RENAME")))

(defun clutch-db-sql-select-query-p (sql)
  "Return non-nil if SQL returns a result set."
  (clutch-db-sql-starts-with-keyword-p
   sql '("SELECT" "WITH" "DESCRIBE" "DESC" "SHOW" "EXPLAIN")))

(defun clutch-db-sql-strip-top-level-order-by (sql)
  "Strip a top-level ORDER BY tail from SQL.
Leaves nested ORDER BY clauses inside subqueries or window functions intact."
  (if-let* ((order-pos (clutch-db-sql-find-top-level-clause sql "ORDER\\s-+BY")))
      (string-trim-right (substring sql 0 order-pos))
    sql))

(defun clutch-db-sql-derived-table-body (sql)
  "Return SQL normalized for derived-table wrapping.
This preserves user-visible query semantics such as top-level ORDER BY."
  (clutch-db-sql-normalize sql))

(defun clutch-db-sql-count-derived-table-body (sql)
  "Return SQL normalized for COUNT(*) derived-table wrapping.
Top-level ORDER BY is removed when there is no top-level LIMIT/OFFSET because
it cannot affect the row count.  Limited result sets keep their tail clauses so
counts target the user's visible result set."
  (let ((normalized (clutch-db-sql-normalize sql)))
    (if (or (clutch-db-sql-has-top-level-limit-p normalized)
            (clutch-db-sql-has-top-level-offset-p normalized))
        normalized
      (clutch-db-sql-strip-top-level-order-by normalized))))

(defun clutch-db-build-count-sql (conn sql)
  "Return a COUNT(*) query for SQL using CONN's derived-table syntax."
  (format "SELECT COUNT(*) FROM (%s) %s"
          (clutch-db-sql-count-derived-table-body sql)
          (clutch-db-derived-table-alias conn "_clutch_count")))

(defun clutch-db-apply-where (conn sql filter)
  "Return SQL wrapped as a derived table with outer WHERE FILTER.
CONN supplies the dialect-specific derived-table alias syntax."
  (format "SELECT * FROM (%s) %s WHERE %s"
          (clutch-db-sql-derived-table-body sql)
          (clutch-db-derived-table-alias conn "_clutch_filter")
          filter))

(defun clutch-db--build-limit-offset-paged-sql (base-sql page-num page-size
                                                         order-by escape-fn
                                                         &optional page-offset)
  "Build a LIMIT/OFFSET paginated query from BASE-SQL.
PAGE-NUM is zero-based and PAGE-SIZE is the row count per page.
ORDER-BY is (COL . DIR) or nil.  ESCAPE-FN escapes the column name.
PAGE-OFFSET, when non-nil, overrides the offset derived from PAGE-NUM."
  (if (clutch-db-sql-has-top-level-limit-p base-sql)
      base-sql
    (let* ((trimmed (string-trim-right
                     (replace-regexp-in-string ";\\s-*\\'" "" base-sql)))
           (sortable-sql (if order-by
                             (clutch-db-sql-strip-top-level-order-by trimmed)
                           trimmed))
           (offset (or page-offset (* page-num page-size)))
           (order-clause (when order-by
                           (format " ORDER BY %s %s"
                                   (funcall escape-fn (car order-by))
                                   (cdr order-by)))))
      (format "%s%s LIMIT %d OFFSET %d"
              sortable-sql (or order-clause "") page-size offset))))

;;;; Generic interface

;; Lifecycle

(cl-defgeneric clutch-db-disconnect (conn)
  "Disconnect CONN from the database server.")

(cl-defgeneric clutch-db-live-p (conn)
  "Return non-nil if CONN is still connected and usable."
  (ignore conn)
  nil)

(cl-defgeneric clutch-db-error-details (conn)
  "Return structured error details for CONN, or nil."
  (ignore conn)
  nil)

(cl-defgeneric clutch-db-clear-error-details (conn)
  "Forget any backend-local structured error details for CONN."
  (ignore conn)
  nil)

(cl-defgeneric clutch-db-init-connection (conn)
  "Perform post-connect initialization on CONN.
For example, SET NAMES utf8mb4 on MySQL.")

(cl-defgeneric clutch-db--restore-connection-timeouts (conn params)
  "Restore connection-level timeout state on CONN from PARAMS."
  (ignore conn params)
  nil)

(cl-defgeneric clutch-db-backend-key (conn)
  "Return the registered backend key for CONN, or nil when unknown.")

(cl-defmethod clutch-db-backend-key ((_conn t))
  "Fallback implementation for opaque connection objects."
  nil)

(cl-defgeneric clutch-db-manual-commit-p (conn)
  "Return non-nil when CONN is in manual-commit mode.")

(cl-defmethod clutch-db-manual-commit-p ((_conn t))
  "Fallback implementation for backends without manual-commit mode."
  nil)

(cl-defgeneric clutch-db-manual-commit-supported-p (conn)
  "Return non-nil when CONN supports Clutch-managed manual commit.")

(cl-defmethod clutch-db-manual-commit-supported-p ((_conn t))
  "Fallback implementation for backends without manual-commit support."
  nil)

(cl-defgeneric clutch-db-commit (conn)
  "Commit the current transaction on CONN.")

(cl-defmethod clutch-db-commit ((_conn t))
  "Fallback implementation for backends without explicit commit support."
  nil)

(cl-defgeneric clutch-db-rollback (conn)
  "Roll back the current transaction on CONN.")

(cl-defmethod clutch-db-rollback ((_conn t))
  "Fallback implementation for backends without explicit rollback support."
  nil)

(cl-defgeneric clutch-db-set-auto-commit (conn auto-commit)
  "Set CONN's auto-commit mode.
AUTO-COMMIT non-nil enables auto-commit; nil enables manual-commit.")

(cl-defmethod clutch-db-set-auto-commit ((_conn t) _auto-commit)
  "Signal that the backend does not support runtime auto-commit changes."
  (user-error "Manual commit is not supported by this connection"))

(cl-defgeneric clutch-db-schema-transaction-effect (conn sql)
  "Return dirty-cache effect for successful schema SQL on CONN.
SQL is known to affect schema metadata.  Return `dirty' when the SQL leaves
uncommitted work, `clear' when it commits the transaction, or nil when the
backend should preserve the current dirty state.")

(cl-defmethod clutch-db-schema-transaction-effect ((_conn t) _sql)
  "Preserve dirty state for backends without declared DDL transaction semantics."
  nil)

(cl-defgeneric clutch-db-eager-schema-refresh-p (conn)
  "Return non-nil when CONN should refresh schema synchronously on connect.")

(cl-defmethod clutch-db-eager-schema-refresh-p ((_conn t))
  "Most backends refresh schema immediately after connect."
  t)

(cl-defgeneric clutch-db-completion-sync-columns-p (conn)
  "Return non-nil when completion may synchronously load column metadata for CONN.")

(cl-defmethod clutch-db-completion-sync-columns-p ((_conn t))
  "Most backends can synchronously load column metadata during completion."
  t)

(cl-defgeneric clutch-db-refresh-schema-async (conn callback &optional errback)
  "Start an asynchronous schema refresh for CONN.
CALLBACK receives the table name list on success.  ERRBACK receives
an error message string on failure.  Return non-nil when async refresh
was started, nil when unsupported.")

(cl-defmethod clutch-db-refresh-schema-async ((_conn t) _callback &optional _errback)
  "Backends without asynchronous schema refresh support return nil."
  nil)

(cl-defgeneric clutch-db-column-details-async (conn table callback &optional errback)
  "Start an asynchronous column-detail fetch for TABLE on CONN.
CALLBACK receives the column detail plist list on success.  ERRBACK
receives an error message string on failure.  Return non-nil when async
fetch was started, nil when unsupported.")

(cl-defmethod clutch-db-column-details-async ((_conn t) _table _callback
                                              &optional _errback)
  "Backends without asynchronous column detail support return nil."
  nil)

(cl-defgeneric clutch-db-list-columns-async (conn table callback &optional errback)
  "Start an asynchronous column-name fetch for TABLE on CONN.
CALLBACK receives the column name list on success.  ERRBACK receives an
error message string on failure.  Return non-nil when async fetch was
started, nil when unsupported.")

(cl-defmethod clutch-db-list-columns-async ((_conn t) _table _callback
                                            &optional _errback)
  "Backends without asynchronous column-name support return nil."
  nil)

(defun clutch-db--schedule-idle-metadata-call (conn callback errback fn
                                                    &rest args)
  "Schedule metadata FN for CONN on the main thread once Emacs is idle.
CALLBACK receives the result of calling FN with CONN and ARGS.
ERRBACK receives an error-message string when the work fails."
  (cl-labels
      ((run ()
         (if (clutch-db-live-p conn)
             (if (or (clutch-db-busy-p conn)
                     (clutch-db--foreground-busy-p conn))
                 (run-with-idle-timer 0.1 nil #'run)
               (condition-case err
                   (when callback
                     (funcall callback (apply fn conn args)))
                 (error
                  (when errback
                    (funcall errback (error-message-string err))))))
           (when errback
             (funcall errback "Connection closed")))))
    (run-with-idle-timer 0 nil #'run)))

;; Query

(cl-defgeneric clutch-db-query (conn sql)
  "Execute SQL on CONN and return a `clutch-db-result'.")

(cl-defgeneric clutch-db-execute-params (conn sql params)
  "Execute SQL on CONN with positional PARAMS.
SQL uses `?' placeholders.  PARAMS is a list of Elisp values.
Return the same shape as `clutch-db-query'.")

(cl-defmethod clutch-db-execute-params ((conn t) sql params)
  "Fallback parameter execution for CONN by literal substitution.
Substitute PARAMS into SQL before calling `clutch-db-query'."
  (clutch-db-query
   conn
   (clutch-db-substitute-params sql params
                                (lambda (value)
                                  (clutch-db-value-to-literal conn value)))))

(cl-defgeneric clutch-db-interrupt-query (conn)
  "Interrupt the current query on CONN.
Return non-nil when the query was handed off to a backend-specific
interrupt path and the connection should remain usable.")

(cl-defmethod clutch-db-interrupt-query ((_conn t))
  "Backends without query interrupt support return nil."
  nil)

(cl-defgeneric clutch-db-build-paged-sql (conn base-sql page-num page-size
                                          &optional order-by page-offset)
  "Build a paginated SQL query for CONN's dialect.
BASE-SQL is the original query.  PAGE-NUM is 0-based, PAGE-SIZE is
the row limit.  ORDER-BY is (COL-NAME . DIRECTION) or nil.  PAGE-OFFSET,
when non-nil, overrides PAGE-NUM for last-window pagination.")

;; SQL dialect

(cl-defgeneric clutch-db-escape-identifier (conn name)
  "Escape NAME as a SQL identifier for CONN's dialect.")

(cl-defgeneric clutch-db--source-table-name (conn token)
  "Return backend-canonical source table name for SQL table TOKEN.")

(cl-defmethod clutch-db--source-table-name ((_conn t) token)
  "Return the default backend-canonical source table name for TOKEN."
  (clutch-db-sql-table-name token))

(cl-defgeneric clutch-db-escape-literal (conn value)
  "Escape VALUE as a SQL string literal for CONN's dialect.")

(cl-defgeneric clutch-db-derived-table-alias (conn alias)
  "Return CONN's derived-table alias clause for ALIAS.")

(cl-defmethod clutch-db-derived-table-alias ((_conn t) alias)
  "Return the default SQL derived-table alias clause for ALIAS."
  (format "AS %s" alias))

(defun clutch-db-value-to-literal (conn value &optional fallback-format-fn)
  "Render VALUE as a SQL literal for CONN.
FALLBACK-FORMAT-FN formats non-scalar result values before string escaping.
When absent, non-scalar values fall back to `format' with `%S'."
  (cond
   ((null value) "NULL")
   ((numberp value) (number-to-string value))
   ((stringp value) (clutch-db-escape-literal conn value))
   ((and (listp value)
         (clutch-db-format-temporal value))
    (clutch-db-escape-literal conn (clutch-db-format-temporal value)))
   ((or (hash-table-p value) (vectorp value))
    (clutch-db-escape-literal
     conn
     (clutch--json-serialize-text value "parameter value")))
   (t
    (clutch-db-escape-literal
     conn
     (if fallback-format-fn
         (funcall fallback-format-fn value)
       (format "%S" value))))))

(defun clutch-db-substitute-params (sql params render-fn)
  "Return SQL with PARAMS substituted using RENDER-FN.
SQL uses `?' positional placeholders.  PARAMS is a list of parameter values.
RENDER-FN is called once per parameter and must return the replacement string."
  (let ((len (length sql))
        (pos 0)
        (remaining params)
        parts)
    (while (< pos len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (progn
            (push (substring sql pos skip) parts)
            (setq pos skip))
        (let ((ch (aref sql pos)))
          (if (= ch ??)
              (progn
                (unless remaining
                  (signal 'clutch-db-error
                          (list (format "Not enough parameters for SQL template: %s" sql))))
                (push (funcall render-fn (car remaining)) parts)
                (setq remaining (cdr remaining))
                (cl-incf pos))
            (push (string ch) parts)
            (cl-incf pos)))))
    (when remaining
      (signal 'clutch-db-error
              (list (format "Too many parameters for SQL template: %s" sql))))
    (apply #'concat (nreverse parts))))

;; Schema

(cl-defgeneric clutch-db-list-tables (conn)
  "Return a list of table name strings for CONN's current database.")

(cl-defgeneric clutch-db-list-schemas (conn)
  "Return available schema names for CONN, or nil when unsupported.")

(cl-defmethod clutch-db-list-schemas ((_conn t))
  "Backends without schema enumeration support return nil."
  nil)

(cl-defgeneric clutch-db-current-schema (conn)
  "Return the effective current schema for CONN, or nil when not applicable.")

(cl-defmethod clutch-db-current-schema ((_conn t))
  "Default: no current schema abstraction."
  nil)

(cl-defgeneric clutch-db-set-current-schema (conn schema)
  "Switch CONN to SCHEMA for subsequent metadata and query context.")

(cl-defmethod clutch-db-set-current-schema ((_conn t) _schema)
  "Default: runtime schema switching is unsupported."
  (user-error "This backend does not support switching schemas"))

(cl-defgeneric clutch-db-list-table-entries (conn)
  "Return browseable table-like object entries for CONN.
Each entry is a plist containing at least :name and :type, and may also
include :schema, :source-schema, :target-schema, and :target-name.")

(cl-defmethod clutch-db-list-table-entries ((conn t))
  "Default table-entry implementation for CONN.
Derived from `clutch-db-list-tables'."
  (mapcar (lambda (table)
            (list :name table
                  :type "TABLE"))
          (clutch-db-list-tables conn)))

(cl-defgeneric clutch-db-list-columns (conn table)
  "Return a list of column name strings for TABLE on CONN.")

(cl-defgeneric clutch-db-complete-tables (conn prefix)
  "Return table name candidates for PREFIX on CONN, or nil when unsupported.")

(cl-defmethod clutch-db-complete-tables ((_conn t) _prefix)
  "Backends without direct completion support return nil."
  nil)

(cl-defgeneric clutch-db-search-table-entries (conn prefix)
  "Return table entry plists matching PREFIX on CONN, or nil when unsupported.")

(cl-defmethod clutch-db-search-table-entries ((conn t) prefix)
  "Default table entry search for CONN and PREFIX.
Derived from `clutch-db-complete-tables'."
  (mapcar (lambda (name) (list :name name :type "TABLE"))
          (or (clutch-db-complete-tables conn prefix) '())))

(cl-defgeneric clutch-db-browseable-object-entries (conn)
  "Return the base browseable object entry list for CONN.
This is the fast object-discovery snapshot used by clutch's object picker.")

(cl-defmethod clutch-db-browseable-object-entries ((conn t))
  "Default browseable-object snapshot for CONN.
Merges direct table-like entries with empty-prefix search-discovered entries."
  (append (clutch-db-list-table-entries conn)
          (clutch-db-search-table-entries conn "")))

(cl-defgeneric clutch-db-complete-columns (conn table prefix)
  "Return column candidates for TABLE and PREFIX on CONN.
Return nil when the backend does not support direct column completion.")

(cl-defmethod clutch-db-complete-columns ((_conn t) _table _prefix)
  "Backends without direct column completion support return nil."
  nil)

(cl-defgeneric clutch-db-show-create-table (conn table)
  "Return the DDL string for TABLE on CONN.")

(cl-defgeneric clutch-db-list-objects (conn category)
  "Return object entry plists for CATEGORY on CONN.
CATEGORY is one of: indexes, sequences, procedures, functions, triggers.")

(cl-defmethod clutch-db-list-objects ((_conn t) _category)
  "Default: return nil when CATEGORY is unsupported."
  nil)

(cl-defgeneric clutch-db-list-objects-async (conn category callback &optional errback)
  "Fetch object entry plists for CATEGORY on CONN asynchronously.
CALLBACK receives the entry plist list on success.  ERRBACK receives an error
message string on failure.  Return non-nil when an async fetch was started.")

(cl-defmethod clutch-db-list-objects-async ((_conn t) _category _callback &optional _errback)
  "Default: asynchronous object loading is unsupported."
  nil)

(cl-defgeneric clutch-db-object-details (conn entry)
  "Return detail data for object ENTRY on CONN.
ENTRY is the full entry plist so the backend can use :identity or
other backend-specific keys as needed.")

(cl-defmethod clutch-db-object-details ((_conn t) _entry)
  "Default: return nil when no detail loader is available."
  nil)

(cl-defgeneric clutch-db-object-source (conn entry)
  "Return source text for source-bearing object ENTRY on CONN.")

(cl-defmethod clutch-db-object-source ((_conn t) _entry)
  "Default: return nil when source is unavailable."
  nil)

(cl-defgeneric clutch-db-show-create-object (conn entry)
  "Return DDL text for non-table object ENTRY on CONN.")

(cl-defmethod clutch-db-show-create-object ((_conn t) _entry)
  "Default: return nil when DDL is unavailable."
  nil)

(cl-defgeneric clutch-db-table-comment (conn table)
  "Return the comment string for TABLE on CONN, or nil if none.")

(cl-defgeneric clutch-db-symbol-help (conn symbol)
  "Return backend-specific help for SYMBOL on CONN.
The return value is a plist with :sig and :desc, or nil when unsupported or
unknown.")

(cl-defmethod clutch-db-symbol-help ((_conn t) _symbol)
  "Fallback implementation for backends without live symbol help."
  nil)

(cl-defgeneric clutch-db-table-comment-async (conn table callback &optional errback)
  "Start an asynchronous table-comment fetch for TABLE on CONN.
CALLBACK receives the comment string or nil on success.  ERRBACK receives an
error message string on failure.  Return non-nil when async fetch was started,
nil when unsupported.")

(cl-defmethod clutch-db-table-comment-async ((_conn t) _table _callback
                                             &optional _errback)
  "Backends without asynchronous table-comment support return nil."
  nil)

(defmacro clutch-db--define-idle-metadata-methods (type backend-name)
  "Define idle metadata async methods for connection TYPE.
BACKEND-NAME is used only in generated docstrings."
  `(progn
     (cl-defmethod clutch-db-refresh-schema-async ((conn ,type) callback
                                                   &optional errback)
       ,(format "Refresh %s schema names on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-list-tables))

     (cl-defmethod clutch-db-list-columns-async ((conn ,type) table callback
                                                 &optional errback)
       ,(format "Fetch %s column names on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-list-columns table))

     (cl-defmethod clutch-db-column-details-async ((conn ,type) table callback
                                                   &optional errback)
       ,(format "Fetch %s column details on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-column-details table))

     (cl-defmethod clutch-db-table-comment-async ((conn ,type) table callback
                                                  &optional errback)
       ,(format "Fetch %s table comments on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-table-comment table))

     (cl-defmethod clutch-db-list-objects-async ((conn ,type) category callback
                                                 &optional errback)
       ,(format "Fetch %s object entries on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-list-objects category))))

(cl-defgeneric clutch-db-primary-key-columns (conn table)
  "Return a list of primary key column name strings for TABLE on CONN.")

(cl-defgeneric clutch-db-row-identity-candidates (conn table)
  "Return row identity candidate plists for TABLE on CONN.
Candidates are ordered from most stable to least stable.  A candidate with
:kind `primary-key' or `unique-key' has :columns as source column names.  A
candidate with :kind `row-locator' has :select-expressions as SQL expressions
that can be hidden in SELECT results and :where-sql as the predicate used by
UPDATE and DELETE.")

(cl-defmethod clutch-db-row-identity-candidates ((conn t) table)
  "Return the primary-key row identity candidate for CONN and TABLE."
  (condition-case nil
      (when-let* ((pk-cols (clutch-db-primary-key-columns conn table)))
        (list (list :kind 'primary-key
                    :name "PRIMARY"
                    :columns pk-cols)))
    (error nil)))

(cl-defgeneric clutch-db-foreign-keys (conn table)
  "Return foreign key info for TABLE on CONN.
Returns an alist of (COLUMN-NAME . (:ref-table T :ref-column C)).")

(cl-defgeneric clutch-db-referencing-objects (conn table)
  "Return objects that reference TABLE on CONN.
Each element is an entry plist suitable for object navigation, typically
including at least :name and :type, and optionally :schema / :source-schema.")

(cl-defmethod clutch-db-referencing-objects ((_conn t) _table)
  "Default: return nil when reverse-reference lookup is unsupported."
  nil)

(cl-defgeneric clutch-db-column-details (conn table)
  "Return detailed column info for TABLE on CONN.
Returns a list of plists with keys:
  :name STR  :type STR  :nullable BOOL
  :primary-key BOOL  :foreign-key PLIST-OR-NIL  :comment STR-OR-NIL
Optional keys:
  :default STR-OR-NIL  :generated BOOL")

;; Re-entrancy guard

(cl-defgeneric clutch-db-busy-p (conn)
  "Return non-nil if CONN is currently executing a query.
Used to prevent re-entrant queries from completion timers.")

;; Metadata

(cl-defgeneric clutch-db-user (conn)
  "Return the username string for CONN.")

(cl-defgeneric clutch-db-host (conn)
  "Return the host string for CONN.")

(cl-defgeneric clutch-db-port (conn)
  "Return the port number for CONN.")

(cl-defgeneric clutch-db-database (conn)
  "Return the current database name string for CONN.")

(cl-defgeneric clutch-db-display-name (conn)
  "Return a display name string for CONN's backend type.
E.g., \"MySQL\" or \"PostgreSQL\".")

;;;; Connect dispatcher

(defvar clutch-db--backend-features
  '((mysql  . (:require clutch-db-mysql
	       :connect-fn clutch-db-mysql-connect
	       :display-name "MySQL"
	       :default-port 3306
	       :sql-product mysql))
    (pg     . (:require clutch-db-pg
	       :connect-fn clutch-db-pg-connect
	       :display-name "PostgreSQL"
	       :default-port 5432
	       :sql-product postgres))
    (sqlite . (:require clutch-db-sqlite
	       :connect-fn clutch-db-sqlite-connect
	       :display-name "SQLite"
	       :sql-product sqlite)))
  "Alist mapping backend symbols to their feature plists.
Each plist has :require (the feature to load), :connect-fn (a function taking
a plist of connection params and returning a conn), and optional UI metadata
such as :display-name and :default-port.")

(defun clutch-db-backend-feature (backend)
  "Return the registered feature plist for BACKEND, loading JDBC if needed."
  (or (alist-get backend clutch-db--backend-features)
      (progn
        (require 'clutch-db-jdbc nil t)
        (alist-get backend clutch-db--backend-features))))

(defun clutch-db-backends (&optional load-optional)
  "Return registered backend symbols in user-facing order.
When LOAD-OPTIONAL is non-nil, load optional backend registries such as JDBC
before returning the list."
  (when load-optional
    (require 'clutch-db-jdbc nil t))
  (mapcar #'car clutch-db--backend-features))

(defun clutch-db-backend-display-name (backend)
  "Return registered display name for BACKEND, or nil."
  (and backend
       (plist-get (clutch-db-backend-feature backend) :display-name)))

(defun clutch-db-backend-default-port (backend)
  "Return registered default TCP port for BACKEND, or nil."
  (and backend
       (plist-get (clutch-db-backend-feature backend) :default-port)))

(defun clutch-db-backend-sql-product (backend)
  "Return registered `sql-product' symbol for BACKEND, or nil."
  (and backend
       (plist-get (clutch-db-backend-feature backend) :sql-product)))

(defun clutch-db-connect (backend params)
  "Connect to a database using BACKEND with PARAMS.
BACKEND is a symbol (e.g., \\='mysql, \\='pg).
PARAMS is a plist of connection parameters (:host, :port, :user,
:password, :database, etc.).
Returns a backend-specific connection object."
  (if-let* ((feature-plist
             (clutch-db-backend-feature backend))
            (connect-fn
             (progn
               (condition-case err
                   (require (plist-get feature-plist :require))
                 (file-missing
                  (pcase backend
                    ('mysql (user-error "MySQL backend requires the mysql package"))
                    ('pg (user-error "PostgreSQL backend requires the pg package"))
                    (_ (signal (car err) (cdr err))))))
               (plist-get feature-plist :connect-fn))))
      (condition-case err
          (let ((conn (funcall connect-fn params)))
            (clutch-db-init-connection conn)
            conn)
        (clutch-db-error
         (signal (car err) (cdr err)))
        (error
         (signal 'clutch-db-error
                 (list (format "Connection failed (%s): %s"
                               backend (error-message-string err))))))
    (user-error "Unknown backend: %s" backend)))

;;;; Temporal value formatting

(defun clutch-db-format-temporal (val)
  "Format temporal plist VAL as a string, or nil if VAL is not temporal.
Handles datetime (with :year and :hours), date (with :year only), and
time (with :hours only) plists returned by the protocol layers."
  (when (listp val)
    (let ((year (plist-get val :year))
          (month (plist-get val :month))
          (day (plist-get val :day))
          (hours (plist-get val :hours))
          (minutes (plist-get val :minutes))
          (seconds (plist-get val :seconds))
          (negative (plist-get val :negative)))
      (cond
       ((and year hours)
        (format "%04d-%02d-%02d %02d:%02d:%02d"
                year month day hours minutes seconds))
       (year
        (format "%04d-%02d-%02d" year month day))
       (hours
        (format "%s%02d:%02d:%02d"
                (if negative "-" "")
                hours minutes seconds))))))

(provide 'clutch-db)
;;; clutch-db.el ends here
