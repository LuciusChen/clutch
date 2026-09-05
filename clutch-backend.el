;;; clutch-backend.el --- Database backend protocol facade -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later


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

;;;; Configuration

(defcustom clutch-connect-timeout-seconds 10
  "Timeout in seconds for establishing a database connection.
Applies to networked backends.  SQLite ignores this setting."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-read-idle-timeout-seconds 30
  "Idle timeout in seconds while waiting for query I/O.
Applies to MySQL, PostgreSQL, and JDBC network I/O.  SQLite ignores this
setting."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-query-timeout-seconds 30
  "Timeout in seconds for database-side query execution.
Currently applied by PostgreSQL and JDBC.  Native MySQL does not yet enforce a
server-side statement timeout."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-jdbc-rpc-timeout-seconds 30
  "Timeout in seconds for round-trips to the JDBC agent process."
  :type 'natnum
  :group 'clutch)

;;;; Error types

(define-error 'clutch-db-error "Database error")
(define-error 'clutch-db-execution-not-started
  "Database operation did not start"
  'clutch-db-error)
(define-error 'clutch-db-session-restore-error
  "Database session state was not restored after a known batch outcome"
  'clutch-db-error)
(define-error 'clutch-db-batch-outcome-uncertain
  "Atomic mutation batch outcome is uncertain"
  'clutch-db-error)
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

(defvar clutch-backend--registry)

(defun clutch-backend-normalize (backend)
  "Return the canonical backend symbol for BACKEND.
Known aliases are read from `clutch-backend--registry'.  Unknown symbols are
returned unchanged so the normal backend lookup can report the final error."
  (let ((sym (clutch-db--normalize-symbol-option backend)))
    (or (and sym
             (cl-loop for (key . features) in clutch-backend--registry
                      when (or (eq sym key)
                               (memq sym (plist-get features :aliases)))
                      return key))
        (and sym
             (progn
               (require 'clutch-db-jdbc nil t)
               (cl-loop for (key . features) in clutch-backend--registry
                        when (or (eq sym key)
                                 (memq sym (plist-get features :aliases)))
                        return key)))
        sym)))

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

(defun clutch-db--normalize-connect-params (backend params)
  "Return connection PARAMS normalized for BACKEND."
  (let* ((params (clutch-db--reject-removed-connect-params params))
         (features (and backend (clutch-backend-feature backend)))
         (normalize-fn (plist-get features :normalize-fn)))
    (when (and (symbolp normalize-fn)
               (not (fboundp normalize-fn))
               (plist-get features :require))
      (require (plist-get features :require)))
    (if normalize-fn
        (funcall normalize-fn params)
      params)))

;;;; Result struct

(cl-defstruct clutch-db-value-preview
  "An incomplete result value, with TYPE, original LENGTH and preview TEXT.
This value may be displayed, but must not be used as complete database data."
  type length text)

(defun clutch-db-require-complete-value (value)
  "Return VALUE, or reject an incomplete result preview."
  (when (clutch-db-value-preview-p value)
    (user-error "%s value is only a preview (%s total); complete content is unavailable"
                (upcase (symbol-name (clutch-db-value-preview-type value)))
                (clutch-db-value-preview-length value)))
  value)

(cl-defstruct clutch-db-result
  "A database query result.
CONNECTION is the backend connection object.
COLUMNS is a list of plists.  Required keys are :name STR and
:type-category SYM, where :type-category is one of: numeric, blob, json,
text, date, time, datetime, other.  Backends may add :backend-type metadata.
ROWS is a list of lists (one per row).  Incomplete values use
`clutch-db-value-preview' and cannot be treated as complete write/export data.
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

(defvar clutch-db--idle-metadata-connections (make-hash-table :test 'eq)
  "Connections currently executing an idle metadata call.")

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

(defun clutch-db-sql-dialect (product)
  "Return the lexical rules for SQL PRODUCT as a plist.
These describe how a statement is tokenized, not how it executes, so they
only cover constructs that change where a literal or statement ends:
`:dollar-quotes' for PostgreSQL dollar-quoted bodies, and
`:backslash-escapes' for the MySQL family, where a backslash escapes the
next character inside a string literal.  PRODUCT is an `sql-mode' product
symbol as registered by `clutch-backend-sql-product'."
  (pcase product
    ('postgres '(:dollar-quotes t))
    ('mysql '(:backslash-escapes t))
    (_ nil)))

(defun clutch-db-connection-sql-dialect (conn)
  "Return the `clutch-db-sql-dialect' rules for CONN, or nil.
A backend registering explicit `:sql-dialect' rules wins; everything else
derives its rules from the registered `sql-mode' product.  The override
serves engines whose lexical rules have no product equivalent, such as
ClickHouse and Snowflake, or differ from their product's, such as
Redshift."
  (when conn
    (let ((backend (clutch-db-backend-key conn)))
      (or (plist-get (clutch-backend-feature backend) :sql-dialect)
          (clutch-db-sql-dialect
           (clutch-backend-sql-product backend))))))

(defun clutch-db-sql-skip-literal-or-comment (sql pos &optional identifiers dialect)
  "If POS in SQL is at a string literal or comment, return position past it.
Handles single-quoted strings (with `''' escape), -- line comments, and
/* block comments */.  When IDENTIFIERS is non-nil, also skip double-quoted,
backtick-quoted, and bracket-quoted identifiers, including doubled closing
delimiter escapes.  DIALECT is a `clutch-db-sql-dialect' plist; its
`:backslash-escapes' rule additionally treats a backslash as escaping the
next character inside a literal, and its `:dollar-quotes' rule skips
dollar-quoted bodies.  Returns nil when POS is at normal code."
  (let ((len (length sql))
        (backslash (plist-get dialect :backslash-escapes))
        (ch (and (< pos (length sql)) (aref sql pos))))
    (cond
     ((and (eq ch ?$) (plist-get dialect :dollar-quotes))
      (clutch-db-sql--skip-dollar-quote sql pos))
     ((let ((delimiter
             (cond
              ((eq ch ?\') ?\')
              ((and identifiers (memq ch '(?\" ?`))) ch)
              ((and identifiers (eq ch ?\[)) ?\]))))
        (when delimiter
          ;; Bracket-quoted identifiers have no backslash escape even in
          ;; dialects that use one inside quoted strings.
          (let ((escaping (and backslash (not (eq delimiter ?\])))))
            (cl-loop for i from (1+ pos) below len
                     do (cond
                         ((and escaping (= (aref sql i) ?\\))
                          (cl-incf i))
                         ((= (aref sql i) delimiter)
                          (if (and (< (1+ i) len)
                                   (= (aref sql (1+ i)) delimiter))
                              (cl-incf i)
                            (cl-return (1+ i)))))
                     finally return len)))))
     ((eq ch ?-)  ;; Possible -- line comment.
      (when (and (< (1+ pos) len) (= (aref sql (1+ pos)) ?-))
        (or (cl-loop for i from (+ pos 2) below len
                     when (= (aref sql i) ?\n) return (1+ i))
            len)))
     ((eq ch ?/)  ;; Possible /* block comment */.
      (when (and (< (1+ pos) len) (= (aref sql (1+ pos)) ?*))
        (or (cl-loop for i from (+ pos 2) below (1- len)
                     when (and (= (aref sql i) ?*)
                               (= (aref sql (1+ i)) ?/))
                     return (+ i 2))
            len))))))

(defun clutch-db-sql--skip-dollar-quote (sql pos)
  "Return the end of a PostgreSQL dollar-quoted body at POS, or nil.
Only PostgreSQL identifier-style tags are accepted.  An opener immediately
following an identifier character is rejected, as PostgreSQL requires lexical
separation there.  An unterminated opener consumes the rest of SQL so statement
selection fails closed.  This parser is linear and does not alter match data."
  (let ((len (length sql)))
    (when (and (< pos len)
               (= (aref sql pos) ?$)
               (or (zerop pos)
                   (not (let ((previous (aref sql (1- pos))))
                          (or (and (>= previous ?A) (<= previous ?Z))
                              (and (>= previous ?a) (<= previous ?z))
                              (and (>= previous ?0) (<= previous ?9))
                              (memq previous '(?_ ?$)))))))
      (let ((tag-end
             (cond
              ((and (< (1+ pos) len) (= (aref sql (1+ pos)) ?$))
               (+ pos 2))
              ((and (< (1+ pos) len)
                    (let ((first (aref sql (1+ pos))))
                      (or (and (>= first ?A) (<= first ?Z))
                          (and (>= first ?a) (<= first ?z))
                          (= first ?_))))
               (let ((index (+ pos 2)))
                 (while (and (< index len)
                             (let ((char (aref sql index)))
                               (or (and (>= char ?A) (<= char ?Z))
                                   (and (>= char ?a) (<= char ?z))
                                   (and (>= char ?0) (<= char ?9))
                                   (= char ?_))))
                   (setq index (1+ index)))
                 (and (< index len)
                      (= (aref sql index) ?$)
                      (1+ index)))))))
        (when tag-end
          (let* ((delimiter (substring sql pos tag-end))
                 (close (string-search delimiter sql tag-end)))
            (if close (+ close (length delimiter)) len)))))))

(defun clutch-db-sql-mask-literal-or-comment (sql &optional dialect)
  "Return a string the same length as SQL with literals/comments blanked.
Single-quoted content (between the quotes) and comment text become spaces.
Quote delimiters are preserved.  Double-quoted identifiers and backticks
are left intact.  DIALECT is a `clutch-db-sql-dialect' plist deciding where
a literal ends.  Safe for multibyte strings (avoids `aset')."
  (let ((pieces nil)
        (copy-from 0)
        (pos 0)
        (len (length sql)))
    (while (< pos len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment
                       sql pos nil dialect)))
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

(defun clutch-db-sql-map-placeholders (sql fn &optional dialect)
  "Replace each `?' placeholder in SQL with FN's result.
FN receives the zero-based placeholder ordinal and returns the replacement
string.  `??' collapses to a literal question mark and `?|' / `?&' pass
through untouched, so operators that spell themselves with a question mark
are not taken for placeholders.  Literals, comments, and (per DIALECT)
dollar-quoted bodies are copied verbatim.  Returns a cons of the rewritten
string and the number of placeholders replaced."
  (let ((len (length sql))
        (pos 0)
        (count 0)
        (copy-from 0)
        parts)
    (cl-flet ((emit (upto next)
                (push (substring sql copy-from upto) parts)
                (setq copy-from next pos next)))
      (while (< pos len)
        (if-let* ((skip (clutch-db-sql-skip-literal-or-comment
                         sql pos t dialect)))
            (setq pos skip)
          (let ((ch (aref sql pos))
                (next (and (< (1+ pos) len) (aref sql (1+ pos)))))
            (cond
             ((not (eq ch ??))
              (cl-incf pos))
             ((eq next ??)
              ;; Escaped literal question mark: emit one of the pair.
              (emit (1+ pos) (+ pos 2)))
             ((memq next '(?| ?&))
              (setq pos (+ pos 2)))
             (t
              (emit pos (1+ pos))
              (push (funcall fn count) parts)
              (cl-incf count))))))
      (emit len len))
    (cons (apply #'concat (nreverse parts)) count)))

(defun clutch-db-sql-scan-code (sql start end fn &optional dialect)
  "Scan SQL code characters from START to END, skipping strings/comments.
FN is called with (POS CHAR DEPTH), where DEPTH is the parenthesis depth before
CHAR is applied.  When FN returns non-nil, stop and return that value.
DIALECT is a `clutch-db-sql-dialect' plist selecting the lexical rules that
decide where literals end."
  (let ((pos (or start 0))
        (end (or end (length sql)))
        (depth 0)
        result)
    (while (and (< pos end) (not result))
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment
                       sql pos t dialect)))
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

(defun clutch-db-sql-statement-breaks (sql &optional dialect)
  "Return zero-based offsets of top-level semicolons in SQL.
Semicolons inside strings and comments do not count.  DIALECT is a
`clutch-db-sql-dialect' plist; its rules decide where a literal ends, so a
semicolon inside one is not a break."
  (let (breaks)
    (clutch-db-sql-scan-code
     sql 0 nil
     (lambda (pos ch depth)
       (when (and (zerop depth) (= ch ?\;))
         (push pos breaks))
       nil)
     dialect)
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

(defun clutch-db-sql-semicolon-statement-bounds
    (text offset &optional dialect)
  "Return zero-based statement bounds around OFFSET in TEXT.
Top-level semicolons delimit statements.  Semicolons inside strings and
comments are ignored.  DIALECT is a `clutch-db-sql-dialect' plist."
  (let ((beg 0)
        (end (length text))
        (effective-offset
         (clutch-db-sql-statement-effective-offset text offset)))
    (dolist (break (clutch-db-sql-statement-breaks text dialect))
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
    (text offset &optional strict-leading-space dialect)
  "Return zero-based semicolon statement bounds around OFFSET in TEXT.
When STRICT-LEADING-SPACE is non-nil and OFFSET is before the trimmed
statement body, return an empty range at OFFSET.  This lets execute-at-point
avoid running the previous statement from blank space between semicolon
delimited statements.  DIALECT is a `clutch-db-sql-dialect' plist."
  (let* ((bounds (clutch-db-sql-semicolon-statement-bounds
                  text offset dialect))
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
  "Return zero-based blank-line-delimited bounds in TEXT around OFFSET."
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

(defun clutch-db-sql-context-statement-bounds (text offset &optional dialect)
  "Return statement bounds for SQL context features in TEXT at OFFSET.
Use semicolon-aware bounds when TEXT has top-level semicolons; otherwise fall
back to blank-line paragraph bounds.  DIALECT is a `clutch-db-sql-dialect'
plist, so context features split statements the same way execution does."
  (if (clutch-db-sql-statement-breaks text dialect)
      (clutch-db-sql-semicolon-statement-bounds text offset dialect)
    (clutch-db-sql-blank-line-statement-bounds text offset)))

;;;; SQL helpers (top-level clause detection)

(defun clutch-db-sql-code-match-positions (sql start end regexp)
  "Return a hash mapping REGEXP match positions in SQL to their match ends.
Matching runs case-insensitively from START to END without interpreting SQL
structure, so callers must still confirm each position is top-level code
through `clutch-db-sql-scan-code'.  Collecting candidates in one pass keeps
that confirmation linear; retrying REGEXP at every scanned position instead
searches the remainder of SQL each time, which is quadratic."
  (let ((case-fold-search t)
        (limit (or end (length sql)))
        (positions (make-hash-table :test 'eq))
        (pos (or start 0)))
    (while (and (< pos limit)
                (string-match regexp sql pos)
                (< (match-beginning 0) limit))
      (when (<= (match-end 0) limit)
        (puthash (match-beginning 0) (match-end 0) positions))
      (setq pos (1+ (match-beginning 0))))
    positions))

(defun clutch-db-sql--clause-match-positions (sql start patterns)
  "Return a hash mapping match position to pattern for PATTERNS in SQL.
START is the initial search offset.  PATTERNS are case-insensitive regex
fragments matched with word boundaries; earlier patterns win a position."
  (let ((positions (make-hash-table :test 'eq)))
    (dolist (pattern (reverse patterns))
      (maphash (lambda (pos _end) (puthash pos pattern positions))
               (clutch-db-sql-code-match-positions
                sql start nil (format "\\b%s\\b" pattern))))
    positions))

(defun clutch-db-sql--top-level-clause-match (sql start patterns)
  "Return (POS . PATTERN) for the first top-level PATTERNS match in SQL.
START is the initial search offset.  PATTERNS are case-insensitive regex
fragments matched with word boundaries."
  ;; Collect candidate positions in one pass per pattern, then walk the code
  ;; once.  Testing each pattern at every scanned position instead searches
  ;; the remainder of SQL per position, which is quadratic on long statements.
  (let ((positions (clutch-db-sql--clause-match-positions sql start patterns)))
    (unless (zerop (hash-table-count positions))
      (clutch-db-sql-scan-code
       sql start nil
       (lambda (pos _ch depth)
         (and (zerop depth)
              (when-let* ((pattern (gethash pos positions)))
                (cons pos pattern))))))))

(defun clutch-db-sql-find-top-level-clause (sql pattern &optional start)
  "Return start position of top-level PATTERN in SQL, or nil.
PATTERN is matched case-insensitively with word boundaries.
START defaults to 0."
  (car (clutch-db-sql--top-level-clause-match
        sql (or start 0) (list pattern))))

(defun clutch-db-sql-has-top-level-clause-p (sql pattern &optional start)
  "Return non-nil when SQL has top-level PATTERN starting at START."
  (clutch-db-sql-find-top-level-clause sql pattern start))

(defun clutch-db-sql-has-top-level-row-limit-p (sql)
  "Return non-nil when SQL has a top-level row-limit clause.
Check TOP's SELECT modifier position and FETCH's FIRST/NEXT count syntax.
Preserve the established LIMIT and OFFSET checks across dialects.
Quoted text, comments and nested queries do not contribute clauses."
  (let* ((gap (concat "\\(?:"
                      (rx (or (in " \t\r\n")
                              (seq "/*"
                                   (* (or (not (in "*"))
                                          (seq (+ "*") (not (in "*/")))))
                                   (+ "*") "/")
                              (seq "--" (* (not (in "\n"))) "\n")))
                      "\\)"))
         (pattern
          (concat "\\b\\(?:LIMIT\\b\\|OFFSET\\b\\|FETCH" gap "+"
                  "\\(?:FIRST\\|NEXT\\)" gap "+"
                  "\\(?:[0-9]+\\b\\|ROWS?\\b\\|[?@$(:]\\)"
                  "\\|SELECT" gap "+\\(?:\\(?:ALL\\|DISTINCT\\)" gap "+\\)?"
                  "TOP\\b" gap "*\\(?:[0-9]+\\b\\|(\\)\\)"))
         (positions (clutch-db-sql-code-match-positions sql 0 nil pattern)))
    (unless (zerop (hash-table-count positions))
      (clutch-db-sql-scan-code
       sql 0 nil
       (lambda (pos _char depth)
         (and (zerop depth) (gethash pos positions)))))))

(defun clutch-db-sql-starts-with-keyword-p (sql keywords)
  "Return non-nil for SQL with one of KEYWORDS as the leading token."
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

(defconst clutch-db-sql--identifier-token-pattern
  "\\(?:`[^`]+`\\|\"[^\"]+\"\\|\\[[^]]+\\]\\|[^[:space:],();.]+\\)"
  "SQL identifier token pattern accepted by source-table helpers.")

(defconst clutch-db-sql--table-token-pattern
  (concat clutch-db-sql--identifier-token-pattern
          "\\(?:\\."
          clutch-db-sql--identifier-token-pattern
          "\\)*")
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

(defun clutch-db-sql--table-raw-parts (table)
  "Return delimiter-preserving identifier parts from TABLE, or nil."
  (let ((len (length table))
        (pos 0)
        parts
        valid)
    (while (< pos len)
      (let* ((start pos)
             (quoted-end (clutch-db-sql-skip-literal-or-comment table pos t))
             (end (or quoted-end
                      (cl-loop for i from pos below len
                               when (= (aref table i) ?.) return i
                               finally return len))))
        (if (= start end)
            (setq pos len parts nil)
          (push (substring table start end) parts)
          (setq pos end)
          (cond
           ((= pos len)
            (setq valid t))
           ((= (aref table pos) ?.)
            (cl-incf pos))
           (t
            (setq pos len parts nil))))))
    (and valid (nreverse parts))))

(defun clutch-db-sql-table-qualifier (table)
  "Return the exposed table qualifier from TABLE."
  (car (last (clutch-db-sql--table-raw-parts table))))

(defun clutch-db-sql--unquote-identifier (identifier)
  "Return IDENTIFIER without SQL identifier delimiters."
  (cond
   ((string-match "\\`\"\\([^\"]+\\)\"\\'" identifier)
    (match-string 1 identifier))
   ((string-match "\\``\\([^`]+\\)`\\'" identifier)
    (match-string 1 identifier))
   ((string-match "\\`\\[\\([^]]+\\)\\]\\'" identifier)
    (match-string 1 identifier))
   (t identifier)))

(defun clutch-db-sql-table-name (table)
  "Return the unquoted table name represented by TABLE."
  (when-let* ((name (clutch-db-sql-table-qualifier table)))
    (clutch-db-sql--unquote-identifier name)))

(defun clutch-db-sql-table-schema (table)
  "Return the unquoted schema qualifier represented by TABLE, or nil."
  (let ((parts (clutch-db-sql--table-raw-parts table)))
    (when (> (length parts) 1)
      (clutch-db-sql--unquote-identifier (nth (- (length parts) 2) parts)))))

(defun clutch-db-sql-table-catalog (table)
  "Return the unquoted catalog qualifier represented by TABLE, or nil."
  (let ((parts (clutch-db-sql--table-raw-parts table)))
    (when (> (length parts) 2)
      (clutch-db-sql--unquote-identifier (nth (- (length parts) 3) parts)))))

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

(defun clutch-db-sql-target-table (conn table sql)
  "Return a SQL reference to TABLE on CONN, retaining its source from SQL.
Only reuse SQL's relation when it is a simple query of that same TABLE."
  (or (when-let* ((token (and sql (clutch-db-sql--source-table-token sql t)))
                  ((equal table (clutch-db-sql-table-name token)))
                  ((clutch-db-sql-table-schema token)))
        token)
      (clutch-db-escape-identifier conn table)))

(defun clutch-db-sql-destructive-p (sql)
  "Return non-nil if SQL is a destructive operation."
  (clutch-db-sql-starts-with-keyword-p
   sql '("DELETE" "DROP" "TRUNCATE" "ALTER")))

(defun clutch-db-sql-schema-affecting-p (sql)
  "Return non-nil if SQL is likely to invalidate cached schema."
  (clutch-db-sql-starts-with-keyword-p
   sql '("CREATE" "ALTER" "DROP" "TRUNCATE" "RENAME")))

(defun clutch-db-sql-pageable-query-p (sql)
  "Return non-nil when SQL is a SELECT that accepts a pagination tail."
  (or (clutch-db-sql-starts-with-keyword-p sql '("SELECT"))
      (and (clutch-db-sql-starts-with-keyword-p sql '("WITH"))
           (equal (clutch-db-sql-main-op-keyword sql) "SELECT"))))

(defun clutch-db-sql-select-query-p (sql)
  "Return non-nil for SQL that yields a result set."
  (or (clutch-db-sql-pageable-query-p sql)
      (clutch-db-sql-starts-with-keyword-p
       sql '("DESCRIBE" "DESC" "SHOW" "EXPLAIN"))))

(defun clutch-db-sql-strip-top-level-order-by (sql)
  "Strip a top-level ORDER BY tail from SQL.
Leaves nested ORDER BY clauses inside subqueries or window functions intact."
  (if-let* ((order-pos (clutch-db-sql-find-top-level-clause sql "ORDER\\s-+BY")))
      (string-trim-right (substring sql 0 order-pos))
    sql))

(defun clutch-db-sql-derived-table-body (sql)
  "Return SQL normalized for derived-table wrapping.
Top-level ORDER BY is removed when there is no top-level row limit because
it is invalid inside derived tables on some dialects and cannot change the
derived row set.  Limited result sets keep their tail clauses so wrapping
targets the user's visible result set."
  (let ((normalized (clutch-db-sql-normalize sql)))
    (if (clutch-db-sql-has-top-level-row-limit-p normalized)
        normalized
      (clutch-db-sql-strip-top-level-order-by normalized))))

(defun clutch-db-sql-count-derived-table-body (sql)
  "Return SQL normalized for COUNT(*) derived-table wrapping.
Top-level ORDER BY is removed when there is no top-level row limit because
it cannot affect the row count.  Limited result sets keep their tail clauses so
counts target the user's visible result set."
  (clutch-db-sql-derived-table-body sql))

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
  (if (clutch-db-sql-has-top-level-row-limit-p base-sql)
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
  "Finish the current transaction on CONN.
Return `rolled-back' when the backend had to roll back an already failed
transaction instead of committing it.  Other successful commits may return
any value except `rolled-back'.")

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
  "Signal unsupported runtime auto-commit toggling for this backend."
  (user-error "Manual commit is not supported by this connection"))

(defun clutch-db--signal-batch-outcome-uncertain
    (phase original-error &optional recovery-error)
  "Signal that a mutation batch outcome is uncertain.
PHASE is either `commit' or `recovery'.  ORIGINAL-ERROR is the condition that
started that phase.  RECOVERY-ERROR is the failed recovery condition, when
PHASE is `recovery'."
  (signal
   'clutch-db-batch-outcome-uncertain
   (list
    (if recovery-error
        (format "Mutation batch failed (%s); recovery also failed (%s); outcome is uncertain"
                (error-message-string original-error)
                (error-message-string recovery-error))
      (format "Mutation batch commit failed (%s); commit outcome is uncertain"
              (error-message-string original-error)))
    :phase phase
    :original original-error
    :recovery recovery-error)))

(defun clutch-db--recover-batch-or-signal-uncertain
    (original-error recover)
  "Call RECOVER after ORIGINAL-ERROR or signal an uncertain batch outcome."
  (condition-case recovery-error
      (funcall recover)
    ((error quit)
     (clutch-db--signal-batch-outcome-uncertain
      'recovery original-error recovery-error))))

(defun clutch-db--call-batch-body-with-recovery (function recover)
  "Call FUNCTION, using RECOVER if it signals.
After successful recovery, re-signal FUNCTION's original condition."
  (condition-case original-error
      (funcall function)
    ((error quit)
     (clutch-db--recover-batch-or-signal-uncertain
      original-error recover)
     (signal (car original-error) (cdr original-error)))))

(defun clutch-db--call-with-transaction-boundary
    (open function commit rollback)
  "Call FUNCTION inside a transaction opened by OPEN.
COMMIT finishes successful work.  ROLLBACK recovers a FUNCTION failure before
the original condition is re-signaled.  A failed rollback or any COMMIT error
signals `clutch-db-batch-outcome-uncertain'; COMMIT errors are never followed
by rollback because the server may already have committed."
  (funcall open)
  (let ((result
         (clutch-db--call-batch-body-with-recovery function rollback)))
    (condition-case commit-error
        (progn
          (funcall commit)
          result)
      ((error quit)
       (clutch-db--signal-batch-outcome-uncertain
        'commit commit-error)))))

(defun clutch-db--call-with-savepoint-boundary
    (open function release recover)
  "Call FUNCTION inside a savepoint opened by OPEN.
RELEASE finishes successful work.  RECOVER rolls back to the savepoint after a
FUNCTION or RELEASE failure, then the original condition is re-signaled.  A
failed recovery signals `clutch-db-batch-outcome-uncertain'."
  (funcall open)
  (let ((result
         (clutch-db--call-batch-body-with-recovery function recover)))
    (condition-case release-error
        (progn
          (funcall release)
          result)
      ((error quit)
       (clutch-db--recover-batch-or-signal-uncertain
        release-error recover)
       (signal (car release-error) (cdr release-error))))))

(defvar clutch-db--savepoint-sequence 0
  "Sequence used to generate collision-resistant staged-submit savepoint names.")

(defconst clutch-db--savepoint-namespace
  (format "%d_%d" (emacs-pid) (random most-positive-fixnum))
  "Process-local namespace for staged-submit SQL savepoints.")

(defun clutch-db--call-with-sql-savepoint (conn function ensure-transaction)
  "Call FUNCTION inside a SQL savepoint on CONN.
ENSURE-TRANSACTION is called before creating the savepoint and must leave an
outer transaction open without committing user work."
  (let ((name (format "clutch_submit_%s_%d"
                      clutch-db--savepoint-namespace
                      (cl-incf clutch-db--savepoint-sequence))))
    (clutch-db--call-with-savepoint-boundary
     (lambda ()
       (funcall ensure-transaction)
       (clutch-db-query conn (format "SAVEPOINT %s" name)))
     function
     (lambda ()
       (clutch-db-query conn (format "RELEASE SAVEPOINT %s" name)))
     (lambda ()
       (clutch-db-query conn (format "ROLLBACK TO SAVEPOINT %s" name))
       (clutch-db-query conn (format "RELEASE SAVEPOINT %s" name))))))

(cl-defgeneric clutch-db-call-with-atomic-batch (conn function)
  "Call zero-argument FUNCTION as one atomic mutation batch on CONN.
In Auto mode, use a backend-owned transaction and commit all work when FUNCTION
returns.  In Manual mode, use a savepoint inside the user-owned transaction and
leave that outer transaction uncommitted.  In either mode, roll back all work
performed by FUNCTION when it signals and preserve the selected transaction
mode.  A backend that cannot restore its original session mode after a known
outcome may signal `clutch-db-session-restore-error' with an `:outcome' of
`committed' or `rolled-back'.  An unknown commit outcome or failed recovery
signals `clutch-db-batch-outcome-uncertain'.")

(cl-defmethod clutch-db-call-with-atomic-batch ((_conn t) _function)
  "Reject atomic mutation batches on unsupported connections."
  (user-error "Atomic mutation submission is not supported by this connection"))

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

(cl-defgeneric clutch-db-completion-deferred-columns-p (conn)
  "Return non-nil when CONN defers uncached completion column metadata.")

(cl-defmethod clutch-db-completion-deferred-columns-p ((_conn t))
  "Backends use their ordinary synchronous or direct completion path by default."
  nil)

(cl-defgeneric clutch-db-refresh-schema-async (conn callback &optional errback
                                                   idle-delay)
  "Start an asynchronous schema refresh for CONN.
CALLBACK receives the table name list on success.  ERRBACK receives
an error message string on failure.  Return non-nil when async refresh
was started, nil when unsupported.
IDLE-DELAY, when non-nil, delays low-priority idle refresh work by at least
that many seconds before it may run.")

(cl-defmethod clutch-db-refresh-schema-async ((_conn t) _callback
                                              &optional _errback _idle-delay)
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

(cl-defgeneric clutch-db-foreign-keys-async (conn table callback &optional errback)
  "Start an asynchronous foreign-key fetch for TABLE on CONN.
CALLBACK receives the foreign-key alist on success.  ERRBACK receives an
error message string on failure.  Return non-nil when async fetch was
started, nil when unsupported.")

(cl-defmethod clutch-db-foreign-keys-async ((_conn t) _table _callback
                                             &optional _errback)
  "Backends without asynchronous foreign-key metadata support return nil."
  nil)

(defun clutch-db--schedule-idle-metadata-call (conn callback errback fn
                                                    &optional initial-delay
                                                    &rest args)
  "Schedule metadata FN for CONN on the main thread once Emacs is idle.
CALLBACK receives the result of calling FN with CONN and ARGS.
ERRBACK receives an error-message string when the work fails.
INITIAL-DELAY, when positive, is the idle delay before the first attempt."
  (cl-labels
      ((run ()
         (if (clutch-db-live-p conn)
             (if (or (clutch-db-busy-p conn)
                     (clutch-db--foreground-busy-p conn)
                     (gethash conn clutch-db--idle-metadata-connections))
                 (run-with-idle-timer 0.1 nil #'run)
               (puthash conn t clutch-db--idle-metadata-connections)
               (unwind-protect
                   (condition-case err
                       (when callback
                         (funcall callback (apply fn conn args)))
                     (error
                      (when errback
                        (funcall errback (error-message-string err)))))
                 (remhash conn clutch-db--idle-metadata-connections)))
           (when errback
             (funcall errback "Connection closed")))))
    (run-with-idle-timer (or initial-delay 0) nil #'run)))

;; Query

(cl-defgeneric clutch-db-query (conn sql)
  "Execute SQL on CONN and return a `clutch-db-result'.")

(cl-defgeneric clutch-db-result-query-p (conn sql)
  "Return non-nil when SQL should render as a tabular result for CONN.")

(cl-defmethod clutch-db-result-query-p ((_conn t) sql)
  "Return non-nil when SQL is a normal SQL result-set query."
  (clutch-db-sql-select-query-p sql))

(cl-defgeneric clutch-db-query-result-context (conn sql)
  "Return result-buffer context plist for SQL on CONN.")

(cl-defmethod clutch-db-query-result-context ((_conn t) _sql)
  "Default: return no additional result-buffer context."
  nil)

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
                                (lambda (param)
                                  (clutch-db-value-to-literal
                                   conn param))
                                (clutch-db-connection-sql-dialect conn))))

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
  "Return source table name for CONN and SQL table TOKEN.")

(cl-defmethod clutch-db--source-table-name ((_conn t) token)
  "Return the default backend-canonical source table name for TOKEN."
  (clutch-db-sql-table-name token))

(cl-defgeneric clutch-db--source-table-schema (conn token)
  "Return source schema for CONN and SQL table TOKEN, or nil.")

(cl-defmethod clutch-db--source-table-schema ((_conn t) token)
  "Return nil because TOKEN has no default schema-aware metadata scope."
  (ignore token)
  nil)

(cl-defgeneric clutch-db--source-table-catalog (conn token)
  "Return source catalog for CONN and SQL table TOKEN, or nil.")

(cl-defmethod clutch-db--source-table-catalog ((_conn t) token)
  "Return nil because TOKEN has no default catalog-aware metadata scope."
  (ignore token)
  nil)

(cl-defgeneric clutch-db-escape-literal (conn value)
  "Escape VALUE as a SQL string literal for CONN's dialect.")

(cl-defgeneric clutch-db-derived-table-alias (conn alias)
  "Return CONN's derived-table alias clause for ALIAS.")

(cl-defmethod clutch-db-derived-table-alias ((_conn t) alias)
  "Return the default SQL derived-table alias clause for ALIAS."
  (format "AS %s" alias))

(cl-defstruct (clutch-db-param
               (:constructor clutch-db-param-create)
               (:conc-name clutch-db-param--))
  "A SQL parameter value with optional backend type metadata.
VALUE is the raw Elisp value.  TYPE is backend-specific metadata such as a
PostgreSQL type name."
  value type)

(defun clutch-db-typed-param (value type)
  "Return VALUE tagged with backend TYPE for parameterized SQL.
When TYPE is nil, return VALUE unchanged."
  (if type
      (clutch-db-param-create :value value :type type)
    value))

(defun clutch-db-param-value (param)
  "Return PARAM's raw value, ignoring type metadata."
  (if (clutch-db-param-p param)
      (clutch-db-param--value param)
    param))

(defun clutch-db-param-type (param)
  "Return PARAM's backend type metadata, or nil."
  (when (clutch-db-param-p param)
    (clutch-db-param--type param)))

(defun clutch-db-param-values (params)
  "Return PARAMS with backend type metadata removed."
  (mapcar #'clutch-db-param-value params))

(defun clutch-db--basic-value-to-literal (conn value &optional fallback-format-fn)
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

(cl-defgeneric clutch-db-value-to-typed-literal
    (conn value type fallback-format-fn)
  "Render VALUE as a SQL literal for CONN using backend TYPE metadata.")

(cl-defmethod clutch-db-value-to-typed-literal
    ((conn t) value _type fallback-format-fn)
  "Render VALUE for CONN, ignoring backend TYPE metadata by default."
  (clutch-db--basic-value-to-literal conn value fallback-format-fn))

(defun clutch-db-value-to-literal (conn param &optional fallback-format-fn)
  "Render PARAM as a SQL literal for CONN.
PARAM may be a raw value or a `clutch-db-param' with backend type metadata.
FALLBACK-FORMAT-FN formats non-scalar result values before string escaping."
  (let ((value (clutch-db-param-value param))
        (type (clutch-db-param-type param)))
    (if type
        (clutch-db-value-to-typed-literal conn value type fallback-format-fn)
      (clutch-db--basic-value-to-literal conn value fallback-format-fn))))

(defun clutch-db-substitute-params (sql params render-fn &optional dialect)
  "Return SQL with PARAMS substituted using RENDER-FN.
SQL uses `?' positional placeholders; `clutch-db-sql-map-placeholders'
decides which question marks are placeholders.  PARAMS is a list of
parameter values.  RENDER-FN is called once per parameter and must return
the replacement string.  DIALECT is a `clutch-db-sql-dialect' plist
deciding where literals end."
  (let* ((remaining params)
         (rendered
          (clutch-db-sql-map-placeholders
           sql
           (lambda (_index)
             (unless remaining
               (signal 'clutch-db-error
                       (list (format "Not enough parameters for SQL template: %s" sql))))
             (prog1 (funcall render-fn (car remaining))
               (setq remaining (cdr remaining))))
           dialect)))
    (when remaining
      (signal 'clutch-db-error
              (list (format "Too many parameters for SQL template: %s" sql))))
    (car rendered)))

;; Schema

(cl-defgeneric clutch-db-list-tables (conn)
  "Return a list of table name strings for CONN's current database.")

(cl-defgeneric clutch-db-list-schemas (conn)
  "Return switchable schema/database names for CONN, or nil when unsupported.")

(cl-defmethod clutch-db-list-schemas ((_conn t))
  "Backends without schema enumeration support return nil."
  nil)

(cl-defgeneric clutch-db-current-schema (conn)
  "Return CONN's effective current schema/database, or nil when not applicable.")

(cl-defmethod clutch-db-current-schema ((_conn t))
  "Default: no current schema abstraction."
  nil)

(cl-defgeneric clutch-db-set-current-schema (conn schema)
  "Switch CONN to SCHEMA for subsequent metadata and query context.")

(cl-defmethod clutch-db-set-current-schema ((_conn t) _schema)
  "Default: runtime schema switching is unsupported."
  (user-error "This backend does not support switching schemas"))

(cl-defgeneric clutch-db-update-namespace-params (conn params)
  "Return connection PARAMS updated with CONN's current namespace.")

(cl-defmethod clutch-db-update-namespace-params ((conn t) params)
  "Store CONN's current schema in a copy of connection PARAMS."
  (let ((schema (clutch-db-current-schema conn)))
    (unless schema
      (error "Backend switched namespace without reporting a current schema"))
    (plist-put (copy-sequence params) :schema schema)))

(cl-defgeneric clutch-db-namespace-reconnect-params (conn params namespace)
  "Return replacement PARAMS when switching CONN to NAMESPACE needs reconnecting.")

(cl-defmethod clutch-db-namespace-reconnect-params ((_conn t) _params _namespace)
  "Default to switching the namespace within the existing connection."
  nil)

(cl-defgeneric clutch-db-list-table-entries (conn)
  "Return browseable table-like object entries for CONN.
Each entry is a plist containing at least :name and :type, and may also
include :schema, :source-schema, :target-schema, :target-name, and :comment.")

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

(cl-defgeneric clutch-db-find-table-entry (conn name)
  "Return the exact table-like entry named NAME on CONN, or nil.")

(cl-defmethod clutch-db-find-table-entry ((conn t) name)
  "Find NAME exactly in the table-entry search results for CONN."
  (cl-find-if
   (lambda (entry)
     (string= name (or (plist-get entry :name) "")))
   (clutch-db-search-table-entries conn name)))

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

(cl-defgeneric clutch-db-object-entry-metadata (conn entry)
  "Return object ENTRY augmented with display metadata for CONN.
Backends may add cheap, caller-facing metadata used by object pickers.")

(cl-defmethod clutch-db-object-entry-metadata ((_conn t) entry)
  "Default: return ENTRY unchanged."
  entry)

(cl-defgeneric clutch-db-object-source (conn entry)
  "Return source text for source-bearing object ENTRY on CONN.")

(cl-defmethod clutch-db-object-source ((_conn t) _entry)
  "Default: return nil when source is unavailable."
  nil)

(cl-defgeneric clutch-db-object-definition (conn entry)
  "Return definition or source text for object ENTRY on CONN.")

(cl-defmethod clutch-db-object-definition ((_conn t) _entry)
  "Default: return nil when object definition is unavailable."
  nil)

(cl-defgeneric clutch-db-object-browse-query (conn entry)
  "Return query-console text to browse object ENTRY on CONN.
Return nil when CONN does not provide a backend-specific browse query.")

(cl-defmethod clutch-db-object-browse-query ((_conn t) _entry)
  "Default: object browsing is built by the Clutch UI."
  nil)

(cl-defgeneric clutch-db-collection-profile (conn collection)
  "Return schema/profile metadata text for COLLECTION on CONN.")

(cl-defmethod clutch-db-collection-profile ((_conn t) _collection)
  "Default: return nil when collection profile metadata is unavailable."
  nil)

(cl-defgeneric clutch-db-object-action-supported-p (conn entry action-id)
  "Return non-nil when ACTION-ID is supported for object ENTRY on CONN.")

(cl-defmethod clutch-db-object-action-supported-p ((_conn t) _entry _action-id)
  "Default: backend-specific object actions are unsupported."
  nil)

(cl-defgeneric clutch-db-object-action-metadata (conn entry action-id)
  "Return metadata text for backend ACTION-ID on object ENTRY using CONN.")

(cl-defmethod clutch-db-object-action-metadata ((_conn t) _entry _action-id)
  "Default: return nil when backend-specific action metadata is unavailable."
  nil)

(cl-defgeneric clutch-db-document-mutation-supported-p (conn action)
  "Return non-nil when CONN can build document mutation ACTION snippets.")

(cl-defmethod clutch-db-document-mutation-supported-p ((_conn t) _action)
  "Default: document mutation snippets are unsupported."
  nil)

(cl-defgeneric clutch-db-document-mutation-snippets
    (conn action collection documents &optional fields)
  "Return document mutation snippets for ACTION on COLLECTION using CONN.
DOCUMENTS is a list of backend-native documents.  FIELDS is an optional list of
field names for field-scoped actions.")

(cl-defmethod clutch-db-document-mutation-snippets
  ((_conn t) action _collection _documents &optional _fields)
  "Default: signal that ACTION is unsupported for document mutation snippets."
  (user-error "Document mutation %s is not available for this backend" action))

(cl-defgeneric clutch-db-explain-query (conn query)
  "Return explain metadata text for QUERY on CONN.")

(cl-defmethod clutch-db-explain-query ((_conn t) _query)
  "Default: return nil when query explain is unavailable."
  nil)

(cl-defgeneric clutch-db-table-comment (conn table &optional schema)
  "Return TABLE's comment on CONN, or nil if none.
SCHEMA, when non-nil, identifies TABLE's namespace.")

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
                                                   &optional errback
                                                   idle-delay)
       ,(format "Refresh %s schema names on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-list-tables idle-delay))

     (cl-defmethod clutch-db-list-columns-async ((conn ,type) table callback
                                                 &optional errback)
       ,(format "Fetch %s column names on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-list-columns nil table))

     (cl-defmethod clutch-db-column-details-async ((conn ,type) table callback
                                                   &optional errback)
       ,(format "Fetch %s column details on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-column-details nil table))

     (cl-defmethod clutch-db-table-comment-async ((conn ,type) table callback
                                                  &optional errback)
       ,(format "Fetch %s table comments on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-table-comment nil table))

     (cl-defmethod clutch-db-foreign-keys-async ((conn ,type) table callback
                                                 &optional errback)
       ,(format "Fetch %s foreign keys on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-foreign-keys nil table))

     (cl-defmethod clutch-db-list-objects-async ((conn ,type) category callback
                                                 &optional errback)
       ,(format "Fetch %s object entries on the main thread when idle."
                backend-name)
       (clutch-db--schedule-idle-metadata-call
        conn callback errback #'clutch-db-list-objects nil category))))

(cl-defgeneric clutch-db-primary-key-columns (conn table)
  "Return a list of primary key column name strings for TABLE on CONN.")

(cl-defmethod clutch-db-primary-key-columns ((_conn t) _table)
  "Return nil because CONN has no default primary-key metadata support."
  nil)

(cl-defgeneric clutch-db-row-identity-candidates (conn table &optional schema catalog)
  "Return row identity candidate plists for TABLE on CONN.
SCHEMA identifies TABLE's namespace when the SQL source was qualified.
CATALOG identifies the enclosing JDBC catalog when present.
Candidates are ordered from most stable to least stable.  A candidate with
:kind `primary-key' or `unique-key' has :columns as source column names.  A
candidate with :kind `row-locator' has :select-expressions as SQL expressions
that can be hidden in SELECT results and :where-sql as the predicate used by
UPDATE and DELETE.")

(cl-defmethod clutch-db-row-identity-candidates ((conn t) table
                                                 &optional _schema _catalog)
  "Return the primary-key row identity candidate for CONN and TABLE."
  (when-let* ((pk-cols (clutch-db-primary-key-columns conn table)))
    (list (list :kind 'primary-key
                :name "PRIMARY"
                :columns pk-cols))))

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
  :default STR-OR-NIL  :generated BOOL  :backend-type ANY")

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

(defvar clutch-backend--registry
  '((mysql  . (:require clutch-db-mysql
               :aliases (mariadb)
               :connect-fn clutch-db-mysql-connect
               :normalize-fn clutch-db-mysql--normalize-connect-params
               :display-name "MySQL"
               :default-port 3306
               :support-level core
               :data-model relational
               :update-default t
               :sql-product mysql))
    (pg     . (:require clutch-db-pg
               :aliases (postgres postgresql)
               :connect-fn clutch-db-pg-connect
               :normalize-fn clutch-db-pg--normalize-connect-params
               :display-name "PostgreSQL"
               :default-port 5432
               :support-level core
               :data-model relational
               :update-default t
               :sql-product postgres))
    (sqlite . (:require clutch-db-sqlite
               :connect-fn clutch-db-sqlite-connect
               :display-name "SQLite"
               :support-level core
               :data-model relational
               :sql-product sqlite))
    (mongodb . (:require clutch-mongodb
                :aliases (mongo)
                :connect-fn clutch-mongodb-connect
                :display-name "MongoDB"
                :default-port 27017
                :support-level basic
                :data-model document
                :query-mode clutch-mongodb-mode
                :query-mode-require clutch-document
                :surfaces ((sql-interface . (:query-mode clutch-mode
                                              :execution-model sql
                                              :transport jdbc)))))
    (redis . (:require clutch-redis
              :connect-fn clutch-redis-connect
              :display-name "Redis"
              :default-port 6379
              :support-level basic
              :data-model key-value
              :query-mode clutch-redis-mode)))
  "Alist mapping backend symbols to their feature plists.
Each plist has :require (the feature to load), :connect-fn (a function taking
a plist of connection params and returning a conn), and optional :aliases,
:normalize-fn plus UI metadata such as :display-name, :default-port,
:support-level, :data-model, :query-mode, :surfaces, and :manual-choice, plus
capability metadata such as :update-default.
Surface entries may set :execution-model and :transport for non-default
execution paths.")

(defun clutch-backend-feature (backend)
  "Return the registered feature plist for BACKEND.
Load optional registries if needed."
  (or (alist-get backend clutch-backend--registry)
      (progn
        (require 'clutch-db-jdbc nil t)
        (alist-get backend clutch-backend--registry))))

(defun clutch-backends (&optional load-optional)
  "Return registered backend symbols in user-facing order.
When LOAD-OPTIONAL is non-nil, load optional backend registries such as JDBC
before returning the list."
  (when load-optional
    (require 'clutch-db-jdbc nil t))
  (mapcar #'car clutch-backend--registry))

(defun clutch-backend-display-name (backend)
  "Return registered display name for BACKEND, or nil."
  (and backend
       (plist-get (clutch-backend-feature backend) :display-name)))

(defun clutch-backend-default-port (backend)
  "Return registered default TCP port for BACKEND, or nil."
  (and backend
       (plist-get (clutch-backend-feature backend) :default-port)))

(defun clutch-backend-support-level (backend)
  "Return registered support level for BACKEND, or nil."
  (and backend
       (plist-get (clutch-backend-feature backend) :support-level)))

(defun clutch-backend-data-model (backend)
  "Return registered data model for BACKEND, or nil."
  (and backend
       (plist-get (clutch-backend-feature
                   (clutch-backend-normalize backend))
                  :data-model)))

(defun clutch-backend-update-default-p (backend)
  "Return non-nil when BACKEND supports DEFAULT in UPDATE assignments."
  (plist-get (clutch-backend-feature backend) :update-default))

(defun clutch-backend-surface-feature (backend surface)
  "Return registered feature plist for BACKEND SURFACE, or nil."
  (let* ((backend (clutch-backend-normalize backend))
         (surface (clutch-db--normalize-symbol-option surface))
         (features (and backend (clutch-backend-feature backend))))
    (and surface
         (alist-get surface (plist-get features :surfaces)))))

(defun clutch-backend-sql-execution-p (backend params)
  "Return non-nil if BACKEND with PARAMS is configured for SQL execution."
  (let* ((backend (clutch-backend-normalize backend))
         (surface-features
          (clutch-backend-surface-feature backend (plist-get params :surface))))
    (or (eq (clutch-backend-data-model backend) 'relational)
        (eq (plist-get surface-features :execution-model) 'sql))))

(defun clutch-backend-jdbc-transport-p (backend params)
  "Return non-nil when BACKEND with PARAMS connects through JDBC."
  (let* ((backend (clutch-backend-normalize backend))
         (features (and backend (clutch-backend-feature backend)))
         (surface-features
          (clutch-backend-surface-feature backend (plist-get params :surface))))
    (or (eq (plist-get features :require) 'clutch-db-jdbc)
        (eq (plist-get surface-features :transport) 'jdbc))))

(defun clutch-db-native-document-surface-p (conn params)
  "Return non-nil when CONN and PARAMS describe a native document surface."
  (let ((backend (and conn (clutch-db-backend-key conn))))
    (and (eq (clutch-backend-data-model backend) 'document)
         (not (clutch-backend-sql-execution-p backend params)))))

(defun clutch-db-sql-surface-p (conn params)
  "Return non-nil when CONN and PARAMS describe a SQL execution surface."
  (let ((backend (or (and conn (clutch-db-backend-key conn))
                     (clutch-db--normalize-symbol-option
                      (or (plist-get params :backend)
                          (plist-get params :driver))))))
    (clutch-backend-sql-execution-p backend params)))

(defun clutch-backend-manual-choice-p (backend)
  "Return non-nil if manual connection UI should include BACKEND."
  (when-let* ((features (clutch-backend-feature backend)))
    (if (plist-member features :manual-choice)
        (plist-get features :manual-choice)
      t)))

(defun clutch-backend-sql-product (backend)
  "Return registered `sql-product' symbol for BACKEND, or nil."
  (and backend
       (plist-get (clutch-backend-feature backend) :sql-product)))

(defun clutch-backend-query-mode (backend &optional params)
  "Return query-console major mode for BACKEND and PARAMS, or nil.
Backends may register a default :query-mode and optional surface-specific
entries in :surfaces.  Each surface entry is an alist element whose car is a
surface symbol and whose cdr may contain :query-mode and
:query-mode-require."
  (when-let* ((features (and backend (clutch-backend-feature backend))))
    (let* ((surface (clutch-db--normalize-symbol-option
                     (plist-get params :surface)))
           (surface-features
            (and surface (alist-get surface (plist-get features :surfaces))))
           (require-feature
            (if surface-features
                (plist-get surface-features :query-mode-require)
              (plist-get features :query-mode-require)))
           (query-mode
            (or (plist-get surface-features :query-mode)
                (plist-get features :query-mode))))
      (when require-feature
        (require require-feature))
      query-mode)))

(defun clutch-db-connect (backend params)
  "Connect to a database using BACKEND with PARAMS.
BACKEND is a symbol (e.g., \\='mysql, \\='pg).
PARAMS is a plist of connection parameters (:host, :port, :user,
:password, :database, etc.).
Returns a backend-specific connection object."
  (if-let* ((feature-plist
             (clutch-backend-feature backend))
            (connect-fn
             (progn
               (condition-case err
                   (require (plist-get feature-plist :require))
                 (file-missing
                  (pcase backend
                    ('mysql (user-error "MySQL backend requires the mysql package"))
                    ('pg (user-error "PostgreSQL backend requires pgsql.el"))
                    (_ (signal (car err) (cdr err))))))
               (plist-get feature-plist :connect-fn))))
      (condition-case err
          ;; Initialization runs statements, so it can fail or be quit after
          ;; the backend already holds a socket.  Close that connection rather
          ;; than losing the only reference to it.
          (let (conn established)
            (unwind-protect
                (progn
                  (setq conn (funcall connect-fn params))
                  (clutch-db-init-connection conn)
                  (setq established t)
                  conn)
              (when (and conn (not established))
                (ignore-errors (clutch-db-disconnect conn)))))
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

(provide 'clutch-backend)
;;; clutch-backend.el ends here
