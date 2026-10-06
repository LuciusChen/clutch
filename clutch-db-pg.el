;;; clutch-db-pg.el --- Native backend over the PostgreSQL client -*- lexical-binding: t; -*-

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

;; PostgreSQL backend for the clutch generic database interface.  A small
;; adapter-owned wrapper keeps Clutch state separate from pgsql.el's opaque
;; protocol connection.  The xtdb backend reuses this adapter for XTDB 2,
;; which speaks the same protocol.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'json)

(declare-function pgsql-array-literal "pgsql" (value type))
(declare-function pgsql-busy-p "pgsql" (connection))
(declare-function pgsql-cancel "pgsql" (connection))
(declare-function pgsql-connect "pgsql" (&rest parameters))
(declare-function pgsql-disconnect "pgsql" (connection))
(declare-function pgsql-database "pgsql" (connection))
(declare-function pgsql-escape-identifier "pgsql" (identifier))
(declare-function pgsql-escape-literal "pgsql" (value))
(declare-function pgsql-exec "pgsql" (connection sql))
(declare-function pgsql-exec-params "pgsql" (connection sql typed-parameters))
(declare-function pgsql-exec-async "pgsql" (connection sql callback))
(declare-function pgsql-live-p "pgsql" (connection))
(declare-function pgsql-host "pgsql" (connection))
(declare-function pgsql-port "pgsql" (connection))
(declare-function pgsql-result-affected-rows "pgsql" (result))
(declare-function pgsql-result-columns "pgsql" (result))
(declare-function pgsql-result-rows "pgsql" (result))
(declare-function pgsql-set-connect-timeout "pgsql" (connection seconds))
(declare-function pgsql-set-read-timeout "pgsql" (connection seconds))
(declare-function pgsql-transaction-status "pgsql" (connection))
(declare-function pgsql-type-name "pgsql" (oid))
(declare-function pgsql-user "pgsql" (connection))
(declare-function pgsql-column-name "pgsql" (column))
(declare-function pgsql-column-type-oid "pgsql" (column))
(defvar pgsql-null)

(cl-defstruct (clutch-db-pg--connection
               (:constructor clutch-db-pg--make-connection)
               (:copier nil))
  "Clutch-owned state for one pgsql.el connection."
  client
  current-schema
  manual-commit)

(defun clutch-db-pg--ensure-client-api ()
  "Ensure pgsql.el is available."
  (unless (require 'pgsql nil t)
    (signal 'clutch-db-error
            (list "PostgreSQL backend requires pgsql.el. Install pgsql from MELPA or ensure a source checkout is on load-path."))))

(defun clutch-db-pg--apply-timeout-defaults (params)
  "Return PARAMS with PostgreSQL timeout defaults filled in."
  (clutch-db--apply-connect-defaults
   params
   `((:connect-timeout . ,clutch-connect-timeout-seconds)
     (:read-idle-timeout . ,clutch-read-idle-timeout-seconds)
     (:query-timeout . ,clutch-query-timeout-seconds))))

(defun clutch-db-pg--normalize-sslmode (sslmode)
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

(defun clutch-db-pg--normalize-connect-params (params)
  "Return PARAMS normalized for the PostgreSQL backend."
  (let* ((params (copy-sequence params))
         (tls-specified-p (plist-member params :tls))
         (tls (plist-get params :tls))
         (sslmode (clutch-db-pg--normalize-sslmode
                   (plist-get params :sslmode))))
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

;;;; OID → type-category mapping

(defconst clutch-db-pg--oid-bool 16)
(defconst clutch-db-pg--oid-bytea 17)
(defconst clutch-db-pg--oid-int8 20)
(defconst clutch-db-pg--oid-int2 21)
(defconst clutch-db-pg--oid-int4 23)
(defconst clutch-db-pg--oid-json 114)
(defconst clutch-db-pg--oid-float4 700)
(defconst clutch-db-pg--oid-float8 701)
(defconst clutch-db-pg--oid-date 1082)
(defconst clutch-db-pg--oid-time 1083)
(defconst clutch-db-pg--oid-timestamp 1114)
(defconst clutch-db-pg--oid-timestamptz 1184)
(defconst clutch-db-pg--oid-numeric 1700)
(defconst clutch-db-pg--oid-jsonb 3802)

(defconst clutch-db-pg--type-category-alist
  `((,clutch-db-pg--oid-int2 . numeric)
    (,clutch-db-pg--oid-int4 . numeric)
    (,clutch-db-pg--oid-int8 . numeric)
    (,clutch-db-pg--oid-float4 . numeric)
    (,clutch-db-pg--oid-float8 . numeric)
    (,clutch-db-pg--oid-numeric . numeric)
    (,clutch-db-pg--oid-bool . text)
    (,clutch-db-pg--oid-json . json)
    (,clutch-db-pg--oid-jsonb . json)
    (,clutch-db-pg--oid-bytea . blob)
    (,clutch-db-pg--oid-date . date)
    (,clutch-db-pg--oid-time . time)
    (,clutch-db-pg--oid-timestamp . datetime)
    (,clutch-db-pg--oid-timestamptz . datetime))
  "Alist mapping PostgreSQL OIDs to type-category symbols.")

(defun clutch-db-pg--type-category (oid)
  "Map a PostgreSQL type OID to a type-category symbol."
  (or (alist-get oid clutch-db-pg--type-category-alist)
      'text))

(cl-defgeneric clutch-db-pg--result-columns (conn pg-columns)
  "Return `clutch-db' column plists for PG-COLUMNS of a result on CONN.")

(cl-defmethod clutch-db-pg--result-columns ((_conn clutch-db-pg--connection)
                                           pg-columns)
  "Convert PG-COLUMNS of a result on a PostgreSQL connection."
  (mapcar (lambda (col)
            (let* ((name (pgsql-column-name col))
                   (type-oid (pgsql-column-type-oid col))
                   (type-name (pgsql-type-name type-oid))
                   (column (list :name name
                                 :type-category
                                 (clutch-db-pg--type-category type-oid))))
              (if type-name
                  (plist-put column :backend-type type-name)
                column)))
          pg-columns))

(defun clutch-db-pg--normalize-date-value (value)
  "Normalize PostgreSQL DATE VALUE to clutch's date plist representation."
  (if (string-match "\\`\\([0-9]+\\)-\\([0-9][0-9]\\)-\\([0-9][0-9]\\)\\'" value)
      (list :year (string-to-number (match-string 1 value))
            :month (string-to-number (match-string 2 value))
            :day (string-to-number (match-string 3 value)))
    value))

(defun clutch-db-pg--normalize-time-value (value)
  "Normalize PostgreSQL TIME VALUE to clutch's time plist representation."
  (let* ((negative (string-prefix-p "-" value))
         (rest (if negative (substring value 1) value))
         (rest (replace-regexp-in-string "[+-][0-9:]+\\'" "" rest))
         (dot-pos (string-search "." rest))
         (time-part (if dot-pos (substring rest 0 dot-pos) rest))
         (parts (split-string time-part ":")))
    (pcase parts
      (`(,hours ,minutes ,seconds)
       (list :hours (string-to-number hours)
             :minutes (string-to-number minutes)
             :seconds (string-to-number seconds)
             :negative negative))
      (_ value))))

(defun clutch-db-pg--normalize-datetime-value (value)
  "Normalize PostgreSQL DATETIME VALUE to clutch's datetime plist representation."
  (cond
   ((and (stringp value)
         (string-match
          "\\`\\([0-9]+\\)-\\([0-9][0-9]\\)-\\([0-9][0-9]\\)[ T]\\([0-9][0-9]\\):\\([0-9][0-9]\\):\\([.0-9]+\\)\\'"
          value))
    (list :year (string-to-number (match-string 1 value))
          :month (string-to-number (match-string 2 value))
          :day (string-to-number (match-string 3 value))
          :hours (string-to-number (match-string 4 value))
          :minutes (string-to-number (match-string 5 value))
          :seconds (string-to-number (match-string 6 value))))
   ((stringp value) value)
   (t
    (pcase-let ((`(,seconds ,minutes ,hours ,day ,month ,year . ,_)
                  (decode-time value)))
      (list :year year
            :month month
            :day day
            :hours hours
            :minutes minutes
            :seconds seconds)))))

(defun clutch-db-pg--normalize-value (value col-def)
  "Normalize PG VALUE according to COL-DEF's clutch type category."
  (let ((backend-type (plist-get col-def :backend-type)))
    (cond
     ((eq value pgsql-null) nil)
     ((equal backend-type "bool") (if (null value) :false value))
     ((and (clutch-db-pg--array-type-name-p backend-type)
           (vectorp value))
      (let ((boolean-p (member backend-type '("_bool" "bool[]")))
            (timestamptz-p (member backend-type '("_timestamptz" "timestamptz[]"))))
        (cl-labels
            ((normalize-element
              (element)
              (cond
               ((eq element pgsql-null) nil)
               ((and boolean-p (null element)) :false)
               ((vectorp element)
                (vconcat (mapcar #'normalize-element element)))
               ;; pgsql.el decodes timestamptz as a Lisp time value; show the
               ;; text a timestamptz cell shows.
               ((and timestamptz-p (not (stringp element)))
                (clutch-db-format-temporal
                 (clutch-db-pg--normalize-datetime-value element)))
               (t element))))
          (vconcat (mapcar #'normalize-element value)))))
     ((eq (plist-get col-def :type-category) 'date)
      (clutch-db-pg--normalize-date-value value))
     ((eq (plist-get col-def :type-category) 'time)
      (clutch-db-pg--normalize-time-value value))
     ((eq (plist-get col-def :type-category) 'datetime)
      (clutch-db-pg--normalize-datetime-value value))
     (t value))))

(defun clutch-db-pg--normalize-row (row columns)
  "Normalize PG ROW using clutch column metadata COLUMNS."
  (cl-mapcar #'clutch-db-pg--normalize-value row columns))

(defun clutch-db-pg--wrap-result (conn pg-result)
  "Convert PG-RESULT from CONN to a `clutch-db-result'."
  (let* ((raw-cols (pgsql-result-columns pg-result))
         (cols (when raw-cols (clutch-db-pg--result-columns conn raw-cols)))
         (rows (if cols
                   (mapcar (lambda (row)
                             (clutch-db-pg--normalize-row row cols))
                           (pgsql-result-rows pg-result))
                 (pgsql-result-rows pg-result))))
    (make-clutch-db-result
     :connection conn
     :columns cols
     :rows rows
     :affected-rows (pgsql-result-affected-rows pg-result)
     :last-insert-id nil
     :warnings nil)))

(defun clutch-db-pg--exec (conn sql)
  "Execute SQL through the pgsql.el client owned by CONN."
  (pgsql-exec (clutch-db-pg--connection-client conn) sql))

(defun clutch-db-pg--cache-current-schema (conn schema)
  "Cache SCHEMA as the current schema for CONN."
  (setf (clutch-db-pg--connection-current-schema conn) schema)
  schema)

(defun clutch-db-pg--set-search-path (conn schema)
  "Set CONN search_path to SCHEMA and update the local cache."
  (let ((schema (string-trim schema)))
    (clutch-db-pg--exec conn
             (format "SET search_path TO %s"
                     (pgsql-escape-identifier schema)))
    (clutch-db-pg--cache-current-schema conn schema)))

(defun clutch-db-pg--tx-open-p (conn)
  "Return non-nil when CONN has an open foreground transaction."
  (memq (pgsql-transaction-status
         (clutch-db-pg--connection-client conn))
        '(in-transaction failed-transaction)))

(defun clutch-db-pg--tx-failed-p (conn)
  "Return non-nil when CONN's foreground transaction is failed/aborted."
  (eq (pgsql-transaction-status
       (clutch-db-pg--connection-client conn))
      'failed-transaction))

(defun clutch-db-pg--transaction-control-query-p (sql)
  "Return non-nil when SQL is explicit PostgreSQL transaction control."
  (let ((case-fold-search t)
        (trimmed (clutch-db-sql-strip-leading-comments sql)))
    (string-match-p
     "\\`\\s-*\\(?:BEGIN\\|START\\s-+TRANSACTION\\|COMMIT\\|END\\|ABORT\\|ROLLBACK\\|SAVEPOINT\\|RELEASE\\)\\b"
     trimmed)))

(defun clutch-db-pg--ensure-foreground-transaction (conn sql)
  "Lazily open a foreground transaction on CONN before running SQL."
  (when (and (clutch-db-pg--connection-manual-commit conn)
             (not (clutch-db-pg--transaction-control-query-p sql))
             (not (clutch-db-pg--tx-open-p conn)))
    (clutch-db-pg--exec conn "BEGIN")))

(defun clutch-db-pg--run-query-with-transaction-state (conn sql thunk)
  "Run THUNK for SQL on CONN after applying lazy-BEGIN semantics."
  (condition-case err
      (progn
        (clutch-db-pg--ensure-foreground-transaction conn sql)
        (funcall thunk))
    (pgsql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

;;;; Connect function

(defun clutch-db-pg-connect (params &optional make-connection)
  "Connect to PostgreSQL using PARAMS plist.
PARAMS keys: :host, :port, :user, :password, :database, :tls,
:sslmode, :schema, :connect-timeout, :read-idle-timeout, :query-timeout.
`:tls' is a convenience shortcut; `:sslmode' is the canonical PostgreSQL name.
MAKE-CONNECTION, when non-nil, builds the connection from its :client, for a
backend such as XTDB that speaks the PostgreSQL protocol."
  (clutch-db-pg--ensure-client-api)
  (setq params (clutch-db-pg--apply-timeout-defaults
                (clutch-db-pg--normalize-connect-params
                 (clutch-db--reject-removed-connect-params params))))
  (let ((schema (plist-get params :schema))
        (sslmode (plist-get params :sslmode))
        (connect-timeout (plist-get params :connect-timeout))
        (read-idle-timeout (plist-get params :read-idle-timeout))
        (query-timeout (plist-get params :query-timeout))
        client conn)
    (condition-case err
        (progn
          (setq client
                (pgsql-connect
                 :database (plist-get params :database)
                 :user (plist-get params :user)
                 :password (plist-get params :password)
                 :host (or (plist-get params :host) "localhost")
                 :port (or (plist-get params :port) 5432)
                 :sslmode (or sslmode 'disable)
                 :connect-timeout connect-timeout
                 :read-timeout read-idle-timeout
                 :application-name "clutch")
                conn (funcall (or make-connection
                                  #'clutch-db-pg--make-connection)
                              :client client))
          (when query-timeout
            (clutch-db-pg--exec
             conn (format "SET statement_timeout = %d" (* query-timeout 1000))))
          (when schema
            (clutch-db-pg--set-search-path conn schema))
          conn)
      (pgsql-error
       (when client
         (pgsql-disconnect client))
       (signal 'clutch-db-error
               (list (error-message-string err)))))))

(defun clutch-db-pg--rewrite-param-sql (sql &optional param-count)
  "Return SQL with `?' placeholders rewritten to PostgreSQL `$N' form.
`clutch-db-sql-map-placeholders' decides which question marks are
placeholders, so jsonb's `?|' / `?&' operators pass through and `??'
writes the bare `?' operator.  When PARAM-COUNT is non-nil, signal
`clutch-db-error' if the placeholder count differs from it, since a bare
`?' meant as an operator would otherwise bind parameters to the wrong
positions."
  (pcase-let ((`(,rewritten . ,count)
               (clutch-db-sql-map-placeholders
                sql (lambda (index) (format "$%d" (1+ index)))
                (clutch-db-sql-dialect 'postgres))))
    (when (and param-count (/= count param-count))
      (signal 'clutch-db-error
              (list (format
                     "SQL has %d `?' placeholders but %d parameters; write `??' for the jsonb operator"
                     count param-count))))
    rewritten))

(defun clutch-db-pg--array-type-name-p (type)
  "Return non-nil when PostgreSQL TYPE names an array type."
  (and (stringp type)
       (or (string-prefix-p "_" type)
           (string-suffix-p "[]" type))))

(defun clutch-db-pg--parse-json-array-param (value type)
  "Parse JSON array VALUE for PostgreSQL array TYPE."
  (condition-case err
      (let ((parsed (json-parse-string value
                                       :array-type 'array
                                       :object-type 'hash-table
                                       :null-object pgsql-null
                                       :false-object :false)))
        (unless (vectorp parsed)
          (user-error "PostgreSQL array value for %s must be a JSON array"
                      type))
        parsed)
    (error
     (user-error "PostgreSQL array value for %s must be a JSON array or curly-brace array literal: %s"
                 type
                 (error-message-string err)))))

(defun clutch-db-pg--prepare-array-value (value)
  "Translate Clutch array VALUE to pgsql.el's public value model."
  (cond
   ((eq value pgsql-null) value)
   ((null value) pgsql-null)
   ((eq value :false) nil)
   ((clutch-db-format-temporal value)
    (clutch-db-format-temporal value))
   ((vectorp value)
    (vconcat (mapcar #'clutch-db-pg--prepare-array-value value)))
   ((and (listp value) (not (keywordp (car value))))
    (mapcar #'clutch-db-pg--prepare-array-value value))
   (t value)))

(defun clutch-db-pg--array-string-value (trimmed type)
  "Return TRIMMED PostgreSQL array text as a literal string or prepared value.
A curly-brace literal or a dimension-slice literal (e.g. \"[1:2]={1,2}\") of
TYPE is returned unchanged as text.  A JSON array is parsed and returned as a
value prepared for pgsql.el.  Signal `user-error' when TRIMMED is neither."
  (cond
   ((string-prefix-p "{" trimmed) trimmed)
   ((string-match-p "\\`\\(?:\\[[+-]?[0-9]+:[+-]?[0-9]+\\]\\)+=" trimmed)
    trimmed)
   ((string-prefix-p "[" trimmed)
    (clutch-db-pg--prepare-array-value
     (clutch-db-pg--parse-json-array-param trimmed type)))
   (t
    (user-error "PostgreSQL array value for %s must be a JSON array or curly-brace array literal"
                type))))

(defun clutch-db-pg--array-literal-string (value type)
  "Return PostgreSQL array literal text for VALUE of PostgreSQL TYPE."
  (cond
   ((stringp value)
    (let ((result (clutch-db-pg--array-string-value (string-trim value) type)))
      (if (stringp result) result (pgsql-array-literal result type))))
   ((or (vectorp value) (listp value))
    (pgsql-array-literal (clutch-db-pg--prepare-array-value value) type))
   (t
    (user-error "PostgreSQL array value for %s must be a sequence, JSON array, or curly-brace array literal"
                type))))

(defconst clutch-db-pg--datetime-without-offset-regexp
  (concat "\\`[0-9]\\{4\\}-[0-9][0-9]-[0-9][0-9][ T]"
          "[0-9][0-9]:[0-9][0-9]\\(?::[0-9][0-9]\\(?:\\.[0-9]+\\)?\\)?\\'")
  "Regexp matching a date and a time with no UTC offset.")

(defun clutch-db-pg--timestamptz-text (value)
  "Return timestamptz VALUE as text with its UTC offset, or nil.
VALUE is a temporal plist from a result or text typed for one.  Clutch
shows a timestamptz in Emacs's local time without an offset, and
PostgreSQL reads text without one in the session time zone, which may
differ, so such text gains the offset of Emacs's time zone at that time."
  (when-let* ((text (or (clutch-db-format-temporal value)
                        (and (stringp value) value))))
    (if (string-match-p clutch-db-pg--datetime-without-offset-regexp text)
        (concat text
                (format-time-string "%:z" (encode-time (parse-time-string text))))
      text)))

(defun clutch-db-pg--typed-argument (param)
  "Return PARAM as one pgsql.el typed argument."
  (let* ((value (clutch-db-param-value param))
         (type (clutch-db-param-type param))
         (array-type-p (clutch-db-pg--array-type-name-p type))
         (temporal (and (not array-type-p)
                        (clutch-db-format-temporal value))))
    (cons (cond
           ((null value) pgsql-null)
           ((eq value :false) nil)
           ((equal type "timestamptz")
            (or (clutch-db-pg--timestamptz-text value) value))
           (temporal temporal)
           (array-type-p
            (if (stringp value)
                (clutch-db-pg--array-string-value (string-trim value) type)
              (clutch-db-pg--prepare-array-value value)))
           (t value))
          type)))

(defun clutch-db-pg--typed-arguments (params)
  "Return PARAMS as pgsql.el typed arguments."
  (mapcar #'clutch-db-pg--typed-argument params))

(defun clutch-db-pg--metadata-rows (result)
  "Return metadata rows from RESULT with SQL NULL sentinels normalized."
  (mapcar (lambda (row)
            (mapcar (lambda (value)
                      (unless (eq value pgsql-null)
                        value))
                    row))
          (pgsql-result-rows result)))

(defun clutch-db-pg--format-column-ddl (col)
  "Format a single column COL row as a DDL line."
  (pcase-let ((`(,name ,dtype ,max-len ,default-val ,nullable) col))
    (let* ((type-str (if max-len (format "%s(%s)" dtype max-len) dtype))
           (parts (append (list (pgsql-escape-identifier name) type-str)
                          (when default-val
                            (list (format "DEFAULT %s" default-val)))
                          (when (equal nullable "NO")
                            '("NOT NULL")))))
      (format "    %s" (mapconcat #'identity parts " ")))))

(defun clutch-db-pg--unique-not-null-identities (conn table &optional schema)
  "Return unique-not-null row identity candidates for TABLE in SCHEMA on CONN."
  (clutch-db--translate-library-error pgsql-error
    (let* ((sql (format "SELECT idx.relname,
       string_agg(a.attname, E'\\x1f' ORDER BY keys.ord) AS columns
FROM pg_index i
JOIN pg_class idx ON idx.oid = i.indexrelid
JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS keys(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = keys.attnum
WHERE i.indrelid = %s
  AND i.indisunique
  AND NOT i.indisprimary
  AND i.indpred IS NULL
  AND i.indexprs IS NULL
GROUP BY idx.relname
HAVING bool_and(a.attnotnull)
ORDER BY idx.relname"
                        (clutch-db-pg--relation table schema)))
           (result (clutch-db-pg--exec conn sql)))
      (mapcar (lambda (row)
                (pcase-let ((`(,name ,columns) row))
                  (list :kind 'unique-key
                        :name name
                        :columns (split-string columns "\x1f" t))))
              (clutch-db-pg--metadata-rows result)))))

(defun clutch-db-pg--ctid-identity (conn table &optional schema)
  "Return a CTID row locator candidate for TABLE in SCHEMA on CONN, or nil."
  (clutch-db--translate-library-error pgsql-error
    (let* ((sql (format "SELECT c.relkind::text
FROM pg_class c
WHERE c.oid = %s"
                        (clutch-db-pg--relation table schema)))
           (result (clutch-db-pg--exec conn sql))
           (relkind (car (car (clutch-db-pg--metadata-rows result)))))
      (when (equal relkind "r")
        (list :kind 'row-locator
              :name "ctid"
              :select-expressions '("ctid::text")
              :where-sql "ctid = ?::tid")))))

(defun clutch-db-pg--format-type (data-type max-len num-prec num-scale)
  "Build a concise type string for DATA-TYPE.
MAX-LEN, NUM-PREC, and NUM-SCALE refine the rendered PostgreSQL
information_schema type."
  (cond
   ((member data-type '("character varying" "varchar"))
    (if max-len (format "varchar(%s)" max-len) "varchar"))
   ((member data-type '("character" "char"))
    (if max-len (format "char(%s)" max-len) "char"))
   ((string= data-type "numeric")
    (cond ((and num-prec num-scale) (format "numeric(%s,%s)" num-prec num-scale))
          (num-prec                 (format "numeric(%s)" num-prec))
          (t                        "numeric")))
   (t data-type)))

(defun clutch-db-pg--column-details-row (row pk-cols fks)
  "Convert a column-details ROW to a clutch-db column plist.
ROW's ninth value is \"YES\" for an identity or generated column.
PK-COLS is a list of primary key column names.
FKS is an alist of (column-name . fk-plist)."
  (pcase-let ((`(,name ,dtype ,backend-type ,nullable-str ,max-len
                 ,num-prec ,num-scale ,default-val ,generated-str ,comment) row))
    (let* ((type     (clutch-db-pg--format-type dtype max-len num-prec num-scale))
           (nullable (equal nullable-str "YES"))
           (pk-p     (member name pk-cols))
           (fk       (cdr (assoc name fks)))
           (generated (or (equal generated-str "YES")
                          (and (stringp default-val)
                               (string-match-p "\\`nextval(" default-val)))))
      (let ((detail (list :name name :type type :nullable nullable
                          :primary-key (and pk-p t)
                          :foreign-key fk
                          :default (and default-val (not generated) default-val)
                          :generated (and generated t)
                          :comment (and (stringp comment)
                                        (not (string-empty-p comment))
                                        comment))))
        (if (and (stringp backend-type)
                 (not (string-empty-p backend-type)))
            (plist-put detail :backend-type backend-type)
          detail)))))

;;;; Lifecycle methods

(cl-defmethod clutch-db-disconnect ((conn clutch-db-pg--connection))
  "Disconnect PostgreSQL CONN."
  (setf (clutch-db-pg--connection-manual-commit conn) nil)
  (pgsql-disconnect (clutch-db-pg--connection-client conn)))

(cl-defmethod clutch-db-live-p ((conn clutch-db-pg--connection))
  "Return non-nil if PostgreSQL CONN is live."
  (pgsql-live-p (clutch-db-pg--connection-client conn)))

(cl-defmethod clutch-db-backend-key ((_conn clutch-db-pg--connection))
  "Return the registered backend key for PostgreSQL connections."
  'pg)

(cl-defmethod clutch-db-eager-schema-refresh-p
    ((_conn clutch-db-pg--connection))
  "PostgreSQL schema refresh should not block connect."
  nil)

(cl-defmethod clutch-db-completion-deferred-columns-p
    ((_conn clutch-db-pg--connection))
  "PostgreSQL completion should hydrate uncached columns when idle."
  t)

;;;; Transaction methods

(cl-defmethod clutch-db-manual-commit-p ((conn clutch-db-pg--connection))
  "Return non-nil when PostgreSQL CONN is in manual-commit mode."
  (clutch-db-pg--connection-manual-commit conn))

(cl-defmethod clutch-db-manual-commit-supported-p
    ((_conn clutch-db-pg--connection))
  "Return non-nil because PostgreSQL supports Clutch-managed manual commit."
  t)

(cl-defmethod clutch-db-commit ((conn clutch-db-pg--connection))
  "Finish the current foreground transaction on PostgreSQL CONN.
Return `rolled-back' after rolling back an already failed transaction."
  (clutch-db--translate-library-error pgsql-error
    (when (clutch-db-pg--tx-open-p conn)
      (if (clutch-db-pg--tx-failed-p conn)
          (progn
            (clutch-db-pg--exec conn "ROLLBACK")
            'rolled-back)
        (clutch-db-pg--exec conn "COMMIT")))))

(cl-defmethod clutch-db-rollback ((conn clutch-db-pg--connection))
  "Roll back the current foreground transaction on PostgreSQL CONN."
  (clutch-db--translate-library-error pgsql-error
    (when (clutch-db-pg--tx-open-p conn)
      (clutch-db-pg--exec conn "ROLLBACK"))))

(cl-defmethod clutch-db-set-auto-commit
    ((conn clutch-db-pg--connection) auto-commit)
  "Set foreground autocommit mode on PostgreSQL CONN.
AUTO-COMMIT non-nil enables autocommit; nil enables clutch-managed
manual-commit mode via lazy BEGIN."
  (clutch-db--translate-library-error pgsql-error
    (if auto-commit
        (progn
          (when (clutch-db-pg--tx-open-p conn)
            (clutch-db-pg--exec conn (if (clutch-db-pg--tx-failed-p conn)
                              "ROLLBACK"
                            "COMMIT")))
          (setf (clutch-db-pg--connection-manual-commit conn) nil))
      (setf (clutch-db-pg--connection-manual-commit conn) t))))

(cl-defmethod clutch-db-call-with-atomic-batch
    ((conn clutch-db-pg--connection) function)
  "Call FUNCTION atomically in CONN's selected PostgreSQL transaction mode."
  (if (clutch-db-manual-commit-p conn)
      (clutch-db--call-with-sql-savepoint
       conn function
       (lambda ()
         (unless (clutch-db-pg--tx-open-p conn)
           (clutch-db-query conn "BEGIN"))))
    (when (clutch-db-pg--tx-open-p conn)
      (user-error
       "Session already has an explicit transaction; switch to Manual mode or finish it before submitting"))
    (clutch-db--call-with-transaction-boundary
     (lambda () (clutch-db-query conn "BEGIN"))
     function
     (lambda () (clutch-db-commit conn))
     (lambda () (clutch-db-rollback conn)))))

(cl-defmethod clutch-db-schema-transaction-effect
    ((_conn clutch-db-pg--connection) _sql)
  "Return `dirty' because PostgreSQL DDL participates in transactions."
  'dirty)

;;;; Query methods

(cl-defmethod clutch-db-query ((conn clutch-db-pg--connection) sql)
  "Execute SQL on PostgreSQL CONN, returning a `clutch-db-result'."
  (clutch-db-pg--run-query-with-transaction-state
   conn sql
   (lambda ()
     (clutch-db-pg--wrap-result conn (clutch-db-pg--exec conn sql)))))

(cl-defmethod clutch-db-query-async
    ((conn clutch-db-pg--connection) sql callback)
  "Start SQL on PostgreSQL CONN and pass the outcome to CALLBACK.
Decline when the installed pgsql.el cannot execute asynchronously."
  (when (fboundp 'pgsql-exec-async)
    (clutch-db-pg--run-query-with-transaction-state
     conn sql
     (lambda ()
       (pgsql-exec-async
        (clutch-db-pg--connection-client conn) sql
        (lambda (result error)
          (funcall callback
                   (and result (clutch-db-pg--wrap-result conn result))
                   (and error
                        (list 'clutch-db-error
                              (error-message-string error))))))))
    t))

(cl-defmethod clutch-db-execute-params
    ((conn clutch-db-pg--connection) sql params)
  "Execute parameterized SQL on PostgreSQL CONN with PARAMS."
  (clutch-db-pg--run-query-with-transaction-state
   conn sql
   (lambda ()
     (let* ((pg-sql (clutch-db-pg--rewrite-param-sql
                     sql (length params)))
            (typed-arguments (clutch-db-pg--typed-arguments params))
            (result (pgsql-exec-params
                     (clutch-db-pg--connection-client conn)
                     pg-sql typed-arguments)))
       (clutch-db-pg--wrap-result conn result)))))

(cl-defmethod clutch-db-interrupt-query ((conn clutch-db-pg--connection))
  "Interrupt the current PostgreSQL query on CONN without dropping the session."
  (let ((client (clutch-db-pg--connection-client conn)))
    (if (and (pgsql-live-p client)
             (not (pgsql-busy-p client)))
        t
      (condition-case nil
          (progn
            (pgsql-cancel client)
            t)
        (pgsql-error nil)))))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier
    ((_conn clutch-db-pg--connection) name)
  "Escape NAME as a PostgreSQL identifier (double-quoted)."
  (pgsql-escape-identifier name))

(cl-defmethod clutch-db-escape-literal
    ((_conn clutch-db-pg--connection) value)
  "Escape VALUE as a PostgreSQL string literal."
  (pgsql-escape-literal value))

(defun clutch-db-pg--fold-identifier (raw)
  "Return identifier RAW in its PostgreSQL catalog form.
A quoted identifier keeps its case, and an unquoted one folds to lower case."
  (if (string-prefix-p "\"" raw)
      (clutch-db-sql--unquote-identifier raw)
    (downcase raw)))

(cl-defmethod clutch-db--source-table-name ((_conn clutch-db-pg--connection) token)
  "Return the name of the table that SQL table TOKEN names."
  (clutch-db-pg--fold-identifier (clutch-db-sql-table-qualifier token)))

(cl-defmethod clutch-db--source-table-schema ((_conn clutch-db-pg--connection) token)
  "Return the schema that SQL table TOKEN names, or nil."
  (let ((parts (clutch-db-sql--table-raw-parts token)))
    (when (cdr parts)
      (clutch-db-pg--fold-identifier (car (last parts 2))))))

(defun clutch-db-pg--relation (table schema)
  "Return SQL that resolves TABLE in SCHEMA to its oid.
TABLE and SCHEMA are names as PostgreSQL stores them.  Without SCHEMA, the
search path resolves TABLE, as it did for the query."
  (format "%s::regclass"
          (pgsql-escape-literal
           (if schema
               (concat (pgsql-escape-identifier schema) "."
                       (pgsql-escape-identifier table))
             (pgsql-escape-identifier table)))))

(cl-defmethod clutch-db-value-to-typed-literal
    ((conn clutch-db-pg--connection) value type fallback-format-fn)
  "Render VALUE as a PostgreSQL literal for CONN using TYPE metadata."
  (cond
   ((and (eq value :false)
         (stringp type)
         (member (downcase type) '("bool" "boolean")))
    "false")
   ((and (not (null value))
         (clutch-db-pg--array-type-name-p type))
    (clutch-db-escape-literal
     conn
     (clutch-db-pg--array-literal-string value type)))
   ((equal type "timestamptz")
    (if-let* ((text (clutch-db-pg--timestamptz-text value)))
        (clutch-db-escape-literal conn text)
      (clutch-db--basic-value-to-literal conn value fallback-format-fn)))
   (t
    (clutch-db--basic-value-to-literal conn value fallback-format-fn))))

;;;; Schema methods

(cl-defmethod clutch-db-list-schemas ((conn clutch-db-pg--connection))
  "Return visible schema names for PostgreSQL CONN."
  (clutch-db--translate-library-error pgsql-error
    (let ((result (clutch-db-pg--exec
                   conn
                   "SELECT schema_name FROM information_schema.schemata \
WHERE schema_name <> 'information_schema' \
  AND schema_name NOT LIKE 'pg\\_%' ESCAPE '\\' \
ORDER BY schema_name")))
      (mapcar #'car (clutch-db-pg--metadata-rows result)))))

(cl-defmethod clutch-db-current-schema ((conn clutch-db-pg--connection))
  "Return the current effective schema for PostgreSQL CONN."
  (or (clutch-db-pg--connection-current-schema conn)
      (clutch-db--translate-library-error pgsql-error
        (let* ((result (clutch-db-pg--exec conn "SELECT current_schema()"))
               (schema (caar (clutch-db-pg--metadata-rows result))))
          (when schema
            (clutch-db-pg--cache-current-schema conn schema))))))

(cl-defmethod clutch-db-set-current-schema ((conn clutch-db-pg--connection) schema)
  "Switch PostgreSQL CONN to SCHEMA via search_path."
  (clutch-db--translate-library-error pgsql-error
    (clutch-db-pg--set-search-path conn schema)))

(clutch-db--define-idle-metadata-methods clutch-db-pg--connection "PostgreSQL")

(cl-defmethod clutch-db-list-tables ((conn clutch-db-pg--connection))
  "Return table names for the current PostgreSQL database on CONN."
  (clutch-db--translate-library-error pgsql-error
    (let ((result (clutch-db-pg--exec
                   conn
                   "SELECT tablename FROM pg_tables \
WHERE schemaname = current_schema() \
ORDER BY tablename")))
      (mapcar #'car (clutch-db-pg--metadata-rows result)))))

(cl-defmethod clutch-db-list-table-entries ((conn clutch-db-pg--connection))
  "Return table/view entry plists for the current PostgreSQL schema on CONN."
  (clutch-db--translate-library-error pgsql-error
    (let* ((schema (clutch-db-current-schema conn))
           (result (clutch-db-pg--exec
                    conn
                    "SELECT objects.name, objects.type, obj_description(c.oid, 'pg_class')
FROM (
  SELECT tablename AS name, 'TABLE' AS type
  FROM pg_tables
  WHERE schemaname = current_schema()
  UNION ALL
  SELECT viewname AS name, 'VIEW' AS type
  FROM pg_views
  WHERE schemaname = current_schema()
) objects
JOIN pg_class c ON c.relname = objects.name
JOIN pg_namespace n ON n.oid = c.relnamespace
  AND n.nspname = current_schema()
ORDER BY objects.name")))
      (mapcar
       (lambda (row)
         (pcase-let ((`(,name ,type ,comment) row))
           (list :name name
                 :type type
                 :schema schema
                 :source-schema schema
                 :comment (and (not (string-empty-p (or comment "")))
                               comment))))
       (clutch-db-pg--metadata-rows result)))))

(cl-defmethod clutch-db-list-columns ((conn clutch-db-pg--connection) table)
  "Return column names for TABLE on PostgreSQL CONN."
  (clutch-db--translate-library-error pgsql-error
    (let ((result (clutch-db-pg--exec
                   conn
                   (format "SELECT column_name FROM information_schema.columns \
WHERE table_name = %s AND table_schema = current_schema() \
ORDER BY ordinal_position"
                           (pgsql-escape-literal table)))))
      (mapcar #'car (clutch-db-pg--metadata-rows result)))))

(cl-defmethod clutch-db-list-objects ((conn clutch-db-pg--connection) category)
  "Return object entry plists for CATEGORY on PostgreSQL CONN."
  (clutch-db--translate-library-error pgsql-error
    (let ((schema (clutch-db-current-schema conn)))
      (pcase category
        ('indexes
         (let ((result
                (clutch-db-pg--exec
                 conn
                 "SELECT i.indexname, i.tablename, ix.indisunique
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
JOIN pg_index ix ON ix.indexrelid = c.oid
WHERE i.schemaname = current_schema()
ORDER BY i.tablename, i.indexname")))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,table-name ,unique) row))
                (list :name name :type "INDEX" :schema schema
                      :source-schema schema
                      :target-table table-name :unique unique)))
            (clutch-db-pg--metadata-rows result))))
        ('sequences
         (let ((result
                (clutch-db-pg--exec
                 conn
                 "SELECT sequencename, min_value, max_value, increment_by, last_value
FROM pg_sequences
WHERE schemaname = current_schema()
ORDER BY sequencename")))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,min ,max ,increment ,last) row))
                (list :name name :type "SEQUENCE" :schema schema
                      :source-schema schema
                      :min min :max max :increment increment :last last)))
            (clutch-db-pg--metadata-rows result))))
        ((or 'procedures 'functions)
         (let* ((routine-type (if (eq category 'procedures)
                                  "PROCEDURE"
                                "FUNCTION"))
                (prokind (if (eq category 'procedures) "p" "f"))
                (result
                 (clutch-db-pg--exec
                  conn
                  (format "SELECT p.proname, p.oid
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = current_schema()
  AND p.prokind = %s
ORDER BY p.proname"
                          (pgsql-escape-literal prokind)))))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,oid) row))
                (list :name name :type routine-type :schema schema
                      :source-schema schema
                      :identity (format "OID:%s" oid))))
            (clutch-db-pg--metadata-rows result))))
        ('triggers
         (let ((result
                (clutch-db-pg--exec
                 conn
                 "SELECT t.trigger_name, t.event_object_table, t.event_manipulation,
        t.action_timing, pg_t.oid
FROM information_schema.triggers t
JOIN pg_class c ON c.relname = t.event_object_table
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_trigger pg_t ON pg_t.tgrelid = c.oid
                    AND pg_t.tgname = t.trigger_name
WHERE t.trigger_schema = current_schema()
  AND NOT pg_t.tgisinternal
ORDER BY t.event_object_table, t.trigger_name")))
           (let ((rows (clutch-db-pg--metadata-rows result))
                 grouped)
             (dolist (row rows (nreverse grouped))
               (pcase-let ((`(,name ,table-name ,event ,timing ,oid) row))
                 (if-let* ((existing (cl-find-if
                                      (lambda (entry)
                                        (and (string= (plist-get entry :name) name)
                                             (string= (plist-get entry :target-table) table-name)))
                                      grouped)))
                     (unless (string-match-p (regexp-quote event)
                                             (or (plist-get existing :event) ""))
                       (setf (plist-get existing :event)
                             (concat (plist-get existing :event) " OR " event)))
                   (push (list :name name :type "TRIGGER" :schema schema
                               :source-schema schema
                               :target-table table-name :event event :timing timing
                               :status "ENABLED"
                               :identity (format "OID:%s" oid))
                         grouped)))))))
        (_ nil)))))

(cl-defmethod clutch-db-object-details ((conn clutch-db-pg--connection) entry)
  "Return detail plists for PostgreSQL object ENTRY on CONN."
  (clutch-db--translate-library-error pgsql-error
    (pcase (upcase (or (plist-get entry :type) ""))
        ("INDEX"
         (let* ((result
                 (clutch-db-pg--exec
                  conn
                  (format "SELECT a.attname, k.ordinality,
       CASE WHEN ((pi.indoption::int2[])[k.ordinality] & 1) = 1
            THEN 'DESC' ELSE 'ASC' END AS descend
FROM pg_class idx
JOIN pg_namespace n ON n.oid = idx.relnamespace
JOIN pg_index pi ON pi.indexrelid = idx.oid
JOIN LATERAL unnest(pi.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
JOIN pg_attribute a ON a.attrelid = pi.indrelid AND a.attnum = k.attnum
WHERE idx.relkind = 'i'
  AND idx.relname = %s
  AND n.nspname = current_schema()
ORDER BY k.ordinality"
                          (pgsql-escape-literal (plist-get entry :name))))))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,position ,descend) row))
                (list :name name :position position :descend descend)))
            (clutch-db-pg--metadata-rows result))))
        ((or "PROCEDURE" "FUNCTION")
         (let* ((oid (substring (plist-get entry :identity) 4))
                (sql (concat
                      "SELECT name, type, mode, position FROM ("
                      (if (string= (upcase (plist-get entry :type)) "FUNCTION")
                          (format "SELECT NULL::text AS name,
       pg_catalog.format_type(p.prorettype, NULL) AS type,
       'RETURN' AS mode, 0 AS position
FROM pg_proc p
WHERE p.oid = %s
UNION ALL " oid)
                        "")
                      (format "SELECT (p.proargnames::text[])[s.n] AS name,
       pg_catalog.format_type(COALESCE((p.proallargtypes)[s.n],
                                       (p.proargtypes::oid[])[s.n]), NULL) AS type,
       CASE COALESCE((p.proargmodes::text[])[s.n], 'i')
         WHEN 'i' THEN 'IN'
         WHEN 'o' THEN 'OUT'
         WHEN 'b' THEN 'INOUT'
         WHEN 'v' THEN 'VARIADIC'
         WHEN 't' THEN 'TABLE'
         ELSE 'IN'
       END AS mode,
       s.n AS position
FROM pg_proc p
JOIN LATERAL generate_subscripts(COALESCE(p.proallargtypes,
                                          p.proargtypes::oid[]), 1) AS s(n) ON true
WHERE p.oid = %s) args
ORDER BY position" oid)))
                (result (clutch-db-pg--exec conn sql)))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,type ,mode ,position) row))
                (list :name name :type type :mode mode :position position)))
            (clutch-db-pg--metadata-rows result))))
      (_ nil))))

(cl-defmethod clutch-db-object-definition ((conn clutch-db-pg--connection) entry)
  "Return definition or source text for PostgreSQL object ENTRY on CONN."
  (clutch-db--translate-library-error pgsql-error
    (let ((type (upcase (or (plist-get entry :type) "")))
          (name (plist-get entry :name)))
      (pcase type
        ("TABLE"
         (let* ((cols-result
                 (clutch-db-pg--exec
                  conn
                  (format "SELECT column_name, data_type, \
character_maximum_length, column_default, is_nullable \
FROM information_schema.columns \
WHERE table_name = %s AND table_schema = current_schema() \
ORDER BY ordinal_position"
                          (pgsql-escape-literal name))))
                (lines (mapcar #'clutch-db-pg--format-column-ddl
                               (clutch-db-pg--metadata-rows cols-result))))
           (format "CREATE TABLE %s (\n%s\n);"
                   (pgsql-escape-identifier name)
                   (mapconcat #'identity lines ",\n"))))
        ((or "PROCEDURE" "FUNCTION")
         (let ((oid (substring (plist-get entry :identity) 4)))
           (caar (clutch-db-pg--metadata-rows
                  (clutch-db-pg--exec
                   conn (format "SELECT pg_get_functiondef(%s::oid)" oid))))))
        ("TRIGGER"
         (let ((oid (substring (plist-get entry :identity) 4)))
           (caar (clutch-db-pg--metadata-rows
                  (clutch-db-pg--exec
                   conn
                   (format "SELECT pg_get_triggerdef(%s::oid, true)" oid))))))
        ("INDEX"
         (caar (clutch-db-pg--metadata-rows
                (clutch-db-pg--exec
                 conn
                 (format "SELECT pg_get_indexdef(idx.oid)
FROM pg_class idx
JOIN pg_namespace n ON n.oid = idx.relnamespace
WHERE idx.relkind = 'i'
  AND idx.relname = %s
  AND n.nspname = current_schema()"
                         (pgsql-escape-literal name))))))
        ("VIEW"
         (caar (clutch-db-pg--metadata-rows
                (clutch-db-pg--exec
                 conn
                 (format "SELECT 'CREATE OR REPLACE VIEW ' || quote_ident(viewname) || E' AS\n' ||
       pg_get_viewdef((quote_ident(schemaname) || '.' || quote_ident(viewname))::regclass, true)
FROM pg_views
WHERE schemaname = current_schema()
  AND viewname = %s"
                         (pgsql-escape-literal name))))))
        ("SEQUENCE"
         (caar (clutch-db-pg--metadata-rows
                (clutch-db-pg--exec
                 conn
                 (format "SELECT format(
  'CREATE SEQUENCE %%I.%%I INCREMENT BY %%s MINVALUE %%s MAXVALUE %%s START WITH %%s;',
  schemaname, sequencename, increment_by, min_value, max_value, start_value)
FROM pg_sequences
WHERE schemaname = current_schema()
  AND sequencename = %s"
                         (pgsql-escape-literal name))))))
        (_ nil)))))

(cl-defmethod clutch-db-table-comment ((conn clutch-db-pg--connection) table &optional schema)
  "Return the comment for TABLE in SCHEMA on PostgreSQL CONN, or nil if none."
  (clutch-db--translate-library-error pgsql-error
    (let* ((result (clutch-db-pg--exec
                      conn
                      (format "SELECT obj_description(%s, 'pg_class')"
                              (clutch-db-pg--relation table schema))))
             (row (car (clutch-db-pg--metadata-rows result)))
             (comment (car row)))
      (when (and comment (not (string-empty-p comment)))
        comment))))

(cl-defmethod clutch-db-primary-key-columns ((conn clutch-db-pg--connection) table
                                             &optional schema _catalog)
  "Return primary key column names for TABLE in SCHEMA on PostgreSQL CONN."
  (clutch-db--translate-library-error pgsql-error
    (let ((result (clutch-db-pg--exec
                   conn
                   (format "SELECT a.attname
FROM (SELECT i.indrelid, i.indkey::smallint[] AS key_array,
             generate_subscripts(i.indkey::smallint[], 1) AS ord
      FROM pg_index i
      WHERE i.indrelid = %s AND i.indisprimary) pk
JOIN pg_attribute a
  ON a.attrelid = pk.indrelid AND a.attnum = pk.key_array[pk.ord]
ORDER BY pk.ord"
                           (clutch-db-pg--relation table schema)))))
      (mapcar #'car (clutch-db-pg--metadata-rows result)))))

(cl-defmethod clutch-db-row-identity-candidates ((conn clutch-db-pg--connection) table
                                                 &optional schema _catalog)
  "Return row identity candidates for TABLE in SCHEMA on PostgreSQL CONN."
  (or (cl-call-next-method)
      (clutch-db-pg--unique-not-null-identities conn table schema)
      (when-let* ((ctid (clutch-db-pg--ctid-identity conn table schema)))
        (list ctid))))

(cl-defmethod clutch-db-foreign-keys ((conn clutch-db-pg--connection) table
                                      &optional schema _catalog)
  "Return foreign key info for TABLE in SCHEMA on PostgreSQL CONN.
The query keeps the keys that reference a table in TABLE's schema, and
each carries that schema as :ref-schema, also when the search path found
TABLE."
  (clutch-db--translate-library-error pgsql-error
    (let* ((sql (format "SELECT
    kcu.column_name,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column,
    n.nspname AS referenced_schema
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
JOIN pg_class c ON c.oid = %s
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name = c.relname
    AND tc.table_schema = n.nspname"
                          (clutch-db-pg--relation table schema)))
             (result (clutch-db-pg--exec conn sql)))
        (mapcar
         (lambda (row)
           (pcase-let ((`(,col-name ,ref-table ,ref-column ,ref-schema) row))
             (cons col-name
                   (list :ref-table ref-table :ref-column ref-column
                         :ref-schema ref-schema))))
         (clutch-db-pg--metadata-rows result)))))

(cl-defmethod clutch-db-column-details ((conn clutch-db-pg--connection) table
                                        &optional schema _catalog)
  "Return detailed column info for TABLE in SCHEMA on PostgreSQL CONN."
  (clutch-db--translate-library-error pgsql-error
    (let* ((col-result
              (clutch-db-pg--exec
               conn
               (format "SELECT c.column_name, c.data_type, c.udt_name, c.is_nullable, \
c.character_maximum_length, c.numeric_precision, c.numeric_scale, \
c.column_default, \
CASE WHEN c.is_identity = 'YES' OR c.is_generated = 'ALWAYS' THEN 'YES' END, \
col_description(pc.oid, a.attnum) \
FROM information_schema.columns c \
JOIN pg_class pc ON pc.relname = c.table_name \
JOIN pg_namespace pn ON pn.oid = pc.relnamespace \
  AND pn.nspname = c.table_schema \
JOIN pg_attribute a ON a.attrelid = pc.oid AND a.attname = c.column_name \
WHERE pc.oid = %s \
ORDER BY c.ordinal_position"
                       (clutch-db-pg--relation table schema))))
             (col-rows (clutch-db-pg--metadata-rows col-result))
             (namespace (clutch-db--namespace-arguments schema nil))
             (pk-cols  (apply #'clutch-db-primary-key-columns conn table namespace))
             (fks      (apply #'clutch-db-foreign-keys conn table namespace)))
      (mapcar (lambda (row) (clutch-db-pg--column-details-row row pk-cols fks))
              col-rows))))

;;;; Re-entrancy guard

(cl-defmethod clutch-db-busy-p ((conn clutch-db-pg--connection))
  "Return non-nil if PostgreSQL CONN is executing a query."
  (pgsql-busy-p (clutch-db-pg--connection-client conn)))

;;;; Metadata methods

(cl-defmethod clutch-db-user ((conn clutch-db-pg--connection))
  "Return the user for PostgreSQL CONN."
  (pgsql-user (clutch-db-pg--connection-client conn)))

(cl-defmethod clutch-db-host ((conn clutch-db-pg--connection))
  "Return the host for PostgreSQL CONN."
  (pgsql-host (clutch-db-pg--connection-client conn)))

(cl-defmethod clutch-db-port ((conn clutch-db-pg--connection))
  "Return the port for PostgreSQL CONN."
  (pgsql-port (clutch-db-pg--connection-client conn)))

(cl-defmethod clutch-db-database ((conn clutch-db-pg--connection))
  "Return the database for PostgreSQL CONN."
  (pgsql-database (clutch-db-pg--connection-client conn)))

;;;; XTDB

;; XTDB 2 speaks the PostgreSQL wire protocol, so the xtdb backend is this
;; adapter with the methods below overridden where XTDB differs: its
;; catalog has no comment functions and no array casts, every table is
;; keyed by _id, it reports its own column types, and json for a result
;; column that PostgreSQL has no type for, it needs typed DML parameters,
;; it reports no affected-row counts, it has no savepoints, and it has no
;; objects but tables and no schema to switch to.

(cl-defstruct (clutch-db-pg--xtdb-connection
               (:include clutch-db-pg--connection)
               (:constructor clutch-db-pg--make-xtdb-connection)
               (:copier nil))
  "Clutch-owned state for one XTDB connection over pgsql.el.")

(defun clutch-db-pg-xtdb-connect (params)
  "Connect to XTDB using PARAMS, which take the PostgreSQL keys.
XTDB cannot switch its current schema, so PARAMS may not set :schema."
  (when (plist-get params :schema)
    (user-error "XTDB cannot switch its current schema; remove :schema"))
  (clutch-db-pg-connect params #'clutch-db-pg--make-xtdb-connection))

(cl-defmethod clutch-db-backend-key ((_conn clutch-db-pg--xtdb-connection))
  "Return the registered backend key for XTDB connections."
  'xtdb)

(defconst clutch-db-pg--xtdb-types
  '((:utf8 "text" "text")
    (:i64 "bigint" "int8")
    (:i32 "integer" "int4")
    (:i16 "smallint" "int2")
    (:f64 "double precision" "float8")
    (:f32 "real" "float4")
    (:bool "boolean" "bool")
    (:decimal "numeric" "numeric")
    (:date "date" "date")
    (:instant "timestamp with time zone" "timestamptz")
    (:timestamp-tz "timestamp with time zone" "timestamptz")
    (:timestamp-local "timestamp without time zone" "timestamp")
    (:time-local "time without time zone" "time")
    (:uuid "uuid" "uuid")
    (:varbinary "bytea" "bytea"))
  "XTDB base types with their PostgreSQL type and parameter type names.
XTDB stores a parameter with the type it is sent as, so a column keeps its
own type only when each parameter names the matching one.")

(defconst clutch-db-pg--xtdb-system-columns
  '("_system_from" "_system_to" "_valid_from" "_valid_to")
  "Columns that XTDB maintains for every row.
No UPDATE may set them, though an INSERT may set _valid_from and _valid_to.")

(defun clutch-db-pg--xtdb-base-types (type)
  "Return the base type keywords of XTDB TYPE, as read from its printed form.
A nullable type such as [:? :date :day] has the type after `:?', and a
union has the base types of each member.  The :null that a union gains
when a NULL is stored only makes the column nullable, so it has none."
  (cond
   ((eq type :null) nil)
   ((keywordp type) (list type))
   ((and (vectorp type) (> (length type) 0))
    (pcase (aref type 0)
      (:? (and (> (length type) 1)
               (clutch-db-pg--xtdb-base-types (aref type 1))))
      (:union (mapcan #'clutch-db-pg--xtdb-base-types (cdr (append type nil))))
      (head (list head))))))

(defconst clutch-db-pg--xtdb-number-types
  '("int8" "int4" "int2" "numeric" "float8" "float4")
  "Parameter types of XTDB's number types, in the order an integer takes them.")

(defun clutch-db-pg--xtdb-pg-type (xtdb-type)
  "Return (DATA-TYPE PARAMETER-TYPE) for the printed XTDB-TYPE, or nil.
A union maps when all of its members map to the same type.  A union of
number types has no DATA-TYPE, and its PARAMETER-TYPE is the list of its
members' types, of which each value takes the one it fits."
  (let* ((type (condition-case nil (car (read-from-string xtdb-type))
                 (error nil)))
         (mapped (delete-dups
                  (mapcar (lambda (base)
                            (cdr (assq base clutch-db-pg--xtdb-types)))
                          (clutch-db-pg--xtdb-base-types type)))))
    (cond
     ((and mapped (null (cdr mapped))) (car mapped))
     ((and mapped
           (cl-every (lambda (pg-type)
                       (member (cadr pg-type) clutch-db-pg--xtdb-number-types))
                     mapped))
      (list nil (mapcar #'cadr mapped))))))

(defun clutch-db-pg--xtdb-number-type (value types)
  "Return the type among number TYPES that VALUE fits, or nil.
An integer takes an integer type and any other number a fractional one, so
a column whose type is a union of TYPES gains no member.  NULL takes any of
TYPES, and a value that is no number takes none, which XTDB refuses."
  (let ((text (cond ((numberp value) (number-to-string value))
                    ((stringp value) (string-trim value)))))
    (cl-find-if (lambda (type) (member type types))
                (cond
                 ((null value) types)
                 ((not (and text (string-match-p clutch-db-number-regexp text)))
                  nil)
                 ((string-match-p "\\`[+-]?[0-9]+\\'" text)
                  clutch-db-pg--xtdb-number-types)
                 (t '("float8" "float4" "numeric"))))))

(cl-defmethod clutch-db-pg--result-columns ((_conn clutch-db-pg--xtdb-connection)
                                           _pg-columns)
  "Return result columns, dropping the type of XTDB's json columns.
XTDB reports a column as json when PostgreSQL has no type for it, such as
a time or a union, and stores a json parameter as the value it decodes to.
A staged value for such a column takes its type from the column details
instead, or has none and is refused."
  (mapcar (lambda (column)
            (if (equal (plist-get column :backend-type) "json")
                (list :name (plist-get column :name))
              column))
          (cl-call-next-method)))

(cl-defmethod clutch-db-primary-key-columns ((_conn clutch-db-pg--xtdb-connection)
                                            _table &optional _schema _catalog)
  "Return _id, the key of every XTDB table."
  (list "_id"))

(cl-defmethod clutch-db-column-details ((conn clutch-db-pg--xtdb-connection) table
                                        &optional _schema _catalog)
  "Return detailed column info for TABLE on XTDB CONN.
Column types map to the PostgreSQL types that XTDB parameters take, so a
staged value is sent with its column's type; a type that does not map keeps
its XTDB name and no parameter type, which XTDB then refuses.  A union of
number types keeps its XTDB name too, with the list of its members' types
as the parameter type.  A row may omit any column but _id, so every other
column is nullable."
  (clutch-db--translate-library-error pgsql-error
    (let ((result (clutch-db-pg--exec
                   conn
                   (format "SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = %s AND table_schema = current_schema()
ORDER BY ordinal_position"
                           (pgsql-escape-literal table)))))
      (mapcar
       (lambda (row)
         (pcase-let* ((`(,name ,xtdb-type) row)
                      (`(,data-type ,parameter-type)
                       (clutch-db-pg--xtdb-pg-type xtdb-type))
                      (detail
                       (clutch-db-pg--column-details-row
                        (list name (or data-type xtdb-type) parameter-type
                              (if (equal name "_id") "NO" "YES")
                              nil nil nil nil
                              (and (member name clutch-db-pg--xtdb-system-columns)
                                   "YES")
                              nil)
                        '("_id") nil)))
           ;; The details row takes a single type name only.
           (if (consp parameter-type)
               (plist-put detail :backend-type parameter-type)
             detail)))
       (clutch-db-pg--metadata-rows result)))))

(cl-defmethod clutch-db-list-table-entries ((conn clutch-db-pg--xtdb-connection))
  "Return table entry plists for the current XTDB schema on CONN.
XTDB has no table comments."
  (let ((schema (clutch-db-current-schema conn)))
    (mapcar (lambda (name)
              (list :name name :type "TABLE"
                    :schema schema :source-schema schema))
            (clutch-db-list-tables conn))))

(cl-defmethod clutch-db-table-comment ((_conn clutch-db-pg--xtdb-connection) _table
                                       &optional _schema)
  "Return nil, since XTDB has no table comments."
  nil)

(cl-defmethod clutch-db-foreign-keys ((_conn clutch-db-pg--xtdb-connection) _table
                                      &optional _schema _catalog)
  "Return nil, since XTDB has no foreign keys."
  nil)

(cl-defmethod clutch-db-list-objects ((_conn clutch-db-pg--xtdb-connection) _category)
  "Return nil, since XTDB has no indexes, sequences, routines or triggers."
  nil)

(cl-defmethod clutch-db-list-schemas ((_conn clutch-db-pg--xtdb-connection))
  "Return nil, since XTDB cannot switch its current schema."
  nil)

(cl-defmethod clutch-db-execute-params ((conn clutch-db-pg--xtdb-connection)
                                        sql params)
  "Execute SQL with PARAMS on XTDB CONN, leaving its affected-row count unknown.
XTDB reports zero rows for every INSERT, UPDATE and DELETE, so a staged
UPDATE or DELETE cannot be checked against its one row.  A parameter for a
union of number types, whose type is a list, takes the member its value
fits."
  (let ((result
         (cl-call-next-method
          conn sql
          (mapcar (lambda (param)
                    (let ((type (clutch-db-param-type param))
                          (value (clutch-db-param-value param)))
                      (if (consp type)
                          (clutch-db-typed-param
                           value (clutch-db-pg--xtdb-number-type value type))
                        param)))
                  params))))
    (setf (clutch-db-result-affected-rows result) nil)
    result))

(cl-defmethod clutch-db-call-with-atomic-batch
    ((conn clutch-db-pg--xtdb-connection) _function)
  "Run a staged batch in a transaction on XTDB CONN.
Manual mode would run it inside the user's transaction through a
savepoint, which XTDB lacks, so it is refused there."
  (when (clutch-db-manual-commit-p conn)
    (user-error
     "XTDB has no savepoints; switch to Auto mode to submit staged changes"))
  (cl-call-next-method))

(provide 'clutch-db-pg)
;;; clutch-db-pg.el ends here
