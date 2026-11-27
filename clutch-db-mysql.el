;;; clutch-db-mysql.el --- Native backend over the MySQL wire client -*- lexical-binding: t; -*-

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

;; MySQL backend for the clutch generic database interface.
;; Implements all `clutch-db-*' generics by dispatching on `mysql-conn'.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'clutch-backend)

(declare-function mysql-async-pending-p "mysql" (conn))
(declare-function mysql-autocommit-p "mysql" (conn))
(declare-function mysql-busy-p "mysql" (conn))
(declare-function mysql-commit "mysql" (conn))
(declare-function mysql-connect "mysql" (&rest params))
(declare-function mysql-connection-host "mysql" (conn))
(declare-function mysql-connection-id "mysql" (conn))
(declare-function mysql-connection-port "mysql" (conn))
(declare-function mysql-connection-user "mysql" (conn))
(declare-function mysql-current-database "mysql" (conn))
(declare-function mysql-disconnect "mysql" (conn))
(declare-function mysql-drain-query-response "mysql" (conn read-idle-timeout))
(declare-function mysql-escape-identifier "mysql" (identifier))
(declare-function mysql-escape-literal "mysql" (string))
(declare-function mysql-execute "mysql" (stmt &rest params))
(declare-function mysql-in-transaction-p "mysql" (conn))
(declare-function mysql-live-p "mysql" (conn))
(declare-function mysql-prepare "mysql" (conn sql))
(declare-function mysql-query "mysql" (conn sql))
(declare-function mysql-query-rows-async "mysql" (conn sql function callback))
(declare-function mysql-query-async "mysql" (conn sql callback))
(declare-function mysql-refresh-current-database "mysql" (conn))
(declare-function mysql-result-affected-rows "mysql" (object))
(declare-function mysql-result-columns "mysql" (object))
(declare-function mysql-result-connection "mysql" (object))
(declare-function mysql-result-last-insert-id "mysql" (object))
(declare-function mysql-result-rows "mysql" (object))
(declare-function mysql-result-warnings "mysql" (object))
(declare-function mysql-conn-server-version "mysql" (conn))
(declare-function mysql-rollback "mysql" (conn))
(declare-function mysql-select-database "mysql" (conn database))
(declare-function mysql-set-autocommit "mysql" (conn autocommit))
(declare-function mysql-stmt-close "mysql" (stmt))
;;;; Type-category mapping

(defconst clutch-db-mysql--type-category-alist
  '((0   . numeric) ; DECIMAL
    (1   . numeric) ; TINYINT
    (2   . numeric) ; SMALLINT
    (3   . numeric) ; INT
    (4   . numeric) ; FLOAT
    (5   . numeric) ; DOUBLE
    (8   . numeric) ; BIGINT
    (9   . numeric) ; MEDIUMINT
    (13  . numeric) ; YEAR
    (246 . numeric) ; NEWDECIMAL
    (245 . json)    ; JSON
    (252 . blob)    ; BLOB
    (249 . blob)    ; TINYBLOB
    (250 . blob)    ; MEDIUMBLOB
    (251 . blob)    ; LONGBLOB
    (10  . date)    ; DATE
    (11  . time)    ; TIME
    (12  . datetime) ; DATETIME
    (7   . datetime)) ; TIMESTAMP
  "Alist mapping MySQL type codes to type-category symbols.")

(defconst clutch-db-mysql--binary-charset 63
  "MySQL charset code for binary.
Blob-family types with this charset are true BLOBs; others are TEXT.")

(defconst clutch-db-mysql--blob-family-types
  '(252 249 250 251)
  "MySQL type codes that share BLOB/TEXT family encodings.")

(defcustom clutch-db-mysql-cancel-timeout-seconds 5
  "Seconds to wait while cancelling and draining an interrupted MySQL query."
  :type 'number
  :group 'clutch)

(defvar clutch-db-mysql--connection-params
  (make-hash-table :test 'eq :weakness 'key)
  "Wire connection parameters keyed by MySQL connection objects.")

(defvar clutch-db-mysql--methods-installed nil
  "Non-nil when MySQL generic methods were installed.")

(defun clutch-db-mysql--timeout-error-message (err recovered)
  "Return a user-facing timeout message for ERR.
RECOVERED is non-nil when the MySQL wire connection was resynchronized."
  (concat (clutch-db--error-message err)
          (if recovered
              "; restored MySQL connection"
            "; disconnected MySQL connection because timeout recovery failed")))

(defun clutch-db-mysql--handle-query-timeout (conn err &optional stmt)
  "Recover or close CONN after MySQL query timeout ERR, then signal error.
STMT is released only once recovery has resynchronized the wire."
  (let ((recovered (clutch-db-interrupt-query conn)))
    (if recovered
        (when stmt (ignore-errors (mysql-stmt-close stmt)))
      (ignore-errors (mysql-disconnect conn)))
    (signal 'clutch-db-error
            (list (clutch-db-mysql--timeout-error-message err recovered)))))

(defun clutch-db-mysql--metadata-query (conn sql)
  "Read metadata with SQL on CONN, recovering the wire after a timeout.
Return the native result, retaining the metadata readers' value handling."
  (condition-case err
      (mysql-query conn sql)
    (mysql-timeout
     (clutch-db-mysql--handle-query-timeout conn err))))

(defun clutch-db-mysql--ensure-client-api ()
  "Ensure mysql.el is available and this adapter installed its methods."
  (unless (require 'mysql nil t)
    (signal 'clutch-db-error
            (list "MySQL backend requires mysql.el. Install LuciusChen/mysql.el, ensure it is on load-path, then restart Emacs.")))
  (unless clutch-db-mysql--methods-installed
    (signal 'clutch-db-error
            (list "MySQL backend was loaded before mysql.el was available. Restart Emacs after installing mysql.el."))))

(defun clutch-db-mysql--apply-timeout-defaults (params)
  "Return PARAMS with MySQL timeout defaults filled in."
  (clutch-db--apply-connect-defaults
   params
   `((:connect-timeout . ,clutch-connect-timeout-seconds)
     (:read-idle-timeout . ,clutch-read-idle-timeout-seconds))))

(defun clutch-db-mysql--wire-connect-args (params)
  "Return PARAMS with clutch-only keys removed for `mysql-connect'."
  (cl-loop for (key value) on params by #'cddr
           unless (memq key '(:sql-product :backend :pass-entry))
           append (list key value)))

(defun clutch-db-mysql--type-category (mysql-type charset)
  "Map a MySQL type code MYSQL-TYPE (with CHARSET) to a type-category symbol.
For the blob-family type codes, charset 63 (binary) means a true BLOB;
any other charset means a TEXT column."
  (if (memq mysql-type clutch-db-mysql--blob-family-types)
      (if (= charset clutch-db-mysql--binary-charset) 'blob 'text)
    (or (alist-get mysql-type clutch-db-mysql--type-category-alist)
        'text)))

(defun clutch-db-mysql--convert-columns (mysql-columns)
  "Convert MYSQL-COLUMNS to result column plists.
MySQL gives constants no original table, even when a CTE names them.
Its original column name, however, can be the name exposed by the CTE."
  (mapcar (lambda (col)
            (let ((column (list :name (plist-get col :name)
                                :type-category (clutch-db-mysql--type-category
                                                (plist-get col :type)
                                                (plist-get col :character-set)))))
              (cond ((equal (plist-get col :org-table) "") (plist-put column :source-column nil))
                    ((and (plist-get col :org-table) (not (string-empty-p (plist-get col :org-name))))
                     (plist-put column :origin (cons (plist-get col :org-table) (plist-get col :org-name))))
                    (t column))))
          mysql-columns))

(defconst clutch-db-mysql--max-warning-messages 100
  "Most warnings read for one statement.
An INSERT of many rows can raise one per row, up to `max_error_count'.")

(defun clutch-db-mysql--warning-messages (conn count)
  "Return the COUNT warnings of CONN's last statement as display strings.
Warnings past `clutch-db-mysql--max-warning-messages', or past the
server's `max_error_count', are counted in one message.  The statement
already ran, so a failure to read them becomes a message rather than its
error, which would also stop an asynchronous callback, after a timeout
has resynchronized or closed CONN."
  (when (and count (> count 0))
    (condition-case err
        (let ((rows (condition-case timeout
                        (mysql-result-rows
                         (mysql-query
                          conn (format "SHOW WARNINGS LIMIT %d"
                                       clutch-db-mysql--max-warning-messages)))
                      (mysql-timeout
                       (clutch-db-mysql--handle-query-timeout conn timeout)))))
          (append
           (mapcar (pcase-lambda (`(,level ,code ,message))
                     (format "%s (%s): %s" level code message))
                   rows)
           (when (> count (length rows))
             (list (format "%d more warnings not shown"
                           (- count (length rows)))))))
      ((clutch-db-error mysql-error)
       (list (format "Warnings could not be read: %s"
                     (clutch-db--error-message err)))))))

(defun clutch-db-mysql--wrap-result (mysql-result)
  "Convert MYSQL-RESULT to a `clutch-db-result'.
Its warnings are read at once, before another statement replaces them."
  (let ((cols (mysql-result-columns mysql-result))
        (conn (mysql-result-connection mysql-result))
        (warnings (mysql-result-warnings mysql-result)))
    (make-clutch-db-result
     :connection conn
     :columns (when cols (clutch-db-mysql--convert-columns cols))
     :rows (mysql-result-rows mysql-result)
     :affected-rows (mysql-result-affected-rows mysql-result)
     :last-insert-id (mysql-result-last-insert-id mysql-result)
     :warnings warnings
     :messages (clutch-db-mysql--warning-messages conn warnings))))

;;;; Connect function

(defun clutch-db-mysql-connect (params)
  "Connect to MySQL using PARAMS plist.
PARAMS keys: :host, :port, :user, :password, :database, :tls,
:ssl-mode, :connect-timeout, :read-idle-timeout.
For MySQL, explicit `:tls nil' or `:ssl-mode disabled' forces plaintext."
  (clutch-db-mysql--ensure-client-api)
  (clutch-db--translate-library-error mysql-error
    (let* ((wire-args (clutch-db-mysql--wire-connect-args
                       (clutch-db-mysql--apply-timeout-defaults
                        (clutch-db--reject-removed-connect-params params))))
           (conn (apply #'mysql-connect wire-args)))
      (puthash conn (copy-sequence wire-args)
               clutch-db-mysql--connection-params)
      conn)))

(defun clutch-db-mysql--drain-interrupted-response (conn &optional propagate-rejection)
  "Drain the interrupted query response from CONN.
Return non-nil when the wire protocol is synchronized again.
With PROPAGATE-REJECTION, signal the server's error after consuming it."
  (condition-case err
      (progn
        (mysql-drain-query-response conn clutch-db-mysql-cancel-timeout-seconds)
        t)
    ;; KILL QUERY returns an ERR packet after the response has been consumed.
    (mysql-query-error (if propagate-rejection (signal (car err) (cdr err)) t))
    (mysql-error nil)))

(defun clutch-db-mysql--transaction-command (conn command &rest args)
  "Run transaction COMMAND on CONN with ARGS, recovering a timed-out reply."
  (condition-case err
      (apply command conn args)
    (mysql-timeout
     ;; COMMIT may already be durable.  Drain the reply without issuing
     ;; KILL or repeating the command, and preserve the original failure.
     (let* ((inhibit-quit t)
            (drained (clutch-db-mysql--drain-interrupted-response conn)))
       (unless drained (ignore-errors (mysql-disconnect conn)))
       (signal 'clutch-db-error
               (list (clutch-db-mysql--timeout-error-message err drained)))))
    (mysql-error (signal 'clutch-db-error (clutch-db--error-data err)))))

(defun clutch-db-mysql--help-sql (symbol)
  "Return the MySQL HELP query for SYMBOL."
  (format "HELP %s" (mysql-escape-literal (upcase (format "%s" symbol)))))

(defun clutch-db-mysql--parse-help-rows (rows)
  "Return the signature and description from single-topic MySQL HELP ROWS."
  (when-let* ((text (nth 1 (car rows))) ((stringp text))
              ((string-match "\\(?:\\`\\|\n\\)Syntax:[[:blank:]\r\n]*" text))
              (paragraphs (split-string (substring text (match-end 0))
                                        "\n[[:blank:]\r\n]*\n"
                                        t "[[:blank:]\r\n]+"))
              (sig (car paragraphs)))
    (let* ((desc (cadr paragraphs))
           (sig (string-join (split-string sig "\n" t "[[:blank:]]+")
                             " / "))
           (desc (car (split-string (or desc "") "\nURL:" t)))
           (desc (string-join (split-string (or desc "") "\n" t "[[:blank:]]+")
                              " ")))
      (unless (string-empty-p sig)
        (list :sig sig :desc desc)))))

(defun clutch-db-mysql--qualified-identifier (name &optional schema)
  "Return backtick-quoted NAME, qualified by SCHEMA when non-empty."
  (if (and schema (not (string-empty-p schema)))
      (format "%s.%s"
              (mysql-escape-identifier schema)
              (mysql-escape-identifier name))
    (mysql-escape-identifier name)))

(defun clutch-db-mysql--entry-schema (entry)
  "Return non-empty schema name for MySQL object ENTRY, or nil."
  (let ((schema (or (plist-get entry :schema)
                    (plist-get entry :source-schema))))
    (unless (or (null schema) (string-empty-p schema))
      schema)))

(defun clutch-db-mysql--unique-not-null-identities (conn table &optional schema)
  "Return unique-not-null row identity candidates for TABLE in SCHEMA on CONN."
  (clutch-db--translate-library-error mysql-error
    (let ((result
           (clutch-db-mysql--metadata-query
            conn
            (format "SHOW KEYS FROM %s WHERE Non_unique = 0 AND Key_name <> 'PRIMARY'"
                    (clutch-db-mysql--qualified-identifier table schema))))
          (indexes (make-hash-table :test 'equal))
          (invalid (make-hash-table :test 'equal))
          order)
      (dolist (row (mysql-result-rows result))
        (pcase-let ((`(,_table ,_non-unique ,name ,seq ,column
                       ,_collation ,_cardinality ,_sub-part ,_packed
                       ,nullable . ,_)
                     row))
          (when (and (stringp name) (not (gethash name indexes)))
            (push name order))
          (if (and (stringp name)
                   (stringp column)
                   (or (null nullable)
                       (string-empty-p (format "%s" nullable))))
              (puthash name
                       (cons (cons (or seq 0) column)
                             (gethash name indexes))
                       indexes)
            (puthash name t invalid))))
      (cl-loop for name in (sort (nreverse order) #'string-collate-lessp)
               unless (gethash name invalid)
               collect (list
                        :kind 'unique-key
                        :name name
                        :columns (mapcar
                                  #'cdr
                                  (sort (gethash name indexes)
                                        (lambda (a b)
                                          (< (car a) (car b))))))))))

(defun clutch-db-mysql--namespace-statement-p (sql)
  "Return non-nil for SQL after which the current database may differ.
USE chooses another one, and a DROP of the current database leaves the
session with none.  Only the leading keyword counts, and the server then
says which database is current, provided mysql.el can record it."
  (and (fboundp 'mysql-refresh-current-database)
       (member (clutch-db-sql-leading-keyword sql) '("USE" "DROP"))))

(defun clutch-db-mysql--follow-database (conn sql)
  "Record the database CONN is on after SQL, when SQL may have moved it.
mysql.el keeps the database that `mysql-select-database' chose, so a USE
typed into a console would leave it on the previous one, and a DROP of
that one on a database that no longer exists.  Failing to ask the server
is only reported, after a timeout has resynchronized or closed CONN: SQL
itself succeeded."
  (when (clutch-db-mysql--namespace-statement-p sql)
    (condition-case err
        (condition-case timeout
            (mysql-refresh-current-database conn)
          (mysql-timeout
           (clutch-db-mysql--handle-query-timeout conn timeout)))
      ((clutch-db-error mysql-error)
       (message "Could not read the current database back: %s"
                (clutch-db--error-message err))))))

(defun clutch-db-mysql--key-column-rows (conn columns where)
  "Return KEY_COLUMN_USAGE rows of foreign keys on CONN matching WHERE.
COLUMNS lists the columns to select.  Rows come in key order, and names
come back as strings."
  (let ((sql (clutch-db-mysql--key-column-sql columns where)))
    (mapcar (lambda (row)
              (mapcar (lambda (value)
                        (if (stringp value) value (format "%s" value)))
                      row))
            (mysql-result-rows (clutch-db-mysql--metadata-query conn sql)))))

(defun clutch-db-mysql--schema-literal (schema)
  "Return SQL naming SCHEMA, or the current database without one."
  (if schema (mysql-escape-literal schema) "DATABASE()"))

;;;; Lifecycle methods

(defun clutch-db-mysql--legacy-target-sql (table schema)
  "Build the scoped engine lookup for TABLE in SCHEMA."
  (format "SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
          (clutch-db-mysql--schema-literal schema) (mysql-escape-literal table)))

(defun clutch-db-mysql--dictionary-sql (rows)
  "Build a dictionary lookup when target ROWS have safe InnoDB SQL names."
  (pcase rows
    (`((,owner ,name "InnoDB"))
     ;; Other names are filename encoded in the legacy dictionary.
     (when (and (string-match-p "\\`[A-Za-z0-9_]+\\'" owner)
                (string-match-p "\\`[A-Za-z0-9_]+\\'" name))
       (format "SELECT FOR_NAME FROM INFORMATION_SCHEMA.INNODB_SYS_FOREIGN WHERE REF_NAME = %s ORDER BY FOR_NAME"
               (mysql-escape-literal (concat owner "/" name)))))))

(defun clutch-db-mysql--dictionary-tables (rows)
  "Decode dictionary ROWS to child tables, or `unsupported' for encoded names."
  (let ((tables (delete-dups (mapcar #'car rows))))
    (if (cl-every (lambda (name)
                    (string-match-p "\\`[A-Za-z0-9_]+/[A-Za-z0-9_]+\\'" name))
                  tables)
        (mapcar (lambda (name) (split-string name "/")) tables)
      'unsupported)))

(defun clutch-db-mysql--dictionary-unavailable-p (err)
  "Return non-nil when ERR means the legacy dictionary is unavailable."
  (string-match-p "\\`\\[\\(?:1109\\|1142\\|1146\\|1227\\)\\]"
                  (clutch-db--error-message err)))

(defun clutch-db-mysql--legacy-referencing-tables (conn table schema)
  "Find InnoDB tables referencing TABLE in SCHEMA on CONN.
Return schema/table pairs, or `unsupported' for the standard lookup."
  (condition-case err
      (if-let* ((sql (clutch-db-mysql--dictionary-sql
                      (mysql-result-rows
                       (clutch-db-mysql--metadata-query
                        conn (clutch-db-mysql--legacy-target-sql table schema))))))
          (clutch-db-mysql--dictionary-tables
           (mysql-result-rows (clutch-db-mysql--metadata-query conn sql)))
        'unsupported)
    (mysql-query-error
     (if (clutch-db-mysql--dictionary-unavailable-p err) 'unsupported
       (signal (car err) (cdr err))))))

(defun clutch-db-mysql--send-metadata (conn sql callback &optional background)
  "Send metadata SQL on CONN, delivering raw rows and error to CALLBACK.
BACKGROUND requests also defer while foreground work owns the connection."
  (unless (or (mysql-busy-p conn)
              (and background (clutch-db--foreground-busy-p conn)))
    (mysql-query-async conn sql
                       (lambda (result error)
                         (funcall callback (and result (mysql-result-rows result)) error)))
    t))

(defun clutch-db-mysql--referencing-keys-async (conn table schema callback continue)
  "Read incoming keys of TABLE in SCHEMA on CONN, observing CONTINUE."
  (let ((send (lambda (sql outcome) (clutch-db-mysql--send-metadata conn sql outcome)))
        (columns '("TABLE_SCHEMA" "TABLE_NAME" "COLUMN_NAME"
                   "REFERENCED_COLUMN_NAME" "CONSTRAINT_NAME"))
        (where (format "REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s"
                       (clutch-db-mysql--schema-literal schema)
                       (mysql-escape-literal table))))
    (cl-labels
        ((read-keys (tables)
           (clutch-db--metadata-request-series
            send
            (if (eq tables 'unsupported)
                (list (clutch-db-mysql--key-column-sql columns where))
              (cl-loop for (owner child) in tables
                       collect (clutch-db-mysql--key-column-sql
                                columns (format "TABLE_SCHEMA = %s AND TABLE_NAME = %s AND %s"
                                                (mysql-escape-literal owner)
                                                (mysql-escape-literal child) where))))
            (lambda (results)
              (clutch-db-mysql--referencing-key-values (apply #'append results)))
            callback continue)))
      (if (string-match-p "\\`5\\.[67]\\." (or (mysql-conn-server-version conn) ""))
          (clutch-db--metadata-request-series
           send (list (clutch-db-mysql--legacy-target-sql table schema))
           (lambda (results) (clutch-db-mysql--dictionary-sql (car results)))
           (lambda (sql error)
             (cond
              (error (funcall callback nil error))
              ((null sql) (read-keys 'unsupported))
              (t
               (clutch-db--metadata-request-series
                send (list sql)
                (lambda (results) (clutch-db-mysql--dictionary-tables (car results)))
                (lambda (tables error)
                  (cond ((and error (clutch-db-mysql--dictionary-unavailable-p error))
                         (read-keys 'unsupported))
                        (error (funcall callback nil error))
                        (t (read-keys tables))))
                continue))))
           continue)
        (read-keys 'unsupported)))))


(defun clutch-db-mysql--primary-key-sql (table schema)
  "Build the metadata query for TABLE in SCHEMA."
  (format "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'"
          (if schema
              (concat (mysql-escape-identifier schema) "."
                      (mysql-escape-identifier table))
            (mysql-escape-identifier table))))

(defun clutch-db-mysql--column-details-sql (table schema)
  "Build the metadata query for TABLE in SCHEMA."
  (format "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, \
COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT \
FROM INFORMATION_SCHEMA.COLUMNS \
WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s \
ORDER BY ORDINAL_POSITION"
          (if schema (mysql-escape-literal schema) "DATABASE()")
          (mysql-escape-literal table)))

(defun clutch-db-mysql--column-details-row (row pk-cols fks)
  "Decode column ROW using PK-COLS and FKS."

  (pcase-let ((`(,name ,type ,nullable-str ,default-val ,extra ,comment) row))
    (let* ((nullable (string= nullable-str "YES"))
           (pk-p (member name pk-cols))
           (fk (cdr (assoc name fks)))
           (read-only (and extra
                           (string-match-p
                            "\\_<\\(VIRTUAL\\|STORED\\) GENERATED\\_>"
                            extra)))
           (generated (or read-only
                          (and extra
                               (string-match-p "\\_<auto_increment\\_>"
                                               extra)))))
      (list :name name :type type :nullable nullable
            :primary-key (and pk-p t)
            :foreign-key fk
            :default (unless generated default-val)
            :generated (and generated t)
            :read-only (and read-only t)
            :comment (and comment (not (string-empty-p comment)) comment)))))

(defun clutch-db-mysql--key-column-sql (columns where)
  "Build a foreign-key query selecting COLUMNS under WHERE."
  (format "SELECT %s \
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
WHERE %s AND REFERENCED_TABLE_NAME IS NOT NULL \
ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION"
          (string-join columns ", ") where))

(defun clutch-db-mysql--foreign-key-values (rows)
  "Decode foreign-key ROWS."
  (mapcar (pcase-lambda (`(,column ,ref-schema ,ref-table ,ref-column ,constraint))
            (cons column (list :ref-table ref-table :ref-column ref-column
                               :ref-schema ref-schema :constraint constraint))) rows))

(defun clutch-db-mysql--referencing-key-values (rows)
  "Decode foreign-key ROWS."
  (mapcar (pcase-lambda (`(,ref-schema ,ref-table ,column ,ref-column ,constraint))
            (list :table ref-table :schema ref-schema :column column
                  :ref-column ref-column :constraint constraint)) rows))

(defun clutch-db-mysql--column-names-sql (table schema)
  "Return the column-name query for TABLE in optional SCHEMA."
  (if schema
      (format "SELECT column_name FROM information_schema.columns WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION"
              (mysql-escape-literal schema) (mysql-escape-literal table))
    (format "SHOW COLUMNS FROM %s" (mysql-escape-identifier table))))

(defun clutch-db-mysql--table-comment-sql (table &optional schema)
  "Return the comment query for TABLE in optional SCHEMA."
  (format "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
          (clutch-db-mysql--schema-literal schema)
          (mysql-escape-literal table)))

(defun clutch-db-mysql--foreign-key-sql (table schema)
  "Return the outgoing-key query for TABLE in optional SCHEMA."
  (clutch-db-mysql--key-column-sql
   '("COLUMN_NAME" "REFERENCED_TABLE_SCHEMA" "REFERENCED_TABLE_NAME"
     "REFERENCED_COLUMN_NAME" "CONSTRAINT_NAME")
   (format "TABLE_SCHEMA = %s AND TABLE_NAME = %s"
           (clutch-db-mysql--schema-literal schema) (mysql-escape-literal table))))

(defun clutch-db-mysql--object-sql (category)
  "Return the metadata query for object CATEGORY, or nil when unsupported."
  (pcase category
    ('indexes
     "SELECT DISTINCT INDEX_NAME, TABLE_NAME, NON_UNIQUE
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
ORDER BY TABLE_NAME, INDEX_NAME")
    ((or 'procedures 'functions)
     (format "SELECT ROUTINE_NAME, ROUTINE_TYPE
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_SCHEMA = DATABASE()
  AND ROUTINE_TYPE = %s
ORDER BY ROUTINE_NAME"
             (mysql-escape-literal
              (if (eq category 'procedures) "PROCEDURE" "FUNCTION"))))
    ('triggers
     "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING
FROM INFORMATION_SCHEMA.TRIGGERS
WHERE TRIGGER_SCHEMA = DATABASE()
ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME")))

(defun clutch-db-mysql--object-rows (schema category rows)
  "Convert object ROWS from SCHEMA and CATEGORY to entry plists."
  (mapcar
   (lambda (row)
     (pcase category
       ('indexes
        (pcase-let ((`(,name ,table ,non-unique) row))
          (list :name name :type "INDEX" :schema schema :source-schema schema
                :target-table table :unique (equal non-unique 0))))
       ((or 'procedures 'functions)
        (pcase-let ((`(,name ,type) row))
          (list :name name :type type :schema schema :source-schema schema)))
       ('triggers
        (pcase-let ((`(,name ,table ,event ,timing) row))
          (list :name name :type "TRIGGER" :schema schema :source-schema schema
                :target-table table :event event :timing timing :status "ENABLED")))))
   rows))

(defun clutch-db-mysql--column-metadata-async (conn table schema outcome continue &optional background)
  "Read TABLE columns and key metadata on CONN in SCHEMA asynchronously.
Deliver OUTCOME while CONTINUE allows requests, retaining keys for ER.
BACKGROUND defers dispatch while foreground work owns the connection."
  (clutch-db--metadata-request-series
   (lambda (sql outcome) (clutch-db-mysql--send-metadata conn sql outcome background))
   (list (clutch-db-mysql--primary-key-sql table schema)
         (clutch-db-mysql--foreign-key-sql table schema)
         (clutch-db-mysql--column-details-sql table schema))
   (pcase-lambda (`(,primary ,foreign ,columns))
     (let ((primary (mapcar (lambda (row) (nth 4 row)) primary))
           (foreign (clutch-db-mysql--foreign-key-values foreign)))
       (list :columns
             (mapcar (lambda (row) (clutch-db-mysql--column-details-row row primary foreign)) columns)
             :foreign-keys foreign)))
   outcome continue))

(when (require 'mysql nil t)

(cl-defmethod clutch-db-disconnect ((conn mysql-conn))
  "Disconnect MySQL CONN."
  (mysql-disconnect conn))

(cl-defmethod clutch-db-live-p ((conn mysql-conn))
  "Return non-nil if MySQL CONN is live."
  (mysql-live-p conn))

(cl-defmethod clutch-db-backend-key ((_conn mysql-conn))
  "Return the registered backend key for MySQL connections."
  'mysql)

(cl-defmethod clutch-db-interrupt-query ((conn mysql-conn))
  "Interrupt the active MySQL query on CONN without dropping the session."
  (let ((thread-id (mysql-connection-id conn))
        (params (gethash conn clutch-db-mysql--connection-params))
        ;; An asynchronous query reads its own verdict, even one that
        ;; finishes while the killer connects, so it needs no drain.
        (async (and (fboundp 'mysql-async-pending-p)
                    (mysql-async-pending-p conn)))
        killer)
    (and thread-id
         params
         (condition-case nil
             (let ((inhibit-quit t))
               (unwind-protect
                   (progn
                     (setq killer
                           (apply #'mysql-connect
                                  ;; KILL QUERY needs no database, and the one
                                  ;; CONN was opened on may have been dropped.
                                  (plist-put
                                   (plist-put (copy-sequence params)
                                              :database nil)
                                   :read-idle-timeout
                                   clutch-db-mysql-cancel-timeout-seconds)))
                     (mysql-query killer (format "KILL QUERY %d" thread-id))
                     (or async
                         (clutch-db-mysql--drain-interrupted-response conn)))
                 (when killer
                   (ignore-errors (mysql-disconnect killer)))))
           (mysql-error nil)))))

(cl-defmethod clutch-db-init-connection ((conn mysql-conn))
  "Initialize MySQL CONN with utf8mb4."
  (condition-case err
      (mysql-query conn "SET NAMES utf8mb4")
    (mysql-error
     (signal 'clutch-db-error
             (list (format "Init failed: %s" (clutch-db--error-message err)))))))

(cl-defmethod clutch-db-eager-schema-refresh-p ((_conn mysql-conn))
  "MySQL schema refresh should not block connect."
  nil)

(cl-defmethod clutch-db-completion-deferred-columns-p ((_conn mysql-conn))
  "MySQL completion should hydrate uncached columns when idle."
  t)

;;;; Transaction methods

(cl-defmethod clutch-db-manual-commit-p ((conn mysql-conn))
  "Return non-nil for MySQL CONN with autocommit disabled."
  (not (mysql-autocommit-p conn)))

(cl-defmethod clutch-db-manual-commit-supported-p ((_conn mysql-conn))
  "Return non-nil for MySQL runtime autocommit toggling."
  t)

(cl-defmethod clutch-db-transaction-open-p ((conn mysql-conn))
  "Return t when the last OK or EOF packet on CONN had a transaction open.
An error packet carries no status flags, so after an error this still
gives the status from before it."
  (and (mysql-in-transaction-p conn) t))

(cl-defmethod clutch-db-commit ((conn mysql-conn))
  "Commit the current transaction on MySQL CONN."
  (clutch-db-mysql--transaction-command conn #'mysql-commit))

(cl-defmethod clutch-db-rollback ((conn mysql-conn))
  "Roll back the current transaction on MySQL CONN."
  (clutch-db-mysql--transaction-command conn #'mysql-rollback))

(cl-defmethod clutch-db-set-auto-commit ((conn mysql-conn) auto-commit)
  "Set autocommit mode on MySQL CONN.
AUTO-COMMIT non-nil enables autocommit; nil enables manual commit."
  (clutch-db-mysql--transaction-command conn #'mysql-set-autocommit auto-commit))

(cl-defmethod clutch-db-call-with-atomic-batch
  ((conn mysql-conn) function)
  "Call FUNCTION atomically in the selected MySQL transaction mode on CONN."
  (clutch-db--translate-library-error mysql-error
    (if (clutch-db-manual-commit-p conn)
        (clutch-db--call-with-sql-savepoint
         conn function
         (lambda ()
           (unless (mysql-in-transaction-p conn)
             (clutch-db-query conn "START TRANSACTION"))))
      (when (mysql-in-transaction-p conn)
        (user-error
         "Session already has an explicit transaction; switch to Manual mode or finish it before submitting"))
      (clutch-db--call-with-transaction-boundary
       (lambda () (clutch-db-query conn "START TRANSACTION"))
       function
       (lambda () (clutch-db-commit conn))
       (lambda () (clutch-db-rollback conn))))))

(cl-defmethod clutch-db-schema-transaction-effect ((_conn mysql-conn) _sql)
  "Return `clear' because MySQL DDL commits the current transaction."
  'clear)

;;;; Query methods

(cl-defmethod clutch-db-query ((conn mysql-conn) sql)
  "Execute SQL on MySQL CONN, returning a `clutch-db-result'."
  (condition-case err
      (prog1 (clutch-db-mysql--wrap-result (mysql-query conn sql))
        (clutch-db-mysql--follow-database conn sql))
    (mysql-timeout
     (clutch-db-mysql--handle-query-timeout conn err))
    (mysql-error
     (signal 'clutch-db-error (clutch-db--error-data err)))))

(cl-defmethod clutch-db-query-async ((conn mysql-conn) sql callback)
  "Start SQL on MySQL CONN and pass the outcome to CALLBACK.
Decline when the installed mysql.el cannot execute asynchronously."
  (when (fboundp 'mysql-query-async)
    (clutch-db--translate-library-error mysql-error
      (mysql-query-async
       conn sql
       (lambda (result error)
         ;; CONN is idle when this runs.  Wrapping reads the statement's
         ;; warnings, so it goes before asking for the database.
         (let ((wrapped (and result (clutch-db-mysql--wrap-result result))))
           (when result
             (clutch-db-mysql--follow-database conn sql))
           (funcall callback
                    wrapped
                    (and error
                         (cons 'clutch-db-error
                               (clutch-db--error-data error))))))))
    t))

(cl-defmethod clutch-db--stream-export-rows ((conn mysql-conn) sql emit callback)
  "Stream SQL on CONN to EMIT, then call CALLBACK with the outcome."
  (when (fboundp 'mysql-query-rows-async)
    (let (names)
      (clutch-db--translate-library-error mysql-error
        (mysql-query-rows-async
         conn sql
         (lambda (rows columns)
           (unless names (setq names (mapcar (lambda (col) (plist-get col :name)) columns)))
           (condition-case err
               (funcall emit rows names)
             ((error quit)
              ;; Stop the server's work; mysql.el consumes the final packet.
              (clutch-db-interrupt-query conn)
              (signal (car err) (cdr err)))))
         (lambda (result error)
           (funcall callback (and result (clutch-db-mysql--wrap-result result))
                    (and error (cons 'clutch-db-error (clutch-db--error-data error))))))))
    t))

(cl-defmethod clutch-db-symbol-help ((conn mysql-conn) symbol)
  "Return MySQL HELP metadata for SYMBOL on CONN, or nil when unknown."
  (clutch-db--translate-library-error mysql-error
    (clutch-db-mysql--parse-help-rows
     (mysql-result-rows
      (clutch-db-mysql--metadata-query conn (clutch-db-mysql--help-sql symbol))))))

(cl-defmethod clutch-db-symbol-help-async ((conn mysql-conn) symbol callback)
  "Read MySQL HELP for SYMBOL on CONN and deliver it to CALLBACK."
  (when (fboundp 'mysql-query-async)
    (clutch-db--translate-library-error mysql-error
      (clutch-db-mysql--send-metadata
       conn (clutch-db-mysql--help-sql symbol)
       (lambda (rows err) (funcall callback (clutch-db-mysql--parse-help-rows rows) err))
       t))))

(cl-defmethod clutch-db-execute-params ((conn mysql-conn) sql params)
  "Execute parameterized SQL on MySQL CONN with PARAMS."
  (let (stmt result pending-error timeout-error)
    (condition-case err
        (setq stmt (mysql-prepare conn sql))
      (mysql-timeout
       (setq timeout-error err))
      (mysql-error
       (setq pending-error err)))
    (when stmt
      (unwind-protect
          (condition-case err
              (setq result
                    (clutch-db-mysql--wrap-result
                     (apply #'mysql-execute
                            stmt (clutch-db-param-values params))))
            (mysql-timeout
             (setq timeout-error err))
            (mysql-error
             (setq pending-error err)))
        ;; A timed-out statement leaves its response on the wire; closing it
        ;; here would desynchronize the session before recovery can drain it.
        (unless timeout-error
          (condition-case err
              (mysql-stmt-close stmt)
            (mysql-error
             (unless (or pending-error result)
               (setq pending-error err)))))))
    (when timeout-error
      (clutch-db-mysql--handle-query-timeout conn timeout-error stmt))
    (if pending-error
        (signal 'clutch-db-error (clutch-db--error-data pending-error))
      result)))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier ((_conn mysql-conn) name)
  "Escape NAME as a MySQL identifier (backtick-quoted)."
  (mysql-escape-identifier name))

(cl-defmethod clutch-db-escape-literal ((_conn mysql-conn) value)
  "Escape VALUE as a MySQL string literal."
  (mysql-escape-literal value))

;;;; Schema methods

(cl-defmethod clutch-db-refresh-schema-async
    ((conn mysql-conn) callback &optional errback idle-delay)
  "Refresh MySQL CONN's tables asynchronously after optional IDLE-DELAY.
Deliver the names to CALLBACK and failures to ERRBACK."
  (when (fboundp 'mysql-query-async)
    (if idle-delay
        (run-with-idle-timer idle-delay nil
                            (lambda () (clutch-db-refresh-schema-async conn callback errback)))
      (clutch-db--metadata-request-series
       (lambda (sql outcome) (clutch-db-mysql--send-metadata conn sql outcome t))
       '("SHOW TABLES")
       (lambda (results) (mapcar #'car (car results)))
       (lambda (rows error)
         ;; SHOW TABLES without a current database means an empty schema.
         (if (and error (string-prefix-p "[1046]" (clutch-db--error-message error)))
             (funcall callback nil)
           (funcall (clutch-db--metadata-outcome callback errback) rows error)))
       (lambda () (clutch-db-live-p conn))))
    t))

(cl-defmethod clutch-db-list-tables ((conn mysql-conn))
  "Return table names for the current MySQL database on CONN.
A session with no current database has none, where SHOW TABLES fails
with error 1046.  The server decides that, not mysql.el, which may have
missed a USE or a DROP that changed the database."
  (clutch-db--translate-library-error mysql-error
    (condition-case err
        (mapcar #'car (mysql-result-rows (clutch-db-mysql--metadata-query conn "SHOW TABLES")))
      (mysql-query-error
       ;; mysql.el reports a server error as "[CODE] (STATE) MESSAGE".
       (unless (string-prefix-p "[1046]" (cadr err))
         (signal (car err) (cdr err)))))))

(cl-defmethod clutch-db-list-columns-async
    ((conn mysql-conn) table callback &optional errback schema _catalog)
  "Read TABLE's column names in SCHEMA on MySQL CONN asynchronously."
  (when (fboundp 'mysql-query-async)
    (clutch-db--metadata-request-series
     (lambda (sql outcome) (clutch-db-mysql--send-metadata conn sql outcome t))
     (list (clutch-db-mysql--column-names-sql table schema))
     (lambda (results) (mapcar #'car (car results)))
     (clutch-db--metadata-outcome callback errback)
     (lambda () (clutch-db-live-p conn)))
    t))

(cl-defmethod clutch-db-schema-tables-async
    ((conn mysql-conn) schema callback &optional errback)
  "Fetch MySQL table and view names in SCHEMA on CONN asynchronously."
  (when (fboundp 'mysql-query-async)
    (clutch-db--metadata-request-series
     (lambda (sql outcome) (clutch-db-mysql--send-metadata conn sql outcome t))
     (list (format "SHOW TABLES FROM %s" (mysql-escape-identifier schema)))
     (lambda (results) (mapcar #'car (car results)))
     (clutch-db--metadata-outcome callback errback)
     (lambda () (clutch-db-live-p conn)))
    t))

(cl-defmethod clutch-db-list-schemas ((conn mysql-conn))
  "Return visible MySQL schema/database names for CONN."
  (clutch-db--translate-library-error mysql-error
    (let ((result (clutch-db-mysql--metadata-query conn "SHOW DATABASES")))
      (sort (mapcar #'car (mysql-result-rows result)) #'string-collate-lessp))))

(cl-defmethod clutch-db-current-schema ((conn mysql-conn))
  "Return the current MySQL schema/database for CONN."
  (clutch-db-database conn))

(cl-defmethod clutch-db-set-current-schema ((conn mysql-conn) schema)
  "Switch MySQL CONN to SCHEMA."
  (clutch-db--translate-library-error mysql-error
    (condition-case err
        (mysql-select-database conn schema)
      (mysql-timeout
       ;; A late OK confirms USE; read back the name for mysql.el's cache.
       (let* ((inhibit-quit t)
              (database (and (clutch-db-mysql--drain-interrupted-response conn t)
                             (condition-case nil (mysql-refresh-current-database conn)
                               (mysql-error nil)))))
         (or database
             (progn
               (ignore-errors (mysql-disconnect conn))
               (signal 'clutch-db-error
                       (list (clutch-db-mysql--timeout-error-message err nil))))))))))

(cl-defmethod clutch-db-namespace-switch-p ((_conn mysql-conn) sql)
  "Return non-nil when the current database may differ after SQL."
  (clutch-db-mysql--namespace-statement-p sql))

(cl-defmethod clutch-db-update-namespace-params ((conn mysql-conn) params)
  "Store MySQL CONN's current database in a copy of connection PARAMS."
  (plist-put (copy-sequence params) :database (clutch-db-database conn)))

(cl-defmethod clutch-db-list-table-entries ((conn mysql-conn) &optional all-schemas)
  "Return table and view entries on CONN, across databases if ALL-SCHEMAS."
  (clutch-db--translate-library-error mysql-error
    (let ((result (clutch-db-mysql--metadata-query
                   conn
                   (concat "SELECT TABLE_NAME, TABLE_TYPE, NULLIF(TABLE_COMMENT, ''), TABLE_SCHEMA
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND "
                           (if all-schemas
                               "TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"
                             "TABLE_SCHEMA = DATABASE()")
                           " ORDER BY TABLE_NAME, TABLE_SCHEMA"))))
      (mapcar
       (lambda (row)
         (pcase-let ((`(,name ,table-type ,comment ,schema) row))
           (list :name name
                 :type (if (string= table-type "VIEW") "VIEW" "TABLE")
                 :schema schema
                 :source-schema schema
                 :comment comment)))
       (mysql-result-rows result)))))

(cl-defmethod clutch-db-resolve-column-names ((conn mysql-conn) names source-token)
  "Read original field names for NAMES on SOURCE-TOKEN in CONN's session."
  (clutch-db--translate-library-error mysql-error
    (let* ((sql (concat (clutch-db--column-binding-select conn names source-token) " LIMIT 0"))
           (fields (mysql-result-columns (clutch-db-mysql--metadata-query conn sql))))
      (cl-loop for name in names
               if (eq name 'star)
               collect (cons name (mapcar (lambda (field) (plist-get field :name)) fields))
               else append (let ((physical (plist-get (pop fields) :org-name)))
                             (unless (string-empty-p physical) (list (cons name physical))))))))

(cl-defmethod clutch-db-list-columns ((conn mysql-conn) table)
  "Return column names for TABLE on MySQL CONN."
  (clutch-db--translate-library-error mysql-error
    (mapcar #'car (mysql-result-rows
                   (clutch-db-mysql--metadata-query
                    conn (clutch-db-mysql--column-names-sql table nil))))))

(cl-defmethod clutch-db-list-objects ((conn mysql-conn) category)
  "Return object entry plists for CATEGORY on MySQL CONN."
  (clutch-db--translate-library-error mysql-error
    (when-let* ((sql (clutch-db-mysql--object-sql category)))
      (clutch-db-mysql--object-rows
       (clutch-db-database conn) category
       (mysql-result-rows (clutch-db-mysql--metadata-query conn sql))))))

(cl-defmethod clutch-db-list-objects-async
    ((conn mysql-conn) category callback &optional errback)
  "Fetch object CATEGORY on MySQL CONN without blocking for network replies."
  (when (fboundp 'mysql-query-async)
    (clutch-db--translate-library-error mysql-error
      (if-let* ((sql (clutch-db-mysql--object-sql category)))
          (let ((schema (clutch-db-database conn)))
            (clutch-db-mysql--send-metadata
             conn sql
             (lambda (rows error)
               (if error
                   (when errback (funcall errback (clutch-db--error-message error)))
                 (funcall callback (clutch-db-mysql--object-rows schema category rows))))
             t))
        (funcall callback nil)
        t))))

(cl-defmethod clutch-db-object-details ((conn mysql-conn) entry)
  "Return detail plists for MySQL object ENTRY on CONN."
  (clutch-db--translate-library-error mysql-error
    (let ((type (upcase (or (plist-get entry :type) "")))
          (schema (clutch-db-mysql--entry-schema entry)))
        (pcase type
          ("INDEX"
           (let* ((name (plist-get entry :name))
                  (table (or (plist-get entry :target-table)
                             (plist-get entry :table)))
                  (_ (unless table
                       (error "MySQL index details require :target-table")))
                  (result (clutch-db-mysql--metadata-query
                           conn
                           (format "SELECT COLUMN_NAME, SEQ_IN_INDEX, COLLATION
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = %s
  AND TABLE_NAME = %s
  AND INDEX_NAME = %s
ORDER BY SEQ_IN_INDEX"
                                   (clutch-db-mysql--schema-literal schema)
                                   (mysql-escape-literal table)
                                   (mysql-escape-literal name)))))
             (mapcar
              (lambda (row)
                (pcase-let ((`(,column-name ,position ,collation) row))
                  (list :name column-name
                        :position position
                        :descend (if (string= collation "D") "DESC" "ASC"))))
              (mysql-result-rows result))))
          ((or "PROCEDURE" "FUNCTION")
           (let* ((specific-name (plist-get entry :name))
                  (result (clutch-db-mysql--metadata-query
                           conn
                           (format "SELECT PARAMETER_NAME, DTD_IDENTIFIER,
       COALESCE(PARAMETER_MODE, 'RETURN'), ORDINAL_POSITION
FROM INFORMATION_SCHEMA.PARAMETERS
WHERE SPECIFIC_SCHEMA = %s
  AND SPECIFIC_NAME = %s
ORDER BY ORDINAL_POSITION"
                                   (clutch-db-mysql--schema-literal schema)
                                   (mysql-escape-literal specific-name)))))
             (mapcar
              (lambda (row)
                (pcase-let ((`(,param-name ,dtype ,mode ,position) row))
                  (list :name param-name :type dtype :mode mode :position position)))
              (mysql-result-rows result))))
          (_ nil)))))

(cl-defmethod clutch-db-object-definition ((conn mysql-conn) entry)
  "Return definition or source text for MySQL object ENTRY on CONN."
  (clutch-db--translate-library-error mysql-error
    (let* ((type (upcase (or (plist-get entry :type) "")))
           (name (plist-get entry :name))
           (schema (clutch-db-mysql--entry-schema entry))
           (qualified (clutch-db-mysql--qualified-identifier name schema)))
      (pcase type
        ("TABLE"
         (let* ((result (clutch-db-mysql--metadata-query
                         conn
                         (format "SHOW CREATE TABLE %s" qualified)))
                (rows (mysql-result-rows result)))
           (unless rows
             (signal 'clutch-db-error
                     (list (format "SHOW CREATE TABLE returned no rows for %s"
                                   name))))
           (pcase-let ((`(,_ ,ddl) (car rows)))
             ddl)))
        ((or "PROCEDURE" "FUNCTION" "TRIGGER")
         (nth 2 (car (mysql-result-rows
                      (clutch-db-mysql--metadata-query
                       conn
                       (format "SHOW CREATE %s %s" type qualified))))))
        ("VIEW"
         (let* ((result (clutch-db-mysql--metadata-query
                         conn
                         (format "SHOW CREATE VIEW %s" qualified)))
                (row (car (mysql-result-rows result))))
           (nth 1 row)))
        ("INDEX"
         (let* ((details (clutch-db-object-details conn entry))
                (columns (mapconcat
                          (lambda (col)
                            (format "%s %s"
                                    (mysql-escape-identifier (plist-get col :name))
                                    (plist-get col :descend)))
                          details
                          ", ")))
           (format "CREATE %sINDEX %s ON %s (%s);"
                   (if (plist-get entry :unique) "UNIQUE " "")
                   (mysql-escape-identifier name)
                   (clutch-db-mysql--qualified-identifier
                    (plist-get entry :target-table) schema)
                   columns)))
        (_ nil)))))

(cl-defmethod clutch-db-table-comment ((conn mysql-conn) table &optional schema)
  "Return the comment for TABLE in optional SCHEMA on MySQL CONN, or nil."
  (clutch-db--translate-library-error mysql-error
    (let ((comment (caar (mysql-result-rows
                          (clutch-db-mysql--metadata-query
                           conn (clutch-db-mysql--table-comment-sql table schema))))))
      (unless (string-empty-p (or comment "")) comment))))

(cl-defmethod clutch-db-table-comment-async
    ((conn mysql-conn) table callback &optional errback)
  "Read TABLE's comment on MySQL CONN without blocking."
  (when (fboundp 'mysql-query-async)
    (clutch-db--metadata-request-series
     (lambda (sql outcome) (clutch-db-mysql--send-metadata conn sql outcome t))
     (list (clutch-db-mysql--table-comment-sql table))
     (lambda (results)
       (let ((comment (caar (car results))))
         (unless (string-empty-p (or comment "")) comment)))
     (clutch-db--metadata-outcome callback errback)
     (lambda () (clutch-db-live-p conn)))
    t))

(cl-defmethod clutch-db-primary-key-columns ((conn mysql-conn) table
                                             &optional schema _catalog)
  "Return primary key column names for TABLE in SCHEMA on MySQL CONN."
  (clutch-db--translate-library-error mysql-error
    (let* ((result (clutch-db-mysql--metadata-query
                    conn
                    (clutch-db-mysql--primary-key-sql table schema)))
           (rows (mysql-result-rows result)))
      (mapcar (lambda (row)
                (pcase-let ((`(,_ ,_ ,_ ,_ ,name) row))
                  (if (stringp name) name (format "%s" name))))
              rows))))

(cl-defmethod clutch-db-row-identity-candidates ((conn mysql-conn) table
                                                 &optional schema _catalog)
  "Return row identity candidates for TABLE in optional SCHEMA on MySQL CONN."
  (or (cl-call-next-method)
      (clutch-db-mysql--unique-not-null-identities conn table schema)))

(cl-defmethod clutch-db-plan-sql ((conn mysql-conn) sql analyze)
  "Return SQL asking MySQL CONN for the plan of SQL.
Before MySQL 8.0.16, use traditional EXPLAIN.  ANALYZE needs 8.0.18.
MariaDB plans are not supported here."
  (let* ((version (or (mysql-conn-server-version conn) ""))
         ;; Servers append suffixes such as "-log", which `version<' rejects.
         (number (and (string-match "\\`[0-9]+\\(?:\\.[0-9]+\\)*" version)
                      (match-string 0 version))))
    (unless (string-match-p "MariaDB" version)
      (when (and analyze number (version< number "8.0.18"))
        (user-error "Measuring a MySQL plan requires MySQL 8.0.18 or later"))
      (if (and (not analyze) number (version< number "8.0.16"))
          (concat "EXPLAIN " sql)
        (clutch-db-mysql-plan-sql sql analyze)))))

(cl-defmethod clutch-db-plan ((_conn mysql-conn) result)
  "Return the tree or traditional plan MySQL reported in RESULT.
Traditional rows are independent steps: they do not specify a plan tree
or costs, so neither hierarchy nor costs are inferred."
  (if (seq-find (lambda (column)
                  (equal (plist-get column :name) "select_type"))
                (clutch-db-result-columns result))
      (let ((columns (mapcar (lambda (column) (plist-get column :name))
                             (clutch-db-result-columns result))))
        (list :unit nil
              :roots
              (mapcar
               (lambda (row)
                 (let ((fields (cl-mapcar #'cons columns row)))
                   (list :label
                         (concat (format "%s: %s; access %s"
                                         (cdr (assoc "select_type" fields))
                                         (or (cdr (assoc "table" fields)) "(no table)")
                                         (or (cdr (assoc "type" fields)) "none"))
                                 (when-let* ((key (cdr (assoc "key" fields)))
                                             ((stringp key)))
                                   (concat "; index " key))
                                 (when-let* ((extra (cdr (assoc "Extra" fields)))
                                             ((stringp extra))
                                             ((not (string-empty-p extra))))
                                   (concat "; " extra)))
                         :rows (cdr (assoc "rows" fields)))))
               (clutch-db-result-rows result))))
    (clutch-db-mysql-plan (caar (clutch-db-result-rows result)))))

(cl-defmethod clutch-db-foreign-keys ((conn mysql-conn) table
                                      &optional schema _catalog)
  "Return outgoing key columns for TABLE in SCHEMA on MySQL CONN."
  (clutch-db--translate-library-error mysql-error
    (clutch-db-mysql--foreign-key-values
     (mysql-result-rows (clutch-db-mysql--metadata-query
                         conn (clutch-db-mysql--foreign-key-sql table schema))))))

(cl-defmethod clutch-db-referencing-keys ((conn mysql-conn) table
                                          &optional schema _catalog)
  "Return the foreign keys that reference TABLE in SCHEMA on MySQL CONN."
  (clutch-db--translate-library-error mysql-error
    (let* ((columns '("TABLE_SCHEMA" "TABLE_NAME" "COLUMN_NAME"
                      "REFERENCED_COLUMN_NAME" "CONSTRAINT_NAME"))
           (where (format "REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s"
                          (clutch-db-mysql--schema-literal schema)
                          (mysql-escape-literal table)))
           (tables (if (string-match-p "\\`5\\.[67]\\."
                                       (or (mysql-conn-server-version conn) ""))
                       (clutch-db-mysql--legacy-referencing-tables conn table schema)
                     'unsupported))
           (rows (if (eq tables 'unsupported)
                     (clutch-db-mysql--key-column-rows conn columns where)
                   (cl-loop for (owner child) in tables
                            append (clutch-db-mysql--key-column-rows
                                    conn columns
                                    (format "TABLE_SCHEMA = %s AND TABLE_NAME = %s AND %s"
                                            (mysql-escape-literal owner)
                                            (mysql-escape-literal child) where))))))
      (clutch-db-mysql--referencing-key-values rows))))

;;;; Column details

(cl-defmethod clutch-db-column-details ((conn mysql-conn) table
                                        &optional schema _catalog)
  "Return detailed column info for TABLE in SCHEMA on MySQL CONN."
  (clutch-db--translate-library-error mysql-error
    (let* ((col-result (clutch-db-mysql--metadata-query
                        conn
                        (clutch-db-mysql--column-details-sql table schema)))
           (col-rows (mysql-result-rows col-result))
           (pk-cols (clutch-db-primary-key-columns conn table schema))
           (fks (clutch-db-foreign-keys conn table schema)))
      (mapcar
       (lambda (row) (clutch-db-mysql--column-details-row row pk-cols fks))
       col-rows))))

(cl-defmethod clutch-db-foreign-keys-async
    ((conn mysql-conn) table callback &optional errback schema _catalog)
  "Read TABLE's outgoing keys in SCHEMA on MySQL CONN asynchronously."
  (when (fboundp 'mysql-query-async)
    (clutch-db--metadata-request-series
     (lambda (sql outcome) (clutch-db-mysql--send-metadata conn sql outcome t))
     (list (clutch-db-mysql--foreign-key-sql table schema))
     (lambda (results) (clutch-db-mysql--foreign-key-values (car results)))
     (clutch-db--metadata-outcome callback errback)
     (lambda () (clutch-db-live-p conn)))
    t))

(cl-defmethod clutch-db-column-details-async
    ((conn mysql-conn) table callback &optional errback schema _catalog)
  "Read TABLE's column details in SCHEMA on CONN asynchronously."
  (when (fboundp 'mysql-query-async)
    (clutch-db-mysql--column-metadata-async
     conn table schema
     (lambda (metadata error)
       (funcall (clutch-db--metadata-outcome callback errback)
                (plist-get metadata :columns) error))
     (lambda () (clutch-db-live-p conn)) t)
    t))

(cl-defmethod clutch-db-referencing-keys-async
  ((conn mysql-conn) table callback &optional errback schema _catalog continue)
  "Read MySQL TABLE's incoming keys on CONN without blocking."
  (when (fboundp 'mysql-query-async)
    (clutch-db-mysql--referencing-keys-async
     conn table schema (clutch-db--metadata-outcome callback errback) continue)
    t))

(cl-defmethod clutch-db-table-relations-async
    ((conn mysql-conn) table callback &optional errback schema _catalog continue)
  "Read TABLE's columns and relations in SCHEMA on CONN asynchronously."
  (when (fboundp 'mysql-query-async)
    (let ((outcome (clutch-db--metadata-outcome callback errback)))
      (clutch-db-mysql--column-metadata-async
       conn table schema
       (lambda (metadata error)
         (if error (funcall outcome nil error)
           (clutch-db-referencing-keys-async
            conn table
            (lambda (incoming)
              (funcall outcome (plist-put metadata :referencing-keys incoming) nil))
            (lambda (message) (funcall outcome nil (list 'clutch-db-error message)))
            schema nil continue)))
       continue))
    t))

;;;; Re-entrancy guard

(cl-defmethod clutch-db-request-in-flight-p ((conn mysql-conn))
  "Return non-nil while MySQL CONN has a request in flight."
  (mysql-busy-p conn))

(cl-defmethod clutch-db-busy-p ((conn mysql-conn))
  "Return non-nil if MySQL CONN is executing a query."
  (mysql-busy-p conn))

;;;; Metadata methods

(cl-defmethod clutch-db-user ((conn mysql-conn))
  "Return the user for MySQL CONN."
  (mysql-connection-user conn))

(cl-defmethod clutch-db-host ((conn mysql-conn))
  "Return the host for MySQL CONN."
  (mysql-connection-host conn))

(cl-defmethod clutch-db-port ((conn mysql-conn))
  "Return the port for MySQL CONN."
  (mysql-connection-port conn))

(cl-defmethod clutch-db-database ((conn mysql-conn))
  "Return the database for MySQL CONN."
  (mysql-current-database conn))

 (setq clutch-db-mysql--methods-installed t))

(provide 'clutch-db-mysql)
;;; clutch-db-mysql.el ends here
