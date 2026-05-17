;;; clutch.el --- Interactive database client -*- lexical-binding: t; -*-
;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1") (mysql "0.2.0") (pg "0.40") (transient "0.3.7"))
;; Keywords: comm, data, tools
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

;; Interactive database client with native and JDBC backends.
;;
;; Provides:
;; - `clutch-mode': SQL editing major mode (derived from `sql-mode')
;; - `clutch-repl': REPL via `comint-mode'
;; - Query execution with horizontally scrollable result tables
;; - Object discovery and completion
;;
;; Entry points:
;;   M-x clutch-mode      — open a SQL editing buffer
;;   M-x clutch-repl      — open a REPL
;;   Open a .mysql file   — activates clutch-mode automatically

;;; Code:

(require 'clutch-compat)
(require 'clutch-db)
(require 'clutch-connection)
(require 'clutch-query)
(require 'sql)
(require 'comint)
(require 'cl-lib)

(defvar embark-general-map)
(defvar embark-target-finders)
(defvar embark-keymap-alist)
(require 'transient)
(require 'clutch-schema)
(require 'clutch-sql)
(require 'clutch-ui)
(require 'clutch-object)
(require 'clutch-edit)
(require 'clutch-result)
(require 'xref)

(defvar clutch--describe-object-entry)

(declare-function clutch--cached-column-details "clutch-schema" (conn table))
(declare-function clutch--cached-table-comment "clutch-schema" (conn table))
(declare-function clutch--table-comment-cached-p "clutch-schema" (conn table))
(declare-function clutch--ensure-column-details-async "clutch-schema" (conn table))
(declare-function clutch--ensure-columns-async "clutch-schema" (conn schema table))
(declare-function clutch--ensure-point-visible-horizontally "clutch-ui" ())
(declare-function clutch--ensure-table-comment-async "clutch-schema" (conn table))
(declare-function clutch--refresh-footer-cursor "clutch-ui" ())
(declare-function clutch--refresh-footer-line "clutch-ui" ())
(declare-function clutch--refresh-header-line "clutch-ui" ())
(declare-function clutch--replace-row-at-index "clutch-ui" (ridx))
(declare-function clutch--column-border-position "clutch-ui" (cidx))
(declare-function clutch--column-info-message-string "clutch-ui" (info))
(declare-function clutch--column-info-string "clutch-ui" (cidx))
(declare-function clutch--message-count "clutch-ui" (value))
(declare-function clutch--message-ident "clutch-ui" (value))
(declare-function clutch--message-keyword "clutch-ui" (value))
(declare-function clutch--message-literal "clutch-ui" (value))
(declare-function clutch--resolve-result-column-details "clutch-ui" (conn sql col-names))
(declare-function clutch--status-separator "clutch-ui" ())

;;;; Customization

(defgroup clutch nil
  "Interactive database lens."
  :group 'comm
  :prefix "clutch-")

(defface clutch-field-name-face
  '((((class color) (background light))
     :weight bold :foreground "#2563eb")
    (((class color) (background dark))
     :weight bold :foreground "#b8d7ec")
    (t :weight bold))
  "Face for database field and column names."
  :group 'clutch)

(defface clutch-header-face
  '((t :weight bold))
  "Face for header text that is not a database field name."
  :group 'clutch)

(defface clutch-insert-field-tag-face
  '((t :inherit shadow))
  "Face for metadata tags in the insert buffer."
  :group 'clutch)

(defface clutch-insert-field-error-face
  '((((class color) (background light))
     :underline (:color "#b91c1c" :style wave))
    (((class color) (background dark))
     :underline (:color "#fca5a5" :style wave))
    (t :inherit error))
  "Face for invalid values in the insert buffer."
  :group 'clutch)

(defface clutch-insert-inline-error-face
  '((t :inherit error))
  "Face for inline `insert-buffer' validation messages."
  :group 'clutch)

(defface clutch-insert-active-field-face
  '((t :inherit hl-line))
  "Face for the active line in the insert buffer."
  :group 'clutch)

(defface clutch-insert-active-field-name-face
  '((((class color) (background light))
     :weight bold :foreground "#2563eb")
    (((class color) (background dark))
     :weight bold :foreground "#b8d7ec")
    (t :weight bold))
  "Face for the active `insert-buffer' field prefix."
  :group 'clutch)

(defface clutch-header-active-face
  '((((class color) (background light))
     :background "#e5e7eb" :weight bold)
    (((class color) (background dark))
     :background "#263238" :weight bold)
    (t :weight bold))
  "Face for the column header under the cursor."
  :group 'clutch)

(defface clutch-border-face
  '((t :inherit shadow))
  "Face for table borders (pipes and separators)."
  :group 'clutch)

(defface clutch-object-source-face
  '((t :inherit shadow))
  "Face for object-source annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-object-public-source-face
  '((t :inherit shadow))
  "Face for PUBLIC object-source annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-object-type-face
  '((t :inherit shadow))
  "Face for object-type annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-null-face
  '((t :inherit shadow :slant italic))
  "Face for NULL values."
  :group 'clutch)

(defconst clutch--cell-generated-placeholder :clutch-generated-placeholder
  "Sentinel for values generated by the database at insert time.")

(defconst clutch--cell-default-placeholder :clutch-default-placeholder
  "Sentinel for values populated by database defaults at insert time.")

(defface clutch-modified-face
  '((((class color) (background light))
     :inherit warning :background "#fff3cd")
    (((class color) (background dark))
     :inherit warning :background "#3d2b00")
    (t :inherit warning))
  "Face for staged-edit cell values."
  :group 'clutch)

(defface clutch-fk-face
  '((t :inherit font-lock-type-face :underline t))
  "Face for foreign key column values.
Underlined to indicate clickable (RET to follow)."
  :group 'clutch)

(defface clutch-marked-face
  '((t :inherit dired-marked))
  "Face for marked rows in result buffer."
  :group 'clutch)

(defface clutch-executed-sql-marker-face
  '((t :inherit success))
  "Face for the executed SQL gutter marker."
  :group 'clutch)

(defface clutch-failed-sql-marker-face
  '((t :inherit error))
  "Face for the failed SQL gutter marker."
  :group 'clutch)

(define-fringe-bitmap 'clutch-executed-sql-dot
  [24 60 126 255 255 126 60 24]
  nil nil 'center)

(defface clutch-pending-delete-face
  '((((class color) (background light))
     :background "#fde8e8" :foreground "#9b1c1c" :strike-through t)
    (((class color) (background dark))
     :background "#3b1212" :foreground "#fca5a5" :strike-through t)
    (t :strike-through t))
  "Face for rows staged for deletion."
  :group 'clutch)

(defface clutch-pending-insert-face
  '((((class color) (background light))
     :background "#e6f4ea" :foreground "#1e4620")
    (((class color) (background dark))
     :background "#1a3320" :foreground "#86efac")
    (t :inherit success))
  "Face for rows staged for insertion."
  :group 'clutch)

(defface clutch-error-summary-face
  '((((class color) (background dark)) :foreground "#ffb4b4" :weight semibold)
    (((class color) (background light)) :foreground "#b42318" :weight semibold)
    (t :inherit error :weight bold))
  "Face for SQL execution error summaries."
  :group 'clutch)

(defcustom clutch-connection-alist nil
  "Alist of saved database connections.
Each entry has the form:
  (NAME . (:host H :port P :user U [:password P] :database D
           [:backend SYM] [:sql-product SYM] [:pass-entry STR]
           [:ssh-host SSH-HOST] [:tramp TRAMP-DIRECTORY]
           [:url STR] [:display-name STR] [:props ALIST]
           [:tls BOOLEAN] [:ssl-mode disabled] [:sslmode require]
           [:connect-timeout N] [:read-idle-timeout N]
           [:query-timeout N] [:rpc-timeout N]))
NAME is a string used for `completing-read'.
:backend is required and names the backend symbol (\\='mysql, \\='pg,
\\='sqlite, or a JDBC backend such as \\='oracle).
:sql-product overrides `clutch-sql-product' for this connection.
:tls is a convenience shortcut for backend TLS defaults.  For MySQL,
an explicit `:tls nil' forces plaintext and suppresses the automatic
MySQL 8 TLS retry path; for PostgreSQL, `:tls t' maps to `:sslmode require'
and `:tls nil' maps to `:sslmode disable'.
:ssl-mode is currently MySQL-only; `disabled' is a compatibility spelling for
the same explicit plaintext mode.  The older alias `off' is also accepted.
:sslmode is PostgreSQL-only and follows the upstream naming.  Supported values
are `disable', `prefer', `require', and `verify-full'.
:ssh-host enables a local SSH tunnel using the named host from ~/.ssh/config.
clutch starts `ssh -N -L ... SSH-HOST' automatically, so this currently
requires structured `:host' / `:port' params and does not apply to `:url'
based JDBC entries.
:tramp enables the same local forward from an ssh-like TRAMP directory such as
/ssh:host:/path/ or /rpc:host:/path/.  `:tramp-default-directory' is also
accepted as a longer spelling.

Password resolution order:
  1. :password — used as-is when present.
  2. Pass store by connection name — when `auth-source-pass' is loaded,
     clutch automatically looks up a pass entry whose name matches NAME
     (the car of this alist entry).  The password is on the first line.
     Use :pass-entry STR to override the entry name if it differs.
  3. `auth-source-search' — searches ~/.authinfo / ~/.authinfo.gpg / pass
     by :host, :user, and :port (standard auth-source matching)."
  :type '(alist :key-type string
                :value-type (plist :options
                                   ((:host string)
                                    (:port integer)
                                    (:user string)
                                    (:password string)
                                    (:database string)
                                    (:backend symbol)
                                    (:sql-product symbol)
                                    (:pass-entry string)
                                    (:ssh-host string)
                                    (:tramp string)
                                    (:tramp-default-directory string)
                                    (:url string)
                                    (:display-name string)
                                    (:props (alist :key-type string :value-type string))
                                    (:ssl-mode (choice (const :tag "Disabled" disabled)
                                                       (const :tag "Off (alias)" off)))
                                    (:sslmode (choice (const :tag "Disable" disable)
                                                      (const :tag "Prefer" prefer)
                                                      (const :tag "Require" require)
                                                      (const :tag "Verify Full" verify-full)))
                                    (:connect-timeout natnum)
                                    (:read-idle-timeout natnum)
                                    (:query-timeout natnum)
                                    (:rpc-timeout natnum)
                                    (:tls boolean))))
  :group 'clutch)

(defcustom clutch-insert-validation-idle-delay 0.2
  "Idle seconds before validating heavier insert fields such as JSON."
  :type 'number
  :group 'clutch)

(defcustom clutch-console-directory
  (expand-file-name "clutch" user-emacs-directory)
  "Directory for persisting query console buffer content."
  :type 'directory
  :group 'clutch)

(defcustom clutch-result-window-height 0.33
  "Height of the result window as a fraction of the frame height.
A float between 0.0 and 1.0.  Only applies when creating a new result
window; an existing result window is reused at its current height."
  :type 'float
  :group 'clutch)

(defcustom clutch-result-max-rows 500
  "Maximum number of rows to display in result tables."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-agent-context-max-result-rows 20
  "Maximum number of current result rows copied for external agent context."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-agent-context-max-cell-width 200
  "Maximum width of a single copied cell in external agent context."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-column-width-max 30
  "Maximum display width for a single column in the result table."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-column-width-step 5
  "Step size for widening/narrowing columns with +/-."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-column-padding 1
  "Number of padding spaces on each side of a cell."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-sql-product 'mysql
  "SQL product used for syntax highlighting.
Must be a symbol recognized by `sql-mode' (e.g. mysql, postgres)."
  :type '(choice (const :tag "MySQL" mysql)
                 (const :tag "PostgreSQL" postgres)
                 (const :tag "MariaDB" mariadb)
                 (symbol :tag "Other"))
  :group 'clutch)

(defcustom clutch-connect-timeout-seconds 10
  "Timeout in seconds for establishing a database connection.
Applies to networked backends.  SQLite ignores this setting."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-tramp-context-policy 'ask
  "How connection commands use the current TRAMP buffer context.
When nil, Clutch never infers TRAMP transport from the current buffer.
When `ask', Clutch prompts before using the current TRAMP default directory.
When `auto', Clutch uses the current TRAMP default directory without asking.
This only applies when a connection has no explicit transport such as
:ssh-host or :tramp.  TRAMP transport currently supports ssh-like TRAMP
directories.  `:tramp-default-directory' remains accepted as a longer spelling
for `:tramp'."
  :type '(choice (const :tag "Never infer TRAMP context" nil)
                 (const :tag "Ask before using current TRAMP context" ask)
                 (const :tag "Automatically use current TRAMP context" auto))
  :group 'clutch)

(defcustom clutch-read-idle-timeout-seconds 30
  "Idle timeout in seconds while waiting for query I/O.
Applies to MySQL, PostgreSQL, and JDBC network I/O.  SQLite ignores this
setting."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-query-timeout-seconds 30
  "Timeout in seconds for database-side query execution.
Currently applied by the JDBC backend.  Native MySQL/PostgreSQL backends do
not yet enforce a server-side statement timeout."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-jdbc-rpc-timeout-seconds 30
  "Timeout in seconds for round-trips to the JDBC agent process."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-object-warmup-idle-delay-seconds 0.5
  "Idle delay before warming non-table object metadata.
A small non-zero delay keeps connect and initial UI painting responsive before
background object discovery starts."
  :type 'number
  :group 'clutch)

(defcustom clutch-primary-object-types '("TABLE" "VIEW" "SYNONYM")
  "Object types preferred by clutch's primary object entrypoint.
When nil, the primary entrypoint includes all schema object types."
  :type '(repeat string)
  :group 'clutch)

(defcustom clutch-sql-completion-case-style 'preserve
  "How SQL completion inserts keywords and identifiers.
`preserve' keeps backend-provided identifier case and uppercase SQL keywords.
`lower' inserts lowercase keywords and lowercases completion identifiers.
`upper' inserts uppercase keywords and uppercases completion identifiers."
  :type '(choice (const :tag "Preserve backend / default keyword case" preserve)
                 (const :tag "Lowercase keywords and identifiers" lower)
                 (const :tag "Uppercase keywords and identifiers" upper))
  :group 'clutch)

(defcustom clutch-schema-cache-install-batch-size 500
  "Maximum number of schema entries to install per idle slice.
Large schema snapshots are installed incrementally to keep Emacs responsive
after async metadata refreshes."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-csv-export-default-coding-system 'utf-8-with-signature
  "Default coding system when exporting CSV files."
  :type '(choice (const :tag "UTF-8 (with BOM)" utf-8-with-signature)
                 (const :tag "UTF-8" utf-8)
                 (const :tag "GBK" gbk)
                 (coding-system :tag "Other coding system"))
  :group 'clutch)

(defcustom clutch-debug-event-limit 25
  "Maximum number of recent debug events kept per buffer or connection.
Only recorded while `clutch-debug-mode' is enabled."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-console-yank-cleanup t
  "When non-nil, clean whitespace in pasted text in query consoles.
After `yank', `yank-pop', or `clipboard-yank' in a query console buffer,
trailing whitespace, mixed indentation, and CRLF line endings are
cleaned up in the pasted region only."
  :type 'boolean
  :group 'clutch)

(defconst clutch-debug-buffer-name "*clutch-debug*"
  "Name of the dedicated clutch debug buffer.")

;;;; Buffer-local variables

(defvar-local clutch-connection nil
  "Current database connection for this buffer.")

(defvar clutch-debug-mode nil
  "Non-nil when Clutch debug capture is enabled.")

(defvar-local clutch--buffer-error-details nil
  "Current problem record scoped to this buffer.")

(defvar clutch--problem-records-by-conn (make-hash-table :test 'eq :weakness 'key)
  "Current problem records keyed by live connection object.")

(defvar-local clutch--executing-p nil
  "Non-nil while a query is executing in this buffer.
Used to update the mode-line with a spinner during execution.")

(defvar clutch--source-window nil
  "Window that initiated the current query execution.
Dynamically bound by `clutch--execute' so result buffers open
adjacent to the correct console window.")

(defvar clutch--executing-sql-start nil
  "Buffer position where the currently executing SQL begins, or nil.
Dynamically bound by `clutch--execute-and-mark'.")

(defvar clutch--executing-sql-end nil
  "Buffer position where the currently executing SQL ends, or nil.
Dynamically bound by `clutch--execute-and-mark'.")

(defvar-local clutch--conn-sql-product nil
  "SQL product for the current connection, or nil to use the default.")

(defvar clutch--oracle-i18n-warning-shown nil
  "Non-nil after showing the Oracle orai18n completion warning once.")

(defvar clutch--completion-metadata-warning-cache (make-hash-table :test 'equal)
  "Completion metadata errors already surfaced in this session.")

(defvar-local clutch--last-query nil
  "Last executed SQL query string.")

(defvar-local clutch--last-result-buffer nil
  "Latest result buffer produced from this query source buffer.")

(defvar-local clutch--debug-events nil
  "Recent redacted debug events captured for this buffer.")

(defvar clutch--debug-events-by-conn (make-hash-table :test 'eq :weakness 'key)
  "Recent redacted debug events keyed by live connection object.")

(defvar clutch--debug-buffer-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "q") #'quit-window)
    map)
  "Keymap for `clutch--debug-buffer-mode'.")

(define-derived-mode clutch--debug-buffer-mode special-mode "clutch-debug"
  "Mode for inspecting the dedicated clutch debug buffer.")

(defun clutch--debug-buffer ()
  "Return the dedicated clutch debug buffer, creating it if needed."
  (let ((buf (get-buffer-create clutch-debug-buffer-name)))
    (with-current-buffer buf
      (unless (derived-mode-p 'clutch--debug-buffer-mode)
        (clutch--debug-buffer-mode))
      (setq-local header-line-format " Clutch debug capture"))
    buf))

(defun clutch--reset-debug-buffer ()
  "Reset the dedicated clutch debug buffer for a new capture window."
  (with-current-buffer (clutch--debug-buffer)
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (format "Clutch Debug\n============\nStarted: %s\n"
                      (format-time-string "%F %T"))))))

(defun clutch--debug-buffer-source-label (buffer)
  "Return a human-readable source label for BUFFER."
  (when (buffer-live-p buffer)
    (buffer-name buffer)))

(defun clutch--debug-buffer-connection-label (connection)
  "Return a human-readable connection label for CONNECTION."
  (when connection
    (condition-case nil
        (clutch--connection-key connection)
      (error nil))))

(defun clutch--debug-format-label (key)
  "Return a human-readable label for KEY."
  (capitalize
   (replace-regexp-in-string
    "-" " "
    (string-remove-prefix ":" (format "%s" key)))))

(defun clutch--debug-indent-block (text spaces)
  "Indent TEXT by SPACES."
  (let ((prefix (make-string spaces ?\s)))
    (string-join
     (mapcar (lambda (line)
               (if (string-empty-p line)
                   line
                 (concat prefix line)))
             (split-string (string-trim-right text) "\n"))
     "\n")))

(defun clutch--debug-format-plist-data (plist)
  "Return a human-readable string for PLIST."
  (with-temp-buffer
    (cl-loop for (key val) on plist by #'cddr
             when val
             do (let ((rendered (clutch--debug-format-data val)))
                  (when rendered
                    (if (string-match-p "\n" rendered)
                        (insert (format "%s:\n%s\n"
                                        (clutch--debug-format-label key)
                                        (clutch--debug-indent-block rendered 2)))
                      (insert (format "%s: %s\n"
                                      (clutch--debug-format-label key)
                                      rendered))))))
    (string-trim-right (buffer-string))))

(defun clutch--debug-format-list-data (items)
  "Return a human-readable string for ITEMS."
  (string-join
   (mapcar
    (lambda (item)
      (let ((rendered (clutch--debug-format-data item)))
        (if (string-match-p "\n" rendered)
            (concat "-\n" (clutch--debug-indent-block rendered 2))
          (concat "- " rendered))))
    items)
   "\n"))

(defun clutch--debug-format-data (data)
  "Return a human-readable string for DATA."
  (cond
   ((null data) nil)
   ((stringp data) data)
   ((vectorp data) (clutch--debug-format-data (append data nil)))
   ((and (listp data) (keywordp (car-safe data)))
    (clutch--debug-format-plist-data data))
   ((listp data)
    (clutch--debug-format-list-data data))
   (t (format "%s" data))))

(defun clutch--debug-insert-field (label value)
  "Insert LABEL and VALUE into the current debug output buffer."
  (when-let* ((rendered (clutch--debug-format-data value)))
    (if (string-match-p "\n" rendered)
        (insert (format "%s:\n%s\n"
                        label
                        (clutch--debug-indent-block rendered 2)))
      (insert (format "%s: %s\n" label rendered)))))

(defun clutch--debug-insert-section (title value)
  "Insert a TITLE section containing VALUE into the current debug buffer."
  (when-let* ((rendered (clutch--debug-format-data value)))
    (insert "\n" title "\n")
    (insert (clutch--debug-indent-block rendered 2) "\n")))

(defun clutch--debug-context-without-inline-sql (context)
  "Return CONTEXT plist without inline SQL payload entries."
  (when context
    (cl-loop for (key val) on context by #'cddr
             unless (memq key '(:generated-sql :sql))
             append (list key val))))

(defun clutch--append-debug-buffer-entry (heading body)
  "Append HEADING and BODY to the dedicated debug buffer."
  (with-current-buffer (clutch--debug-buffer)
    (let ((inhibit-read-only t))
      (goto-char (point-max))
      (unless (bobp)
        (insert "\n\n"))
      (insert heading "\n")
      (insert (make-string (length heading) ?-) "\n")
      (when body
        (insert body))
      (unless (or (bobp) (eq (char-before) ?\n))
        (insert "\n")))))

(defun clutch--append-problem-record-to-debug-buffer (buffer connection problem)
  "Append PROBLEM for BUFFER and CONNECTION to the dedicated debug buffer."
  (when (and clutch-debug-mode problem)
    (let* ((backend (plist-get problem :backend))
           (diag (plist-get problem :diag))
           (debug-payload (plist-get problem :debug))
           (stderr-tail (plist-get problem :stderr-tail))
           (context (copy-tree (plist-get diag :context)))
           (sql (plist-get context :sql))
           (generated-sql (plist-get context :generated-sql))
           (display-context
            (clutch--debug-context-without-inline-sql context))
           (body
            (with-temp-buffer
              (clutch--debug-insert-field "Recorded"
                                          (format-time-string "%F %T"))
              (clutch--debug-insert-field "Backend"
                                          (and backend
                                               (upcase (symbol-name backend))))
              (clutch--debug-insert-field "Source"
                                          (clutch--debug-buffer-source-label buffer))
              (clutch--debug-insert-field "Connection"
                                          (clutch--debug-buffer-connection-label connection))
              (clutch--debug-insert-field "Summary"
                                          (plist-get problem :summary))
              (clutch--debug-insert-field "Category" (plist-get diag :category))
              (clutch--debug-insert-field "Operation" (plist-get diag :op))
              (clutch--debug-insert-field "Request ID" (plist-get diag :request-id))
              (clutch--debug-insert-field "Conn ID" (plist-get diag :conn-id))
              (clutch--debug-insert-field "Exception"
                                          (plist-get diag :exception-class))
              (clutch--debug-insert-field "SQLState" (plist-get diag :sql-state))
              (clutch--debug-insert-field "Vendor code" (plist-get diag :vendor-code))
              (clutch--debug-insert-field "Raw message"
                                          (plist-get diag :raw-message))
              (clutch--debug-insert-section "SQL" sql)
              (clutch--debug-insert-section "Generated SQL" generated-sql)
              (clutch--debug-insert-section "Context" display-context)
              (clutch--debug-insert-section "Cause chain"
                                            (plist-get diag :cause-chain))
              (clutch--debug-insert-section "Backend debug" debug-payload)
              (clutch--debug-insert-section "Agent stderr tail" stderr-tail)
              (string-trim-right (buffer-string)))))
      (clutch--append-debug-buffer-entry "Problem" body))))

(defun clutch--append-debug-event-to-buffer (buffer connection event)
  "Append EVENT for BUFFER and CONNECTION to the dedicated debug buffer."
  (when clutch-debug-mode
    (let* ((backend (plist-get event :backend))
           (body
            (with-temp-buffer
              (clutch--debug-insert-field "Recorded"
                                          (or (plist-get event :time)
                                              (format-time-string "%F %T")))
              (clutch--debug-insert-field "Operation" (plist-get event :op))
              (clutch--debug-insert-field "Phase" (plist-get event :phase))
              (clutch--debug-insert-field "Backend"
                                          (and backend
                                               (upcase (symbol-name backend))))
              (clutch--debug-insert-field "Source"
                                          (clutch--debug-buffer-source-label buffer))
              (clutch--debug-insert-field "Connection"
                                          (clutch--debug-buffer-connection-label connection))
              (when-let* ((elapsed (plist-get event :elapsed)))
                (clutch--debug-insert-field "Elapsed"
                                            (clutch--format-elapsed elapsed)))
              (clutch--debug-insert-field "Summary" (plist-get event :summary))
              (clutch--debug-insert-field "SQL preview"
                                          (plist-get event :sql-preview))
              (clutch--debug-insert-section "Context"
                                            (plist-get event :context))
              (string-trim-right (buffer-string)))))
      (clutch--append-debug-buffer-entry "Trace Event" body))))

(defun clutch--clear-debug-capture ()
  "Forget captured debug events and reset the dedicated debug buffer."
  (setq clutch--debug-events-by-conn (make-hash-table :test 'eq :weakness 'key))
  (dolist (buf (buffer-list))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (setq-local clutch--debug-events nil))))
  (clutch--reset-debug-buffer))

(defun clutch--attached-buffer-for-connection (connection)
  "Return one live buffer attached to CONNECTION, or nil."
  (when connection
    (or (and (eq clutch-connection connection)
             (current-buffer))
        (cl-loop for buf in (buffer-list)
                 when (and (buffer-live-p buf)
                           (eq (buffer-local-value 'clutch-connection buf)
                               connection))
                 return buf))))

(defun clutch--replay-problem-records-to-debug-buffer ()
  "Replay stored problem records into the dedicated debug buffer.
This preserves historical failure context when debug capture starts after a
problem was already recorded."
  (let (records seen)
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when clutch--buffer-error-details
            (let ((entry (list :buffer buf
                               :connection clutch-connection
                               :problem (copy-tree clutch--buffer-error-details))))
              (unless (member entry seen)
                (push entry seen)
                (push entry records)))))))
    (maphash
     (lambda (connection problem)
       (let ((entry (list :buffer (clutch--attached-buffer-for-connection connection)
                          :connection connection
                          :problem (copy-tree problem))))
         (unless (member entry seen)
           (push entry seen)
           (push entry records))))
     clutch--problem-records-by-conn)
    (when records
      (clutch--append-debug-buffer-entry
       "Historical Problems"
       "Recorded before debug mode was enabled.")
      (dolist (entry (nreverse records))
        (clutch--append-problem-record-to-debug-buffer
         (plist-get entry :buffer)
         (plist-get entry :connection)
         (plist-get entry :problem))))))

;;;###autoload
(define-minor-mode clutch-debug-mode
  "Capture additional redacted troubleshooting data for clutch workflows.
When enabled, clutch records a bounded recent-event trace per buffer and per
connection.  JDBC requests also ask the agent for an optional debug payload,
and captured output is appended to the dedicated `*clutch-debug*' buffer."
  :global t
  :group 'clutch
  :lighter " ClutchDbg"
  (when clutch-debug-mode
    (clutch--clear-debug-capture)
    (clutch--replay-problem-records-to-debug-buffer)))

(defun clutch--remember-problem-record (&rest args)
  "Store the current problem record described by ARGS.
Recognized keys are :buffer, :connection, and :problem.  Problem records are
stored buffer-locally and, when CONNECTION is non-nil, in the shared
connection-scoped registry."
  (let* ((buffer (or (plist-get args :buffer) (current-buffer)))
         (connection (plist-get args :connection))
         (problem (copy-tree (plist-get args :problem))))
    (when (and buffer (buffer-live-p buffer))
      (with-current-buffer buffer
        (setq-local clutch--buffer-error-details problem)))
    (when connection
      (if problem
          (puthash connection problem clutch--problem-records-by-conn)
        (remhash connection clutch--problem-records-by-conn)))
    (when problem
      (clutch--append-problem-record-to-debug-buffer buffer connection problem))
    problem))

(defun clutch--forget-problem-record (&optional buffer connection)
  "Forget the current problem record for BUFFER and CONNECTION."
  (when (and buffer (buffer-live-p buffer))
    (with-current-buffer buffer
      (setq-local clutch--buffer-error-details nil)))
  (when connection
    (remhash connection clutch--problem-records-by-conn)))

(defun clutch--forget-problem-records-for-connection (connection)
  "Forget problem records for CONNECTION across all attached buffers."
  (when connection
    (remhash connection clutch--problem-records-by-conn)
    (dolist (buf (buffer-list))
      (when (and (buffer-live-p buf)
                 (eq (buffer-local-value 'clutch-connection buf) connection))
        (with-current-buffer buf
          (setq-local clutch--buffer-error-details nil))))))

(defun clutch--clear-connection-problem-capture (connection)
  "Forget problem records and backend-local diagnostics for CONNECTION."
  (when connection
    (clutch--forget-problem-records-for-connection connection)
    (clutch-db-clear-error-details connection)))

(defun clutch--debug-sql-preview (sql)
  "Return a compact single-line preview of SQL."
  (when sql
    (truncate-string-to-width
     (replace-regexp-in-string "[\n\r\t ]+" " " (string-trim sql))
     160 0 nil "...")))

(defun clutch--debug-trim-events (events)
  "Return EVENTS truncated to `clutch-debug-event-limit'."
  (let ((limit (max 1 clutch-debug-event-limit)))
    (cl-subseq events 0 (min limit (length events)))))

(defun clutch--normalize-debug-event (event)
  "Return EVENT normalized for storage."
  (let ((normalized (copy-tree event)))
    (unless (plist-get normalized :time)
      (setq normalized
            (plist-put normalized :time
                       (format-time-string "%F %T"))))
    (when-let* ((sql (plist-get normalized :sql)))
      (setq normalized (plist-put normalized :sql-preview
                                  (clutch--debug-sql-preview sql)))
      (setq normalized (plist-put normalized :sql-length (length sql)))
      (cl-remf normalized :sql))
    normalized))

(defun clutch--remember-debug-event (&rest event)
  "Record EVENT for the current buffer and optional connection.
Recognized keys include :buffer, :connection, :op, :phase, :summary, :sql,
:backend, :context, and :elapsed.  Recording is disabled unless
`clutch-debug-mode' is non-nil."
  (when clutch-debug-mode
    (let* ((buffer (or (plist-get event :buffer) (current-buffer)))
           (conn (plist-get event :connection))
           (normalized (clutch--normalize-debug-event event)))
      (cl-remf normalized :buffer)
      (cl-remf normalized :connection)
      (when (buffer-live-p buffer)
        (with-current-buffer buffer
          (setq-local clutch--debug-events
                      (clutch--debug-trim-events
                       (cons normalized clutch--debug-events)))))
      (when conn
        (puthash conn
                 (clutch--debug-trim-events
                  (cons normalized (gethash conn clutch--debug-events-by-conn)))
                 clutch--debug-events-by-conn))
      (clutch--append-debug-event-to-buffer buffer conn normalized)
      normalized)))

(defun clutch--effective-sql-product (params)
  "Return the SQL product to use for connection PARAMS."
  (or (plist-get params :sql-product)
      (pcase (plist-get params :backend)
        ((or 'pg 'postgresql) 'postgres)
        ((or 'mysql 'mariadb) 'mysql)
        ('sqlite 'sqlite)
        ('oracle 'oracle)
        (_ nil))))

(defun clutch--oracle-i18n-missing-p (err)
  "Return non-nil when ERR indicates Oracle needs orai18n.jar."
  (string-match-p
   "orai18n\\.jar\\|Non supported character set"
   (error-message-string err)))

(defun clutch--warn-oracle-i18n-once ()
  "Warn once that Oracle completion needs orai18n.jar for this session."
  (unless clutch--oracle-i18n-warning-shown
    (setq clutch--oracle-i18n-warning-shown t)
    (message (concat "Oracle completion needs orai18n.jar for this character set. "
                     "Run M-x clutch-jdbc-install-driver RET oracle "
                     "(or oracle-i18n if you already manage ojdbc manually)."))))

(defun clutch--warn-completion-metadata-error-once (message-text)
  "Warn once that completion metadata is unavailable with MESSAGE-TEXT."
  (unless (gethash message-text clutch--completion-metadata-warning-cache)
    (puthash message-text t clutch--completion-metadata-warning-cache)
    (message "Completion metadata unavailable: %s" message-text)))

(defun clutch--remember-recoverable-metadata-warning (connection op err &optional context)
  "Record a recoverable metadata warning for CONNECTION and operation OP.
ERR is the original condition object.  Optional CONTEXT is attached to the
debug event when `clutch-debug-mode' is enabled."
  (when clutch-debug-mode
    (let* ((buffer (or (clutch--attached-buffer-for-connection connection)
                       (current-buffer)))
           (summary (clutch--humanize-db-error (error-message-string err)))
           (backend (and connection
                         (condition-case nil
                             (clutch--backend-key-from-conn connection)
                           (error nil)))))
      (clutch--remember-debug-event
       :buffer buffer
       :connection connection
       :op op
       :phase "warning"
       :backend backend
       :summary summary
       :context context))))

(defun clutch--safe-completion-call (thunk)
  "Call THUNK for completion and swallow recoverable metadata errors."
  (condition-case err
      (funcall thunk)
    (clutch-db-error
     (clutch--remember-recoverable-metadata-warning
      clutch-connection "completion" err)
     (if (clutch--oracle-i18n-missing-p err)
         (clutch--warn-oracle-i18n-once)
       (clutch--warn-completion-metadata-error-once
        (error-message-string err)))
     nil)))

(defvar clutch--aggregate-summary)
(defvar clutch--column-widths)
(defvar clutch--filter-pattern)
(defvar clutch--fk-info)
(defvar clutch--filtered-rows)
(defvar clutch--header-active-col)
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
(defvar clutch--refine-callback)
(defvar clutch--refine-excluded-cols)
(defvar clutch--refine-excluded-rows)
(defvar clutch--refine-overlays)
(defvar clutch--refine-rect)
(defvar clutch--refine-saved-mode-line)
(defvar clutch--result-column-defs)
(defvar clutch--result-columns)
(defvar clutch--result-rows)
(defvar clutch--row-identity)
(defvar clutch--sort-column)
(defvar clutch--sort-descending)
(defvar clutch--where-filter)
(defvar-local clutch--base-query nil
  "The original unfiltered SQL query, used by WHERE filtering.")

(defvar-local clutch--connection-params nil
  "Params plist used to establish the current connection.
Stored at connect time so the connection can be re-established
automatically when it drops.")

(defvar-local clutch--console-name nil
  "Display name if this buffer is a query console, nil otherwise.
Set by `clutch-query-console'; used for buffer display and persistence.")

(defvar-local clutch--console-storage-name nil
  "Stable storage identity for this query console, or nil.
When nil, console persistence falls back to `clutch--console-name'.")

(defvar-local clutch--console-ad-hoc-params nil
  "Connection params for a query console not backed by a saved profile.")

(defun clutch--console-buffer-base-name (name)
  "Return canonical buffer name for console NAME."
  (format "*clutch: %s*" name))

(defun clutch--console-buffer-name (&optional name conn)
  "Return display buffer name for console NAME and schema state on CONN."
  (let* ((console-name (or name clutch--console-name "?"))
         (base (clutch--console-buffer-base-name console-name))
         (entry (and conn (clutch--schema-status-entry conn)))
         (state (plist-get entry :state))
         (tables (plist-get entry :tables))
         (suffix (pcase state
                   ('refreshing " [schema...]")
                   ('stale " [schema~]")
                   ('failed " [schema!]")
                   ('ready (format " [schema %dt]" (or tables 0)))
                   (_ ""))))
    (concat base suffix)))

(defun clutch--console-buffer-storage-match-p (storage-name)
  "Return non-nil when the current console buffer matches STORAGE-NAME."
  (and storage-name
       (or (equal clutch--console-storage-name storage-name)
           (and (not clutch--console-storage-name)
                clutch--connection-params
                (equal (clutch--console-persistence-name
                        clutch--console-name
                        clutch--connection-params)
                       storage-name)))))

(defun clutch--find-console-buffer (name &optional storage-name)
  "Return the live console buffer for NAME or STORAGE-NAME, or nil."
  (or (and storage-name
           (cl-find-if
            (lambda (buf)
              (and (buffer-live-p buf)
                   (with-current-buffer buf
                     (and (derived-mode-p 'clutch-mode)
                          (clutch--console-buffer-storage-match-p
                           storage-name)))))
            (buffer-list)))
      (cl-find-if
       (lambda (buf)
         (and (buffer-live-p buf)
              (with-current-buffer buf
                (and (derived-mode-p 'clutch-mode)
                     (equal clutch--console-name name)
                     (or (not storage-name)
                         (and (not clutch--console-storage-name)
                              (not clutch--connection-params)))))))
       (buffer-list))))

(defun clutch--update-console-buffer-name ()
  "Rename the current console buffer to reflect schema status."
  (when clutch--console-name
    (rename-buffer (clutch--console-buffer-name clutch--console-name clutch-connection)
                   t)))

(defun clutch--refresh-current-schema (&optional quiet force-sync)
  "Refresh schema for the current connection and report the outcome.
When QUIET is non-nil, do not emit a minibuffer message.
When FORCE-SYNC is non-nil, bypass any background refresh path.
Returns non-nil on success, nil on failure."
  (clutch--ensure-connection)
  (let* ((conn clutch-connection)
         (entry (clutch--schema-status-entry conn)))
    (if (eq (plist-get entry :state) 'refreshing)
        (progn
          (unless quiet
            (message "Schema refresh already in progress"))
          nil)
      (if (or force-sync
              (clutch-db-eager-schema-refresh-p conn))
          (let* ((ok (clutch--refresh-schema-cache conn))
                 (entry (clutch--schema-status-entry conn))
                 (tables (plist-get entry :tables))
                 (err (plist-get entry :error)))
            (unless quiet
              (message (if ok
                           (format "Schema refreshed%s"
                                   (if tables (format " (%d tables)" tables) ""))
                         (format "Schema refresh failed%s"
                                 (if err (format ": %s" err) "")))))
            ok)
        (let ((started (clutch--refresh-schema-cache-async conn)))
          (if started
              (progn
                (unless quiet
                  (message "Schema refresh started in background"))
                t)
            (let* ((ok (clutch--refresh-schema-cache conn))
                   (entry (clutch--schema-status-entry conn))
                   (tables (plist-get entry :tables))
                   (err (plist-get entry :error)))
              (unless quiet
                (message (if ok
                             (format "Schema refreshed%s"
                                     (if tables (format " (%d tables)" tables) ""))
                           (format "Schema refresh failed%s"
                                   (if err (format ": %s" err) "")))))
              ok)))))))

(defun clutch--schema-cache-guidance (conn)
  "Return a recovery hint for CONN schema cache state, or nil."
  (when-let* ((entry (clutch--schema-status-entry conn))
              (state (plist-get entry :state)))
    (pcase state
      ('stale
       "Schema cache is stale — press C-c C-s before relying on cached table names or object discovery metadata")
      ('failed
       "Schema refresh failed earlier — press C-c C-s to retry before relying on cached table names or object discovery metadata")
      ('refreshing
       "Schema refresh is in progress — cached table names or object discovery metadata may still be behind")
      (_ nil))))

(defun clutch--warn-schema-cache-state (&optional conn)
  "Show a recovery hint when CONN schema cache is not ready."
  (when-let* ((hint (clutch--schema-cache-guidance (or conn clutch-connection))))
    (message "%s" hint)))

;;;; Console persistence

(defconst clutch--console-url-secret-param-regexp
  (concat "\\([?&;]"
          (regexp-opt '("access_token" "pass" "password" "passwd"
                        "private_key" "private-key" "pwd" "secret" "token"))
          "=\\)[^&;]*")
  "Regexp matching URL parameters that must not affect console identity.")

(defconst clutch--console-identity-param-keys
  '(:user :host :port :database :schema :sid :ssh-host
    :tramp-default-directory)
  "Connection params that distinguish query console identity.")

(defun clutch--console-redacted-url (url)
  "Return URL with obvious password parameters redacted."
  (when url
    (let ((case-fold-search t))
      (replace-regexp-in-string
       clutch--console-url-secret-param-regexp
       "\\1REDACTED"
       url))))

(defun clutch--console-identity-pairs (params)
  "Return canonical non-secret identity pairs for connection PARAMS."
  (when params
    (let ((backend (or (plist-get params :backend)
                       (plist-get params :driver)))
          (url (clutch--console-redacted-url (plist-get params :url))))
      (append
       (and backend (list (cons :backend backend)))
       (cl-loop for key in clutch--console-identity-param-keys
                when (plist-member params key)
                collect (cons key (plist-get params key)))
       (and url (list (cons :url url)))))))

(defun clutch--console-identity-from-params (params)
  "Return a stable query-console persistence identity from PARAMS."
  (when-let* ((pairs (clutch--console-identity-pairs params)))
    (concat "console-"
            (secure-hash 'sha256 (prin1-to-string pairs)))))

(defun clutch--console-persistence-name (name &optional params)
  "Return storage identity for console NAME and PARAMS."
  (or (clutch--console-identity-from-params params)
      name))

(defun clutch--console-file (name)
  "Return the persistence file path for console NAME."
  (expand-file-name
   (concat (replace-regexp-in-string "[/:\\*?\"<>|]" "_" name) ".sql")
   clutch-console-directory))

(defun clutch--save-console ()
  "Save console buffer content to its persistence file."
  (when clutch--console-name
    (condition-case err
        (progn
          (make-directory clutch-console-directory t)
          (let ((coding-system-for-write 'utf-8-unix)
                (storage-name (or clutch--console-storage-name
                                  clutch--console-name)))
            (write-region (point-min) (point-max)
                          (clutch--console-file storage-name)
                          nil 'silent)))
      (error
       (message "Failed to save console %s: %s"
                clutch--console-name
                (error-message-string err))))))

(defun clutch--save-all-consoles ()
  "Save content of all open query console buffers.
Run from `kill-emacs-hook' to persist consoles on Emacs exit."
  (dolist (buf (buffer-list))
    (with-current-buffer buf
      (clutch--save-console))))

;;;; Schema cache + completion

;; NOTE: connection helpers live in clutch-connection.el, query execution and
;; SQL rewriting in clutch-query.el, and result display helpers in clutch-ui.el.

(defun clutch--refresh-result-status-line ()
  "Refresh the result buffer status line without rebuilding the table body."
  (when (derived-mode-p 'clutch-result-mode)
    (let* ((rows (or clutch--filtered-rows clutch--result-rows))
           (visible-cols (clutch--visible-columns))
           (widths (clutch--effective-widths))
           (nw (clutch--row-number-digits)))
      (clutch--update-result-line-formats rows visible-cols widths nw))))

(defun clutch--refresh-schema-status-ui (conn)
  "Refresh mode-line or status line in buffers attached to CONN."
  (when conn
    (let ((key (clutch--connection-key conn)))
      (dolist (buf (buffer-list))
        (when (buffer-live-p buf)
          (with-current-buffer buf
            (when (and clutch-connection
                       (string= (clutch--connection-key clutch-connection) key))
              (cond
               ((derived-mode-p 'clutch-describe-mode)
                (when clutch--describe-object-entry
                  (clutch--render-object-describe clutch-connection
                                                 clutch--describe-object-entry
                                                 clutch--connection-params
                                                 clutch--conn-sql-product)))
               ((derived-mode-p 'clutch-result-mode)
                (clutch--refresh-result-status-line)
                (clutch--update-position-indicator))
               ((derived-mode-p 'clutch-mode)
                (clutch--update-console-buffer-name)
                (clutch--update-mode-line))))))))))

;;;###autoload
(defun clutch-refresh-schema ()
  "Refresh the schema cache for the current connection.
Useful after DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE)
executed outside clutch that would otherwise leave stale completions."
  (interactive)
  (clutch--refresh-current-schema nil t))

(defun clutch--update-connection-params-for-buffers (conn update-fn)
  "Apply UPDATE-FN to buffer-local connection params for buffers attached to CONN."
  (dolist (buf (buffer-list))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (when (eq clutch-connection conn)
          (setq-local clutch--connection-params
                      (funcall update-fn clutch--connection-params))
          (clutch--update-mode-line))))))

(defun clutch--list-clickhouse-databases (conn)
  "Return sorted database names visible to ClickHouse CONN."
  (sort
   (delete-dups
    (cl-loop for row in (clutch-db-result-rows (clutch-db-query conn "SHOW DATABASES"))
             for db = (car row)
             when (and (stringp db) (not (string-empty-p db)))
             collect db))
   #'string-collate-lessp))

;;;###autoload
(defun clutch-switch-database ()
  "Switch the current ClickHouse connection to another database by reconnecting."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (clutch--user-error "No active connection")))
         (params (or (plist-get context :params)
                     (car (clutch--connection-context conn))
                     (clutch--user-error "No reconnect parameters for this connection")))
         (current (or (plist-get params :database) "default"))
         (databases (clutch--list-clickhouse-databases conn)))
    (unless (or (clutch--connection-clickhouse-p conn)
                (clutch--params-clickhouse-p params))
      (clutch--user-error "Runtime database switching is currently available only for ClickHouse"))
    (unless databases
      (clutch--user-error "No databases returned by SHOW DATABASES"))
    (let ((database (completing-read
                     (if current
                         (format "Switch to database (current %s): " current)
                       "Switch to database: ")
                     databases nil t nil nil current)))
      (unless (string-empty-p database)
        (if (string-equal database current)
            (message "Already on database %s" current)
          (when (clutch--connection-alive-p conn)
            (clutch--confirm-disconnect-transaction-loss
             conn
             "Uncommitted changes will be lost.  Switch database? "))
          (clutch--replace-connection
           conn
           (plist-put (copy-sequence params) :database database)
           (plist-get context :product))
          (message "Current database: %s" database))))))

;;;###autoload
(defun clutch-switch-schema ()
  "Switch the current schema or database on the active connection."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (clutch--user-error "No active connection")))
         (params (or clutch--connection-params
                     (plist-get context :params))))
    (if (or (clutch--connection-clickhouse-p conn)
            (clutch--params-clickhouse-p params))
        (clutch-switch-database)
      (let* ((schemas (clutch-db-list-schemas conn))
             (current (clutch-db-current-schema conn))
             (old-key (clutch--connection-key conn)))
        (unless schemas
          (clutch--user-error "Runtime schema switching is not available for this connection"))
        (let ((schema (completing-read
                       (if current
                           (format "Switch to schema (current %s): " current)
                         "Switch to schema: ")
                       schemas nil t nil nil current)))
          (unless (string-empty-p schema)
            (if (and current
                     (string= (downcase schema) (downcase current)))
                (message "Already on schema %s" current)
              (condition-case err
                  (progn
                    (clutch-db-set-current-schema conn schema)
                    (clutch--clear-connection-problem-capture conn)
                    (clutch--update-connection-params-for-buffers
                     conn
                     (lambda (params)
                       (if (eq (plist-get params :backend) 'mysql)
                           (plist-put params :database schema)
                         (plist-put params :schema schema))))
                    (clutch--clear-connection-metadata-caches conn old-key)
                    (clutch--clear-connection-metadata-caches conn)
                    (clutch--refresh-current-schema t)
                    (message "Current schema: %s" schema))
                (clutch-db-error
                 (let* ((message (error-message-string err))
                        (summary (clutch--humanize-db-error message)))
                   (clutch--remember-buffer-query-error-details
                    (current-buffer) conn nil err)
                   (when clutch-debug-mode
                     (clutch--remember-debug-event
                      :connection conn
                      :op "schema-switch"
                      :phase "error"
                      :backend (clutch--backend-key-from-conn conn)
                      :summary summary
                      :context (list :schema schema
                                     :current-schema current)))
                   (clutch--user-error "%s"
                               (clutch--debug-workflow-message summary))))))))))))

;;;; clutch-mode (SQL editing major mode)

(defvar clutch-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-c") #'clutch-execute-dwim)
    (define-key map (kbd "C-c C-r") #'clutch-execute-region)
    (define-key map (kbd "C-c C-b") #'clutch-execute-buffer)
    (define-key map (kbd "C-c C-e") #'clutch-connect)
    (define-key map (kbd "C-c C-m") #'clutch-commit)
    (define-key map (kbd "C-c C-u") #'clutch-rollback)
    (define-key map (kbd "C-c C-a") #'clutch-toggle-auto-commit)
    (define-key map (kbd "C-c C-j") #'clutch-jump)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "C-c C-l") #'clutch-switch-schema)
    (define-key map (kbd "C-c C-p") #'clutch-preview-execution-sql)
    (define-key map (kbd "C-c C-s") #'clutch-refresh-schema)
    (define-key map (kbd "C-c ?") #'clutch-dispatch)
    (define-key map (kbd "C-c TAB") #'clutch-complete-at-point)
    (define-key map (kbd "C-c <tab>") #'clutch-complete-at-point)
    (define-key map (kbd "TAB") #'clutch-complete-qualified-or-indent)
    (define-key map (kbd "<tab>") #'clutch-complete-qualified-or-indent)
    map)
  "Keymap for `clutch-mode'.")

;;;###autoload
(define-derived-mode clutch-mode sql-mode "clutch"
  "Major mode for editing and executing SQL queries.

\\<clutch-mode-map>
Key bindings:
  \\[clutch-execute-dwim]	Execute region or statement/query at point
  \\[clutch-execute-region]	Execute region
  \\[clutch-execute-buffer]	Execute buffer
  \\[clutch-connect]	Connect to server
  \\[clutch-jump]	Object jump
  \\[clutch-describe-dwim]	Describe object
  \\[clutch-act-dwim]	Object actions
  \\[clutch-switch-schema]	Switch schema/database
  \\[clutch-complete-at-point]	Complete SQL identifier at point
  \\[clutch-complete-qualified-or-indent]	Complete qualified column or indent
  \\[clutch-preview-execution-sql]	Preview execution"
  (set-buffer-file-coding-system 'utf-8-unix nil t)
  (add-hook 'kill-emacs-hook #'clutch--save-all-consoles)
  (add-hook 'kill-buffer-hook #'clutch--disconnect-on-kill nil t)
  (add-hook 'kill-buffer-hook #'clutch--save-console nil t)
  (clutch--install-completion-capfs)
  (add-hook 'eldoc-documentation-functions
            #'clutch--eldoc-function nil t)
  (add-hook 'xref-backend-functions #'clutch--xref-backend nil t)
  (clutch--update-mode-line))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mysql\\'" . clutch-mode))

;;;###autoload
(autoload 'clutch-query-console "clutch" nil t)
;;;###autoload
(autoload 'clutch-query-sqlite-file "clutch" nil t)
;;;###autoload
(autoload 'clutch-switch-console "clutch" nil t)
;;;###autoload
(autoload 'clutch-execute "clutch" nil t)
;;;###autoload
(autoload 'clutch-edit-indirect "clutch" nil t)


;;;; REPL mode

(defvar clutch-repl-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "C-c C-e") #'clutch-connect)
    (define-key map (kbd "C-c C-m") #'clutch-commit)
    (define-key map (kbd "C-c C-u") #'clutch-rollback)
    (define-key map (kbd "C-c C-a") #'clutch-toggle-auto-commit)
    (define-key map (kbd "C-c C-j") #'clutch-jump)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "C-c C-l") #'clutch-switch-schema)
    map)
  "Keymap for `clutch-repl-mode'.")

(defvar-local clutch-repl--pending-input ""
  "Accumulated partial SQL input waiting for a semicolon.")
;;;###autoload
(define-derived-mode clutch-repl-mode comint-mode "clutch-repl"
  "Major mode for database REPL.

\\<clutch-repl-mode-map>
  \\[clutch-connect]	Connect to server
  \\[clutch-jump]	Object jump
  \\[clutch-describe-dwim]	Describe object
  \\[clutch-act-dwim]	Object actions
  \\[clutch-switch-schema]	Switch schema/database"
  (setq comint-prompt-regexp "^db> \\|^    -> ")
  (setq comint-input-sender #'clutch-repl--input-sender)
  (clutch--install-completion-capfs)
  (add-hook 'xref-backend-functions #'clutch--xref-backend nil t)
  (add-hook 'kill-buffer-hook #'clutch--disconnect-on-kill nil t))

(defun clutch-repl--input-sender (_proc input)
  "Process INPUT from comint.
Accumulates input until a semicolon is found, then executes."
  (let ((combined (concat clutch-repl--pending-input
                          (unless (string-empty-p clutch-repl--pending-input) "\n")
                          input)))
    (if (string-match-p ";\\s-*$" combined)
        ;; Complete statement — execute
        (progn
          (setq clutch-repl--pending-input "")
          (clutch-repl--execute-and-print (string-trim combined)))
      ;; Incomplete — accumulate and show continuation prompt
      (setq clutch-repl--pending-input combined)
      (clutch-repl--output "    -> "))))

(defun clutch-repl--output (text)
  "Insert TEXT into the REPL buffer at the process mark."
  (let ((inhibit-read-only t)
        (proc (get-buffer-process (current-buffer))))
    (goto-char (process-mark proc))
    (insert text)
    (set-marker (process-mark proc) (point))))

(defun clutch-repl--format-dml-result (result elapsed)
  "Format a DML RESULT with ELAPSED time as a string for the REPL."
  (let ((msg (format "\nAffected rows: %s"
                     (or (clutch-db-result-affected-rows result) 0))))
    (when-let* ((id (clutch-db-result-last-insert-id result))
                ((> id 0)))
      (setq msg (concat msg (format ", Last insert ID: %s" id))))
    (when-let* ((w (clutch-db-result-warnings result))
                ((> w 0)))
      (setq msg (concat msg (format ", Warnings: %s" w))))
    (format "%s (%.3fs)\n\ndb> " msg elapsed)))

(defun clutch-repl--execute-and-print (sql)
  "Execute SQL and print results inline in the REPL."
  (setq-local clutch--buffer-error-details nil)
  (condition-case err
      (progn
        (clutch--ensure-connection)
        (setq clutch--last-query sql)
        (let* ((start (float-time))
               (result (clutch--run-db-query clutch-connection sql))
               (elapsed (- (float-time) start))
               (columns (clutch-db-result-columns result))
               (rows (clutch-db-result-rows result)))
          (if columns
              (let* ((col-names (clutch--column-names columns))
                     (table-str (clutch--render-static-table
                                 col-names rows columns)))
                (clutch-repl--output
                 (format "\n%s\n%d row%s in %.3fs\n\ndb> "
                         table-str (length rows)
                         (if (= (length rows) 1) "" "s")
                         elapsed)))
            (clutch-repl--output (clutch-repl--format-dml-result result elapsed)))))
    (quit
     (condition-case nil
         (clutch--handle-query-quit clutch-connection)
       (clutch-query-interrupted
        (clutch-repl--output "\nERROR: Query interrupted\n\ndb> "))))
    (error
     (clutch--remember-buffer-query-error-details
      (current-buffer) clutch-connection sql err)
     (clutch-repl--output
      (format "\nERROR: %s\n\ndb> "
              (clutch--humanize-db-error (error-message-string err)))))))

;;;###autoload
(defun clutch-repl ()
  "Start a database REPL buffer."
  (interactive)
  (let* ((buf-name "*clutch REPL*")
         (buf (get-buffer-create buf-name)))
    (unless (comint-check-proc buf)
      (with-current-buffer buf
        ;; Start a dummy process for comint
        (let ((proc (start-process "clutch-repl" buf "cat")))
          (set-process-query-on-exit-flag proc nil)
          (clutch-repl-mode)
          (clutch-repl--output "db> "))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

;;;; Transient dispatch menus

(defun clutch--dispatch-transaction-controls-inapt-p ()
  "Return non-nil when current connection has no transaction controls."
  (not (and clutch-connection
            (clutch--manual-commit-supported-p clutch-connection))))

(transient-define-suffix clutch--dispatch-commit ()
  "Transient suffix for `clutch-commit'."
  :inapt-if #'clutch--dispatch-transaction-controls-inapt-p
  (interactive)
  (call-interactively #'clutch-commit))

(transient-define-suffix clutch--dispatch-rollback ()
  "Transient suffix for `clutch-rollback'."
  :inapt-if #'clutch--dispatch-transaction-controls-inapt-p
  (interactive)
  (call-interactively #'clutch-rollback))

(transient-define-suffix clutch--dispatch-toggle-auto-commit ()
  "Transient suffix for `clutch-toggle-auto-commit' with a dynamic label."
  :description (lambda ()
                 (cond
                  ((clutch--dispatch-transaction-controls-inapt-p)
                   "Auto-commit unavailable")
                  ((and clutch-connection
                        (clutch-db-manual-commit-p clutch-connection))
                   "Enable auto-commit")
                  (t
                   "Disable auto-commit")))
  :inapt-if (lambda ()
               (or (clutch--dispatch-transaction-controls-inapt-p)
                   (and clutch-connection
                        (clutch-db-manual-commit-p clutch-connection)
                        (clutch--tx-dirty-p clutch-connection))))
  (interactive)
  (call-interactively #'clutch-toggle-auto-commit))

;;;###autoload (autoload 'clutch-dispatch "clutch" nil t)
(transient-define-prefix clutch-dispatch ()
  "Main dispatch menu for clutch."
  [ :pad-keys t
   ["Connection"
    ("c" "Connect"    clutch-connect)
    ("q" "Query console" clutch-query-console)
    ("f" "SQLite file" clutch-query-sqlite-file)
    ("S" "Prepare SSH" clutch-prepare-ssh-host)
    ("d" "Disconnect" clutch-disconnect)
    ("m" "Commit"            clutch--dispatch-commit)
    ("u" "Rollback"          clutch--dispatch-rollback)
    ("a" clutch--dispatch-toggle-auto-commit)
    ("R" "REPL"              clutch-repl)]
   ["Execute"
    ("x" "Query at point" clutch-execute-query-at-point)
    ("X" "Statement (;-only)" clutch-execute-statement-at-point)
    ("r" "Region"         clutch-execute-region)
    ("b" "Buffer"         clutch-execute-buffer)
    ("p" "Preview execution" clutch-preview-execution-sql)
    ("k" "Copy agent context" clutch-copy-context-for-agent)]
   ["Edit"
    ("'" "Indirect edit"  clutch-edit-indirect)]
   ["Objects"
    ("j" "Jump to object"     clutch-jump)
    ("D" "Describe object"    clutch-describe-dwim)
    ("o" "Object actions"     clutch-act-dwim)
    ("l" "Switch schema/db"   clutch-switch-schema)
    ("s" "Refresh schema"   clutch-refresh-schema)]])

(provide 'clutch)
;;; clutch.el ends here
