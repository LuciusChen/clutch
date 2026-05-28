;;; clutch-connection.el --- Connection lifecycle and transaction commands -*- lexical-binding: t; -*-

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

;; Connection lifecycle management, transaction state tracking, backend
;; detection, header-line rendering, and authentication for clutch.
;;
;; This module is required by `clutch.el' — do not require `clutch' here.

;;; Code:

(require 'clutch-db)
(require 'clutch-schema)
(require 'auth-source)
(require 'cl-lib)
(require 'comint)
(require 'subr-x)
(require 'tramp)

(declare-function auth-source-pass-entries "auth-source-pass" ())
(declare-function auth-source-pass-parse-entry "auth-source-pass" (entry))
(declare-function tramp-rpc-controlmaster-options "tramp-rpc" (vec))
(defvar tramp-rpc-use-controlmaster)

;; Forward declarations — shared buffer-local variables
(defvar clutch-connection)
(defvar clutch--executing-p)
(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar clutch--console-name)
(defvar clutch--console-ad-hoc-params)
(defvar clutch--describe-object-entry)
(defvar clutch-connection-alist nil)
(defvar clutch-connect-timeout-seconds 10)
(defvar clutch-read-idle-timeout-seconds 30)
(defvar clutch-query-timeout-seconds 30)
(defvar clutch-jdbc-rpc-timeout-seconds 30)
(defvar clutch-tramp-context-policy 'ask)
(defvar clutch--dml-result)
(defvar clutch-debug-mode nil)
(defvar clutch--spinner-timer nil
  "Timer driving the mode-line spinner animation, or nil.")
(defvar clutch--spinner-index 0
  "Current frame index into `clutch--spinner-frames'.")

(defconst clutch--spinner-frames
  ["⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏"]
  "Braille spinner frames.")

(defconst clutch--spinner-interval 0.1
  "Seconds between spinner frame advances.")

(defconst clutch--ssh-tunnel-ready-poll-interval 0.05
  "Seconds between SSH tunnel readiness checks.")

(defconst clutch--ssh-direct-first-probe-timeout 1.0
  "Seconds to wait when probing a direct endpoint before SSH fallback.")

;; Forward declarations — sibling module functions
(declare-function clutch--clear-connection-problem-capture "clutch-query" (connection))
(declare-function clutch--forget-problem-record "clutch-query" (&optional buffer connection))
(declare-function clutch--remember-debug-event "clutch-query" (&rest event))
(declare-function clutch--remember-problem-record "clutch-query" (&rest args))
(declare-function clutch--update-console-buffer-name "clutch-query" ())
(declare-function clutch--refresh-result-status-line "clutch-ui" ())
(declare-function clutch--debug-workflow-message "clutch-query" (message))
(declare-function clutch--render-object-describe
                  "clutch-object" (conn entry params product))

;; Forward declarations — functions defined in other modules
(declare-function clutch--icon "clutch-ui" (name &optional fallback &rest icon-args))
(declare-function clutch--icon-with-face "clutch-ui"
                  (name fallback face &rest icon-args))
(declare-function clutch--nerd-icons-available-p "clutch-ui" ())

;;;; Connection identity

(defvar clutch--connection-remote-params-cache
  (make-hash-table :test 'eq :weakness 'key)
  "Original remote connection params keyed by live connection object.")

(defvar clutch--connection-transport-cache
  (make-hash-table :test 'eq :weakness 'key)
  "Transport process plists keyed by live connection object.")

(defvar clutch--tramp-rpc-controlmaster-warning-reported nil
  "Non-nil after warning about an old tramp-rpc ControlMaster API.")

(defconst clutch--tramp-ssh-forward-methods '("ssh" "scp" "rsync" "rpc")
  "TRAMP methods Clutch can map to a binary-clean ssh command.")

(defconst clutch--tramp-container-forward-methods '("docker" "podman")
  "TRAMP container methods Clutch can bridge with runtime exec.")

(defconst clutch--container-relay-script
  (string-join
   '("host=$1"
     "port=$2"
     "if command -v socat >/dev/null 2>&1; then"
     "  exec socat - TCP:\"$host\":\"$port\""
     "fi"
     "if command -v nc >/dev/null 2>&1; then"
     "  exec nc \"$host\" \"$port\""
     "fi"
     "if command -v netcat >/dev/null 2>&1; then"
     "  exec netcat \"$host\" \"$port\""
     "fi"
     "if command -v bash >/dev/null 2>&1; then"
     "  exec bash -lc 'host=$1; port=$2;"
     "    exec 3<>/dev/tcp/$host/$port;"
     "    cat <&3 & cat >&3; wait' clutch-bash \"$host\" \"$port\""
     "fi"
     "echo 'clutch container relay requires socat, nc, netcat, or bash' >&2"
     "exit 127")
   "\n")
  "Shell script run inside a container to proxy stdio to HOST:PORT.")

(defun clutch--connection-remote-param (conn key)
  "Return remote KEY for CONN when clutch cached transport metadata."
  (when conn
    (plist-get (gethash conn clutch--connection-remote-params-cache) key)))

(defun clutch--connection-remote-host (conn)
  "Return the remote host label for CONN."
  (or (clutch--connection-remote-param conn :host)
      (clutch-db-host conn)))

(defun clutch--connection-remote-port (conn)
  "Return the remote port label for CONN."
  (or (clutch--connection-remote-param conn :port)
      (clutch-db-port conn)))

(defun clutch--tramp-vector-display-target (vec)
  "Return a compact display target for TRAMP VEC."
  (let ((host (tramp-file-name-host vec))
        (user (tramp-file-name-user vec))
        (port (tramp-file-name-port vec)))
    (when (and (stringp host) (not (string-empty-p host)))
      (concat
       (if (and (stringp user) (not (string-empty-p user)))
           (format "%s@%s" user host)
         host)
       (if port (format ":%s" port) "")))))

(defun clutch--tramp-display-label (tramp-default-directory)
  "Return a compact label for TRAMP-DEFAULT-DIRECTORY."
  (let* ((vec (clutch--tramp-dissect-file-name tramp-default-directory))
         (hops (clutch--tramp-hop-vectors (tramp-file-name-hop vec)))
         (targets (delq nil
                        (append
                         (mapcar #'clutch--tramp-vector-display-target hops)
                         (list (clutch--tramp-vector-display-target vec))))))
    (if targets
        (string-join targets "->")
      (or (file-remote-p tramp-default-directory)
          tramp-default-directory))))

(defun clutch--connection-transport-label (conn)
  "Return a compact transport label for CONN, or nil."
  (or (clutch--connection-remote-param conn :ssh-host)
      (when-let* ((dir (clutch--connection-remote-param
                        conn :tramp-default-directory)))
        (clutch--tramp-display-label dir))))

(defun clutch--stop-connection-transport (transport)
  "Stop TRANSPORT and any process it owns."
  (when-let* ((proc (plist-get transport :process)))
    (when (processp proc)
      (dolist (child (process-get proc :clutch-container-children))
        (when (process-live-p child)
          (delete-process child))))
    (when (process-live-p proc)
      (delete-process proc))))

(defun clutch--remember-connection-transport (conn params &optional tunnel)
  "Remember original PARAMS and optional TRANSPORT for CONN."
  (when conn
    (let ((remote nil))
      (when-let* ((host (plist-get params :host)))
        (setq remote (plist-put remote :host host)))
      (when-let* ((port (plist-get params :port)))
        (setq remote (plist-put remote :port port)))
      (when-let* ((ssh-host (plist-get tunnel :ssh-host)))
        (setq remote (plist-put remote :ssh-host ssh-host)))
      (when-let* ((tramp-default-directory
                   (plist-get tunnel :tramp-default-directory)))
        (setq remote (plist-put remote :tramp-default-directory
                                tramp-default-directory)))
      (when-let* ((kind (plist-get tunnel :kind)))
        (setq remote (plist-put remote :transport kind)))
      (puthash conn remote clutch--connection-remote-params-cache))
    (if tunnel
        (puthash conn tunnel clutch--connection-transport-cache)
      (remhash conn clutch--connection-transport-cache))))

(defun clutch--release-connection-transport (conn)
  "Stop any connection transport and forget cached metadata for CONN."
  (when conn
    (when-let* ((transport (gethash conn clutch--connection-transport-cache)))
      (clutch--stop-connection-transport transport))
    (remhash conn clutch--connection-transport-cache)
    (remhash conn clutch--connection-remote-params-cache)))

(defun clutch--sqlite-database-display-label (database &optional compact)
  "Return a display label for SQLite DATABASE.
When COMPACT is non-nil, prefer the file basename for header-line use."
  (cond
   ((not (stringp database)) "SQLite")
   ((string= database ":memory:") ":memory:")
   (compact
    (let ((name (file-name-nondirectory (directory-file-name database))))
      (if (string-empty-p name)
          (abbreviate-file-name database)
        name)))
   (t
    (abbreviate-file-name database))))

(defun clutch--connection-key (conn)
  "Return a descriptive string for CONN like \"user@host:port/db\"."
  (if (eq (clutch--backend-key-from-conn conn) 'sqlite)
      (format "sqlite:%s" (or (clutch-db-database conn) ""))
    (format "%s@%s:%s/%s"
            (or (clutch-db-user conn) "?")
            (or (clutch--connection-remote-host conn) "?")
            (or (clutch--connection-remote-port conn) "?")
            (or (clutch-db-database conn) ""))))

(defun clutch--default-port-for-connection (conn)
  "Return the default port for CONN's backend, or nil when not applicable."
  (let ((backend (clutch--backend-key-from-conn conn)))
    (and backend (clutch-db-backend-default-port backend))))

(defun clutch--connection-display-key (conn)
  "Return a compact display identity for CONN for use in UI only."
  (if (eq (clutch--backend-key-from-conn conn) 'sqlite)
      (clutch--sqlite-database-display-label (clutch-db-database conn) t)
    (let* ((user (or (clutch-db-user conn) "?"))
           (host (or (clutch--connection-remote-host conn) "?"))
           (port (clutch--connection-remote-port conn))
           (transport-label (clutch--connection-transport-label conn))
           (default-port (clutch--default-port-for-connection conn)))
      (concat
       (format "%s@%s%s"
               user
               host
               (if (and port default-port (equal port default-port))
                   ""
                 (if port
                     (format ":%s" port)
                   "")))
       (if transport-label
           (format " via %s" transport-label)
         "")))))

(defun clutch--ensure-clutch-loaded ()
  "Load the `clutch' entrypoint before module-autoloaded commands run.
This ensures user setup attached to feature `clutch' has executed before
interactive readers inspect shared customization such as
`clutch-connection-alist'."
  (unless (featurep 'clutch)
    (require 'clutch)))

(defun clutch--connection-clickhouse-p (conn)
  "Return non-nil when CONN is a ClickHouse connection."
  (and conn
       (eq (clutch--backend-key-from-conn conn) 'clickhouse)))

(defun clutch--params-clickhouse-p (params)
  "Return non-nil when connection PARAMS target ClickHouse."
  (eq (and params (clutch--backend-key-from-params params)) 'clickhouse))

;;;; SQL helpers for transaction state

(defun clutch--manual-commit-dirtying-query-p (sql)
  "Return non-nil when SQL should mark a manual-commit transaction dirty."
  (member (clutch-db-sql-main-op-keyword sql)
          '("INSERT" "UPDATE" "DELETE" "MERGE" "REPLACE")))

(defun clutch--transaction-control-query-p (sql)
  "Return non-nil when SQL is explicit transaction control."
  (member (clutch-db-sql-leading-keyword sql)
          '("COMMIT" "ROLLBACK" "END" "ABORT")))

;;;; Transaction state

(defvar clutch--tx-dirty-cache (make-hash-table :test 'eq :weakness 'key)
  "Connections with uncommitted DML.")

(defun clutch--tx-dirty-p (conn)
  "Return non-nil when CONN has uncommitted DML."
  (and conn (gethash conn clutch--tx-dirty-cache)))

(defun clutch--manual-commit-supported-p (conn)
  "Return non-nil when CONN supports Clutch transaction controls."
  (and conn
       (condition-case nil
           (clutch-db-manual-commit-supported-p conn)
         ((clutch-db-error cl-no-applicable-method wrong-type-argument) nil))))

(defun clutch--refresh-transaction-ui (conn)
  "Refresh transaction indicators for buffers attached to CONN."
  (when conn
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and clutch-connection
                     (eq clutch-connection conn)
                     (derived-mode-p 'clutch-mode 'clutch-repl-mode))
            (clutch--update-mode-line)))))))

(defun clutch--set-tx-dirty (conn)
  "Mark CONN as having uncommitted DML."
  (when conn
    (puthash conn t clutch--tx-dirty-cache)
    (clutch--refresh-transaction-ui conn)))

(defun clutch--clear-tx-dirty (conn)
  "Forget uncommitted transaction state for CONN."
  (when conn
    (remhash conn clutch--tx-dirty-cache)
    (clutch--refresh-transaction-ui conn)))

(defun clutch--annotate-dml-result-buffers (conn banner)
  "Set BANNER as header-line on all open DML result buffers for CONN."
  (dolist (buf (buffer-list))
    (with-current-buffer buf
      (when (and (derived-mode-p 'clutch-result-mode)
                 (eq clutch-connection conn)
                 (bound-and-true-p clutch--dml-result))
        (setq-local header-line-format banner)))))

(defun clutch--mark-dml-results-rolled-back (conn)
  "Add a rollback warning banner to open DML result buffers for CONN."
  (clutch--annotate-dml-result-buffers
   conn
   (propertize "  ⚠  Transaction rolled back — changes not persisted"
               'face '(:inherit warning :weight bold))))

(defun clutch--mark-dml-results-committed (conn)
  "Add a committed confirmation banner to open DML result buffers for CONN."
  (clutch--annotate-dml-result-buffers
   conn
   (propertize "  ✓  Transaction committed"
               'face '(:inherit success :weight bold))))

(defun clutch--mark-dml-results-connection-closed (conn)
  "Add a connection-closed notice to open DML result buffers for CONN."
  (clutch--annotate-dml-result-buffers
   conn
   (propertize "  ✕  Connection closed"
               'face '(:inherit shadow))))

(defun clutch--tx-header-line-segment (conn)
  "Return a header-line segment for CONN transaction state, or nil.
Shows Tx: Auto, Tx: Manual, or Tx: Manual* (dirty)."
  (when (clutch--manual-commit-supported-p conn)
    (let* ((state-face (if (clutch-db-manual-commit-p conn)
                           (if (clutch--tx-dirty-p conn) 'error 'warning)
                         'success))
           (icon (clutch--icon-with-face '(mdicon . "nf-md-database_lock")
                                         "⛁" state-face))
           (label (if (clutch-db-manual-commit-p conn)
                      (if (clutch--tx-dirty-p conn) "Tx: Manual*" "Tx: Manual")
                    "Tx: Auto")))
      (concat (unless (string-empty-p icon)
                (concat icon " "))
              (propertize label 'face state-face)))))

(defun clutch--record-tx-state-after-query (conn sql)
  "Update transaction dirty state for successful SQL on CONN."
  (when (clutch-db-manual-commit-p conn)
    (cond
     ((clutch--transaction-control-query-p sql)
      (clutch--clear-tx-dirty conn))
     ((clutch-db-sql-schema-affecting-p sql)
      (pcase (clutch-db-schema-transaction-effect conn sql)
        ('dirty (clutch--set-tx-dirty conn))
        ('clear (clutch--clear-tx-dirty conn))))
     ((clutch--manual-commit-dirtying-query-p sql)
      (clutch--set-tx-dirty conn)))))

(defun clutch--run-db-query (conn sql &optional params)
  "Execute SQL on CONN with optional PARAMS and keep transaction UI state in sync."
  (let ((result (if params
                    (clutch-db-execute-params conn sql params)
                  (clutch-db-query conn sql))))
    (clutch--clear-connection-problem-capture conn)
    (clutch--record-tx-state-after-query conn sql)
    result))

(defun clutch--confirm-disconnect-transaction-loss (conn prompt)
  "Require confirmation with PROMPT before dropping dirty manual-commit CONN."
  (when (and (clutch-db-manual-commit-p conn)
             (clutch--tx-dirty-p conn)
             (not (yes-or-no-p prompt)))
    (user-error "Disconnect cancelled")))

;;;; Connection lifecycle

(defun clutch--connection-alive-p (conn)
  "Return non-nil if CONN is live."
  (and conn (clutch-db-live-p conn)))

(defun clutch--connection-context (conn)
  "Return `(PARAMS PRODUCT)' for CONN from any attached buffer, or nil."
  (when conn
    (or (and (eq clutch-connection conn)
             clutch--connection-params
             (list clutch--connection-params clutch--conn-sql-product))
        (cl-loop for buf in (buffer-list)
                 when (and (buffer-live-p buf)
                           (eq (buffer-local-value 'clutch-connection buf) conn)
                           (buffer-local-value 'clutch--connection-params buf))
                 return (list (buffer-local-value 'clutch--connection-params buf)
                              (buffer-local-value 'clutch--conn-sql-product buf))))))

(defun clutch--bind-connection-context (conn &optional params product)
  "Bind CONN and related reconnect context in the current buffer.
Also store PARAMS and PRODUCT when present."
  (clutch--forget-problem-record (current-buffer) clutch-connection)
  (setq-local clutch-connection conn)
  (when params
    (setq-local clutch--connection-params params))
  (when (or params product)
    (setq-local clutch--conn-sql-product
                (or product
                    (and params (clutch--effective-sql-product params))
                    clutch--conn-sql-product))))

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

(defun clutch--rebind-connection-buffers (old-conn new-conn params product)
  "Replace OLD-CONN with NEW-CONN across attached buffers using PARAMS and PRODUCT."
  (dolist (buf (buffer-list))
    (when (and (buffer-live-p buf)
               (eq (buffer-local-value 'clutch-connection buf) old-conn))
      (with-current-buffer buf
        (clutch--bind-connection-context new-conn params product)
        (cond
         ((derived-mode-p 'clutch-mode)
          (when clutch--console-name
            (clutch--update-console-buffer-name))
          (clutch--update-mode-line))
         ((derived-mode-p 'clutch-result-mode)
          (clutch--refresh-result-status-line)))))))

(defun clutch--activate-current-buffer-connection (conn params &optional product)
  "Bind CONN as the current buffer connection and prime local UI state.
Also remember PARAMS and PRODUCT."
  (clutch--bind-connection-context conn params product)
  (clutch--prime-schema-cache conn)
  (clutch--update-mode-line)
  conn)

(defun clutch--finalize-rebound-connection (conn)
  "Prime metadata and refresh UI after CONN has been rebound to buffers."
  (clutch--prime-schema-cache conn)
  (clutch--refresh-schema-status-ui conn)
  (clutch--refresh-transaction-ui conn)
  conn)

(defun clutch--try-reconnect ()
  "Attempt to re-establish the connection for the current logical session.
Find reconnect params from the current buffer or any attached buffer that
still references the same dead connection, then rebind all attached buffers
to the new connection on success.
Staged result-buffer changes are preserved across reconnects because
they are client-side DML.  Only query re-execution should discard them.
Returns non-nil on success, nil on failure."
  (when-let* ((old-conn clutch-connection)
              (context (clutch--connection-context old-conn))
              (params (car context))
              (product (cadr context)))
    (condition-case err
        (let ((conn (clutch--build-conn params)))
          (clutch--clear-tx-dirty old-conn)
          (clutch--release-connection-transport old-conn)
          (clutch--rebind-connection-buffers old-conn conn params product)
          (clutch--finalize-rebound-connection conn)
          (message "Reconnected to %s" (clutch--connection-key conn))
          t)
      (error
       (message "Reconnect failed: %s" (error-message-string err))
       nil))))

(defun clutch--replace-connection (old-conn params &optional product)
  "Replace OLD-CONN with a new connection built from PARAMS.
PRODUCT is the effective SQL product for the new logical session."
  (let* ((product (or product (clutch--effective-sql-product params)))
         (old-key (clutch--connection-key old-conn))
         (new-conn (clutch--build-conn params)))
    (clutch--clear-tx-dirty old-conn)
    (unwind-protect
        (when (clutch--connection-alive-p old-conn)
          (clutch-db-disconnect old-conn))
      (clutch--release-connection-transport old-conn))
    (unless (clutch--connection-alive-p new-conn)
      (setq new-conn (clutch--build-conn params)))
    (clutch--rebind-connection-buffers old-conn new-conn params product)
    (clutch--clear-connection-metadata-caches old-conn old-key)
    (clutch--clear-connection-metadata-caches new-conn)
    (clutch--finalize-rebound-connection new-conn)))

(defun clutch--ensure-connection ()
  "Ensure current buffer has a live connection.
If the connection has dropped, attempts to reconnect automatically
using the stored params.  Signals a user-error if not recoverable."
  (unless (clutch--connection-alive-p clutch-connection)
    (unless (clutch--try-reconnect)
      (user-error
       (if (derived-mode-p 'clutch-result-mode 'clutch-record-mode
                           'clutch-describe-mode)
           "Connection closed.  Reconnect from the SQL buffer or REPL"
         "Not connected.  Use C-c C-e to connect")))))

;;;; Schema and metadata status UI

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
                (clutch--refresh-result-status-line))
               ((derived-mode-p 'clutch-mode)
                (clutch--update-console-buffer-name)
                (clutch--update-mode-line))))))))))

(defun clutch--current-namespace-name (conn)
  "Return the current schema/database label for CONN, or nil."
  (or (and (clutch--connection-clickhouse-p conn)
           (clutch-db-database conn))
      (clutch-db-current-schema conn)))

(defun clutch--current-schema-header-line-segment (conn)
  "Return a header-line segment for CONN's current schema or database, or nil."
  (when-let* ((schema (clutch--current-namespace-name conn)))
    (let ((icon (clutch--icon-with-face '(mdicon . "nf-md-sitemap_outline")
                                        "≣" 'header-line)))
      (if (string-empty-p icon)
          schema
        (format "%s %s" icon schema)))))

;;;; Backend detection and icons

(defconst clutch--db-icon-specs
  ;; Each entry: (BACKEND . (ICON-SPEC FALLBACK :color COLOR &rest ICON-ARGS))
  ;; :color sets the icon foreground; remaining ICON-ARGS (e.g. :height) are
  ;; forwarded to the nerd-icons function.
  '((mysql      . ((devicon . "nf-dev-mysql")              ""  :color "#469AD7"))
    (pg         . ((devicon . "nf-dev-postgresql")         ""  :color "#336791"))
    (sqlite     . ((devicon . "nf-dev-sqlite")             ""  :color "#3A7EC6"))
    (jdbc       . ((mdicon  . "nf-md-database_cog_outline") "" :color "#59636e"))
    (oracle     . ((mdicon  . "nf-md-alpha_o_circle")      "O" :color "#C74634"))
    (sqlserver  . ((devicon . "nf-dev-microsoftsqlserver") ""  :color "#CC2927"))
    (snowflake  . ((mdicon  . "nf-md-snowflake")           "❄" :color "#29B5E8"))
    (db2        . ((mdicon  . "nf-md-database")            ""  :color "#1F70C1"))
    (redshift   . ((mdicon  . "nf-md-database")            ""  :color "#8C4FFF"))
    (clickhouse . ((faicon  . "nf-fa-barcode")             ""  :color "#FFCC00")))
  "Alist mapping backend symbols to icon specs.
Each value is (ICON-SPEC FALLBACK :color COLOR &rest ICON-ARGS).
ICON-ARGS beyond :color are forwarded to the nerd-icons render function.")

(defun clutch--db-backend-icon-for-key (key)
  "Return a colored backend icon for KEY, or nil."
  (when-let* ((spec (alist-get key clutch--db-icon-specs)))
    (let* ((rest      (cddr spec))
           (color     (plist-get rest :color))
           (icon-args (cl-loop for (k v) on rest by #'cddr
                               unless (eq k :color) nconc (list k v)))
           (icon      (apply #'clutch--icon (car spec) (cadr spec) icon-args)))
      (if (and color (not (string-empty-p icon)))
          (propertize icon 'face `(:foreground ,color :inherit ,(get-text-property 0 'face icon)))
        icon))))

(defun clutch--backend-key-from-conn (conn)
  "Return backend icon key for live connection CONN, or nil."
  (or (condition-case nil
          (clutch-db-backend-key conn)
        ((clutch-db-error cl-no-applicable-method wrong-type-argument) nil))
      (let ((display-name
             (condition-case nil
                 (clutch-db-display-name conn)
               ((cl-no-applicable-method wrong-type-argument) nil))))
        (cl-loop for backend in (clutch-db-backends)
                 when (equal display-name
                             (clutch-db-backend-display-name backend))
                 return backend))))

(defun clutch--normalize-backend-key (backend)
  "Return the registered backend key for BACKEND, including public aliases."
  (pcase backend
    ((or 'pg 'postgresql) 'pg)
    ((or 'mysql 'mariadb) 'mysql)
    (_ (and (memq backend (clutch-db-backends t)) backend))))

(defun clutch--backend-key-from-params (params)
  "Return backend icon key for connection PARAMS, or nil."
  (let* ((backend (clutch--normalize-backend-key (plist-get params :backend)))
         (driver  (clutch--normalize-backend-key (plist-get params :driver))))
    (or (and (not (eq backend 'jdbc)) backend)
        driver
        backend)))

(defun clutch--effective-sql-product (params)
  "Return the SQL product to use for connection PARAMS."
  (or (plist-get params :sql-product)
      (clutch-db-backend-sql-product
       (clutch--backend-key-from-params params))))

(defun clutch--backend-display-name-from-params (params)
  "Return UI backend name for connection PARAMS, or nil."
  (or (plist-get params :display-name)
      (clutch-db-backend-display-name
       (clutch--backend-key-from-params params))))

(defun clutch--manual-backend-choices ()
  "Return backend choices offered by manual connection readers."
  (remq 'jdbc (clutch-db-backends t)))

(defun clutch--completion-backend-icon-prefix (key)
  "Return a minibuffer completion icon prefix for backend KEY."
  (let ((icon (clutch--db-backend-icon-for-key key)))
    (if (and icon
             (not (string-empty-p icon))
             (clutch--nerd-icons-available-p))
        (concat icon " ")
      "")))

(defun clutch--completion-annotation (parts)
  "Return a `completing-read' suffix annotation from non-empty PARTS."
  (let ((parts (cl-remove-if (lambda (part)
                               (or (null part)
                                   (string-empty-p part)))
                             parts)))
    (if parts
        (propertize (concat "  " (mapconcat #'identity parts " "))
                    'face 'completions-annotations)
      "")))

(defun clutch--connection-candidate-target (params)
  "Return the target annotation for connection PARAMS."
  (let* ((backend (clutch--backend-key-from-params params))
         (database (plist-get params :database))
         (sid (plist-get params :sid))
         (url (plist-get params :url))
         (host (plist-get params :host))
         (port (plist-get params :port)))
    (cond
     ((and (eq backend 'sqlite) database)
      (clutch--sqlite-database-display-label database))
     ((and host port database)
      (format "%s:%s/%s" host port database))
     ((and host database)
      (format "%s/%s" host database))
     ((and host port)
      (format "%s:%s" host port))
     (host host)
     (database database)
     (sid sid)
     (url url))))

(defun clutch--connection-candidates-affixation (candidates)
  "Return affixation triples for saved connection CANDIDATES."
  (mapcar
   (lambda (candidate)
     (let* ((params (cdr (assoc candidate clutch-connection-alist)))
            (backend (and params (clutch--backend-key-from-params params))))
       (list candidate
             (if backend
                 (clutch--completion-backend-icon-prefix backend)
               "")
             (if params
                 (clutch--completion-annotation
                  (list (clutch--connection-candidate-target params)))
               ""))))
   candidates))

(defun clutch--backend-candidates-affixation (candidates)
  "Return affixation triples for backend-name CANDIDATES."
  (mapcar
   (lambda (candidate)
     (let ((key (intern candidate)))
       (list candidate
             (clutch--completion-backend-icon-prefix key)
             "")))
   candidates))

(defun clutch--connection-backend-segment (&optional conn params)
  "Return the shared backend segment for CONN or PARAMS, or nil.
When nerd-icons is available, show only the icon; otherwise fall back
to the display name (e.g. \"MySQL\")."
  (let* ((icon (clutch--db-backend-icon-for-key
                (or (and conn (clutch--backend-key-from-conn conn))
                    (and params (clutch--backend-key-from-params params)))))
         (name (or (and conn (clutch-db-display-name conn))
                   (and params (clutch--backend-display-name-from-params params)))))
    (cond
     ((and icon (not (string-empty-p icon))
           (clutch--nerd-icons-available-p))
      icon)
     (name (propertize name 'face 'bold)))))

(defun clutch--connection-state-icon (connected)
  "Return a connection state icon for CONNECTED."
  (if connected
      (clutch--icon '(mdicon . "nf-md-database_check_outline") "⬢")
    (clutch--icon '(mdicon . "nf-md-database_off") "⨯")))

;;;; Header-line and mode-line

(defun clutch--header-line-indent ()
  "Return leading spaces to align header-line text with the buffer text area.
Accounts for the line-number gutter when `display-line-numbers-mode' is on."
  (make-string (max 1 (line-number-display-width)) ?\s))

(defun clutch--build-connection-header-line ()
  "Build the header-line string for the current clutch buffer."
  (let ((indent (clutch--header-line-indent)))
    (if (not (clutch--connection-alive-p clutch-connection))
        (let* ((sep          (propertize "  •  " 'face 'shadow))
               (backend      (clutch--connection-backend-segment
                              clutch-connection clutch--connection-params))
               (disconnect   (propertize
                              (concat (clutch--connection-state-icon nil)
                                      " Disconnect")
                              'face 'warning))
               (parts        (delq nil (list (if backend
                                                 backend
                                               nil)
                                             disconnect))))
          (concat indent
                  (if parts
                      (mapconcat #'identity parts sep)
                    disconnect)))
      (let* ((sep         (propertize "  •  " 'face 'shadow))
             (backend-sep (propertize "  ›  " 'face 'shadow))
             (backend     (clutch--connection-backend-segment clutch-connection))
             (key         (concat (clutch--connection-state-icon t)
                                  " "
                                  (clutch--connection-display-key clutch-connection)))
             (current-schema
              (clutch--current-schema-header-line-segment clutch-connection))
             (schema      (clutch--schema-status-header-line-segment clutch-connection))
             (tx          (clutch--tx-header-line-segment clutch-connection))
             (tail        (delq nil (list current-schema schema tx))))
        (concat indent
                (cond
                 ((and backend key)
                  (concat backend backend-sep key
                          (when tail
                            (concat sep (mapconcat #'identity tail sep)))))
                 (backend backend)
                 (key (mapconcat #'identity (cons key tail) sep))
                 (t (mapconcat #'identity tail sep))))))))

(defun clutch--spinner-start ()
  "Start the spinner timer if not already running."
  (unless clutch--spinner-timer
    (setq clutch--spinner-index 0)
    (setq clutch--spinner-timer
          (run-at-time 0 clutch--spinner-interval
                       #'clutch--spinner-tick))))

(defun clutch--spinner-stop ()
  "Stop the spinner timer and reset state."
  (when clutch--spinner-timer
    (cancel-timer clutch--spinner-timer)
    (setq clutch--spinner-timer nil)
    (setq clutch--spinner-index 0)))

(defun clutch--spinner-tick ()
  "Advance spinner frame and update mode-lines of busy buffers."
  (setq clutch--spinner-index
        (mod (1+ clutch--spinner-index)
             (length clutch--spinner-frames)))
  (let ((any-busy nil))
    (dolist (buf (buffer-list))
      (when (and (buffer-live-p buf)
                 (buffer-local-value 'clutch--executing-p buf))
        (setq any-busy t)
        (with-current-buffer buf
          (clutch--update-mode-line))))
    (if any-busy
        (redisplay)
      (clutch--spinner-stop))))

(defun clutch--spinner-string ()
  "Return the current spinner frame string, or nil when idle."
  (when clutch--spinner-timer
    (aref clutch--spinner-frames clutch--spinner-index)))

(defun clutch--update-mode-line ()
  "Update buffer-local execution UI with connection status."
  (let* ((base (if (derived-mode-p 'clutch-repl-mode) "clutch-repl" "clutch"))
         (spinner (clutch--spinner-string)))
    (setq mode-name
          (if (and clutch--executing-p spinner)
              (concat base " " (propertize spinner 'face 'success))
            base)))
  (when (derived-mode-p 'clutch-result-mode)
    (when (fboundp 'clutch--refresh-footer-timing)
      (clutch--refresh-footer-timing)))
  (when (derived-mode-p 'clutch-mode 'clutch-repl-mode)
    ;; Use :eval so line-number-display-width is recomputed on each redraw,
    ;; keeping alignment correct when display-line-numbers-mode is toggled.
    (setq header-line-format '((:eval (clutch--build-connection-header-line)))))
  (force-mode-line-update))

;;;; JDBC backend detection

(defun clutch--jdbc-backend-p (backend)
  "Return non-nil when BACKEND is handled by JDBC."
  (eq (plist-get (clutch-db-backend-feature backend) :require)
      'clutch-db-jdbc))

;;;; Password resolution and connection building

(defun clutch--auth-source-target (params)
  "Return a human-readable auth-source target string for PARAMS."
  (let ((user (plist-get params :user))
        (host (plist-get params :host))
        (port (plist-get params :port)))
    (cond
     ((and user host port) (format "%s@%s:%s" user host port))
     ((and user host) (format "%s@%s" user host))
     (host host)
     (t "the configured credential source"))))

(defun clutch--pass-entry-by-suffix (suffix)
  "Return the first pass entry path whose tail matches SUFFIX.
Matches e.g. `dev-mysql' against `mysql/dev-mysql'.
Returns nil when no matching entry is found or auth-source-pass is absent."
  (when (and (fboundp 'auth-source-pass-entries)
             (fboundp 'auth-source-pass-parse-entry))
    (let ((re (format "\\(^\\|/\\)%s$" (regexp-quote suffix))))
      (cl-find-if (lambda (entry) (string-match-p re entry))
                  (auth-source-pass-entries)))))

(defun clutch--resolve-pass-entry-password (entry)
  "Return the password from pass ENTRY.
Signal `user-error' when a matching pass entry exists but cannot be read."
  (when-let* ((path (clutch--pass-entry-by-suffix entry)))
    (let ((parsed (auth-source-pass-parse-entry path)))
      (cond
       ((null parsed)
        (user-error
         "Database password lookup failed for pass entry %s. Unlock pass/auth-source-pass and retry"
         path))
       ((not (assq 'secret parsed))
        (user-error
         "Database password lookup failed for pass entry %s because it does not contain a secret"
         path))
       (t
        (cdr (assq 'secret parsed)))))))

(defun clutch--auth-source-first-match (params target)
  "Return the first auth-source match for PARAMS targeting TARGET."
  (condition-case err
      (car (auth-source-search
            :host (plist-get params :host)
            :user (plist-get params :user)
            :port (plist-get params :port)
            :max 1))
    (error
     (user-error "Database password lookup failed via auth-source for %s: %s"
                 target
                 (error-message-string err)))))

(defun clutch--auth-source-secret-value (secret target)
  "Return auth-source SECRET for TARGET, or signal `user-error'."
  (cond
   ((null secret)
    (user-error
     "Database password lookup failed via auth-source for %s. The matching credential has no secret"
     target))
   ((functionp secret)
    (let ((value
           (condition-case secret-err
               (funcall secret)
             (error
              (user-error
               "Database password lookup failed via auth-source for %s: %s"
               target
               (error-message-string secret-err))))))
      (or value
          (user-error
           "Database password lookup failed via auth-source for %s. Unlock the credential store and retry"
           target))))
   (t secret)))

(defun clutch--resolve-auth-source-password (params)
  "Return a password from `auth-source' for PARAMS, or nil when absent.
Signal `user-error' when auth-source finds a credential but cannot
read its secret."
  (let ((target (clutch--auth-source-target params)))
    (when-let* ((found (clutch--auth-source-first-match params target)))
      (clutch--auth-source-secret-value (plist-get found :secret) target))))

(defun clutch--resolve-password (params)
  "Return the password for connection PARAMS.
Checks in order:
  1. :password key (non-empty string) — used as-is.
  2. :pass-entry key — suffix-matched against all pass entries, so
     \\='dev-mysql\\=' finds \\='mysql/dev-mysql\\='.  Automatically set to the
     connection name by callers; override in `clutch-connection-alist'.
  3. `auth-source-search' by :host/:user/:port (authinfo / pass).
Returns nil when nothing is found (caller should prompt if needed).
Signals `user-error' when a configured credential source matches but
cannot be read."
  (let ((pw    (plist-get params :password))
        (entry (plist-get params :pass-entry)))
    (cond
     ((and (stringp pw) (not (string-empty-p pw))) pw)
     (t
      (or (and entry (clutch--resolve-pass-entry-password entry))
          (clutch--resolve-auth-source-password params))))))

(defun clutch--canonicalize-connection-params (params)
  "Return PARAMS with public connection aliases normalized."
  (let ((has-tramp (plist-member params :tramp))
        (has-tramp-default-directory
         (plist-member params :tramp-default-directory))
        (tramp (plist-get params :tramp))
        (tramp-default-directory
         (plist-get params :tramp-default-directory)))
    (cond
     ((not has-tramp) params)
     ((and has-tramp-default-directory
           (not (equal tramp tramp-default-directory)))
      (user-error
       "Connection cannot set both :tramp and :tramp-default-directory"))
     (t
      (let ((out (cl-loop for (k v) on params by #'cddr
                          unless (eq k :tramp)
                          append (list k v))))
        (if has-tramp-default-directory
            out
          (plist-put out :tramp-default-directory tramp)))))))

(defun clutch--debug-connection-context (backend params)
  "Return a redacted connect context for BACKEND and PARAMS."
  (setq params (clutch--canonicalize-connection-params params))
  (let ((context nil))
    (when-let* ((user (plist-get params :user)))
      (setq context (plist-put context :user user)))
    (when-let* ((host (plist-get params :host)))
      (setq context (plist-put context :host host)))
    (when-let* ((port (plist-get params :port)))
      (setq context (plist-put context :port port)))
    (when-let* ((database (plist-get params :database)))
      (setq context (plist-put context :database database)))
    (when-let* ((display-name (plist-get params :display-name)))
      (setq context (plist-put context :display-name display-name)))
    (when-let* ((ssh-host (plist-get params :ssh-host)))
      (setq context (plist-put context :ssh-host ssh-host)))
    (when-let* ((tramp-default-directory
                 (plist-get params :tramp-default-directory)))
      (setq context (plist-put context :tramp-default-directory
                               tramp-default-directory)))
    (setq context (plist-put context :backend backend))
    context))

(defun clutch--connection-transport-kind (params)
  "Return the explicit transport kind requested by PARAMS, or nil."
  (setq params (clutch--canonicalize-connection-params params))
  (let* ((ssh-host (plist-get params :ssh-host))
         (tramp-default-directory
          (plist-get params :tramp-default-directory))
         (ssh (and (stringp ssh-host)
                   (not (string-empty-p ssh-host))))
         (tramp (and (stringp tramp-default-directory)
                     (not (string-empty-p tramp-default-directory))))
         (ssh-mode (plist-get params :ssh-tunnel)))
    (cond
     ((and ssh tramp)
      (user-error
       "Connection cannot combine :ssh-host with :tramp"))
     ((and ssh-mode (not ssh))
      (user-error "Connection :ssh-tunnel requires :ssh-host"))
     (ssh 'ssh)
     (tramp 'tramp))))

(defun clutch--ssh-tunnel-mode (params)
  "Return the SSH tunnel mode from PARAMS."
  (let ((mode (or (plist-get params :ssh-tunnel) 'always)))
    (unless (memq mode '(always direct-first))
      (user-error "Connection :ssh-tunnel must be always or direct-first"))
    mode))

(defun clutch--tramp-origin-compatible-p (params)
  "Return non-nil when PARAMS can use an inferred TRAMP origin."
  (and (not (eq (plist-get params :backend) 'sqlite))
       (not (plist-get params :url))
       (plist-get params :host)
       (plist-get params :port)))

(defun clutch--source-tramp-default-directory (&optional source-default-directory)
  "Return SOURCE-DEFAULT-DIRECTORY when it names a TRAMP context."
  (let ((dir (or source-default-directory default-directory)))
    (when (and (stringp dir)
               (file-remote-p dir))
      dir)))

(defun clutch--connection-origin-summary (params)
  "Return a compact connection identity for PARAMS origin prompts."
  (let ((host (plist-get params :host))
        (port (plist-get params :port))
        (database (or (plist-get params :database)
                      (plist-get params :sid))))
    (string-join
     (delq nil
           (list (and host
                      (if port
                          (format "%s:%s" host port)
                        host))
                 database))
     "/")))

(defun clutch--use-source-tramp-context-p (params tramp-default-directory)
  "Return non-nil when PARAMS should use TRAMP-DEFAULT-DIRECTORY."
  (pcase clutch-tramp-context-policy
    ('auto t)
    ('ask
     (y-or-n-p
      (format "Use TRAMP context %s for database connection%s? "
              (or (file-remote-p tramp-default-directory)
                  tramp-default-directory)
              (let ((summary (clutch--connection-origin-summary params)))
                (if (string-empty-p summary)
                    ""
                  (format " to %s" summary))))))
    (_ nil)))

(defun clutch--prepare-connection-origin-params
    (params &optional source-default-directory)
  "Return PARAMS with any command-source connection origin applied.
Explicit transports in PARAMS always win.  When PARAMS has no explicit
transport, `clutch-tramp-context-policy' controls whether the current TRAMP
SOURCE-DEFAULT-DIRECTORY is copied into :tramp-default-directory.  Unsupported
TRAMP methods are ignored for inference."
  (setq params (clutch--canonicalize-connection-params params))
  (let ((explicit-kind (clutch--connection-transport-kind params)))
    (if-let* ((tramp-default-directory
               (and (not explicit-kind)
                    (clutch--tramp-origin-compatible-p params)
                    (clutch--source-tramp-default-directory
                     source-default-directory)))
              ((clutch--tramp-forward-vector tramp-default-directory))
              ((clutch--use-source-tramp-context-p
                params tramp-default-directory)))
        (plist-put (copy-sequence params)
                   :tramp-default-directory tramp-default-directory)
      params)))

(defun clutch-prepare-connection-params
    (params &optional source-default-directory)
  "Return PARAMS prepared according to Clutch connection rules.
This normalizes public aliases such as `:tramp'.  When PARAMS has no explicit
transport, SOURCE-DEFAULT-DIRECTORY may provide a TRAMP origin according to
`clutch-tramp-context-policy'."
  (clutch--prepare-connection-origin-params params source-default-directory))

(defun clutch--carry-current-connection-origin (params)
  "Return PARAMS with the current buffer's inferred origin preserved.
Saved query consoles re-read their saved connection on `clutch-connect'.
When the live logical session was originally opened from a TRAMP context,
keep that origin unless the saved connection now specifies an explicit
transport."
  (setq params (clutch--canonicalize-connection-params params))
  (if (or (clutch--connection-transport-kind params)
          (not (clutch--tramp-origin-compatible-p params))
          (null clutch--connection-params))
      params
    (if-let* ((tramp-default-directory
               (plist-get clutch--connection-params :tramp-default-directory)))
        (plist-put (copy-sequence params)
                   :tramp-default-directory tramp-default-directory)
      params)))

(defun clutch--allocate-local-port ()
  "Return an available local TCP port for an SSH tunnel."
  (condition-case err
      (let* ((listener (make-network-process :name "clutch-ssh-port-reserve"
                                             :server t
                                             :host "127.0.0.1"
                                             :service t
                                             :family 'ipv4
                                             :noquery t))
             (port (process-contact listener :service)))
        (delete-process listener)
        port)
    (file-error
     (signal 'clutch-db-error
             (list (format "Cannot allocate a local port for the SSH tunnel: %s"
                           (error-message-string err)))))))

(defun clutch--ssh-tunnel-buffer-name (ssh-host)
  "Return the process buffer name for SSH-HOST."
  (format " *clutch-ssh %s*" ssh-host))

(defun clutch--ssh-prepare-buffer-name (ssh-host)
  "Return the interactive SSH prepare buffer name for SSH-HOST."
  (format "*clutch-ssh-prepare %s*" ssh-host))

(defun clutch--default-ssh-host ()
  "Return the current buffer's default SSH host alias, or nil."
  (let* ((params (or clutch--connection-params
                     (car-safe (clutch--connection-context clutch-connection))))
         (ssh-host (plist-get params :ssh-host)))
    (when (and (stringp ssh-host)
               (not (string-empty-p ssh-host)))
      ssh-host)))

(defun clutch--read-ssh-host-alias ()
  "Prompt for an SSH host alias from OpenSSH config."
  (let* ((default (clutch--default-ssh-host))
         (prompt (if default
                     (format "SSH host from ~/.ssh/config (%s): " default)
                   "SSH host from ~/.ssh/config: "))
         (ssh-host (read-string prompt nil nil default)))
    (if (string-empty-p ssh-host)
        (user-error "An SSH host alias is required")
      ssh-host)))

(defun clutch--ssh-buffer-output (buffer)
  "Return BUFFER contents as a trimmed string, or an empty string."
  (if (buffer-live-p buffer)
      (with-current-buffer buffer
        (string-trim (buffer-substring-no-properties (point-min) (point-max))))
    ""))

(defun clutch--ssh-output-last-line (output)
  "Return the last non-empty line from SSH OUTPUT."
  (when (and output
             (not (string-empty-p output)))
    (let ((lines (split-string output "\n" t "[ \t\r]+")))
      (car (last lines)))))

(defun clutch--ssh-diagnose-output (ssh-host output)
  "Return a user-facing diagnosis for SSH-HOST using SSH OUTPUT."
  (let* ((cleaned (string-trim (or output "")))
         (last-line (or (clutch--ssh-output-last-line cleaned)
                        "the ssh process exited before the tunnel became ready"))
         (case-fold-search t))
    (cond
     ((or (string-match-p "enter passphrase for key" cleaned)
          (string-match-p "incorrect passphrase" cleaned)
          (string-match-p "agent refused operation" cleaned)
          (string-match-p "sign_and_send_pubkey" cleaned))
      (format
       "the SSH key for %s is locked. Run M-x clutch-prepare-ssh-host or `ssh %s exit` once to unlock it"
       ssh-host ssh-host))
     ((or (string-match-p "host key verification failed" cleaned)
          (string-match-p "host identification has changed" cleaned)
          (string-match-p "are you sure you want to continue connecting" cleaned))
      (format
       "SSH host verification for %s needs attention. Run M-x clutch-prepare-ssh-host or `ssh %s exit` once to confirm the host key"
       ssh-host ssh-host))
     ((string-match-p "permission denied" cleaned)
      (format
       "SSH authentication to %s was rejected. Run M-x clutch-prepare-ssh-host once, or check the remote username and ~/.ssh/authorized_keys"
       ssh-host))
     ((or (string-match-p "could not resolve hostname" cleaned)
          (string-match-p "name or service not known" cleaned))
      (format "OpenSSH could not resolve host %s. Check the alias in ~/.ssh/config"
              ssh-host))
     ((or (string-match-p "connection refused" cleaned)
          (string-match-p "operation timed out" cleaned)
          (string-match-p "connection timed out" cleaned)
          (string-match-p "no route to host" cleaned)
          (string-match-p "network is unreachable" cleaned))
      (format "OpenSSH could not reach %s (%s)" ssh-host last-line))
     ((or (string-match-p "administratively prohibited" cleaned)
          (string-match-p "open failed" cleaned))
      (format "the remote side rejected SSH port forwarding via %s" ssh-host))
     ((string-empty-p cleaned)
      (format
       "OpenSSH could not use host %s in batch mode. Run M-x clutch-prepare-ssh-host or `ssh %s exit` once first"
       ssh-host ssh-host))
     (t last-line))))

(defun clutch--ssh-prepare-sentinel (proc _event)
  "Report completion state for SSH prepare PROC."
  (when (memq (process-status proc) '(exit signal))
    (let* ((ssh-host (process-get proc :clutch-ssh-host))
           (buffer (process-buffer proc))
           (buffer-name (and (buffer-live-p buffer) (buffer-name buffer))))
      (if (and (eq (process-status proc) 'exit)
               (zerop (process-exit-status proc)))
          (message "SSH host %s is ready for batch use" ssh-host)
        (message "SSH prepare for %s exited. If prompts completed, retry clutch-connect; otherwise inspect %s"
                 ssh-host
                 (or buffer-name "the SSH prepare buffer"))))))

(defun clutch--start-ssh-prepare-session (ssh-host)
  "Start an interactive SSH prepare session for SSH-HOST."
  (let* ((buffer-name (clutch--ssh-prepare-buffer-name ssh-host))
         (buffer (get-buffer-create buffer-name))
         (proc (get-buffer-process buffer)))
    (if (process-live-p proc)
        buffer
      (with-current-buffer buffer
        (let ((inhibit-read-only t))
          (erase-buffer)))
      (setq buffer (make-comint-in-buffer
                    (format "clutch-ssh-prepare-%s" ssh-host)
                    buffer
                    "ssh"
                    nil
                    ssh-host
                    "exit"))
      (setq proc (get-buffer-process buffer))
      (process-put proc :clutch-ssh-host ssh-host)
      (set-process-query-on-exit-flag proc nil)
      (set-process-sentinel proc #'clutch--ssh-prepare-sentinel)
      buffer)))

(defun clutch--ssh-prepare-session-live-p (buffer)
  "Return non-nil when BUFFER hosts a live SSH prepare process."
  (when-let* ((proc (and (buffer-live-p buffer)
                         (get-buffer-process buffer))))
    (process-live-p proc)))

(defun clutch--prepare-ssh-host-message (ssh-host buffer)
  "Display a status message after opening SSH-HOST prepare BUFFER."
  (if (clutch--ssh-prepare-session-live-p buffer)
      (message "Complete any SSH prompts in %s, then retry clutch-connect"
               (buffer-name buffer))
    (message "SSH host %s is ready for batch use" ssh-host)))

(defun clutch--ssh-tunnel-error (params buffer reason)
  "Signal a `clutch-db-error' for PARAMS using BUFFER and REASON."
  (let* ((ssh-host (plist-get params :ssh-host))
         (buffer-name (and (buffer-live-p buffer) (buffer-name buffer)))
         (summary (format "SSH tunnel to %s failed" ssh-host))
         (message (if buffer-name
                      (format "%s: %s. Inspect %s for SSH output"
                              summary reason buffer-name)
                    (format "%s: %s" summary reason))))
    (signal 'clutch-db-error
            (list message
                  (list :summary summary
                        :diag (list :raw-message message
                                    :context (list :ssh-host ssh-host
                                                   :ssh-buffer buffer-name
                                                   :host (plist-get params :host)
                                                   :port (plist-get params :port))))))))

(defun clutch--validate-network-forward-params (params transport-name)
  "Validate PARAMS for a structured TCP forward named TRANSPORT-NAME."
  (when (eq (plist-get params :backend) 'sqlite)
    (user-error
     "SQLite opens a local database file and does not support %s"
     transport-name))
  (when (plist-get params :url)
    (user-error
     "%s currently requires structured :host/:port params, not :url"
     transport-name))
  (unless (plist-get params :host)
    (user-error "%s requires :host for the remote database endpoint"
                        transport-name))
  (unless (plist-get params :port)
    (user-error "%s requires :port for the remote database endpoint"
                        transport-name)))

(defun clutch--ssh-local-port-open-p (port)
  "Return non-nil when localhost PORT accepts TCP connections."
  (condition-case nil
      (let ((probe (make-network-process :name "clutch-ssh-ready-probe"
                                         :host "127.0.0.1"
                                         :service port
                                         :family 'ipv4
                                         :noquery t)))
        (delete-process probe)
        t)
    (error nil)))

(defun clutch--wait-for-ssh-tunnel (proc port params buffer timeout)
  "Wait until PROC forwards localhost PORT or signal a tunnel error.
PARAMS describe the original connection, BUFFER captures SSH output,
and TIMEOUT is the maximum wait in seconds."
  (let ((deadline (+ (float-time) (max 1 timeout))))
    (while (and (process-live-p proc)
                (< (float-time) deadline)
                (not (clutch--ssh-local-port-open-p port)))
      (accept-process-output proc clutch--ssh-tunnel-ready-poll-interval))
    (cond
     ((clutch--ssh-local-port-open-p port) t)
     ((process-live-p proc)
      (delete-process proc)
      (clutch--ssh-tunnel-error params buffer "the local forward did not become ready in time"))
     (t
      (let* ((ssh-host (plist-get params :ssh-host))
             (output (clutch--ssh-buffer-output buffer))
             (reason (clutch--ssh-diagnose-output ssh-host output)))
        (clutch--ssh-tunnel-error params buffer reason))))))

(defun clutch--start-ssh-tunnel (params)
  "Start an SSH tunnel for PARAMS using the user's OpenSSH config."
  (unless (executable-find "ssh")
    (user-error "SSH tunnels require the OpenSSH client executable `ssh'"))
  (clutch--validate-network-forward-params params "SSH tunnels")
  (let* ((ssh-host (plist-get params :ssh-host))
         (local-port (clutch--allocate-local-port))
         (buffer (get-buffer-create (clutch--ssh-tunnel-buffer-name ssh-host)))
         (timeout (or (plist-get params :connect-timeout)
                      clutch-connect-timeout-seconds))
         (proc nil))
    (with-current-buffer buffer
      (erase-buffer))
    (setq proc (make-process
                :name (format "clutch-ssh-%s" ssh-host)
                :buffer buffer
                :command (list "ssh"
                               "-N"
                               "-o" "BatchMode=yes"
                               "-o" "ExitOnForwardFailure=yes"
                               "-L" (format "127.0.0.1:%d:%s:%s"
                                            local-port
                                            (plist-get params :host)
                                            (plist-get params :port))
                               ssh-host)
                :coding 'utf-8
                :noquery t))
    (set-process-query-on-exit-flag proc nil)
    (clutch--wait-for-ssh-tunnel
     proc local-port (plist-put (copy-sequence params) :ssh-host ssh-host)
     buffer timeout)
    (list :kind 'ssh
          :process proc
          :local-port local-port
          :buffer buffer
          :ssh-host ssh-host)))

(defun clutch--tramp-forward-buffer-name (tramp-default-directory host port)
  "Return the TRAMP TCP forward buffer name for TRAMP-DEFAULT-DIRECTORY HOST PORT."
  (format " *clutch-tramp %s %s:%s*"
          (or (file-remote-p tramp-default-directory)
              tramp-default-directory)
          host port))

(defun clutch--tramp-ssh-target (vec)
  "Return the ssh target string for TRAMP VEC."
  (let ((host (tramp-file-name-host vec))
        (user (tramp-file-name-user vec)))
    (unless (and (stringp host) (not (string-empty-p host)))
      (user-error "TRAMP forwarding requires an ssh host"))
    (if (and (stringp user) (not (string-empty-p user)))
        (format "%s@%s" user host)
      host)))

(defun clutch--tramp-proxyjump-target (vec)
  "Return the OpenSSH ProxyJump target string for TRAMP VEC."
  (let ((target (clutch--tramp-ssh-target vec))
        (port (tramp-file-name-port vec)))
    (if port
        (format "%s:%s" target port)
      target)))

(defun clutch--tramp-dissect-file-name (tramp-default-directory)
  "Dissect TRAMP-DEFAULT-DIRECTORY for connection-origin parsing.
Clutch can map tramp-rpc's `rpc' method to OpenSSH without using tramp-rpc
file handlers, so provide a local method entry when tramp-rpc is not loaded."
  (let ((tramp-methods
         (if (assoc "rpc" tramp-methods)
             tramp-methods
           (cons '("rpc" (tramp-login-args (("%h")))) tramp-methods))))
    (tramp-dissect-file-name tramp-default-directory)))

(defun clutch--tramp-hop-vectors (hop)
  "Return ssh-like TRAMP vectors parsed from HOP."
  (when (and (stringp hop) (not (string-empty-p hop)))
    (mapcar
     (lambda (hop-part)
       (clutch--tramp-dissect-file-name
        (concat tramp-prefix-format hop-part tramp-postfix-host-format)))
     (split-string hop tramp-postfix-hop-regexp 'omit))))

(defun clutch--tramp-forward-vector (tramp-default-directory)
  "Return supported TRAMP forward vector for TRAMP-DEFAULT-DIRECTORY, or nil."
  (let* ((vec (clutch--tramp-dissect-file-name tramp-default-directory))
         (method (tramp-file-name-method vec)))
    (when (or (member method clutch--tramp-ssh-forward-methods)
              (member method clutch--tramp-container-forward-methods))
      vec)))

(defun clutch--tramp-proxyjump (vec)
  "Return OpenSSH ProxyJump value for VEC hops, or nil."
  (let ((hops (clutch--tramp-hop-vectors (tramp-file-name-hop vec))))
    (when hops
      (dolist (hop-vec hops)
        (unless (member (tramp-file-name-method hop-vec)
                        clutch--tramp-ssh-forward-methods)
          (user-error
           "TRAMP forwarding does not support %s hops"
           (tramp-file-name-method hop-vec))))
      (mapconcat #'clutch--tramp-proxyjump-target hops ","))))

(defun clutch--tramp-rpc-controlmaster-options (vec)
  "Return OpenSSH options for reusing tramp-rpc ControlMaster for VEC."
  (when (string= (tramp-file-name-method vec) "rpc")
    (cond
     ((fboundp 'tramp-rpc-controlmaster-options)
      (tramp-rpc-controlmaster-options vec))
     ((and (boundp 'tramp-rpc-use-controlmaster)
           tramp-rpc-use-controlmaster
           (not clutch--tramp-rpc-controlmaster-warning-reported))
      (setq clutch--tramp-rpc-controlmaster-warning-reported t)
      (display-warning
       'clutch
       "tramp-rpc is too old to expose ControlMaster SSH options; not reusing its ControlMaster"
       :warning)
      nil))))

(defun clutch--start-tramp-ssh-forward (params)
  "Start an OpenSSH local forward for ssh-like TRAMP PARAMS."
  (unless (executable-find "ssh")
    (user-error "TRAMP forwarding requires the OpenSSH client executable `ssh'"))
  (let* ((tramp-default-directory (plist-get params :tramp-default-directory))
         (vec (clutch--tramp-dissect-file-name tramp-default-directory))
         (method (tramp-file-name-method vec))
         (host (plist-get params :host))
         (port (plist-get params :port))
         (local-port (clutch--allocate-local-port))
         (target (clutch--tramp-ssh-target vec))
         (ssh-port (tramp-file-name-port vec))
         (proxyjump (clutch--tramp-proxyjump vec))
         (buffer (get-buffer-create
                  (clutch--tramp-forward-buffer-name
                   tramp-default-directory host port)))
         (timeout (or (plist-get params :connect-timeout)
                      clutch-connect-timeout-seconds))
         proc)
    (unless (member method clutch--tramp-ssh-forward-methods)
      (user-error
       "TRAMP forwarding currently supports ssh-like TRAMP directories such as /ssh:host:/path/ or /rpc:host:/path/"))
    (with-current-buffer buffer
      (erase-buffer))
    (setq proc (make-process
                :name (format "clutch-tramp-ssh-%s:%s" host port)
                :buffer buffer
                :command (append
                          (list "ssh"
                                "-N"
                                "-o" "BatchMode=yes"
                                "-o" "ExitOnForwardFailure=yes"
                                "-L" (format "127.0.0.1:%d:%s:%s"
                                             local-port host port))
                          (clutch--tramp-rpc-controlmaster-options vec)
                          (when proxyjump
                            (list "-J" proxyjump))
                          (when ssh-port
                            (list "-p" (format "%s" ssh-port)))
                          (list target))
                :coding 'utf-8
                :noquery t))
    (set-process-query-on-exit-flag proc nil)
    (clutch--wait-for-ssh-tunnel
     proc local-port (plist-put (copy-sequence params) :ssh-host target)
     buffer timeout)
    (list :kind 'tramp
          :process proc
          :local-port local-port
          :buffer buffer
          :tramp-default-directory tramp-default-directory)))

(defun clutch--tramp-container-command (vec host port)
  "Return the process command for container TRAMP VEC to reach HOST PORT."
  (let* ((method (tramp-file-name-method vec))
         (runtime (pcase method
                    ("docker" "docker")
                    ("podman" "podman")
                    (_ (user-error
                        "Container TRAMP forwarding does not support %s"
                        method))))
         (container (tramp-file-name-host vec))
         (user (tramp-file-name-user vec))
         (exec-command (append
                        (list runtime "exec" "-i")
                        (when (and (stringp user)
                                   (not (string-empty-p user)))
                          (list "-u" user))
                        (list container "sh" "-lc"
                              clutch--container-relay-script
                              "clutch-container-relay"
                              (format "%s" host)
                              (format "%s" port))))
         (hops (clutch--tramp-hop-vectors (tramp-file-name-hop vec))))
    (unless (and (stringp container) (not (string-empty-p container)))
      (user-error "Container TRAMP forwarding requires a container name"))
    (if hops
        (progn
          (unless (executable-find "ssh")
            (user-error
             "Container TRAMP forwarding through SSH requires the OpenSSH client executable `ssh'"))
          (dolist (hop-vec hops)
            (unless (member (tramp-file-name-method hop-vec)
                            clutch--tramp-ssh-forward-methods)
              (user-error
               "Container TRAMP forwarding does not support %s hops"
               (tramp-file-name-method hop-vec))))
          (let* ((target-vec (car (last hops)))
                 (proxyjump-vecs (butlast hops))
                 (proxyjump
                  (when proxyjump-vecs
                    (mapconcat #'clutch--tramp-proxyjump-target
                               proxyjump-vecs ",")))
                 (ssh-port (tramp-file-name-port target-vec)))
            (append
             (list "ssh" "-T" "-o" "BatchMode=yes")
             (clutch--tramp-rpc-controlmaster-options target-vec)
             (when proxyjump
               (list "-J" proxyjump))
             (when ssh-port
               (list "-p" (format "%s" ssh-port)))
             (list (clutch--tramp-ssh-target target-vec))
             (mapcar #'shell-quote-argument exec-command))))
      (unless (executable-find runtime)
        (user-error
         "Container TRAMP forwarding requires the `%s' executable" runtime))
      exec-command)))

(defun clutch--container-forward-register-child (listener child)
  "Register CHILD so deleting LISTENER can stop active relay processes."
  (process-put listener :clutch-container-children
               (cons child
                     (delq child
                           (process-get listener
                                        :clutch-container-children)))))

(defun clutch--container-forward-stop-peer (proc peer-key)
  "Delete PROC's PEER-KEY process when it is still live."
  (when-let* ((peer (process-get proc peer-key)))
    (when (process-live-p peer)
      (delete-process peer))))

(defun clutch--container-forward-relay-filter (relay string)
  "Send STRING bytes from RELAY to its client connection."
  (when-let* ((client (process-get relay :clutch-container-client)))
    (when (process-live-p client)
      (process-send-string client string))))

(defun clutch--container-forward-client-filter (client string)
  "Send STRING bytes from CLIENT to its container relay process."
  (when-let* ((relay (process-get client :clutch-container-relay)))
    (when (process-live-p relay)
      (process-send-string relay string))))

(defun clutch--container-forward-relay-sentinel (relay _event)
  "Close RELAY's client connection when the relay exits."
  (clutch--container-forward-stop-peer relay :clutch-container-client))

(defun clutch--container-forward-client-sentinel (client event)
  "Start or stop the container relay for CLIENT according to EVENT."
  (if (string-prefix-p "open " event)
      (let* ((command (process-get client :clutch-container-command))
             (buffer (process-get client :clutch-container-buffer))
             (listener (process-get client :clutch-container-listener))
             (relay (make-process
                     :name (format "%s relay" (process-name client))
                     :buffer buffer
                     :command command
                     :connection-type 'pipe
                     :coding 'no-conversion
                     :filter #'clutch--container-forward-relay-filter
                     :sentinel #'clutch--container-forward-relay-sentinel
                     :stderr buffer
                     :noquery t)))
        (set-process-coding-system client 'no-conversion 'no-conversion)
        (set-process-query-on-exit-flag relay nil)
        (process-put client :clutch-container-relay relay)
        (process-put relay :clutch-container-client client)
        (when listener
          (clutch--container-forward-register-child listener client)
          (clutch--container-forward-register-child listener relay)))
    (clutch--container-forward-stop-peer client :clutch-container-relay)))

(defun clutch--start-tramp-container-forward (params)
  "Start a local TCP relay for container TRAMP PARAMS."
  (let* ((tramp-default-directory (plist-get params :tramp-default-directory))
         (vec (clutch--tramp-dissect-file-name tramp-default-directory))
         (method (tramp-file-name-method vec))
         (host (plist-get params :host))
         (port (plist-get params :port))
         (buffer (get-buffer-create
                  (clutch--tramp-forward-buffer-name
                   tramp-default-directory host port)))
         (command (clutch--tramp-container-command vec host port))
         listener local-port)
    (unless (member method clutch--tramp-container-forward-methods)
      (user-error
       "Container TRAMP forwarding requires /docker: or /podman:"))
    (with-current-buffer buffer
      (erase-buffer))
    (setq listener
          (make-network-process
           :name (format "clutch-tramp-container-%s:%s" host port)
           :buffer buffer
           :server t
           :host "127.0.0.1"
           :service t
           :family 'ipv4
           :coding 'no-conversion
           :filter #'clutch--container-forward-client-filter
           :sentinel #'clutch--container-forward-client-sentinel
           :noquery t))
    (setq local-port (process-contact listener :service))
    (set-process-query-on-exit-flag listener nil)
    (process-put listener :clutch-container-command command)
    (process-put listener :clutch-container-buffer buffer)
    (process-put listener :clutch-container-listener listener)
    (process-put listener :clutch-container-children nil)
    (list :kind 'tramp
          :process listener
          :local-port local-port
          :buffer buffer
          :tramp-default-directory tramp-default-directory)))

(defun clutch--start-tramp-tcp-forward (params)
  "Start a local TCP forward for TRAMP PARAMS."
  (clutch--validate-network-forward-params params "TRAMP forwarding")
  (let ((tramp-default-directory (plist-get params :tramp-default-directory)))
    (unless (and (stringp tramp-default-directory)
                 (file-remote-p tramp-default-directory))
      (user-error
       "TRAMP forwarding requires :tramp-default-directory to be a remote TRAMP directory"))
    (let* ((vec (clutch--tramp-dissect-file-name tramp-default-directory))
           (method (tramp-file-name-method vec)))
      (cond
       ((member method clutch--tramp-ssh-forward-methods)
        (clutch--start-tramp-ssh-forward params))
       ((member method clutch--tramp-container-forward-methods)
        (clutch--start-tramp-container-forward params))
       (t
        (user-error
         (concat
          "TRAMP forwarding supports ssh-like paths such as /ssh:host:/path/ "
          "or /rpc:host:/path/, and container paths such as "
          "/docker:container:/path/ or /podman:container:/path/")))))))

(defun clutch--tcp-endpoint-open-p (host port timeout)
  "Return non-nil when HOST:PORT accepts a TCP connection within TIMEOUT."
  (let (proc)
    (condition-case nil
        (unwind-protect
            (progn
              (setq proc
                    (make-network-process
                     :name "clutch-direct-probe"
                     :host host
                     :service port
                     :nowait t
                     :noquery t))
              (let ((deadline (+ (float-time) timeout)))
                (while (and (eq (process-status proc) 'connect)
                            (< (float-time) deadline))
                  (accept-process-output
                   proc clutch--ssh-tunnel-ready-poll-interval)))
              (eq (process-status proc) 'open))
          (when (and proc (process-live-p proc))
            (delete-process proc)))
      (file-error nil)
      (error nil))))

(defun clutch--prepare-connect-params (params)
  "Return `(CONNECT-PARAMS TRANSPORT)' for PARAMS.
When PARAMS request a transport, CONNECT-PARAMS targets the local forwarded
port and TRANSPORT contains the live process metadata."
  (setq params (clutch--canonicalize-connection-params params))
  (if-let* ((kind (clutch--connection-transport-kind params)))
      (if (and (eq kind 'ssh)
               (eq (clutch--ssh-tunnel-mode params) 'direct-first)
               (progn
                 (clutch--validate-network-forward-params params "SSH tunnels")
                 (clutch--tcp-endpoint-open-p
                  (plist-get params :host)
                  (plist-get params :port)
                  clutch--ssh-direct-first-probe-timeout)))
          (list params nil)
        (let* ((transport (pcase kind
                            ('ssh (clutch--start-ssh-tunnel params))
                            ('tramp (clutch--start-tramp-tcp-forward params))))
               (connect-params (copy-sequence params)))
          (setq connect-params (plist-put connect-params :host "127.0.0.1"))
          (setq connect-params (plist-put connect-params :port
                                          (plist-get transport :local-port)))
          (list connect-params transport)))
    (list params nil)))

(defun clutch--make-connection-error-details (params err)
  "Return structured error details for a failed connection attempt.
PARAMS describe the attempted connection and ERR is the original
signaled condition."
  (let* ((message (or (cadr err) (error-message-string err)))
         (backend (clutch--backend-key-from-params params))
         (details (copy-tree (nth 2 err)))
         (diag (copy-tree (plist-get details :diag)))
         (context (copy-tree (plist-get diag :context)))
         (default-context (clutch--debug-connection-context backend params)))
    (unless details
      (setq details (list :summary (clutch--humanize-db-error message))))
    (unless (plist-member details :backend)
      (setq details (plist-put details :backend backend)))
    (unless (plist-get details :summary)
      (setq details (plist-put details :summary
                               (clutch--humanize-db-error message))))
    (unless diag
      (setq diag (list :raw-message message)))
    (unless (plist-get diag :raw-message)
      (setq diag (plist-put diag :raw-message message)))
    (cl-loop for (key val) on default-context by #'cddr
             unless (plist-member context key)
             do (setq context (plist-put context key val)))
    (setq diag (plist-put diag :context context))
    (plist-put details :diag diag)))

(defun clutch--materialize-connection-params (params)
  "Return effective connection PARAMS with resolved credentials included.
The returned plist keeps the original backend-facing keys, but fills in the
password that `clutch--resolve-password' produced so later reconnects reuse the
same credentials as the successful foreground connection."
  (setq params (clutch--canonicalize-connection-params params))
  (let* ((backend (or (plist-get params :backend)
                      (user-error "Connection params require :backend")))
         (password (clutch--resolve-password params)))
    (when (and (clutch--jdbc-backend-p backend)
               (plist-get params :pass-entry)
               (null password))
      (user-error
       (concat "No password resolved for JDBC connection %s (:pass-entry %s). "
               "Enable auth-source-pass/auth-source, or set :password explicitly")
       backend
       (plist-get params :pass-entry)))
    (if password
        (plist-put (copy-sequence params) :password password)
      params)))

(defun clutch--build-conn (params)
  "Connect to a database using PARAMS, resolving the password via auth-source.
Returns a live connection object or signals a `user-error'."
  (setq params (clutch--canonicalize-connection-params params))
  (let* ((effective-params params)
         (backend (plist-get params :backend))
         (transport nil))
    (condition-case err
        (progn
          (setq effective-params (clutch--materialize-connection-params params))
          (setq backend (plist-get effective-params :backend))
          (let* ((prepared (clutch--prepare-connect-params effective-params))
                 (connect-params (car prepared))
                 (password (plist-get connect-params :password))
                 (db-params (cl-loop for (k v) on connect-params by #'cddr
                                     unless (memq k '(:sql-product :backend :password :pass-entry
                                                                   :ssh-host
                                                                   :ssh-tunnel
                                                                   :tramp
                                                                   :tramp-default-directory))
                                     append (list k v)))
                 (db-params (if password
                                (append db-params (list :password password))
                              db-params)))
            (setq transport (cadr prepared))
            (let ((conn (clutch-db-connect backend db-params)))
              (clutch--remember-connection-transport conn effective-params transport)
              (when clutch-debug-mode
                (clutch--remember-debug-event
                 :connection conn
                 :op "connect"
                 :phase "success"
                 :backend backend
                 :summary (condition-case nil
                              (format "Connected to %s" (clutch--connection-key conn))
                            (error "Connected"))
                 :context (clutch--debug-connection-context backend effective-params)))
              conn)))
      (clutch-db-error
       (when transport
         (clutch--stop-connection-transport transport))
       (clutch--remember-problem-record
        :buffer (current-buffer)
        :problem (clutch--make-connection-error-details effective-params err))
       (let ((message (clutch--humanize-db-error
                       (or (car (cdr err))
                           (error-message-string err)))))
         (when clutch-debug-mode
           (clutch--remember-debug-event
            :op "connect"
            :phase "error"
            :backend backend
            :summary message
            :context (clutch--debug-connection-context backend effective-params)))
         (user-error "%s"
                              (clutch--debug-workflow-message message)))))))

(defun clutch-open-connection (params)
  "Open a database connection from PARAMS using Clutch connection rules.
PARAMS must include `:backend' and backend endpoint keys.  It may also include
Clutch connection keys such as `:ssh-host', `:tramp',
`:tramp-default-directory', `:pass-entry', and `:sql-product'.  The caller owns
the returned connection and should close it with `clutch-db-disconnect'.  Call
`clutch-prepare-connection-params' first when the current command source should
be allowed to supply TRAMP context."
  (clutch--build-conn params))

(defun clutch--inject-entry-name (params name)
  "Return PARAMS with :pass-entry defaulting to NAME.
Leaves PARAMS unchanged when :password or :pass-entry is already set."
  (if (or (plist-get params :pass-entry) (plist-get params :password))
      params
    (append params (list :pass-entry name))))

(defun clutch--saved-connection-params (name)
  "Return saved connection params for NAME, or nil when missing."
  (when-let* ((params (cdr (assoc name clutch-connection-alist))))
    (clutch--inject-entry-name params name)))

(defun clutch--normalize-sqlite-database-file (file)
  "Return canonical SQLite database FILE for connection identity."
  (if (string= file ":memory:")
      file
    (expand-file-name file)))

(defun clutch--read-sqlite-file-params ()
  "Read an ad hoc SQLite database file and return connection params."
  (list :backend 'sqlite
        :database (clutch--normalize-sqlite-database-file
                   (read-file-name "SQLite database file: " nil nil t))))

(defun clutch--read-manual-connection-params (&optional sqlite-file)
  "Prompt for a new connection plist.
When SQLITE-FILE is non-nil, SQLite reads a database file path instead of a
raw database string."
  (let* ((backend (intern
                   (let ((completion-extra-properties
                          '(:affixation-function clutch--backend-candidates-affixation)))
                     (completing-read
                      "Backend: "
                      (mapcar #'symbol-name (clutch--manual-backend-choices))
                      nil t nil nil "mysql")))))
    (if (eq backend 'sqlite)
        (if sqlite-file
            (clutch--read-sqlite-file-params)
          (list :backend 'sqlite
                :database (read-string "Database (:memory:): " nil nil ":memory:")))
      (let* ((port-default (clutch-db-backend-default-port backend))
             (host (read-string "Host (127.0.0.1): " nil nil "127.0.0.1"))
             (port (if port-default
                       (read-number (format "Port (%d): " port-default)
                                    port-default)
                     (read-number "Port: ")))
             (user (read-string "User: "))
             (ssh-host (read-string "SSH host from ~/.ssh/config (optional): "))
             (manual-params (append (list :backend backend
                                          :host host :port port :user user)
                                    (unless (string-empty-p ssh-host)
                                      (list :ssh-host ssh-host))))
             (pw (or (clutch--resolve-password manual-params)
                     (read-passwd "Password: ")))
             (db (read-string "Database (optional): ")))
        (append manual-params
                (list :password pw
                      :database (unless (string-empty-p db) db)))))))

(defun clutch--connect-params-for-current-buffer ()
  "Return connection params appropriate for the current buffer."
  (cond
   (clutch--console-ad-hoc-params
    clutch--console-ad-hoc-params)
   (clutch--console-name
    (clutch--carry-current-connection-origin
     (or (clutch--saved-connection-params clutch--console-name)
         (user-error "Saved connection %s for this query console no longer exists"
                             clutch--console-name))))
   (t
    (clutch--read-connection-params))))

(defun clutch--read-connection-params ()
  "Prompt the user for connection parameters and return a params plist.
Offers saved connections from `clutch-connection-alist' when non-empty,
otherwise prompts for :backend first, then for the backend-specific
connection parameters.
The password is resolved via `auth-source' before falling back to `read-passwd'."
  (clutch--ensure-clutch-loaded)
  (if clutch-connection-alist
      (let* ((name (let ((completion-extra-properties
                          '(:affixation-function clutch--connection-candidates-affixation)))
                     (completing-read "Connection: "
                                      (mapcar #'car clutch-connection-alist)
                                      nil t))))
        (clutch--saved-connection-params name))
    (clutch--read-manual-connection-params)))

;;;; Interactive connect/disconnect

;;;###autoload
(defun clutch-connect ()
  "Connect to a database server interactively.
If `clutch-connection-alist' is non-empty, offer saved connections via
  `completing-read'.  Otherwise prompt for each parameter.
The password is resolved via `auth-source' when not in the connection
params; see `clutch-connection-alist' for details."
  (interactive)
  (let ((old-conn clutch-connection)
        (old-live-p (clutch--connection-alive-p clutch-connection)))
    (when old-live-p
      (clutch--confirm-disconnect-transaction-loss
       old-conn
       "Uncommitted changes will be lost.  Disconnect? "))
    (let* ((source-default-directory default-directory)
           (params  (clutch-prepare-connection-params
                     (clutch--connect-params-for-current-buffer)
                     source-default-directory))
           (effective-params (clutch--materialize-connection-params params))
           (product (clutch--effective-sql-product effective-params))
           (conn    (clutch--build-conn params)))
      (when old-live-p
        (clutch--do-disconnect old-conn))
      (unless (clutch--connection-alive-p conn)
        (setq conn (clutch--build-conn params)))
      (clutch--activate-current-buffer-connection conn effective-params product)
      (message "Connected to %s" (clutch--connection-key conn)))))

;;;###autoload
(defun clutch-prepare-ssh-host (&optional ssh-host)
  "Open an interactive SSH session to SSH-HOST for host/key setup.
This is useful before `clutch-connect' when a host alias in `~/.ssh/config'
still needs an initial passphrase entry or host-key confirmation."
  (interactive (list (clutch--read-ssh-host-alias)))
  (unless (executable-find "ssh")
    (user-error "SSH preparation requires the OpenSSH client executable `ssh'"))
  (let* ((ssh-host (or ssh-host
                       (clutch--read-ssh-host-alias)))
         (buffer (clutch--start-ssh-prepare-session ssh-host)))
    (pop-to-buffer buffer)
    (clutch--prepare-ssh-host-message ssh-host buffer)))

;;;###autoload
(defun clutch-disconnect ()
  "Disconnect from the current database server."
  (interactive)
  (when (clutch--connection-alive-p clutch-connection)
    (clutch--confirm-disconnect-transaction-loss
     clutch-connection
     "Uncommitted changes will be lost.  Disconnect? ")
    (clutch--do-disconnect clutch-connection)
    (message "Disconnected"))
  (setq clutch-connection nil)
  (when clutch--console-name
    (clutch--update-console-buffer-name))
  (clutch--update-mode-line))

(defun clutch--invalidate-derived-buffers (conn)
  "Nil out `clutch-connection' in all non-current buffers sharing CONN.
Also refreshes their mode-line/header-line to reflect the disconnected state."
  (dolist (buf (buffer-list))
    (when (and (buffer-live-p buf)
               (not (eq buf (current-buffer)))
               (eq (buffer-local-value 'clutch-connection buf) conn))
      (with-current-buffer buf
        (setq-local clutch-connection nil)
        (force-mode-line-update)))))

(defun clutch--do-disconnect (conn)
  "Perform full disconnect sequence for CONN.
Marks DML results, invalidates derived buffers, clears transaction
state, and disconnects the underlying connection."
  (clutch--mark-dml-results-connection-closed conn)
  (clutch--invalidate-derived-buffers conn)
  (clutch--clear-tx-dirty conn)
  (when clutch-debug-mode
    (clutch--remember-debug-event
     :connection conn
     :op "disconnect"
     :phase "success"
     :backend (clutch--backend-key-from-conn conn)
     :summary (condition-case nil
                  (format "Disconnected from %s" (clutch--connection-key conn))
                (error "Disconnected"))))
  (clutch--forget-problem-record nil conn)
  (unwind-protect
      (clutch-db-disconnect conn)
    (clutch--release-connection-transport conn)))

(defun clutch--disconnect-on-kill ()
  "Disconnect the connection owned by this buffer.
Invalidates all derived buffers that share the same connection.
Does nothing in indirect SQL buffers (`clutch--indirect-mode')."
  (when (and (not (bound-and-true-p clutch--indirect-mode))
             (clutch--connection-alive-p clutch-connection))
    (clutch--confirm-disconnect-transaction-loss
     clutch-connection
     "Uncommitted changes will be lost.  Kill buffer? ")
    (clutch--do-disconnect clutch-connection)))

;;;; Transaction commands

;;;###autoload
(defun clutch-commit ()
  "Commit the current transaction."
  (interactive)
  (clutch--ensure-connection)
  (unless (clutch--manual-commit-supported-p clutch-connection)
    (user-error "Manual commit is not supported by this connection"))
  (unless (clutch-db-manual-commit-p clutch-connection)
    (user-error "Connection is in autocommit mode"))
  (clutch-db-commit clutch-connection)
  (clutch--mark-dml-results-committed clutch-connection)
  (clutch--clear-tx-dirty clutch-connection)
  (message "Transaction committed"))

;;;###autoload
(defun clutch-rollback ()
  "Roll back the current transaction."
  (interactive)
  (clutch--ensure-connection)
  (unless (clutch--manual-commit-supported-p clutch-connection)
    (user-error "Manual commit is not supported by this connection"))
  (unless (clutch-db-manual-commit-p clutch-connection)
    (user-error "Connection is in autocommit mode"))
  (clutch-db-rollback clutch-connection)
  (clutch--mark-dml-results-rolled-back clutch-connection)
  (clutch--clear-tx-dirty clutch-connection)
  (message "Transaction rolled back"))

;;;###autoload
(defun clutch-toggle-auto-commit ()
  "Toggle auto-commit mode for the current connection.
When switching from manual-commit to auto-commit, the backend finishes
any open transaction according to its own semantics."
  (interactive)
  (clutch--ensure-connection)
  (unless (clutch--manual-commit-supported-p clutch-connection)
    (user-error "Manual commit is not supported by this connection"))
  (let ((manual-now (clutch-db-manual-commit-p clutch-connection)))
    (when (and manual-now (clutch--tx-dirty-p clutch-connection))
      (user-error "Cannot toggle: commit or roll back staged changes first"))
    (clutch-db-set-auto-commit clutch-connection manual-now)
    (when manual-now
      (clutch--clear-tx-dirty clutch-connection))
    (clutch--update-mode-line)
    (message "Auto-commit %s" (if manual-now "enabled" "disabled"))))

(provide 'clutch-connection)
;;; clutch-connection.el ends here
