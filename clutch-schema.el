;;; clutch-schema.el --- Schema refresh and metadata caches -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Schema refresh services, status, and metadata cache state.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'clutch-diagnostics)
(require 'subr-x)

(defcustom clutch-schema-refresh-idle-delay-seconds 0.5
  "Idle delay before automatic schema cache refresh after connect.
A small non-zero delay keeps the first query responsive for native backends
whose schema refresh runs on the foreground connection.  Manual schema
refresh commands still start immediately."
  :type 'number
  :group 'clutch)

(defcustom clutch-schema-cache-install-batch-size 500
  "Maximum number of schema entries to install per idle slice.
Large schema snapshots are installed incrementally to keep Emacs responsive
after async metadata refreshes."
  :type 'natnum
  :group 'clutch)

(defvar clutch--schema-cache (make-hash-table :test 'eq)
  "Global schema cache keyed by connection object identity.")

(defvar clutch--table-metadata-cache (make-hash-table :test 'eq)
  "Table metadata and load status keyed by connection and table identity.")

(defvar clutch--column-details-queue-cache (make-hash-table :test 'eq)
  "Per-connection queue of tables waiting for async column-detail fetch.")

(defvar clutch--column-details-active-cache (make-hash-table :test 'eq)
  "Per-connection active async column-detail fetch as (TABLE . TICKET).")

(defvar clutch--metadata-ticket-counter 0
  "Monotonic counter used to reject stale async table metadata callbacks.")

(defvar clutch--help-doc-cache (make-hash-table :test 'eq)
  "Cache for live function docs fetched from the database server.")

(defvar clutch--schema-install-timers (make-hash-table :test 'eq)
  "Idle timers finishing large schema installs keyed by connection identity.")

(defvar clutch--schema-status-cache (make-hash-table :test 'eq)
  "Schema refresh status cache keyed by connection object identity.")

(defvar clutch--schema-refresh-ticket-counter 0
  "Monotonic counter used to reject stale async schema refreshes.")

(defvar clutch--schema-refresh-tickets (make-hash-table :test 'eq)
  "Latest schema refresh ticket keyed by connection object identity.")

(defvar clutch--schema-cache-updated-hook nil
  "Hook run when a connection's schema cache lifecycle changes.
Functions receive CONN and STATE, where STATE is `invalidated' or `ready'.")

(defvar clutch--metadata-state-changed-hook nil
  "Hook run after metadata state changes for a connection.
Functions receive the connection object as their sole argument.")

(defvar clutch--table-metadata-updated-hook nil
  "Hook run after table-scoped metadata is updated.
Functions receive CONN, TABLE, and KIND.")

(defconst clutch--schema-dependent-cache-symbols
  '(clutch--table-metadata-cache
    clutch--column-details-queue-cache
    clutch--column-details-active-cache
    clutch--help-doc-cache)
  "Metadata cache variables invalidated when schema cache changes.")

(defvar clutch-connection)
(defvar clutch--oracle-i18n-warning-shown nil
  "Non-nil after showing the Oracle orai18n completion warning once.")
(defvar clutch--completion-metadata-warning-cache (make-hash-table :test 'equal)
  "Completion metadata errors already surfaced in this session.")

(defun clutch--notify-schema-cache-updated (conn state)
  "Notify schema-cache consumers that CONN entered STATE."
  (run-hook-with-args 'clutch--schema-cache-updated-hook conn state))

(defun clutch--notify-metadata-state-changed (conn)
  "Notify metadata consumers that state changed for CONN."
  (run-hook-with-args 'clutch--metadata-state-changed-hook conn))

(defun clutch--metadata-debug-backend (conn)
  "Return CONN's backend key for debug metadata events, or nil."
  (when clutch-debug-mode
    (clutch-db-backend-key conn)))

(defun clutch--metadata-debug-event (conn op phase backend summary &optional context)
  "Record a metadata debug event.
CONN, OP, PHASE, BACKEND, SUMMARY, and CONTEXT describe the event."
  (when clutch-debug-mode
    (apply #'clutch--remember-debug-event
           (append (list :connection conn
                         :op op
                         :phase phase
                         :backend backend
                         :summary summary)
                   (when context
                     (list :context context))))))

(defun clutch--metadata-debug-table-event (conn op phase backend table summary)
  "Record a metadata debug event for TABLE and OP on CONN."
  (clutch--metadata-debug-event conn op phase backend summary
                                (list :table table)))

(defun clutch--metadata-debug-stale-table-event (conn op backend table what)
  "Record a stale metadata debug event for TABLE, OP, and WHAT on CONN."
  (clutch--metadata-debug-table-event
   conn op "stale-drop" backend table
   (format "Ignored stale %s for %s" what table)))

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

(defun clutch--table-metadata (conn key)
  "Return the metadata plist for KEY on CONN, or nil."
  (let ((cache (gethash conn clutch--table-metadata-cache)))
    (and cache (gethash key cache))))

(defun clutch--set-table-metadata (conn table property value)
  "Set PROPERTY to VALUE in TABLE metadata for CONN."
  (let* ((cache (or (gethash conn clutch--table-metadata-cache)
                    (let ((cache (make-hash-table :test 'equal)))
                      (puthash conn cache clutch--table-metadata-cache)
                      cache)))
         (metadata (plist-put (gethash table cache) property value)))
    (puthash table metadata cache)))

(defun clutch--clear-table-metadata-property (conn table property)
  "Remove PROPERTY from TABLE metadata for CONN."
  (when-let* ((cache (gethash conn clutch--table-metadata-cache))
              (metadata (gethash table cache)))
    (setq metadata (cl-loop for (key value) on metadata by #'cddr
                            unless (eq key property)
                            append (list key value)))
    (if metadata
        (puthash table metadata cache)
      (remhash table cache))))

(defun clutch--clear-table-metadata-caches (conn table)
  "Clear table-scoped metadata caches for TABLE on CONN."
  (when-let* ((schema (gethash conn clutch--schema-cache)))
    (unless (eq (gethash table schema 'missing) 'missing)
      (puthash table nil schema)))
  (when-let* ((cache (gethash conn clutch--table-metadata-cache)))
    (remhash table cache)
    (let (comment-keys)
      (maphash (lambda (cache-key _value)
                 (when (equal (cdr-safe cache-key) table)
                   (push cache-key comment-keys)))
               cache)
      (dolist (comment-key comment-keys)
        (remhash comment-key cache))))
  (when-let* ((queue (gethash conn clutch--column-details-queue-cache)))
    (puthash conn
             (cl-remove table queue :test #'equal)
             clutch--column-details-queue-cache))
  (when-let* ((active (gethash conn clutch--column-details-active-cache)))
    (when (equal (car active) table)
      (remhash conn clutch--column-details-active-cache))))

(defun clutch--begin-metadata-ticket ()
  "Issue a new table metadata freshness ticket."
  (cl-incf clutch--metadata-ticket-counter))

(defun clutch--metadata-ticket-current-p (conn ticket status)
  "Return non-nil when TICKET is current for live CONN and STATUS."
  (and conn
       (clutch-db-live-p conn)
       (eql (plist-get status :ticket) ticket)))

(defun clutch--clear-schema-dependent-caches (conn)
  "Clear metadata caches derived from the schema cache for CONN."
  (dolist (cache-symbol clutch--schema-dependent-cache-symbols)
    (remhash conn (symbol-value cache-symbol))))

(defun clutch--schema-status-entry (conn)
  "Return schema status plist for CONN, or nil."
  (and conn
       (gethash conn clutch--schema-status-cache)))

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

(defun clutch--schema-table-candidates (conn schema prefix)
  "Return ready cached table names matching PREFIX for CONN and SCHEMA."
  (when (and (hash-table-p schema)
             (eq (plist-get (clutch--schema-status-entry conn) :state) 'ready))
    (let ((uprefix (upcase prefix)))
      (cl-loop for table being the hash-keys of schema
               when (and (stringp table)
                         (string-prefix-p uprefix (upcase table)))
               collect table))))

(defun clutch--cached-columns (schema table)
  "Return cached columns for TABLE from SCHEMA, or nil if not loaded."
  (let ((cols (and schema (gethash table schema 'missing))))
    (unless (eq cols 'missing) cols)))

(defun clutch--set-schema-status (conn state &optional table-count error-message)
  "Record schema STATE for CONN and refresh connected UI.
TABLE-COUNT is the number of known tables when STATE is \\='ready.
ERROR-MESSAGE is stored when STATE is \\='failed."
  (when conn
    (puthash conn
             (list :state state
                   :tables table-count
                   :error error-message)
             clutch--schema-status-cache)
    (clutch--notify-metadata-state-changed conn)))

(defun clutch--begin-schema-refresh-ticket (conn)
  "Issue and record a new schema refresh ticket for CONN."
  (let ((ticket (cl-incf clutch--schema-refresh-ticket-counter)))
    (puthash conn ticket clutch--schema-refresh-tickets)
    ticket))

(defun clutch--schema-refresh-ticket-latest-p (conn ticket)
  "Return non-nil when TICKET is the latest refresh ticket for CONN."
  (and conn
       (eql (gethash conn clutch--schema-refresh-tickets)
            ticket)))

(defun clutch--schema-refresh-ticket-current-p (conn ticket)
  "Return non-nil when TICKET is current on a live CONN."
  (and (clutch-db-live-p conn)
       (clutch--schema-refresh-ticket-latest-p conn ticket)))

(defun clutch--metadata-status (conn key status-property)
  "Return STATUS-PROPERTY from metadata KEY on CONN."
  (plist-get (clutch--table-metadata conn key) status-property))

(defun clutch--set-metadata-status (conn key status-property state
                                         &optional error-message ticket)
  "Record STATE under STATUS-PROPERTY for metadata KEY on CONN."
  (clutch--set-table-metadata
   conn key status-property
   (list :state state :error error-message :ticket ticket)))

(defun clutch--clear-metadata-status (conn key status-property)
  "Clear STATUS-PROPERTY from metadata KEY on CONN."
  (clutch--clear-table-metadata-property conn key status-property))

(defun clutch--cached-column-details (conn table)
  "Return cached column details for TABLE on CONN, or nil if not loaded."
  (let ((metadata (clutch--table-metadata conn table)))
    (when (plist-member metadata :column-details)
      (plist-get metadata :column-details))))

(defun clutch--column-details-cached-p (conn table)
  "Return non-nil when TABLE has cached column details on CONN."
  (plist-member (clutch--table-metadata conn table) :column-details))

(defun clutch--table-comment-key (conn table &optional schema)
  "Return the schema-qualified cache key for TABLE on CONN."
  (cons (or schema (clutch-db-current-schema conn)) table))

(defun clutch--cached-table-comment (conn table &optional schema)
  "Return TABLE's cached comment in SCHEMA on CONN, or nil."
  (let ((metadata
         (clutch--table-metadata
          conn (clutch--table-comment-key conn table schema))))
    (when (plist-member metadata :comment)
      (plist-get metadata :comment))))

(defun clutch--table-comment-cached-p (conn table &optional schema)
  "Return non-nil when TABLE in SCHEMA has a cached comment on CONN."
  (plist-member
   (clutch--table-metadata conn (clutch--table-comment-key conn table schema))
   :comment))

(defun clutch--cached-foreign-keys (conn table)
  "Return cached foreign-key metadata for TABLE on CONN, or nil."
  (plist-get (clutch--table-metadata conn table) :foreign-keys))

(defun clutch--foreign-keys-cached-p (conn table)
  "Return non-nil when TABLE has cached foreign-key metadata on CONN."
  (plist-member (clutch--table-metadata conn table) :foreign-keys))

(defun clutch--foreign-key-column-info (conn table col-names)
  "Return cached foreign-key metadata indexed by COL-NAMES for CONN/TABLE."
  (when (clutch--foreign-keys-cached-p conn table)
    (cl-loop for (col-name . ref-info) in (clutch--cached-foreign-keys conn table)
             for idx = (cl-position col-name col-names :test #'string=)
             when idx collect (cons idx ref-info))))

(defun clutch--cache-table-entry-comments (conn entries)
  "Prime CONN's table-comment cache from ENTRIES with explicit comments."
  (when conn
    (dolist (entry entries)
      (let ((name (plist-get entry :name)))
        (when (and name (plist-member entry :comment))
          (let ((comment (plist-get entry :comment)))
            (clutch--set-table-metadata
             conn (clutch--table-comment-key
                   conn name (plist-get entry :schema))
             :comment
             (if (and (stringp comment) (string-empty-p comment))
                 nil
               comment)))
          (clutch--clear-metadata-status
           conn (clutch--table-comment-key
                 conn name (plist-get entry :schema))
           :comment-status))))))

(defun clutch--column-details-queue (conn)
  "Return the async column-details queue for CONN."
  (gethash conn clutch--column-details-queue-cache))

(defun clutch--set-column-details-queue (conn queue)
  "Store async column-details QUEUE for CONN."
  (puthash conn queue clutch--column-details-queue-cache))

(defun clutch--column-details-active (conn)
  "Return the active async column-details fetch for CONN, or nil."
  (gethash conn clutch--column-details-active-cache))

(defun clutch--set-column-details-active (conn table ticket)
  "Record TABLE/TICKET as the active async column-details fetch for CONN."
  (puthash conn (cons table ticket)
           clutch--column-details-active-cache))

(defun clutch--clear-column-details-active (conn)
  "Clear the active async column-details fetch for CONN."
  (remhash conn clutch--column-details-active-cache))

(defun clutch--finish-stale-column-details-callback (conn table ticket)
  "Release stale TABLE/TICKET work on CONN and resume its details queue."
  (let ((active (clutch--column-details-active conn)))
    (when (or (null active)
              (and (equal (car active) table)
                   (eql (cdr active) ticket)))
      (when active
        (clutch--clear-column-details-active conn))
      (clutch--drain-column-details-async conn))))

(defun clutch--cancel-schema-install (conn)
  "Cancel any pending schema-install timer for CONN."
  (when-let* ((timer (gethash conn clutch--schema-install-timers)))
    (cancel-timer timer)
    (remhash conn clutch--schema-install-timers)))

(defun clutch--finish-install-schema-cache (conn schema)
  "Publish installed SCHEMA cache for CONN."
  (puthash conn schema clutch--schema-cache)
  (clutch--clear-schema-dependent-caches conn)
  (clutch--set-schema-status conn 'ready (hash-table-count schema))
  (clutch--notify-schema-cache-updated conn 'ready)
  t)

(defun clutch--install-schema-cache-batched (conn table-names ticket)
  "Install TABLE-NAMES for CONN incrementally using idle timers."
  (let ((schema (make-hash-table :test 'equal))
        (remaining table-names)
        (batch-size (max 1 clutch-schema-cache-install-batch-size)))
    (cl-labels ((step ()
                  (remhash conn clutch--schema-install-timers)
                  (when (and conn
                             (clutch-db-live-p conn)
                             (or (null ticket)
                                 (clutch--schema-refresh-ticket-current-p conn ticket)))
                    (let ((count 0))
                      (while (and remaining (< count batch-size))
                        (puthash (car remaining) nil schema)
                        (setq remaining (cdr remaining))
                        (cl-incf count))
                      (if remaining
                          (puthash conn
                                   (run-with-idle-timer 0 nil #'step)
                                   clutch--schema-install-timers)
                        (clutch--finish-install-schema-cache conn schema))))))
      (puthash conn
               (run-with-idle-timer 0 nil #'step)
               clutch--schema-install-timers))
    t))

(defun clutch--install-schema-cache (conn table-names &optional ticket)
  "Install TABLE-NAMES as the schema cache for CONN.
When TICKET is non-nil, ignore the update unless it is still current."
  (when (and conn
             (clutch-db-live-p conn)
             (or (null ticket)
                 (clutch--schema-refresh-ticket-current-p conn ticket)))
    (let* ((small-p (<= (length table-names) clutch-schema-cache-install-batch-size))
           (schema (and small-p (make-hash-table :test 'equal))))
      (clutch--cancel-schema-install conn)
      (clutch--notify-schema-cache-updated conn 'invalidated)
      (if small-p
          (progn
            (dolist (tbl table-names)
              (puthash tbl nil schema))
            (clutch--finish-install-schema-cache conn schema))
        (clutch--install-schema-cache-batched conn table-names ticket)))))

(defun clutch--clear-connection-metadata-caches (conn)
  "Clear schema-scoped metadata caches for CONN."
  (remhash conn clutch--schema-cache)
  (clutch--clear-schema-dependent-caches conn)
  (clutch--cancel-schema-install conn)
  (remhash conn clutch--schema-status-cache)
  (remhash conn clutch--schema-refresh-tickets)
  (clutch--notify-schema-cache-updated conn 'invalidated))

(defun clutch--remember-schema-refresh-error (conn message backend)
  "Record schema refresh MESSAGE for CONN and BACKEND."
  (clutch--set-schema-status conn 'failed nil message)
  (clutch--remember-problem-record
   :connection conn
   :problem (list :backend (clutch-db-backend-key conn)
                  :summary (clutch--humanize-db-error message)
                  :diag (list :category "metadata"
                              :op "schema-refresh"
                              :raw-message message)))
  (clutch--metadata-debug-event
   conn "schema-refresh" "error" backend message))

(defun clutch--refresh-schema-cache-async (conn &optional idle-delay)
  "Refresh schema cache for CONN asynchronously when supported.
Return non-nil when an asynchronous refresh was started.
IDLE-DELAY, when non-nil, is passed to idle metadata backends."
  (let ((ticket (clutch--begin-schema-refresh-ticket conn))
        (backend (clutch--metadata-debug-backend conn)))
    (clutch--set-schema-status conn 'refreshing)
    (let ((started
           (clutch-db-refresh-schema-async
            conn
            (lambda (table-names)
              (if (clutch--schema-refresh-ticket-current-p conn ticket)
                  (progn
                    (clutch--metadata-debug-event
                     conn "schema-refresh" "success" backend
                     (format "Loaded %d tables" (length table-names)))
                    (clutch--install-schema-cache conn table-names ticket))
                (clutch--metadata-debug-event
                 conn "schema-refresh" "stale-drop" backend
                 "Ignored stale schema refresh result")))
            (lambda (message)
              (if (clutch--schema-refresh-ticket-latest-p conn ticket)
                  (clutch--remember-schema-refresh-error conn message backend)
                (clutch--metadata-debug-event
                 conn "schema-refresh" "stale-drop" backend
                 "Ignored stale schema refresh error")))
            idle-delay)))
      (when started
        (clutch--metadata-debug-event
         conn "schema-refresh" "submit" backend
         "Queued background schema refresh"))
      started)))

(defun clutch--refresh-schema-cache (conn)
  "Refresh schema cache for CONN.
Only loads table names (fast).  Column info is loaded lazily."
  (let ((ticket (clutch--begin-schema-refresh-ticket conn)))
    (clutch--set-schema-status conn 'refreshing)
    (condition-case err
        (let ((table-names (clutch-db-list-tables conn)))
          (prog1
              (clutch--install-schema-cache conn table-names ticket)
            (clutch--metadata-debug-event
             conn "schema-refresh" "success"
             (clutch--metadata-debug-backend conn)
             (format "Loaded %d tables" (length table-names)))))
      (clutch-db-error
       (clutch--remember-schema-refresh-error
        conn (error-message-string err) (clutch--metadata-debug-backend conn))
       nil))))

(defun clutch--prime-schema-cache (conn)
  "Kick off the appropriate schema refresh strategy for CONN."
  (if (clutch-db-eager-schema-refresh-p conn)
      (clutch--refresh-schema-cache conn)
    (unless (clutch--refresh-schema-cache-async
             conn clutch-schema-refresh-idle-delay-seconds)
      (clutch--refresh-schema-cache conn))))

(defun clutch--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA for CONN.
Fetches from the backend if not yet cached.  Returns column list."
  (let ((cols (gethash table schema 'missing))
        (status (clutch--metadata-status conn table :columns-status)))
    (unless (eq cols 'missing)
      (or cols
          (unless (eq (plist-get status :state) 'failed)
            (condition-case err
                (let ((col-names (clutch-db-list-columns conn table)))
                  (puthash table col-names schema)
                  (clutch--clear-metadata-status conn table :columns-status)
                  col-names)
              (clutch-db-error
               (clutch--set-metadata-status
                conn table :columns-status 'failed (error-message-string err))
               nil)))))))

(defun clutch--metadata-callback-current-p
    (conn ticket status table op backend outcome)
  "Return non-nil when a metadata callback is current.
Record a stale OUTCOME for TABLE and OP on CONN otherwise.  TICKET and STATUS
identify the request; BACKEND labels its diagnostic event."
  (if (clutch--metadata-ticket-current-p conn ticket status)
      t
    (clutch--metadata-debug-stale-table-event
     conn op backend table outcome)
    nil))

(defun clutch--start-table-metadata-request
    (conn key table status-property op fetch install &optional report-error)
  "Start one independent async metadata request for TABLE on CONN.
KEY and STATUS-PROPERTY locate its state.  OP labels diagnostics.  FETCH is
called with CONN, TABLE, a success callback, and an error callback.  INSTALL
stores the successful value.  REPORT-ERROR, when non-nil, handles an error
message after its failed state is recorded."
  (let ((ticket (clutch--begin-metadata-ticket))
        (backend (clutch--metadata-debug-backend conn)))
    (clutch--set-metadata-status
     conn key status-property 'loading nil ticket)
    (let ((started
           (funcall
            fetch conn table
            (lambda (value)
              (when (clutch--metadata-callback-current-p
                     conn ticket
                     (clutch--metadata-status conn key status-property)
                     table op backend "result")
                (clutch--metadata-debug-table-event
                 conn op "success" backend table
                 (format "Loaded %s for %s" op table))
                (funcall install value)
                (clutch--clear-metadata-status conn key status-property)
                (clutch--notify-metadata-state-changed conn)))
            (lambda (message)
              (when (clutch--metadata-callback-current-p
                     conn ticket
                     (clutch--metadata-status conn key status-property)
                     table op backend "error")
                (clutch--set-metadata-status
                 conn key status-property 'failed message ticket)
                (clutch--metadata-debug-table-event
                 conn op "error" backend table message)
                (when report-error
                  (funcall report-error message))
                (clutch--notify-metadata-state-changed conn))))))
      (when started
        (clutch--metadata-debug-table-event
         conn op "submit" backend table
         (format "Queued background %s for %s" op table)))
      started)))

(defun clutch--ensure-columns-async (conn schema table)
  "Queue an async column-name fetch for TABLE in SCHEMA on CONN when needed."
  (let ((state (plist-get
                (clutch--metadata-status conn table :columns-status)
                :state)))
    (unless (or (clutch--cached-columns schema table)
                (memq state '(queued loading)))
      (let ((started
             (clutch--start-table-metadata-request
              conn table table :columns-status "list-columns"
              #'clutch-db-list-columns-async
              (lambda (columns)
                (when-let* ((live-schema
                             (gethash conn clutch--schema-cache)))
                  (puthash table columns live-schema))))))
        (unless started
          (clutch--ensure-columns conn schema table)
          (clutch--notify-metadata-state-changed conn)))
      t)))

(defun clutch--ensure-column-details (conn table &optional strict)
  "Return column details for TABLE on CONN, loading lazily if needed.
Returns a list of plists with :name :type :nullable :primary-key :foreign-key,
or nil on error.  When STRICT is non-nil, signal `clutch-db-error'."
  (let* ((metadata (clutch--table-metadata conn table))
         (cached (if (plist-member metadata :column-details)
                     (plist-get metadata :column-details)
                   'missing))
         (status (clutch--metadata-status
                  conn table :column-details-status)))
    (if (not (eq cached 'missing))
        cached
      (if (eq (plist-get status :state) 'failed)
          (when strict
            (let* ((message (or (plist-get status :error)
                                (format "Failed to load column details for %s"
                                        table)))
                   (details (clutch-db-error-details conn)))
              (signal 'clutch-db-error
                      (if details
                          (list message (copy-tree details))
                        (list message)))))
        (condition-case err
            (let ((details (clutch-db-column-details conn table)))
              (clutch--set-table-metadata conn table :column-details details)
              (clutch--clear-metadata-status
               conn table :column-details-status)
              details)
          (clutch-db-error
           (let* ((message (error-message-string err))
                  (details (or (nth 2 err)
                               (clutch-db-error-details conn))))
             (clutch--set-metadata-status
              conn table :column-details-status 'failed message)
             (when strict
               (signal 'clutch-db-error
                       (if details
                           (list message (copy-tree details))
                         (list message))))
             nil)))))))

(defun clutch--drain-column-details-async (conn)
  "Start the next queued async column-details fetch for CONN."
  (unless (or (clutch--column-details-active conn)
              (not (clutch-db-live-p conn)))
    (let ((backend (clutch--metadata-debug-backend conn)))
      (when-let* ((queue (clutch--column-details-queue conn))
                  (table (car queue))
                  (ticket
                   (plist-get
                    (clutch--metadata-status
                     conn table :column-details-status)
                    :ticket)))
        (clutch--set-column-details-queue conn (cdr queue))
        (clutch--set-column-details-active conn table ticket)
        (clutch--set-metadata-status
         conn table :column-details-status 'loading nil ticket)
        (let ((started
               (clutch-db-column-details-async
                conn table
                (lambda (details)
                  (if (clutch--metadata-callback-current-p
                       conn ticket
                       (clutch--metadata-status
                        conn table :column-details-status)
                       table "column-details" backend "column-detail result")
                      (progn
                        (clutch--metadata-debug-table-event
                         conn "column-details" "success" backend table
                         (format "Loaded %d column details for %s"
                                 (length details) table))
                        (clutch--set-table-metadata
                         conn table :column-details details)
                        (clutch--clear-metadata-status
                         conn table :column-details-status)
                        (clutch--clear-column-details-active conn)
                        (run-hook-with-args
                         'clutch--table-metadata-updated-hook
                         conn table 'column-details)
                        (clutch--notify-metadata-state-changed conn)
                        (clutch--drain-column-details-async conn))
                    (clutch--finish-stale-column-details-callback
                     conn table ticket)))
                (lambda (message)
                  (if (clutch--metadata-callback-current-p
                       conn ticket
                       (clutch--metadata-status
                        conn table :column-details-status)
                       table "column-details" backend "column-detail error")
                      (progn
                        (clutch--set-metadata-status
                         conn table :column-details-status
                         'failed message ticket)
                        (clutch--metadata-debug-table-event
                         conn "column-details" "error" backend table message)
                        (clutch--clear-column-details-active conn)
                        (clutch--notify-metadata-state-changed conn)
                        (clutch--drain-column-details-async conn))
                    (clutch--finish-stale-column-details-callback
                     conn table ticket))))))
          (when started
            (clutch--metadata-debug-table-event
             conn "column-details" "submit" backend table
             (format "Queued background column-detail preheat for %s" table)))
          (unless started
            (clutch--ensure-column-details conn table)
            (clutch--clear-column-details-active conn)
            (when (clutch--column-details-cached-p conn table)
              (run-hook-with-args
               'clutch--table-metadata-updated-hook
               conn table 'column-details))
            (clutch--notify-metadata-state-changed conn)
            (clutch--drain-column-details-async conn)))))))

(defun clutch--ensure-column-details-async (conn table)
  "Queue an async column-detail fetch for TABLE on CONN when needed."
  (let ((state (plist-get
                (clutch--metadata-status
                 conn table :column-details-status)
                :state))
        (ticket (clutch--begin-metadata-ticket)))
    (unless (or (clutch--column-details-cached-p conn table)
                (memq state '(queued loading)))
      (clutch--set-metadata-status
       conn table :column-details-status 'queued nil ticket)
      (clutch--set-column-details-queue
       conn
       (append (clutch--column-details-queue conn) (list table)))
      (clutch--drain-column-details-async conn)
      t)))

(defun clutch--ensure-table-comment (conn table &optional schema)
  "Return TABLE's comment in SCHEMA on CONN, loading it when needed.
Returns a string or nil."
  (let* ((key (clutch--table-comment-key conn table schema))
         (metadata (clutch--table-metadata conn key)))
    (if (plist-member metadata :comment)
        (plist-get metadata :comment)
      (condition-case err
          (let ((comment (clutch-db-table-comment conn table schema)))
            (clutch--set-table-metadata conn key :comment comment)
            comment)
        (clutch-db-error
         (clutch--remember-recoverable-metadata-warning
          conn "table comment" err `(:table ,table :schema ,schema))
         nil)))))

(defun clutch--ensure-table-comment-async (conn table)
  "Queue an async table-comment fetch for TABLE on CONN when needed."
  (let* ((schema (clutch-db-current-schema conn))
         (key (clutch--table-comment-key conn table schema))
         (state (plist-get
                 (clutch--metadata-status conn key :comment-status)
                 :state)))
    (unless (or (not table)
                (clutch--table-comment-cached-p conn table schema)
                (memq state '(queued loading failed)))
      (let ((started
             (clutch--start-table-metadata-request
              conn key table :comment-status "table-comment"
              #'clutch-db-table-comment-async
              (lambda (comment)
                (clutch--set-table-metadata conn key :comment comment)))))
        (unless started
          (condition-case err
              (progn
                (clutch--set-table-metadata
                 conn key :comment
                 (clutch-db-table-comment conn table schema))
                (clutch--clear-metadata-status conn key :comment-status))
            (clutch-db-error
             (clutch--set-metadata-status
              conn key :comment-status 'failed (error-message-string err)
              (plist-get (clutch--metadata-status conn key :comment-status)
                         :ticket))))
          (clutch--notify-metadata-state-changed conn)))
      t)))

(defun clutch--ensure-foreign-keys-async (conn table)
  "Queue an async foreign-key fetch for TABLE on CONN when needed."
  (let ((state (plist-get
                (clutch--metadata-status conn table :foreign-keys-status)
                :state)))
    (unless (or (not table)
                (clutch--foreign-keys-cached-p conn table)
                (memq state '(queued loading failed)))
      (let ((started
             (clutch--start-table-metadata-request
              conn table table :foreign-keys-status "foreign-keys"
              #'clutch-db-foreign-keys-async
              (lambda (fks)
                (clutch--set-table-metadata conn table :foreign-keys fks)
                (clutch--clear-metadata-status
                 conn table :foreign-keys-status)
                (run-hook-with-args
                 'clutch--table-metadata-updated-hook
                 conn table 'foreign-keys))
              (lambda (message)
                (clutch--remember-recoverable-metadata-warning
                 conn "foreign-key metadata"
                 `(clutch-db-error ,message)
                 `(:table ,table))))))
        (unless started
          (clutch--set-table-metadata conn table :foreign-keys nil)
          (clutch--clear-metadata-status conn table :foreign-keys-status)))
      t)))

(defun clutch--format-help-doc (doc)
  "Format a DOC plist (:sig SIG :desc DESC) as a propertized eldoc string."
  (let ((sig  (plist-get doc :sig))
        (desc (plist-get doc :desc)))
    (concat (propertize sig 'face 'font-lock-function-name-face)
            (when (and desc (not (string-empty-p desc)))
              (propertize (concat "  — " desc) 'face 'shadow)))))

(defun clutch--ensure-help-doc (conn sym)
  "Return a backend-provided eldoc string for SYM from CONN, with caching.
Queries the server on first access; subsequent calls read from cache.
Returns nil when SYM is not a known built-in on this server."
  (let* ((cache (or (gethash conn clutch--help-doc-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash conn h clutch--help-doc-cache)
                      h)))
         (uname (upcase sym))
         (entry (gethash uname cache 'missing)))
    (cond
     ((eq entry 'missing)
      (condition-case err
          (let ((doc (clutch-db-symbol-help conn sym)))
            (puthash uname (or doc 'not-found) cache)
            (when doc (clutch--format-help-doc doc)))
        (clutch-db-error
         (clutch--remember-recoverable-metadata-warning
          conn "symbol help" err `(:symbol ,sym))
         nil)))
     ((eq entry 'not-found) nil)
     (t (clutch--format-help-doc entry)))))

(defun clutch--schema-for-connection (&optional conn)
  "Return the schema hash-table for CONN, or nil.
When CONN is nil, use `clutch-connection'."
  (let ((conn (or conn clutch-connection)))
    (when (and conn (clutch-db-live-p conn))
      (gethash conn clutch--schema-cache))))

(provide 'clutch-schema)

;;; clutch-schema.el ends here
