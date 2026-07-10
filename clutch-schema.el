;;; clutch-schema.el --- Schema refresh and metadata caches -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Schema refresh commands, status, and metadata cache state.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'subr-x)

(defvar clutch--schema-cache (make-hash-table :test 'equal)
  "Global schema cache keyed by connection key string.")

(defvar clutch--column-details-cache (make-hash-table :test 'equal)
  "Cache for full column details keyed by connection key string.")

(defvar clutch--column-details-status-cache (make-hash-table :test 'equal)
  "Per-connection status for column-detail fetches.")

(defvar clutch--column-details-queue-cache (make-hash-table :test 'equal)
  "Per-connection queue of tables waiting for async column-detail fetch.")

(defvar clutch--column-details-active-cache (make-hash-table :test 'equal)
  "Per-connection active async column-detail fetch as (TABLE . TICKET).")

(defvar clutch--metadata-ticket-counter 0
  "Monotonic counter used to reject stale async table metadata callbacks.")

(defvar clutch--columns-status-cache (make-hash-table :test 'equal)
  "Per-connection status for synchronous column-name loads.")

(defvar clutch--table-comment-cache (make-hash-table :test 'equal)
  "Cache for table comments keyed by connection key string.")

(defvar clutch--table-comment-status-cache (make-hash-table :test 'equal)
  "Per-connection status for async table-comment fetches.")

(defvar clutch--foreign-keys-cache (make-hash-table :test 'equal)
  "Cache for foreign-key metadata keyed by connection key string.")

(defvar clutch--foreign-keys-status-cache (make-hash-table :test 'equal)
  "Per-connection status for async foreign-key fetches.")

(defvar clutch--help-doc-cache (make-hash-table :test 'equal)
  "Cache for live function docs fetched from the database server.")

(defvar clutch--schema-install-timers (make-hash-table :test 'equal)
  "Idle timers finishing large schema installs keyed by connection key string.")

(defvar clutch--schema-status-cache (make-hash-table :test 'equal)
  "Schema refresh status cache keyed by connection key string.")

(defvar clutch--schema-refresh-ticket-counter 0
  "Monotonic counter used to reject stale async schema refreshes.")

(defvar clutch--schema-refresh-tickets (make-hash-table :test 'equal)
  "Latest schema refresh ticket keyed by connection key string.")

(defvar clutch--table-metadata-updated-hook nil
  "Hook run after table-scoped metadata is updated.
Functions receive CONN, TABLE, and KIND.")

(defconst clutch--schema-dependent-cache-symbols
  '(clutch--columns-status-cache
    clutch--column-details-cache
    clutch--column-details-status-cache
    clutch--column-details-queue-cache
    clutch--column-details-active-cache
    clutch--table-comment-cache
    clutch--table-comment-status-cache
    clutch--foreign-keys-cache
    clutch--foreign-keys-status-cache
    clutch--help-doc-cache)
  "Metadata cache variables invalidated when schema cache changes.")

(defvar clutch--object-cache)
(defvar clutch-connection)
(defvar clutch-debug-mode nil)
(defvar clutch-schema-cache-install-batch-size)
(defvar clutch-schema-refresh-idle-delay-seconds)
(defvar clutch--oracle-i18n-warning-shown nil
  "Non-nil after showing the Oracle orai18n completion warning once.")
(defvar clutch--completion-metadata-warning-cache (make-hash-table :test 'equal)
  "Completion metadata errors already surfaced in this session.")

(declare-function clutch--connection-alive-p "clutch-connection" (conn))
(declare-function clutch--backend-key-from-conn "clutch-connection" (conn))
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch--ensure-connection "clutch-connection" ())
(declare-function clutch--attached-buffer-for-connection "clutch-connection" (connection))
(declare-function clutch--invalidate-object-warmup "clutch-object" (conn &optional key))
(declare-function clutch--remember-debug-event "clutch-query" (&rest event))
(declare-function clutch--remember-problem-record "clutch-query" (&rest args))
(declare-function clutch--schedule-object-warmup "clutch-object" (conn))
(declare-function clutch--refresh-schema-status-ui "clutch-connection" (conn))
(declare-function clutch--run-db-query "clutch-connection" (conn sql))

(defun clutch--metadata-debug-backend (conn)
  "Return CONN's backend key for debug metadata events, or nil."
  (when clutch-debug-mode
    (condition-case nil
        (clutch--backend-key-from-conn conn)
      (error nil))))

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

(defun clutch--metadata-cache-for (conn cache-table)
  "Return CONN's per-connection metadata cache from CACHE-TABLE."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key cache-table)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h cache-table)
                      h))))
    cache))

(defun clutch--metadata-cache-get (conn table cache-table &optional default)
  "Return TABLE's metadata value for CONN from CACHE-TABLE.
Return DEFAULT when CONN has no cache entry or TABLE is absent."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key cache-table)))
    (if cache
        (gethash table cache default)
      default)))

(defun clutch--metadata-table-status (conn table cache-table)
  "Return TABLE status plist for CONN from CACHE-TABLE, or nil."
  (clutch--metadata-cache-get conn table cache-table))

(defun clutch--set-metadata-table-status (conn table cache-table state
                                               &optional error-message ticket)
  "Record TABLE metadata STATE for CONN in CACHE-TABLE."
  (puthash table
           (list :state state :error error-message :ticket ticket)
           (clutch--metadata-cache-for conn cache-table)))

(defun clutch--clear-metadata-table-status (conn table cache-table)
  "Clear TABLE metadata status for CONN from CACHE-TABLE."
  (when-let* ((cache (gethash (clutch--connection-key conn) cache-table)))
    (remhash table cache)))

(defun clutch--clear-table-metadata-caches (conn table)
  "Clear table-scoped metadata caches for TABLE on CONN."
  (let ((key (clutch--connection-key conn)))
    (when-let* ((schema (gethash key clutch--schema-cache)))
      (unless (eq (gethash table schema 'missing) 'missing)
        (puthash table nil schema)))
    (dolist (cache-symbol '(clutch--columns-status-cache
                            clutch--column-details-cache
                            clutch--column-details-status-cache
                            clutch--foreign-keys-cache
                            clutch--foreign-keys-status-cache))
      (when-let* ((cache (gethash key (symbol-value cache-symbol))))
        (remhash table cache)))
    (dolist (cache-symbol '(clutch--table-comment-cache
                            clutch--table-comment-status-cache))
      (when-let* ((cache (gethash key (symbol-value cache-symbol))))
        (let (table-keys)
          (maphash (lambda (cache-key _value)
                     (when (equal (cdr-safe cache-key) table)
                       (push cache-key table-keys)))
                   cache)
          (dolist (table-key table-keys)
            (remhash table-key cache)))))
    (when-let* ((queue (gethash key clutch--column-details-queue-cache)))
      (puthash key
               (cl-remove table queue :test #'equal)
               clutch--column-details-queue-cache))
    (when-let* ((active (gethash key clutch--column-details-active-cache)))
      (when (equal (car active) table)
        (remhash key clutch--column-details-active-cache)))))

(defun clutch--begin-metadata-ticket ()
  "Issue a new table metadata freshness ticket."
  (cl-incf clutch--metadata-ticket-counter))

(defun clutch--metadata-ticket-current-p (conn table ticket cache-table)
  "Return non-nil when TICKET is current for CONN and TABLE in CACHE-TABLE."
  (and conn
       (clutch--connection-alive-p conn)
       (eql (plist-get (clutch--metadata-table-status conn table cache-table)
                       :ticket)
            ticket)))

(defun clutch--clear-schema-dependent-caches (key)
  "Clear metadata caches derived from schema cache KEY."
  (dolist (cache-symbol clutch--schema-dependent-cache-symbols)
    (remhash key (symbol-value cache-symbol))))

(defun clutch--schema-status-entry (conn)
  "Return schema status plist for CONN, or nil."
  (and conn
       (gethash (clutch--connection-key conn) clutch--schema-status-cache)))

(defun clutch--schema-status-header-line-segment (conn)
  "Return a header-line segment for CONN schema status, or nil.
Returns nil when the schema is ready (no noise for the happy path)."
  (when-let* ((entry (clutch--schema-status-entry conn))
              (state (plist-get entry :state)))
    (pcase state
      ('refreshing (propertize "schema…" 'face 'shadow))
      ('stale      (propertize "schema~" 'face 'warning))
      ('failed     (propertize "schema!" 'face 'error))
      (_ nil))))

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
    (puthash (clutch--connection-key conn)
             (list :state state
                   :tables table-count
                   :error error-message)
             clutch--schema-status-cache)
    (clutch--refresh-schema-status-ui conn)))

(defun clutch--begin-schema-refresh-ticket (conn)
  "Issue and record a new schema refresh ticket for CONN."
  (let ((ticket (cl-incf clutch--schema-refresh-ticket-counter)))
    (puthash (clutch--connection-key conn) ticket clutch--schema-refresh-tickets)
    ticket))

(defun clutch--schema-refresh-ticket-latest-p (conn ticket)
  "Return non-nil when TICKET is the latest refresh ticket for CONN."
  (and conn
       (eql (gethash (clutch--connection-key conn) clutch--schema-refresh-tickets)
            ticket)))

(defun clutch--schema-refresh-ticket-current-p (conn ticket)
  "Return non-nil when TICKET is current on a live CONN."
  (and (clutch--connection-alive-p conn)
       (clutch--schema-refresh-ticket-latest-p conn ticket)))

(defun clutch--columns-status (conn table)
  "Return column-name load status plist for TABLE on CONN, or nil."
  (clutch--metadata-table-status conn table clutch--columns-status-cache))

(defun clutch--set-columns-status (conn table state &optional error-message ticket)
  "Record synchronous column-name load STATE for TABLE on CONN."
  (clutch--set-metadata-table-status
   conn table clutch--columns-status-cache state error-message ticket))

(defun clutch--clear-columns-status (conn table)
  "Clear any recorded column-name load status for TABLE on CONN."
  (clutch--clear-metadata-table-status conn table clutch--columns-status-cache))

(defun clutch--column-details-status (conn table)
  "Return async column-detail status plist for TABLE on CONN, or nil."
  (clutch--metadata-table-status
   conn table clutch--column-details-status-cache))

(defun clutch--cached-column-details (conn table)
  "Return cached column details for TABLE on CONN, or nil if not loaded."
  (let ((details (clutch--metadata-cache-get
                  conn table clutch--column-details-cache 'missing)))
    (unless (eq details 'missing) details)))

(defun clutch--table-comment-key (conn table &optional schema)
  "Return the schema-qualified cache key for TABLE on CONN."
  (cons (or schema (clutch-db-current-schema conn)) table))

(defun clutch--table-comment-status (conn table &optional schema)
  "Return async comment status for TABLE in SCHEMA on CONN, or nil."
  (clutch--metadata-table-status
   conn (clutch--table-comment-key conn table schema)
   clutch--table-comment-status-cache))

(defun clutch--cached-table-comment (conn table &optional schema)
  "Return TABLE's cached comment in SCHEMA on CONN, or nil."
  (let ((comment (clutch--metadata-cache-get
                  conn (clutch--table-comment-key conn table schema)
                  clutch--table-comment-cache 'missing)))
    (unless (eq comment 'missing) comment)))

(defun clutch--table-comment-cached-p (conn table &optional schema)
  "Return non-nil when TABLE in SCHEMA has a cached comment on CONN."
  (not (eq (clutch--metadata-cache-get
            conn (clutch--table-comment-key conn table schema)
            clutch--table-comment-cache 'missing)
           'missing)))

(defun clutch--foreign-keys-status (conn table)
  "Return async foreign-key status plist for TABLE on CONN, or nil."
  (clutch--metadata-table-status conn table clutch--foreign-keys-status-cache))

(defun clutch--foreign-keys-cache-entry (conn table)
  "Return cached foreign-key entry for TABLE on CONN, or `missing'."
  (clutch--metadata-cache-get
   conn table clutch--foreign-keys-cache 'missing))

(defun clutch--cached-foreign-keys (conn table)
  "Return cached foreign-key metadata for TABLE on CONN, or nil."
  (let ((fks (clutch--foreign-keys-cache-entry conn table)))
    (unless (eq fks 'missing) fks)))

(defun clutch--foreign-keys-cached-p (conn table)
  "Return non-nil when TABLE has cached foreign-key metadata on CONN."
  (not (eq (clutch--foreign-keys-cache-entry conn table) 'missing)))

(defun clutch--foreign-key-column-info (conn table col-names)
  "Return cached foreign-key metadata indexed by COL-NAMES for CONN/TABLE."
  (when (clutch--foreign-keys-cached-p conn table)
    (cl-loop for (col-name . ref-info) in (clutch--cached-foreign-keys conn table)
             for idx = (cl-position col-name col-names :test #'string=)
             when idx collect (cons idx ref-info))))

(defun clutch--set-foreign-keys-status (conn table state
                                             &optional error-message ticket)
  "Record async foreign-key STATE for TABLE on CONN."
  (clutch--set-metadata-table-status
   conn table clutch--foreign-keys-status-cache state error-message ticket))

(defun clutch--clear-foreign-keys-status (conn table)
  "Clear any recorded foreign-key status for TABLE on CONN."
  (clutch--clear-metadata-table-status conn table clutch--foreign-keys-status-cache))

(defun clutch--set-table-comment-status (conn table state
                                              &optional error-message ticket schema)
  "Record async comment STATE for TABLE in SCHEMA on CONN."
  (clutch--set-metadata-table-status
   conn (clutch--table-comment-key conn table schema)
   clutch--table-comment-status-cache state error-message ticket))

(defun clutch--clear-table-comment-status (conn table &optional schema)
  "Clear recorded comment status for TABLE in SCHEMA on CONN."
  (clutch--clear-metadata-table-status
   conn (clutch--table-comment-key conn table schema)
   clutch--table-comment-status-cache))

(defun clutch--cache-table-entry-comments (conn entries)
  "Prime CONN's table-comment cache from ENTRIES with explicit comments."
  (when conn
    (let (cache)
      (dolist (entry entries)
        (let ((name (plist-get entry :name)))
          (when (and name (plist-member entry :comment))
            (unless cache
              (setq cache
                    (clutch--metadata-cache-for conn clutch--table-comment-cache)))
            (let ((comment (plist-get entry :comment)))
              (puthash (clutch--table-comment-key
                        conn name (plist-get entry :schema))
                       (if (and (stringp comment) (string-empty-p comment))
                           nil
                         comment)
                       cache))
            (clutch--clear-table-comment-status
             conn name (plist-get entry :schema))))))))

(defun clutch--set-column-details-status (conn table state
                                               &optional error-message ticket)
  "Record async column-detail STATE for TABLE on CONN."
  (clutch--set-metadata-table-status
   conn table clutch--column-details-status-cache state error-message ticket))

(defun clutch--clear-column-details-status (conn table)
  "Clear any recorded column-detail status for TABLE on CONN."
  (clutch--clear-metadata-table-status
   conn table clutch--column-details-status-cache))

(defun clutch--column-details-queue (conn)
  "Return the async column-details queue for CONN."
  (gethash (clutch--connection-key conn) clutch--column-details-queue-cache))

(defun clutch--set-column-details-queue (conn queue)
  "Store async column-details QUEUE for CONN."
  (puthash (clutch--connection-key conn) queue clutch--column-details-queue-cache))

(defun clutch--column-details-active (conn)
  "Return the active async column-details fetch for CONN, or nil."
  (gethash (clutch--connection-key conn) clutch--column-details-active-cache))

(defun clutch--set-column-details-active (conn table ticket)
  "Record TABLE/TICKET as the active async column-details fetch for CONN."
  (puthash (clutch--connection-key conn) (cons table ticket)
           clutch--column-details-active-cache))

(defun clutch--clear-column-details-active (conn)
  "Clear the active async column-details fetch for CONN."
  (remhash (clutch--connection-key conn) clutch--column-details-active-cache))

(defun clutch--cancel-schema-install (conn &optional key)
  "Cancel any pending schema-install timer for CONN or explicit KEY."
  (let ((key (or key (clutch--connection-key conn))))
    (when-let* ((timer (gethash key clutch--schema-install-timers)))
      (cancel-timer timer)
      (remhash key clutch--schema-install-timers))))

(defun clutch--finish-install-schema-cache (conn key schema)
  "Publish installed SCHEMA cache for CONN under KEY."
  (puthash key schema clutch--schema-cache)
  (clutch--clear-schema-dependent-caches key)
  (clutch--invalidate-object-warmup conn)
  (remhash key clutch--object-cache)
  (clutch--set-schema-status conn 'ready (hash-table-count schema))
  (clutch--schedule-object-warmup conn)
  t)

(defun clutch--install-schema-cache-batched (conn table-names key ticket)
  "Install TABLE-NAMES for CONN incrementally using idle timers."
  (let ((schema (make-hash-table :test 'equal))
        (remaining table-names)
        (batch-size (max 1 clutch-schema-cache-install-batch-size)))
    (cl-labels ((step ()
                  (remhash key clutch--schema-install-timers)
                  (when (and conn
                             (clutch--connection-alive-p conn)
                             (or (null ticket)
                                 (clutch--schema-refresh-ticket-current-p conn ticket)))
                    (let ((count 0))
                      (while (and remaining (< count batch-size))
                        (puthash (car remaining) nil schema)
                        (setq remaining (cdr remaining))
                        (cl-incf count))
                      (if remaining
                          (puthash key
                                   (run-with-idle-timer 0 nil #'step)
                                   clutch--schema-install-timers)
                        (clutch--finish-install-schema-cache conn key schema))))))
      (puthash key
               (run-with-idle-timer 0 nil #'step)
               clutch--schema-install-timers))
    t))

(defun clutch--install-schema-cache (conn table-names &optional ticket)
  "Install TABLE-NAMES as the schema cache for CONN.
When TICKET is non-nil, ignore the update unless it is still current."
  (when (and conn
             (clutch--connection-alive-p conn)
             (or (null ticket)
                 (clutch--schema-refresh-ticket-current-p conn ticket)))
    (let* ((key (clutch--connection-key conn))
           (small-p (<= (length table-names) clutch-schema-cache-install-batch-size))
           (schema (and small-p (make-hash-table :test 'equal))))
      (clutch--cancel-schema-install conn)
      (clutch--invalidate-object-warmup conn)
      (remhash key clutch--object-cache)
      (if small-p
          (progn
            (dolist (tbl table-names)
              (puthash tbl nil schema))
            (clutch--finish-install-schema-cache conn key schema))
        (clutch--install-schema-cache-batched conn table-names key ticket)))))

(defun clutch--clear-connection-metadata-caches (conn &optional key)
  "Clear schema-scoped metadata caches for CONN.
When KEY is non-nil, clear that cache namespace instead of CONN's current key."
  (let ((key (or key (clutch--connection-key conn))))
    (remhash key clutch--schema-cache)
    (clutch--clear-schema-dependent-caches key)
    (clutch--cancel-schema-install conn key)
    (clutch--invalidate-object-warmup conn key)
    (remhash key clutch--object-cache)
    (remhash key clutch--schema-status-cache)
    (remhash key clutch--schema-refresh-tickets)))

(defun clutch--remember-schema-refresh-error (conn message backend)
  "Record schema refresh MESSAGE for CONN and BACKEND."
  (clutch--set-schema-status conn 'failed nil message)
  (clutch--remember-problem-record
   :connection conn
   :problem (list :backend (clutch--backend-key-from-conn conn)
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
      (error
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

;;;###autoload
(defun clutch-refresh-schema ()
  "Refresh the schema cache for the current connection.
Useful after DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE)
executed outside clutch that would otherwise leave stale completions."
  (interactive)
  (clutch--refresh-current-schema nil t))

(defun clutch--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA for CONN.
Fetches from the backend if not yet cached.  Returns column list."
  (let ((cols (gethash table schema 'missing))
        (status (clutch--columns-status conn table)))
    (unless (eq cols 'missing)
      (or cols
          (unless (eq (plist-get status :state) 'failed)
            (condition-case err
                (let ((col-names (clutch-db-list-columns conn table)))
                  (puthash table col-names schema)
                  (clutch--clear-columns-status conn table)
                  col-names)
              (clutch-db-error
               (clutch--set-columns-status conn table 'failed
                                           (error-message-string err))
               nil)))))))

(defun clutch--ensure-columns-async (conn schema table)
  "Queue an async column-name fetch for TABLE in SCHEMA on CONN when needed."
  (let ((state (plist-get (clutch--columns-status conn table) :state))
        (ticket (clutch--begin-metadata-ticket))
        (backend (clutch--metadata-debug-backend conn)))
    (unless (or (clutch--cached-columns schema table)
                (memq state '(queued loading)))
      (clutch--set-columns-status conn table 'loading nil ticket)
      (let ((started
             (clutch-db-list-columns-async
              conn table
              (lambda (columns)
                (if (clutch--metadata-ticket-current-p
                     conn table ticket clutch--columns-status-cache)
                    (progn
                      (clutch--metadata-debug-table-event
                       conn "list-columns" "success" backend table
                       (format "Loaded %d column names for %s"
                               (length columns) table))
                      (when-let* ((live-schema
                                   (gethash (clutch--connection-key conn)
                                            clutch--schema-cache)))
                        (puthash table columns live-schema))
                      (clutch--clear-columns-status conn table)
                      (clutch--refresh-schema-status-ui conn))
                  (clutch--metadata-debug-stale-table-event
                   conn "list-columns" backend table "column-name result")))
              (lambda (message)
                (if (clutch--metadata-ticket-current-p
                     conn table ticket clutch--columns-status-cache)
                    (progn
                      (clutch--set-columns-status conn table 'failed message ticket)
                      (clutch--metadata-debug-table-event
                       conn "list-columns" "error" backend table message)
                      (clutch--refresh-schema-status-ui conn))
                  (clutch--metadata-debug-stale-table-event
                   conn "list-columns" backend table "column-name error"))))))
        (when started
          (clutch--metadata-debug-table-event
           conn "list-columns" "submit" backend table
           (format "Queued background column-name preheat for %s" table)))
        (unless started
          (clutch--ensure-columns conn schema table)
          (clutch--refresh-schema-status-ui conn)))
      t)))

(defun clutch--ensure-column-details (conn table &optional strict)
  "Return column details for TABLE on CONN, loading lazily if needed.
Returns a list of plists with :name :type :nullable :primary-key :foreign-key,
or nil on error.  When STRICT is non-nil, signal `clutch-db-error'."
  (let* ((cache (clutch--metadata-cache-for conn clutch--column-details-cache))
         (cached (gethash table cache 'missing))
         (status (clutch--column-details-status conn table)))
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
              (puthash table details cache)
              (clutch--clear-column-details-status conn table)
              details)
          (clutch-db-error
           (let* ((message (error-message-string err))
                  (details (or (nth 2 err)
                               (clutch-db-error-details conn))))
             (clutch--set-column-details-status conn table 'failed message)
             (when strict
               (signal 'clutch-db-error
                       (if details
                           (list message (copy-tree details))
                         (list message))))
             nil)))))))

(defun clutch--drain-column-details-async (conn)
  "Start the next queued async column-details fetch for CONN."
  (unless (or (clutch--column-details-active conn)
              (not (clutch--connection-alive-p conn)))
    (let ((backend (clutch--metadata-debug-backend conn)))
      (when-let* ((queue (clutch--column-details-queue conn))
                  (table (car queue))
                  (ticket (plist-get (clutch--column-details-status conn table) :ticket)))
        (clutch--set-column-details-queue conn (cdr queue))
        (clutch--set-column-details-active conn table ticket)
        (clutch--set-column-details-status conn table 'loading nil ticket)
        (let ((started
               (clutch-db-column-details-async
                conn table
                (lambda (details)
                  (if (clutch--metadata-ticket-current-p
                       conn table ticket clutch--column-details-status-cache)
                      (let ((cache (clutch--metadata-cache-for
                                    conn clutch--column-details-cache)))
                        (clutch--metadata-debug-table-event
                         conn "column-details" "success" backend table
                         (format "Loaded %d column details for %s"
                                 (length details) table))
                        (puthash table details cache)
                        (clutch--clear-column-details-status conn table)
                        (clutch--clear-column-details-active conn)
                        (run-hook-with-args
                         'clutch--table-metadata-updated-hook
                         conn table 'column-details)
                        (clutch--refresh-schema-status-ui conn)
                        (clutch--drain-column-details-async conn))
                    (clutch--metadata-debug-stale-table-event
                     conn "column-details" backend table "column-detail result")))
                (lambda (message)
                  (if (clutch--metadata-ticket-current-p
                       conn table ticket clutch--column-details-status-cache)
                      (progn
                        (clutch--set-column-details-status conn table 'failed
                                                           message ticket)
                        (clutch--metadata-debug-table-event
                         conn "column-details" "error" backend table message)
                        (clutch--clear-column-details-active conn)
                        (clutch--refresh-schema-status-ui conn)
                        (clutch--drain-column-details-async conn))
                    (clutch--metadata-debug-stale-table-event
                     conn "column-details" backend table "column-detail error"))))))
          (when started
            (clutch--metadata-debug-table-event
             conn "column-details" "submit" backend table
             (format "Queued background column-detail preheat for %s" table)))
          (unless started
            (clutch--ensure-column-details conn table)
            (clutch--clear-column-details-active conn)
            (when (clutch--cached-column-details conn table)
              (run-hook-with-args
               'clutch--table-metadata-updated-hook
               conn table 'column-details))
            (clutch--refresh-schema-status-ui conn)
            (clutch--drain-column-details-async conn)))))))

(defun clutch--ensure-column-details-async (conn table)
  "Queue an async column-detail fetch for TABLE on CONN when needed."
  (let ((state (plist-get (clutch--column-details-status conn table) :state))
        (ticket (clutch--begin-metadata-ticket)))
    (unless (or (clutch--cached-column-details conn table)
                (memq state '(queued loading)))
      (clutch--set-column-details-status conn table 'queued nil ticket)
      (clutch--set-column-details-queue
       conn
       (append (clutch--column-details-queue conn) (list table)))
      (clutch--drain-column-details-async conn)
      t)))

(defun clutch--ensure-table-comment (conn table &optional schema)
  "Return TABLE's comment in SCHEMA on CONN, loading it when needed.
Returns a string or nil."
  (let* ((cache (clutch--metadata-cache-for conn clutch--table-comment-cache))
         (key (clutch--table-comment-key conn table schema))
         (cached (gethash key cache 'missing)))
    (if (not (eq cached 'missing))
        cached
      (condition-case err
          (let ((comment (clutch-db-table-comment conn table schema)))
            (puthash key comment cache)
            comment)
        (clutch-db-error
         (clutch--remember-recoverable-metadata-warning
          conn "table comment" err `(:table ,table :schema ,schema))
         nil)))))

(defun clutch--ensure-table-comment-async (conn table)
  "Queue an async table-comment fetch for TABLE on CONN when needed."
  (let* ((schema (clutch-db-current-schema conn))
         (key (clutch--table-comment-key conn table schema))
         (state (plist-get (clutch--table-comment-status conn table schema) :state))
         (ticket (clutch--begin-metadata-ticket))
         (backend (clutch--metadata-debug-backend conn)))
    (unless (or (not table)
                (clutch--table-comment-cached-p conn table schema)
                (memq state '(queued loading failed)))
      (clutch--set-table-comment-status conn table 'loading nil ticket schema)
      (let ((started
             (clutch-db-table-comment-async
              conn table
              (lambda (comment)
                (if (clutch--metadata-ticket-current-p
                     conn key ticket clutch--table-comment-status-cache)
                    (progn
                      (clutch--metadata-debug-table-event
                       conn "table-comment" "success" backend table
                       (format "Loaded table comment for %s" table))
                      (let ((cache (clutch--metadata-cache-for
                                    conn clutch--table-comment-cache)))
                        (puthash key comment cache))
                      (clutch--clear-table-comment-status conn table schema)
                      (clutch--refresh-schema-status-ui conn))
                  (clutch--metadata-debug-stale-table-event
                   conn "table-comment" backend table "table-comment result")))
              (lambda (message)
                (if (clutch--metadata-ticket-current-p
                     conn key ticket clutch--table-comment-status-cache)
                    (progn
                      (clutch--set-table-comment-status
                       conn table 'failed message ticket schema)
                      (clutch--metadata-debug-table-event
                       conn "table-comment" "error" backend table message)
                      (clutch--refresh-schema-status-ui conn))
                  (clutch--metadata-debug-stale-table-event
                   conn "table-comment" backend table "table-comment error"))))))
        (when started
          (clutch--metadata-debug-table-event
           conn "table-comment" "submit" backend table
           (format "Queued background table-comment preheat for %s" table)))
        (unless started
          (condition-case err
              (let ((cache (clutch--metadata-cache-for
                            conn clutch--table-comment-cache)))
                (puthash key (clutch-db-table-comment conn table schema) cache)
                (clutch--clear-table-comment-status conn table schema))
            (clutch-db-error
             (clutch--set-table-comment-status
              conn table 'failed (error-message-string err) ticket schema)))
          (clutch--refresh-schema-status-ui conn)))
      t)))

(defun clutch--ensure-foreign-keys-async (conn table)
  "Queue an async foreign-key fetch for TABLE on CONN when needed."
  (let ((state (plist-get (clutch--foreign-keys-status conn table) :state))
        (ticket (clutch--begin-metadata-ticket))
        (backend (clutch--metadata-debug-backend conn)))
    (unless (or (not table)
                (clutch--foreign-keys-cached-p conn table)
                (memq state '(queued loading failed)))
      (clutch--set-foreign-keys-status conn table 'loading nil ticket)
      (let ((started
             (clutch-db-foreign-keys-async
              conn table
              (lambda (fks)
                (if (clutch--metadata-ticket-current-p
                     conn table ticket clutch--foreign-keys-status-cache)
                    (progn
                      (clutch--metadata-debug-table-event
                       conn "foreign-keys" "success" backend table
                       (format "Loaded %d foreign keys for %s"
                               (length fks) table))
                      (let ((cache (clutch--metadata-cache-for
                                    conn clutch--foreign-keys-cache)))
                        (puthash table fks cache))
                      (clutch--clear-foreign-keys-status conn table)
                      (run-hook-with-args
                       'clutch--table-metadata-updated-hook
                       conn table 'foreign-keys)
                      (clutch--refresh-schema-status-ui conn))
                  (clutch--metadata-debug-stale-table-event
                   conn "foreign-keys" backend table "foreign-key result")))
              (lambda (message)
                (if (clutch--metadata-ticket-current-p
                     conn table ticket clutch--foreign-keys-status-cache)
                    (progn
                      (clutch--set-foreign-keys-status conn table 'failed
                                                       message ticket)
                      (clutch--metadata-debug-table-event
                       conn "foreign-keys" "error" backend table message)
                      (clutch--remember-recoverable-metadata-warning
                       conn "foreign-key metadata"
                       `(clutch-db-error ,message)
                       `(:table ,table))
                      (clutch--refresh-schema-status-ui conn))
                  (clutch--metadata-debug-stale-table-event
                   conn "foreign-keys" backend table "foreign-key error"))))))
        (when started
          (clutch--metadata-debug-table-event
           conn "foreign-keys" "submit" backend table
           (format "Queued background foreign-key preheat for %s" table)))
        (unless started
          (let ((cache (clutch--metadata-cache-for
                        conn clutch--foreign-keys-cache)))
            (puthash table nil cache))
          (clutch--clear-foreign-keys-status conn table)))
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
  (let* ((key   (clutch--connection-key conn))
         (cache (or (gethash key clutch--help-doc-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--help-doc-cache)
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
    (when (clutch--connection-alive-p conn)
      (gethash (clutch--connection-key conn) clutch--schema-cache))))

(provide 'clutch-schema)

;;; clutch-schema.el ends here
