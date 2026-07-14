;;; clutch-object.el --- Object discovery and describe workflow -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Internal object discovery, describe, action, and Embark helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'seq)
(require 'sql)
(require 'subr-x)
(require 'transient)

(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar clutch-debug-mode nil)
(defvar-local clutch--describe-object-entry nil
  "Object entry currently displayed in a clutch describe buffer.")
(defvar clutch--object-cache (make-hash-table :test 'equal)
  "Object discovery cache keyed by connection key string.
Each value is a plist with at least :entries and :fetched-at.")
(defvar clutch--object-warmup-timers (make-hash-table :test 'equal)
  "Idle timers warming object discovery caches keyed by connection key string.")
(defvar clutch--object-warmup-generations (make-hash-table :test 'equal)
  "Warmup generations keyed by object-cache key string.")
(defvar clutch-connection)
(defvar clutch-object-warmup-idle-delay-seconds)
(defvar clutch-primary-object-types)
(defvar clutch-sql-product)

(defvar embark-default-action-overrides)
(defvar embark-target-finders)
(defvar embark-keymap-alist)
(defvar embark-around-action-hooks)
(defvar clutch--embark-setup-done nil
  "Non-nil after clutch object Embark integration has been installed.")
(defvar clutch-embark-object-actions nil
  "Embark actions for clutch objects.")
(defvar clutch-embark-target-object-actions nil
  "Embark actions for clutch objects with explicit targets.")

(declare-function clutch--bind-connection-context "clutch-connection" (conn &optional params product))
(declare-function clutch--backend-key-from-conn "clutch-connection" (conn))
(declare-function clutch--header-with-disconnect-badge "clutch-ui" (base))
(declare-function clutch--connection-alive-p "clutch-connection" (conn))
(declare-function clutch--effective-sql-product "clutch-connection" (params))
(declare-function clutch--clear-table-metadata-caches "clutch-schema" (conn table))
(declare-function clutch--cache-table-entry-comments "clutch-schema" (conn entries))
(declare-function clutch--ensure-column-details "clutch-schema" (conn table &optional strict))
(declare-function clutch--ensure-connection "clutch-connection" ())
(declare-function clutch--ensure-table-comment "clutch-schema" (conn table &optional schema))
(declare-function clutch--icon-with-face "clutch-ui"
                  (name fallback face &rest icon-args))
(declare-function clutch--json-display-mode "clutch-ui" ())
(declare-function clutch--json-metadata-text "clutch-ui" (text))
(declare-function clutch--key-hints "clutch-ui" (hints))
(declare-function clutch--message-ident "clutch-ui" (value))
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch--query-buffer-p "clutch-connection" ())
(declare-function clutch--clear-connection-problem-capture "clutch-query" (connection))
(declare-function clutch--remember-recoverable-metadata-warning "clutch-schema"
                  (connection op err &optional context))
(declare-function clutch--remember-buffer-query-error-details "clutch-query"
                  (buffer connection sql err))
(declare-function clutch--remember-debug-event "clutch-query" (&rest event))
(declare-function clutch--refresh-current-schema "clutch-schema" (&optional silent))
(declare-function clutch--warn-completion-metadata-error-once "clutch-schema" (message-text))
(declare-function clutch--warn-schema-cache-state "clutch-schema" (&optional conn))

(defun clutch--object-type-allowed-p (entry allowed-types)
  "Return non-nil when ENTRY is permitted by ALLOWED-TYPES.
When ALLOWED-TYPES is nil, allow all entries."
  (or (null allowed-types)
      (member (upcase (or (plist-get entry :type) ""))
              allowed-types)))

(defun clutch--filter-object-entries-by-types (entries allowed-types)
  "Return ENTRIES filtered by ALLOWED-TYPES.
When ALLOWED-TYPES is nil, return ENTRIES unchanged."
  (if (null allowed-types)
      entries
    (seq-filter
     (lambda (entry)
       (clutch--object-type-allowed-p entry allowed-types))
     entries)))

(defun clutch--object-cache-key (conn)
  "Return the object-discovery cache key for CONN."
  (or (clutch--connection-key conn)
      (format "%S" conn)))

(defun clutch--object-connection-alive-p (conn)
  "Return non-nil when CONN is usable for object metadata work.
Recoverable database liveness-check failures are warned once and treated as
temporarily unavailable."
  (condition-case err
      (clutch--connection-alive-p conn)
    (clutch-db-error
     (clutch--remember-recoverable-metadata-warning
      conn "object-warmup" err '(:phase "liveness"))
     (clutch--warn-completion-metadata-error-once (error-message-string err))
     nil)))

;;;; Object discovery

(defconst clutch--object-categories
  '(indexes sequences procedures functions triggers)
  "Metadata categories loaded into clutch object pickers.")

(defconst clutch--object-type-order
  '("TABLE" "VIEW" "SYNONYM" "COLLECTION" "KEY" "INDEX" "SEQUENCE"
    "PROCEDURE" "FUNCTION" "TRIGGER")
  "Display order for grouped clutch object completion candidates.")

(defvar clutch--object-affixation-metadata-limit 64
  "Maximum object candidates to enrich during one affixation call.")

(defvar-local clutch-browser-current-object nil
  "Most recently selected database object for the current buffer.")

(defvar clutch--object-action-entry nil
  "Object entry currently targeted by `clutch-act-dwim'.")

(defvar clutch--object-dispatch-entry nil
  "Dynamic object entry currently being dispatched via Embark.")

(defvar clutch--object-completion-entry-map nil
  "Hash table from the most recent object completion session.
Maps candidate strings to entry plists.  Replaced at the start of
each `clutch--object-entry-reader' call so that Embark action
hooks can resolve a target string back to an entry after the
minibuffer has been quit.")

(defun clutch--object-entry-key (entry)
  "Return a stable identity key for object ENTRY."
  (or (plist-get entry :identity)
      (list (or (plist-get entry :name) "")
            (or (plist-get entry :type) "")
            (or (plist-get entry :schema) "")
            (or (plist-get entry :source-schema) ""))))

(defun clutch--merge-object-entries (&rest entry-lists)
  "Merge ENTRY-LISTS by stable object identity."
  (let ((seen (make-hash-table :test 'equal))
        merged)
    (dolist (entries entry-lists)
      (dolist (entry entries)
        (let ((key (clutch--object-entry-key entry)))
          (unless (gethash key seen)
            (puthash key t seen)
            (push entry merged)))))
    (nreverse merged)))

(defun clutch--object-cache-entry (conn)
  "Return cached object discovery metadata for CONN, or nil."
  (and conn
       (gethash (clutch--object-cache-key conn)
                clutch--object-cache)))

(defun clutch--object-cache-entries (conn)
  "Return cached object entries for CONN, or nil."
  (plist-get (clutch--object-cache-entry conn) :entries))

(defun clutch--object-cache-loaded-categories (conn)
  "Return loaded object category symbols for CONN, or nil."
  (plist-get (clutch--object-cache-entry conn) :loaded-categories))

(defun clutch--object-cache-complete-p (conn)
  "Return non-nil when CONN has a fully warmed object cache."
  (let ((loaded (clutch--object-cache-loaded-categories conn)))
    (and loaded
         (cl-every (lambda (category)
                     (memq category loaded))
                   clutch--object-categories))))

(defun clutch--filter-object-entries-by-type (entries type)
  "Return ENTRIES filtered to normalized object TYPE."
  (seq-filter
   (lambda (entry)
     (equal (clutch--normalize-object-type (plist-get entry :type))
            (clutch--normalize-object-type type)))
   entries))

(defun clutch--object-cache-type-entries (conn type)
  "Return cached object entries for CONN filtered to TYPE, or nil."
  (when-let* ((entries (clutch--object-cache-entries conn)))
    (clutch--filter-object-entries-by-type entries type)))

(defun clutch--store-object-cache (conn entries)
  "Store object ENTRIES for CONN and return ENTRIES."
  (clutch--cache-table-entry-comments conn entries)
  (puthash (clutch--object-cache-key conn)
           (list :entries entries
                 :loaded-categories (copy-sequence clutch--object-categories)
                 :fetched-at (float-time))
           clutch--object-cache)
  entries)

(defun clutch--store-object-cache-type-entries (conn type entries)
  "Store per-type object ENTRIES for CONN and TYPE, returning ENTRIES."
  (let* ((key (clutch--object-cache-key conn))
         (cache (or (gethash key clutch--object-cache) (list)))
         (loaded (copy-sequence (plist-get cache :loaded-categories)))
         (type (clutch--normalize-object-type type))
         (category (pcase type
                     ("INDEX" 'indexes)
                     ("SEQUENCE" 'sequences)
                     ("PROCEDURE" 'procedures)
                     ("FUNCTION" 'functions)
                     ("TRIGGER" 'triggers)
                     (_ nil))))
    (let ((browseable (clutch--browseable-object-entries conn)))
      (clutch--cache-table-entry-comments conn browseable)
      (when category
        (cl-pushnew category loaded))
      (puthash key
               (list :entries (clutch--merge-object-entries
                               browseable
                               (cl-remove type (plist-get cache :entries)
                                          :key (lambda (entry)
                                                 (clutch--normalize-object-type
                                                  (plist-get entry :type)))
                                          :test #'equal)
                               entries)
                     :loaded-categories loaded
                     :fetched-at (float-time))
               clutch--object-cache))
    entries))

(defun clutch--table-like-entry-p (entry)
  "Return non-nil when ENTRY can be browsed as rows."
  (member (upcase (or (plist-get entry :type) ""))
          '("TABLE" "VIEW" "SYNONYM" "COLLECTION" "KEY")))

(defun clutch--normalize-object-type (type)
  "Return a normalized object TYPE string."
  (let ((type (upcase (or type ""))))
    (pcase type
      ("PUBLIC SYNONYM" "SYNONYM")
      (_ type))))

(defun clutch--object-type-title (type)
  "Return a plural display title for object TYPE."
  (pcase (clutch--normalize-object-type type)
    ("TABLE" "Tables")
    ("VIEW" "Views")
    ("SYNONYM" "Synonyms")
    ("COLLECTION" "Collections")
    ("KEY" "Keys")
    ("INDEX" "Indexes")
    ("SEQUENCE" "Sequences")
    ("PROCEDURE" "Procedures")
    ("FUNCTION" "Functions")
    ("TRIGGER" "Triggers")
    (_ "Objects")))

(defun clutch--object-entry-group-title (entry)
  "Return the grouped completion title for ENTRY."
  (clutch--object-type-title (plist-get entry :type)))

(defun clutch--object-type-rank (type)
  "Return the display rank for object TYPE."
  (or (seq-position clutch--object-type-order
                    (clutch--normalize-object-type type)
                    #'equal)
      most-positive-fixnum))

(defun clutch--object-category-type (category)
  "Return the normalized object TYPE string for CATEGORY."
  (pcase category
    ('indexes "INDEX")
    ('sequences "SEQUENCE")
    ('procedures "PROCEDURE")
    ('functions "FUNCTION")
    ('triggers "TRIGGER")
    (_ nil)))

(defun clutch--cancel-object-warmup (conn &optional key)
  "Cancel any pending object warmup timer for CONN or explicit KEY."
  (let ((key (or key (clutch--object-cache-key conn))))
    (when-let* ((timer (gethash key clutch--object-warmup-timers)))
      (cancel-timer timer)
      (remhash key clutch--object-warmup-timers))))

(defun clutch--object-warmup-generation (conn &optional key)
  "Return the current warmup generation for CONN or explicit KEY."
  (gethash (or key (clutch--object-cache-key conn))
           clutch--object-warmup-generations
           0))

(defun clutch--invalidate-object-warmup (conn &optional key)
  "Cancel pending warmup work and bump its generation for CONN or KEY."
  (let ((key (or key (clutch--object-cache-key conn))))
    (clutch--cancel-object-warmup conn key)
    (puthash key
             (1+ (clutch--object-warmup-generation conn key))
             clutch--object-warmup-generations)))

(defun clutch--object-warmup-current-p (conn key generation)
  "Return non-nil when CONN still owns warmup KEY and GENERATION."
  (and conn
       (clutch--object-connection-alive-p conn)
       (= generation (clutch--object-warmup-generation conn key))))

(defun clutch--object-warmup-debug-event (conn phase backend category summary)
  "Record an object warmup debug event for CATEGORY and PHASE on CONN."
  (when clutch-debug-mode
    (clutch--remember-debug-event
     :connection conn
     :op "object-warmup"
     :phase phase
     :backend backend
     :summary summary
     :context (list :object-category category))))

(defun clutch--object-warmup-stale-debug-event (conn backend category what)
  "Record a stale object warmup event.
CATEGORY, BACKEND, WHAT, and CONN describe the stale work item."
  (clutch--object-warmup-debug-event
   conn "stale-drop" backend category
   (format "Ignored stale %s warmup %s" category what)))

(defun clutch--object-warmup-success (conn key generation backend category type entries)
  "Handle successful warmup ENTRIES for CATEGORY, KEY, and BACKEND on CONN."
  (if (clutch--object-warmup-current-p conn key generation)
      (progn
        (clutch--object-warmup-debug-event
         conn "success" backend category
         (format "Loaded %d %s entries" (length entries) category))
        (clutch--store-object-cache-type-entries conn type entries)
        (clutch--schedule-object-warmup conn))
    (clutch--object-warmup-stale-debug-event conn backend category "result")))

(defun clutch--object-warmup-error (conn key generation backend category message)
  "Handle a warmup error MESSAGE for CATEGORY, KEY, and BACKEND on CONN."
  (if (clutch--object-warmup-current-p conn key generation)
      (progn
        (clutch--object-warmup-debug-event
         conn "error" backend category
         (or message (format "%s warmup failed" category)))
        (clutch--schedule-object-warmup conn))
    (clutch--object-warmup-stale-debug-event conn backend category "error")))

(defun clutch--schedule-object-warmup (conn)
  "Warm non-table object categories for CONN during idle time."
  (let* ((key (clutch--object-cache-key conn))
         (loaded (clutch--object-cache-loaded-categories conn))
         (next (seq-find (lambda (category)
                           (not (memq category loaded)))
                         clutch--object-categories))
         (generation (clutch--object-warmup-generation conn key))
         (backend (when clutch-debug-mode
                    (condition-case nil
                        (clutch--backend-key-from-conn conn)
                      (error nil)))))
    (cond
     ((or (not conn)
          (not (clutch--object-connection-alive-p conn))
          (null next))
      (clutch--cancel-object-warmup conn))
     ((gethash key clutch--object-warmup-timers)
      nil)
     (t
      (puthash
       key
       (run-with-idle-timer
       clutch-object-warmup-idle-delay-seconds nil
        (lambda ()
          (remhash key clutch--object-warmup-timers)
          (when (clutch--object-warmup-current-p conn key generation)
            (condition-case err
                (if (or (clutch-db-busy-p conn)
                        (clutch-db--foreground-busy-p conn))
                    (clutch--schedule-object-warmup conn)
                  (let ((type (clutch--object-category-type next)))
                    (unless
                        (and type
                             (let ((started
                                    (clutch-db-list-objects-async
                                     conn next
                                     (lambda (entries)
                                       (clutch--object-warmup-success
                                        conn key generation backend next type entries))
                                     (lambda (message)
                                       (clutch--object-warmup-error
                                        conn key generation backend next message)))))
                               (when (and started clutch-debug-mode)
                                 (clutch--object-warmup-debug-event
                                  conn "submit" backend next
                                  (format "Queued background object warmup for %s"
                                          next)))
                               started))
                      (progn
                        (when type
                          (clutch--object-type-entries conn type))
                        (clutch--schedule-object-warmup conn)))))
              (clutch-db-error err
               (clutch--remember-recoverable-metadata-warning
                conn "object-warmup" err (list :object-category next))
               (clutch--warn-completion-metadata-error-once
                (error-message-string err))
               (when (and conn
                          (clutch--object-connection-alive-p conn))
                 (clutch--schedule-object-warmup conn)))))))
       clutch--object-warmup-timers)))))

(defun clutch--partial-object-entries (conn)
  "Return a fast object snapshot for CONN.
Includes browseable objects immediately and any already-warmed categories."
  (let ((entries
         (clutch--merge-object-entries
          (clutch--browseable-object-entries conn)
          (clutch--object-cache-entries conn))))
    (unless (clutch--object-cache-complete-p conn)
      (clutch--schedule-object-warmup conn))
    entries))

(defun clutch--object-entries (conn &optional refresh)
  "Return cached object entries for CONN.
When REFRESH is non-nil, bypass any cached discovery snapshot."
  (if refresh
      (clutch--store-object-cache
       conn
       (apply
        #'clutch--merge-object-entries
        (append
         (list (clutch-db-list-table-entries conn)
               (clutch-db-search-table-entries conn ""))
         (mapcar (lambda (category)
                   (clutch-db-list-objects conn category))
                 clutch--object-categories))))
    (clutch--partial-object-entries conn)))

(defun clutch--object-type-entries (conn type &optional refresh)
  "Return object entries for CONN filtered to TYPE.
When REFRESH is non-nil, bypass any cached per-type entries."
  (let ((type (clutch--normalize-object-type type)))
    (or (and (not refresh)
             (clutch--object-cache-type-entries conn type))
        (clutch--store-object-cache-type-entries
         conn type
         (pcase type
           ((or "TABLE" "VIEW" "SYNONYM" "COLLECTION" "KEY")
            (clutch--filter-object-entries-by-type
             (clutch--browseable-object-entries conn)
             type))
           ("INDEX" (clutch-db-list-objects conn 'indexes))
           ("SEQUENCE" (clutch-db-list-objects conn 'sequences))
           ("PROCEDURE" (clutch-db-list-objects conn 'procedures))
           ("FUNCTION" (clutch-db-list-objects conn 'functions))
           ("TRIGGER" (clutch-db-list-objects conn 'triggers))
           (_ nil))))))

(defun clutch--object-entry-display-detail (entry duplicate-counts)
  "Return optional disambiguation detail for ENTRY.
DUPLICATE-COUNTS maps object names to the number of visible entries."
  (let* ((name (or (plist-get entry :name) ""))
         (target (clutch--object-entry-target entry))
         (schema (or (plist-get entry :schema) ""))
         (source (or (plist-get entry :source-schema) ""))
         (identity (plist-get entry :identity)))
    (when (> (gethash name duplicate-counts 0) 1)
      (cond
       ((and target (not (string-empty-p target))) target)
       ((and schema source (not (string-empty-p schema)) (not (string= schema source)))
        schema)
       ((and identity (not (string-empty-p identity))) identity)
       ((not (string-empty-p schema)) schema)
       (t nil)))))

(defun clutch--object-entry-candidate (entry duplicate-counts)
  "Return a completion candidate string for ENTRY.
DUPLICATE-COUNTS maps object names to the number of visible entries."
  (let* ((name (or (plist-get entry :name) ""))
         (count (gethash name duplicate-counts 0)))
    (if (> count 1)
        (concat name
                (propertize
                 (format "\t%s\t%s\t%s\t%s"
                         (clutch--object-entry-label entry)
                         (or (plist-get entry :schema) "")
                         (or (plist-get entry :source-schema) "")
                         (or (plist-get entry :identity) ""))
                 'invisible t))
      name)))

(defun clutch--object-entry-annotation (entry duplicate-counts)
  "Return a propertized minibuffer annotation string for object ENTRY.
Use DUPLICATE-COUNTS to disambiguate duplicates."
  (let* ((label (clutch--object-entry-label entry))
         (detail (clutch--object-entry-display-detail entry duplicate-counts)))
    (concat
     (propertize "  " 'face 'shadow)
     (propertize label
                 'face (if (equal (plist-get entry :source-schema) "PUBLIC")
                           'clutch-object-public-source-face
                         'clutch-object-source-face))
     (when detail
       (concat
        (propertize "  " 'face 'shadow)
        (propertize detail 'face 'clutch-object-type-face))))))

(defun clutch--object-entry-affixation (cands entry-map duplicate-counts)
  "Return affixated completion tuples for object CANDS.
Use ENTRY-MAP and DUPLICATE-COUNTS to build labels and annotations."
  (let* ((labels
          (mapcar (lambda (cand)
                    (clutch--object-entry-label (gethash cand entry-map)))
                  cands))
         (label-width (if labels
                          (apply #'max (mapcar #'string-width labels))
                        0)))
    (mapcar
     (lambda (cand)
       (let* ((entry (gethash cand entry-map))
              (label (clutch--object-entry-label entry))
              (detail (clutch--object-entry-display-detail entry duplicate-counts))
              (label-face (if (equal (plist-get entry :source-schema) "PUBLIC")
                              'clutch-object-public-source-face
                            'clutch-object-source-face))
              (suffix
               (concat
                (propertize "  " 'face 'shadow)
                (propertize
                 (format (format "%%-%ds" label-width) label)
                 'face label-face)
                (if detail
                    (concat
                     (propertize "  " 'face 'shadow)
                     (propertize detail 'face 'clutch-object-type-face))
                  ""))))
         (list cand "" suffix)))
     cands)))

(defun clutch--object-entry-reader (conn prompt entries &optional initial-input category)
  "Read an object entry from ENTRIES on CONN using PROMPT."
  (let* ((sorted
          (sort (copy-sequence entries)
                (lambda (a b)
                  (clutch--object-entry-key<
                   (clutch--object-entry-sort-key a)
                   (clutch--object-entry-sort-key b)))))
         (entry-map (make-hash-table :test 'equal))
         (duplicate-counts (make-hash-table :test 'equal))
         (metadata-map (make-hash-table :test 'equal))
         candidates)
    (dolist (entry sorted)
      (puthash (plist-get entry :name)
               (1+ (gethash (plist-get entry :name) duplicate-counts 0))
               duplicate-counts))
    (dolist (entry sorted)
      (let ((candidate
             (clutch--object-entry-candidate entry duplicate-counts)))
        (puthash candidate entry entry-map)
        (push candidate candidates)))
    (setq candidates (nreverse candidates))
    (cl-labels ((candidate-list () candidates)
                (metadata-entry (candidate)
                  (when-let* ((entry (gethash candidate entry-map)))
                    (or (gethash candidate metadata-map)
                        (puthash
                         candidate
                         (clutch-db-object-entry-metadata conn entry)
                         metadata-map))))
                (annotation (candidate)
                  (when-let* ((entry (metadata-entry candidate)))
                    (clutch--object-entry-annotation entry duplicate-counts)))
                (group (candidate transform)
                  (if transform
                      candidate
                    (when-let* ((entry (gethash candidate entry-map)))
                      (clutch--object-entry-group-title entry))))
                (affixate (cands)
                  (let ((display-map (make-hash-table :test 'equal))
                        (remaining clutch--object-affixation-metadata-limit))
                    (dolist (cand cands)
                      (when-let* ((entry
                                   (or (gethash cand metadata-map)
                                       (and (> remaining 0)
                                            (gethash cand entry-map)
                                            (prog1 (metadata-entry cand)
                                              (setq remaining (1- remaining))))
                                       (gethash cand entry-map))))
                        (puthash cand entry display-map)))
                    (clutch--object-entry-affixation
                     cands display-map duplicate-counts)))
                (complete (str pred action)
                  (if (eq action 'metadata)
                      `(metadata
                        ,@(when category `((category . ,category)))
                        (annotation-function . ,#'annotation)
                        (group-function . ,#'group)
                        (affixation-function . ,#'affixate)
                        (display-sort-function . identity)
                        (cycle-sort-function . identity))
                    (complete-with-action action (candidate-list) str pred))))
      (setq clutch--object-completion-entry-map entry-map)
      (let ((choice
             (completing-read
              prompt
              #'complete
              nil t initial-input)))
        (or (gethash choice entry-map)
            (user-error "Unknown clutch object: %s" choice))))))

(defun clutch--synonym-entry-p (entry)
  "Return non-nil when ENTRY is a synonym-like object."
  (string-match-p "SYNONYM" (upcase (or (plist-get entry :type) ""))))

(defun clutch--public-source-entry-p (entry)
  "Return non-nil when ENTRY comes from PUBLIC source schema."
  (string= "public" (downcase (or (plist-get entry :source-schema) ""))))

(defun clutch--preferred-object-match (matches &optional table-like-only)
  "Return the preferred entry from MATCHES, or nil when still ambiguous.
When TABLE-LIKE-ONLY is non-nil, only consider table-like matches."
  (let* ((candidates (if table-like-only
                         (seq-filter #'clutch--table-like-entry-p matches)
                       matches))
         (non-public (seq-remove #'clutch--public-source-entry-p candidates))
         (preferred (seq-remove #'clutch--synonym-entry-p non-public)))
    (cond
     ((= (length preferred) 1) (car preferred))
     ((= (length non-public) 1) (car non-public))
     ((= (length candidates) 1) (car candidates))
     (t nil))))

(defun clutch--object-matches-by-name (conn name &optional table-like-only allowed-types)
  "Return object entries on CONN whose name matches NAME.
TABLE-LIKE-ONLY and ALLOWED-TYPES narrow the result set."
  (let ((entries
         (clutch--filter-object-entries-by-types
          (if table-like-only
              (clutch--browseable-object-entries conn)
            (clutch--object-entries conn))
          allowed-types)))
    (seq-filter
     (lambda (entry)
       (and (or (not table-like-only)
                (clutch--table-like-entry-p entry))
            (string= (downcase name)
                     (downcase (or (plist-get entry :name) "")))))
     entries)))

(defun clutch--object-matches-at-point (&optional table-like-only allowed-types)
  "Return matching object entries for the symbol at point.
TABLE-LIKE-ONLY and ALLOWED-TYPES narrow the result set."
  (when-let* ((conn clutch-connection)
              ((clutch--object-connection-alive-p conn))
              (sym (thing-at-point 'symbol t)))
    (clutch--object-matches-by-name conn sym table-like-only allowed-types)))

(defun clutch--remember-current-object (entry)
  "Store ENTRY as the current object for the active buffer."
  (setq clutch-browser-current-object entry)
  entry)

(defun clutch--command-context-buffer ()
  "Return the active clutch buffer for the current command."
  (if (and (minibufferp)
           (window-live-p (minibuffer-selected-window)))
      (window-buffer (minibuffer-selected-window))
    (current-buffer)))

(defun clutch--command-connection-context ()
  "Return connection context for the current command."
  (let ((buf (clutch--command-context-buffer)))
    (list :buffer buf
          :connection (buffer-local-value 'clutch-connection buf)
          :params (buffer-local-value 'clutch--connection-params buf)
          :product (buffer-local-value 'clutch--conn-sql-product buf))))

(defun clutch-object-at-point ()
  "Return the uniquely identified object entry at point, or nil."
  (clutch--preferred-object-match (clutch--object-matches-at-point)))

(defun clutch-object-read (&optional prompt table-like-only initial-input category allowed-types)
  "Read and return a database object entry for the current connection.
PROMPT, INITIAL-INPUT, CATEGORY, and ALLOWED-TYPES customize the
reader.  When TABLE-LIKE-ONLY is non-nil, only include table-like
objects."
  (clutch--ensure-connection)
  (clutch--warn-schema-cache-state clutch-connection)
  (let* ((entries
          (clutch--filter-object-entries-by-types
           (if table-like-only
               (clutch--browseable-object-entries clutch-connection)
             (clutch--object-entries clutch-connection))
           allowed-types)))
    (clutch--remember-current-object
     (clutch--object-entry-reader
      clutch-connection
      (or prompt "Object: ")
      entries
      initial-input
      (or category 'clutch-object)))))

(defun clutch--buffer-current-object (&optional table-like-only allowed-types)
  "Return the current object associated with the command context buffer.
When TABLE-LIKE-ONLY is non-nil, only return table-like objects
allowed by ALLOWED-TYPES."
  (let* ((buf (clutch--command-context-buffer))
         (entry (and (buffer-live-p buf)
                     (buffer-local-value 'clutch-browser-current-object buf))))
    (when (and entry
               (or (not table-like-only)
                   (clutch--table-like-entry-p entry))
               (clutch--object-type-allowed-p entry allowed-types)
               (not (with-current-buffer buf
                      (or (clutch--query-buffer-p)
                          (derived-mode-p 'clutch-repl-mode)))))
      entry)))

(defun clutch--symbol-has-local-completions-p (symbol entries)
  "Return non-nil when SYMBOL prefix-matches any entry name in ENTRIES."
  (let ((downcased (downcase symbol)))
    (cl-loop for entry in entries
             thereis (string-prefix-p downcased
                                      (downcase (or (plist-get entry :name) ""))))))

(defun clutch--on-demand-object-search (conn sym table-like-only allowed-types)
  "Search for objects matching SYM on CONN beyond the local cache.
Returns a list of matching entries from table search and, when cache is
incomplete and TABLE-LIKE-ONLY is nil, from a sync-refreshed full snapshot.
Results are filtered by ALLOWED-TYPES and deduplicated."
  (let* ((table-hits (clutch-db-search-table-entries conn sym))
         (full-entries
          (when (and (not table-like-only)
                     (not (clutch--object-cache-complete-p conn)))
            (clutch--filter-object-entries-by-types
             (clutch--object-entries conn t)
             allowed-types)))
         (name-from-full
          (when full-entries
            (let ((downcased (downcase sym)))
              (seq-filter
               (lambda (e)
                 (string-prefix-p downcased
                                  (downcase (or (plist-get e :name) ""))))
               full-entries)))))
    (list :hits (clutch--filter-object-entries-by-types
                  (clutch--merge-object-entries-by-name
                   (append table-hits name-from-full))
                  allowed-types)
          :full-entries full-entries)))

(defun clutch--resolve-object-entry (prompt &optional table-like-only category allowed-types)
  "Return the object entry for PROMPT in the current buffer context.
Uses a layered resolution strategy:
1. Buffer-local current object or exact match at point
2. Multiple matches → picker with pre-fill
3. Local prefix match → pre-fill picker
4. On-demand remote search → direct return or picker with results
5. No match → picker with full candidate list, no pre-fill

TABLE-LIKE-ONLY, CATEGORY, and ALLOWED-TYPES refine the candidate set."
  (let ((current-object (clutch--buffer-current-object table-like-only allowed-types))
        (matches (clutch--object-matches-at-point table-like-only allowed-types)))
    (clutch--remember-current-object
     (cond
      (clutch--object-dispatch-entry
       clutch--object-dispatch-entry)
      ((and (eq transient-current-command 'clutch-object-actions-menu)
            clutch--object-action-entry)
       clutch--object-action-entry)
      (current-object
       current-object)
      ((clutch--preferred-object-match matches table-like-only))
      ((> (length matches) 1)
       (clutch--object-entry-reader clutch-connection
                                    prompt
                                    matches
                                    (thing-at-point 'symbol t)
                                    category))
      (t
       (clutch--ensure-connection)
       (clutch--warn-schema-cache-state clutch-connection)
       (let* ((sym (thing-at-point 'symbol t))
              (entries
               (clutch--filter-object-entries-by-types
                (if table-like-only
                    (clutch--browseable-object-entries clutch-connection)
                  (clutch--object-entries clutch-connection))
                allowed-types))
              (cat (or category 'clutch-object)))
         (cond
          ((null sym)
           (clutch--object-entry-reader clutch-connection
                                         (or prompt "Object: ") entries nil cat))
          ((clutch--symbol-has-local-completions-p sym entries)
           (clutch--object-entry-reader clutch-connection
                                         (or prompt "Object: ") entries sym cat))
          ((clutch--object-connection-alive-p clutch-connection)
           (let* ((result (clutch--on-demand-object-search
                           clutch-connection sym table-like-only allowed-types))
                  (hits (plist-get result :hits))
                  (full (plist-get result :full-entries)))
             (cond
              ((= (length hits) 1) (car hits))
              ((> (length hits) 1)
               (clutch--object-entry-reader clutch-connection prompt
                                             hits sym cat))
              (t
               (message "No matching object found for: %s" sym)
               (clutch--object-entry-reader clutch-connection
                                             (or prompt "Object: ")
                                             (or full entries) nil cat)))))
          (t
           (message "No matching object found for: %s" sym)
           (clutch--object-entry-reader clutch-connection
                                         (or prompt "Object: ") entries nil cat)))))))))

(defun clutch--object-entry-label (entry)
  "Return a compact source/type label for object ENTRY."
  (let* ((source (plist-get entry :source-schema))
         (type (clutch--object-entry-type-display entry)))
    (cond
     ((and source (not (string-empty-p type)))
      (format "%s/%s" source type))
     (source source)
     ((not (string-empty-p type)) type)
     (t ""))))

(defun clutch--object-entry-type-display (entry)
  "Return the compact type display for object ENTRY."
  (let ((type (upcase (or (plist-get entry :value-type)
                          (plist-get entry :type)
                          ""))))
    (cond
     ((equal type "PUBLIC SYNONYM") "synonym")
     ((string-empty-p type) "")
     (t (downcase type)))))

(defun clutch--object-entry-identity-key (entry)
  "Return a stable identity key for object ENTRY."
  (list (or (plist-get entry :name) "")
        (or (plist-get entry :type) "")
        (or (plist-get entry :schema) "")
        (or (plist-get entry :source-schema) "")))

(defun clutch--object-entry-sort-key (entry)
  "Return a stable sort key for ENTRY."
  (list (clutch--object-type-rank (plist-get entry :type))
        (if (equal (plist-get entry :source-schema) "PUBLIC") 1 0)
        (or (plist-get entry :source-schema) "")
        (or (plist-get entry :name) "")))

(defun clutch--object-entry-key< (left right)
  "Return non-nil when sort key LEFT should sort before RIGHT."
  (pcase-let ((`(,left-rank ,left-public ,left-schema ,left-name) left)
              (`(,right-rank ,right-public ,right-schema ,right-name) right))
    (or (< left-rank right-rank)
        (and (= left-rank right-rank)
             (or (< left-public right-public)
                 (and (= left-public right-public)
                      (or (string< left-schema right-schema)
                          (and (string= left-schema right-schema)
                               (string< left-name right-name)))))))))

(defun clutch--merge-object-entries-by-name (&rest entry-lists)
  "Merge ENTRY-LISTS by object identity, preserving the first occurrence."
  (let ((seen (make-hash-table :test 'equal))
        merged)
    (dolist (entries entry-lists)
      (dolist (entry entries)
        (let ((key (clutch--object-entry-identity-key entry)))
          (unless (gethash key seen)
            (puthash key t seen)
            (push entry merged)))))
    (nreverse merged)))

(defun clutch--object-entry-target (entry)
  "Return the target schema display for object ENTRY, or nil."
  (let ((schema (or (plist-get entry :schema) ""))
        (source (or (plist-get entry :source-schema) "")))
    (unless (or (string-empty-p schema)
                (string= schema source))
      schema)))

(defun clutch--browseable-object-entries (conn)
  "Return the base browseable object entry list for CONN.
Includes both schema/browser entries and search-discovered entries so object
selection can surface objects from different Oracle sources and types."
  (clutch--merge-object-entries-by-name
   (clutch-db-browseable-object-entries conn)))

(defun clutch--find-console-for-conn (conn)
  "Return the query console buffer that owns CONN, or nil."
  (cl-loop for buf in (buffer-list)
           when (and (with-current-buffer buf
                       (clutch--query-buffer-p))
                     (eq (buffer-local-value 'clutch-connection buf) conn))
           return buf))

(defun clutch--insert-console-sql-block (sql)
  "Insert SQL into the current console with normalized blank-line spacing."
  (let ((nonblank-re "[^[:space:]\n\r\t]"))
    (if (not (save-excursion
               (goto-char (point-min))
               (re-search-forward nonblank-re nil t)))
        (progn
          (erase-buffer)
          (insert sql))
      (let* ((pos (point))
             (before-p (save-excursion
                         (goto-char pos)
                         (re-search-backward nonblank-re nil t)))
             (after-p (save-excursion
                        (goto-char pos)
                        (re-search-forward nonblank-re nil t)))
             (before-nl
              (and before-p
                   (save-excursion
                     (goto-char pos)
                     (let ((end (point)))
                       (skip-chars-backward " \t\n")
                       (cl-count ?\n (buffer-substring-no-properties
                                      (point) end))))))
             (after-nl
              (and after-p
                   (save-excursion
                     (goto-char pos)
                     (let ((start (point)))
                       (skip-chars-forward " \t\n")
                       (cl-count ?\n (buffer-substring-no-properties
                                      start (point))))))))
        (insert (if before-p (make-string (max 0 (- 2 before-nl)) ?\n) "")
                sql
                (if after-p (make-string (max 0 (- 2 after-nl)) ?\n) ""))))))

(defun clutch--object-fqname (entry)
  "Return a display FQNAME for object ENTRY."
  (let* ((schema (or (plist-get entry :source-schema)
                     (plist-get entry :schema)))
         (name (or (plist-get entry :name) "")))
    (if (and schema (not (string-empty-p schema)))
        (format "%s.%s" schema name)
      name)))

(defun clutch--object-sql-name (conn entry)
  "Return SQL-ready object name for ENTRY on CONN.
Preserve object schema qualification, falling back to source schema only
when the real object schema is unavailable."
  (let* ((schema (or (plist-get entry :schema)
                     (plist-get entry :source-schema)))
         (name (or (plist-get entry :name) "")))
    (mapconcat
     (lambda (part) (clutch-db-escape-identifier conn part))
     (if (and schema (not (string-empty-p schema)))
         (list schema name)
       (list name))
     ".")))

(defun clutch--use-object-action-keymap ()
  "Install object action keys on top of the current local map."
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map (current-local-map))
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (use-local-map map)))

(defun clutch--show-object-text-buffer
    (conn entry text &optional params product title-suffix)
  "Display TEXT for ENTRY using CONN's object definition mode.
TITLE-SUFFIX, when non-nil, disambiguates the generated buffer name."
  (let* ((product (or product
                      (and params
                           (clutch--effective-sql-product params))
                      clutch-sql-product))
         (title (if title-suffix
                    (format "%s %s"
                            (clutch--object-fqname entry)
                            title-suffix)
                  (clutch--object-fqname entry)))
         (buf (get-buffer-create (format "*clutch: %s*" title))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (if (clutch-db-native-document-surface-p conn params)
            (clutch--json-display-mode)
          (sql-mode)
          (sql-set-product product))
        (clutch--bind-connection-context conn params product)
        (setq-local clutch-browser-current-object entry)
        (clutch--use-object-action-keymap)
        (erase-buffer)
        (insert text)
        (insert "\n")
        (font-lock-ensure)
        (setq buffer-read-only t)
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun clutch--object-type-string (entry)
  "Return ENTRY's object type string, defaulting to OBJECT."
  (let ((type (upcase (or (plist-get entry :type) ""))))
    (if (string-empty-p type) "OBJECT" type)))

(defun clutch--object-display-name (entry)
  "Return a display name for ENTRY."
  (or (plist-get entry :name) ""))

(defun clutch--object-supports-definition-p (entry)
  "Return non-nil when ENTRY supports DDL/source display."
  (member (clutch--object-type-string entry)
          '("TABLE" "VIEW" "SYNONYM" "COLLECTION" "INDEX" "SEQUENCE"
            "PROCEDURE" "FUNCTION" "TRIGGER")))

(defun clutch--document-collection-entry-p (entry)
  "Return non-nil when ENTRY is a document collection object."
  (string= (clutch--object-type-string entry) "COLLECTION"))

(defun clutch--object-supports-jump-target-p (entry)
  "Return non-nil when ENTRY supports forward relationship jumps."
  (member (clutch--object-type-string entry)
          '("SYNONYM" "INDEX" "TRIGGER")))

(defun clutch--object-summary-pairs (entry)
  "Return summary label/value pairs for object ENTRY."
  (let* ((type (clutch--object-type-string entry))
         (target-schema (plist-get entry :target-schema))
         (target-name (plist-get entry :target-name))
         (target (cond
                  ((and target-schema target-name)
                   (format "%s.%s" target-schema target-name))
                  ((plist-get entry :target-table)
                   (plist-get entry :target-table))
                  (target-name target-name)
                  (t nil))))
    (delq nil
          (list
           (cons "Name" (or (plist-get entry :name) ""))
           (cons "Type" type)
           (and (plist-get entry :source-schema)
                (cons "Source schema" (plist-get entry :source-schema)))
           (and (plist-get entry :schema)
                (not (equal (plist-get entry :schema)
                            (plist-get entry :source-schema)))
                (cons "Schema" (plist-get entry :schema)))
           (and target
                (cons (if (string= type "SYNONYM") "Target object" "Target")
                      target))
           (and (plist-get entry :status)
                (cons "Status" (plist-get entry :status)))
           (and (plist-get entry :timing)
                (cons "Timing" (plist-get entry :timing)))
           (and (plist-get entry :event)
                (cons "Event" (plist-get entry :event)))
           (and (eq (plist-get entry :unique) t)
                (cons "Unique" "yes"))
           (and (string= type "SEQUENCE")
                (cons "Range"
                      (format "%s -> %s"
                              (or (plist-get entry :min) "?")
                              (or (plist-get entry :max) "?"))))
           (and (string= type "SEQUENCE")
                (cons "Increment"
                      (format "%s" (or (plist-get entry :increment) "?"))))
           (and (string= type "SEQUENCE")
                (cons "Last value"
                      (format "%s" (or (plist-get entry :last) "?"))))
           (and (plist-get entry :identity)
                (cons "Identity" (plist-get entry :identity)))))))

(defun clutch--object-summary-lines (entry)
  "Return formatted summary lines for object ENTRY."
  (let* ((pairs (clutch--object-summary-pairs entry))
         (width (if pairs
                    (apply #'max (mapcar (lambda (pair) (length (car pair))) pairs))
                  0)))
    (mapcar (lambda (pair)
              (format (format "  %%-%ds  %%s" width)
                      (car pair)
                      (cdr pair)))
            pairs)))

(defun clutch--object-tag-string (parts)
  "Return a compact display string for non-empty PARTS."
  (string-join (seq-remove #'string-empty-p (delq nil parts)) ", "))

(defun clutch--object-format-column (column)
  "Return a display line for COLUMN."
  (let* ((name (or (plist-get column :name) ""))
         (type (or (plist-get column :type) ""))
         (fk (plist-get column :foreign-key))
         (fk-target (when fk
                      (format "FK -> %s.%s"
                              (plist-get fk :ref-table)
                              (plist-get fk :ref-column))))
         (tags (clutch--object-tag-string
                (list (unless (plist-get column :nullable) "NOT NULL")
                      (and (plist-get column :primary-key) "PK")
                      fk-target
                      (and (plist-get column :generated) "generated")
                      (and (plist-get column :default)
                           (format "default %s" (plist-get column :default)))
                      (plist-get column :comment)))))
    (string-trim-right
     (format "  %-18s  %-18s%s"
             name
             type
             (if (string-empty-p tags)
                 ""
               (format "  %s" tags))))))

(defun clutch--object-format-index-column (column)
  "Return a display line for index COLUMN."
  (string-trim-right
   (format "  %-18s  #%s  %s"
           (or (plist-get column :name) "")
           (or (plist-get column :position) "?")
           (or (plist-get column :descend) "ASC"))))

(defun clutch--object-format-routine-param (param)
  "Return a display line for routine PARAM."
  (string-trim-right
   (format "  %-18s  %-18s%s"
           (or (plist-get param :name)
               (plist-get param :mode)
               "")
           (or (plist-get param :type) "")
           (if-let* ((mode (plist-get param :mode))
                     ((not (string-empty-p mode))))
               (format "  %s" mode)
             ""))))

(defun clutch--object-related-entries (conn entry type &optional refresh)
  "Return related TYPE entries for table-like ENTRY on CONN.
When REFRESH is non-nil, bypass cached entries for TYPE."
  (when-let* ((name (plist-get entry :name))
              (objects (if refresh
                           (clutch--object-type-entries conn type t)
                         (clutch--object-entries conn))))
    (seq-filter
     (lambda (candidate)
       (and (equal (clutch--normalize-object-type (plist-get candidate :type))
                   (clutch--normalize-object-type type))
            (string= (downcase (or (plist-get candidate :target-table) ""))
                     (downcase name))))
     objects)))

(defun clutch--object-index-lines (indexes)
  "Return describe lines for INDEXES."
  (mapcar (lambda (index)
            (format "  %-18s%s"
                    (or (plist-get index :name) "")
                    (if (eq (plist-get index :unique) t)
                        "  UNIQUE"
                      "")))
          indexes))

(defun clutch--object-describe-json-text (conn entry)
  "Return pretty JSON describe text for document-backend ENTRY on CONN."
  (let* ((type (clutch--object-type-string entry))
         (name (plist-get entry :name))
         (text (pcase type
                 ("COLLECTION"
                  (clutch-db-collection-profile conn name))
                 (_
                  (clutch-db-object-definition conn entry)))))
    (clutch--json-metadata-text
     (or text
         (clutch--json-serialize-text
          (delq nil
                `((name . ,name)
                  (type . ,type)
                  ,(when (plist-get entry :schema)
                     `(database . ,(plist-get entry :schema)))))
          "document object description")))))

(defun clutch--object-describe-sections (conn entry)
  "Return describe sections for ENTRY on CONN."
  (pcase (clutch--object-type-string entry)
    ((or "TABLE" "VIEW")
     (let ((name (plist-get entry :name))
           (schema (plist-get entry :schema)))
       (delq nil
             (list
              (when-let* ((comment (and (string= (clutch--object-type-string entry) "TABLE")
                                         (clutch--ensure-table-comment
                                          conn name schema))))
                (cons "Comment" (list (format "  %s" comment))))
              (when-let* ((details (or (clutch--ensure-column-details conn name t)
                                       (clutch-db-list-columns conn name))))
                (cons "Columns"
                      (mapcar (if (and details (plist-get (car-safe details) :name))
                                  #'clutch--object-format-column
                                (lambda (column) (format "  %s" column)))
                              details)))
              (when-let* ((indexes (clutch--object-related-entries conn entry "INDEX")))
                (cons "Indexes" (clutch--object-index-lines indexes)))
              (when-let* ((triggers (clutch--object-related-entries conn entry "TRIGGER")))
                (cons "Triggers"
                      (mapcar (lambda (trigger)
                                (format "  %-18s  %s"
                                        (or (plist-get trigger :name) "")
                                        (clutch--object-tag-string
                                         (list (plist-get trigger :timing)
                                               (plist-get trigger :event)
                                               (plist-get trigger :status)))))
                              triggers)))))))
    ("COLLECTION"
     (let ((name (plist-get entry :name)))
       (delq nil
             (list
              (when-let* ((details (or (clutch--ensure-column-details conn name t)
                                       (clutch-db-list-columns conn name))))
                (cons "Fields"
                      (mapcar (if (and details (plist-get (car-safe details) :name))
                                  #'clutch--object-format-column
                                (lambda (column) (format "  %s" column)))
                              details)))
              (when-let* ((indexes (clutch--object-related-entries
                                    conn entry "INDEX" t)))
                (cons "Indexes" (clutch--object-index-lines indexes)))))))
    ("KEY"
     (when-let* ((details (clutch-db-object-details conn entry)))
       (list
        (cons "Metadata"
              (mapcar (lambda (pair)
                        (format "  %-18s  %s" (car pair) (cdr pair)))
                      details)))))
    ("INDEX"
     (when-let* ((details (clutch-db-object-details conn entry)))
       (list (cons "Columns" (mapcar #'clutch--object-format-index-column details)))))
    ((or "PROCEDURE" "FUNCTION")
     (when-let* ((details (clutch-db-object-details conn entry)))
       (list (cons "Parameters" (mapcar #'clutch--object-format-routine-param details)))))
    (_ nil)))

(defun clutch--object-describe-text (conn entry &optional params)
  "Return describe text for ENTRY on CONN."
  (if (clutch-db-native-document-surface-p conn params)
      (clutch--object-describe-json-text conn entry)
    (let* ((header (format "%s (%s)"
                           (clutch--object-fqname entry)
                           (clutch--object-type-string entry)))
           (sections (delq nil
                           (append
                            (list (cons "Summary"
                                        (clutch--object-summary-lines entry)))
                            (clutch--object-describe-sections conn entry)))))
      (string-join
       (cons
        header
        (mapcar
         (lambda (section)
           (let* ((title (car section))
                  (lines (cdr section))
                  (count (length lines))
                  (display-title (if (member title '("Summary" "Comment"))
                                     title
                                   (format "%s (%d)" title count))))
             (format "%s\n%s" display-title (string-join lines "\n"))))
         sections))
       "\n\n"))))

(defun clutch--refresh-object-describe-metadata (conn entry)
  "Invalidate CONN metadata that should be live for describing ENTRY."
  (when (member (clutch--object-type-string entry) '("TABLE" "VIEW" "COLLECTION"))
    (when-let* ((name (plist-get entry :name)))
      (clutch--clear-table-metadata-caches conn name))))

(defun clutch--fontify-object-describe ()
  "Apply lightweight highlighting to the current describe buffer."
  (let ((inhibit-read-only t))
    (save-excursion
      (goto-char (point-min))
      (when (re-search-forward "^\\(.+\\) (\\([A-Z ]+\\))$" (line-end-position) t)
        (add-face-text-property (match-beginning 1) (match-end 1)
                                'font-lock-function-name-face t)
        (add-face-text-property (match-beginning 2) (match-end 2)
                                'font-lock-type-face t))
      (goto-char (point-min))
      (while (re-search-forward "^\\([A-Z][A-Za-z ]+\\(?: ([0-9]+)\\)?\\)$" nil t)
        (unless (= (match-beginning 1) (point-min))
          (add-face-text-property (match-beginning 1) (match-end 1)
                                  'font-lock-keyword-face t)))
      (goto-char (point-min))
      (while (re-search-forward
              "^  \\([A-Za-z][A-Za-z0-9/_-]*\\(?: [A-Za-z0-9/_-]+\\)*\\)\\(?:[[:blank:]]\\{2,\\}\\|:\\|$\\)"
              nil t)
        (add-face-text-property (match-beginning 1) (match-end 1)
                                'font-lock-variable-name-face t))
      (goto-char (point-min))
      (while (re-search-forward
              "\\_<\\(PK\\|FK\\|UNIQUE\\|VALID\\|INVALID\\|ENABLED\\|DISABLED\\|ASC\\|DESC\\|RETURN\\)\\_>"
              nil t)
        (add-face-text-property (match-beginning 1) (match-end 1)
                                'font-lock-constant-face t)))))

(defun clutch--render-object-describe (conn entry &optional params product)
  "Render describe content for ENTRY using CONN."
  (let ((inhibit-read-only t)
        (json-metadata-p (clutch-db-native-document-surface-p conn params)))
    (if json-metadata-p
        (progn
          (clutch--json-display-mode)
          (clutch--use-object-action-keymap)
          (local-set-key (kbd "s") #'clutch-object-show-ddl-or-source)
          (local-set-key (kbd "g") #'clutch-describe-refresh))
      (clutch-describe-mode))
    (clutch--bind-connection-context conn params product)
    (setq-local clutch-browser-current-object entry
                clutch--describe-object-entry entry
                clutch-describe--header-base
                (let ((icon (clutch--icon-with-face '(mdicon . "nf-md-table")
                                                    "▦" 'header-line))
                      (hints (concat "["
                                     (clutch--key-hints
                                      '(("s" "Show definition")
                                        ("C-c C-o" "Object actions")
                                        ("g" "Refresh")))
                                     "]")))
                  (if (string-empty-p icon)
                      (concat " " hints)
                    (format " %s  %s" icon hints)))
                header-line-format
                '(:eval (clutch--header-with-disconnect-badge
                         clutch-describe--header-base))
                revert-buffer-function #'clutch-describe-refresh)
    (erase-buffer)
    (insert (clutch--object-describe-text conn entry params))
    (insert "\n")
    (if json-metadata-p
        (font-lock-ensure)
      (clutch--fontify-object-describe))
    (setq buffer-read-only t)
    (goto-char (point-min))))

(defun clutch--show-object-describe-buffer (conn entry &optional params product)
  "Display a describe buffer for ENTRY using CONN."
  (let ((buf (get-buffer-create
              (format "*clutch describe: %s*" (clutch--object-fqname entry)))))
    (with-current-buffer buf
      (clutch--render-object-describe conn entry params product))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun clutch--remember-object-operation-error (buffer conn entry op err)
  "Remember object-operation ERR for BUFFER on CONN while targeting ENTRY.
OP names the object workflow, such as \"describe\" or \"show-definition\"."
  (let* ((msg (error-message-string err))
         (summary (clutch--humanize-db-error msg)))
    (clutch--remember-buffer-query-error-details buffer conn nil err)
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :buffer buffer
       :connection conn
       :op op
       :phase "error"
       :backend (clutch--backend-key-from-conn conn)
       :summary summary
       :context (list :entry-name (plist-get entry :name)
                      :entry-type (plist-get entry :type))))))

(defmacro clutch--with-object-error-capture (buffer conn entry op &rest body)
  "Execute BODY; on clutch-db-error, record to BUFFER/CONN/ENTRY/OP and re-signal."
  (declare (indent 4))
  `(condition-case err
       (progn
         ,@body
         (clutch--clear-connection-problem-capture ,conn))
     (clutch-db-error
      (clutch--remember-object-operation-error ,buffer ,conn ,entry ,op err)
      (signal (car err) (cdr err)))))

;;;###autoload
(defun clutch-describe-refresh (&optional _ignore-auto _noconfirm)
  "Refresh the current describe buffer."
  (interactive)
  (unless clutch--describe-object-entry
    (user-error "No object is associated with this buffer"))
  (clutch--refresh-current-schema (not (called-interactively-p 'interactive)))
  (clutch--with-object-error-capture
      (current-buffer) clutch-connection clutch--describe-object-entry "describe"
    (clutch--refresh-object-describe-metadata clutch-connection
                                             clutch--describe-object-entry)
    (clutch--render-object-describe clutch-connection
                                    clutch--describe-object-entry
                                    clutch--connection-params
                                    clutch--conn-sql-product)))

(defvar clutch-describe-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "s") #'clutch-object-show-ddl-or-source)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "g") #'clutch-describe-refresh)
    map)
  "Keymap for `clutch-describe-mode'.")

(defvar-local clutch-describe--header-base nil
  "Cached describe header string, set during render.")

;;;###autoload
(define-derived-mode clutch-describe-mode special-mode "clutch-describe"
  "Major mode for clutch object describe buffers.")

;;;###autoload
(defun clutch-object-show-ddl-or-source (&optional entry)
  "Show DDL or source for ENTRY."
  (interactive)
  (let* ((source-buffer (current-buffer))
         (context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Show definition: ")))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection")))
         (name (clutch--object-display-name entry))
         (type (clutch--object-type-string entry)))
    (unless (clutch--object-supports-definition-p entry)
      (user-error "%s %s does not expose a definition"
                  type name))
    (clutch--with-object-error-capture source-buffer conn entry "show-definition"
      (let ((text (clutch-db-object-definition conn entry)))
        (unless text
          (user-error "DDL/source unavailable for %s %s" type name))
        (when (clutch-db-native-document-surface-p
               conn (plist-get context :params))
          (setq text (clutch--json-metadata-text text)))
        (clutch--remember-current-object entry)
        (clutch--show-object-text-buffer conn entry text
                                         (plist-get context :params)
                                         (plist-get context :product))))))

;;;###autoload
(defun clutch-object-describe (&optional entry)
  "Show a describe buffer for ENTRY."
  (interactive)
  (let* ((source-buffer (current-buffer))
         (context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Describe object: ")))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection"))))
    (clutch--remember-current-object entry)
    (clutch--with-object-error-capture source-buffer conn entry "describe"
      (clutch--refresh-object-describe-metadata conn entry)
      (clutch--show-object-describe-buffer conn entry
                                           (plist-get context :params)
                                           (plist-get context :product)))))

(defun clutch--object-browse-query (conn entry &optional params)
  "Return a query-console browse query for ENTRY on CONN."
  (or (clutch-db-object-browse-query conn entry)
      (if (clutch-db-native-document-surface-p conn params)
          (user-error "Document backend %s does not provide object browse queries"
                      (clutch--backend-key-from-conn conn))
        (format "SELECT * FROM %s;"
                (clutch--object-sql-name conn entry)))))

(defun clutch--document-object-action-context (entry action-id)
  "Return native document context for ENTRY and ACTION-ID."
  (let* ((context (clutch--command-connection-context))
         (conn (or (plist-get context :connection)
                   clutch-connection
                   (user-error "No active connection")))
         (type (clutch--object-type-string entry))
         (label (clutch--object-action-label action-id)))
    (unless (clutch--document-collection-entry-p entry)
      (user-error "%s %s does not support %s"
                  type (clutch--object-display-name entry) (downcase label)))
    (unless (clutch-db-native-document-surface-p
             conn (plist-get context :params))
      (user-error "%s requires a native document database connection" label))
    (unless (clutch-db-object-action-supported-p conn entry action-id)
      (user-error "%s %s does not support %s"
                  type (clutch--object-display-name entry) (downcase label)))
    (setq context (plist-put context :connection conn))
    (plist-put context :source-buffer (current-buffer))))

(defun clutch--document-collection-action-entry (entry action-id)
  "Return collection ENTRY for document ACTION-ID, prompting when needed."
  (or entry
      (clutch--resolve-object-entry
       (plist-get (clutch--object-action-spec action-id) :prompt)
       t nil '("COLLECTION"))))

(defun clutch--run-document-collection-action (entry action-id)
  "Run document collection ACTION-ID for ENTRY and show its metadata."
  (let* ((entry (clutch--document-collection-action-entry entry action-id))
         (spec (clutch--object-action-spec action-id))
         (context (clutch--document-object-action-context entry action-id))
         (conn (plist-get context :connection))
         (source-buffer (plist-get context :source-buffer)))
    (clutch--remember-current-object entry)
    (clutch--with-object-error-capture source-buffer conn entry
        (symbol-name action-id)
      (let ((text (clutch-db-object-action-metadata conn entry action-id)))
        (when-let* ((message (and (null text)
                                  (plist-get spec :empty-message))))
          (user-error message (clutch--object-display-name entry)))
        (clutch--show-object-text-buffer
         conn entry
         (clutch--json-metadata-text text)
         (plist-get context :params)
         (plist-get context :product)
         (plist-get spec :title-suffix))))))

;;;###autoload
(defun clutch-object-show-index-insight (&optional entry)
  "Show index definitions and usage insight for collection ENTRY."
  (interactive)
  (clutch--run-document-collection-action entry 'index-insight))

;;;###autoload
(defun clutch-object-explain-sample-query (&optional entry)
  "Explain a sample document query for collection ENTRY."
  (interactive)
  (clutch--run-document-collection-action entry 'explain-sample))

;;;###autoload
(defun clutch-object-show-validation (&optional entry)
  "Show validation metadata for collection ENTRY."
  (interactive)
  (clutch--run-document-collection-action entry 'show-validation))

;;;###autoload
(defun clutch-object-show-stats (&optional entry)
  "Show storage statistics for collection ENTRY."
  (interactive)
  (clutch--run-document-collection-action entry 'show-stats))

;;;###autoload
(defun clutch-object-browse (&optional entry)
  "Insert a row-browse query for ENTRY into a query console.
When ENTRY is nil, use the current table-like object."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Browse object: " t)))
         (type (clutch--object-type-string entry)))
    (unless (clutch--table-like-entry-p entry)
      (user-error "%s %s does not support row browsing"
                  type (clutch--object-display-name entry)))
    (clutch--remember-current-object entry)
    (let* ((conn (or clutch-connection
                     (plist-get context :connection)
                     (user-error "No active connection")))
           (sql (clutch--object-browse-query conn entry
                                             (plist-get context :params)))
           (console (or (and (clutch--query-buffer-p)
                             (eq clutch-connection conn)
                             (current-buffer))
                        (clutch--find-console-for-conn conn)
                        (user-error "No query console open for this connection"))))
      (pop-to-buffer console)
      (unless (eq (current-buffer) console)
        (goto-char (point-max)))
      (clutch--insert-console-sql-block sql))))

(defun clutch--filter-object-target-matches (entries &optional schema)
  "Return target ENTRIES, optionally preferring SCHEMA matches."
  (let ((entries (clutch--merge-object-entries entries)))
    (if (and schema (not (string-empty-p schema)))
        (or (seq-filter
             (lambda (entry)
               (let ((ds (downcase schema)))
                 (or (string= ds (downcase (or (plist-get entry :schema) "")))
                     (string= ds (downcase (or (plist-get entry :source-schema) ""))))))
             entries)
            entries)
      entries)))

(defun clutch--resolve-object-targets (conn entry)
  "Return target object entries for ENTRY on CONN."
  (let ((type (upcase (or (plist-get entry :type) ""))))
    (pcase type
      ((or "INDEX" "TRIGGER")
       (let* ((target-table (plist-get entry :target-table))
              (matches (and target-table
                            (clutch--object-matches-by-name conn target-table t))))
         (or matches
             (when target-table
               (list (list :name target-table
                           :type "TABLE"
                           :schema (plist-get entry :schema)
                           :source-schema (plist-get entry :source-schema)))))))
      ("SYNONYM"
       (when-let* ((target-name (plist-get entry :target-name)))
         (clutch--filter-object-target-matches
          (clutch--object-matches-by-name conn target-name)
          (plist-get entry :target-schema))))
      (_ nil))))

(defun clutch--read-object-target (conn entry)
  "Read a target object for ENTRY on CONN."
  (unless (clutch--object-supports-jump-target-p entry)
    (user-error "%s %s has no target object"
                (clutch--object-type-string entry)
                (clutch--object-display-name entry)))
  (let ((targets (clutch--resolve-object-targets conn entry)))
    (pcase (length targets)
      (0 (user-error "No target object for %s %s"
                     (clutch--object-type-string entry)
                     (clutch--object-display-name entry)))
      (1 (car targets))
      (_ (clutch--object-entry-reader conn "Jump to target: " targets)))))

;;;###autoload
(defun clutch-object-jump-target (&optional entry)
  "Jump from ENTRY to its target object."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Jump from object: ")))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection")))
         (target (clutch--read-object-target conn entry)))
    (clutch--remember-current-object target)
    (clutch-object-describe target)))

;;;###autoload
(defun clutch-object-default-action (&optional entry)
  "Run the default action for ENTRY."
  (interactive)
  (let ((entry (or entry
                   (clutch--resolve-object-entry "Object: "))))
    (clutch--run-object-action entry (clutch--object-default-action-id entry))))

(defun clutch--resolve-object-dwim (&optional prompt table-like-only category allowed-types)
  "Resolve a clutch object from point or prompt.
PROMPT is passed to the fallback reader.  When TABLE-LIKE-ONLY is non-nil,
only resolve table-like objects.  CATEGORY and ALLOWED-TYPES are
passed to the fallback reader."
  (clutch--resolve-object-entry (or prompt "Object: ")
                                table-like-only
                                category
                                allowed-types))

;;;###autoload
(defun clutch-copy-object-name (&optional entry)
  "Copy the object name from ENTRY to the kill ring."
  (interactive)
  (let* ((entry (or entry
                    (clutch--resolve-object-entry "Copy object name: ")))
         (name (plist-get entry :name)))
    (kill-new name)
    (clutch--remember-current-object entry)
    (message "Copied object name: %s" (clutch--message-ident name))))

;;;###autoload
(defun clutch-copy-object-fqname (&optional entry)
  "Copy the fully qualified object name from ENTRY to the kill ring."
  (interactive)
  (let* ((entry (or entry
                    (clutch--resolve-object-entry "Copy object fqname: ")))
         (fqname (clutch--object-fqname entry)))
    (kill-new fqname)
    (clutch--remember-current-object entry)
    (message "Copied object fqname: %s" (clutch--message-ident fqname))))

(defconst clutch--object-action-registry
  '((:id describe
     :key "d"
     :label "Describe object"
     :command clutch-object-describe)
    (:id show-definition
     :key "s"
     :label "Show definition"
     :command clutch-object-show-ddl-or-source
     :predicate clutch--object-supports-definition-p)
    (:id browse
     :key "b"
     :label "Browse rows"
     :command clutch-object-browse
     :predicate clutch--table-like-entry-p)
    (:id index-insight
     :key "i"
     :label "Show index insight"
     :command clutch-object-show-index-insight
     :predicate clutch--document-collection-entry-p
     :backend-action t
     :prompt "Show index insight for collection: "
     :empty-message "Index insight unavailable for %s"
     :title-suffix "index insight")
    (:id explain-sample
     :key "e"
     :label "Explain sample query"
     :command clutch-object-explain-sample-query
     :predicate clutch--document-collection-entry-p
     :backend-action t
     :prompt "Explain collection: "
     :title-suffix "explain")
    (:id show-validation
     :key "v"
     :label "Show validation"
     :command clutch-object-show-validation
     :predicate clutch--document-collection-entry-p
     :backend-action t
     :prompt "Show validation for collection: "
     :empty-message "Validation metadata unavailable for %s"
     :title-suffix "validation")
    (:id show-stats
     :key "t"
     :label "Show stats"
     :command clutch-object-show-stats
     :predicate clutch--document-collection-entry-p
     :backend-action t
     :prompt "Show stats for collection: "
     :empty-message "Collection stats unavailable for %s"
     :title-suffix "stats")
    (:id jump-target
     :key "j"
     :label "Jump target"
     :command clutch-object-jump-target
     :predicate clutch--object-supports-jump-target-p)
    (:id copy-name
     :key "n"
     :label "Copy name"
     :command clutch-copy-object-name)
    (:id copy-fqname
     :key "f"
     :label "Copy fqname"
     :command clutch-copy-object-fqname))
  "Shared object action registry for clutch object workflows.")

(defun clutch--object-action-spec (action-id)
  "Return the action spec for ACTION-ID."
  (or (seq-find (lambda (spec)
                  (eq (plist-get spec :id) action-id))
                clutch--object-action-registry)
      (error "Unknown clutch object action: %s" action-id)))

(defun clutch--object-action-label (action-id)
  "Return the display label for ACTION-ID."
  (plist-get (clutch--object-action-spec action-id) :label))

(defun clutch--object-action-command (action-id)
  "Return the command implementing ACTION-ID."
  (plist-get (clutch--object-action-spec action-id) :command))

(defun clutch--object-action-current-connection ()
  "Return the connection relevant to the current object action, or nil."
  (or (plist-get (clutch--command-connection-context) :connection)
      clutch-connection))

(defun clutch--object-action-available-p (entry action-id &optional conn)
  "Return non-nil when ACTION-ID is applicable to ENTRY on CONN."
  (let* ((spec (clutch--object-action-spec action-id))
         (predicate (plist-get spec :predicate))
         (backend-action (plist-get spec :backend-action))
         (conn (or conn
                   (and backend-action
                        (clutch--object-action-current-connection)))))
    (and (or (null predicate)
             (funcall predicate entry))
         (or (not backend-action)
             (and conn
                  (clutch-db-object-action-supported-p conn entry action-id))))))

(defun clutch--object-default-action-id (entry)
  "Return the default action id for ENTRY."
  (if (clutch--table-like-entry-p entry)
      'browse
    'show-definition))

(defun clutch--run-object-action (entry action-id)
  "Run ACTION-ID for ENTRY."
  (unless (clutch--object-action-available-p entry action-id)
    (user-error "%s does not support %s"
                (clutch--object-fqname entry)
                (downcase (clutch--object-action-label action-id))))
  (clutch--remember-current-object entry)
  (funcall (clutch--object-action-command action-id) entry))

(defun clutch--object-act-jump-target-p ()
  "Return non-nil when the current action target supports forward jumps."
  (let ((entry clutch--object-action-entry))
    (and entry
         (clutch--object-supports-jump-target-p entry))))

(defun clutch--object-act-backend-action-p (action-id)
  "Return non-nil if ACTION-ID is available for current action target."
  (let ((entry clutch--object-action-entry)
        (conn (clutch--object-action-current-connection)))
    (and entry
         conn
         (clutch--object-action-available-p entry action-id conn))))

(defun clutch--object-act-document-actions-p ()
  "Return non-nil if any document action is available for action target."
  (let ((entry clutch--object-action-entry)
        (conn (clutch--object-action-current-connection)))
    (and entry
         conn
         (seq-some (lambda (action-id)
                     (clutch--object-action-available-p entry action-id conn))
                   '(index-insight explain-sample show-validation show-stats)))))

(transient-define-prefix clutch-object-actions-menu ()
  "Transient fallback for clutch object actions."
  [["Open"
    ("d" (lambda () (clutch--object-action-label 'describe))
     clutch-object-describe)
    ("s" (lambda () (clutch--object-action-label 'show-definition))
     clutch-object-show-ddl-or-source)]
   ["Document"
    :if clutch--object-act-document-actions-p
    ("i" (lambda () (clutch--object-action-label 'index-insight))
     clutch-object-show-index-insight
     :inapt-if (lambda () (not (clutch--object-act-backend-action-p 'index-insight))))
    ("e" (lambda () (clutch--object-action-label 'explain-sample))
     clutch-object-explain-sample-query
     :inapt-if (lambda () (not (clutch--object-act-backend-action-p 'explain-sample))))
    ("v" (lambda () (clutch--object-action-label 'show-validation))
     clutch-object-show-validation
     :inapt-if (lambda () (not (clutch--object-act-backend-action-p 'show-validation))))
    ("t" (lambda () (clutch--object-action-label 'show-stats))
     clutch-object-show-stats
     :inapt-if (lambda () (not (clutch--object-act-backend-action-p 'show-stats))))]
   ["Navigate"
    :if clutch--object-act-jump-target-p
    ("j" (lambda () (clutch--object-action-label 'jump-target))
     clutch-object-jump-target)]
   ["Copy"
    ("n" (lambda () (clutch--object-action-label 'copy-name))
     clutch-copy-object-name)
    ("f" (lambda () (clutch--object-action-label 'copy-fqname))
     clutch-copy-object-fqname)]
   ["Cache"
    ("g" "Refresh schema" clutch-refresh-schema)]])

(defun clutch--present-object-actions-natively (entry)
  "Present actions for ENTRY via clutch's native action UI."
  (setq clutch--object-action-entry entry)
  (clutch--remember-current-object entry)
  (cond
   ((fboundp 'transient-setup)
    (transient-setup 'clutch-object-actions-menu)
    t)
   (t nil)))

;;;###autoload
(defun clutch-act-dwim (&optional entry)
  "Resolve ENTRY, or an object at point, and present its action UI."
  (interactive)
  (let ((entry (or entry
                   (clutch--resolve-object-dwim "Object actions for: "))))
    (unless (clutch--present-object-actions-natively entry)
      (user-error "No object action UI is available"))))

;;;###autoload
(defun clutch-jump (&optional entry)
  "Resolve ENTRY, or an object at point, and run its default action."
  (interactive)
  (let* ((prompt "Jump to object: ")
         (entry (or entry
                    (if-let* (((or (clutch--query-buffer-p)
                                   (derived-mode-p 'clutch-repl-mode)))
                              (at-point (clutch-object-at-point))
                              ((clutch--table-like-entry-p at-point))
                              ((clutch--object-type-allowed-p
                                at-point clutch-primary-object-types)))
                        (clutch-object-read
                         prompt nil
                         (or (thing-at-point 'symbol t)
                             (plist-get at-point :name))
                         nil
                         clutch-primary-object-types)
                      (clutch--resolve-object-dwim prompt
                                                   nil nil
                                                   clutch-primary-object-types)))))
    (clutch--run-object-action entry (clutch--object-default-action-id entry))))

;;;###autoload
(defun clutch-describe-dwim (&optional entry)
  "Resolve ENTRY, or an object at point, and open its describe view."
  (interactive)
  (clutch--run-object-action
   (or entry
       (clutch--resolve-object-dwim "Describe object: "))
   'describe))

(defun clutch--embark-action-specs (&optional predicate)
  "Return object actions that should appear in Embark menus.
When PREDICATE is non-nil, keep only action specs matching it."
  (seq-filter
   (lambda (spec)
     (and (not (eq (plist-get spec :id) 'browse))
          (or (null predicate)
              (funcall predicate spec))))
   clutch--object-action-registry))

(defun clutch--embark-target-type (entry)
  "Return the Embark target type for ENTRY."
  (if (clutch--object-supports-jump-target-p entry)
      'clutch-target-object
    'clutch-object))

(defun clutch--embark-command-label (cmd)
  "Return Embark display label for clutch command CMD, or nil."
  (if (eq cmd 'clutch-object-default-action)
      "Default action"
    (cl-loop for spec in clutch--object-action-registry
             when (eq cmd (plist-get spec :command))
             return (plist-get spec :label))))

(defun clutch--embark-command-name-advice (orig cmd)
  "Return a clutch-specific display name for CMD, delegating to ORIG otherwise."
  (or (clutch--embark-command-label cmd)
      (funcall orig cmd)))

;;;; Embark integration (optional)

(defun clutch--embark-object-target ()
  "Return an Embark target for the clutch object at point."
  (when (or clutch--object-dispatch-entry
            (clutch--connection-alive-p clutch-connection))
    (if clutch--object-dispatch-entry
        `(,(clutch--embark-target-type clutch--object-dispatch-entry)
          ,clutch--object-dispatch-entry)
      (pcase major-mode
        ((or 'clutch-mode 'clutch-repl-mode)
         (when-let* ((entry (clutch-object-at-point))
                     (bounds (bounds-of-thing-at-point 'symbol)))
           `(,(clutch--embark-target-type entry)
             ,entry ,(car bounds) ,(cdr bounds))))
        (_
         (when clutch-browser-current-object
           `(,(clutch--embark-target-type clutch-browser-current-object)
             ,clutch-browser-current-object)))))))

(cl-defun clutch--embark-with-resolved-entry
    (&rest rest &key target run &allow-other-keys)
  "Resolve Embark TARGET string in REST to an entry plist for action dispatch.
Looks up TARGET in `clutch--object-completion-entry-map' and binds
the result to `clutch--object-dispatch-entry' so that action commands
see the minibuffer candidate rather than the object at point.
Clears the map after resolution to prevent stale cross-session matches."
  (let* ((map clutch--object-completion-entry-map)
         (clutch--object-dispatch-entry
          (when (and (stringp target) map)
            (gethash target map))))
    (setq clutch--object-completion-entry-map nil)
    (when run (apply run rest))))

(defun clutch--embark-actions-keymap (&optional include-jump-target)
  "Return a keymap of clutch object actions for Embark.
INCLUDE-JUMP-TARGET non-nil keeps the jump-target action."
  (let ((map (make-sparse-keymap))
        (predicate (unless include-jump-target
                     (lambda (spec)
                       (not (eq (plist-get spec :id) 'jump-target))))))
    (dolist (spec (clutch--embark-action-specs predicate))
      (define-key map
                  (kbd (plist-get spec :key))
                  (plist-get spec :command)))
    map))

(defun clutch--embark-setup ()
  "Install optional Embark integration for clutch object actions."
  (unless clutch--embark-setup-done
    (setq clutch--embark-setup-done t
          clutch-embark-object-actions (clutch--embark-actions-keymap)
          clutch-embark-target-object-actions
          (clutch--embark-actions-keymap t))
    (add-to-list 'embark-default-action-overrides
                 '(clutch-object . clutch-object-default-action))
    (add-to-list 'embark-default-action-overrides
                 '(clutch-target-object . clutch-object-default-action))
    (advice-add 'embark--command-name :around #'clutch--embark-command-name-advice)
    (add-to-list 'embark-target-finders #'clutch--embark-object-target)
    (add-to-list 'embark-keymap-alist
                 '(clutch-object . clutch-embark-object-actions))
    (add-to-list 'embark-keymap-alist
                 '(clutch-target-object . clutch-embark-target-object-actions))
    ;; Resolve minibuffer candidate to entry plist before running actions.
    ;; Covers both explicit keymap actions and the default action (RET / embark-dwim).
    (dolist (cmd (cons 'clutch-object-default-action
                       (mapcar (lambda (spec) (plist-get spec :command))
                               (clutch--embark-action-specs))))
      (add-to-list 'embark-around-action-hooks
                   (list cmd #'clutch--embark-with-resolved-entry)))))

(defun clutch--embark-after-load (_file)
  "Install clutch Embark integration after loading a file when Embark is ready."
  (when (featurep 'embark)
    (remove-hook 'after-load-functions #'clutch--embark-after-load)
    (clutch--embark-setup)))

(if (featurep 'embark)
    (clutch--embark-setup)
  (add-hook 'after-load-functions #'clutch--embark-after-load))

(provide 'clutch-object)
;;; clutch-object.el ends here
