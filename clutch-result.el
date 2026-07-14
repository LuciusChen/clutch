;;; clutch-result.el --- Result buffer workflows -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Result buffer navigation, selection, copy/export, value viewing,
;; and record-buffer workflows for Clutch.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'clutch-connection)
(require 'clutch-diagnostics)
(require 'clutch-query)
(require 'clutch-object)
(require 'clutch-schema)
(require 'clutch-sql)
(require 'clutch-ui)
(require 'clutch-edit)
(require 'json)
(require 'subr-x)
(require 'transient)

(defvar clutch-connection)
(defvar clutch-agent-context-max-cell-width)
(defvar clutch-agent-context-max-result-rows)
(defvar clutch-csv-export-default-coding-system)
(defvar clutch-column-width-step)
(defvar clutch-result-window-height)
(defvar clutch-result-max-rows)
(defvar clutch--base-query)
(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar clutch--footer-base-string)
(defvar clutch--footer-cursor-cache)
(defvar clutch--footer-display-cache)
(defvar clutch--footer-filters-cache)
(defvar clutch--footer-timing-cache)
(defvar clutch--header-line-string)
(defvar clutch--last-cell-position)
(defvar clutch--last-query)
(defvar clutch--last-result-buffer)
(defvar clutch--pre-fullscreen-config)
(defvar clutch--source-window)

(defvar-local clutch--aggregate-summary nil
  "Last aggregate summary plist for result footer, or nil.
Plist keys: :label, :rows, :cells, :skipped, :sum, :avg, :min, :max, :count.")
(defvar-local clutch--dml-result nil
  "Non-nil when this result buffer shows a DML result.")
(defvar-local clutch--filter-pattern nil
  "Current client-side filter string, or nil.")
(defvar-local clutch--filtered-rows nil
  "Filtered subset of `clutch--result-rows', or nil when unfiltered.")
(defvar-local clutch--marked-rows nil
  "List of marked row indices.")
(defvar-local clutch--order-by nil
  "Current ORDER BY state as (COL-NAME . DIRECTION) or nil.")
(defvar-local clutch--local-sort-original-rows nil
  "Current page rows in database-returned order during a local sort.")
(defvar-local clutch--local-sort-column-index nil
  "Actual result column index used by the active local sort.")
(defvar-local clutch--page-current 0
  "Current data page number (0-based).")
(defvar-local clutch--page-has-more nil
  "Non-nil when one-row lookahead found rows after the current page.")
(defvar-local clutch--page-offset nil
  "Zero-based SQL offset for the first row in the current result page.
Nil means derive the offset from `clutch--page-current'.")
(defvar-local clutch--page-total-rows nil
  "Total row count from COUNT(*), or nil if not yet queried.")
(defvar-local clutch--query-elapsed nil
  "Elapsed time in seconds for the last query execution.")
(defvar-local clutch--result-source-table nil
  "Detected source table name for the current result buffer, or nil.")
(defvar-local clutch--result-server-pageable nil
  "Non-nil when server-side page navigation is safe for this result.")
(defvar-local clutch--result-server-rewritable nil
  "Non-nil when server-side sort/filter/count rewrites are safe.")
(defvar-local clutch--result-column-defs nil
  "Full column definition plists from the last result.")
(defvar-local clutch--result-columns nil
  "Column names from the last result.")
(defvar-local clutch--result-rows nil
  "Row data from the last result.")
(defvar-local clutch--result-column-details nil
  "Column detail plists aligned with `clutch--result-columns'.
Each element corresponds to the same-index column.  Nil when unavailable.")
(defvar-local clutch--active-edit-cell nil
  "Cons cell (ROW-IDX . COL-IDX) currently open in a cell edit buffer.")
(defvar-local clutch--row-identity nil
  "Row identity metadata for staging edits and deletes in the current result.")
(defvar-local clutch--row-identity-status nil
  "Row identity capability status for the current result buffer.")
(defvar-local clutch--row-identity-error-message nil
  "Row identity metadata error message for the current result buffer.")
(defvar-local clutch--sort-column nil
  "Column name currently sorted by, or nil.")
(defvar-local clutch--sort-descending nil
  "Non-nil if the current sort is descending.")
(defvar-local clutch--where-filter nil
  "Current WHERE filter string, or nil if no filter is active.")
(defvar-local clutch--refine-rect nil
  "Rectangle (ROW-INDICES . COL-INDICES) being refined, or nil.")
(defvar-local clutch--refine-excluded-rows nil
  "Row indices (0-based) excluded during refine mode.")
(defvar-local clutch--refine-excluded-cols nil
  "Column indices (0-based) excluded during refine mode.")
(defvar-local clutch--refine-overlays nil
  "Overlays created during refine mode.")
(defvar-local clutch--refine-callback nil
  "Callback called with final rect when refine is confirmed.")
(defvar-local clutch--refine-saved-mode-line nil
  "Saved `mode-line-format' to restore after refine mode exits.")

(defvar-local clutch--live-view-buffer nil
  "Live value viewer buffer attached to the current source buffer, or nil.")

(defvar-local clutch--live-view-source-buffer nil
  "Source buffer followed by the current live value viewer.")
(defvar-local clutch--live-view-frozen nil
  "Non-nil when the live value viewer is frozen.")
(defvar-local clutch--live-view-source-cell-id nil
  "Last source cell identity rendered by a live value viewer.")

(defvar-local clutch-record--result-buffer nil
  "Reference to the parent result buffer for record display.")
(defvar-local clutch-record--row-idx nil
  "Current row index being displayed in a record buffer.")
(defvar-local clutch-record--expanded-fields nil
  "List of expanded long field column indices in a record buffer.")
(defvar-local clutch-record--header-base nil
  "Cached record header string, set during render.")

(declare-function clutch--column-border-position "clutch-ui" (cidx &optional widths nw))
(declare-function clutch--column-info-message-string "clutch-ui" (info))
(declare-function clutch--column-info-string "clutch-ui" (cidx))
(declare-function clutch--dwim-bounds-at-point "clutch-query" ())
(declare-function clutch--center-column-in-window "clutch-ui" (col-idx))
(declare-function clutch--ensure-point-visible-horizontally "clutch-ui" ())
(declare-function clutch--execute "clutch-query" (sql &optional conn result-context))
(declare-function clutch--format-value "clutch-ui" (val))
(declare-function clutch--header-with-disconnect-badge "clutch-ui" (base))
(declare-function clutch--key-hints "clutch-ui" (hints))
(declare-function clutch--message-count "clutch-ui" (value))
(declare-function clutch--message-ident "clutch-ui" (value))
(declare-function clutch--message-keyword "clutch-ui" (value))
(declare-function clutch--message-literal "clutch-ui" (value))
(declare-function clutch--null-display-string "clutch-ui" ())
(declare-function clutch--delete-row-at-index "clutch-ui" (ridx))
(declare-function clutch--refresh-display "clutch-ui" ())
(declare-function clutch--refresh-footer-line "clutch-ui" ())
(declare-function clutch--schedule-column-width-refresh "clutch-ui" ())
(declare-function clutch--sync-result-cursor-ui "clutch-ui" ())
(declare-function clutch--status-separator "clutch-ui" ())
(declare-function clutch-preview-execution-sql "clutch-query" ())

;;;; Result buffer lifecycle

(defun clutch--result-column-details (conn table col-names &optional load)
  "Return detail plists aligned with result columns COL-NAMES.
Uses cached metadata for CONN/TABLE.  When LOAD is non-nil, synchronously load
missing table metadata."
  (when-let* ((details (and table
                            (or (clutch--cached-column-details conn table)
                                (and load
                                     (clutch--ensure-column-details conn table))))))
    (let ((by-name (make-hash-table :test 'equal)))
      (dolist (detail details)
        (puthash (downcase (plist-get detail :name)) detail by-name))
      (mapcar (lambda (name)
                (gethash (downcase name) by-name))
              col-names))))

(defun clutch--refresh-result-metadata-buffers (conn table)
  "Refresh cached result column metadata for live result buffers on CONN/TABLE."
  (when-let* ((conn-key (and conn (clutch--connection-key conn))))
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and (derived-mode-p 'clutch-result-mode)
                     clutch-connection
                     clutch--result-columns
                     (string= (clutch--connection-key clutch-connection) conn-key)
                     (equal clutch--result-source-table table))
            (setq-local clutch--result-column-details
                        (clutch--result-column-details
                         clutch-connection table clutch--result-columns))
            (when clutch--pending-inserts
              (clutch--refresh-display))))))))

(defun clutch--refresh-result-foreign-key-buffers (conn table)
  "Refresh cached foreign-key display metadata for result buffers on CONN/TABLE."
  (when-let* ((conn-key (and conn (clutch--connection-key conn))))
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and (derived-mode-p 'clutch-result-mode)
                     clutch-connection
                     clutch--result-columns
                     (string= (clutch--connection-key clutch-connection) conn-key)
                     (equal clutch--result-source-table table))
            (setq-local clutch--fk-info
                        (clutch--foreign-key-column-info
                         clutch-connection table clutch--result-columns))
            (clutch--refresh-display)))))))

(defun clutch--handle-table-metadata-updated (conn table kind)
  "Refresh result UI for CONN/TABLE metadata KIND."
  (pcase kind
    ('column-details
     (clutch--refresh-result-metadata-buffers conn table))
    ('foreign-keys
     (clutch--refresh-result-foreign-key-buffers conn table))))

(add-hook 'clutch--table-metadata-updated-hook
          #'clutch--handle-table-metadata-updated)

(defun clutch-result--show-buffer (buf)
  "Display BUF in the result window slot.
Reuses the existing result window when one is visible, replacing its
buffer in place.  Creates a new window below `clutch--source-window'
when no result window exists yet."
  (let ((result-win
         (cl-find-if (lambda (w)
                       (string-prefix-p "*clutch-result:"
                                        (buffer-name (window-buffer w))))
                     (window-list nil 'no-minibuf))))
    (if result-win
        (progn
          (set-window-buffer result-win buf)
          (select-window result-win))
      (pop-to-buffer buf `(display-buffer-in-direction
                           (window . ,(or clutch--source-window
                                          (selected-window)))
                           (direction . below)
                           (window-height . ,clutch-result-window-height))))))

(defun clutch-result--buffer-name ()
  "Return the result buffer name based on current connection.
Uses the full connection key so each console gets its own result buffer."
  (if (clutch--connection-alive-p clutch-connection)
      (format "*clutch-result: %s*" (clutch--connection-key clutch-connection))
    "*clutch-result: results*"))

(defun clutch-result--server-pageable-p ()
  "Return non-nil when server-side page navigation is safe here."
  (and (local-variable-p 'clutch--result-server-pageable (current-buffer))
       clutch--result-server-pageable))

(defun clutch-result--server-rewritable-p ()
  "Return non-nil when server-side sort/filter/count rewrites are safe here."
  (and (local-variable-p 'clutch--result-server-rewritable (current-buffer))
       clutch--result-server-rewritable))

(defconst clutch-result--action-requirements
  '((sql-staged . (:surface sql))
    (sql-mutation . (:surface sql))
    (copy-insert . (:surface sql))
    (copy-update . (:surface sql))
    (export-insert . (:surface sql))
    (export-update . (:surface sql))
    (document-insert-one . (:surface document :mutation insert-one))
    (document-insert-many . (:surface document :mutation insert-many))
    (document-replace-one . (:surface document :mutation replace-one))
    (document-delete-one . (:surface document :mutation delete-one))
    (document-update-one-set . (:surface document :mutation update-one-set))
    (export-document-insert-many . (:surface document :mutation insert-many)))
  "Result action requirements keyed by logical action symbol.")

(defun clutch-result--action-requirement (action)
  "Return requirement plist for result ACTION."
  (or (alist-get action clutch-result--action-requirements)
      (error "Unknown result action: %s" action)))

(defun clutch-result--action-supported-p (action)
  "Return non-nil when result ACTION is available in the current buffer."
  (let* ((req (clutch-result--action-requirement action))
         (surface (plist-get req :surface))
         (mutation (plist-get req :mutation)))
    (pcase surface
      ('sql
       (clutch-db-sql-surface-p clutch-connection clutch--connection-params))
      ('document
       (and (clutch-db-native-document-surface-p
             clutch-connection clutch--connection-params)
            (or (null mutation)
                (clutch-db-document-mutation-supported-p
                 clutch-connection mutation))))
      (_
       (error "Unknown result action surface: %s" surface)))))

(defun clutch-result--require-action (action op)
  "Signal unless result ACTION is available for OP."
  (unless (clutch-result--action-supported-p action)
    (pcase (plist-get (clutch-result--action-requirement action) :surface)
      ('sql
       (user-error "%s is SQL-only and is not available for non-SQL results"
                   op))
      (_
       (user-error "%s is not available for this result" op)))))

(defun clutch-result--document-source-collection (op)
  "Return source collection for document OP, or signal a user error."
  (or clutch--result-source-table
      (user-error "Cannot %s: result has no source collection" op)))

(defun clutch-result--document-source-column-index ()
  "Return the hidden source-document column index, or nil."
  (cl-position-if (lambda (col)
                    (plist-get col :document-source))
                  clutch--result-column-defs))

(defun clutch-result--document-source-documents (rows op)
  "Return original backend documents from ROWS for document OP."
  (let ((idx (or (clutch-result--document-source-column-index)
                 (user-error
                  "Cannot %s: result has no source document metadata" op))))
    (cl-loop for row in rows
             collect (nth idx row))))

(defun clutch-result--query-plan (base &optional filter)
  "Return SQL execution plan for BASE with optional FILTER.
The plan keeps the user-visible SQL and the internal row-identity SQL together
so callers cannot apply WHERE before hidden identity columns are injected."
  (when base
    (let* ((prep (if clutch--row-identity
                     (clutch--prepare-row-identity-query
                      clutch-connection base clutch--row-identity
                      clutch--result-source-table)
                   (list :sql base
                         :table clutch--result-source-table
                         :identity-status clutch--row-identity-status
                         :identity-error-message
                         clutch--row-identity-error-message)))
           (sql (if filter (clutch-db-apply-where clutch-connection base filter) base)))
      (list :sql sql
            :row-identity-prep
            (if filter
                (plist-put (copy-sequence prep)
                           :sql (clutch-db-apply-where
                                 clutch-connection (plist-get prep :sql) filter))
              prep)))))

(defun clutch-result--current-query-plan ()
  "Return SQL execution plan for the current result workflow."
  (clutch-result--query-plan
   (or clutch--base-query clutch--last-query)
   (and clutch--base-query clutch--where-filter)))

(defun clutch-result--effective-query ()
  "Return the effective SQL for the current result workflow."
  (plist-get (clutch-result--current-query-plan) :sql))

(defun clutch-result--pending-changes-p ()
  "Return non-nil for staged row mutations in the current result buffer."
  (or clutch--pending-edits
      clutch--pending-deletes
      clutch--pending-inserts))

(defun clutch-result--staged-transient-heading ()
  "Return the transient heading for staged row mutations."
  (let ((count (+ (length clutch--pending-edits)
                  (length clutch--pending-deletes)
                  (length clutch--pending-inserts))))
    (if (zerop count)
        "Staged"
      (concat "Staged ("
              (propertize (format "%d pending" count) 'face 'warning)
              ")"))))

(defun clutch-result--confirm-discard-pending (prompt cancel-message)
  "Ask before discarding staged result mutations.
PROMPT is passed to `yes-or-no-p'.  Signal `user-error' with CANCEL-MESSAGE
when the user declines."
  (when (and (clutch-result--pending-changes-p)
             (not (yes-or-no-p prompt)))
    (user-error "%s" cancel-message)))

(defun clutch-result--check-pending-changes ()
  "Prompt to discard staged row mutations in the current connection result buffer.
Signal `user-error' if the user declines."
  (when-let* ((result-buf (get-buffer (clutch-result--buffer-name))))
    (with-current-buffer result-buf
      (clutch-result--confirm-discard-pending
       "Discard staged changes and re-run query? " "Execution cancelled"))))

(defun clutch-result--preview-execution-sql ()
  "Return the SQL that would execute from a result buffer."
  (if (clutch-result--pending-changes-p)
      (string-trim-right (clutch-result--pending-sql-content))
    (clutch-result--effective-query)))

(defun clutch-result--clear-staged-state ()
  "Clear staged mutation and row selection state in the current result buffer."
  (setq-local clutch--pending-edits nil
              clutch--pending-deletes nil
              clutch--pending-inserts nil
              clutch--marked-rows nil))

(defun clutch-result--split-page-lookahead-rows (rows page-size)
  "Return (VISIBLE-ROWS . HAS-MORE) from ROWS after PAGE-SIZE lookahead trimming."
  (let ((has-more (> (length rows) page-size)))
    (cons (if has-more
              (cl-subseq rows 0 page-size)
            rows)
          has-more)))

(defun clutch-result--install-page-state
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
         (prep-identity-status (plist-get row-identity-prep :identity-status))
         (row-identity-status
          (cond
           (row-identity 'available)
           ((eq prep-identity-status 'error) 'error)
           ((or (plist-get row-identity-prep :table)
                clutch--result-source-table)
            'unsupported)))
         (column-names (clutch-db-result-column-names column-defs))
         (existing-widths clutch--column-widths)
         (offset (or page-offset (* page-num clutch-result-max-rows)))
         (existing-offset (or clutch--page-offset
                              (* clutch--page-current clutch-result-max-rows)))
         (same-columns (and (vectorp existing-widths)
                            (equal column-names clutch--result-columns)
                            (= (length existing-widths)
                               (length column-names))))
         (same-render-shape
          (and same-columns
               (equal column-defs clutch--result-column-defs)))
         (same-cache-page
          (and same-render-shape
               (= offset existing-offset)))
         (column-widths
          (if same-columns
              existing-widths
            (clutch--compute-column-widths column-names rows column-defs))))
    (setq-local clutch--dml-result nil
                clutch--result-columns column-names
                clutch--result-column-defs column-defs
                clutch--row-identity row-identity
                clutch--row-identity-status row-identity-status
                clutch--row-identity-error-message
                (and (eq row-identity-status 'error)
                     (plist-get row-identity-prep :identity-error-message))
                clutch--result-rows rows
                clutch--page-current page-num
                clutch--page-offset offset
                clutch--page-has-more page-has-more
                clutch--query-elapsed elapsed
                clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--column-widths column-widths
                clutch--column-pixel-widths nil
                clutch--column-pixel-metric nil
                clutch--column-pixel-logical-widths nil
                clutch--cell-render-cache
                (and same-cache-page clutch--cell-render-cache)
                clutch--cell-render-cache-signature
                (and same-cache-page clutch--cell-render-cache-signature)
                clutch--char-pixel-width-cache
                (and same-render-shape clutch--char-pixel-width-cache)
                clutch--char-pixel-width-cache-signature
                (and same-render-shape clutch--char-pixel-width-cache-signature))
    (clutch-result--clear-staged-state)
    column-names))

(defun clutch-result--init-state (conn sql columns rows elapsed
                                       &optional row-identity-prep
                                       page-offset page-has-more
                                       server-pageable server-rewritable
                                       source-table)
  "Initialize buffer-local state for a fresh query result.
CONN is the connection, SQL the original query, COLUMNS and ROWS
the result data, ELAPSED the query time.  ROW-IDENTITY-PREP describes any
hidden row identity columns in COLUMNS.  PAGE-OFFSET is the zero-based row
offset for ROWS, and PAGE-HAS-MORE records one-row lookahead.
SERVER-PAGEABLE, SERVER-REWRITABLE, and SOURCE-TABLE describe whether clutch
may treat the result as a re-executable relation source.
Returns column names."
  (clutch-result--reset-state)
  (setq-local clutch--last-query sql
              clutch--base-query sql
              clutch-connection conn
              clutch--result-source-table source-table
              clutch--result-server-pageable server-pageable
              clutch--result-server-rewritable server-rewritable
              clutch--page-total-rows (and (not server-pageable)
                                           (length rows)))
  (clutch-result--install-page-state
   columns rows elapsed 0 row-identity-prep page-offset page-has-more))

(defun clutch-result--display-select
    (connection sql result elapsed row-identity-prep server-pageable
                result-context source-buffer)
  "Display SELECT RESULT for SQL on CONNECTION in a result buffer.
ROW-IDENTITY-PREP, SERVER-PAGEABLE, RESULT-CONTEXT, SOURCE-BUFFER, and ELAPSED
are produced by the query execution layer."
  (let* ((page-size clutch-result-max-rows)
         (buf (get-buffer-create (clutch-result--buffer-name)))
         (params clutch--connection-params)
         (product clutch--conn-sql-product)
         (raw-columns (clutch-db-result-columns result))
         (columns (clutch--apply-row-identity-column-metadata
                   raw-columns row-identity-prep))
         (visible-columns
          (cl-remove-if (lambda (col) (plist-get col :hidden)) columns))
         (server-rewritable
          (if (plist-member result-context :server-rewritable)
              (plist-get result-context :server-rewritable)
            (and server-pageable
                 (clutch--server-rewritable-result-p sql visible-columns))))
         (source-table (or (plist-get result-context :source-table)
                           (and server-rewritable
                                (plist-get row-identity-prep :table))))
         (page (if server-pageable
                   (clutch-result--split-page-lookahead-rows
                    (clutch-db-result-rows result) page-size)
                 (cons (clutch-db-result-rows result) nil)))
         (rows (car page))
         (has-more (cdr page))
         col-names)
    (with-current-buffer buf
      (clutch-result-mode)
      (clutch--bind-connection-context connection params product)
      (setq col-names
            (clutch-result--init-state
             connection sql raw-columns rows elapsed
             row-identity-prep 0 has-more
             server-pageable server-rewritable source-table))
      (clutch--load-fk-info))
    (when (buffer-live-p source-buffer)
      (with-current-buffer source-buffer
        (setq-local clutch--last-result-buffer buf)))
    (clutch-result--show-buffer buf)
    (when col-names
      (with-current-buffer buf
        (clutch--refresh-display)))
    buf))

(defun clutch-result--execute-page (page-num &optional page-offset)
  "Execute PAGE-NUM and refresh the current result buffer.
PAGE-OFFSET, when non-nil, overrides PAGE-NUM for last-window pagination."
  (unless (clutch-result--server-pageable-p)
    (user-error "Server-side pagination is not available for this query result"))
  (let* ((plan (clutch-result--current-query-plan))
         (source-buffer (current-buffer))
         (effective-sql (plist-get plan :sql))
         (page-size clutch-result-max-rows)
         (offset (or page-offset (* page-num page-size)))
         (fetch-size (1+ page-size)))
    (unless effective-sql
      (user-error "Pagination not available for this query"))
    (clutch--ensure-connection)
    (clutch-result--confirm-discard-pending
     "Discard staged changes and change page? " "Page change cancelled")
    (clutch-db-with-foreground-connection clutch-connection
      (let* ((row-identity-prep (plist-get plan :row-identity-prep))
             (identity-sql (plist-get row-identity-prep :sql))
             (paged-sql (clutch-db-build-paged-sql
                         clutch-connection identity-sql page-num fetch-size
                         clutch--order-by offset))
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
                          (user-error "%s" (clutch--debug-workflow-message summary))))))
             (elapsed (- (float-time) start))
             (page (clutch-result--split-page-lookahead-rows
                    (clutch-db-result-rows result) page-size))
             (rows (car page))
             (has-more (cdr page)))
        (clutch-result--install-page-state
         (clutch-db-result-columns result) rows elapsed
         page-num row-identity-prep offset has-more)
        (when (and clutch--sort-column (null clutch--order-by))
          (setq clutch--local-sort-original-rows
                (copy-sequence clutch--result-rows))
          (clutch-result--sort-local-page
           clutch--sort-column clutch--sort-descending
           clutch--local-sort-column-index))
        (clutch--refresh-display)
        (message "Rows %s loaded (%s, %s row%s)"
                 (clutch--message-count
                  (format "%d-%d" (if rows (1+ offset) 0)
                          (+ offset (length rows))))
                 (clutch--message-literal (clutch--format-elapsed elapsed))
                 (clutch--message-count (length rows))
                 (if (= (length rows) 1) "" "s"))))))

(defun clutch-result--execute-page-at-offset (page-offset &optional page-num)
  "Execute result page for PAGE-OFFSET as its first row offset.
PAGE-NUM records the logical page index for navigation when provided."
  (clutch-result--execute-page
   (or page-num
       (if (> clutch-result-max-rows 0)
           (floor (max 0 page-offset) clutch-result-max-rows)
         0))
   page-offset))

(defun clutch-result--reset-state (&optional dml)
  "Clear result-buffer state before rendering a fresh result.
When DML is non-nil, mark the buffer as a non-tabular result."
  (setq-local clutch--dml-result dml
              clutch--base-query nil
              clutch--result-source-table nil
              clutch--result-server-pageable nil
              clutch--result-server-rewritable nil
              clutch--column-widths nil
              clutch--column-pixel-widths nil
              clutch--column-pixel-metric nil
              clutch--column-pixel-logical-widths nil
              clutch--cell-render-cache nil
              clutch--cell-render-cache-signature nil
              clutch--char-pixel-width-cache nil
              clutch--char-pixel-width-cache-signature nil
              clutch--result-columns nil
              clutch--result-column-defs nil
              clutch--result-rows nil
              clutch--result-column-details nil
              clutch--row-identity nil
              clutch--row-identity-status nil
              clutch--row-identity-error-message nil
              clutch--sort-column nil
              clutch--sort-descending nil
              clutch--order-by nil
              clutch--local-sort-original-rows nil
              clutch--local-sort-column-index nil
              clutch--page-current 0
              clutch--page-offset nil
              clutch--page-has-more nil
              clutch--page-total-rows nil
              clutch--query-elapsed nil
              clutch--filter-pattern nil
              clutch--filtered-rows nil
              clutch--where-filter nil
              clutch--aggregate-summary nil
              clutch--last-cell-position nil
              clutch--header-active-col nil
              clutch--header-line-string nil
              clutch--footer-base-string nil
              clutch--footer-display-cache nil
              clutch--footer-timing-cache nil
              clutch--footer-cursor-cache nil
              clutch--footer-filters-cache nil)
  (clutch-result--clear-staged-state)
  (setq header-line-format nil)
  (kill-local-variable 'mode-line-format))

(defun clutch-result--display-dml (result sql elapsed)
  "Render a DML RESULT (INSERT/UPDATE/DELETE) with SQL and ELAPSED time."
  (let ((inhibit-read-only t))
    (erase-buffer)
    (clutch-result--reset-state t)
    (insert (propertize (format "-- %s\n" (string-trim sql))
                        'face 'font-lock-comment-face))
    (insert (format "Affected rows: %s\n"
                    (or (clutch-db-result-affected-rows result) 0)))
    (when-let* ((id (clutch-db-result-last-insert-id result))
                ((> id 0)))
      (insert (format "Last insert ID: %s\n" id)))
    (when-let* ((w (clutch-db-result-warnings result))
                ((> w 0)))
      (insert (format "Warnings: %s\n" w)))
    (insert (propertize (format "\nCompleted in %s\n"
                                (clutch--format-elapsed elapsed))
                        'face 'font-lock-comment-face))
    (goto-char (point-min))))

(defun clutch-result--display-error (connection sql summary message
                                                &optional elapsed hint)
  "Render a SQL execution error for CONNECTION.
SQL is the user-visible statement, SUMMARY is the humanized message,
MESSAGE is the raw backend message, ELAPSED is the failed duration, and HINT
is an optional actionable hint."
  (let* ((buf-name (clutch-result--buffer-name))
         (buf (get-buffer-create buf-name))
         (params clutch--connection-params)
         (product clutch--conn-sql-product)
         (summary (string-trim (or summary "")))
         (message (string-trim (or message "")))
         (hint (string-trim (or hint "")))
         (headline (cond
                    ((not (string-empty-p summary)) summary)
                    ((not (string-empty-p message)) message)
                    (t "SQL execution failed")))
         (sql (string-trim (or sql ""))))
    (with-current-buffer buf
      (clutch-result-mode)
      (clutch--bind-connection-context connection params product)
      (setq-local clutch--last-query sql)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (clutch-result--reset-state t)
        (setq-local truncate-lines nil
                    word-wrap t)
        (insert (propertize headline 'face 'clutch-error-summary-face)
                "\n")
        (unless (string-empty-p hint)
          (insert "\n"
                  (propertize "Hint: " 'face 'shadow)
                  hint
                  "\n"))
        (when elapsed
          (insert "\n"
                  (propertize "Failed in " 'face 'shadow)
                  (clutch--format-elapsed elapsed)
                  "\n"))
        (goto-char (point-min))))
    (clutch-result--show-buffer buf)
    buf))

(defun clutch-result--display (result sql elapsed)
  "Display RESULT in the result buffer.
SQL is the query text, ELAPSED the time in seconds.
If the result has columns, shows a table; otherwise shows DML summary."
  (let* ((buf-name (clutch-result--buffer-name))
         (buf      (get-buffer-create buf-name))
         (params clutch--connection-params)
         (product clutch--conn-sql-product)
         (columns  (clutch-db-result-columns result)))
    (if columns
        (clutch-result--display-select
         (clutch-db-result-connection result) sql result elapsed
         nil nil nil (current-buffer))
      (with-current-buffer buf
        (clutch-result-mode)
        (setq-local clutch--last-query sql)
        (clutch--bind-connection-context
         (clutch-db-result-connection result)
         params
         product)
        (clutch-result--display-dml result sql elapsed))
      (clutch-result--show-buffer buf))))

;;;; Cell navigation

(defun clutch--previous-property-run-beginning (prop)
  "Return the beginning of the previous non-nil PROP run before point."
  (let ((pos (point))
        found)
    (while (and (not found) (> pos (point-min)))
      (setq pos (previous-single-property-change pos prop nil (point-min)))
      (when (get-text-property (max (point-min) (1- pos)) prop)
        (setq found
              (previous-single-property-change (1- pos) prop nil
                                               (point-min)))))
    found))

;;;###autoload
(defun clutch-result-next-cell ()
  "Move point to the next cell (right, then wrap to next row)."
  (interactive)
  (let ((start (point)))
    (goto-char (next-single-property-change (point) 'clutch-col-idx
                                            nil (point-max)))
    (if-let* ((m (text-property-search-forward 'clutch-col-idx nil
                                               (lambda (_val cur) cur))))
        (goto-char (prop-match-beginning m))
      (goto-char start)))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-prev-cell ()
  "Move point to the previous cell (left, then wrap to prev row)."
  (interactive)
  (let ((start (point)))
    (when-let* ((beg (previous-single-property-change
                      (1+ (point)) 'clutch-col-idx nil (point-min))))
      (goto-char beg))
    (if-let* ((beg (clutch--previous-property-run-beginning 'clutch-col-idx)))
        (goto-char beg)
      (goto-char start)))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-down-cell ()
  "Move to the same column in the next row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx)))
    (clutch--goto-cell (1+ ridx) cidx))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-up-cell ()
  "Move to the same column in the previous row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx))
              ((> ridx 0)))
    (clutch--goto-cell (1- ridx) cidx))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

(defun clutch-result--rows-for-display-indices (indices)
  "Return visible result rows at display INDICES."
  (let ((rows (clutch--result-display-rows)))
    (mapcar (lambda (ridx) (nth ridx rows)) indices)))

(defun clutch-result--discard-pending-at (ridx cidx)
  "Discard the staged change for RIDX and CIDX in the current result buffer."
  (let* ((display-rows (clutch--result-display-rows))
         (nrows (length display-rows)))
    (cond
     ((>= ridx nrows)
      (let ((iidx (- ridx nrows)))
        (setq clutch--pending-inserts
              (delq (nth iidx clutch--pending-inserts) clutch--pending-inserts))
        (clutch--delete-row-at-index ridx)
        (clutch--refresh-footer-line)
        (force-mode-line-update)
        (message "Staged insert discarded")))
     (t
      (let* ((row-identity clutch--row-identity)
             (row (nth ridx display-rows))
             (identity-vec (when row-identity
                             (clutch-db-row-identity-values
                              row row-identity)))
             (edit-key (and identity-vec cidx (cons identity-vec cidx)))
             (was-edit (and edit-key
                            (cl-assoc edit-key clutch--pending-edits :test #'equal)))
             (was-delete (and identity-vec
                              (cl-find identity-vec clutch--pending-deletes
                                       :test #'equal))))
        (cond
         (was-edit
          (setq clutch--pending-edits
                (cl-remove edit-key clutch--pending-edits :test #'equal :key #'car))
          (clutch--replace-row-at-index ridx)
          (clutch--refresh-footer-line)
          (force-mode-line-update)
          (message "Staged edit discarded"))
         (was-delete
          (setq clutch--pending-deletes
                (cl-remove identity-vec clutch--pending-deletes :test #'equal))
          (clutch--replace-row-at-index ridx)
          (clutch--refresh-footer-line)
          (force-mode-line-update)
          (message "Staged deletion discarded"))
         (t
          (user-error "No staged change at point"))))))))

;;;###autoload
(defun clutch-result-discard-pending-at-point ()
  "Discard the staged change at point."
  (interactive)
  (if (derived-mode-p 'clutch-record-mode)
      (pcase-let* ((`(,ridx ,cidx ,_) (or (clutch--cell-at-point)
                                         (user-error "No field at point")))
                   (result-buf (or (and (buffer-live-p clutch-record--result-buffer)
                                        clutch-record--result-buffer)
                                   (user-error "Result buffer no longer exists"))))
        (with-current-buffer result-buf
          (clutch-result--discard-pending-at ridx cidx))
        (clutch-record--render)
        (goto-char (point-min))
        (when-let* ((match (text-property-search-forward
                            'clutch-col-idx cidx #'eq)))
          (goto-char (prop-match-beginning match))))
    (let ((ridx (or (clutch--row-idx-at-line)
                    (user-error "No row at point")))
          (cidx (clutch--col-idx-at-point)))
      (clutch-result--discard-pending-at ridx cidx))))

;;;; clutch-result-mode

(defun clutch-result-mouse-set-point (event)
  "Handle mouse EVENT without moving point below the rendered result."
  (interactive "e")
  (let* ((position (event-start event))
         (window (posn-window position))
         (buffer-position (posn-point position))
         (below-result-p
          (and (window-live-p window)
               (integer-or-marker-p buffer-position)
               (with-current-buffer (window-buffer window)
                 (and (= buffer-position (point-max))
                      (save-excursion
                        (goto-char buffer-position)
                        (bolp)))))))
    (cond
     (below-result-p (select-window window))
     ((memq 'down (event-modifiers event))
      (mouse-drag-region event))
     (t (mouse-set-point event)))))

(defvar clutch-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map [mouse-1] #'clutch-result-mouse-set-point)
    (define-key map [down-mouse-1] #'clutch-result-mouse-set-point)
    (define-key map (kbd "C-c '") #'clutch-result-edit-cell)
    (define-key map (kbd "C-c C-c") #'clutch-result-commit)
    (define-key map "g" #'clutch-result-rerun)
    (define-key map "e" #'clutch-result-export)
    (define-key map "C" #'clutch-result-goto-column)
    (define-key map "n" #'clutch-result-down-cell)
    (define-key map "p" #'clutch-result-up-cell)
    (define-key map "N" #'clutch-result-next-page)
    (define-key map "P" #'clutch-result-prev-page)
    (define-key map (kbd "M->") #'clutch-result-last-page)
    (define-key map (kbd "M-<") #'clutch-result-first-page)
    (define-key map "#" #'clutch-result-count-total)
    (define-key map "A" #'clutch-result-aggregate)
    (define-key map "s" #'clutch-result-sort-by-column)
    (define-key map "c" #'clutch-result-copy-dispatch)
    (define-key map "k" #'clutch-copy-context-for-agent)
    (define-key map "v" #'clutch-result-view-value)
    (define-key map "V" #'clutch-result-live-view-value)
    (define-key map "|" #'clutch-result-shell-command-on-cell)
    (define-key map "?" #'clutch-result-column-info)
    (define-key map "W" #'clutch-result-apply-filter)
    (define-key map (kbd "RET") #'clutch-result-open-record)
    (define-key map "]" #'clutch-result-scroll-right)
    (define-key map "[" #'clutch-result-scroll-left)
    (define-key map "=" #'clutch-result-widen-column)
    (define-key map "-" #'clutch-result-narrow-column)
    (define-key map (kbd "C-c C-p") #'clutch-preview-execution-sql)
    (define-key map "f" #'clutch-result-fullscreen-toggle)
    (define-key map (kbd "C-c ?") #'clutch-result-dispatch)
    ;; Cell navigation
    (define-key map (kbd "TAB") #'clutch-result-next-cell)
    (define-key map (kbd "<backtab>") #'clutch-result-prev-cell)
    (define-key map (kbd "M-n") #'clutch-result-down-cell)
    (define-key map (kbd "M-p") #'clutch-result-up-cell)
    ;; n/p are down/up cell (special-mode convention); M-n/M-p are aliases
    ;; Client-side filter
    (define-key map "/" #'clutch-result-filter)
    ;; Delete / Insert
    (define-key map "d" #'clutch-result-delete-rows)
    (define-key map "i" #'clutch-result-insert-row)
    (define-key map "I" #'clutch-clone-row-to-insert)
    (define-key map (kbd "C-c C-k") #'clutch-result-discard-pending-at-point)
    map)
  "Keymap for `clutch-result-mode'.")

;;;###autoload
(define-derived-mode clutch-result-mode special-mode "clutch-result"
  "Mode for displaying database query results as one scrollable table.

\\<clutch-result-mode-map>
Navigate:
  \\[clutch-result-next-cell]	Next cell (Tab)
  \\[clutch-result-prev-cell]	Previous cell (S-Tab)
  \\[clutch-result-down-cell]	Down in same column
  \\[clutch-result-up-cell]	Up in same column
  \\[clutch-result-open-record]	Open record view for row
  \\[clutch-result-goto-column]	Jump to visible column by name
Pages:
  \\[clutch-result-next-page]	Next data page
  \\[clutch-result-prev-page]	Previous data page
Navigate (row):
  \\[clutch-result-down-cell]	Next row (same column)
  \\[clutch-result-up-cell]	Previous row (same column)
  \\[clutch-result-first-page]	First data page
  \\[clutch-result-last-page]	Last data page
  \\[clutch-result-count-total]	Query total row count
  \\[clutch-result-aggregate]	Aggregate current/selected column values
  \\[clutch-result-scroll-right]	Page right (snap to next column border)
  \\[clutch-result-scroll-left]	Page left (snap to previous column border)
Copy:
  \\[clutch-result-copy-dispatch]	Copy… (transient: choose format, -r to refine)
  \\[clutch-result-export]	Export all rows (copy/file)
  \\[clutch-preview-execution-sql]	Preview execution
Inspect:
  \\[clutch-result-view-value]	View current cell once
  \\[clutch-result-live-view-value]	Open live viewer that follows point
Edit:
  \\[clutch-result-edit-cell]	Edit / re-edit at point
  \\[clutch-result-commit]	Commit staged changes
  \\[clutch-result-apply-filter]	Apply WHERE filter
  \\[clutch-result-sort-by-column]	Cycle current column sort
  \\[clutch-result-widen-column]	Widen column
  \\[clutch-result-narrow-column]	Narrow column
  \\[clutch-result-rerun]	Re-execute the query"
  (setq truncate-lines t)
  (hl-line-mode 1)
  (setq-local scroll-step 1)
  (setq-local hscroll-step 1)
  ;; Make mode-line use default background so footer renders cleanly
  (face-remap-add-relative 'mode-line :inherit 'default)
  (face-remap-add-relative 'mode-line-inactive :inherit 'default)
  (setq-local revert-buffer-function #'clutch-result--revert)
  (setq-local clutch--header-sort-function #'clutch-result--sort-by-column-index)
  (add-hook 'post-command-hook
            #'clutch--sync-result-cursor-ui nil t)
  (add-hook 'kill-buffer-hook #'clutch--result-buffer-cleanup nil t)
  (add-hook 'change-major-mode-hook #'clutch--result-buffer-cleanup nil t)
  (clutch--enable-window-size-hook))

;;;###autoload
(defun clutch-result-next-page ()
  "Go to the next data page."
  (interactive)
  (unless clutch--page-has-more
    (user-error "Already on last page"))
  (clutch-result--execute-page (1+ clutch--page-current)))

;;;###autoload
(defun clutch-result-prev-page ()
  "Go to the previous data page."
  (interactive)
  (when (<= clutch--page-current 0)
    (user-error "Already on first page"))
  (clutch-result--execute-page (1- clutch--page-current)))

;;;###autoload
(defun clutch-result-first-page ()
  "Go to the first data page."
  (interactive)
  (when (= clutch--page-current 0)
    (user-error "Already on first page"))
  (clutch-result--execute-page 0))

;;;###autoload
(defun clutch-result-last-page ()
  "Go to the last data page.
Triggers a COUNT(*) query if total rows are not yet known."
  (interactive)
  (unless clutch--page-total-rows
    (clutch-result-count-total))
  (when clutch--page-total-rows
    (let* ((page-size clutch-result-max-rows)
           (last-page (max 0 (1- (ceiling clutch--page-total-rows
                                           (float page-size)))))
           (last-offset (max 0 (- clutch--page-total-rows page-size))))
      (if (and (= clutch--page-current (truncate last-page))
               (= (or clutch--page-offset
                      (* clutch--page-current page-size))
                  last-offset))
          (user-error "Already on last page")
        (clutch-result--execute-page-at-offset last-offset (truncate last-page))))))

;;;###autoload
(defun clutch-result-count-total ()
  "Query the total row count for the current base query."
  (interactive)
  (unless (clutch-result--server-rewritable-p)
    (user-error "Server-side count is not available for this query result"))
  (let* ((conn clutch-connection)
         (base (clutch-result--effective-query)))
    (clutch--ensure-connection)
    (setq conn clutch-connection)
    (let* ((count-sql (clutch-db-build-count-sql conn base))
           (result (condition-case err
                       (clutch--run-db-query conn count-sql)
                     (clutch-db-error
                      (pcase-let ((`(,_message . ,summary)
                                   (clutch--remember-query-error
                                    (current-buffer) conn "count" count-sql err
                                    (list :generated-sql count-sql)
                                    (list :category "query" :op "count"))))
                        (user-error "%s"
                                            (clutch--debug-workflow-message
                                             (format "COUNT query error: %s"
                                                     summary)))))))
           (count-val (caar (clutch-db-result-rows result))))
      (setq-local clutch--page-total-rows
                  (if (numberp count-val) count-val
                    (string-to-number (format "%s" count-val))))
      (clutch--refresh-footer-line)
      (force-mode-line-update)
      (message "Total rows: %s"
               (clutch--message-count clutch--page-total-rows)))))

;;;###autoload
(defun clutch-result-rerun ()
  "Re-execute the last query that produced this result buffer."
  (interactive)
  (if-let* ((sql (clutch-result--effective-query)))
      (clutch--execute sql)
    (user-error "No query to re-execute")))

(defun clutch-result--revert (_ignore-auto _noconfirm)
  "Revert function for result buffer — re-executes the query."
  (clutch-result-rerun))


;;;; Sort

(defun clutch-result--client-filter-rows (rows input)
  "Return ROWS whose visible values contain INPUT."
  (let ((pattern (downcase input))
        (col-indices (clutch--visible-columns)))
    (cl-loop for row in rows
             when (cl-some
                   (lambda (cidx)
                     (when (< cidx (length row))
                       (when-let* ((val (elt row cidx)))
                         (string-match-p
                          (regexp-quote pattern)
                          (downcase (clutch--format-value val))))))
                   col-indices)
             collect row)))

(defun clutch-result--sort-local-page (col-name descending &optional col-index)
  "Sort the current page locally by COL-NAME.
When DESCENDING is non-nil, sort in descending order.  COL-INDEX identifies
the selected column when result labels are not unique."
  (let ((cidx (or col-index
                  (cl-position col-name clutch--result-columns :test #'string=))))
    (unless (and cidx
                 (equal (nth cidx clutch--result-columns) col-name)
                 (memq cidx (clutch--visible-columns)))
      (user-error "Column %s not found" col-name))
    (let ((entries
           (cl-loop for row in clutch--local-sort-original-rows
                    for value = (and (< cidx (length row)) (elt row cidx))
                    collect (cond
                             ((null value) (vector 0 nil row))
                             ((numberp value) (vector 1 value row))
                             (t (vector 2 (clutch--format-value value) row))))))
      (cl-labels
          ((entry-less-p
            (left right)
            (let ((left-rank (aref left 0))
                  (right-rank (aref right 0)))
              (cond
               ((< left-rank right-rank) t)
               ((> left-rank right-rank) nil)
               ((zerop left-rank) nil)
               ((= left-rank 1) (< (aref left 1) (aref right 1)))
               (t (string-collate-lessp
                   (aref left 1) (aref right 1)))))))
        (setq entries
              (cl-stable-sort
               entries
               (if descending
                   (lambda (left right) (entry-less-p right left))
                 #'entry-less-p))))
      (setq clutch--result-rows
            (mapcar (lambda (entry) (aref entry 2)) entries))))
  (when clutch--filter-pattern
    (setq clutch--filtered-rows
          (clutch-result--client-filter-rows
           clutch--result-rows clutch--filter-pattern)))
  (setq clutch--marked-rows nil))

(defun clutch-result--sort (col-name descending &optional col-index)
  "Sort result rows by COL-NAME.
When DESCENDING is non-nil, sort in descending order.  Safely rewritable
results use SQL ORDER BY; other results sort the currently loaded page.
COL-INDEX disambiguates duplicate result labels for local sorting."
  (unless clutch--result-columns
    (user-error "No result data"))
  (let* ((col-names (clutch--visible-column-names))
         (idx (cl-position col-name col-names :test #'string=)))
    (unless idx
      (user-error "Column %s not found" col-name))
    (let ((direction (if descending "DESC" "ASC")))
      (setq clutch--sort-column col-name
            clutch--sort-descending descending)
      (if (clutch-result--server-rewritable-p)
          (progn
            (setq clutch--order-by (cons col-name direction)
                  clutch--local-sort-original-rows nil
                  clutch--local-sort-column-index nil
                  clutch--page-current 0)
            (clutch-result--execute-page 0)
            (message "Sorted by %s %s"
                     (clutch--message-ident col-name)
                     (clutch--message-keyword direction)))
        (unless clutch--local-sort-original-rows
          (setq clutch--local-sort-original-rows
                (copy-sequence clutch--result-rows)))
        (setq clutch--order-by nil
              clutch--local-sort-column-index col-index)
        (clutch-result--sort-local-page col-name descending col-index)
        (clutch--refresh-display)
        (message "Sorted current page by %s %s"
                 (clutch--message-ident col-name)
                 (clutch--message-keyword direction))))))

(defun clutch-result--sort-by-column-index (col-idx &optional expected-name)
  "Cycle sort state for result column COL-IDX.
EXPECTED-NAME, when non-nil, is the column name captured when the header was
rendered.
The cycle is unsorted, ascending, descending, then unsorted again."
  (unless clutch--result-columns
    (user-error "No result data"))
  (let* ((visible-names (clutch--visible-column-names))
         (indexed-name (and (integerp col-idx)
                            (<= 0 col-idx)
                            (< col-idx (length clutch--result-columns))
                            (nth col-idx clutch--result-columns)))
         (col-name (if expected-name
                       (and (member expected-name visible-names)
                            expected-name)
                     indexed-name))
         (resolved-idx
          (if (and indexed-name
                   (or (null expected-name)
                       (string= indexed-name expected-name)))
              col-idx
            (cl-position col-name clutch--result-columns :test #'string=)))
         (server-sort-p (clutch-result--server-rewritable-p)))
    (unless (and col-name (member col-name visible-names))
      (user-error "Column not found"))
    (cond
     ((not (and clutch--sort-column
                (string= col-name clutch--sort-column)
                (or server-sort-p
                    (null clutch--local-sort-column-index)
                    (and (integerp resolved-idx)
                         (= resolved-idx clutch--local-sort-column-index)))))
      (clutch-result--sort col-name nil (and (not server-sort-p) resolved-idx)))
     ((not clutch--sort-descending)
      (clutch-result--sort col-name t (and (not server-sort-p) resolved-idx)))
     (t
      (let ((original-rows clutch--local-sort-original-rows))
        (setq clutch--sort-column nil
              clutch--sort-descending nil
              clutch--order-by nil
              clutch--local-sort-original-rows nil
              clutch--local-sort-column-index nil)
        (if server-sort-p
            (progn
              (setq clutch--page-current 0)
              (clutch-result--execute-page 0)
              (message "Sort cleared"))
          (unless original-rows
            (error "Local sort snapshot is missing"))
          (setq clutch--result-rows (copy-sequence original-rows)
                clutch--marked-rows nil)
          (when clutch--filter-pattern
            (setq clutch--filtered-rows
                  (clutch-result--client-filter-rows
                   clutch--result-rows clutch--filter-pattern)))
          (clutch--refresh-display)
          (message "Current-page sort cleared")))))))

(defun clutch-result--column-name-at-point ()
  "Return the visible result column name at point, or nil."
  (when-let* ((cidx (get-text-property (point) 'clutch-col-idx))
              ((integerp cidx))
              ((<= 0 cidx))
              ((< cidx (length clutch--result-columns)))
              (name (nth cidx clutch--result-columns))
              ((member name (clutch--visible-column-names))))
    name))

;;;###autoload
(defun clutch-result-sort-by-column ()
  "Cycle sort state for the result column at point."
  (interactive)
  (let ((cidx (or (get-text-property (point) 'clutch-col-idx)
                  (user-error "No column at point"))))
    (clutch-result--sort-by-column-index cidx)))

(defun clutch-result--sort-transient-description ()
  "Return the transient description for cycling result sort."
  (let ((point-col (clutch-result--column-name-at-point))
        (point-idx (get-text-property (point) 'clutch-col-idx)))
    (if point-col
        (let* ((server-sort-p (clutch-result--server-rewritable-p))
               (same-column-p
                (and clutch--sort-column
                     (string= point-col clutch--sort-column)
                     (or server-sort-p
                         (null clutch--local-sort-column-index)
                         (and (integerp point-idx)
                              (= point-idx clutch--local-sort-column-index)))))
               (state (cond
                       ((not same-column-p) 'none)
                       (clutch--sort-descending 'desc)
                       (t 'asc))))
          (concat (if (clutch-result--server-rewritable-p)
                      "Sort current "
                    "Sort page ")
                  (clutch--transient-state-display
                   state '((none . "none") (asc . "asc") (desc . "desc")))
                  (propertize (format " [%s]" point-col)
                              'face 'clutch-field-name-face)))
      "Sort current (no column)")))

;;;; WHERE filtering

(defun clutch--where-filter-column-expression (column condition &optional conn)
  "Return a WHERE fragment for COLUMN and user-entered CONDITION.
When CONN is non-nil, escape COLUMN using the backend identifier rules."
  (let ((expr (if (string-match-p
                  "\\`\\(?:[=<>!]\\|IN\\b\\|IS\\b\\|NOT\\b\\|LIKE\\b\\|BETWEEN\\b\\)"
                  (upcase condition))
                  condition
                (concat "= " condition))))
    (format "%s %s" (if conn (clutch-db-escape-identifier conn column) column)
            expr)))

(defun clutch-result--filter-transient-description (label value)
  "Return a transient filter description for LABEL and current VALUE."
  (concat
   label " "
   (clutch--transient-state-display
    (if value 'active 'none)
    '((none . "none") (active . "active")))
   (when value
     (propertize
      (format " [%s]" (truncate-string-to-width value 32 nil nil "…"))
      'face 'clutch-field-name-face))))

(defun clutch-result--client-filter-transient-description ()
  "Return the transient description for the client-side result filter."
  (clutch-result--filter-transient-description
   "Client filter" clutch--filter-pattern))

(defun clutch-result--where-filter-transient-description ()
  "Return the transient description for the SQL WHERE filter."
  (clutch-result--filter-transient-description
   "WHERE filter" clutch--where-filter))

(defun clutch--read-where-filter (current columns default-col &optional conn)
  "Read a WHERE filter string from CURRENT state, COLUMNS, and DEFAULT-COL.
CONN supplies identifier escaping for picker-built column filters.  Raw WHERE
input is passed through unchanged."
  (if (and columns (not current))
      (let* ((col (completing-read
                   (if default-col
                       (format "Filter column (default %s, empty for raw): "
                               default-col)
                     "Filter column (empty for raw): ")
                   columns nil nil nil nil default-col))
             (condition
              (string-trim
               (read-string
                (if (string-empty-p col)
                    "WHERE filter (e.g., age > 18): "
                  (format "%s (e.g., 42, 'foo', > 18, IS NULL): " col))))))
        (cond
         ((string-empty-p condition) "")
         ((string-empty-p col) condition)
         (t (clutch--where-filter-column-expression col condition conn))))
    (string-trim
     (read-string
      (if current
          (format "WHERE filter (current: %s, empty to clear): " current)
        "WHERE filter (e.g., age > 18): ")
      nil nil current))))

;;;###autoload
(defun clutch-result-apply-filter ()
  "Apply or clear a WHERE filter on the current result query.
When columns are available, prompts to pick a column first (defaulting
to the column at point), then asks for the condition.  Enter an empty
string at the column prompt to write a raw WHERE clause; enter an
empty string at the condition prompt to clear the filter."
  (interactive)
  (unless clutch--last-query
    (user-error "No query to filter"))
  (unless (clutch-result--server-rewritable-p)
    (user-error "Server-side filter is not available for this query result"))
  (let* ((base (or clutch--base-query
                   clutch--last-query))
         (source-table clutch--result-source-table)
         (server-pageable (clutch-result--server-pageable-p))
         (current clutch--where-filter)
         (visible-col-indices (clutch--visible-columns))
         (columns (clutch--column-names-for-indices visible-col-indices))
         (default-col (and clutch--header-active-col
                           (memq clutch--header-active-col visible-col-indices)
                           (nth clutch--header-active-col clutch--result-columns)))
         (input (clutch--read-where-filter current columns default-col
                                           clutch-connection))
         (filter (unless (string-empty-p input) input))
         (plan (and filter (clutch-result--query-plan base filter))))
    (clutch--execute (or (plist-get plan :sql) base)
                     clutch-connection
                     (and filter
                          (list :server-pageable server-pageable
                                :server-rewritable t
                                :source-table source-table
                                :row-identity-prep
                                (plist-get plan :row-identity-prep))))
    (setq clutch--base-query (when filter base))
    (setq clutch--where-filter filter)
    (message (if filter
                 (format "Filter applied: WHERE %s" input)
               "Filter cleared"))))

;;;; Client-side filter

(defun clutch-result--apply-filter (input)
  "Apply INPUT as a client-side substring filter and re-render."
  (let ((matching (clutch-result--client-filter-rows
                   clutch--result-rows input)))
    (setq clutch--filter-pattern input
          clutch--filtered-rows matching
          clutch--marked-rows nil)
    (clutch--render-result)
    (message "Filter: %s/%s rows match %s"
             (clutch--message-count (length matching))
             (clutch--message-count (length clutch--result-rows))
             (clutch--message-literal (format "\"%s\"" input)))))

;;;###autoload
(defun clutch-result-filter ()
  "Filter visible rows by substring match (client-side).
Prompts for a pattern; enter empty string to clear."
  (interactive)
  (let ((input (string-trim
                (read-string
                 (if clutch--filter-pattern
                     (format "Filter (current: %s, empty to clear): "
                             clutch--filter-pattern)
                   "Filter (empty to clear): ")))))
    (if (string-empty-p input)
        (progn
          (setq clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--marked-rows nil)
          (clutch--render-result)
          (message "Filter cleared"))
      (clutch-result--apply-filter input))))

;;;; Refine selection

(defvar clutch-refine-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "m") #'clutch-refine-toggle-row)
    (define-key map (kbd "x") #'clutch-refine-toggle-col)
    (define-key map (kbd "RET") #'clutch-refine-confirm)
    (define-key map (kbd "C-g") #'clutch-refine-cancel)
    map)
  "Keymap for `clutch-refine-mode'.")

;;;###autoload
(define-minor-mode clutch-refine-mode
  "Transient minor mode for visually refining a rectangular selection.
\\<clutch-refine-mode-map>
\\[clutch-refine-toggle-row]: toggle row exclusion at point
\\[clutch-refine-toggle-col]: toggle column exclusion at point
\\[clutch-refine-confirm]: confirm and execute
\\[clutch-refine-cancel]: cancel"
  :keymap clutch-refine-mode-map
  :lighter " [REFINE: m=row x=col RET=ok C-g=cancel]"
  (unless clutch-refine-mode
    (clutch-refine--clear-overlays)))

(defun clutch-refine--clear-overlays ()
  "Delete all overlays created during refine mode."
  (mapc #'delete-overlay clutch--refine-overlays)
  (setq clutch--refine-overlays nil))

(defun clutch-refine--make-overlay (beg end face priority &optional tag-prop tag-val)
  "Create a refine overlay from BEG to END with FACE and PRIORITY.
Optionally tag with TAG-PROP = TAG-VAL for incremental removal."
  (let ((ov (make-overlay beg end)))
    (overlay-put ov 'face face)
    (overlay-put ov 'priority priority)
    (when tag-prop (overlay-put ov tag-prop tag-val))
    (push ov clutch--refine-overlays)))

(defun clutch-refine--init-overlays ()
  "Apply layer-1 selection overlays for the rect.  Called once on refine start."
  (clutch-refine--clear-overlays)
  (save-excursion
    (pcase-let ((`(,row-indices . ,col-indices) clutch--refine-rect))
      (dolist (cidx col-indices)
        (goto-char (point-min))
        (cl-loop for match = (text-property-search-forward 'clutch-col-idx cidx #'eql)
                 while match
                 do (let ((beg (prop-match-beginning match))
                          (end (prop-match-end match)))
                      (when (memq (get-text-property beg 'clutch-row-idx) row-indices)
                        (clutch-refine--make-overlay beg end 'secondary-selection 0))))))))

(defun clutch-refine--add-row-exclusion (ridx)
  "Add exclusion overlays for row RIDX within the rect's columns.
Finds the row's line first, then scans only that line — O(buffer-to-row + line)."
  (save-excursion
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-row-idx ridx #'eql)))
      (goto-char (prop-match-beginning match))
      (let ((bol (line-beginning-position))
            (eol (line-end-position))
            (col-set (cdr clutch--refine-rect)))
        (cl-loop with p = bol
                 while (< p eol)
                 do (let ((cidx (get-text-property p 'clutch-col-idx)))
                      (if (and cidx (memq cidx col-set))
                          (let ((end (or (next-single-property-change
                                         p 'clutch-col-idx nil eol)
                                        eol)))
                            (clutch-refine--make-overlay
                             p end '(:inherit shadow :strike-through t) 1
                             'clutch-refine-row ridx)
                            (setq p end))
                        (setq p (1+ p)))))))))

(defun clutch-refine--remove-row-exclusion (ridx)
  "Remove exclusion overlays tagged with RIDX."
  (setq clutch--refine-overlays
        (cl-loop for ov in clutch--refine-overlays
                 if (eql (overlay-get ov 'clutch-refine-row) ridx)
                 do (delete-overlay ov)
                 else collect ov)))

(defun clutch-refine--add-col-exclusion (cidx)
  "Add exclusion overlays for column CIDX (header + rect rows).
Scans buffer once for this column — O(buffer)."
  (save-excursion
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-header-col cidx #'eql)))
      (clutch-refine--make-overlay (prop-match-beginning match) (prop-match-end match)
                                   '(:inherit shadow :strike-through t) 1
                                   'clutch-refine-col cidx))
    (goto-char (point-min))
    (cl-loop for match = (text-property-search-forward 'clutch-col-idx cidx #'eql)
             while match
             do (let ((beg (prop-match-beginning match))
                      (end (prop-match-end match)))
                  (when (memq (get-text-property beg 'clutch-row-idx)
                              (car clutch--refine-rect))
                    (clutch-refine--make-overlay beg end
                                                 '(:inherit shadow :strike-through t) 1
                                                 'clutch-refine-col cidx))))))

(defun clutch-refine--remove-col-exclusion (cidx)
  "Remove exclusion overlays tagged with CIDX."
  (setq clutch--refine-overlays
        (cl-loop for ov in clutch--refine-overlays
                 if (eql (overlay-get ov 'clutch-refine-col) cidx)
                 do (delete-overlay ov)
                 else collect ov)))

;;;###autoload
(defun clutch-refine-toggle-row ()
  "Toggle exclusion of the row at point."
  (interactive)
  (if-let* ((ridx (clutch--row-idx-at-line)))
      (if (memq ridx (car clutch--refine-rect))
          (if (memq ridx clutch--refine-excluded-rows)
              (progn
                (setq clutch--refine-excluded-rows
                      (delq ridx clutch--refine-excluded-rows))
                (clutch-refine--remove-row-exclusion ridx)
                (message "Row %s %s"
                         (clutch--message-count (1+ ridx))
                         (clutch--message-keyword "included")))
            (push ridx clutch--refine-excluded-rows)
            (clutch-refine--add-row-exclusion ridx)
            (message "Row %s %s"
                     (clutch--message-count (1+ ridx))
                     (clutch--message-keyword "excluded")))
        (user-error "Row not in selection"))
    (user-error "No row at point")))

;;;###autoload
(defun clutch-refine-toggle-col ()
  "Toggle exclusion of the column at point."
  (interactive)
  (if-let* ((cidx (or (get-text-property (point) 'clutch-col-idx)
                      (get-text-property (point) 'clutch-header-col))))
      (if (memq cidx (cdr clutch--refine-rect))
          (if (memq cidx clutch--refine-excluded-cols)
              (progn
                (setq clutch--refine-excluded-cols
                      (delq cidx clutch--refine-excluded-cols))
                (clutch-refine--remove-col-exclusion cidx)
                (message "Column %s %s"
                         (clutch--message-ident
                          (format "\"%s\"" (nth cidx clutch--result-columns)))
                         (clutch--message-keyword "included")))
            (push cidx clutch--refine-excluded-cols)
            (clutch-refine--add-col-exclusion cidx)
            (message "Column %s %s"
                     (clutch--message-ident
                      (format "\"%s\"" (nth cidx clutch--result-columns)))
                     (clutch--message-keyword "excluded")))
        (user-error "Column not in selection"))
    (user-error "No column at point")))

;;;###autoload
(defun clutch-refine-confirm ()
  "Confirm the current refine selection and execute the callback."
  (interactive)
  (let* ((row-indices (cl-loop for ridx in (car clutch--refine-rect)
                               unless (memq ridx clutch--refine-excluded-rows)
                               collect ridx))
         (col-indices (cl-loop for cidx in (cdr clutch--refine-rect)
                               unless (memq cidx clutch--refine-excluded-cols)
                               collect cidx)))
    (unless row-indices
      (user-error "No rows left after exclusion"))
    (unless col-indices
      (user-error "No columns left after exclusion"))
    (let ((cb clutch--refine-callback)
          (final-rect (cons row-indices col-indices)))
      (clutch-refine--clear-overlays)
      (clutch-refine-mode -1)
      (setq mode-line-format clutch--refine-saved-mode-line
            clutch--refine-rect nil
            clutch--refine-excluded-rows nil
            clutch--refine-excluded-cols nil
            clutch--refine-callback nil
            clutch--refine-saved-mode-line nil)
      (funcall cb final-rect))))

;;;###autoload
(defun clutch-refine-cancel ()
  "Cancel refine mode without executing the callback."
  (interactive)
  (clutch-refine--clear-overlays)
  (clutch-refine-mode -1)
  (setq mode-line-format clutch--refine-saved-mode-line
        clutch--refine-rect nil
        clutch--refine-excluded-rows nil
        clutch--refine-excluded-cols nil
        clutch--refine-callback nil
        clutch--refine-saved-mode-line nil)
  (message "Refine cancelled"))

(defun clutch-result--start-refine (rect callback)
  "Enter refine mode for RECT with CALLBACK called with final rect on confirm.
RECT is (ROW-INDICES . COL-INDICES)."
  (deactivate-mark)
  (setq-local clutch--refine-rect rect
              clutch--refine-excluded-rows nil
              clutch--refine-excluded-cols nil
              clutch--refine-callback callback
              clutch--refine-saved-mode-line mode-line-format
              mode-line-format
              (concat
               (propertize " " 'display '(space :align-to 0))
               (propertize "REFINE  " 'face 'font-lock-warning-face)
               (propertize "m" 'face 'font-lock-keyword-face)
               (propertize " row   " 'face 'font-lock-comment-face)
               (propertize "x" 'face 'font-lock-keyword-face)
               (propertize " col   " 'face 'font-lock-comment-face)
               (propertize "RET" 'face 'font-lock-keyword-face)
               (propertize " confirm   " 'face 'font-lock-comment-face)
               (propertize "C-g" 'face 'font-lock-keyword-face)
               (propertize " cancel" 'face 'font-lock-comment-face)))
  (clutch-refine-mode 1)
  (clutch-refine--init-overlays))

;;;; Copy commands

(defun clutch-result-copy (format &optional rect)
  "Unified copy entry point for result buffer.
FORMAT is one of symbols: `tsv', `csv', `org-table', `insert', `update',
`document-insert-one', `document-insert-many', `document-replace-one',
`document-delete-one', or `document-update-one-set'.
When RECT is non-nil, use it as precomputed rectangle bounds.  If region
is active, copy rectangle bounds from region endpoints.
Otherwise, copy the current cell."
  (pcase format
    ('tsv
     (if rect
         (clutch-result--yank-rectangle-cells rect)
       (if (use-region-p)
           (clutch-result--yank-region-cells)
         (pcase-let* ((`(,_ridx ,_cidx ,val) (or (clutch--cell-at-point)
                                               (user-error "No cell at point"))))
           (clutch-result--yank-cell-value val)))))
    ('csv
     (clutch-result--copy-rows 'csv rect))
    ('org-table
     (clutch-result--copy-rows 'org-table rect))
    ('insert
     (clutch-result--require-action 'copy-insert "Copy INSERT SQL")
     (clutch-result--copy-rows 'insert rect))
    ('update
     (clutch-result--require-action 'copy-update "Copy UPDATE SQL")
     (clutch-result--copy-rows 'update rect))
    ('document-insert-one
     (clutch-result--copy-rows 'document-insert-one rect))
    ('document-insert-many
     (clutch-result--copy-rows 'document-insert-many rect))
    ('document-replace-one
     (clutch-result--copy-rows 'document-replace-one rect))
    ('document-delete-one
     (clutch-result--copy-rows 'document-delete-one rect))
    ('document-update-one-set
     (clutch-result--copy-rows 'document-update-one-set rect))
    (_
     (user-error "Unsupported copy format: %s" format))))

(defclass clutch--transient-yes-no-switch (transient-switch) ()
  "Transient switch that displays its state as a highlighted No/Yes pair.")

(cl-defmethod transient-format-value ((obj clutch--transient-yes-no-switch))
  "Format OBJ's current switch value as a No/Yes state pair."
  (clutch--transient-state-display
   (if (oref obj value) 'yes 'no)
   '((no . "No") (yes . "Yes"))))

(defun clutch-result--copy-fmt (fmt)
  "Copy in FMT, entering refine mode first if --refine switch is set."
  (if (transient-arg-value "--refine" (transient-args 'clutch-result-copy-dispatch))
      (progn
        (unless (use-region-p)
          (user-error "Set a region before using refine mode"))
        (clutch-result--start-refine
         (clutch-result--region-rectangle-indices)
         (lambda (final-rect) (clutch-result-copy fmt final-rect))))
    (clutch-result-copy fmt)))

;;;###autoload
(defun clutch-result-copy-tsv ()
  "Copy as TSV."
  (interactive)
  (clutch-result--copy-fmt 'tsv))

;;;###autoload
(defun clutch-result-copy-csv ()
  "Copy as CSV with header."
  (interactive)
  (clutch-result--copy-fmt 'csv))

;;;###autoload
(defun clutch-result-copy-org-table ()
  "Copy as an Org table with header."
  (interactive)
  (clutch-result--copy-fmt 'org-table))

;;;###autoload
(defun clutch-result-copy-insert ()
  "Copy as INSERT statements."
  (interactive)
  (clutch-result--copy-fmt 'insert))

;;;###autoload
(defun clutch-result-copy-update ()
  "Copy as UPDATE statements."
  (interactive)
  (clutch-result--copy-fmt 'update))

(defun clutch-result-copy-document-insert-one ()
  "Copy selected documents as native document insert-one snippets."
  (interactive)
  (clutch-result--copy-fmt 'document-insert-one))

(defun clutch-result-copy-document-insert-many ()
  "Copy selected documents as a native document insert-many snippet."
  (interactive)
  (clutch-result--copy-fmt 'document-insert-many))

(defun clutch-result-copy-document-replace-one ()
  "Copy selected documents as native document replace-one snippets."
  (interactive)
  (clutch-result--copy-fmt 'document-replace-one))

(defun clutch-result-copy-document-delete-one ()
  "Copy selected documents as native document delete-one snippets."
  (interactive)
  (clutch-result--copy-fmt 'document-delete-one))

(defun clutch-result-copy-document-update-one-set ()
  "Copy selected fields as native document update-one snippets."
  (interactive)
  (clutch-result--copy-fmt 'document-update-one-set))

(transient-define-prefix clutch-result-copy-dispatch ()
  "Copy result buffer data.
Enable --refine to exclude rows/columns interactively before copying
\(requires an active region set with \\<global-map>\\[set-mark-command] or mouse)."
  ["Options"
   :pad-keys t
   ("-r" "Refine selection" "--refine"
    :class clutch--transient-yes-no-switch
    :format " %k %d %v")]
  ["Copy as"
   :pad-keys t
   ("t" "TSV"             clutch-result-copy-tsv)
   ("c" "CSV with header" clutch-result-copy-csv)
   ("o" "Org table"       clutch-result-copy-org-table)
   ("i" "INSERT SQL"      clutch-result-copy-insert
    :if (lambda () (clutch-result--action-supported-p 'copy-insert)))
   ("u" "UPDATE SQL"      clutch-result-copy-update
    :if (lambda () (clutch-result--action-supported-p 'copy-update)))]
  ["Document helper"
   :pad-keys t
   :if (lambda ()
         (cl-some #'clutch-result--action-supported-p
                  '(document-insert-one
                    document-insert-many
                    document-replace-one
                    document-update-one-set
                    document-delete-one)))
   ("I" "Insert one"      clutch-result-copy-document-insert-one
    :if (lambda () (clutch-result--action-supported-p 'document-insert-one)))
   ("M" "Insert many"     clutch-result-copy-document-insert-many
    :if (lambda () (clutch-result--action-supported-p 'document-insert-many)))
   ("R" "Replace one"     clutch-result-copy-document-replace-one
    :if (lambda () (clutch-result--action-supported-p 'document-replace-one)))
   ("U" "Update fields"   clutch-result-copy-document-update-one-set
    :if (lambda () (clutch-result--action-supported-p 'document-update-one-set)))
   ("D" "Delete one"      clutch-result-copy-document-delete-one
    :if (lambda () (clutch-result--action-supported-p 'document-delete-one)))])


;;;; Agent context export

(defun clutch--agent-context-current-sql ()
  "Return SQL text that should anchor an external agent context export."
  (cl-labels ((sql-from-bounds
               (bounds)
               (pcase-let ((`(,beg . ,end) bounds))
                 (string-trim (buffer-substring-no-properties beg end))))
              (dwim-sql
               ()
               (let ((sql (sql-from-bounds (clutch--dwim-bounds-at-point))))
                 (if (not (string-empty-p sql))
                     sql
                   (save-excursion
                     (skip-chars-backward " \t\n\r;")
                     (when (and (not (bobp))
                                (eq (char-after) ?\;))
                       (backward-char))
                     (when (not (eobp))
                       (sql-from-bounds (clutch--dwim-bounds-at-point))))))))
    (let ((sql
           (cond
            ((derived-mode-p 'clutch-result-mode)
             (clutch-result--effective-query))
            ((use-region-p)
             (buffer-substring-no-properties (region-beginning) (region-end)))
            ((derived-mode-p 'clutch-mode)
             (dwim-sql))
            (t clutch--last-query))))
      (when (stringp sql)
        (string-trim sql)))))

(defun clutch--agent-context-tables (sql)
  "Return table names that should be documented for SQL."
  (let ((tables (clutch--statement-table-identifiers-in-sql sql)))
    (if (and (derived-mode-p 'clutch-result-mode)
             clutch--result-source-table
             (not (clutch--identifier-match clutch--result-source-table
                                            tables)))
        (append tables (list clutch--result-source-table))
      tables)))

(defun clutch--agent-context-text (conn sql &optional tables)
  "Return Markdown context text for CONN, SQL, and optional TABLES."
  (cl-labels
      ((inline-value
        (value)
        (let ((text (replace-regexp-in-string
                     "[\n\r\t ]+" " "
                     (clutch--format-value value))))
          (setq text (string-trim text))
          (if (> (string-width text) clutch-agent-context-max-cell-width)
              (truncate-string-to-width text clutch-agent-context-max-cell-width
                                        nil nil "...")
            text)))
       (format-tsv-line
        (values)
        (mapconcat #'inline-value values "\t"))
       (row-list
        (row)
        (cond
         ((vectorp row)
          (cl-loop for i below (length row) collect (aref row i)))
         ((listp row) row)
         (t (list row))))
       (sql-match-p
        (a b)
        (let ((a (and (stringp a) (clutch-db-sql-normalize a)))
              (b (and (stringp b) (clutch-db-sql-normalize b))))
          (and a b (string= a b))))
       (matching-result-buffer
        ()
        (cond
         ((derived-mode-p 'clutch-result-mode)
          (current-buffer))
         ((and (buffer-live-p clutch--last-result-buffer)
               (with-current-buffer clutch--last-result-buffer
                 (and (derived-mode-p 'clutch-result-mode)
                      clutch--result-columns
                      (sql-match-p sql (clutch-result--effective-query)))))
          clutch--last-result-buffer)))
       (result-sample
        ()
        (when-let* ((result-buffer (matching-result-buffer)))
          (with-current-buffer result-buffer
            (when (and (derived-mode-p 'clutch-result-mode)
                       clutch--result-columns)
              (let* ((rows (clutch--result-display-rows))
                     (col-indices (clutch--visible-columns))
                     (columns (clutch--column-names-for-indices col-indices))
                     (max-rows (max 0 clutch-agent-context-max-result-rows))
                     (sample (cl-subseq rows 0 (min max-rows (length rows)))))
                (when columns
                  (concat
                   "## Result sample\n\n"
                   (format "Showing %d of %d visible rows from the latest matching result buffer.\n\n"
                           (length sample) (length rows))
                   "```text\n"
                   (format-tsv-line columns)
                   "\n"
                   (mapconcat
                    (lambda (row)
                      (let ((values (row-list row)))
                        (format-tsv-line
                         (mapcar (lambda (i) (nth i values)) col-indices))))
                    sample
                    "\n")
                   (when sample "\n")
                   "```\n\n")))))))
       (connection-section
        ()
        (let ((schema (clutch-db-current-schema conn))
              (database (clutch-db-database conn)))
          (concat "## Connection\n\n"
                  (format "- Backend: %s\n" (clutch-db-display-name conn))
                  (format "- Connection: %s\n" (clutch--connection-key conn))
                  (format "- Database: %s\n" (or database "none"))
                  (format "- Current schema/database: %s\n\n"
                          (or schema database "none")))))
       (table-section
        (table)
        (concat
         "## Table: " table "\n\n"
         (condition-case err
             (concat "```text\n"
                     (clutch--object-describe-text
                      conn
                      (list :name table :type "TABLE"))
                     "\n```\n\n")
           ((clutch-db-error user-error)
            (let ((message (error-message-string err)))
              (format "- Table metadata unavailable: %s\n\n" message)))))))
    (let* ((tables (or tables (clutch--agent-context-tables sql)))
           (sample (result-sample)))
      (with-temp-buffer
        (insert "# Clutch database context\n\n")
        (insert (connection-section))
        (insert "## SQL\n\n```sql\n" sql "\n```\n\n")
        (insert "## Referenced tables\n\n")
        (if tables
            (insert (mapconcat (lambda (table) (concat "- " table)) tables "\n")
                    "\n\n")
          (insert "- None detected\n\n"))
        (when sample
          (insert sample))
        (dolist (table tables)
          (insert (table-section table)))
        (string-trim-right (buffer-string))))))

;;;###autoload
(defun clutch-copy-context-for-agent ()
  "Copy current SQL, result sample, and related metadata for an external agent.
The copied Markdown is intended for tools such as ChatGPT, Claude, or
DeepSeek.  The command uses the current connection's metadata APIs and the
latest matching result buffer; it does not execute the SQL being copied."
  (interactive)
  (clutch--ensure-connection)
  (let ((sql (clutch--agent-context-current-sql)))
    (when (or (null sql) (string-empty-p sql))
      (user-error "No SQL context to copy"))
    (let* ((tables (clutch--agent-context-tables sql))
           (text (clutch--agent-context-text clutch-connection sql tables)))
      (kill-new text)
      (message "Copied context for %s table%s"
               (clutch--message-count (length tables))
               (if (= (length tables) 1) "" "s")))))

;;;; Cell and region selection

(defun clutch-result--yank-cell-value (val)
  "Copy VAL to kill ring and show a compact preview message."
  (let ((text (clutch--format-value val)))
    (kill-new text)
    (message "Copied: %s"
             (clutch--message-literal
              (truncate-string-to-width text 60 nil nil "…")))))

(defun clutch-result--region-rectangle-bounds ()
  "Return active region bounds as (ROW-INDICES . COL-INDICES)."
  (pcase-let* ((`(,r1 ,c1 ,_v1) (or (clutch--cell-at-or-near
                                     (region-beginning))
                                    (user-error "No cell at region start")))
               (`(,r2 ,c2 ,_v2) (or (clutch--cell-at-or-near
                                      (max (point-min) (1- (region-end))))
                                    (user-error "No cell at region end")))
               (row-min (min r1 r2))
               (row-max (max r1 r2))
               (col-min (min c1 c2))
               (col-max (max c1 c2)))
    (cons (cl-loop for ridx from row-min to row-max collect ridx)
          (cl-loop for cidx from col-min to col-max collect cidx))))

(defun clutch-result--cells-for-indices (row-indices col-indices)
  "Return cell triples for ROW-INDICES and COL-INDICES."
  (let ((rows (clutch--result-display-rows)))
    (cl-loop for ridx in row-indices
             append
             (let ((row (nth ridx rows)))
               (cl-loop for cidx in col-indices
                        collect (list ridx cidx (nth cidx row)))))))

(defun clutch-result--region-cells ()
  "Return cells in active region as a rectangle of (ROW-IDX COL-IDX VALUE)."
  (pcase-let ((`(,row-indices . ,col-indices)
               (clutch-result--region-rectangle-bounds)))
    (clutch-result--cells-for-indices row-indices col-indices)))

(defun clutch-result--region-rectangle-indices ()
  "Return rectangle row/column indices from active region.
Result is a cons cell (ROW-INDICES . COL-INDICES)."
  (unless (use-region-p)
    (user-error "Set a region to select rows and columns"))
  (clutch-result--region-rectangle-bounds))

(defun clutch-result--cells-tsv-text (cells)
  "Return TSV text for CELLS grouped by row index."
  (let (lines
        current-row
        current-values)
    (dolist (cell cells)
      (pcase-let ((`(,ridx ,_cidx ,val) cell))
        (if (or (null current-row) (= ridx current-row))
            (progn
              (setq current-row ridx)
              (push (clutch--format-value val) current-values))
          (push (string-join (nreverse current-values) "\t") lines)
          (setq current-row ridx
                current-values (list (clutch--format-value val))))))
    (when current-values
      (push (string-join (nreverse current-values) "\t") lines))
    (string-join (nreverse lines) "\n")))

(defun clutch-result--copy-cells-as-tsv (cells &optional deactivate)
  "Copy CELLS as TSV and report the copied cell count.
When DEACTIVATE is non-nil, deactivate the active region after copying."
  (unless cells
    (user-error "No cells in region"))
  (kill-new (clutch-result--cells-tsv-text cells))
  (when deactivate
    (deactivate-mark))
  (message "Copied %s cell%s from region"
           (clutch--message-count (length cells))
           (if (= (length cells) 1) "" "s")))

(defun clutch-result--yank-region-cells ()
  "Copy cell values from region as TSV-like text."
  (unless (use-region-p)
    (user-error "Set a region to copy multiple cells"))
  (clutch-result--copy-cells-as-tsv (clutch-result--region-cells) t))

(defun clutch-result--yank-rectangle-cells (rect)
  "Copy cells from RECT as TSV-like text."
  (pcase-let* ((`(,row-indices . ,col-indices) rect)
               (cells (clutch-result--cells-for-indices
                       row-indices col-indices)))
    (clutch-result--copy-cells-as-tsv cells)))

;;;; Aggregate values

(defun clutch-result--aggregate-target (&optional rect)
  "Return aggregate target as (ROW-INDICES COL-INDICES).
When RECT is non-nil, use it directly.  With region: use all selected columns.
Without region: use current cell."
  (if (or rect (use-region-p))
      (pcase-let* ((`(,row-indices . ,col-indices)
                    (or rect (clutch-result--region-rectangle-indices))))
        (unless col-indices
          (user-error "No columns selected for aggregate"))
        (list row-indices col-indices))
    (pcase-let* ((`(,ridx ,cidx ,_val) (or (clutch--cell-at-point)
                                           (user-error "No cell at point"))))
      (list (list ridx) (list cidx)))))

(defun clutch-result--parse-number (val)
  "Parse VAL into a number or return nil."
  (cond
   ((numberp val) val)
   ((stringp val)
    (let ((s (string-trim val)))
      (when (and (not (string-empty-p s))
                 (string-match-p
                  "\\`[+-]?\\(?:[0-9]+\\(?:\\.[0-9]*\\)?\\|\\.[0-9]+\\)\\'" s))
        (string-to-number s))))
   (t nil)))

(defun clutch-result--compute-aggregate (row-indices col-indices)
  "Compute aggregate stats for ROW-INDICES across COL-INDICES."
  (let* ((rows (length row-indices))
         (display-rows (clutch--result-display-rows))
         (cells (* rows (length col-indices)))
         (count 0)
        (sum 0.0)
        min-val
        max-val)
    (dolist (ridx row-indices)
      (let ((row (nth ridx display-rows)))
        (dolist (cidx col-indices)
          (let ((num (clutch-result--parse-number (nth cidx row))))
            (when num
              (setq count (1+ count)
                    sum (+ sum num))
              (setq min-val (if min-val (min min-val num) num))
              (setq max-val (if max-val (max max-val num) num)))))))
    (list :rows rows
          :cells cells
          :count count
          :skipped (- cells count)
          :sum sum
          :avg (if (> count 0) (/ sum count) 0)
          :min min-val
          :max max-val)))

(defun clutch-result--do-aggregate (rect)
  "Perform aggregate on RECT (ROW-INDICES . COL-INDICES) and update display."
  (pcase-let* ((`(,row-indices . ,col-indices) rect)
               (label (if (= (length col-indices) 1)
                          (nth (car col-indices) clutch--result-columns)
                        "selection"))
               (stats (clutch-result--compute-aggregate row-indices col-indices))
               (count (plist-get stats :count))
               (summary
                (if (> count 0)
                    (format "Aggregate [%s]: sum=%g avg=%g min=%g max=%g [rows=%d cells=%d skipped=%d]"
                            label
                            (plist-get stats :sum)
                            (plist-get stats :avg)
                            (plist-get stats :min)
                            (plist-get stats :max)
                            (plist-get stats :rows)
                            (plist-get stats :cells)
                            (plist-get stats :skipped))
                  (format "Aggregate [%s]: n/a [rows=%d cells=%d skipped=%d]"
                          label
                          (plist-get stats :rows)
                          (plist-get stats :cells)
                          (plist-get stats :skipped)))))
    (setq-local clutch--aggregate-summary
                (list :label label
                      :rows (plist-get stats :rows)
                      :cells (plist-get stats :cells)
                      :skipped (plist-get stats :skipped)
                      :sum (plist-get stats :sum)
                      :avg (plist-get stats :avg)
                      :min (plist-get stats :min)
                      :max (plist-get stats :max)
                      :count (plist-get stats :count)))
    (clutch--refresh-display)
    (kill-new summary)))

;;;###autoload
(defun clutch-result-aggregate (&optional refine)
  "Aggregate numeric values from selected columns or current cell.
With prefix arg REFINE and an active region, enter visual refine mode."
  (interactive "P")
  (if (and refine (use-region-p))
      (clutch-result--start-refine
       (clutch-result--region-rectangle-indices)
       #'clutch-result--do-aggregate)
    (pcase-let* ((`(,row-indices ,col-indices)
                  (clutch-result--aggregate-target nil)))
      (clutch-result--do-aggregate (cons row-indices col-indices)))))

;;;; Value viewers

(defun clutch--render-view-buffer (buffer val setup-fn)
  "Render string VAL into BUFFER, then call SETUP-FN there.
SETUP-FN is called with no args; it should activate a mode and may also
reformat the current buffer."
  (with-current-buffer buffer
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert val)
      (funcall setup-fn)
      (goto-char (point-min))
      (setq buffer-read-only t)))
  buffer)

(defun clutch--view-in-buffer (val buf-name setup-fn)
  "Insert string VAL into BUF-NAME, call SETUP-FN, then pop to it."
  (pop-to-buffer
   (clutch--render-view-buffer (get-buffer-create buf-name) val setup-fn)))

(defun clutch--setup-json-view-buffer ()
  "Enable JSON display mode for the current buffer."
  (json-pretty-print-buffer)
  (clutch--json-display-mode))

(defun clutch--decode-xml-char-refs-string (text)
  "Return TEXT with numeric XML character references decoded for display."
  (replace-regexp-in-string
   "&#\\(x[[:xdigit:]]+\\|X[[:xdigit:]]+\\|[[:digit:]]+\\);"
   (lambda (ref)
     (let* ((body (substring ref 2 -1))
            (hex (memq (aref body 0) '(?x ?X)))
            (code (string-to-number (if hex (substring body 1) body)
                                    (if hex 16 10)))
            (char (and (> code 0) (decode-char 'ucs code))))
       (if char
           (char-to-string char)
         ref)))
   text t t))

(defun clutch--decode-xml-char-refs-in-buffer ()
  "Decode numeric XML character references in the current buffer for display."
  (let ((decoded (clutch--decode-xml-char-refs-string (buffer-string))))
    (unless (equal decoded (buffer-string))
      (erase-buffer)
      (insert decoded))))

(defun clutch--xml-declaration-prefix-p (text)
  "Return non-nil when TEXT begins with an XML declaration."
  (string-match-p "\\`[[:space:]\n\r\t]*<\\?xml\\_>" text))

(defun clutch--setup-xml-view-buffer (val &optional quiet)
  "Pretty-print XML VAL in the current buffer and enable XML mode.
When QUIET is non-nil, suppress informational fallback messages."
  (let ((raw (buffer-string)))
    (if (executable-find "xmllint")
        (let ((err-file (make-temp-file "clutch-xmllint-")))
          (unwind-protect
              (unless (eq 0 (call-process-region
                             (point-min) (point-max)
                             "xmllint" t (list t err-file) nil "--format" "-"))
                (erase-buffer)
                (insert raw)
                (unless quiet
                  (message "xmllint: %s"
                           (string-trim (with-temp-buffer
                                          (insert-file-contents err-file)
                                          (buffer-string))))))
            (delete-file err-file)))
      (unless quiet
        (message "xmllint not found — showing raw XML without formatting")))
    (when (and (not (clutch--xml-declaration-prefix-p raw))
               (clutch--xml-declaration-prefix-p (buffer-string)))
      (goto-char (point-min))
      (when (looking-at "[[:space:]\n\r\t]*<\\?xml[^>]*\\?>[[:space:]\n\r\t]*")
        (replace-match ""))))
  ;; Readability matters more than preserving numeric character references in
  ;; the transient viewer buffer; keep the raw XML value unchanged elsewhere.
  (clutch--decode-xml-char-refs-in-buffer)
  (cond ((fboundp 'nxml-mode) (nxml-mode))
        ((fboundp 'xml-mode) (xml-mode))
        (t (special-mode)))
  (setq-local header-line-format
              (format " XML%s%d bytes"
                      (clutch--status-separator)
                      (string-bytes val)))
  ;; Force fontification so XML is highlighted immediately in popup buffers.
  (when (fboundp 'font-lock-ensure)
    (font-lock-ensure (point-min) (point-max)))
  (when (fboundp 'jit-lock-fontify-now)
    (jit-lock-fontify-now (point-min) (point-max))))

(defun clutch--setup-plain-view-buffer ()
  "Enable plain text view mode for the current buffer."
  (special-mode))

(defun clutch--blob-bytes (val)
  "Return a unibyte string for blob-like VAL."
  (cond
   ((stringp val) (encode-coding-string val 'binary))
   ((vectorp val) (apply #'unibyte-string (append val nil)))
   (t (encode-coding-string (clutch--format-value val) 'binary))))

(defun clutch--blob-hexdump-lines (bytes &optional max-bytes)
  "Return hex dump lines for BYTES, up to MAX-BYTES bytes."
  (let* ((total (length bytes))
         (limit (min total (or max-bytes total)))
         (offset 0)
         lines)
    (while (< offset limit)
      (let* ((line-len (min 16 (- limit offset)))
             (hex-parts nil)
             (ascii-parts nil))
        (dotimes (i line-len)
          (let* ((b (aref bytes (+ offset i)))
                 (ch (if (and (>= b 32) (<= b 126)) b ?.)))
            (push (format "%02x" b) hex-parts)
            (push (char-to-string ch) ascii-parts)))
        (push (format "%08x  %-47s  |%s|"
                      offset
                      (mapconcat #'identity (nreverse hex-parts) " ")
                      (mapconcat #'identity (nreverse ascii-parts) ""))
              lines)
        (setq offset (+ offset line-len))))
    (nreverse lines)))

(defun clutch--blob-likely-text-p (bytes &optional sample-size)
  "Return non-nil when BYTES appears mostly text-like within SAMPLE-SIZE bytes."
  (let* ((n (min (length bytes) (or sample-size 512)))
         (printable 0))
    (if (= n 0)
        t
      (dotimes (i n)
        (let ((b (aref bytes i)))
          (when (or (and (>= b 32) (<= b 126))
                    (memq b '(9 10 13)))
            (setq printable (1+ printable)))))
      (>= (/ (float printable) n) 0.85))))

(defun clutch--blob-view-string (val)
  "Build a concise DataGrip-like display string for blob VAL."
  (let* ((bytes (clutch--blob-bytes val))
         (size (length bytes))
         (text-like (clutch--blob-likely-text-p bytes))
         (max-bytes (if text-like 1024 256))
         (shown (min size max-bytes))
         (truncated (> size max-bytes)))
    (concat
     (format "BLOB size: %d bytes\n\n" size)
     (if text-like
         (let ((preview (condition-case nil
                            (decode-coding-string (substring bytes 0 shown) 'utf-8 t)
                          (error ""))))
           (concat "Text preview:\n"
                   (if (string-empty-p preview) "<empty>" preview)))
       (concat "Hex preview:\n"
               (mapconcat #'identity
                          (clutch--blob-hexdump-lines bytes max-bytes)
                          "\n")))
     (if truncated
         (format "\n\n... truncated, showing first %d bytes" max-bytes)
       ""))))

(defun clutch--json-view-string-p (val)
  "Return non-nil when VAL is string content suitable for the JSON viewer."
  (and (clutch--json-like-string-p val)
       (condition-case _err
           (progn (ignore (json-parse-string val)) t)
         (json-parse-error nil))))

(defun clutch--view-format-value (val)
  "Format VAL for value viewers."
  (if (null val)
      (clutch--null-display-string)
    (clutch--format-value val)))

(defun clutch--view-spec (val col-def &optional quiet)
  "Return rendering spec for VAL with column metadata COL-DEF.
When QUIET is non-nil, suppress nonessential viewer messages."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((null val)
      (list :kind "Value"
            :content (clutch--view-format-value val)
            :setup #'clutch--setup-plain-view-buffer))
     ((or (eq cat 'json)
          (clutch--json-view-string-p val))
      (list :kind "JSON"
            :content (if (stringp val) val
                       (clutch--json-value-to-string val))
            :setup #'clutch--setup-json-view-buffer))
     ((clutch--xml-like-string-p val)
      (list :kind "XML"
            :content val
            :setup (lambda () (clutch--setup-xml-view-buffer val quiet))))
     ((eq cat 'blob)
      (list :kind "BLOB"
            :content (clutch--blob-view-string val)
            :setup #'clutch--setup-plain-view-buffer))
     (t
      (list :kind "Value"
            :content (clutch--view-format-value val)
            :setup #'clutch--setup-plain-view-buffer)))))

(defun clutch--dispatch-view (val col-def)
  "Open the appropriate viewer for VAL given column metadata COL-DEF.
Dispatch order: JSON content → JSON viewer; XML content → XML viewer;
blob type with non-text value → binary string; otherwise plain text."
  (pcase-let* ((`(:kind ,kind :content ,content :setup ,setup)
                (clutch--view-spec val col-def))
               (buffer-name
                (cdr (assoc kind
                            '(("JSON" . "*clutch-json*")
                              ("XML" . "*clutch-xml*")
                              ("BLOB" . "*clutch-blob*")
                              ("Value" . "*clutch-value*"))))))
    (when (and (string= kind "JSON") (string-empty-p content))
      (user-error "No JSON value at point"))
    (clutch--view-in-buffer content (or buffer-name "*clutch-value*") setup)))

(defvar clutch--live-view-follow-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "f" #'clutch--live-view-toggle-freeze)
    (define-key map "g" #'clutch--live-view-refresh)
    (define-key map "q" #'clutch--live-view-quit)
    map)
  "Keymap for `clutch--live-view-follow-mode'.")

(define-minor-mode clutch--live-view-follow-mode
  "Minor mode for a clutch live-follow value viewer."
  :init-value nil
  :lighter " LiveView"
  :keymap clutch--live-view-follow-mode-map)

(defun clutch--live-view-current-context ()
  "Return the current live-view source context, or nil when none is available."
  (cond
   ((derived-mode-p 'clutch-result-mode)
    (when-let* ((ridx (get-text-property (point) 'clutch-row-idx))
                (cidx (get-text-property (point) 'clutch-col-idx)))
      (list :source-buffer (current-buffer)
            :source-kind 'result
            :cell-id (list 'result (buffer-chars-modified-tick) ridx cidx)
            :ridx ridx
            :cidx cidx
            :row-count (length (clutch--result-display-rows))
            :table clutch--result-source-table
            :column (nth cidx clutch--result-columns)
            :col-def (nth cidx clutch--result-column-defs)
            :value (get-text-property (point) 'clutch-full-value))))
   ((derived-mode-p 'clutch-record-mode)
    (when-let* ((result-buf clutch-record--result-buffer)
                ((buffer-live-p result-buf))
                (ridx (get-text-property (point) 'clutch-row-idx))
                (cidx (get-text-property (point) 'clutch-col-idx)))
      (list :source-buffer (current-buffer)
            :source-kind 'record
            :cell-id (list 'record (buffer-chars-modified-tick) ridx cidx)
            :ridx ridx
            :cidx cidx
            :row-count (with-current-buffer result-buf
                         (length (clutch--result-display-rows)))
            :table (with-current-buffer result-buf clutch--result-source-table)
            :column (with-current-buffer result-buf
                      (nth cidx clutch--result-columns))
            :col-def (with-current-buffer result-buf
                       (nth cidx clutch--result-column-defs))
            :value (get-text-property (point) 'clutch-full-value))))))

(defun clutch--live-view-header (kind context frozen)
  "Return live viewer header for KIND using CONTEXT and FROZEN state."
  (let* ((table (plist-get context :table))
         (column (or (plist-get context :column) "?"))
         (label (if table (format "%s.%s" table column) column))
         (ridx (plist-get context :ridx))
         (cidx (plist-get context :cidx))
         (row-count (or (plist-get context :row-count) 0)))
    (string-join
     (list (format " %s" kind)
           (if frozen "FROZEN" "FOLLOW")
           label
           (format "R%d/%d C%d" (1+ ridx) row-count (1+ cidx))
           (clutch--key-hints '(("f" "Toggle freeze")
                                ("g" "Refresh")
                                ("q" "Quit"))))
     (clutch--status-separator))))

(defun clutch--live-view-detach-source (source-buf &optional viewer-buf)
  "Detach live viewer state from SOURCE-BUF.
When VIEWER-BUF is non-nil, only detach if SOURCE-BUF points at VIEWER-BUF."
  (when (buffer-live-p source-buf)
    (with-current-buffer source-buf
      (when (or (null viewer-buf)
                (eq clutch--live-view-buffer viewer-buf))
        (setq-local clutch--live-view-buffer nil)
        (remove-hook 'post-command-hook #'clutch--live-view-source-post-command t)
        (remove-hook 'kill-buffer-hook #'clutch--live-view-source-killed t)
        (remove-hook 'change-major-mode-hook #'clutch--live-view-source-killed t)))))

(defun clutch--live-view-buffer-killed ()
  "Clean up source-buffer hooks when the live viewer is killed."
  (let ((viewer (current-buffer))
        (source clutch--live-view-source-buffer))
    (setq-local clutch--live-view-source-buffer nil)
    (clutch--live-view-detach-source source viewer)))

(defun clutch--live-view-source-killed ()
  "Dispose of any live viewer attached to the current source buffer."
  (when (buffer-live-p clutch--live-view-buffer)
    (let ((viewer clutch--live-view-buffer))
      (setq-local clutch--live-view-buffer nil)
      (when (buffer-live-p viewer)
        (with-current-buffer viewer
          (setq-local clutch--live-view-source-buffer nil))
        (kill-buffer viewer))))
  (remove-hook 'post-command-hook #'clutch--live-view-source-post-command t)
  (remove-hook 'kill-buffer-hook #'clutch--live-view-source-killed t)
  (remove-hook 'change-major-mode-hook #'clutch--live-view-source-killed t))

(defun clutch--render-live-view (viewer-buf context &optional force)
  "Render CONTEXT into VIEWER-BUF.
When FORCE is non-nil, refresh even if the source cell has not changed."
  (with-current-buffer viewer-buf
    (let ((frozen clutch--live-view-frozen)
          (source (plist-get context :source-buffer))
          (cell-id (plist-get context :cell-id)))
      (unless (and (not force)
                   (equal cell-id clutch--live-view-source-cell-id))
        (pcase-let* ((`(:kind ,kind :content ,content :setup ,setup)
                      (clutch--view-spec (plist-get context :value)
                                         (plist-get context :col-def)
                                         t)))
          (clutch--render-view-buffer viewer-buf content setup)
          (setq-local clutch--live-view-source-buffer source)
          (setq-local clutch--live-view-source-cell-id cell-id)
          (setq-local clutch--live-view-frozen frozen)
          (setq-local header-line-format
                      (clutch--live-view-header kind context frozen))
          (clutch--live-view-follow-mode 1))))))

(defun clutch--live-view-source-post-command ()
  "Refresh the attached live viewer after point movement in a source buffer."
  (if (not (buffer-live-p clutch--live-view-buffer))
      (clutch--live-view-detach-source (current-buffer))
    (let ((viewer clutch--live-view-buffer)
          (context (clutch--live-view-current-context)))
      (with-current-buffer viewer
        (unless clutch--live-view-frozen
          (when context
            (clutch--render-live-view viewer context)))))))

(defun clutch--live-view-refresh ()
  "Refresh the current clutch live viewer from its source point."
  (interactive)
  (unless (buffer-live-p clutch--live-view-source-buffer)
    (user-error "Live viewer source buffer is no longer available"))
  (when-let* ((context (with-current-buffer clutch--live-view-source-buffer
                         (clutch--live-view-current-context))))
    (clutch--render-live-view (current-buffer) context t)
    (message "Live viewer refreshed")))

(defun clutch--live-view-toggle-freeze ()
  "Toggle live viewer tracking of source-buffer point."
  (interactive)
  (setq-local clutch--live-view-frozen (not clutch--live-view-frozen))
  (setq-local header-line-format
              (replace-regexp-in-string
               (if clutch--live-view-frozen "FOLLOW" "FROZEN")
               (if clutch--live-view-frozen "FROZEN" "FOLLOW")
               (format "%s" header-line-format)
               t t))
  (unless clutch--live-view-frozen
    (clutch--live-view-refresh))
  (message "Live viewer %s"
           (if clutch--live-view-frozen "frozen" "following point")))

(defun clutch--live-view-quit ()
  "Close the current clutch live viewer."
  (interactive)
  (kill-buffer (current-buffer)))

(defun clutch--open-live-view ()
  "Open or refresh a live-follow viewer for the current clutch cell."
  (let* ((source (current-buffer))
         (context (or (clutch--live-view-current-context)
                      (user-error "No cell at point")))
         (viewer (get-buffer-create "*clutch-live-view*")))
    (with-current-buffer viewer
      (when (and (buffer-live-p clutch--live-view-source-buffer)
                 (not (eq clutch--live-view-source-buffer source)))
        (clutch--live-view-detach-source clutch--live-view-source-buffer viewer))
      (add-hook 'kill-buffer-hook #'clutch--live-view-buffer-killed nil t))
    (setq-local clutch--live-view-buffer viewer)
    (add-hook 'post-command-hook #'clutch--live-view-source-post-command nil t)
    (add-hook 'kill-buffer-hook #'clutch--live-view-source-killed nil t)
    (add-hook 'change-major-mode-hook #'clutch--live-view-source-killed nil t)
    (with-current-buffer viewer
      (setq-local clutch--live-view-frozen nil)
      (setq-local clutch--live-view-source-buffer source))
    (clutch--render-live-view viewer context t)
    (display-buffer viewer '(display-buffer-at-bottom . ((window-height . 0.33))))
    viewer))

;;;###autoload
(defun clutch-result-view-value ()
  "Display the cell value at point in an appropriate pop-up buffer.
Selects JSON, XML, or binary string view based on column type and content."
  (interactive)
  (pcase-let ((`(,_ridx ,cidx ,val) (or (clutch--cell-at-point)
                                         (user-error "No cell at point"))))
    (clutch--dispatch-view val (nth cidx clutch--result-column-defs))))

;;;###autoload
(defun clutch-result-live-view-value ()
  "Open a live-follow viewer for the result cell at point."
  (interactive)
  (clutch--open-live-view))

;;;###autoload
(defun clutch-result-shell-command-on-cell (command)
  "Pipe the cell value at point through shell COMMAND and display the output."
  (interactive "sShell command on cell: ")
  (pcase-let ((`(,_ridx ,_cidx ,val) (or (clutch--cell-at-point)
                                          (user-error "No cell at point"))))
    (let ((input (if (stringp val)
                     val
                   (clutch--format-value val))))
      (clutch--view-in-buffer
       (with-temp-buffer
         (insert input)
         (shell-command-on-region (point-min) (point-max) command t t)
         (buffer-string))
       "*clutch-shell-output*"
       #'special-mode))))

;;;; SQL and document copy builders

(defun clutch-result--build-insert-statements-for-rows (rows col-indices table)
  "Return INSERT statements for ROWS using COL-INDICES into TABLE."
  (let* ((conn      clutch-connection)
         (col-names (clutch--column-names-for-indices col-indices))
         (cols      (mapconcat (lambda (c) (clutch-db-escape-identifier conn c))
                               col-names ", ")))
    (cl-loop for row in rows
             for vals = (cl-mapcar
                          (lambda (cidx col-name)
                            (clutch-result--typed-param-for-column
                             table col-name (nth cidx row) cidx))
                          col-indices col-names)
             collect (format "INSERT INTO %s (%s) VALUES (%s);"
                             (clutch-db-escape-identifier conn table)
                             cols
                             (mapconcat
                              (lambda (param)
                                (clutch-db-value-to-literal
                                 conn param #'clutch--format-value))
                              vals ", ")))))

(defconst clutch--insert-placeholder-table "MY_TABLE"
  "Placeholder target table used for ambiguous INSERT copy/export output.")

(defun clutch--insert-target-table ()
  "Return a safe target table name for INSERT copy/export.
Simple single-table result sets use the detected table name.  Ambiguous
results use `clutch--insert-placeholder-table' instead."
  (or (when clutch--last-query
        (clutch-db-sql-source-table clutch--last-query t))
      clutch--insert-placeholder-table))

(defun clutch-result--selected-update-col-indices (row-identity col-indices op)
  "Return writable update column indices from ROW-IDENTITY, COL-INDICES, and OP.
Hidden identity columns are excluded.  Primary-key source columns are excluded
to preserve the existing copy/export UPDATE behavior."
  (let* ((pk-source-indices
          (and (eq (plist-get row-identity :kind) 'primary-key)
               (or (plist-get row-identity :source-indices)
                   (plist-get row-identity :indices))))
         (set-col-indices
          (cl-loop for cidx in col-indices
                   unless (or (plist-get (nth cidx clutch--result-column-defs)
                                         :hidden)
                              (memq cidx pk-source-indices))
                   collect cidx)))
    (unless set-col-indices
      (user-error "Cannot %s: no writable source columns selected" op))
    set-col-indices))

(defun clutch-result--ensure-update-source-columns (table col-indices op)
  "Ensure COL-INDICES map to writable source columns for TABLE during OP."
  (let* ((details (or (clutch--ensure-column-details clutch-connection table t)
                      (user-error "Cannot %s: source column metadata is unavailable"
                                  op)))
         (detail-map
          (cl-loop for detail in details
                   collect (cons (plist-get detail :name) detail)))
         (invalid (cl-loop for cidx in col-indices
                           for col-name = (nth cidx clutch--result-columns)
                           for detail = (cdr (assoc col-name detail-map))
                           unless (and detail (not (plist-get detail :generated)))
                           collect col-name)))
    (when invalid
      (user-error "Cannot %s: selected columns are not writable source columns: %s"
                  op
                  (string-join invalid ", ")))))

(defun clutch-result--build-update-statements-for-rows (rows col-indices op)
  "Return UPDATE preview statements for ROWS using COL-INDICES.
OP is a short operation description used in user-facing error messages."
  (let* ((table (clutch--result-source-table-or-user-error op))
         (row-identity (clutch-result--row-identity-or-user-error table op))
         (set-col-indices (clutch-result--selected-update-col-indices
                           row-identity col-indices op))
         (col-names clutch--result-columns)
         statements)
    (clutch-result--ensure-update-source-columns table set-col-indices op)
    (dolist (row rows)
      (let* ((identity-vec (clutch-db-row-identity-values
                            row row-identity))
             (edits (cl-loop for cidx in set-col-indices
                             collect (cons cidx (nth cidx row)))))
        (push (clutch-result--build-update-stmt
               table identity-vec edits col-names row-identity)
              statements)))
    (clutch-result--render-statements (nreverse statements))))

(defconst clutch--result-export-formats
  '(("csv-copy" :kind csv :destination clipboard)
    ("csv-file" :kind csv :destination file)
    ("insert-copy" :kind insert :action export-insert :destination clipboard)
    ("insert-file" :kind insert :action export-insert :destination file)
    ("update-copy" :kind update :action export-update :destination clipboard)
    ("update-file" :kind update :action export-update :destination file)
    ("document-insert-many-copy" :kind document-insert-many
     :action export-document-insert-many :destination clipboard)
    ("document-insert-many-file" :kind document-insert-many
     :action export-document-insert-many :destination file))
  "Available result export choices and their execution targets.")

(defconst clutch--result-export-kinds
  '((csv . (:content clutch--export-csv-content
            :file-prompt "Export CSV to file: "
            :default-file "export.csv"
            :copy-message "Copied %d row%s as CSV"
            :file-message "Exported %d row%s to %s (%s)"
            :file-coding csv))
    (insert . (:content clutch--export-insert-content
               :file-prompt "Export SQL to file: "
               :default-file "export.sql"
               :copy-message "Copied %d row%s as INSERT SQL"
               :file-message "Exported %d row%s as INSERT SQL to %s"))
    (update . (:content clutch--export-update-content
               :file-prompt "Export SQL to file: "
               :default-file "export.sql"
               :copy-message "Copied %d row%s as UPDATE SQL"
               :file-message "Exported %d row%s as UPDATE SQL to %s"))
    (document-insert-many . (:content clutch--export-document-insert-many-content
                             :file-prompt "Export document helper to file: "
                             :default-file "documents.txt"
                             :copy-message "Copied %d document%s as native insert-many helper"
                             :file-message "Exported %d document%s as native insert-many helper to %s")))
  "Result export behavior keyed by logical export kind.")

(defun clutch-result--export-format-available-p (format)
  "Return non-nil when export FORMAT is available in the current result."
  (if-let* ((action (plist-get format :action)))
      (clutch-result--action-supported-p action)
    t))

(defun clutch-result--available-export-formats ()
  "Return export formats available for the current result buffer."
  (cl-loop for format in clutch--result-export-formats
           when (clutch-result--export-format-available-p (cdr format))
           collect format))

(defun clutch-result--copy-selection-indices (&optional rect)
  "Return row and column indices for result copy commands.
RECT, when non-nil, has priority.  Otherwise active regions are treated as a
rectangle and inactive regions fall back to the current cell."
  (let ((rect (or rect
                  (if (use-region-p)
                      (clutch-result--region-rectangle-indices)
                    (pcase-let ((`(,ridx ,cidx ,_v)
                                 (or (clutch--cell-at-point)
                                     (user-error "No cell at point"))))
                      (cons (list ridx) (list cidx)))))))
    (cons (or (car-safe rect)
              (clutch--selected-row-indices)
              (user-error "No row at point"))
          (or (cdr-safe rect)
              (clutch--visible-columns)))))

(defun clutch-result--copy-lines (kind rows col-indices)
  "Return copy output lines for KIND using ROWS and COL-INDICES."
  (pcase kind
    ('csv (clutch--csv-lines-for-rows rows col-indices))
    ('org-table (clutch--org-table-lines-for-rows rows col-indices))
    ('insert
     (clutch-result--build-insert-statements-for-rows
      rows col-indices (clutch--insert-target-table)))
    ('update
     (clutch-result--build-update-statements-for-rows
      rows col-indices "copy UPDATE SQL"))
    ((or 'document-insert-one 'document-insert-many
         'document-replace-one 'document-delete-one
         'document-update-one-set)
     (let* ((action (pcase kind
                      ('document-insert-one 'insert-one)
                      ('document-insert-many 'insert-many)
                      ('document-replace-one 'replace-one)
                      ('document-delete-one 'delete-one)
                      ('document-update-one-set 'update-one-set)))
            (op (format "copy %s" kind))
            (collection (clutch-result--document-source-collection op))
            (documents (clutch-result--document-source-documents rows op))
            (fields (when (eq action 'update-one-set)
                      (clutch--column-names-for-indices col-indices))))
       (clutch-result--require-action
        (pcase action
          ('insert-one 'document-insert-one)
          ('insert-many 'document-insert-many)
          ('replace-one 'document-replace-one)
          ('delete-one 'document-delete-one)
          ('update-one-set 'document-update-one-set))
        op)
       (clutch-db-document-mutation-snippets
        clutch-connection action collection documents fields)))
    (_
     (user-error "Unsupported copy format: %s" kind))))

(defun clutch-result--copy-rows (kind &optional rect)
  "Copy selected rows as KIND using optional RECT."
  (let* ((selection (clutch-result--copy-selection-indices rect))
         (indices (car selection))
         (col-indices (cdr selection))
         (rows (clutch-result--rows-for-display-indices indices))
         (lines (clutch-result--copy-lines kind rows col-indices)))
    (kill-new (mapconcat #'identity lines "\n"))
    (deactivate-mark)
    (pcase kind
      ((or 'csv 'org-table)
       (message "Copied %s row%s as %s (%s col%s)"
                (clutch--message-count (length indices))
                (if (= (length indices) 1) "" "s")
                (clutch--message-keyword
                 (if (eq kind 'csv) "CSV" "Org table"))
                (clutch--message-count (length col-indices))
                (if (= (length col-indices) 1) "" "s")))
      ((or 'insert 'update)
       (let ((op (upcase (symbol-name kind))))
         (message "Copied %s %s statement%s (%s col%s)"
                  (clutch--message-count (length lines))
                  (clutch--message-keyword op)
                  (if (= (length lines) 1) "" "s")
                  (clutch--message-count (length col-indices))
                  (if (= (length col-indices) 1) "" "s"))))
      ((or 'document-insert-one 'document-insert-many
           'document-replace-one 'document-delete-one
           'document-update-one-set)
       (message "Copied %s document helper snippet%s"
                (clutch--message-count (length lines))
                (if (= (length lines) 1) "" "s"))))))

(defun clutch--csv-escape (val)
  "Return CSV-escaped string for VAL."
  (let ((s (clutch--format-value val)))
    (if (string-match-p "[,\"\r\n]" s)
        (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" s))
      s)))

(defun clutch--csv-lines-for-rows (rows col-indices)
  "Return CSV lines for ROWS using COL-INDICES."
  (let ((col-names (clutch--column-names-for-indices col-indices)))
    (cons (mapconcat #'clutch--csv-escape col-names ",")
          (cl-loop for row in rows
                   for vals = (mapcar (lambda (i) (nth i row)) col-indices)
                   collect (mapconcat #'clutch--csv-escape vals ",")))))

(defun clutch--org-table-lines-for-rows (rows col-indices)
  "Return aligned Org table lines for ROWS using COL-INDICES."
  (cl-labels
      ((cell (val)
         (let ((s (clutch--format-value val)))
           (setq s (replace-regexp-in-string "[\r\n]+" "\\\\n" s))
           (string-replace "|" "\\vert" s)))
       (table-row (values widths numeric-cols &optional header)
         (format "| %s |"
                 (mapconcat
                  #'identity
                  (cl-mapcar
                   (lambda (text width numericp)
                     (let ((pad (make-string
                                 (max 0 (- width (string-width text)))
                                 ?\s)))
                       (if (and numericp (not header))
                           (concat pad text)
                         (concat text pad))))
                   values widths numeric-cols)
                  " | "))))
    (let* ((col-names (clutch--column-names-for-indices col-indices))
           (numeric-cols
            (mapcar (lambda (i)
                      (eq (plist-get (nth i clutch--result-column-defs)
                                     :type-category)
                          'numeric))
                    col-indices))
           (data-rows
            (cl-loop for row in rows
                     collect (mapcar (lambda (i) (nth i row)) col-indices)))
           (table-rows
            (cons (mapcar #'cell col-names)
                  (cl-loop for row in data-rows
                           collect (mapcar #'cell row))))
           (widths
            (cl-loop for index below (length col-indices)
                     collect
                     (max 3
                          (cl-loop for row in table-rows
                                   maximize (string-width (nth index row))))))
           (separator
            (format "|%s|"
                    (mapconcat (lambda (width)
                                 (make-string (+ width 2) ?-))
                               widths "+"))))
      (cons (table-row (car table-rows) widths numeric-cols t)
            (cons separator
                  (cl-loop for row in (cdr table-rows)
                           collect (table-row row widths numeric-cols)))))))

;;;; Export commands

;;;###autoload
(defun clutch-result-export ()
  "Export the current result.
Prompts for format:
- csv-copy: all rows to clipboard as CSV text
- csv-file: all rows to CSV file
- insert-copy: all rows to clipboard as INSERT statements
- insert-file: all rows to a .sql file as INSERT statements
- update-copy: all rows to clipboard as UPDATE statements
- update-file: all rows to a .sql file as UPDATE statements."
  (interactive)
  (let* ((formats (clutch-result--available-export-formats))
         (choice (completing-read
                  "Export format: "
                  (mapcar #'car formats)
                  nil t))
         (format (cdr (assoc choice formats))))
    (unless format
      (user-error "Unsupported export format: %s" choice))
    (clutch--export-result format)))

(defun clutch--export-csv-content (rows)
  "Return CSV export text for ROWS using current visible result columns."
  (let* ((lines (clutch--csv-lines-for-rows rows (clutch--visible-columns)))
         (body (mapconcat #'identity (cdr lines) "\n")))
    (if (string-empty-p body)
        (concat (car lines) "\n")
      (concat (car lines) "\n" body "\n"))))

(defun clutch--csv-export-coding-choices ()
  "Return alist of CSV export coding labels to coding systems."
  (let ((pairs '(("utf-8-bom" . utf-8-with-signature)
                 ("utf-8" . utf-8)
                 ("gbk" . gbk)
                 ("cp936" . cp936))))
    (cl-loop for (label . coding) in pairs
             when (coding-system-p coding)
             collect (cons label coding))))

(defun clutch--read-csv-export-coding-system ()
  "Read coding system for CSV file export."
  (let* ((choices (clutch--csv-export-coding-choices))
         (default (if (coding-system-p clutch-csv-export-default-coding-system)
                      clutch-csv-export-default-coding-system
                    'utf-8-with-signature))
         (default-label (car (rassoc default choices)))
         (label (completing-read
                 (format "CSV encoding (default %s): "
                         (or default-label (symbol-name default)))
                 (mapcar #'car choices) nil t nil nil default-label)))
    (or (cdr (assoc label choices)) default)))

(defun clutch-result--collect-all-export-rows ()
  "Return all rows for current result by auto-paging when needed."
  (clutch--ensure-connection)
  (let* ((plan (clutch-result--current-query-plan))
         (effective-sql (plist-get plan :sql)))
    (if (or (null effective-sql)
            (not (clutch-result--server-pageable-p)))
        clutch--result-rows
      (let* ((row-identity-prep (plist-get plan :row-identity-prep))
             (identity-sql (plist-get row-identity-prep :sql)))
        (if (or (null clutch--base-query)
                (and (null clutch--order-by)
                     (clutch-db-sql-has-top-level-limit-p effective-sql)))
            (clutch-db-result-rows
             (clutch--run-db-query clutch-connection identity-sql))
          (cl-loop with page-size = clutch-result-max-rows
                   for page-num from 0
                   for paged-sql = (clutch-db-build-paged-sql
                                    clutch-connection identity-sql page-num
                                    page-size clutch--order-by)
                   for result = (clutch--run-db-query
                                  clutch-connection paged-sql)
                   for batch = (clutch-db-result-rows result)
                   append batch into rows
                   until (< (length batch) page-size)
                   finally return rows))))))

(defun clutch--export-insert-content (rows)
  "Return INSERT statement export text for ROWS using current result metadata."
  (let* ((table (clutch--insert-target-table))
         (col-indices (clutch--visible-columns))
         (stmts (clutch-result--build-insert-statements-for-rows
                 rows col-indices table)))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-update-content (rows)
  "Return UPDATE statement export text for ROWS using current result metadata."
  (let* ((col-indices (clutch--visible-columns))
         (stmts (clutch-result--build-update-statements-for-rows
                 rows col-indices "export UPDATE SQL")))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-document-insert-many-content (rows)
  "Return native document insertMany export text for ROWS."
  (let* ((collection
          (clutch-result--document-source-collection
           "export document insert-many"))
         (documents
          (clutch-result--document-source-documents
           rows "export document insert-many"))
         (stmts (clutch-db-document-mutation-snippets
                 clutch-connection 'insert-many collection documents)))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-result (format)
  "Execute result export described by FORMAT."
  (let* ((kind (plist-get format :kind))
         (destination (plist-get format :destination))
         (spec (or (cdr (assq kind clutch--result-export-kinds))
                   (user-error "Unsupported export kind: %s" kind)))
         (rows (clutch-result--collect-all-export-rows))
         (coding (when (and (eq destination 'file)
                            (eq (plist-get spec :file-coding) 'csv))
                   (clutch--read-csv-export-coding-system)))
         (text (funcall (plist-get spec :content) rows))
         (row-count (length rows))
         (row-suffix (if (= (length rows) 1) "" "s")))
    (pcase destination
      ('clipboard
       (kill-new text)
       (message (plist-get spec :copy-message) row-count row-suffix))
      ('file
       (let* ((path (read-file-name (plist-get spec :file-prompt)
                                    nil nil nil
                                    (plist-get spec :default-file)))
              (coding-system-for-write (or coding coding-system-for-write)))
         (with-temp-buffer
           (insert text)
           (write-region (point-min) (point-max) path nil 'silent))
         (apply #'message (plist-get spec :file-message)
                (append (list row-count row-suffix path)
                        (when coding (list coding))))))
      (_
       (user-error "Unsupported export destination: %s" destination)))))

;;;; Column navigation and metadata

(defun clutch-result--goto-col-idx (col-idx)
  "Move point to COL-IDX in the current row, preserving the row position.
When point is at line-end or a border, scan backward to find the row."
  (let ((ridx (or (get-text-property (point) 'clutch-row-idx)
                   (and (not (bolp))
                        (get-text-property (1- (point)) 'clutch-row-idx))
                   (save-excursion
                     (let ((prev (previous-single-property-change
                                  (point) 'clutch-row-idx)))
                       (when prev
                         (get-text-property (max (1- prev) (point-min))
                                            'clutch-row-idx)))))))
    (if ridx
        (clutch--goto-cell ridx col-idx)
      (goto-char (point-min))
      (when-let* ((found (text-property-search-forward
                          'clutch-col-idx col-idx #'eq)))
        (goto-char (prop-match-beginning found))))
    (clutch--center-column-in-window col-idx)))

;;;###autoload
(defun clutch-result-column-info ()
  "Show type information for the column at point.
When details are not yet cached, attempts to load them from the database."
  (interactive)
  (let* ((cidx (or (get-text-property (point) 'clutch-col-idx)
                   (get-text-property (point) 'clutch-header-col))))
    (unless cidx
      (user-error "No column at point"))
    ;; Try to populate details on demand if missing.
    (unless clutch--result-column-details
      (when-let* ((table clutch--result-source-table)
                  (cols clutch--result-columns))
        (setq-local clutch--result-column-details
                    (clutch--result-column-details
                     clutch-connection table cols t))))
    (if-let* ((info (clutch--column-info-string cidx)))
        (message "%s" (clutch--column-info-message-string info))
      (message "%s (no detail info)" (nth cidx clutch--result-columns)))))

;;;###autoload
(defun clutch-result-goto-column ()
  "Jump to a visible column in the current row."
  (interactive)
  (unless clutch--result-columns
    (user-error "No result columns"))
  (let* ((visible-cols (clutch--visible-columns))
         (col-names (clutch--column-names-for-indices visible-cols))
         (choice (completing-read "Go to column: " col-names nil t))
         (visible-idx (cl-position choice col-names :test #'string=))
         (idx (and visible-idx (nth visible-idx visible-cols))))
    (when idx
      (clutch-result--goto-col-idx idx))))

;;;; Horizontal scrolling and width adjustment

;;;###autoload
(defun clutch-result-scroll-right ()
  "Page the result window right with one-column overlap.
The last column whose border falls within the current viewport becomes
the first column of the new view, so partially visible edge columns
remain visible after paging."
  (interactive)
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let* ((hs (window-hscroll win))
           (width (window-body-width win))
           (right-edge (+ hs width))
           (widths (clutch--effective-widths))
           (nw (clutch--row-number-digits))
           (ncols (length clutch--result-columns))
           (last-in-view nil)
           (first-past nil))
      (dotimes (i ncols)
        (let ((border (clutch--column-border-position i widths nw)))
          (cond
           ((and (> border hs) (< border right-edge))
            (setq last-in-view border))
           ((and (>= border right-edge) (null first-past))
            (setq first-past border)))))
      (cond
       (last-in-view (set-window-hscroll win last-in-view))
       (first-past   (set-window-hscroll win first-past))
       (t (message "Already at rightmost columns"))))))

;;;###autoload
(defun clutch-result-scroll-left ()
  "Page the result window left with one-column overlap.
The column at the current left edge remains visible near the right
edge of the new view, so partially visible edge columns stay visible
after paging."
  (interactive)
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let* ((hs (window-hscroll win))
           (width (window-body-width win))
           (widths (clutch--effective-widths))
           (nw (clutch--row-number-digits))
           (ncols (length clutch--result-columns)))
      (when (> hs 0)
        ;; Column at the current left edge (largest border <= hs).
        (let ((first-border 0)
              (target nil))
          (dotimes (i ncols)
            (let ((border (clutch--column-border-position i widths nw)))
              (when (<= border hs)
                (setq first-border border))))
          ;; Smallest column border that keeps first-border in the new view:
          ;; new-hs + width > first-border  →  new-hs > first-border - width
          (let ((min-new (- first-border width)))
            (dotimes (i ncols)
              (let ((border (clutch--column-border-position i widths nw)))
                (when (and (> border min-new) (< border hs) (null target))
                  (setq target border)))))
          (set-window-hscroll win (max 0 (or target 0))))))))

;;;###autoload
(defun clutch-result-widen-column ()
  "Widen the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (progn
        (cl-incf (aref clutch--column-widths cidx)
                 clutch-column-width-step)
        (clutch--schedule-column-width-refresh))
    (user-error "No column at point")))

;;;###autoload
(defun clutch-result-narrow-column ()
  "Narrow the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (let ((new-w (max 5 (- (aref clutch--column-widths cidx)
                              clutch-column-width-step))))
        (aset clutch--column-widths cidx new-w)
        (clutch--schedule-column-width-refresh))
    (user-error "No column at point")))

;;;; Fullscreen toggle

(defvar-local clutch--pre-fullscreen-config nil
  "Window configuration saved before entering fullscreen.")

(defun clutch-result--fullscreen-transient-description ()
  "Return the transient description for the result window layout."
  (concat
   "Layout "
   (clutch--transient-state-display
    (if clutch--pre-fullscreen-config 'fullscreen 'window)
    '((window . "window") (fullscreen . "fullscreen")))))

;;;###autoload
(defun clutch-result-fullscreen-toggle ()
  "Toggle fullscreen display for the result buffer.
Expands the result buffer to fill the frame, or restores the
previous window layout."
  (interactive)
  (if clutch--pre-fullscreen-config
      (progn
        (set-window-configuration clutch--pre-fullscreen-config)
        (setq clutch--pre-fullscreen-config nil)
        (message "Restored window layout"))
    (setq clutch--pre-fullscreen-config
          (current-window-configuration))
    (delete-other-windows)
    (clutch--refresh-display)
    (message "Fullscreen (press f again to restore)")))

;;;; Record buffer

(defvar clutch-record-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "RET") #'clutch-record-toggle-expand)
    (define-key map "n" #'clutch-record-next-row)
    (define-key map "p" #'clutch-record-prev-row)
    (define-key map "v" #'clutch-record-view-value)
    (define-key map "V" #'clutch-record-live-view-value)
    (define-key map (kbd "C-c '") #'clutch-result-edit-cell)
    (define-key map (kbd "C-c C-k") #'clutch-result-discard-pending-at-point)
    (define-key map "I" #'clutch-clone-row-to-insert)
    (define-key map "q" #'quit-window)
    (define-key map "g" #'clutch-record-refresh)
    (define-key map (kbd "C-c ?") #'clutch-record-dispatch)
    map)
  "Keymap for `clutch-record-mode'.")
;;;###autoload
(define-derived-mode clutch-record-mode special-mode "clutch-record"
  "Mode for displaying a single database row in detail.

\\<clutch-record-mode-map>
  \\[clutch-record-toggle-expand]	Expand/collapse field or follow FK
  \\[clutch-record-next-row]	Next row
  \\[clutch-record-prev-row]	Previous row
  \\[clutch-result-edit-cell]	Edit / re-edit field
  \\[clutch-result-discard-pending-at-point]	Discard staged field or row change
  \\[clutch-record-view-value]	View current field once
  \\[clutch-record-live-view-value]	Open live viewer that follows point
  \\[clutch-record-refresh]	Refresh"
  (setq truncate-lines nil))

;;;###autoload
(defun clutch-result-open-record ()
  "Open the Record buffer showing the row at point.
Reuses a single *clutch-record* buffer, updating it in place."
  (interactive)
  (let* ((ridx (or (clutch--row-idx-at-line)
                   (user-error "No row at point")))
         (result-buf (current-buffer))
         (buf (get-buffer-create "*clutch-record*")))
    (with-current-buffer buf
      (unless (derived-mode-p 'clutch-record-mode)
        (clutch-record-mode))
      (setq-local clutch-record--result-buffer result-buf)
      (setq-local clutch-record--row-idx ridx)
      (setq-local clutch-record--expanded-fields nil)
      (clutch-record--render))
    (pop-to-buffer buf '(display-buffer-at-bottom))))

(defun clutch-record--render-field (name cidx val col-def row ridx row-identity
                                        edits fk-info expanded-fields max-name-w)
  "Insert one field line for column NAME at CIDX.
VAL is the cell value, COL-DEF the column metadata, ROW the full row.
RIDX is the row index.  ROW-IDENTITY, EDITS, FK-INFO, and EXPANDED-FIELDS
provide edit/FK/expand state.  MAX-NAME-W is the label column width."
  (let* ((identity-vec (and row-identity row
                            (clutch-db-row-identity-values
                             row row-identity)))
         (edited (and identity-vec (assoc (cons identity-vec cidx) edits)))
         (display-val (if edited (cdr edited) val))
         (long-p (clutch--long-field-type-p col-def))
         (expanded-p (memq cidx expanded-fields))
         (fk (cdr (assq cidx fk-info)))
         (formatted (if (null display-val)
                        clutch--null-cell-display-text
                      (clutch--format-value display-val)))
         (display (if (and long-p (not expanded-p) (> (length formatted) 80))
                      (concat (substring formatted 0 80) "…")
                    formatted))
         (face (cond (edited 'clutch-modified-face)
                     ((null val) 'clutch-null-face)
                     (fk 'clutch-fk-face)
                     (t nil)))
         (cell-props (list 'clutch-row-idx ridx
                           'clutch-col-idx cidx
                           'clutch-full-value display-val)))
    (insert (apply #'propertize
                   (clutch--string-pad name max-name-w)
                   (append (list 'face 'clutch-field-name-face)
                           cell-props))
            (apply #'propertize " : "
                   (append (list 'face 'clutch-border-face)
                           cell-props))
            (apply #'propertize display
                   (append (when face (list 'face face))
                           cell-props))
            "\n")))

(defun clutch-record--render ()
  "Render the current row in the Record buffer."
  (unless (buffer-live-p clutch-record--result-buffer)
    (user-error "Result buffer no longer exists"))
  (let* ((result-buf clutch-record--result-buffer)
         (ridx clutch-record--row-idx)
         (col-names (buffer-local-value 'clutch--result-columns result-buf))
         (col-defs (buffer-local-value 'clutch--result-column-defs result-buf))
         (rows (with-current-buffer result-buf
                 (clutch--result-display-rows)))
         (row-identity (buffer-local-value 'clutch--row-identity result-buf))
         (fk-info (buffer-local-value 'clutch--fk-info result-buf))
         (edits (buffer-local-value 'clutch--pending-edits result-buf))
         (inhibit-read-only t))
    (unless (< ridx (length rows))
      (user-error "Row %d no longer exists" ridx))
    (clutch--bind-connection-context
     (buffer-local-value 'clutch-connection result-buf)
     (buffer-local-value 'clutch--connection-params result-buf)
     (buffer-local-value 'clutch--conn-sql-product result-buf))
    (erase-buffer)
    (setq-local clutch-record--header-base
                (propertize (format " Record: row %d/%d" (1+ ridx) (length rows))
                            'face 'clutch-header-face))
    (setq header-line-format
          '(:eval (clutch--header-with-disconnect-badge clutch-record--header-base)))
    (let* ((row (nth ridx rows))
           (max-name-w (apply #'max (mapcar #'string-width col-names))))
      (cl-loop for name in col-names
               for col-def in col-defs
               for cidx from 0
               unless (plist-get col-def :hidden)
               do (clutch-record--render-field
                   name cidx (nth cidx row) col-def
                   row ridx row-identity edits fk-info clutch-record--expanded-fields
                   max-name-w)))
    (goto-char (point-min))))

(defun clutch-record--follow-fk (fk val result-buf)
  "Navigate to the FK-referenced row for VAL using FK plist, via RESULT-BUF."
  (when (null val)
    (user-error "NULL value — cannot follow"))
  (with-current-buffer result-buf
    (let ((c (buffer-local-value 'clutch-connection result-buf)))
      (clutch--execute
       (format "SELECT * FROM %s WHERE %s = %s"
               (clutch-db-escape-identifier c (plist-get fk :ref-table))
               (clutch-db-escape-identifier c (plist-get fk :ref-column))
               (clutch-db-value-to-literal c val #'clutch--format-value))
       clutch-connection))))

(defun clutch-record--field-action-context ()
  "Return the action context for the Record field at point, or nil."
  (when-let* ((cidx (get-text-property (point) 'clutch-col-idx))
              (ridx (get-text-property (point) 'clutch-row-idx))
              (result-buf clutch-record--result-buffer)
              ((buffer-live-p result-buf)))
    (let* ((fk-info (buffer-local-value 'clutch--fk-info result-buf))
           (fk (cdr (assq cidx fk-info)))
           (col-defs
            (buffer-local-value 'clutch--result-column-defs result-buf))
           (col-def (nth cidx col-defs))
           (value (get-text-property (point) 'clutch-full-value))
           (expandable-p
            (and (clutch--long-field-type-p col-def)
                 (> (length (clutch--format-value value)) 80)))
           (action (cond
                    (fk 'follow-fk)
                    ((and expandable-p
                          (memq cidx clutch-record--expanded-fields))
                     'collapse)
                    (expandable-p 'expand)
                    (t 'show-value))))
      (list :action action
            :column-index cidx
            :row-index ridx
            :result-buffer result-buf
            :foreign-key fk
            :value value))))

(defun clutch-record--field-action-description ()
  "Return the transient description for the Record field action at point."
  (if-let* ((context (clutch-record--field-action-context)))
      (pcase (plist-get context :action)
        ('follow-fk "Follow FK")
        ('expand "Expand")
        ('collapse "Collapse")
        ('show-value "Show value"))
    "Field action unavailable"))

(defun clutch-record--pending-changes-p ()
  "Return non-nil if the parent result buffer has staged edits."
  (when-let* ((result-buf clutch-record--result-buffer)
              ((buffer-live-p result-buf)))
    (with-current-buffer result-buf
      (clutch-result--pending-changes-p))))

;;;###autoload
(defun clutch-record-toggle-expand ()
  "Run the expand, collapse, foreign-key, or display action at point."
  (interactive)
  (if-let* ((context (clutch-record--field-action-context)))
      (pcase (plist-get context :action)
        ('follow-fk
         (clutch-record--follow-fk
          (plist-get context :foreign-key)
          (plist-get context :value)
          (plist-get context :result-buffer)))
        ((or 'expand 'collapse)
         (let ((cidx (plist-get context :column-index)))
           (if (eq (plist-get context :action) 'collapse)
               (setq clutch-record--expanded-fields
                     (delq cidx clutch-record--expanded-fields))
             (push cidx clutch-record--expanded-fields))
           (clutch-record--render)))
        ('show-value
         (message "%s"
                  (clutch--view-format-value (plist-get context :value)))))
    (user-error "No field at point")))

;;;###autoload
(defun clutch-record-next-row ()
  "Show the next row in the Record buffer."
  (interactive)
  (let ((total (with-current-buffer clutch-record--result-buffer
                 (length (clutch--result-display-rows)))))
    (if (>= (1+ clutch-record--row-idx) total)
        (user-error "Already at last row")
      (cl-incf clutch-record--row-idx)
      (setq clutch-record--expanded-fields nil)
      (clutch-record--render))))

;;;###autoload
(defun clutch-record-prev-row ()
  "Show the previous row in the Record buffer."
  (interactive)
  (if (<= clutch-record--row-idx 0)
      (user-error "Already at first row")
    (cl-decf clutch-record--row-idx)
    (setq clutch-record--expanded-fields nil)
    (clutch-record--render)))

;;;###autoload
(defun clutch-record-view-value ()
  "Display the field value at point in an appropriate pop-up buffer.
Selects JSON, XML, or binary string view based on column type and content."
  (interactive)
  (let* ((cidx (get-text-property (point) 'clutch-col-idx))
         (_ridx (get-text-property (point) 'clutch-row-idx))
         (val  (if cidx
                   (get-text-property (point) 'clutch-full-value)
                 (user-error "No field at point")))
         (col-def (when (and cidx (buffer-live-p clutch-record--result-buffer))
                    (with-current-buffer clutch-record--result-buffer
                      (nth cidx clutch--result-column-defs)))))
    (clutch--dispatch-view val col-def)))

;;;###autoload
(defun clutch-record-live-view-value ()
  "Open a live-follow viewer for the record field at point."
  (interactive)
  (clutch--open-live-view))

;;;###autoload
(defun clutch-record-refresh ()
  "Refresh the Record buffer."
  (interactive)
  (clutch-record--render))

;;;; Dispatch menus

(transient-define-prefix clutch-result-dispatch ()
  "Dispatch menu for clutch result buffer."
  [ :pad-keys t
   ["Navigate"
    ("TAB" "Next cell"       clutch-result-next-cell)
    ("<backtab>" "Prev cell" clutch-result-prev-cell)
    ("n" "Down row"          clutch-result-down-cell)
    ("p" "Up row"            clutch-result-up-cell)
    ("RET" "Open record"     clutch-result-open-record)
    ("C" "Go to column"      clutch-result-goto-column)
    ("?" "Column info"       clutch-result-column-info)]
   ["Query"
    ("g" "Re-execute"        clutch-result-rerun)
    ("x" "Preview execution" clutch-preview-execution-sql)
    ("#" "Count total"       clutch-result-count-total
     :if clutch-result--server-rewritable-p)
    ("A" "Aggregate"         clutch-result-aggregate)]
   [ :description clutch-result--staged-transient-heading
     :if (lambda () (clutch-result--action-supported-p 'sql-staged))
    ("y" "Copy staged SQL"  clutch-result-copy-pending-sql
     :inapt-if-not clutch-result--pending-changes-p)
    ("Y" "Save staged SQL"  clutch-result-save-pending-sql
     :inapt-if-not clutch-result--pending-changes-p)]
   ["Filter / Sort"
    ("/" "Client filter" clutch-result-filter
     :description clutch-result--client-filter-transient-description)
    ("W" "WHERE filter" clutch-result-apply-filter
     :description clutch-result--where-filter-transient-description
     :if clutch-result--server-rewritable-p)
    ("s" "Sort current" clutch-result-sort-by-column
     :description clutch-result--sort-transient-description
     :inapt-if-not clutch-result--column-name-at-point)]]
  [ :pad-keys t
   ["Pages"
    ("N" "Next page"         clutch-result-next-page)
    ("P" "Prev page"         clutch-result-prev-page)
    ("M-<" "First page"      clutch-result-first-page)
    ("M->" "Last page"       clutch-result-last-page)
    ("]" "Page right →│"     clutch-result-scroll-right)
    ("[" "Page left │←"      clutch-result-scroll-left)]
   ["Mutate"
    :if (lambda () (clutch-result--action-supported-p 'sql-mutation))
    ("C-c '" "Edit / re-edit" clutch-result-edit-cell)
    ("i" "Stage insert"      clutch-result-insert-row)
    ("I" "Clone row → insert" clutch-clone-row-to-insert)
    ("d" "Stage delete"      clutch-result-delete-rows)
    ("C-c C-c" "Commit staged" clutch-result-commit
     :inapt-if-not clutch-result--pending-changes-p)
    ("C-c C-k" "Discard staged at point" clutch-result-discard-pending-at-point
     :inapt-if-not clutch-result--pending-changes-p)]
   ["Layout"
    ("=" "Widen column"      clutch-result-widen-column)
    ("-" "Narrow column"     clutch-result-narrow-column)
    ("f" "Fullscreen" clutch-result-fullscreen-toggle
     :description clutch-result--fullscreen-transient-description)]]
  [ :pad-keys t
   ["Inspect"
    ("v" "View value" clutch-result-view-value)
    ("V" "Live view (follow point)" clutch-result-live-view-value)]
   ["Copy / Export"
    ("c" "Copy…" clutch-result-copy-dispatch)
    ("k" "Copy agent context" clutch-copy-context-for-agent)
    ("e" "Export" clutch-result-export)]])

(transient-define-prefix clutch-record-dispatch ()
  "Dispatch menu for clutch record buffer."
  [ :pad-keys t
   ["Navigate"
    ("n" "Next row"     clutch-record-next-row)
    ("p" "Prev row"     clutch-record-prev-row)
    ("RET" "Toggle field" clutch-record-toggle-expand
     :description clutch-record--field-action-description
     :inapt-if-not clutch-record--field-action-context)]
   ["Inspect"
    ("v" "View value" clutch-record-view-value)
    ("V" "Live view (follow point)" clutch-record-live-view-value)]
   ["Mutate"
    ("C-c '" "Edit / re-edit" clutch-result-edit-cell)
    ("C-c C-k" "Discard staged at point" clutch-result-discard-pending-at-point
     :inapt-if-not clutch-record--pending-changes-p)
    ("I" "Clone row → insert" clutch-clone-row-to-insert)]
   ["Other"
    ("g" "Refresh" clutch-record-refresh)
    ("q" "Quit"    quit-window)]])

(provide 'clutch-result)

;;; clutch-result.el ends here
