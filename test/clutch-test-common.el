;;; clutch-test-common.el --- Shared ERT helpers for clutch tests -*- lexical-binding: t; -*-

;;; Commentary:

;; Shared setup and helpers used by clutch ERT files.

;;; Code:

(require 'cl-lib)

(require 'ert)

(require 'clutch-backend)

(require 'clutch-db-jdbc)

(require 'clutch)

;;;; Test helpers

(defun clutch-test--execute-and-present (sql connection &optional context)
  "Execute SQL on CONNECTION and present its result using CONTEXT."
  (clutch--present-statement-outcome
   sql connection (clutch--execute-statement sql connection t context)))

(defun clutch-test--debug-buffer-string ()
  "Return the current dedicated clutch debug buffer contents."
  (let ((buf (get-buffer clutch-debug-buffer-name)))
    (should (buffer-live-p buf))
    (with-current-buffer buf
      (buffer-string))))

(defun clutch-test--clear-problem-capture ()
  "Clear captured problem records across test buffers."
  (setq clutch--problem-records-by-conn
        (make-hash-table :test 'eq :weakness 'key))
  (dolist (buf (buffer-list))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (setq-local clutch--buffer-error-details nil)))))

(defmacro clutch-test--with-isolated-metadata-caches (&rest body)
  "Run BODY with fresh metadata state and no installed lifecycle consumers."
  (declare (indent 0) (debug (body)))
  `(let ((clutch--schema-cache (make-hash-table :test 'eq))
         (clutch--table-metadata-cache (make-hash-table :test 'eq))
         (clutch--column-details-queue-cache (make-hash-table :test 'eq))
         (clutch--column-details-active-cache (make-hash-table :test 'eq))
         (clutch--help-doc-cache (make-hash-table :test 'eq))
         (clutch--object-cache (make-hash-table :test 'eq))
         (clutch--object-warmup-timers (make-hash-table :test 'eq))
         (clutch--object-warmup-generations (make-hash-table :test 'eq))
         (clutch--schema-status-cache (make-hash-table :test 'eq))
         (clutch--schema-refresh-tickets (make-hash-table :test 'eq))
         (clutch--schema-refresh-ticket-counter 0)
         (clutch--schema-install-timers (make-hash-table :test 'eq))
         (clutch--metadata-ticket-counter 0)
         (clutch--schema-cache-updated-hook nil)
         (clutch--metadata-state-changed-hook nil)
         (clutch--table-metadata-updated-hook nil))
     ,@body))

(defun clutch-test--primary-row-identity (&optional table columns indices)
  "Return primary-key row identity metadata for tests."
  (let ((indices (or indices '(0))))
    (list :kind 'primary-key
          :name "PRIMARY"
          :table (or table "users")
          :columns (or columns '("id"))
          :indices indices
          :source-indices indices)))

(defun clutch-test--completion-candidates (capf &optional prefix)
  "Return CAPF completion candidates matching PREFIX.
When PREFIX is nil, use the text between CAPF's bounds, matching the real
`completion-at-point' filtering path."
  (all-completions
   (or prefix
       (buffer-substring-no-properties (nth 0 capf) (nth 1 capf)))
   (nth 2 capf)))

(defun clutch-test--insert-field-value-bounds (field-name)
  "Return visible value bounds for FIELD-NAME in an insert form test buffer."
  (save-excursion
    (goto-char (point-min))
    (unless (re-search-forward
             (concat "^" (regexp-quote field-name)
                     "\\(?:[[:space:]][^:\n]*\\)?: ")
             nil t)
      (ert-fail (format "No visible insert field named %s" field-name)))
    (cons (point) (line-end-position))))

(defun clutch-test--goto-insert-field-value (field-name &optional end)
  "Move point to FIELD-NAME's visible value start, or end when END is non-nil."
  (let ((bounds (clutch-test--insert-field-value-bounds field-name)))
    (goto-char (if end (cdr bounds) (car bounds)))))

(defun clutch-test--set-insert-field-value (field-name value)
  "Replace FIELD-NAME's visible insert-form value with VALUE."
  (pcase-let ((`(,beg . ,end)
               (clutch-test--insert-field-value-bounds field-name)))
    (goto-char beg)
    (delete-region beg end)
    (insert value)))

(defmacro clutch-test--with-connection-data-model (spec &rest body)
  "Run BODY with SPEC identifying a test connection's backend data model.
SPEC is (CONN BACKEND MODEL)."
  (declare (indent 1) (debug ((form form form) body)))
  (pcase-let ((`(,conn ,backend ,model) spec))
    `(let ((clutch-test--conn ,conn)
           (clutch-test--backend ,backend)
           (clutch-test--model ,model))
       (cl-letf (((symbol-function 'clutch-db-backend-key)
                  (lambda (conn)
                    (should (eq conn clutch-test--conn))
                    clutch-test--backend))
                 ((symbol-function 'clutch--backend-key-from-conn)
                  (lambda (conn)
                    (should (eq conn clutch-test--conn))
                    clutch-test--backend))
                 ((symbol-function 'clutch-backend-data-model)
                  (lambda (backend)
                    (should (eq backend clutch-test--backend))
                    clutch-test--model)))
         ,@body))))

(defmacro clutch-test--with-native-document-result-buffer (&rest body)
  "Run BODY in a temporary result buffer for a native document surface."
  (declare (indent 0) (debug (body)))
  `(with-temp-buffer
     (setq-local clutch-connection 'document-conn
                 clutch--connection-params nil)
     (clutch-test--with-connection-data-model
         ('document-conn 'mongodb 'document)
       ,@body)))

(defun clutch-test--init-result-state (spec)
  "Initialize the current buffer as a small result buffer.
SPEC is a plist.  Common keys are :columns, :column-defs, :rows,
:connection, :connection-params, :source-table, :base-query, :last-query,
:where-filter, :order-by, :row-identity, :row-identity-status,
:row-identity-error-message, :filter-pattern, :filtered-rows, :marked-rows,
:pending-edits, :pending-deletes, :pending-inserts, :sort-column,
:sort-descending, :page-current, :page-total-rows, :column-widths,
:server-pageable, :server-rewritable, :result-max-rows, and :render."
  (let* ((columns (if (plist-member spec :columns)
                      (plist-get spec :columns)
                    '("id" "name")))
         (raw-column-defs (if (plist-member spec :column-defs)
                              (plist-get spec :column-defs)
                            (mapcar (lambda (name) (list :name name)) columns)))
         (column-defs
          (cl-mapcar
           (lambda (name definition)
             (if (plist-member definition :source-column)
                 definition
               (plist-put (copy-sequence definition) :source-column name)))
           columns raw-column-defs))
         (rows (if (plist-member spec :rows)
                   (plist-get spec :rows)
                 '((1 "alice") (2 "bob"))))
         (connection (if (plist-member spec :connection)
                         (plist-get spec :connection)
                       'fake-conn))
         (page-current (if (plist-member spec :page-current)
                           (plist-get spec :page-current)
                         0))
         (page-total-rows (if (plist-member spec :page-total-rows)
                              (plist-get spec :page-total-rows)
                            (length rows)))
         (result-max-rows (if (plist-member spec :result-max-rows)
                              (plist-get spec :result-max-rows)
                            100))
         (column-widths (if (plist-member spec :column-widths)
                            (plist-get spec :column-widths)
                          (vconcat
                           (cl-loop for idx below (length columns)
                                    collect (if (= idx 0) 2 8))))))
    (clutch-result-mode)
    (setq-local clutch-connection connection
                clutch--connection-params (plist-get spec :connection-params)
                clutch--result-source-table (plist-get spec :source-table)
                clutch--base-query (plist-get spec :base-query)
                clutch--last-query (plist-get spec :last-query)
                clutch--where-filter (plist-get spec :where-filter)
                clutch--order-by (plist-get spec :order-by)
                clutch--result-columns columns
                clutch--result-column-defs column-defs
                clutch--result-rows rows
                clutch--filtered-rows (plist-get spec :filtered-rows)
                clutch--filter-pattern (plist-get spec :filter-pattern)
                clutch--pending-edits (plist-get spec :pending-edits)
                clutch--pending-deletes (plist-get spec :pending-deletes)
                clutch--pending-inserts (plist-get spec :pending-inserts)
                clutch--marked-rows (plist-get spec :marked-rows)
                clutch--row-identity (plist-get spec :row-identity)
                clutch--row-identity-status (plist-get spec
                                                       :row-identity-status)
                clutch--row-identity-error-message
                (plist-get spec :row-identity-error-message)
                clutch--sort-column (plist-get spec :sort-column)
                clutch--sort-descending (plist-get spec :sort-descending)
                clutch--page-current page-current
                clutch--page-total-rows page-total-rows
                clutch--result-server-pageable (plist-get spec :server-pageable)
                clutch--result-server-rewritable (plist-get spec
                                                             :server-rewritable)
                clutch--query-elapsed nil
                clutch-result-max-rows result-max-rows
                clutch--column-widths column-widths)
    (when (plist-get spec :render)
      (clutch--render-result))))

(defmacro clutch-test--with-result-state (spec &rest body)
  "Run BODY in a temporary `clutch-result-mode' buffer.
SPEC has the same shape as `clutch-test--init-result-state'."
  (declare (indent 1) (debug (sexp body)))
  `(with-temp-buffer
     (clutch-test--init-result-state (list ,@spec))
     (let ((inhibit-read-only t))
       ,@body)))

(defmacro clutch-test--with-result-state-buffer (var spec &rest body)
  "Bind VAR to a named result buffer initialized from SPEC while running BODY."
  (declare (indent 2) (debug (symbolp sexp body)))
  `(let ((,var (generate-new-buffer "*clutch-result*")))
     (unwind-protect
         (progn
           (with-current-buffer ,var
             (clutch-test--init-result-state (list ,@spec)))
           ,@body)
       (when (buffer-live-p ,var)
         (kill-buffer ,var)))))

(defmacro clutch-test--with-result-buffer (spec &rest body)
  "Run BODY with result rendering isolated to buffer NAME.
SPEC is (NAME &optional REFRESH-FN).
REFRESH-FN, when non-nil, replaces `clutch--refresh-display'."
  (declare (indent 1) (debug ((form &optional form) body)))
  (pcase-let ((`(,name ,refresh-fn) spec))
    `(let ((clutch-test--result-name ,name)
           (clutch-test--refresh-fn ,refresh-fn))
       (cl-letf (((symbol-function 'clutch-result--buffer-name)
                  (lambda () clutch-test--result-name))
                 ((symbol-function 'clutch-result--show-buffer) #'ignore)
                 ((symbol-function 'clutch--load-fk-info) #'ignore)
                 ((symbol-function 'clutch--refresh-display)
                  (or clutch-test--refresh-fn #'ignore)))
         (unwind-protect
             (progn ,@body)
           (when-let* ((buf (get-buffer clutch-test--result-name)))
             (kill-buffer buf)))))))

(defun clutch-test--setup-rendered-result (&optional rows)
  "Populate the current buffer with a rendered three-column result table.
ROWS defaults to a small three-row sample."
  (let ((rows (or rows '((1 "alpha" "oslo")
                         (2 "bravo" "rome")
                         (3 "charlie" "paris")))))
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name" "city")
                clutch--result-column-defs
                '((:name "id" :type-category numeric)
                  (:name "name" :type-category text)
                  (:name "city" :type-category text))
                clutch--result-rows rows
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--row-identity (clutch-test--primary-row-identity
                                       "users" '("id") '(0))
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows (length rows)
                clutch--query-elapsed nil
                clutch-result-max-rows 100
                clutch--column-widths [3 8 8])
    (clutch--render-result)))

(defun clutch-test--rendered-line-at (ridx)
  "Return rendered line RIDX from the current result buffer."
  (let ((start (aref clutch--row-start-positions ridx))
        (end (or (and (< (1+ ridx) (length clutch--row-start-positions))
                      (aref clutch--row-start-positions (1+ ridx)))
                 (point-max))))
    (buffer-substring start end)))

(provide 'clutch-test-common)

;;; clutch-test-common.el ends here
