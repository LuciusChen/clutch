;;; clutch-test-common.el --- Shared ERT helpers for clutch tests -*- lexical-binding: t; -*-

;;; Commentary:

;; Shared setup and helpers used by clutch ERT files.

;;; Code:

(require 'cl-lib)

(require 'ert)
(require 'ert-x)

(require 'clutch-backend)

(require 'clutch-db-jdbc)

(require 'clutch)

;; Tests stand in for connections with symbols such as `fake-conn', which
;; implement no backend; a result they produce records no context.
(cl-defmethod clutch-db-resolution-context ((_conn symbol))
  "Return nil for a symbol standing in for a connection."
  nil)

;;;; Test helpers

(cl-defstruct clutch-test-conn
  "A single-table metadata backend for result workflows, with no SQL engine.
TABLE and COLUMNS declare the available metadata.  Other tables and every
query fail the test rather than silently supplying empty results."
  (live t) table columns)

(cl-defmethod clutch-db-live-p ((conn clutch-test-conn))
  "Return whether CONN is open."
  (clutch-test-conn-live conn))

(cl-defmethod clutch-db-disconnect ((conn clutch-test-conn))
  "Close CONN."
  (setf (clutch-test-conn-live conn) nil))

(cl-defmethod clutch-db-resolution-context ((_conn clutch-test-conn))
  "Return nil because this backend has no namespace switches."
  nil)

(cl-defmethod clutch-db-column-details
  ((conn clutch-test-conn) table &optional schema catalog)
  "Return CONN's declared columns for TABLE, without SCHEMA or CATALOG."
  (should (clutch-db-live-p conn))
  (should-not (or schema catalog))
  (should (equal table (clutch-test-conn-table conn)))
  (clutch-test-conn-columns conn))

(cl-defmethod clutch-db-query ((_conn clutch-test-conn) sql)
  "Fail on unexpected SQL instead of pretending to execute SQL."
  (ert-fail (format "Unexpected test-backend query: %S" sql)))

(cl-defmethod clutch-db-escape-identifier ((_conn clutch-test-conn) name)
  "Quote NAME using standard SQL identifier syntax."
  (concat "\"" (string-replace "\"" "\"\"" name) "\""))

(cl-defmethod clutch-db-escape-literal ((_conn clutch-test-conn) value)
  "Quote VALUE using standard SQL string syntax."
  (concat "'" (string-replace "'" "''" value) "'"))

(defun clutch-test--await (predicate)
  "Run process output and idle timers until PREDICATE returns non-nil."
  (let ((deadline (+ (float-time) 30)))
    (while (not (funcall predicate))
      (when (> (float-time) deadline)
        (error "Timed out waiting for an asynchronous statement"))
      (accept-process-output nil 0.05)
      (ert-run-idle-timers))))

(defun clutch-test--await-queries ()
  "Wait until no statement runs, so every finished one has been presented."
  (clutch-test--await
   (lambda () (zerop (hash-table-count clutch--running-queries)))))

(defvar clutch-test--minibuffer-answers nil
  "Answers left for `clutch-test--with-minibuffer-answers'.")

(defun clutch-test--minibuffer-answer (default)
  "Return the next minibuffer answer, or DEFAULT when that answer is empty."
  (let ((answer (or (pop clutch-test--minibuffer-answers)
                    (error "No minibuffer answer left"))))
    (if (and (string-empty-p answer) default)
        (if (consp default) (car default) default)
      answer)))

(defmacro clutch-test--with-minibuffer-answers (answers &rest body)
  "Run BODY answering `read-string' and `completing-read' with ANSWERS.
Like the real readers, an empty answer to a read with a default returns
the default, which is what a user who just presses RET gets."
  (declare (indent 1))
  `(let ((clutch-test--minibuffer-answers (copy-sequence ,answers)))
     (cl-letf (((symbol-function 'read-string)
                (lambda (_prompt &optional _initial _history default &rest _)
                  (clutch-test--minibuffer-answer default)))
               ((symbol-function 'completing-read)
                (lambda (_prompt _collection &optional _predicate _require-match
                                 _initial _history default &rest _)
                  (clutch-test--minibuffer-answer default))))
       ,@body
       (should-not clutch-test--minibuffer-answers))))

(defun clutch-test--transient-menu-text (prefix)
  "Return the text of PREFIX's menu, opened from the current buffer.
Open the menu and quit it with keys, as a user would: its descriptions
then see this buffer, and Transient's command and quit handling does not
outlast the caller."
  (save-window-excursion
    (switch-to-buffer (current-buffer))
    (let ((suggest-key-bindings nil))
      (execute-kbd-macro (vconcat [?\M-x] (symbol-name prefix) [return])))
    (unwind-protect
        (with-current-buffer " *transient*"
          (buffer-string))
      (execute-kbd-macro (kbd "C-g")))))

(defun clutch-test--await-outcome (start)
  "Call START with a continuation and return the value passed to it."
  (let (outcome done)
    (funcall start (lambda (value) (setq outcome value done t)))
    (clutch-test--await (lambda () done))
    outcome))

(defun clutch-test--execute-and-present (sql connection &optional context)
  "Execute SQL on CONNECTION, present it using CONTEXT and return its result."
  (let ((outcome (clutch-test--await-outcome
                  (lambda (k)
                    (clutch--execute-statement sql connection t nil k context)))))
    (clutch--present-statement-outcome sql connection outcome)
    (plist-get outcome :result)))

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
:row-identity-error-message, :filter-pattern, :filtered-rows,
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

(defun clutch-test--select-cells (from &optional to)
  "Select the rendered cells from FROM to TO, each a (ROW COLUMN) list.
Set the mark on FROM's cell and leave point inside TO's, as dragging over
them does.  Without TO, only move point to FROM's cell."
  (apply #'clutch--goto-cell from)
  (when to
    ;; Batch Emacs starts with Transient Mark mode off.
    (setq-local transient-mark-mode t)
    (push-mark (point) t t)
    (apply #'clutch--goto-cell to)
    (forward-char 1)))

;;;; Helpers shared by the result, edit and query tests

(defun clutch-test--transient-suffix-for-key (prefix key)
  "Return the suffix under PREFIX bound to KEY."
  (cl-find-if
   (lambda (suffix)
     (and (slot-boundp suffix 'key)
          (equal (oref suffix key) key)))
   (transient-suffixes prefix)))

(defmacro clutch-test--with-sqlite-result (bindings setup sql &rest body)
  "Run SQL over SETUP in a fresh SQLite console, then evaluate BODY.
BINDINGS is (CONN RESULT).  SETUP is a list of SQL statements.
BODY runs in RESULT with its window selected.  Close the connection and
both buffers, and stop the refresh timer, even when BODY fails."
  (declare (indent 3) (debug ((symbolp symbolp) form form body)))
  (cl-destructuring-bind (conn result) bindings
    (let ((source (make-symbol "source")))
      `(progn
         (skip-unless (sqlite-available-p))
         (let* ((,conn (clutch-db-sqlite-connect '(:database ":memory:")))
                (,source (generate-new-buffer " *clutch-sqlite-source*"))
                (clutch--execution-refresh-timer nil)
                ,result)
           (unwind-protect
               (save-window-excursion
                 (dolist (statement ,setup)
                   (clutch-db-query ,conn statement))
                 (set-window-buffer (selected-window) ,source)
                 (with-current-buffer ,source
                   (clutch-mode)
                   (setq-local clutch-connection ,conn
                               clutch--connection-params
                               '(:backend sqlite :database ":memory:"))
                   (insert ,sql)
                   (clutch-execute-buffer)
                   (setq ,result clutch--last-result-buffer))
                 (set-window-buffer (selected-window) ,result)
                 (with-current-buffer ,result ,@body))
             (clutch--execution-refresh-stop)
             (when (buffer-live-p ,result) (kill-buffer ,result))
             (when (buffer-live-p ,source) (kill-buffer ,source))
             (when (clutch-db-live-p ,conn) (clutch-db-disconnect ,conn)))
           (should-not clutch--execution-refresh-timer))))))

(defmacro clutch-test--with-pop-to-buffer-capture (var &rest body)
  "Bind VAR to the buffer passed to `pop-to-buffer' while running BODY."
  (declare (indent 1) (debug (symbolp body)))
  `(let (,var)
     (unwind-protect
         (cl-letf (((symbol-function 'pop-to-buffer)
                    (lambda (buf &rest _args)
                      (setq ,var buf)
                      buf)))
           ,@body)
       (when (and ,var (buffer-live-p ,var))
         (kill-buffer ,var)))))

(defmacro clutch-test--with-insert-result-buffer (var spec &rest body)
  "Bind VAR to a result buffer initialized for insert tests by SPEC."
  (declare (indent 2))
  (let ((normalized-spec spec))
    (unless (memq :connection normalized-spec)
      (setq normalized-spec (append normalized-spec '(:connection nil))))
    (unless (memq :rows normalized-spec)
      (setq normalized-spec (append normalized-spec '(:rows nil))))
    `(clutch-test--with-result-state-buffer ,var ,normalized-spec
       ,@body)))

(provide 'clutch-test-common)

;;; clutch-test-common.el ends here
