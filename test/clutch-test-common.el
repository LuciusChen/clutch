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

(defun clutch-test--goto-insert-field-value-start (field-name)
  "Move point to the visible value start for FIELD-NAME."
  (goto-char (car (clutch-test--insert-field-value-bounds field-name))))

(defun clutch-test--goto-insert-field-value-end (field-name)
  "Move point to the visible value end for FIELD-NAME."
  (goto-char (cdr (clutch-test--insert-field-value-bounds field-name))))

(defun clutch-test--current-insert-field-name ()
  "Return the visible insert field name on the current line."
  (save-excursion
    (beginning-of-line)
    (when (looking-at "\\([^[:space:]:]+\\)")
      (match-string-no-properties 1))))

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

(provide 'clutch-test-common)

;;; clutch-test-common.el ends here
