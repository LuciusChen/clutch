;;; check-architecture.el --- Check module dependency boundaries -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:
;; Run with `emacs -Q --batch -L . -L test -l check-architecture'.

;;; Code:

(require 'cl-lib)

(defconst clutch--architecture-repo
  (file-name-directory (directory-file-name (file-name-directory load-file-name)))
  "Repository root containing the Clutch Lisp modules.")

(defconst clutch--architecture-cross-declaration-baseline 43
  "Maximum permitted number of cross-module Clutch declarations.")

(defconst clutch--architecture-adapter-workflow-allowlist
  '(("clutch-redis" . "clutch-query"))
  "Known adapter-to-workflow dependencies, as (SOURCE . TARGET) pairs.")

(defconst clutch--architecture-adapter-modules
  '("clutch-db-jdbc" "clutch-db-mysql" "clutch-db-pg" "clutch-db-sqlite"
    "clutch-mongodb" "clutch-redis")
  "Modules which adapt external database protocols.")

(defconst clutch--architecture-workflow-modules
  '("clutch-connection" "clutch-document" "clutch-edit" "clutch-object"
    "clutch-query" "clutch-result" "clutch-schema" "clutch-ui")
  "Modules which own user-facing workflows.")

(defconst clutch--architecture-foundation-allowlists
  '(("clutch-connection" "clutch-backend" "clutch-diagnostics"
                         "clutch-schema" "clutch-ui")
    ("clutch-schema" "clutch-backend" "clutch-diagnostics")
    ("clutch-sql" "clutch-backend" "clutch-schema")
    ("clutch-ui" "clutch-backend" "clutch-schema"))
  "Allowed outbound Clutch dependencies for lower-layer modules.")

(defconst clutch--architecture-largest-scc-baseline 2
  "Maximum permitted strongly connected component size.")

(defconst clutch--architecture-root-state-baseline 2
  "Maximum mutable state definitions permitted in the composition root.")

(defun clutch--architecture-root-state-forms (forms)
  "Return mutable package-state definitions from root FORMS."
  (let (state-forms)
    (dolist (form forms)
      (clutch--architecture-walk-dependencies
       form
       (lambda (candidate)
         (when (memq (car candidate)
                     '(defcustom defvar defvar-local define-minor-mode))
           (push candidate state-forms)))))
    (nreverse state-forms)))

(defun clutch--architecture-read-forms (file)
  "Read every top-level form from FILE without evaluating it."
  (with-temp-buffer
    (insert-file-contents file)
    (goto-char (point-min))
    (let (forms form)
      (condition-case err
          (while t
            (setq form (read (current-buffer)))
            (push form forms))
        (end-of-file (nreverse forms))
        (error (error "Cannot read %s: %s" file (error-message-string err)))))))

(defun clutch--architecture-feature-name (form)
  "Return feature name represented by FORM, or nil.
FORM may be a symbol, string, or a quoted symbol."
  (cond ((symbolp form) (symbol-name form))
        ((stringp form) form)
        ((and (consp form) (eq (car form) 'quote) (symbolp (nth 1 form)))
         (symbol-name (nth 1 form)))))

(defun clutch--architecture-target-name (target)
  "Return Clutch module name represented by TARGET, or nil."
  (let ((name (clutch--architecture-feature-name target)))
    (and name (or (string= name "clutch")
                  (string-prefix-p "clutch-" name)) name)))

(defun clutch--architecture-walk-dependencies (form function)
  "Call FUNCTION for dependency-bearing lists in FORM.
Ordinary quoted data is ignored.  In quasiquoted data, only unquoted forms
are scanned because those are evaluated at runtime."
  (cond ((vectorp form)
         (cl-loop for item across form do (clutch--architecture-walk-dependencies item function)))
        ((consp form)
         (cond ((eq (car form) 'quote) nil)
               ((and (symbolp (car form)) (string= (symbol-name (car form)) "`"))
                (clutch--architecture-walk-quasiquote (nth 1 form) function 1))
               (t (funcall function form)
                  (clutch--architecture-walk-dependencies (car form) function)
                  (clutch--architecture-walk-dependencies (cdr form) function))))))

(defun clutch--architecture-walk-quasiquote (form function depth)
  "Scan only runtime-unquoted portions of quasiquoted FORM."
  (cond ((vectorp form) (cl-loop for item across form do (clutch--architecture-walk-quasiquote item function depth)))
        ((atom form) nil)
        ((and (symbolp (car form)) (string= (symbol-name (car form)) "`"))
         (clutch--architecture-walk-quasiquote (nth 1 form) function (1+ depth)))
        ((and (symbolp (car form)) (member (symbol-name (car form)) '("," ",@")))
         (if (= depth 1)
             (clutch--architecture-walk-dependencies (nth 1 form) function)
           (clutch--architecture-walk-quasiquote (nth 1 form) function (1- depth))))
        (t (clutch--architecture-walk-quasiquote (car form) function depth)
           (clutch--architecture-walk-quasiquote (cdr form) function depth))))

(defun clutch--architecture-dependencies (source forms)
  "Return SOURCE's Clutch dependencies described by FORMS.
Each entry is (SOURCE TARGET KIND SYMBOL), where KIND is `require' or
`declare-function'."
  (let (dependencies)
    (dolist (form forms)
      (clutch--architecture-walk-dependencies
       form
       (lambda (candidate)
         (let* ((kind (car candidate))
                (target (pcase kind
                          ('require (clutch--architecture-target-name (nth 1 candidate)))
                          ('declare-function
                           (clutch--architecture-target-name (nth 2 candidate))))))
           (when target
             (push (list source target kind
                         (and (eq kind 'declare-function) (nth 1 candidate)))
                   dependencies))))))
    (nreverse dependencies)))

(defun clutch--architecture-declarations (source forms)
  "Return SOURCE's Clutch `declare-function' entries from FORMS."
  (cl-loop for dependency in (clutch--architecture-dependencies source forms)
           when (eq (nth 2 dependency) 'declare-function)
           collect (list (nth 0 dependency) (nth 1 dependency) (nth 3 dependency))))

(defun clutch--architecture-top-level-require-target (form)
  "Return the Clutch module mandatorily loaded by top-level FORM."
  (and (eq (car-safe form) 'require)
       (null (nth 3 form))
       (clutch--architecture-target-name (nth 1 form))))

(defun clutch--architecture-top-level-requires (forms)
  "Return Clutch modules mandatorily loaded by top-level FORMS."
  (cl-loop for form in forms
           for target = (clutch--architecture-top-level-require-target form)
           when target collect target))

(defun clutch--architecture-redundant-declarations (parsed)
  "Return declarations already covered by prior top-level requires in PARSED."
  (cl-loop
   for (source . forms) in parsed nconc
   (let (required redundant)
     (dolist (form forms)
       (if-let* ((target (clutch--architecture-top-level-require-target form)))
           (cl-pushnew target required :test #'equal)
         (when (eq (car-safe form) 'declare-function)
           (when-let* ((target (clutch--architecture-target-name (nth 2 form))))
             (when (member target required)
               (push (list source target (nth 1 form)) redundant))))))
     (nreverse redundant))))

(defun clutch--architecture-calls (form)
  "Return function symbols actually called by unquoted FORM.
Declaration forms and definition names are excluded."
  (cond
   ((symbolp form) (list form))
   ((vectorp form)
    (cl-loop for item across form nconc (clutch--architecture-calls item)))
   ((atom form) nil)
   ((eq (car form) 'quote) nil)
   ((and (symbolp (car form)) (string= (symbol-name (car form)) "`"))
    (clutch--architecture-quasiquote-calls (nth 1 form) 1))
   ((eq (car form) 'declare-function) nil)
   ((memq (car form) '(defun cl-defun defmacro cl-defmacro))
    (cl-mapcan #'clutch--architecture-calls (cdddr form)))
   (t (append (clutch--architecture-calls (car form))
              (clutch--architecture-calls (cdr form))))))

(defun clutch--architecture-quasiquote-calls (form depth)
  "Return executable references from quasiquoted FORM at DEPTH."
  (cond ((vectorp form)
         (cl-loop for item across form nconc (clutch--architecture-quasiquote-calls item depth)))
        ((atom form) nil)
        ((and (symbolp (car form)) (string= (symbol-name (car form)) "`"))
         (clutch--architecture-quasiquote-calls (nth 1 form) (1+ depth)))
        ((and (symbolp (car form)) (member (symbol-name (car form)) '("," ",@")))
         (if (= depth 1) (clutch--architecture-calls (nth 1 form))
           (clutch--architecture-quasiquote-calls (nth 1 form) (1- depth))))
        (t (append (clutch--architecture-quasiquote-calls (car form) depth)
                   (clutch--architecture-quasiquote-calls (cdr form) depth)))))

(defun clutch--architecture-stale-declarations (declarations parsed)
  "Return DECLARATIONS without a source-local symbol reference in PARSED."
  (cl-remove-if (lambda (entry)
                  (memq (nth 2 entry)
                        (cl-mapcan #'clutch--architecture-calls
                                   (cdr (assoc (nth 0 entry) parsed)))))
                declarations))

(defun clutch--architecture-unapproved-adapter-edges (dependencies)
  "Return adapter-to-workflow DEPENDENCIES absent from the allowlist."
  (cl-remove-if (lambda (entry)
                  (member (cons (nth 0 entry) (nth 1 entry))
                          clutch--architecture-adapter-workflow-allowlist))
                (cl-remove-if-not (lambda (entry)
                                    (and (member (nth 0 entry) clutch--architecture-adapter-modules)
                                         (member (nth 1 entry) clutch--architecture-workflow-modules)))
                                  dependencies)))

(defun clutch--architecture-foundation-boundary-violations (dependencies)
  "Return lower-layer DEPENDENCIES outside their explicit allowlists."
  (cl-loop for dependency in dependencies
           for source = (nth 0 dependency)
           for allowlist = (assoc source clutch--architecture-foundation-allowlists)
           when (and allowlist
                     (not (member (nth 1 dependency) (cdr allowlist))))
           collect dependency))

(defun clutch--architecture-composition-root-inbound-violations (dependencies)
  "Return implementation DEPENDENCIES that point back to `clutch'."
  (cl-remove-if-not
   (lambda (dependency)
     (and (equal (nth 1 dependency) "clutch")
          (not (equal (nth 0 dependency) "clutch"))))
   dependencies))

(defun clutch--architecture-adjacent-p (source target dependencies)
  "Return non-nil when DEPENDENCIES connects SOURCE to TARGET."
  (cl-some (lambda (dependency)
             (and (equal source (nth 0 dependency))
                  (equal target (nth 1 dependency))))
           dependencies))

(defun clutch--architecture-largest-scc (nodes dependencies)
  "Return the size of the largest strongly connected component.
NODES names modules and DEPENDENCIES is the edge list from the reader."
  (let ((largest 0))
    (dolist (node nodes)
      (let ((component
             (cl-remove-if-not
              (lambda (other)
                (and (clutch--architecture-reachable-p node other nodes dependencies)
                     (clutch--architecture-reachable-p other node nodes dependencies)))
              nodes)))
        (setq largest (max largest (length component)))))
    largest))

(defun clutch--architecture-reachable-p (from to nodes dependencies)
  "Return non-nil when TO is reachable from FROM in DEPENDENCIES."
  (let ((seen (list from))
        (queue (list from)))
    (while queue
      (let ((node (pop queue)))
        (dolist (next nodes)
          (when (and (not (member next seen))
                     (clutch--architecture-adjacent-p node next dependencies))
            (push next seen)
            (push next queue)))))
    (member to seen)))

(defun clutch--architecture-main ()
  "Run architecture checks and exit nonzero when a boundary is violated."
  (let* ((repo clutch--architecture-repo)
         ;; `clutch.el' is deliberately included as the composition root.
         (files (directory-files repo t "\\`clutch.*\\.el\\'"))
         (modules (mapcar #'file-name-base files))
         (parsed (mapcar (lambda (file)
                           (cons (file-name-base file)
                                 (clutch--architecture-read-forms file)))
                         files))
         (dependencies (cl-mapcan (lambda (entry)
                                    (clutch--architecture-dependencies
                                     (car entry) (cdr entry)))
                                  parsed))
         (declarations (cl-mapcan (lambda (entry)
                                    (clutch--architecture-declarations
                                     (car entry) (cdr entry)))
                                  parsed))
         (stale (clutch--architecture-stale-declarations declarations parsed))
         (redundant (clutch--architecture-redundant-declarations parsed))
         (cross (cl-remove-if (lambda (entry) (equal (nth 0 entry) (nth 1 entry)))
                              declarations))
         (unapproved (clutch--architecture-unapproved-adapter-edges dependencies))
         (foundation-violations
          (clutch--architecture-foundation-boundary-violations dependencies))
         (composition-root-violations
          (clutch--architecture-composition-root-inbound-violations
           dependencies))
         (largest-scc (clutch--architecture-largest-scc modules dependencies))
         (root-state
          (clutch--architecture-root-state-forms
           (cdr (assoc "clutch" parsed))))
         errors)
    (dolist (edge (sort (cl-delete-duplicates
                          (mapcar (lambda (dependency)
                                    (cons (nth 0 dependency) (nth 1 dependency)))
                                  dependencies)
                          :test #'equal)
                         (lambda (left right)
                           (string< (format "%s/%s" (car left) (cdr left))
                                    (format "%s/%s" (car right) (cdr right))))))
      (princ (format "%s -> %s (require=%d declare-function=%d)\n"
                     (car edge) (cdr edge)
                     (cl-count-if (lambda (dependency)
                                    (and (equal edge
                                                (cons (nth 0 dependency)
                                                      (nth 1 dependency)))
                                         (eq (nth 2 dependency) 'require)))
                                  dependencies)
                     (cl-count-if (lambda (dependency)
                                    (and (equal edge
                                                (cons (nth 0 dependency)
                                                      (nth 1 dependency)))
                                         (eq (nth 2 dependency) 'declare-function)))
                                  dependencies))))
    (when stale
      (push (format "stale declarations: %S" stale) errors))
    (when redundant
      (push (format "declarations duplicated by top-level require: %S"
                    redundant)
            errors))
    (when (> (length cross) clutch--architecture-cross-declaration-baseline)
      (push (format "cross-module declarations: %d (maximum %d)"
                    (length cross) clutch--architecture-cross-declaration-baseline)
            errors))
    (when (> largest-scc clutch--architecture-largest-scc-baseline)
      (push (format "largest SCC: %d (maximum %d)"
                    largest-scc clutch--architecture-largest-scc-baseline)
            errors))
    (when (> (length root-state) clutch--architecture-root-state-baseline)
      (push (format "composition-root state definitions: %d (maximum %d)"
                    (length root-state)
                    clutch--architecture-root-state-baseline)
            errors))
    (when unapproved
      (push (format "unapproved adapter-to-workflow dependencies: %S" unapproved)
            errors))
    (when foundation-violations
      (push (format "lower-layer boundary violations: %S" foundation-violations)
            errors))
    (when composition-root-violations
      (push (format "dependencies into composition root: %S"
                    composition-root-violations)
            errors))
    (if errors
        (progn
          (dolist (error-message (nreverse errors))
            (princ (format "ARCHITECTURE ERROR: %s\n" error-message)))
          (kill-emacs 1))
      (princ (format (concat "Architecture OK: stale=0 cross-declarations=%d "
                             "largest-scc=%d root-state=%d\n")
                     (length cross) largest-scc (length root-state))))))

(unless (bound-and-true-p clutch--architecture-skip-main)
  (clutch--architecture-main))

(provide 'check-architecture)

;;; check-architecture.el ends here
