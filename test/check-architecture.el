;;; check-architecture.el --- Check module dependency boundaries -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: MIT

;;; Commentary:
;; Run with `emacs -Q --batch -L . -L test -l check-architecture'.

;;; Code:

(require 'cl-lib)

(defconst clutch--architecture-cross-declaration-baseline 183
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

(defun clutch--architecture-symbols (form)
  "Return symbols contained in FORM."
  (cond
   ((symbolp form) (list form))
   ((consp form)
    (nconc (clutch--architecture-symbols (car form))
           (clutch--architecture-symbols (cdr form))))
   ((vectorp form)
    (cl-loop for value across form
             nconc (clutch--architecture-symbols value)))))

(defun clutch--architecture-target-name (target)
  "Return Clutch module name represented by TARGET, or nil."
  (let ((name (cond ((symbolp target) (symbol-name target))
                    ((stringp target) target))))
    (and name (string-prefix-p "clutch-" name) name)))

(defun clutch--architecture-dependencies (source forms)
  "Return SOURCE's Clutch dependencies described by FORMS.
Each entry is (SOURCE TARGET KIND SYMBOL), where KIND is `require' or
`declare-function'."
  (cl-loop for form in forms
           for kind = (and (consp form) (car form))
           for target = (pcase kind
                          ('require (clutch--architecture-target-name (nth 1 form)))
                          ('declare-function
                           (clutch--architecture-target-name (nth 2 form))))
           when target
           collect (list source target kind
                         (and (eq kind 'declare-function) (nth 1 form)))))

(defun clutch--architecture-declarations (source forms)
  "Return SOURCE's Clutch `declare-function' entries from FORMS."
  (cl-loop for form in forms
           for target = (and (consp form)
                             (eq (car form) 'declare-function)
                             (clutch--architecture-target-name (nth 2 form)))
           when target collect (list source target (nth 1 form))))

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
  (let* ((repo (file-name-directory (directory-file-name
                                     (file-name-directory load-file-name))))
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
         (uses (cl-mapcan (lambda (entry)
                            (cl-mapcan #'clutch--architecture-symbols
                                       (cl-remove-if (lambda (form)
                                                       (and (consp form)
                                                            (eq (car form)
                                                                'declare-function)))
                                                     (cdr entry))))
                          parsed))
         (stale (cl-remove-if (lambda (entry) (memq (nth 2 entry) uses)) declarations))
         (cross (cl-remove-if (lambda (entry) (equal (nth 0 entry) (nth 1 entry)))
                              declarations))
         (adapter-workflow
          (cl-remove-if-not
           (lambda (entry)
             (and (member (nth 0 entry) clutch--architecture-adapter-modules)
                  (member (nth 1 entry) clutch--architecture-workflow-modules)))
           dependencies))
         (unapproved
          (cl-remove-if (lambda (entry)
                          (member (cons (nth 0 entry) (nth 1 entry))
                                  clutch--architecture-adapter-workflow-allowlist))
                        adapter-workflow))
         (largest-scc (clutch--architecture-largest-scc modules dependencies))
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
    (when (> (length cross) clutch--architecture-cross-declaration-baseline)
      (push (format "cross-module declarations: %d (maximum %d)"
                    (length cross) clutch--architecture-cross-declaration-baseline)
            errors))
    (when (> largest-scc 8)
      (push (format "largest SCC: %d (maximum 8)" largest-scc) errors))
    (when unapproved
      (push (format "unapproved adapter-to-workflow dependencies: %S" unapproved)
            errors))
    (if errors
        (progn
          (dolist (error-message (nreverse errors))
            (princ (format "ARCHITECTURE ERROR: %s\n" error-message)))
          (kill-emacs 1))
      (princ (format "Architecture OK: stale=0 cross-declarations=%d largest-scc=%d\n"
                     (length cross) largest-scc)))))

(clutch--architecture-main)

;;; check-architecture.el ends here
