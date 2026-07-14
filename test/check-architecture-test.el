;;; check-architecture-test.el --- Tests for architecture reader -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

(require 'ert)
(defvar clutch--architecture-skip-main t)
(require 'check-architecture)

(ert-deftest clutch-architecture-diagnostics-standalone-load ()
  "Diagnostics must own debug state without loading workflow modules."
  (should-not (featurep 'clutch))
  (should-not (featurep 'clutch-connection))
  (require 'clutch-diagnostics)
  (should-not (featurep 'clutch))
  (should-not (featurep 'clutch-connection))
  (should (fboundp 'clutch-debug-mode))
  (should (boundp 'clutch-debug-mode))
  (should (equal clutch-debug-buffer-name "*clutch-debug*")))

(ert-deftest clutch-architecture-reader-cases ()
  "Exercise dependency and source-reference reader cases."
  (dolist (case '((nested ((progn (require 'clutch-query))) ("clutch-query"))
                  (quoted ('(require 'clutch-query)) nil)
                  (transient (["x" "Run" clutch-execute-dwim]) (clutch-execute-dwim))
                  (callback ('(:eval (clutch--header-with-disconnect-badge base)))
                            nil)))
    (pcase-let ((`(,name ,forms ,expected) case))
      (if (memq name '(nested quoted quasiquote))
          (should (equal (mapcar (lambda (edge) (nth 1 edge))
                                 (clutch--architecture-dependencies "source" forms))
                         expected))
        (should (cl-every (lambda (symbol)
                            (memq symbol (clutch--architecture-calls (car forms))))
                          expected)))))
  (should (equal (clutch--architecture-target-name 'clutch) "clutch"))
  (should (equal (mapcar (lambda (edge) (nth 1 edge))
                         (clutch--architecture-dependencies
                          "source" (list (read "`((require 'clutch-ui) ,(require 'clutch-query) ,@(list (require 'clutch-object)) `,(require 'clutch-schema))"))))
                 '("clutch-query" "clutch-object")))
  (should (equal (mapcar (lambda (edge) (nth 1 edge))
                         (clutch--architecture-dependencies
                          "source"
                          (list (read "[\"x\" (lambda () (require 'clutch-query))]"))))
                 '("clutch-query")))
  (should (equal (clutch--architecture-target-name "clutch-query") "clutch-query"))
  (let* ((decl '("source" "clutch-target" target-fn))
         (parsed '(("source" (defun target-fn () nil)))) )
    (should (equal (clutch--architecture-stale-declarations (list decl) parsed)
                   (list decl))))
  (let* ((decl '("source" "clutch-target" target-fn))
         (parsed '(("source" (declare-function target-fn "clutch-target" ())))))
    (should (equal (clutch--architecture-stale-declarations (list decl) parsed)
                   (list decl))))
  (should (= 2 (clutch--architecture-largest-scc
                '(a b c) '((a b require nil) (b a require nil)))))
  (should (member '("clutch-db-pg" "clutch-query" require nil)
                  (clutch--architecture-unapproved-adapter-edges
                   '(("clutch-redis" "clutch-query" require nil)
                     ("clutch-db-pg" "clutch-query" require nil))))))

(ert-deftest clutch-architecture-quoted-reference-cases ()
  "Distinguish quoted data from executable reference payloads."
  (dolist (case `((quoted-data ,(read "'(target-fn metadata)") t)
                  (quoted-eval ,(read "'(:eval (target-fn))") t)
                  (inert-defconst ,(read "(defconst inert '(:eval (target-fn)))") t)
                  (function-ref ,(read "#'target-fn") nil)
                  (transient-vector ["x" "Run" target-fn] nil)
                  (static-quasiquote ,(read "`(target-fn metadata)") t)
                  (unquoted-quasiquote ,(read "`(,(target-fn))") nil)
                  (spliced-quasiquote ,(read "`(,@(list (target-fn)))") nil)
                  (nested-quasiquote ,(read "`(`,(target-fn))") t)))
    (pcase-let ((`(,_ ,form ,stale-p) case)
                (decl '("source" "clutch-target" target-fn)))
      (should (eq stale-p
                  (equal (clutch--architecture-stale-declarations
                          (list decl) `(("source" ,form)))
                         (list decl)))))))

(ert-deftest clutch-architecture-top-level-requires-cover-declarations ()
  "A top-level require should make declarations to that owner redundant."
  (let* ((forms '((require 'clutch-eager)
                  (require 'clutch-optional nil t)
                  (when enabled (require 'clutch-lazy))
                  '(require 'clutch-quoted)
                  (declare-function eager-fn "clutch-eager" ())
                  (declare-function optional-fn "clutch-optional" ())
                  (declare-function lazy-fn "clutch-lazy" ())
                  (declare-function quoted-fn "clutch-quoted" ())
                  (declare-function late-fn "clutch-late" ())
                  (require 'clutch-late)
                  (autoload 'autoload-fn "clutch-autoload")
                  (declare-function autoload-fn "clutch-autoload" ())))
         (reverse-edge
          '((declare-function owner-fn "clutch-owner" ())))
         (parsed `(("clutch-source" . ,forms)
                   ("clutch-reverse" . ,reverse-edge)
                   ("clutch-owner"
                    (require 'clutch-reverse)
                    (declare-function reverse-fn "clutch-reverse" ())))))
    (should (equal (clutch--architecture-top-level-requires forms)
                   '("clutch-eager" "clutch-late")))
    (should
     (equal
      (clutch--architecture-redundant-declarations parsed)
      '(("clutch-source" "clutch-eager" eager-fn)
        ("clutch-owner" "clutch-reverse" reverse-fn))))))

(ert-deftest clutch-architecture-schema-is-a-metadata-leaf ()
  "Schema may depend only on backend and diagnostics modules."
  (should-not (featurep 'clutch-connection))
  (should-not (featurep 'clutch-object))
  (require 'clutch-schema)
  (should-not (featurep 'clutch-connection))
  (should-not (featurep 'clutch-object))
  (let ((dependencies '(("clutch-schema" "clutch-backend" require nil)
                        ("clutch-schema" "clutch-diagnostics" require nil)
                        ("clutch-schema" "clutch-connection"
                         declare-function clutch--connection-key)
                        ("clutch-object" "clutch-schema" require nil))))
    (should (equal (clutch--architecture-foundation-boundary-violations
                    dependencies)
                   '(("clutch-schema" "clutch-connection"
                      declare-function clutch--connection-key))))))

(ert-deftest clutch-architecture-ui-is-a-rendering-leaf ()
  "UI may render backend/schema data but must not depend on workflows."
  (let ((dependencies
         '(("clutch-ui" "clutch-backend" require nil)
           ("clutch-ui" "clutch-schema"
            declare-function clutch--cached-column-details)
           ("clutch-ui" "clutch-connection"
            declare-function clutch--connection-alive-p)
           ("clutch-result" "clutch-ui" require nil))))
    (should (equal (clutch--architecture-foundation-boundary-violations
                    dependencies)
                   '(("clutch-ui" "clutch-connection"
                      declare-function clutch--connection-alive-p))))))

(ert-deftest clutch-architecture-connection-does-not-depend-on-presenters ()
  "Connection lifecycle may publish UI data but not call workflow presenters."
  (let ((dependencies
         '(("clutch-connection" "clutch-backend" require nil)
           ("clutch-connection" "clutch-schema" require nil)
           ("clutch-connection" "clutch-ui"
            declare-function clutch--render-connection-header-line)
           ("clutch-connection" "clutch-query"
            declare-function clutch--update-console-buffer-name)
           ("clutch-connection" "clutch-object"
            declare-function clutch--render-object-describe))))
    (should
     (equal (clutch--architecture-foundation-boundary-violations dependencies)
            '(("clutch-connection" "clutch-query"
               declare-function clutch--update-console-buffer-name)
              ("clutch-connection" "clutch-object"
               declare-function clutch--render-object-describe))))))

(ert-deftest clutch-architecture-entrypoint-is-a-one-way-composition-root ()
  "Implementation modules must not depend back on the package entrypoint."
  (let ((dependencies
         '(("clutch" "clutch-query" require nil)
           ("clutch-query" "clutch" declare-function clutch-mode)
           ("clutch-connection" "clutch" require nil)
           ("clutch-result" "clutch-query" require nil))))
    (should
     (equal (clutch--architecture-composition-root-inbound-violations
             dependencies)
            '(("clutch-query" "clutch" declare-function clutch-mode)
              ("clutch-connection" "clutch" require nil))))))

(ert-deftest clutch-architecture-composition-root-has-a-state-budget ()
  "Mutable package state must keep moving from root to its workflow owner."
  (let ((forms '((require 'clutch-query)
                 (defgroup clutch nil "Package group.")
                 (defcustom option nil "Option." :type 'boolean)
                 (defvar shared-state nil "State.")
                 (defvar-local buffer-state nil "Buffer state.")
                 (define-minor-mode package-mode "Mode.")
                 (progn (defvar nested-state nil "Nested state."))
                 '(defvar quoted-state nil "Quoted data.")
                 (defconst immutable-value 1 "Constant.")
                 (defface package-face '((t :inherit default)) "Face."))))
    (should
     (equal (mapcar #'cadr (clutch--architecture-root-state-forms forms))
            '(option shared-state buffer-state package-mode nested-state)))))

(ert-deftest clutch-architecture-workflow-state-has-one-definition-owner ()
  "Workflow configuration and context state must have one definition owner."
  (let ((definitions
         (cl-loop for file in (directory-files
                               clutch--architecture-repo t
                               "\\`clutch.*\\.el\\'")
                  for module = (file-name-base file)
                  nconc (cl-loop
                         for form in (clutch--architecture-root-state-forms
                                      (clutch--architecture-read-forms file))
                         collect (list module (car form) (cadr form)))))
        (owners
         '(("clutch-backend" defcustom t clutch-connect-timeout-seconds
            clutch-read-idle-timeout-seconds clutch-query-timeout-seconds
            clutch-jdbc-rpc-timeout-seconds)
           ("clutch-connection" defcustom nil clutch-connection-alist
            clutch-tramp-context-policy)
           ("clutch-connection" defvar-local nil clutch-connection
            clutch--conn-sql-product clutch--connection-params)
           ("clutch-query" defcustom t clutch-console-directory
            clutch-console-yank-cleanup)
           ("clutch-query" defvar t clutch--source-window
            clutch--executing-sql-start clutch--executing-sql-end)
           ("clutch-query" defvar-local t clutch--last-query
            clutch--last-result-buffer)
           ("clutch-schema" defcustom t clutch-schema-cache-install-batch-size
            clutch-schema-refresh-idle-delay-seconds)
           ("clutch-sql" defcustom t clutch-sql-completion-case-style)
           ("clutch-edit" defcustom t clutch-insert-validation-idle-delay)
           ("clutch-object" defcustom t clutch-object-warmup-idle-delay-seconds
            clutch-primary-object-types clutch-sql-product)
           ("clutch-result" defcustom t clutch-result-window-height
            clutch-agent-context-max-result-rows
            clutch-agent-context-max-cell-width clutch-column-width-step
            clutch-csv-export-default-coding-system)
           ("clutch-result" defvar-local t clutch--base-query)
           ("clutch-ui" defcustom t clutch-column-width-max
            clutch-column-padding)
           ("clutch-diagnostics" defcustom t clutch-debug-event-limit)
           ("clutch-diagnostics" define-minor-mode nil clutch-debug-mode))))
    (dolist (owner owners)
      (pcase-let ((`(,module ,kind ,exclusive . ,symbols) owner))
        (dolist (symbol symbols)
          (should
           (equal
            (cl-loop for (candidate candidate-kind candidate-symbol)
                     in definitions
                     when (and (eq candidate-symbol symbol)
                               (or exclusive (eq candidate-kind kind)))
                     collect (list candidate candidate-kind))
            (list (list module kind)))))))))

(ert-deftest clutch-architecture-public-autoloads-enter-through-root ()
  "Public workflow autoloads must assemble the package through `clutch'."
  (require 'loaddefs-gen)
  (let ((output (make-temp-file "clutch-loaddefs-" nil ".el"))
        autoloads)
    (unwind-protect
        (progn
          (let ((inhibit-message t))
            (loaddefs-generate
             (list clutch--architecture-repo) output nil nil nil t))
          (with-temp-buffer
            (insert-file-contents output)
            (goto-char (point-min))
            (while (re-search-forward
                    (rx "(autoload '"
                        (group (+ (or alnum "-")))
                        (+ (any " \t\n"))
                        "\"" (group (+ (not "\""))) "\"")
                    nil t)
              (push (cons (intern (match-string 1))
                          (match-string 2))
                    autoloads)))
          (dolist (symbol
                   '(clutch-connect
                     clutch-debug-mode
                     clutch-dispatch
                     clutch-edit-indirect
                     clutch-execute
                     clutch-execute-buffer
                     clutch-execute-dwim
                     clutch-execute-region
                     clutch-indirect-abort
                     clutch-indirect-execute
                     clutch-mode
                     clutch-preview-execution-sql
                     clutch-query-console
                     clutch-query-sqlite-file
                     clutch-repl
                     clutch-repl-mode
                     clutch-repl-send-input
                     clutch-switch-console
                     clutch-switch-database
                     clutch-switch-schema))
            (let ((targets
                   (cl-loop for (candidate . target) in autoloads
                            when (eq candidate symbol)
                            collect target)))
              (should targets)
              (should (equal (delete-dups targets) '("clutch"))))))
      (delete-file output))))

(ert-deftest clutch-architecture-workflow-modules-do-not-load-entrypoint ()
  "Direct workflow loads must not reverse-load the composition root."
  (should-not (featurep 'clutch))
  (require 'clutch-query)
  (require 'clutch-edit)
  (require 'clutch-object)
  (require 'clutch-result)
  (should-not (featurep 'clutch))
  (should (custom-variable-p 'clutch-sql-product))
  (should (eq clutch-sql-product 'mysql))
  (dolist (symbol '(clutch-mode clutch-repl clutch-dispatch
                    clutch-switch-schema))
    (should (fboundp symbol))))

;;; check-architecture-test.el ends here
