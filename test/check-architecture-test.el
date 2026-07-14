;;; check-architecture-test.el --- Tests for architecture reader -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

(require 'ert)
(defvar clutch--architecture-skip-main t)
(require 'check-architecture)

(ert-deftest clutch-architecture-diagnostics-standalone-load ()
  "Loading diagnostics must not load the connection module."
  (should-not (featurep 'clutch-connection))
  (require 'clutch-diagnostics)
  (should-not (featurep 'clutch-connection)))

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

;;; check-architecture-test.el ends here
