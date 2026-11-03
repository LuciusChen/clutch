;;; clutch-test-viz.el --- Text chart ERT tests for clutch -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Tests for the pure text charts and the statement plan workflow.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common))

(require 'clutch-viz)

(defun clutch-test-viz--lines (chart)
  "Return the lines of CHART without text properties."
  (split-string (substring-no-properties chart) "\n"))

(ert-deftest clutch-test-viz-bar-draws-eighths ()
  "Bars fill eighths of a cell, never draw a positive value as empty,
and stop at the top of the scale."
  (should (equal (clutch-viz-bar 10 10 4 'unicode) "████"))
  (should (equal (clutch-viz-bar 5 10 4 'unicode) "██"))
  (should (equal (clutch-viz-bar 3 16 2 'unicode) "▍"))
  (should (equal (clutch-viz-bar 0.001 10 4 'unicode) "▏"))
  (should (equal (clutch-viz-bar 99 10 4 'unicode) "████"))
  (should (equal (clutch-viz-bar 63 64 1 'unicode) "█"))
  (dolist (value '(0 -3 nil "12"))
    (should (equal (clutch-viz-bar value 10 4 'unicode) "")))
  (should (equal (clutch-viz-bar 5 0 4 'unicode) "")))

(ert-deftest clutch-test-viz-bar-ascii-uses-whole-cells ()
  "The ASCII charset draws whole cells and still shows small values."
  (should (equal (clutch-viz-bar 5 10 4 'ascii) "##"))
  (should (equal (clutch-viz-bar 0.001 10 4 'ascii) "#"))
  (should (equal (clutch-viz-bar 10 10 4 'ascii) "####")))

(ert-deftest clutch-test-viz-charset-falls-back-for-wide-blocks ()
  "Block characters two columns wide switch charts to ASCII."
  (should (eq (clutch-viz-charset) 'unicode))
  (cl-letf (((symbol-function 'char-width) (lambda (_char) 2)))
    (should (eq (clutch-viz-charset) 'ascii))))

(ert-deftest clutch-test-viz-bar-chart-aligns-columns ()
  "Labels, bars and values line up by display width, including CJK labels."
  (let* ((chart (clutch-viz-bar-chart '(("北京" 40 "40")
                                        ("Oslo" 10 "10")
                                        ("none" nil "NULL"))
                                      30 'unicode))
         (lines (clutch-test-viz--lines chart)))
    (should (= (length lines) 3))
    (dolist (line lines)
      (should (= (string-width line) 30)))
    (should (string-match-p "\\`北京  █+ +  40\\'" (nth 0 lines)))
    (should (string-match-p "\\`none  +NULL\\'" (nth 2 lines)))
    (should (> (cl-count ?█ (nth 0 lines)) (cl-count ?█ (nth 1 lines))))))

(ert-deftest clutch-test-viz-bar-chart-properties ()
  "Each line records its row, bars carry their face, and padding aligns
graphical frames to the same columns."
  (let ((chart (clutch-viz-bar-chart '(("a" 1 "1") ("b" 2 "2")) 20 'unicode)))
    (should (eq (get-text-property 0 'clutch-viz-index chart) 0))
    (should (eq (get-text-property (1+ (string-search "\n" chart))
                                   'clutch-viz-index chart)
                1))
    (should (eq (get-text-property 3 'face chart) 'clutch-viz-bar))
    (should (equal (get-text-property 1 'display chart)
                   '(space :align-to 3)))))

(ert-deftest clutch-test-viz-bar-chart-cuts-long-labels ()
  "A label wider than a third of the chart is cut with an ellipsis."
  (let* ((label (make-string 40 ?x))
         (line (car (clutch-test-viz--lines
                     (clutch-viz-bar-chart `((,label 1 "1")) 30 'unicode)))))
    (should (= (string-width line) 30))
    (should (string-prefix-p (truncate-string-to-width label 10 nil nil t)
                             line))))

(ert-deftest clutch-test-viz-bar-chart-header-and-rules ()
  "A header lines up with the columns below it, between rules as wide as
the chart, and leaves the rows' indexes alone."
  (let* ((chart (clutch-viz-bar-chart '(("a" 1 "10") ("bb" 2 "200")) 30 'unicode
                                      nil '("Step" "Bar" "Rows")))
         (lines (clutch-test-viz--lines chart)))
    (should (= (length lines) 5))
    (should (string-match-p "\\`Step  Bar +  Rows\\'" (nth 0 lines)))
    (should (equal (nth 1 lines) (make-string 30 ?─)))
    (should (equal (nth 4 lines) (nth 1 lines)))
    (should (string-match-p "\\`bb    █+ +200\\'" (nth 3 lines)))
    (dolist (line lines)
      (should (= (string-width line) 30)))
    (should (eq (get-text-property (string-search "bb" chart)
                                   'clutch-viz-index chart)
                1))
    (should (string-prefix-p
             "----"
             (nth 1 (clutch-test-viz--lines
                     (clutch-viz-bar-chart '(("a" 1 "1")) 10 'ascii
                                           nil '("Step" "" ""))))))))

(ert-deftest clutch-test-viz-tree-draws-branches ()
  "A tree draws its branches depth first, in box characters or ASCII."
  (let ((roots '(("Hash Join" ("Seq Scan on a") ("Hash" ("Seq Scan on b")))
                 ("Sort"))))
    (should (equal (mapcar #'car (clutch-viz-tree roots #'cdr #'car 'unicode))
                   '("Hash Join" "├── Seq Scan on a" "└── Hash"
                     "    └── Seq Scan on b" "Sort")))
    (should (equal (mapcar #'car (clutch-viz-tree roots #'cdr #'car 'ascii))
                   '("Hash Join" "|-- Seq Scan on a" "`-- Hash"
                     "    `-- Seq Scan on b" "Sort")))
    (should (equal (cdr (nth 3 (clutch-viz-tree roots #'cdr #'car)))
                   '("Seq Scan on b")))))

(ert-deftest clutch-test-plan-shows-each-steps-own-share ()
  "A step's bar and share are its total less its children's."
  (with-temp-buffer
    (clutch-plan-mode)
    (setq clutch-plan--plan
          (cons "SELECT *\n  FROM a"
                '(:unit ms
                  :roots ((:label "Hash Join" :total 10.0 :rows 5
                           :children ((:label "Seq Scan on a" :total 6.0
                                       :rows 100 :children nil)))))))
    (clutch-plan--render)
    (let ((lines (split-string (buffer-string) "\n")))
      (should (equal (nth 0 lines)
                     "Measured by running the statement; each step's own time"))
      (should (equal (nth 1 lines) "SELECT * FROM a"))
      (should (string-match-p
               "\\`Step  +Own time +Rows     Time  Share\\'" (nth 3 lines)))
      (should (string-match-p "\\`Hash Join  .* +5  4.00 ms    40%\\'"
                              (nth 5 lines)))
      (should (string-match-p
               "\\`└── Seq Scan on a  █+ +100  6.00 ms    60%\\'"
               (nth 6 lines)))
      (should (equal (nth 4 lines) (nth 7 lines))))))

(ert-deftest clutch-test-explain-plan-sqlite-tree ()
  "The plan of a SQLite statement shows its steps from the console, and a
measured plan, which SQLite has not, is refused before anything runs."
  (clutch-test--with-sqlite-connection conn
    (clutch-db-query conn "CREATE TABLE a (id INTEGER PRIMARY KEY, n INT)")
    (clutch-db-query conn "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INT)")
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch-connection conn
                  clutch--connection-params '(:backend sqlite
                                              :database ":memory:"))
      (insert "SELECT * FROM a WHERE n IN (SELECT a_id FROM b) ORDER BY n")
      (should-error (clutch-explain-plan (point) (point) t) :type 'user-error)
      (clutch-test--with-pop-to-buffer-capture plan
        (clutch-explain-plan (point) (point))
        (clutch-test--await (lambda () plan))
        (with-current-buffer plan
          (should (derived-mode-p 'clutch-plan-mode))
          (should (equal (butlast
                          (nthcdr 5 (split-string (buffer-string) "\n"))
                          2)
                         '("SCAN a"
                           "LIST SUBQUERY 1"
                           "├── SCAN b"
                           "└── CREATE BLOOM FILTER"
                           "USE TEMP B-TREE FOR ORDER BY")))
          (should (equal (nth 3 (split-string (buffer-string) "\n"))
                         "Step")))))))

(ert-deftest clutch-test-explain-plan-measures-only-selects ()
  "Measuring a plan runs the statement, so it is refused for a write."
  (with-temp-buffer
    (clutch-mode)
    (insert "DELETE FROM a")
    (should-error (clutch-explain-plan (point) (point) t) :type 'user-error)
    (erase-buffer)
    (insert "SELECT 1; SELECT 2;")
    (goto-char (point-min))
    (should-error (clutch-explain-plan (point-min) (point-max))
                  :type 'user-error)))

(provide 'clutch-test-viz)

;;; clutch-test-viz.el ends here
