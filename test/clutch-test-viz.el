;;; clutch-test-viz.el --- Text chart ERT tests for clutch -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Tests for the pure text charts and the result chart workflow.

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

(defmacro clutch-test-viz--with-chart (spec &rest body)
  "Chart the column at row 0, column 1 of a result made from SPEC, then
run BODY in the chart buffer with RESULT bound to the result buffer."
  (declare (indent 1) (debug (sexp body)))
  `(clutch-test--with-result-state-buffer result (,@spec :render t)
     (let (chart)
       (unwind-protect
           (progn
             (with-current-buffer result
               (clutch--goto-cell 0 1)
               (clutch-result-chart)
               (setq chart (current-buffer)))
             (with-current-buffer chart ,@body))
         (when (buffer-live-p chart) (kill-buffer chart))))))

(ert-deftest clutch-test-result-chart-draws-page-rows ()
  "The chart labels bars by the first text column and counts non-numbers."
  (clutch-test-viz--with-chart
      (:columns '("name" "amount")
       :column-defs '((:name "name" :type-category text)
                      (:name "amount" :type-category numeric))
       :rows '(("alice" 10) ("bob" "2.50") ("carol" nil) ("dave" "1e999")))
    (should (derived-mode-p 'clutch-chart-mode))
    (should (eq (lookup-key clutch-result-mode-map "B") #'clutch-result-chart))
    (let ((lines (split-string (buffer-string) "\n")))
      (should (equal (car lines)
                     "amount by name, rows 1-4, 2 not numbers"))
      (should (string-match-p "\\`alice  █+ +  10\\'" (nth 2 lines)))
      (should (string-match-p "\\`bob    [█▏▎▍▌▋▊▉]+ +2.50\\'" (nth 3 lines)))
      (should (string-match-p "\\`carol  +NULL\\'" (nth 4 lines)))
      (should-not (string-match-p "█" (nth 5 lines))))))

(ert-deftest clutch-test-result-chart-follows-filter-and-goes-to-row ()
  "The chart draws the filtered rows, and RET shows a bar's row."
  (clutch-test-viz--with-chart
      (:columns '("id" "amount")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "amount" :type-category numeric))
       :rows '((1 5) (2 7) (3 9))
       :filter-pattern "x"
       :filtered-rows '((2 7) (3 9)))
    (should (string-prefix-p "amount by row, rows 1-2, filtered"
                             (buffer-string)))
    (goto-char (point-min))
    (forward-line 3)
    (cl-letf (((symbol-function 'pop-to-buffer) #'set-buffer))
      (clutch-chart-goto-row)
      (should (equal (cl-subseq (clutch--cell-at-point) 0 2) '(1 1))))))

(ert-deftest clutch-test-result-chart-refuses-changed-columns ()
  "Redrawing a chart whose result now has other columns asks to chart again."
  (clutch-test-viz--with-chart
      (:columns '("id" "amount") :rows '((1 5)))
    (with-current-buffer (car clutch-chart--source)
      (setq-local clutch--result-columns '("id" "total")))
    (should-error (revert-buffer) :type 'user-error)))

(ert-deftest clutch-test-result-chart-needs-numbers ()
  "Charting a column without numbers on the page is refused."
  (clutch-test--with-result-state-buffer result
      (:columns '("id" "name") :rows '((1 "a") (2 "b")) :render t)
    (with-current-buffer result
      (clutch--goto-cell 0 1)
      (should-error (clutch-result-chart) :type 'user-error))))

(provide 'clutch-test-viz)

;;; clutch-test-viz.el ends here
