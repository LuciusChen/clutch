;;; clutch-viz.el --- Text charts for Clutch -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Pure functions that draw data as text.  Nothing here reads Clutch state,
;; so the charts can be tested, and reused, on plain Lisp data.

;;; Code:

(defface clutch-viz-bar
  '((t :inherit font-lock-constant-face))
  "Face for the bars of a text chart."
  :group 'clutch)

(defconst clutch-viz--eighths [nil ?▏ ?▎ ?▍ ?▌ ?▋ ?▊ ?▉]
  "Block characters filling one to seven eighths of a cell.")

(defun clutch-viz-charset ()
  "Return `unicode' when block characters fill one column, else `ascii'.
CJK language environments can make them two columns wide, which would
break the alignment of every chart."
  (if (and (= (char-width ?█) 1) (= (char-width ?▏) 1))
      'unicode
    'ascii))

(defun clutch-viz-bar (value top width &optional charset)
  "Return a bar for VALUE on a scale from zero to TOP, at most WIDTH columns.
A positive VALUE always draws at least a sliver, so it does not read as
zero; zero, negative and non-numeric VALUEs draw nothing.  CHARSET
`ascii' draws whole cells of #, otherwise eighths of a cell."
  (if (not (and (numberp value) (> value 0) (numberp top) (> top 0)
                (> width 0)))
      ""
    (let ((units (* width (min 1.0 (/ (float value) top)))))
      (if (eq charset 'ascii)
          (make-string (max 1 (round units)) ?#)
        (let ((eighths (max 1 (round (* units 8)))))
          (concat (make-string (/ eighths 8) ?█)
                  (if-let* ((part (aref clutch-viz--eighths (% eighths 8))))
                      (string part)
                    "")))))))

(defun clutch-viz--pad (string width column)
  "Return spaces after STRING up to WIDTH columns, displayed to reach COLUMN.
The spaces keep copied text aligned; the display property keeps graphical
frames aligned when proportional or wide glyphs differ from the grid."
  (propertize (make-string (max 1 (- width (string-width string))) ?\s)
              'display `(space :align-to ,column)))

(defun clutch-viz-bar-chart (rows width &optional charset)
  "Return a horizontal bar chart of ROWS fitted to WIDTH columns.
Each row is (LABEL VALUE TEXT): LABEL names the bar, VALUE is a number or
nil, and TEXT is shown after the bar.  Bars scale to the largest VALUE.
Labels wider than a third of WIDTH are cut with an ellipsis.  Each line
carries its row's position in ROWS as the `clutch-viz-index' property.
CHARSET defaults to `clutch-viz-charset'."
  (let* ((charset (or charset (clutch-viz-charset)))
         (label-width (min (max 1 (/ width 3))
                           (apply #'max 1 (mapcar (lambda (row)
                                                    (string-width (nth 0 row)))
                                                  rows))))
         (text-width (apply #'max 0 (mapcar (lambda (row)
                                              (string-width (nth 2 row)))
                                            rows)))
         (bar-width (max 4 (- width label-width text-width 4)))
         (top (apply #'max 0 (mapcar (lambda (row)
                                       (if (numberp (nth 1 row)) (nth 1 row) 0))
                                     rows)))
         (index -1))
    (mapconcat
     (pcase-lambda (`(,label ,value ,text))
       (let* ((label (truncate-string-to-width label label-width nil nil t))
              (bar (clutch-viz-bar value top bar-width charset)))
         (propertize
          (concat label
                  (clutch-viz--pad label (+ label-width 2) (+ label-width 2))
                  (propertize bar 'face 'clutch-viz-bar)
                  (clutch-viz--pad bar (+ bar-width 2)
                                   (+ label-width bar-width 4))
                  (make-string (- text-width (string-width text)) ?\s)
                  text)
          'clutch-viz-index (setq index (1+ index)))))
     rows "\n")))

(provide 'clutch-viz)

;;; clutch-viz.el ends here
