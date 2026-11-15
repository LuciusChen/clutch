;;; clutch-viz.el --- Text charts for Clutch -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Pure functions that draw data as text.  Nothing here reads Clutch state,
;; so the charts can be tested, and reused, on plain Lisp data.

;;; Code:

(require 'cl-lib)

(defface clutch-viz-bar
  '((t :inherit font-lock-constant-face))
  "Face for the bars of a text chart."
  :group 'clutch)

(defconst clutch-viz--eighths [nil ?▏ ?▎ ?▍ ?▌ ?▋ ?▊ ?▉]
  "Block characters filling one to seven eighths of a cell.")

(defun clutch-viz-charset (&optional chars)
  "Return `unicode' when block and box characters fill one column.
Otherwise return `ascii'.  CJK language environments can make them two
columns wide, which would break the alignment of every chart.  CHARS
lists other characters a caller draws with, which must fill one column
too."
  (if (cl-every (lambda (char) (= (char-width char) 1))
                (append '(?█ ?▏ ?├ ?─) chars))
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

(defun clutch-viz-bar-chart (rows width &optional charset label-share header)
  "Return a horizontal bar chart of ROWS fitted to WIDTH columns.
Each row is (LABEL VALUE TEXT): LABEL names the bar, VALUE is a number or
nil, and TEXT is shown after the bar.  Bars scale to the largest VALUE.
Labels wider than LABEL-SHARE of WIDTH, a third by default, are cut with
an ellipsis; a row with neither bar nor TEXT is its label alone.  Each
line carries its row's position in ROWS as the `clutch-viz-index'
property.  HEADER, a list (LABEL TITLE TEXT), heads the label, bar and
text columns, with rules across WIDTH above the rows and below them.
CHARSET defaults to `clutch-viz-charset'."
  (let* ((charset (or charset (clutch-viz-charset)))
         (labels (mapcar #'car (if header (cons header rows) rows)))
         (texts (mapcar (lambda (row) (nth 2 row))
                        (if header (cons header rows) rows)))
         (label-width (min (max 1 (floor (* width (or label-share (/ 1.0 3)))))
                           (apply #'max 1 (mapcar #'string-width labels))))
         (text-width (apply #'max 0 (mapcar #'string-width texts)))
         (bar-width (max 4 (- width label-width text-width 4)))
         (top (apply #'max 0 (mapcar (lambda (row)
                                       (if (numberp (nth 1 row)) (nth 1 row) 0))
                                     rows)))
         (rule (propertize (make-string width (if (eq charset 'ascii) ?- ?─))
                           'face 'shadow))
         (index -1))
    (cl-flet ((line (label bar text)
                (let ((label (truncate-string-to-width label label-width
                                                       nil nil t)))
                  (if (and (string-empty-p bar) (string-empty-p text))
                      label
                    (concat label
                            (clutch-viz--pad label (+ label-width 2)
                                             (+ label-width 2))
                            bar
                            (clutch-viz--pad bar (+ bar-width 2)
                                             (+ label-width bar-width 4))
                            (make-string (- text-width (string-width text)) ?\s)
                            text)))))
      (concat
       (when header
         (pcase-let ((`(,label ,title ,text) header))
           (concat (propertize (line label (truncate-string-to-width
                                            title bar-width)
                                     text)
                               'face 'bold)
                   "\n" rule "\n")))
       (mapconcat
        (pcase-lambda (`(,label ,value ,text))
          (propertize (line label
                            (propertize (clutch-viz-bar value top bar-width
                                                        charset)
                                        'face 'clutch-viz-bar)
                            text)
                      'clutch-viz-index (setq index (1+ index))))
        rows "\n")
       (when header
         (concat "\n" rule))))))

(defun clutch-viz-columns (rows &optional indent gap)
  "Return ROWS aligned in columns, one string for each row.
A row is a list of cells, which are strings.  Each column is as wide as
its widest cell, and GAP spaces, 2 by default, separate columns; the
last cell of a row is not padded.  Rows start at column INDENT, 0 by
default, from which the padding's `:align-to' counts.  A cell's own
`:align-to' padding counted from column 0, so a cell placed further
right loses its `display' properties and keeps its spaces."
  (let* ((widths (cl-loop for index below (apply #'max 0 (mapcar #'length rows))
                          collect (apply #'max 0
                                         (mapcar (lambda (row)
                                                   (string-width
                                                    (or (nth index row) "")))
                                                 rows)))))
    (mapcar (lambda (row)
              (let ((column (or indent 0))
                    (widths widths)
                    parts)
                (while row
                  (let ((cell (pop row))
                        (width (pop widths)))
                    (push (if (zerop column)
                              cell
                            (let ((cell (copy-sequence cell)))
                              (remove-text-properties 0 (length cell)
                                                      '(display nil) cell)
                              cell))
                          parts)
                    (when row
                      (setq column (+ column width (or gap 2)))
                      (when (< (string-width cell) (+ width (or gap 2)))
                        (push (clutch-viz--pad cell (+ width (or gap 2)) column)
                              parts)))))
                (apply #'concat (nreverse parts))))
            rows)))

(defun clutch-viz-box (title lines &optional charset width)
  "Return LINES framed by a box with TITLE in its top edge.
The result is a list of strings: the top edge, a string for each of
LINES, and the bottom edge.  The box is drawn from column 0, from which
the padding's `:align-to' counts, and is at least WIDTH columns wide
inside.  It uses box characters, or ASCII when CHARSET is `ascii';
CHARSET defaults to `clutch-viz-charset'."
  (pcase-let* ((`(,top-left ,top-right ,bottom-left ,bottom-right ,across ,down)
                (if (eq (or charset (clutch-viz-charset)) 'ascii)
                    '("+" "+" "+" "+" ?- "|")
                  '("┌" "┐" "└" "┘" ?─ "│")))
               (inner (apply #'max (or width 0) (string-width title)
                             (mapcar #'string-width lines))))
    (cl-flet ((edge (&rest parts)
                (propertize (apply #'concat parts) 'face 'shadow)))
      (append
       (list (concat (edge top-left " ")
                     title
                     (edge " " (make-string (- inner (string-width title))
                                            across)
                           top-right)))
       (mapcar (lambda (line)
                 (concat (edge down " ")
                         line
                         (clutch-viz--pad line (1+ inner) (+ inner 3))
                         (edge down)))
               lines)
       (list (edge bottom-left (make-string (+ inner 2) across)
                   bottom-right))))))

(defconst clutch-viz--junctions
  ;; Indexed by the directions a cell joins: up 1, down 2, left 4, right 8.
  [" " "│" "│" "│" "─" "┘" "┐" "┤" "─" "└" "┌" "├" "─" "┴" "┬" "┼"]
  "Box characters joining the directions of a connector cell.")

(defun clutch-viz--junction (mask solid charset)
  "Return the character of a connector cell joining the directions of MASK.
A straight cell is dashed unless SOLID; ASCII when CHARSET is `ascii'."
  (cond ((eq charset 'ascii)
         (pcase mask (0 " ") ((or 1 2 3) "|") ((or 4 8 12) "-") (_ "+")))
        ((and (not solid) (memq mask '(1 2 3))) "┆")
        ((and (not solid) (memq mask '(4 8 12))) "┄")
        (t (aref clutch-viz--junctions mask))))

(defun clutch-viz-connectors (height edges &optional charset)
  "Return the connectors of EDGES across a gutter HEIGHT lines tall.
Each edge is (FROM TO DASHED): it leaves the gutter's left side at line
FROM and ends at its right side at line TO in an arrow, drawn dashed when
DASHED.  Each edge runs down a lane of its own, the first edge nearest
the left side; where edges cross or meet, the characters join.  The
result is (LINES . CELLS): LINES are the gutter's lines, all as wide,
and CELLS lists, for each edge in order, its cells as (LINE . COLUMN).
CHARSET is as in `clutch-viz-tree'."
  (let* ((charset (or charset (clutch-viz-charset '(?▶))))
         (width (+ (length edges) 3))
         (masks (make-vector (* height width) 0))
         (solid (make-vector (* height width) nil))
         (arrows (make-vector height nil))
         (lane 0)
         cells)
    (pcase-dolist (`(,from ,to ,dashed) edges)
      (let (own)
        (cl-flet ((mark (line column bits)
                    (let ((cell (+ (* line width) column)))
                      (aset masks cell (logior (aref masks cell) bits))
                      (unless dashed (aset solid cell t))
                      (push (cons line column) own))))
          (cl-incf lane)
          (dotimes (column lane)
            (mark from column 12))
          (cond ((= from to) (mark from lane 12))
                (t (mark from lane (if (> to from) 6 5))
                   (cl-loop for line from (1+ (min from to)) below (max from to)
                            do (mark line lane 3))
                   (mark to lane (if (> to from) 9 10))))
          (cl-loop for column from (1+ lane) below (1- width)
                   do (mark to column 12))
          (aset arrows to t)
          (push (cons to (1- width)) own))
        (push (nreverse own) cells)))
    (cons (cl-loop for line below height
                   collect
                   (concat
                    (cl-loop for column below (1- width)
                             for cell = (+ (* line width) column)
                             concat (clutch-viz--junction (aref masks cell)
                                                          (aref solid cell)
                                                          charset))
                    (if (aref arrows line)
                        (if (eq charset 'ascii) ">" "▶")
                      " ")))
          (nreverse cells))))

(defun clutch-viz-tree (roots children label &optional charset)
  "Return the lines of a tree of ROOTS, depth first, as (TEXT . NODE).
CHILDREN and LABEL are functions of a node that return its child nodes and
its one-line label.  TEXT draws the branches with box characters, or with
ASCII when CHARSET is `ascii'; CHARSET defaults to `clutch-viz-charset'."
  (pcase-let* ((`(,tee ,elbow ,pipe)
                (if (eq (or charset (clutch-viz-charset)) 'ascii)
                    '("|-- " "`-- " "|   ")
                  '("├── " "└── " "│   ")))
               (lines nil))
    (cl-labels ((walk (nodes prefix)
                  (while nodes
                    (let ((node (pop nodes)))
                      (push (cons (concat prefix (if nodes tee elbow)
                                          (funcall label node))
                                  node)
                            lines)
                      (walk (funcall children node)
                            (concat prefix (if nodes pipe "    ")))))))
      (dolist (root roots)
        (push (cons (funcall label root) root) lines)
        (walk (funcall children root) "")))
    (nreverse lines)))

(provide 'clutch-viz)

;;; clutch-viz.el ends here
