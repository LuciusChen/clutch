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

(defun clutch-viz-box-head (&optional tab)
  "Return the lines `clutch-viz-box' draws before its first line.
TAB is as in `clutch-viz-box'."
  (if tab 1 3))

(defun clutch-viz-box (title lines &optional charset width tab)
  "Return LINES framed by a box headed by TITLE.
The result is a list of strings: the top edge, TITLE and a rule, a
string for each of LINES, and the bottom edge.  With TAB, TITLE sits in
the top edge instead, for a lighter box.  LINES start after
`clutch-viz-box-head' strings.  The box is drawn from column 0, from
which the padding's `:align-to' counts, and is at least WIDTH columns
wide inside.  It uses ASCII when CHARSET is `ascii', else rounded box
characters; CHARSET defaults to `clutch-viz-charset'."
  (pcase-let* ((`(,top-left ,top-right ,bottom-left ,bottom-right ,across ,down
                            ,rule-left ,rule-right)
                (if (eq (or charset (clutch-viz-charset '(?╭))) 'ascii)
                    '("+" "+" "+" "+" ?- "|" "+" "+")
                  '("╭" "╮" "╰" "╯" ?─ "│" "├" "┤")))
               ;; A title in the top edge needs room for a rule on either
               ;; side of it.
               (inner (apply #'max (or width 0)
                             (+ (string-width title) (if tab 2 0))
                             (mapcar #'string-width lines))))
    (cl-flet ((edge (&rest parts)
                (propertize (apply #'concat parts) 'face 'shadow))
              (row (line)
                (concat (propertize (concat down " ") 'face 'shadow)
                        line
                        (clutch-viz--pad line (1+ inner) (+ inner 3))
                        (propertize down 'face 'shadow))))
      (append
       (if tab
           (list (concat (edge top-left (string across) " ")
                         title
                         (edge " " (make-string (- inner (string-width title) 1)
                                                across)
                               top-right)))
         (list (edge top-left (make-string (+ inner 2) across) top-right)
               (row title)
               (edge rule-left (make-string (+ inner 2) across) rule-right)))
       (mapcar #'row lines)
       (list (edge bottom-left (make-string (+ inner 2) across)
                   bottom-right))))))

(defconst clutch-viz--junctions
  ;; Indexed by the directions a cell joins: up 1, down 2, left 4, right 8.
  [" " "│" "│" "│" "─" "╯" "╮" "┤" "─" "╰" "╭" "├" "─" "┴" "┬" "┼"]
  "Box characters joining the directions of a connector cell.")

(defun clutch-viz--junction (mask solid charset)
  "Return the character of a connector cell joining the directions of MASK.
A straight cell is dashed unless SOLID; ASCII when CHARSET is `ascii'."
  (cond ((eq charset 'ascii)
         (pcase mask
           (0 " ")
           ((or 1 2 3) (if solid "|" ":"))
           ((or 4 8 12) (if solid "-" "."))
           (_ "+")))
        ((and (not solid) (memq mask '(1 2 3))) "┆")
        ((and (not solid) (memq mask '(4 8 12))) "┄")
        (t (aref clutch-viz--junctions mask))))

(defun clutch-viz--route (height edges lanes)
  "Route EDGES across a gutter HEIGHT lines tall, edge I down lane I of LANES.
A lane of 0 means a straight edge, which needs none.  Return a plist of
the gutter's :width, the cells' direction :masks and whether each is
:solid, the lines with :arrows, each edge's :cells and the number of
:crossings, cells where one edge runs across another."
  (let* ((width (+ (* 2 (apply #'max 0 lanes)) 5))
         (masks (make-vector (* height width) 0))
         (solid (make-vector (* height width) nil))
         (arrows (make-vector height nil))
         cells)
    (cl-loop
     for (from to dashed) in edges
     for lane in lanes
     do (let (own)
          (cl-flet ((mark (line column bits)
                      (let ((cell (+ (* line width) column)))
                        (aset masks cell (logior (aref masks cell) bits))
                        (unless dashed (aset solid cell t))
                        (push (cons line column) own))))
            ;; Lanes start after two columns, so a connector leaves its
            ;; box before it turns, and stand a column apart, so that
            ;; neighbouring connectors do not read as one.
            (if (zerop lane)
                (dotimes (column (1- width))
                  (mark from column 12))
              (let ((column (* 2 lane)))
                (dotimes (left column)
                  (mark from left 12))
                (mark from column (if (> to from) 6 5))
                (cl-loop for line from (1+ (min from to)) below (max from to)
                         do (mark line column 3))
                (mark to column (if (> to from) 9 10))
                (cl-loop for right from (1+ column) below (1- width)
                         do (mark to right 12))))
            (aset arrows to t)
            (push (cons to (1- width)) own))
          (push (nreverse own) cells)))
    (list :width width :masks masks :solid solid :arrows arrows
          :cells (nreverse cells)
          :crossings (cl-count 15 masks))))

(defun clutch-viz-connectors (height edges &optional charset)
  "Return the connectors of EDGES across a gutter HEIGHT lines tall.
Each edge is (FROM TO DASHED): it leaves the gutter's left side at line
FROM and ends at its right side at line TO in an arrow, drawn dashed when
DASHED.  An edge between lines of the same number runs straight; each
other edge turns down a lane of its own, in the order of lanes that
crosses fewest edges.  Where edges cross or meet, the characters join.
The result is (LINES . CELLS): LINES are the gutter's lines, all as
wide, and CELLS lists, for each edge in order, its cells as (LINE .
COLUMN).  CHARSET is as in `clutch-viz-tree'."
  (let* ((charset (or charset (clutch-viz-charset '(?▶ ?╭))))
         (bent (cl-loop for (from to) in edges
                        for index from 0
                        unless (= from to) collect (list index from (< to from))))
         (by-from (sort (copy-sequence bent) (lambda (a b) (< (nth 1 a) (nth 1 b)))))
         (up (cl-remove-if-not #'cl-third by-from))
         (down (reverse (cl-remove-if #'cl-third by-from)))
         (route
          (car (sort
                (mapcar (lambda (order)
                          (clutch-viz--route
                           height edges
                           (cl-loop for index below (length edges)
                                    collect (let ((lane (cl-position index order
                                                                     :key #'car)))
                                              (if lane (1+ lane) 0)))))
                        (list (append up down) (append down up)
                              by-from (reverse by-from)))
                (lambda (a b) (< (plist-get a :crossings)
                                 (plist-get b :crossings))))))
         (width (plist-get route :width)))
    (cons (cl-loop for line below height
                   collect
                   (concat
                    (cl-loop for column below (1- width)
                             for cell = (+ (* line width) column)
                             concat (clutch-viz--junction
                                     (aref (plist-get route :masks) cell)
                                     (aref (plist-get route :solid) cell)
                                     charset))
                    (if (aref (plist-get route :arrows) line)
                        (if (eq charset 'ascii) ">" "▶")
                      " ")))
          (plist-get route :cells))))

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
