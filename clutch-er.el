;;; clutch-er.el --- Entity-relationship view of a table -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `clutch-er-diagram' shows a table with its key columns, the tables its
;; foreign keys reference and the tables whose foreign keys reference it:
;; one hop of relations around the table.  The keys come from the
;; database; a key over several columns is one relation.  Moving point
;; only moves the highlight of the relation at point; the buffer is drawn
;; again only when its table, its columns or its relations change.

;;; Code:

(require 'cl-lib)
(require 'clutch-object)
(require 'clutch-viz)

(defface clutch-er-highlight
  '((t :inherit highlight))
  "Face for the relation at point and the columns it joins."
  :group 'clutch)

(defface clutch-er-table
  '((t :inherit font-lock-function-name-face :weight bold))
  "Face for table names in the ER view."
  :group 'clutch)

(defface clutch-er-key
  '((t :inherit font-lock-constant-face))
  "Face for the primary-key and foreign-key markers in the ER view."
  :group 'clutch)

(defvar-local clutch-er--entry nil
  "Object entry of the table at the center of the ER view.")

(defvar-local clutch-er--model nil
  "What the ER view shows: a plist of :columns, :relations and :schema.
:schema is the namespace the table's names are shown relative to.")

(defvar-local clutch-er--history nil
  "Entries of the tables shown before the current one, latest first.")

(defvar-local clutch-er--all-columns nil
  "Non-nil when the ER view shows every column, not only key columns.")

(defvar-local clutch-er--lines nil
  "The highlightable lines of the ER view, as (BEG END IDS).
IDS lists the relations a line belongs to.")

(defvar-local clutch-er--highlighted nil
  "Relation ids highlighted now, or `none' before the first highlight.")

(defvar-local clutch-er--overlays nil
  "Overlays of the current highlight.")

(defvar-local clutch-er--header-base nil
  "Header line of the ER view, before the disconnected badge.")

;;;; Relations

(defun clutch-er--runs (items key)
  "Split ITEMS into lists of consecutive items with `equal' KEY values."
  (let (runs)
    (dolist (item items)
      (if (and runs (equal (funcall key item) (funcall key (caar runs))))
          (push item (car runs))
        (push (list item) runs)))
    (nreverse (mapcar #'reverse runs))))

(defun clutch-er--relations (outgoing incoming)
  "Return the relations of a table from its keys, numbered from 0.
OUTGOING is the table's `clutch-db-foreign-keys', INCOMING its
`clutch-db-referencing-keys'.  Each relation is a plist: :id, :direction
`out' or `in', the other end's :table and :schema, and :pairs, a list of
\(COLUMN . OTHER-COLUMN), COLUMN being the table's own."
  (let ((id -1))
    (append
     (mapcar (lambda (run)
               (let ((ref (cdar run)))
                 (list :id (cl-incf id) :direction 'out
                       :table (plist-get ref :ref-table)
                       :schema (plist-get ref :ref-schema)
                       :pairs (mapcar (lambda (fk)
                                        (cons (car fk)
                                              (plist-get (cdr fk) :ref-column)))
                                      run))))
             (clutch-er--runs outgoing
                              (lambda (fk)
                                (let ((ref (cdr fk)))
                                  (list (plist-get ref :constraint)
                                        (plist-get ref :ref-schema)
                                        (plist-get ref :ref-table))))))
     (mapcar (lambda (run)
               (list :id (cl-incf id) :direction 'in
                     :table (plist-get (car run) :table)
                     :schema (plist-get (car run) :schema)
                     :pairs (mapcar (lambda (key)
                                      (cons (plist-get key :ref-column)
                                            (plist-get key :column)))
                                    run)))
             (clutch-er--runs incoming
                              (lambda (key)
                                (list (plist-get key :schema)
                                      (plist-get key :table)
                                      (plist-get key :constraint))))))))

(defun clutch-er--load (conn entry)
  "Return the ER model of the table of ENTRY on CONN.
Its columns and foreign keys come from the metadata cache, loaded now
when missing; the keys that reference it are asked for each time."
  (unless (clutch--connection-alive-p conn)
    (user-error "Connection closed.  Reconnect from the SQL buffer or REPL"))
  (clutch--refuse-while-running conn)
  (clutch--await-background-request conn)
  (let* ((table (plist-get entry :name))
         (schema (plist-get entry :schema))
         (key (clutch--table-key table schema))
         ;; Asked first, so a connection that cannot answer refuses
         ;; before anything else is loaded.
         (incoming (apply #'clutch-db-referencing-keys conn table
                          (and schema (list schema))))
         (columns (clutch--ensure-column-details conn key t))
         (outgoing (clutch--ensure-foreign-keys conn key)))
    (list :columns columns
          :relations (clutch-er--relations outgoing incoming)
          :schema (or schema (clutch-db-current-schema conn)))))

;;;; Drawing

(defun clutch-er--charset ()
  "Return the characters the ER view draws with, as `clutch-viz-charset'."
  (clutch-viz-charset '(?◆ ?→ ?← ?↔ ?…)))

(defun clutch-er--name (table schema shown-schema)
  "Return TABLE, qualified by SCHEMA unless it is SHOWN-SCHEMA."
  (propertize (if (and schema (not (equal schema shown-schema)))
                  (concat schema "." table)
                table)
              'face 'clutch-er-table))

(defun clutch-er--column-list (columns &optional bare)
  "Return COLUMNS, names, in parentheses.
With BARE, a single name stands without them."
  (format (if (and bare (null (cdr columns))) "%s" "(%s)")
          (string-join columns ", ")))

(defun clutch-er--relation-ids (column relations)
  "Return the ids of RELATIONS that join COLUMN of the table."
  (cl-loop for relation in relations
           when (assoc column (plist-get relation :pairs))
           collect (plist-get relation :id)))

(defun clutch-er--marker (column relations unicode)
  "Return the marker of COLUMN among RELATIONS: its key and directions.
Use box characters when UNICODE, else ASCII."
  (let* ((directions (cl-loop for relation in relations
                              when (assoc column (plist-get relation :pairs))
                              collect (plist-get relation :direction)))
         (out (memq 'out directions))
         (in (memq 'in directions)))
    (cond ((and out in) (if unicode "↔" "="))
          (out (if unicode "→" ">"))
          (in (if unicode "←" "<"))
          (t " "))))

(defun clutch-er--relation-row (relation shown-schema arrow)
  "Return the cells of RELATION's line, with ARROW between its ends.
Names in SHOWN-SCHEMA are not qualified."
  (let* ((pairs (plist-get relation :pairs))
         (own (clutch-er--column-list (mapcar #'car pairs) t))
         (other (concat (clutch-er--name (plist-get relation :table)
                                         (plist-get relation :schema)
                                         shown-schema)
                        " "
                        (clutch-er--column-list
                         (mapcar #'cdr pairs)))))
    (if (eq (plist-get relation :direction) 'out)
        (list own arrow other)
      (list other arrow own))))

(defun clutch-er--render ()
  "Draw the current ER model in the current buffer."
  (let* ((inhibit-read-only t)
         (unicode (eq (clutch-er--charset) 'unicode))
         (charset (if unicode 'unicode 'ascii))
         (arrow (propertize (if unicode "→" "->") 'face 'clutch-er-key))
         (columns (plist-get clutch-er--model :columns))
         (relations (plist-get clutch-er--model :relations))
         (shown-schema (plist-get clutch-er--model :schema))
         (key-columns
          (cl-remove-if-not
           (lambda (column)
             (or clutch-er--all-columns
                 (plist-get column :primary-key)
                 (clutch-er--relation-ids (plist-get column :name) relations)))
           columns))
         (hidden (- (length columns) (length key-columns)))
         (line (line-number-at-pos))
         lines)
    (cl-flet ((emit (text &optional ids)
                (let ((beg (point)))
                  (insert text "\n")
                  (push (list beg (1- (point)) ids) lines)))
              (marker (column)
                (propertize
                 (concat (if (plist-get column :primary-key)
                             (if unicode "◆" "*")
                           " ")
                         (clutch-er--marker (plist-get column :name)
                                            relations unicode))
                 'face 'clutch-er-key)))
      (erase-buffer)
      (let* ((rows (clutch-viz-columns
                    (mapcar (lambda (column)
                              (list (marker column)
                                    (plist-get column :name)
                                    (propertize (or (plist-get column :type) "")
                                                'face 'shadow)))
                            key-columns)
                    2))
             (more (when (> hidden 0)
                     (propertize (format "%s %d more column%s"
                                         (if unicode "…" "...")
                                         hidden (if (= hidden 1) "" "s"))
                                 'face 'shadow)))
             (box (clutch-viz-box
                   (clutch-er--name (plist-get clutch-er--entry :name)
                                    (plist-get clutch-er--entry :schema)
                                    nil)
                   (append rows (and more (list more)))
                   charset)))
        (emit (car box))
        (cl-loop for text in (cdr box)
                 for column in key-columns
                 do (emit text (clutch-er--relation-ids
                                (plist-get column :name) relations)))
        (dolist (text (nthcdr (1+ (length key-columns)) box))
          (emit text)))
      (dolist (section '((out . "References") (in . "Referenced by")))
        (let ((group (cl-remove-if-not
                      (lambda (relation)
                        (eq (plist-get relation :direction) (car section)))
                      relations)))
          (insert "\n")
          (emit (propertize (format "%s (%d)" (cdr section) (length group))
                            'face 'bold))
          (if (null group)
              (emit (propertize "  none" 'face 'shadow))
            (cl-loop for text in (clutch-viz-columns
                                  (mapcar (lambda (relation)
                                            (clutch-er--relation-row
                                             relation shown-schema arrow))
                                          group)
                                  2)
                     for relation in group
                     do (let ((beg (point)))
                          (emit (concat "  " text)
                                (list (plist-get relation :id)))
                          (put-text-property beg (point)
                                             'clutch-er-relation relation)))))))
    (setq clutch-er--lines
          (cl-remove-if-not #'cl-third (nreverse lines)))
    (dolist (spec clutch-er--lines)
      (put-text-property (nth 0 spec) (1+ (nth 1 spec))
                         'clutch-er-ids (nth 2 spec)))
    (goto-char (point-min))
    (forward-line (1- line))
    (setq clutch-er--highlighted 'none)
    (clutch-er--update-highlight)))

(defun clutch-er--update-highlight ()
  "Highlight the relations of the line at point, if they changed."
  (let ((ids (get-text-property (point) 'clutch-er-ids)))
    (unless (equal ids clutch-er--highlighted)
      (setq clutch-er--highlighted ids)
      (mapc #'delete-overlay clutch-er--overlays)
      (setq clutch-er--overlays nil)
      (pcase-dolist (`(,beg ,end ,line-ids) clutch-er--lines)
        (when (cl-intersection ids line-ids)
          (let ((overlay (make-overlay beg end)))
            (overlay-put overlay 'face 'clutch-er-highlight)
            (push overlay clutch-er--overlays)))))))

;;;; Buffer

(defun clutch-er--show (entry &optional model)
  "Show the table of ENTRY in the current ER buffer.
MODEL is its ER model, loaded now when nil."
  (let ((model (or model
                   (clutch--with-object-error-capture
                       (current-buffer) clutch-connection entry "er-diagram"
                     (clutch-er--load clutch-connection entry)))))
    (setq clutch-er--entry entry
          clutch-er--model model
          clutch-browser-current-object entry)
    (clutch-er--render)))

(defun clutch-er--relation-at-point ()
  "Return the relation at point, asking when the line joins several."
  (or (get-text-property (point) 'clutch-er-relation)
      (let ((ids (get-text-property (point) 'clutch-er-ids))
            (relations (plist-get clutch-er--model :relations)))
        (pcase ids
          ('nil (user-error "No relation at point"))
          (`(,id) (seq-find (lambda (r) (eql (plist-get r :id) id)) relations))
          (_ (let ((choices
                    (mapcar (lambda (id)
                              (let ((relation
                                     (seq-find (lambda (r)
                                                 (eql (plist-get r :id) id))
                                               relations)))
                                (cons (substring-no-properties
                                       (string-join
                                        (clutch-er--relation-row
                                         relation
                                         (plist-get clutch-er--model :schema)
                                         "->")
                                        " "))
                                      relation)))
                            ids)))
               (cdr (assoc (completing-read "Follow relation: " choices nil t)
                           choices))))))))

(defun clutch-er-follow ()
  "Show the table at the other end of the relation at point."
  (interactive)
  (let* ((relation (clutch-er--relation-at-point))
         (current clutch-er--entry))
    (clutch-er--show (list :name (plist-get relation :table)
                           :type "TABLE"
                           :schema (plist-get relation :schema)))
    (push current clutch-er--history)))

(defun clutch-er-back ()
  "Show the table shown before the current one."
  (interactive)
  (unless clutch-er--history
    (user-error "No earlier table"))
  (clutch-er--show (car clutch-er--history))
  (pop clutch-er--history))

(defun clutch-er-toggle-columns ()
  "Show every column of the table, or only its key columns."
  (interactive)
  (setq clutch-er--all-columns (not clutch-er--all-columns))
  (clutch-er--render))

(defun clutch-er-refresh ()
  "Load the table's columns and relations again."
  (interactive)
  (clutch--clear-table-metadata-caches clutch-connection
                                       (plist-get clutch-er--entry :name))
  (clutch-er--show clutch-er--entry))

(defun clutch-er-next (n)
  "Move to the Nth next line that belongs to a relation."
  (interactive "p")
  (let ((step (if (< n 0) -1 1))
        (start (line-beginning-position)))
    (dotimes (_ (abs n))
      (while (progn (forward-line step)
                    (and (not (get-text-property (point) 'clutch-er-ids))
                         (not (if (> step 0) (eobp) (bobp)))))))
    (unless (get-text-property (point) 'clutch-er-ids)
      (goto-char start)
      (user-error "No more relations"))))

(defun clutch-er-previous (n)
  "Move to the Nth previous line that belongs to a relation."
  (interactive "p")
  (clutch-er-next (- n)))

(defun clutch-er-browse ()
  "Browse the rows of the table at the center of the view."
  (interactive)
  (clutch-object-browse clutch-er--entry))

(defun clutch-er-describe ()
  "Describe the table at the center of the view."
  (interactive)
  (clutch-object-describe clutch-er--entry))

(defvar clutch-er-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "RET") #'clutch-er-follow)
    (define-key map (kbd "TAB") #'clutch-er-toggle-columns)
    (define-key map (kbd "n") #'clutch-er-next)
    (define-key map (kbd "p") #'clutch-er-previous)
    (define-key map (kbd "l") #'clutch-er-back)
    (define-key map (kbd "g") #'clutch-er-refresh)
    (define-key map (kbd "b") #'clutch-er-browse)
    (define-key map (kbd "d") #'clutch-er-describe)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    map)
  "Keymap for `clutch-er-mode'.")

(define-derived-mode clutch-er-mode special-mode "Clutch-ER"
  "Mode for the ER view of a table.
\\{clutch-er-mode-map}"
  (setq-local truncate-lines t
              revert-buffer-function (lambda (&rest _) (clutch-er-refresh))
              clutch-er--header-base
              (concat " ["
                      (clutch--key-hints '(("RET" "Follow")
                                           ("TAB" "Columns")
                                           ("l" "Back")
                                           ("b" "Browse")
                                           ("g" "Refresh")))
                      "]")
              header-line-format
              (list :eval (list #'clutch--header-with-disconnect-badge
                                'clutch-er--header-base)))
  (add-hook 'post-command-hook #'clutch-er--update-highlight nil t))

;;;###autoload
(defun clutch-er-diagram (&optional entry)
  "Show the table of ENTRY with the tables its foreign keys join it to.
Without ENTRY, use the table at point or ask for one."
  (interactive)
  (let* ((source (current-buffer))
         (context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "ER diagram for table: "
                                                  t '("TABLE"))))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection"))))
    (unless (clutch--table-entry-p entry)
      (user-error "%s is not a table" (clutch--object-fqname entry)))
    (let ((model (clutch--with-object-error-capture
                     source conn entry "er-diagram"
                   (clutch-er--load conn entry)))
          (buffer (get-buffer-create
                   (format "*clutch ER: %s*" (clutch--connection-key conn)))))
      (with-current-buffer buffer
        (unless (derived-mode-p 'clutch-er-mode)
          (clutch-er-mode))
        (clutch--bind-connection-context conn (plist-get context :params)
                                         (plist-get context :product))
        (let ((previous clutch-er--entry))
          (clutch-er--show entry model)
          (when (and previous (not (equal previous entry)))
            (push previous clutch-er--history))))
      (pop-to-buffer buffer))))

(provide 'clutch-er)

;;; clutch-er.el ends here
