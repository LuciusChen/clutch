;;; clutch-er.el --- Entity-relationship view of a table -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `clutch-er-diagram' shows a table with its key columns, the tables its
;; foreign keys reference and the tables whose foreign keys reference it:
;; one hop of relations around the table.  A key over several columns is
;; one relation.  Moving point only moves the highlight of the relation
;; at point; the buffer is drawn again only when its table, its columns
;; or its relations change.
;;
;; Relations the database does not declare can be added by hand.  They
;; are kept in a JSON file for each connection and serve browsing only:
;; nothing that edits rows reads them.

;;; Code:

(require 'cl-lib)
(require 'json)
(require 'clutch-object)
(require 'clutch-viz)

(defcustom clutch-er-charset 'auto
  "Characters the ER view draws with.
`auto' draws with box characters where they are one column wide and
with ASCII where they are not, as in CJK language environments that
make ambiguous characters wide.  `unicode' and `ascii' force one set,
for a terminal whose widths differ from those Emacs assumes."
  :type '(choice (const :tag "Choose by character width" auto)
                 (const :tag "Box characters" unicode)
                 (const :tag "ASCII" ascii))
  :group 'clutch)

(defcustom clutch-er-relations-directory
  (expand-file-name "clutch/relations" user-emacs-directory)
  "Directory of the files that keep relations added by hand.
Each connection has its own file there, named after its backend and
endpoint, unless its parameters name one with `:relations-file'."
  :type 'directory
  :group 'clutch)

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

(defface clutch-er-manual
  '((t :inherit font-lock-doc-face))
  "Face for the arrow of a relation added by hand."
  :group 'clutch)

(defvar-local clutch-er--entry nil
  "Object entry of the table at the center of the ER view.")

(defvar-local clutch-er--model nil
  "What the ER view shows, a plist.
:columns are the table's column details, :database the relations its
keys make, :manual the relations stored by hand for the connection,
:relations those of both that the view shows, numbered by :id, :schema
the namespace names are shown relative to and :current the connection's
current schema.")

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

;; A relation is a plist: :direction `out' or `in', the other end's
;; :table and :schema, and :pairs, a list of (COLUMN . OTHER-COLUMN),
;; COLUMN being the shown table's own.  One added by hand also carries
;; :stored, its form in the relations file.

(defun clutch-er--runs (items key)
  "Split ITEMS into lists of consecutive items with `equal' KEY values."
  (let (runs)
    (dolist (item items)
      (if (and runs (equal (funcall key item) (funcall key (caar runs))))
          (push item (car runs))
        (push (list item) runs)))
    (nreverse (mapcar #'reverse runs))))

(defun clutch-er--relations (outgoing incoming)
  "Return the relations a table's keys make.
OUTGOING is the table's `clutch-db-foreign-keys', INCOMING its
`clutch-db-referencing-keys'."
  (append
   (mapcar (lambda (run)
             (let ((ref (cdar run)))
               (list :direction 'out
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
             (list :direction 'in
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
                                    (plist-get key :constraint)))))))

(defun clutch-er--same-relation-p (a b current)
  "Return non-nil when relations A and B join the same columns.
A relation without a schema is in CURRENT, the connection's schema."
  (and (eq (plist-get a :direction) (plist-get b :direction))
       (equal (plist-get a :table) (plist-get b :table))
       (equal (or (plist-get a :schema) current)
              (or (plist-get b :schema) current))
       (= (length (plist-get a :pairs)) (length (plist-get b :pairs)))
       (null (cl-set-exclusive-or (plist-get a :pairs) (plist-get b :pairs)
                                  :test #'equal))))

;;;; Relations added by hand

;; The file holds {"version": 1, "relations": [...]}.  A relation is
;; {"from": END, "to": END, "columns": [[FROM-COLUMN, TO-COLUMN], ...]},
;; and END is {"catalog": C, "schema": S, "table": T}, with catalog and
;; schema optional.  An END without a schema names a table in the
;; connection's current schema.  Relations are read as parsed, alists and
;; vectors, and written back in the same shape.

(defun clutch-er--relations-file (conn params)
  "Return the relations file of CONN, opened with PARAMS."
  (or (plist-get params :relations-file)
      (let ((name (mapconcat (lambda (key) (format "%s" (plist-get params key)))
                             (cl-remove-if-not
                              (lambda (key) (plist-get params key))
                              '(:backend :url :profile-entry :host :port
                                         :database))
                             "-")))
        (expand-file-name
         (concat (replace-regexp-in-string
                  "[^[:alnum:]._-]+" "_"
                  (if (string-empty-p name) (clutch--connection-key conn) name))
                 ".json")
         clutch-er-relations-directory))))

(defun clutch-er--valid-end-p (end)
  "Return non-nil when END is a stored table: a table and maybe namespaces."
  (and (consp end)
       (stringp (alist-get 'table end))
       (cl-every (lambda (field)
                   (and (memq (car field) '(catalog schema table))
                        (stringp (cdr field))
                        (not (string-empty-p (cdr field)))))
                 end)))

(defun clutch-er--valid-relation-p (relation)
  "Return non-nil when RELATION has the stored shape and nothing else."
  (and (consp relation)
       (equal (sort (mapcar #'car relation) #'string<) '(columns from to))
       (clutch-er--valid-end-p (alist-get 'from relation))
       (clutch-er--valid-end-p (alist-get 'to relation))
       (let ((columns (alist-get 'columns relation)))
         (and (vectorp columns)
              (> (length columns) 0)
              (cl-every (lambda (pair)
                          (and (vectorp pair) (= (length pair) 2)
                               (cl-every #'stringp pair)))
                        columns)))))

(defun clutch-er--read-relations (file)
  "Return the relations stored in FILE, or nil when it does not exist.
Signal a `user-error' naming FILE when it is not a relations file of
version 1."
  (when (file-exists-p file)
    (let ((data (condition-case err
                    (with-temp-buffer
                      (insert-file-contents file)
                      (json-parse-buffer :object-type 'alist
                                         :array-type 'array))
                  (json-error
                   (user-error "Relations file %s is not JSON: %s"
                               file (error-message-string err))))))
      (unless (and (consp data) (equal (alist-get 'version data) 1))
        (user-error "Relations file %s is not of version 1" file))
      (let ((relations (alist-get 'relations data)))
        (unless (and (vectorp relations)
                     (cl-every #'clutch-er--valid-relation-p relations))
          (user-error "Relations file %s holds a malformed relation" file))
        (append relations nil)))))

(defun clutch-er--write-relations (file relations)
  "Replace FILE with RELATIONS, in a stable order, one field to a line.
FILE is replaced whole, through a temporary file beside it, so a reader
never sees part of it; a link is followed to the file it names."
  (let* ((file (file-truename file))
         (text (concat
                "{\n  \"version\": 1,\n  \"relations\": ["
                (mapconcat
                 (lambda (relation)
                   (format "\n    {\n      \"from\": %s,\n      \"to\": %s,\n      \"columns\": %s\n    }"
                           (json-encode (alist-get 'from relation))
                           (json-encode (alist-get 'to relation))
                           (json-encode (alist-get 'columns relation))))
                 (sort (copy-sequence relations)
                       (lambda (a b) (string< (json-encode a) (json-encode b))))
                 ",")
                (if relations "\n  " "")
                "]\n}\n"))
         (temporary nil))
    (make-directory (file-name-directory file) t)
    (unwind-protect
        (progn
          (setq temporary (make-temp-file
                           (expand-file-name ".relations-"
                                             (file-name-directory file))
                           nil ".json"))
          (let ((coding-system-for-write 'utf-8-unix))
            (write-region text nil temporary nil 'silent))
          (when (file-exists-p file)
            (set-file-modes temporary (file-modes file)))
          (rename-file temporary file t))
      (when (and temporary (file-exists-p temporary))
        (delete-file temporary)))))

(defun clutch-er--stored-end (conn entry)
  "Return the stored form of the table of ENTRY on CONN.
Its schema is left out when it is CONN's current schema."
  (let ((schema (plist-get entry :schema))
        (catalog (plist-get entry :catalog)))
    (append (and catalog `((catalog . ,catalog)))
            (and schema (not (equal schema (clutch-db-current-schema conn)))
                 `((schema . ,schema)))
            `((table . ,(plist-get entry :name))))))

(defun clutch-er--end-entry (end)
  "Return the object entry of stored table END."
  (list :name (alist-get 'table end) :type "TABLE"
        :schema (alist-get 'schema end) :catalog (alist-get 'catalog end)))

(defun clutch-er--end-p (end entry current)
  "Return non-nil when stored END names the table of ENTRY.
CURRENT is the connection's current schema, which a missing one means."
  (and (equal (alist-get 'table end) (plist-get entry :name))
       (equal (or (alist-get 'schema end) current)
              (or (plist-get entry :schema) current))
       (equal (alist-get 'catalog end) (plist-get entry :catalog))))

(defun clutch-er--stored-relations (stored entry current)
  "Return the relations of the table of ENTRY among STORED ones.
A relation stored from the table to itself is both outgoing and
incoming.  CURRENT is the connection's current schema."
  (cl-loop for relation in stored
           for from = (alist-get 'from relation)
           for to = (alist-get 'to relation)
           for pairs = (mapcar (lambda (pair) (cons (aref pair 0) (aref pair 1)))
                               (alist-get 'columns relation))
           when (clutch-er--end-p from entry current)
           collect (list :direction 'out
                         :table (alist-get 'table to)
                         :schema (alist-get 'schema to)
                         :pairs pairs :stored relation)
           when (clutch-er--end-p to entry current)
           collect (list :direction 'in
                         :table (alist-get 'table from)
                         :schema (alist-get 'schema from)
                         :pairs (mapcar (lambda (pair)
                                          (cons (cdr pair) (car pair)))
                                        pairs)
                         :stored relation)))

(defun clutch-er--with-manual (model manual entry)
  "Return MODEL of the table of ENTRY with MANUAL, the stored relations.
A stored relation that joins the same columns as a key of the database
is not shown again."
  (let* ((current (plist-get model :current))
         (database (plist-get model :database))
         (shown (cl-remove-if
                 (lambda (relation)
                   (cl-some (lambda (key)
                              (clutch-er--same-relation-p relation key current))
                            database))
                 (clutch-er--stored-relations manual entry current)))
         (id -1)
         (model (plist-put (copy-sequence model) :manual manual)))
    (plist-put model :relations
               (mapcar (lambda (relation)
                         (append (list :id (cl-incf id)) relation))
                       (append database shown)))))

(defun clutch-er--load (conn params entry)
  "Return the ER model of the table of ENTRY on CONN, opened with PARAMS.
Its columns and foreign keys come from the metadata cache, loaded now
when missing; the keys that reference it and the relations file are
read each time."
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
         (outgoing (clutch--ensure-foreign-keys conn key))
         (current (clutch-db-current-schema conn)))
    (clutch-er--with-manual
     (list :columns columns
           :database (clutch-er--relations outgoing incoming)
           :schema (or schema current)
           :current current)
     (clutch-er--read-relations (clutch-er--relations-file conn params))
     entry)))

;;;; Drawing

(defun clutch-er--charset ()
  "Return the characters the ER view draws with: `unicode' or `ascii'.
See `clutch-er-charset'."
  (if (eq clutch-er-charset 'auto)
      (clutch-viz-charset '(?◆ ?→ ?← ?↔ ?⇢ ?…))
    clutch-er-charset))

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

(defun clutch-er--relation-row (relation shown-schema unicode)
  "Return the cells of RELATION's line.
Names in SHOWN-SCHEMA are not qualified.  The arrow between the ends
is dashed for a relation added by hand, and ASCII unless UNICODE."
  (let* ((pairs (plist-get relation :pairs))
         (own (clutch-er--column-list (mapcar #'car pairs) t))
         (other (concat (clutch-er--name (plist-get relation :table)
                                         (plist-get relation :schema)
                                         shown-schema)
                        " "
                        (clutch-er--column-list (mapcar #'cdr pairs))))
         (arrow (if (plist-get relation :stored)
                    (propertize (if unicode "⇢" "~>") 'face 'clutch-er-manual)
                  (propertize (if unicode "→" "->") 'face 'clutch-er-key))))
    (if (eq (plist-get relation :direction) 'out)
        (list own arrow other)
      (list other arrow own))))

(defun clutch-er--render ()
  "Draw the current ER model in the current buffer."
  (let* ((inhibit-read-only t)
         (unicode (eq (clutch-er--charset) 'unicode))
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
                   (if unicode 'unicode 'ascii))))
        (emit (car box))
        (cl-loop for text in (cdr box)
                 for column in key-columns
                 for name = (plist-get column :name)
                 do (emit (propertize text 'clutch-er-column name)
                          (clutch-er--relation-ids name relations)))
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
                                             relation shown-schema unicode))
                                          group)
                                  2)
                     for relation in group
                     do (emit (propertize (concat "  " text)
                                          'clutch-er-relation relation)
                              (list (plist-get relation :id))))))))
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

(defun clutch-er--buffer-name (conn)
  "Return the name of the ER buffer of CONN."
  (format "*clutch ER: %s*" (clutch--connection-key conn)))

(defun clutch-er--show (entry &optional model)
  "Show the table of ENTRY in the current ER buffer.
MODEL is its ER model, loaded now when nil."
  (let ((model (or model
                   (clutch--with-object-error-capture
                       (current-buffer) clutch-connection entry "er-diagram"
                     (clutch-er--load clutch-connection clutch--connection-params
                                      entry)))))
    (setq clutch-er--entry entry
          clutch-er--model model
          clutch-browser-current-object entry)
    (clutch-er--render)))

(defun clutch-er--relation-at-point (prompt)
  "Return the relation at point, with PROMPT when the line joins several."
  (or (get-text-property (point) 'clutch-er-relation)
      (let* ((relations (plist-get clutch-er--model :relations))
             (choices
              (mapcar (lambda (id)
                        (let ((relation (seq-find (lambda (relation)
                                                    (eql (plist-get relation :id)
                                                         id))
                                                  relations)))
                          (cons (substring-no-properties
                                 (string-join
                                  (clutch-er--relation-row
                                   relation (plist-get clutch-er--model :schema)
                                   nil)
                                  " "))
                                relation)))
                      (get-text-property (point) 'clutch-er-ids))))
        (pcase choices
          ('nil (user-error "No relation at point"))
          (`(,choice) (cdr choice))
          (_ (cdr (assoc (completing-read prompt choices nil t) choices)))))))

(defun clutch-er--stored-at-point (prompt)
  "Return the stored form of the relation at point, asking with PROMPT.
Signal unless that relation was added by hand."
  (or (plist-get (clutch-er--relation-at-point prompt) :stored)
      (user-error "This relation is a foreign key of the database")))

(defun clutch-er-follow ()
  "Show the table at the other end of the relation at point."
  (interactive)
  (let* ((relation (clutch-er--relation-at-point "Follow relation: "))
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

;;;; Editing relations

(defun clutch-er--read-columns (prompt names defaults)
  "Read distinct column names among NAMES with PROMPT, offering DEFAULTS."
  (let ((columns (completing-read-multiple
                  prompt names nil t (and defaults (string-join defaults ",")))))
    (cond ((null columns) (user-error "No columns given"))
          ((/= (length columns) (length (delete-dups (copy-sequence columns))))
           (user-error "A column is given twice"))
          (t columns))))

(defun clutch-er--read-relation (conn from columns &optional to to-columns)
  "Read a relation from the table of entry FROM on CONN, in stored form.
COLUMNS are the columns of FROM offered first; TO and TO-COLUMNS, the
referenced table and its columns, when an existing relation is edited.
The referenced columns default to the primary key when it has as many
columns."
  (let* ((names (lambda (entry)
                  (mapcar (lambda (column) (plist-get column :name))
                          (clutch--ensure-column-details
                           conn (clutch--table-key (plist-get entry :name)
                                                   (plist-get entry :schema)
                                                   (plist-get entry :catalog))
                           t))))
         (from-columns (clutch-er--read-columns
                        (format "Columns of %s: " (plist-get from :name))
                        (funcall names from) columns))
         (to (clutch-object-read "Referenced table: " (plist-get to :name)
                                 '("TABLE")))
         (details (clutch--ensure-column-details
                   conn (clutch--table-key (plist-get to :name)
                                           (plist-get to :schema)
                                           (plist-get to :catalog))
                   t))
         (key (cl-loop for column in details
                       when (plist-get column :primary-key)
                       collect (plist-get column :name)))
         (to-columns (clutch-er--read-columns
                      (format "Columns of %s: " (plist-get to :name))
                      (mapcar (lambda (column) (plist-get column :name)) details)
                      (or to-columns
                          (and (= (length key) (length from-columns)) key)))))
    (unless (= (length from-columns) (length to-columns))
      (user-error "%d columns cannot reference %d"
                  (length from-columns) (length to-columns)))
    `((from . ,(clutch-er--stored-end conn from))
      (to . ,(clutch-er--stored-end conn to))
      (columns . ,(apply #'vector (cl-mapcar #'vector from-columns to-columns))))))

(defun clutch-er--check-new (conn relation others)
  "Signal unless RELATION, about to be stored on CONN, is new.
It must not join the same columns as a foreign key of the database or
as one of OTHERS, the relations stored already."
  (let* ((current (clutch-db-current-schema conn))
         (from (clutch-er--end-entry (alist-get 'from relation)))
         (new (car (clutch-er--stored-relations (list relation) from current))))
    (when (cl-some (lambda (key) (clutch-er--same-relation-p new key current))
                   (clutch-er--relations
                    (clutch--ensure-foreign-keys
                     conn (clutch--table-key (plist-get from :name)
                                             (plist-get from :schema)
                                             (plist-get from :catalog)))
                    nil))
      (user-error "The database already has this foreign key"))
    (when (cl-some (lambda (other) (clutch-er--same-relation-p new other current))
                   (clutch-er--stored-relations others from current))
      (user-error "This relation is stored already"))))

(defun clutch-er--save (function)
  "Replace the stored relations of the current connection by FUNCTION.
FUNCTION gets the relations the file holds now, read again so that a
change made elsewhere since the view loaded is kept, and returns those to
store.  The ER view of the connection shows them."
  (let* ((conn clutch-connection)
         (file (clutch-er--relations-file conn clutch--connection-params))
         (relations (funcall function (clutch-er--read-relations file))))
    (clutch-er--write-relations file relations)
    (when-let* ((buffer (get-buffer (clutch-er--buffer-name conn))))
      (with-current-buffer buffer
        (when clutch-er--entry
          (setq clutch-er--model (clutch-er--with-manual
                                  clutch-er--model relations clutch-er--entry))
          (clutch-er--render))))
    (message "Relations saved to %s" (abbreviate-file-name file))))

(defun clutch-er-add-relation (&optional from columns)
  "Add a relation from columns of a table to columns of another table.
FROM is the table's entry and COLUMNS its columns offered first; in the
ER view they default to the table at its center and the column at
point.  The relation is stored in the connection's relations file, see
`clutch-er-relations-directory', for browsing only."
  (interactive)
  (let* ((from (or from clutch-er--entry
                   (user-error "No table to add a relation from")))
         (columns (or columns
                      (when-let* ((name (get-text-property (point)
                                                           'clutch-er-column)))
                        (list name))))
         (relation (clutch-er--read-relation clutch-connection from columns)))
    (clutch-er--save (lambda (relations)
                       (clutch-er--check-new clutch-connection relation relations)
                       (cons relation relations)))))

(defun clutch-er-edit-relation ()
  "Change the columns or the referenced table of the relation at point.
Only a relation added by hand can change."
  (interactive)
  (let* ((old (clutch-er--stored-at-point "Edit relation: "))
         (columns (append (alist-get 'columns old) nil))
         (relation (clutch-er--read-relation
                    clutch-connection
                    (clutch-er--end-entry (alist-get 'from old))
                    (mapcar (lambda (pair) (aref pair 0)) columns)
                    (clutch-er--end-entry (alist-get 'to old))
                    (mapcar (lambda (pair) (aref pair 1)) columns))))
    (clutch-er--save (lambda (relations)
                       (unless (member old relations)
                         (user-error "The relation is no longer stored"))
                       (let ((others (remove old relations)))
                         (clutch-er--check-new clutch-connection relation others)
                         (cons relation others))))))

(defun clutch-er-delete-relation ()
  "Delete the relation at point, which must have been added by hand."
  (interactive)
  (let ((old (clutch-er--stored-at-point "Delete relation: ")))
    (when (y-or-n-p "Delete this relation? ")
      (clutch-er--save (lambda (relations)
                         (unless (member old relations)
                           (user-error "The relation is no longer stored"))
                         (remove old relations))))))

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
    (define-key map (kbd "a") #'clutch-er-add-relation)
    (define-key map (kbd "e") #'clutch-er-edit-relation)
    (define-key map (kbd "k") #'clutch-er-delete-relation)
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
                                           ("a" "Add relation")
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
                   (user-error "No active connection")))
         (params (plist-get context :params)))
    (unless (clutch--table-entry-p entry)
      (user-error "%s is not a table" (clutch--object-fqname entry)))
    (let ((model (clutch--with-object-error-capture
                     source conn entry "er-diagram"
                   (clutch-er--load conn params entry)))
          (buffer (get-buffer-create (clutch-er--buffer-name conn))))
      (with-current-buffer buffer
        (unless (derived-mode-p 'clutch-er-mode)
          (clutch-er-mode))
        (clutch--bind-connection-context conn params
                                         (plist-get context :product))
        (let ((previous clutch-er--entry))
          (clutch-er--show entry model)
          (when (and previous (not (equal previous entry)))
            (push previous clutch-er--history))))
      (pop-to-buffer buffer))))

(provide 'clutch-er)

;;; clutch-er.el ends here
