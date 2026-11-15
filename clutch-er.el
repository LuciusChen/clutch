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
  '((((class color) (background light)) :foreground "#b5179e" :weight bold)
    (((class color) (background dark)) :foreground "#ff79c6" :weight bold)
    (t :inverse-video t))
  "Face for the relation at point and the columns it joins.
It colors text rather than its background: box characters in many fonts
are taller than a line, and a graphical frame redraws the background of
the lines next to them over a highlighted line's background."
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
  "The highlightable stretches of the ER view, as (BEG END IDS).
IDS lists the relations the stretch belongs to, as its `clutch-er-ids'
property does.")

(defvar-local clutch-er--layout nil
  "Layout the ER view is drawn in: `wide', three columns, or `list'.")

(defvar-local clutch-er--wide-width nil
  "Columns the wide layout of the ER view needs, or nil without relations.")

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
           ;; Without a :constraint the columns of a key cannot be told
           ;; apart, so each column is a relation of its own.
           (clutch-er--runs outgoing
                            (lambda (fk)
                              (let ((ref (cdr fk)))
                                (if (plist-get ref :constraint)
                                    (list (plist-get ref :constraint)
                                          (plist-get ref :ref-schema)
                                          (plist-get ref :ref-table))
                                  fk)))))
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

(defun clutch-er--add-stored (database stored current)
  "Return DATABASE relations, then the STORED ones that repeat none of them.
A stored relation that joins the same columns as a key of the database
is left out.  CURRENT is the connection's current schema."
  (append database
          (cl-remove-if (lambda (relation)
                          (cl-some (lambda (key)
                                     (clutch-er--same-relation-p
                                      relation key current))
                                   database))
                        stored)))

(defun clutch-er--with-manual (model manual entry)
  "Return MODEL of the table of ENTRY with MANUAL, the stored relations."
  (let* ((current (plist-get model :current))
         (id -1)
         (model (plist-put (copy-sequence model) :manual manual)))
    (plist-put model :relations
               (mapcar (lambda (relation)
                         (append (list :id (cl-incf id)) relation))
                       (clutch-er--add-stored
                        (plist-get model :database)
                        (clutch-er--stored-relations manual entry current)
                        current)))))

(defun clutch-er-outgoing-relations (conn params entry)
  "Return the relations from the table of ENTRY on CONN, opened with PARAMS.
They are its foreign keys, one relation for each key, then the relations
stored by hand from it that repeat none of them; see
`clutch-er-add-relation'.  Each is a plist of the referenced :table and
:schema and :pairs, a list of (COLUMN . REFERENCED-COLUMN); one stored
by hand also has :stored.  Foreign keys come from the metadata cache,
loaded now when missing."
  (let ((current (clutch-db-current-schema conn)))
    (clutch-er--add-stored
     (clutch-er--relations
      (clutch--ensure-foreign-keys
       conn (clutch--table-key (plist-get entry :name)
                               (plist-get entry :schema)
                               (plist-get entry :catalog)))
      nil)
     (cl-remove-if-not
      (lambda (relation) (eq (plist-get relation :direction) 'out))
      (clutch-er--stored-relations
       (clutch-er--read-relations (clutch-er--relations-file conn params))
       entry current))
     current)))

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
      (clutch-viz-charset '(?◆ ?→ ?← ?↔ ?⇢ ?▶ ?…))
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

(defun clutch-er--window-width ()
  "Return the body width of a window showing the current buffer, or 0."
  (if-let* ((window (get-buffer-window (current-buffer) t)))
      (window-body-width window)
    0))

(defun clutch-er--layout ()
  "Return the layout the ER view needs now, `wide' or `list'.
It is `wide', three columns, when a window showing the view is wide
enough for them."
  (if (and clutch-er--wide-width
           (<= clutch-er--wide-width (clutch-er--window-width)))
      'wide
    'list))

(defun clutch-er--focus-lines (columns relations unicode)
  "Return the lines of the box of the shown table with COLUMNS shown.
The line of a column carries its name as `clutch-er-column' and the ids
of the RELATIONS that join it as `clutch-er-ids'.  Use box characters
when UNICODE, else ASCII."
  (let* ((all (plist-get clutch-er--model :columns))
         (hidden (- (length all) (length columns)))
         (rows (clutch-viz-columns
                (mapcar (lambda (column)
                          (list (propertize
                                 (concat (if (plist-get column :primary-key)
                                             (if unicode "◆" "*")
                                           " ")
                                         (clutch-er--marker (plist-get column :name)
                                                            relations unicode))
                                 'face 'clutch-er-key)
                                (plist-get column :name)
                                (propertize (or (plist-get column :type) "")
                                            'face 'shadow)))
                        columns)
                2))
         (box (clutch-viz-box
               (clutch-er--name (plist-get clutch-er--entry :name)
                                (plist-get clutch-er--entry :schema)
                                nil)
               (append rows
                       (when (> hidden 0)
                         (list (propertize
                                (format "%s %d more column%s"
                                        (if unicode "…" "...")
                                        hidden (if (= hidden 1) "" "s"))
                                'face 'shadow))))
               (if unicode 'unicode 'ascii))))
    (cons (car box)
          (cl-loop for line in (cdr box)
                   for index from 0
                   for column = (nth index columns)
                   collect (if column
                               (let ((name (plist-get column :name)))
                                 (propertize line
                                             'clutch-er-column name
                                             'clutch-er-ids
                                             (clutch-er--relation-ids
                                              name relations)))
                             line)))))

(defun clutch-er--side (relations shown-schema unicode)
  "Return the boxes of the tables RELATIONS join, stacked with gaps.
The result is (LINES . ROWS): the lines, and ROWS mapping each listed
\(TABLE SCHEMA COLUMN) to its line.  A column's line carries the ids of
its relations as `clutch-er-ids'.  Names in SHOWN-SCHEMA are not
qualified; box characters are used when UNICODE, else ASCII."
  (let (groups lines rows width)
    (dolist (relation relations)
      (let ((key (list (plist-get relation :table) (plist-get relation :schema))))
        (if-let* ((group (assoc key groups)))
            (setcdr group (append (cdr group) (list relation)))
          (push (list key relation) groups))))
    (setq groups
          (mapcar (pcase-lambda (`((,table ,schema) . ,group))
                    (list (clutch-er--name table schema shown-schema)
                          table schema group
                          (delete-dups
                           (cl-loop for relation in group
                                    append (mapcar #'cdr
                                                   (plist-get relation :pairs))))))
                  (nreverse groups)))
    ;; Boxes of one width end where the connectors start.
    (setq width (apply #'max 0 (mapcar (pcase-lambda (`(,title ,_ ,_ ,_ ,columns))
                                         (apply #'max (string-width title)
                                                (mapcar #'string-width columns)))
                                       groups)))
    (pcase-dolist (`(,title ,table ,schema ,group ,columns) groups)
      (when lines
        (setq lines (append lines (list ""))))
      (cl-loop for line in (clutch-viz-box title columns
                                           (if unicode 'unicode 'ascii) width)
               for index from -1
               for column = (and (>= index 0) (nth index columns))
               do (when column
                    (push (cons (list table schema column) (length lines))
                          rows)
                    (setq line
                          (propertize
                           line 'clutch-er-ids
                           (cl-loop for relation in group
                                    when (rassoc column
                                                 (plist-get relation :pairs))
                                    collect (plist-get relation :id)))))
               (setq lines (append lines (list line)))))
    (cons lines rows)))

(defun clutch-er--diagram (focus names relations shown-schema unicode)
  "Return the lines of the wide layout around FOCUS, the shown table's box.
NAMES are the columns FOCUS lists, in order.  Of RELATIONS, the tables
whose keys reference the table stand on the left, the tables it
references on the right, and connectors join each relation's first pair
of columns, with the arrow at the referenced end.  Names in SHOWN-SCHEMA are not
qualified; box characters are used when UNICODE, else ASCII."
  (pcase-let* ((charset (if unicode 'unicode 'ascii))
               (incoming (cl-remove-if-not
                          (lambda (relation) (eq (plist-get relation :direction) 'in))
                          relations))
               (outgoing (cl-remove-if-not
                          (lambda (relation) (eq (plist-get relation :direction) 'out))
                          relations))
               (`(,left . ,left-rows) (clutch-er--side incoming shown-schema unicode))
               (`(,right . ,right-rows) (clutch-er--side outgoing shown-schema unicode))
               (height (max (length left) (length focus) (length right))))
    (cl-flet ((gutter (group rows)
                ;; Each relation leaves from its upper end first.
                (let* ((ends (mapcar
                              (lambda (relation)
                                (let* ((pair (car (plist-get relation :pairs)))
                                       (own (1+ (cl-position (car pair) names
                                                             :test #'equal)))
                                       (other (cdr (assoc (list (plist-get relation :table)
                                                                (plist-get relation :schema)
                                                                (cdr pair))
                                                          rows))))
                                  (list (if (eq (plist-get relation :direction) 'in)
                                            (list other own)
                                          (list own other))
                                        (plist-get relation :stored)
                                        (plist-get relation :id))))
                              group))
                       (ends (sort ends (lambda (a b) (< (caar a) (caar b)))))
                       (drawn (clutch-viz-connectors
                               height
                               (mapcar (pcase-lambda (`((,from ,to) ,stored ,_))
                                         (list from to stored))
                                       ends)
                               charset))
                       (lines (mapcar (lambda (line)
                                        (propertize line 'face 'shadow))
                                      (car drawn))))
                  (cl-loop for cells in (cdr drawn)
                           for end in ends
                           do (pcase-dolist (`(,line . ,column) cells)
                                (let ((text (nth line lines)))
                                  (put-text-property
                                   column (1+ column) 'clutch-er-ids
                                   (cons (nth 2 end)
                                         (get-text-property column 'clutch-er-ids
                                                            text))
                                   text))))
                  lines)))
      (let ((left-gutter (and incoming (gutter incoming left-rows)))
            (right-gutter (and outgoing (gutter outgoing right-rows))))
        (clutch-viz-columns
         (cl-loop for index below height
                  collect (append
                           (and incoming (list (or (nth index left) "")
                                               (nth index left-gutter)))
                           (list (or (nth index focus) ""))
                           (and outgoing (list (nth index right-gutter)
                                               (or (nth index right) "")))))
         0 0)))))

(defun clutch-er--render ()
  "Draw the current ER model in the current buffer.
The layout is the one a window showing it is wide enough for."
  (let* ((inhibit-read-only t)
         (unicode (eq (clutch-er--charset) 'unicode))
         (relations (plist-get clutch-er--model :relations))
         (shown-schema (plist-get clutch-er--model :schema))
         (columns
          (cl-remove-if-not
           (lambda (column)
             (or clutch-er--all-columns
                 (plist-get column :primary-key)
                 (clutch-er--relation-ids (plist-get column :name) relations)))
           (plist-get clutch-er--model :columns)))
         (focus (clutch-er--focus-lines columns relations unicode))
         (diagram (and relations
                       (clutch-er--diagram
                        focus (mapcar (lambda (column) (plist-get column :name))
                                      columns)
                        relations shown-schema unicode)))
         (line (line-number-at-pos)))
    (setq clutch-er--wide-width
          (and diagram (apply #'max (mapcar #'string-width diagram)))
          clutch-er--layout (clutch-er--layout))
    (erase-buffer)
    (dolist (text (if (eq clutch-er--layout 'wide) diagram focus))
      (insert text "\n"))
    (dolist (section '((out . "References") (in . "Referenced by")))
      (let ((group (cl-remove-if-not
                    (lambda (relation)
                      (eq (plist-get relation :direction) (car section)))
                    relations)))
        (insert "\n" (propertize (format "%s (%d)" (cdr section) (length group))
                                 'face 'bold)
                "\n")
        (if (null group)
            (insert (propertize "  none" 'face 'shadow) "\n")
          (cl-loop for text in (clutch-viz-columns
                                (mapcar (lambda (relation)
                                          (clutch-er--relation-row
                                           relation shown-schema unicode))
                                        group)
                                2)
                   for relation in group
                   do (insert (propertize (concat "  " text "\n")
                                          'clutch-er-relation relation
                                          'clutch-er-ids
                                          (list (plist-get relation :id))))))))
    (setq clutch-er--lines
          (let ((position (point-min)) spans)
            (while (< position (point-max))
              (let ((next (next-single-property-change position 'clutch-er-ids
                                                       nil (point-max))))
                (when-let* ((ids (get-text-property position 'clutch-er-ids)))
                  (push (list position next ids) spans))
                (setq position next)))
            (nreverse spans)))
    (goto-char (point-min))
    (forward-line (1- line))
    (setq clutch-er--highlighted 'none)
    (clutch-er--update-highlight)))

(defun clutch-er--relayout (window)
  "Draw the ER view in WINDOW again for a width that needs another layout."
  (with-current-buffer (window-buffer window)
    (when (and clutch-er--model (not (eq clutch-er--layout (clutch-er--layout))))
      (clutch-er--render))))

(defun clutch-er--update-highlight ()
  "Highlight the relations of the text at point, if they changed."
  (let ((ids (get-text-property (point) 'clutch-er-ids)))
    (unless (equal ids clutch-er--highlighted)
      (setq clutch-er--highlighted ids)
      (mapc #'delete-overlay clutch-er--overlays)
      (setq clutch-er--overlays nil)
      (pcase-dolist (`(,beg ,end ,span-ids) clutch-er--lines)
        (when (cl-intersection ids span-ids)
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
        (start (point))
        found)
    (dotimes (_ (abs n))
      (setq found nil)
      (while (and (not found) (zerop (forward-line step)))
        (setq found (text-property-not-all (line-beginning-position)
                                           (line-end-position)
                                           'clutch-er-ids nil))))
    (if found
        (goto-char found)
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
  (add-hook 'post-command-hook #'clutch-er--update-highlight nil t)
  (add-hook 'window-size-change-functions #'clutch-er--relayout nil t))

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
                                         (plist-get context :product)))
      ;; Shown first, so the view is drawn for the width of its window.
      (pop-to-buffer buffer)
      (with-current-buffer buffer
        (let ((previous clutch-er--entry))
          (clutch-er--show entry model)
          (when (and previous (not (equal previous entry)))
            (push previous clutch-er--history)))))))

(provide 'clutch-er)

;;; clutch-er.el ends here
