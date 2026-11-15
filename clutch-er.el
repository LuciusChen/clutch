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
`auto' draws with Unicode symbols and line characters where each is one
column wide, and with ASCII where one is not, as in CJK language
environments that make ambiguous characters wide.  `unicode' and
`ascii' force one set, for a terminal whose widths differ from those
Emacs assumes."
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

(defface clutch-er-title
  '((t :inherit (bold font-lock-keyword-face)))
  "Face for the name of the table the ER view shows."
  :group 'clutch)

(defface clutch-er-table
  '((t :inherit bold))
  "Face for the names of related tables in the ER view."
  :group 'clutch)

(defface clutch-er-key
  '((t :inherit font-lock-constant-face))
  "Face for the primary-key marker in the ER view."
  :group 'clutch)

(defface clutch-er-reference
  '((t :inherit font-lock-type-face))
  "Face for the marker of a column whose foreign key references a table."
  :group 'clutch)

(defface clutch-er-manual
  '((t :inherit shadow :slant italic))
  "Face for the link and marker of a relation added by hand."
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

;; The view is one card: the table's name over a rule, then a line for
;; each column with, beside it, the relation that column ends.  A
;; relation over several adjacent columns draws a bracket down them,
;; and a column that ends several relations takes a line for each.

(defun clutch-er--charset ()
  "Return the characters the ER view draws with: `unicode' or `ascii'.
See `clutch-er-charset'."
  (if (eq clutch-er-charset 'auto)
      (clutch-viz-charset '(?◆ ?● ?○ ?▸ ?◂ ?┐ ?┤ ?┴ ?┄ ?▌ ?· ?…))
    clutch-er-charset))

(defun clutch-er--name (table schema shown-schema)
  "Return TABLE, qualified by a muted SCHEMA unless it is SHOWN-SCHEMA."
  (concat (if (and schema (not (equal schema shown-schema)))
              (propertize (concat schema ".") 'face 'shadow)
            "")
          (propertize table 'face 'clutch-er-table)))

(defun clutch-er--relation-ids (column relations)
  "Return the ids of RELATIONS that join COLUMN of the table."
  (cl-loop for relation in relations
           when (assoc column (plist-get relation :pairs))
           collect (plist-get relation :id)))

(defun clutch-er--marker (column relations unicode)
  "Return the marker of COLUMN for its primary key or a key from it.
COLUMN is a column details plist; RELATIONS are those shown.  Use box
characters when UNICODE, else ASCII."
  (let ((starts (cl-loop for relation in relations
                         when (and (eq (plist-get relation :direction) 'out)
                                   (assoc (plist-get column :name)
                                          (plist-get relation :pairs)))
                         collect relation)))
    (cond ((plist-get column :primary-key)
           (propertize (if unicode "◆" "*") 'face 'clutch-er-key))
          ((cl-find-if-not (lambda (relation) (plist-get relation :stored)) starts)
           (propertize (if unicode "●" "o") 'face 'clutch-er-reference))
          (starts (propertize (if unicode "○" ".") 'face 'clutch-er-manual))
          (t " "))))

(defun clutch-er--end (relation shown-schema)
  "Return the other end of RELATION: its table and the columns there.
Names in SHOWN-SCHEMA are not qualified."
  (let ((columns (mapcar #'cdr (plist-get relation :pairs))))
    (concat (clutch-er--name (plist-get relation :table)
                             (plist-get relation :schema) shown-schema)
            (propertize (if (cdr columns)
                            (format " (%s)" (string-join columns ", "))
                          (concat "." (car columns)))
                        'face 'shadow))))

(defun clutch-er--link (relation part unicode)
  "Return the link drawn for RELATION on one of its lines.
PART is `only' for a relation over one column, else `first', `middle'
or `last' down the bracket of its columns.  The link is three columns
wide and points from the key to what it references; a relation added
by hand is dotted.  Use box characters when UNICODE, else ASCII."
  (let* ((in (eq (plist-get relation :direction) 'in))
         (dot (plist-get relation :stored))
         (line (if unicode (if dot "┄" "─") (if dot "." "-")))
         (text (pcase part
                 ('first (concat line (if unicode "┐" "+") " "))
                 ('middle (concat line (if unicode "┤" "+") " "))
                 ('last (if in
                            (concat (if unicode "◂┴" "<+") line)
                          (concat line (if unicode "┴▸" "+>"))))
                 (_ (if in
                        (concat (if unicode "◂" "<") line line)
                      (concat line line (if unicode "▸" ">")))))))
    (propertize text 'face (if dot 'clutch-er-manual 'shadow))))

(defun clutch-er--lines (columns relations shown-schema unicode)
  "Return the lines of COLUMNS with RELATIONS beside them, as cell lists.
Each line is (PROPERTIES COLUMN-CELL TYPE-CELL LINK-CELL): a column's
first line carries its marker, name and type, and a further line for a
column that ends several relations leaves them blank.  PROPERTIES are
the text properties of the whole line.  Names in SHOWN-SCHEMA are not
qualified; box characters are used when UNICODE, else ASCII."
  (let* ((names (mapcar (lambda (column) (plist-get column :name)) columns))
         (position (lambda (name) (cl-position name names :test #'equal)))
         ;; A relation ends at its last shown column.  One over several
         ;; adjacent columns that end no other relation brackets them.
         (anchor (lambda (relation)
                   (car (sort (mapcar (lambda (pair) (funcall position (car pair)))
                                      (plist-get relation :pairs))
                              #'>))))
         (anchors (mapcar (lambda (relation) (cons relation (funcall anchor relation)))
                          relations))
         (brackets
          (cl-loop for (relation . end) in anchors
                   for rows = (sort (mapcar (lambda (pair)
                                              (funcall position (car pair)))
                                            (plist-get relation :pairs))
                                    #'<)
                   when (and (cdr rows)
                             (= (- end (car rows)) (1- (length rows)))
                             (cl-notany (lambda (other)
                                          (and (memq (cdr other) (butlast rows))
                                               (not (eq (car other) relation))))
                                        anchors))
                   collect (cons relation rows))))
    (cl-loop
     for column in columns
     for index from 0
     for name = (plist-get column :name)
     for ending = (cl-loop for (relation . end) in anchors
                           when (eql end index) collect relation)
     for crossing = (cl-loop for (relation . rows) in brackets
                             when (and (memq index rows) (/= index (car (last rows))))
                             return (cons relation (if (= index (car rows))
                                                       'first 'middle)))
     for column-cell = (concat " " (clutch-er--marker column relations unicode)
                               " " name)
     for type-cell = (propertize (or (plist-get column :type) "") 'face 'shadow)
     nconc
     (if crossing
         (list (list (list 'clutch-er-column name
                           'clutch-er-relation (car crossing)
                           'clutch-er-ids (clutch-er--relation-ids name relations))
                     column-cell type-cell
                     (clutch-er--link (car crossing) (cdr crossing) unicode)))
       (or (cl-loop
            for relation in ending
            for first = t then nil
            for bracketed = (assq relation brackets)
            collect
            (list (append (and first (list 'clutch-er-column name))
                          (list 'clutch-er-relation relation
                                'clutch-er-ids
                                (if first
                                    (clutch-er--relation-ids name relations)
                                  (list (plist-get relation :id)))))
                  (if first column-cell "")
                  (if first type-cell "")
                  (concat (clutch-er--link relation (if bracketed 'last 'only)
                                           unicode)
                          " "
                          (clutch-er--end relation shown-schema)
                          (and (cdr (plist-get relation :pairs)) (not bracketed)
                               (propertize
                                (format "  %s (%s)"
                                        (if (eq (plist-get relation :direction) 'in)
                                            "to" "from")
                                        (string-join (mapcar #'car (plist-get relation :pairs))
                                                     ", "))
                                'face 'shadow))
                          (and (plist-get relation :stored)
                               (propertize "  manual" 'face 'clutch-er-manual)))))
           (list (list (list 'clutch-er-column name
                             'clutch-er-ids (clutch-er--relation-ids name relations))
                       column-cell type-cell "")))))))

(defun clutch-er--render ()
  "Draw the current ER model in the current buffer."
  (let* ((inhibit-read-only t)
         (unicode (eq (clutch-er--charset) 'unicode))
         (relations (plist-get clutch-er--model :relations))
         (shown-schema (plist-get clutch-er--model :schema))
         (all (plist-get clutch-er--model :columns))
         (columns (cl-remove-if-not
                   (lambda (column)
                     (or clutch-er--all-columns
                         (plist-get column :primary-key)
                         (clutch-er--relation-ids (plist-get column :name) relations)))
                   all))
         (lines (clutch-er--lines columns relations shown-schema unicode))
         (texts (mapcar #'string-trim-right
                        (clutch-viz-columns (mapcar #'cdr lines) 0 3)))
         (hidden (- (length all) (length columns)))
         (title (concat " " (propertize (plist-get clutch-er--entry :name)
                                        'face 'clutch-er-title)))
         (summary (propertize
                   (concat (if shown-schema
                               (concat shown-schema (if unicode " · " " - "))
                             "")
                           (format "%d column%s" (length all)
                                   (if (= (length all) 1) "" "s")))
                   'face 'shadow))
         (width (max 40 (+ (string-width title) (string-width summary) 4)
                     (apply #'max 0 (mapcar #'string-width texts))))
         (point-line (line-number-at-pos)))
    (erase-buffer)
    (insert "\n" title
            (make-string (- width (string-width title) (string-width summary)) ?\s)
            summary "\n"
            " " (propertize (make-string (1- width) (if unicode ?─ ?-)) 'face 'shadow)
            "\n")
    (cl-loop for (properties) in lines
             for text in texts
             do (insert (apply #'propertize (concat text "\n") properties)))
    (when (> hidden 0)
      (insert (propertize (format "   %s %d more column%s\n"
                                  (if unicode "…" "...") hidden
                                  (if (= hidden 1) "" "s"))
                          'face 'shadow)))
    (unless relations
      (insert "\n" (propertize "   No foreign keys reach this table." 'face 'shadow) "\n"))
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
    (forward-line (1- point-line))
    (setq clutch-er--highlighted 'none)
    (clutch-er--update-highlight)))

(defun clutch-er--update-highlight ()
  "Highlight the relations of the line at point, if they changed.
A highlighted line also shows a bar in its first column."
  (let ((ids (get-text-property (point) 'clutch-er-ids))
        (bar (propertize (if (eq (clutch-er--charset) 'unicode) "▌" "|")
                         'face 'clutch-er-highlight)))
    (unless (equal ids clutch-er--highlighted)
      (setq clutch-er--highlighted ids)
      (mapc #'delete-overlay clutch-er--overlays)
      (setq clutch-er--overlays nil)
      (pcase-dolist (`(,beg ,end ,span-ids) clutch-er--lines)
        (when (cl-intersection ids span-ids)
          (let ((overlay (make-overlay beg end)))
            (overlay-put overlay 'face 'clutch-er-highlight)
            (push overlay clutch-er--overlays))
          (let ((overlay (make-overlay beg (1+ beg))))
            (overlay-put overlay 'display bar)
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
                                 (format "%s %s %s"
                                         (string-join
                                          (mapcar #'car (plist-get relation :pairs))
                                          ", ")
                                         (if (eq (plist-get relation :direction) 'in)
                                             "<-" "->")
                                         (clutch-er--end
                                          relation
                                          (plist-get clutch-er--model :schema))))
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
