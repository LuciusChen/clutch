;;; clutch-document.el --- Document database query console UI -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later


;; This file is part of clutch.

;;; Commentary:

;; Query-buffer modes, highlighting, indentation, and completion for document
;; database surfaces.  The MongoDB adapter lives in `clutch-mongodb.el`; this
;; file owns the MongoDB native helper/MQL console surface without loading the
;; external `mongodb' protocol package.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'clutch-query)
(require 'clutch-schema)
(require 'clutch-ui)
(require 'subr-x)
(require 'transient)

(defvar clutch-connection)

(declare-function clutch-act-dwim "clutch-object" (&optional entry))
(declare-function clutch-connect "clutch-connection" ())
(declare-function clutch-copy-context-for-agent "clutch-result" ())
(declare-function clutch-describe-dwim "clutch-object" (&optional entry))
(declare-function clutch-disconnect "clutch-connection" ())
(declare-function clutch--ensure-connection "clutch-connection" ())
(declare-function clutch-jump "clutch-object" (&optional entry))
(declare-function clutch-refresh-schema "clutch-connection" ())
(declare-function clutch-switch-schema "clutch-connection" ())

(defgroup clutch-mongodb nil
  "Native MongoDB backend for clutch."
  :group 'clutch)

(defcustom clutch-mongodb-indent-offset 2
  "Number of spaces for each nested level in MongoDB query buffers."
  :type 'integer
  :group 'clutch-mongodb)

(defface clutch-mongodb-operator-face
  '((t :inherit font-lock-builtin-face))
  "Face for MongoDB MQL operators such as `$match' and `$lookup'."
  :group 'clutch-mongodb)

(defface clutch-mongodb-key-face
  '((t :inherit font-lock-variable-name-face))
  "Face for MongoDB document keys before a colon."
  :group 'clutch-mongodb)

(defface clutch-mongodb-method-face
  '((t :inherit font-lock-function-name-face))
  "Face for MongoDB shell method names such as `find' and `aggregate'."
  :group 'clutch-mongodb)

(defface clutch-mongodb-namespace-face
  '((t :inherit font-lock-type-face))
  "Face for MongoDB namespace names such as `db' and collection names."
  :group 'clutch-mongodb)

(defvar clutch-mongodb-mode-syntax-table
  (let ((table (make-syntax-table prog-mode-syntax-table)))
    (modify-syntax-entry ?_ "w" table)
    (modify-syntax-entry ?$ "_" table)
    (modify-syntax-entry ?/ ". 124b" table)
    (modify-syntax-entry ?* ". 23" table)
    (modify-syntax-entry ?\n "> b" table)
    table)
  "Syntax table for `clutch-mongodb-mode'.")

(defconst clutch-mongodb--function-keywords
  '("aggregate" "countDocuments" "deleteOne" "distinct" "find" "findOne"
    "getCollection" "insertMany" "insertOne" "replaceOne" "runCommand"
    "updateOne")
  "MongoDB shell function names highlighted in `clutch-mongodb-mode'.")

(defconst clutch-mongodb--constructor-keywords
  '("ISODate" "ObjectId")
  "Supported MongoDB BSON constructor names highlighted in `clutch-mongodb-mode'.")

(defconst clutch-mongodb-font-lock-keywords
  `((,(regexp-opt '("db") 'symbols) . 'clutch-mongodb-namespace-face)
    ("\\.\\([[:alpha:]_][[:alnum:]_]*\\)\\_>\\s-*("
     1 'clutch-mongodb-method-face)
    ("\\.\\([[:alpha:]_][[:alnum:]_]*\\)\\_>"
     1 'clutch-mongodb-namespace-face)
    (,(regexp-opt clutch-mongodb--constructor-keywords 'symbols)
     . font-lock-type-face)
    ("\\$[[:alpha:]_][[:alnum:]_]*\\_>" . 'clutch-mongodb-operator-face)
    ("\\_<\\([[:alpha:]_][[:alnum:]_]*\\)\\_>\\s-*:"
     1 'clutch-mongodb-key-face)
    ("\\_<-?[0-9]+\\(?:\\.[0-9]+\\)?\\_>" . font-lock-constant-face)
    (,(regexp-opt '("true" "false" "null") 'symbols)
     . font-lock-constant-face))
  "Font-lock keywords for MongoDB Shell and MQL query buffers.")

(defun clutch-mongodb--object-array-depth-before-line ()
  "Return MongoDB object/array delimiter depth before the current line."
  (save-excursion
    (let ((limit (line-beginning-position))
          (depth 0))
      (goto-char (point-min))
      (while (< (point) limit)
        (unless (nth 8 (syntax-ppss))
          (pcase (char-after)
            ((or ?\[ ?{)
             (setq depth (1+ depth)))
            ((or ?\] ?})
             (setq depth (max 0 (1- depth))))))
        (forward-char 1))
      depth)))

(defun clutch-mongodb--calculate-indentation ()
  "Return indentation for the current MongoDB query line."
  (save-excursion
    (back-to-indentation)
    (let* ((depth (clutch-mongodb--object-array-depth-before-line))
           (closing-delimiter-p (looking-at-p "[]})]")))
      (* clutch-mongodb-indent-offset
         (max 0 (- depth (if closing-delimiter-p 1 0)))))))

(defun clutch-mongodb-indent-line ()
  "Indent the current MongoDB query line."
  (interactive)
  (let ((indent (clutch-mongodb--calculate-indentation))
        (offset (- (current-column) (current-indentation))))
    (indent-line-to indent)
    (when (> offset 0)
      (move-to-column (+ indent offset)))))

(defun clutch-mongodb--call-candidates (names)
  "Return MongoDB call candidates for method NAMES."
  (mapcar (lambda (name) (concat name "()")) names))

(defun clutch-mongodb--unique-candidates (candidates)
  "Return CANDIDATES without duplicates, preserving the first occurrence."
  (delete-dups (copy-sequence candidates)))

(defconst clutch-mongodb--aggregation-stage-candidates
  '("$addFields" "$bucket" "$bucketAuto" "$count" "$facet" "$group"
    "$limit" "$lookup" "$match" "$project" "$replaceRoot" "$replaceWith"
    "$sample" "$set" "$skip" "$sort" "$sortByCount" "$unionWith" "$unset"
    "$unwind")
  "Basic MongoDB aggregation pipeline stage completion candidates.")

(defconst clutch-mongodb--query-operator-candidates
  '("$all" "$and" "$elemMatch" "$eq" "$exists" "$expr" "$gt" "$gte"
    "$in" "$lt" "$lte" "$mod" "$ne" "$nin" "$nor" "$not" "$or" "$regex"
    "$size" "$text" "$type")
  "Basic MongoDB query and projection operator completion candidates.")

(defconst clutch-mongodb--expression-operator-candidates
  '("$add" "$addToSet" "$arrayElemAt" "$avg" "$concat" "$concatArrays"
    "$cond" "$dateAdd" "$dateDiff" "$dateFromString" "$dateToString"
    "$dateTrunc" "$divide" "$filter" "$first" "$ifNull" "$last" "$literal"
    "$map" "$max" "$mergeObjects" "$min" "$multiply" "$push" "$reduce"
    "$setDifference" "$setEquals" "$setIntersection" "$setUnion" "$size"
    "$slice" "$sortArray" "$subtract" "$sum" "$toDate" "$toDecimal"
    "$toDouble" "$toInt" "$toLong" "$toObjectId" "$toString")
  "Basic MongoDB aggregation expression operator completion candidates.")

(defconst clutch-mongodb--update-operator-candidates
  '("$addToSet" "$bit" "$currentDate" "$each" "$inc" "$max" "$min" "$mul"
    "$pop" "$position" "$pull" "$pullAll" "$push" "$rename" "$set"
    "$setOnInsert" "$slice" "$sort" "$unset")
  "MongoDB update operator completion candidates.")

(defconst clutch-mongodb--operator-candidates
  (clutch-mongodb--unique-candidates
   (append clutch-mongodb--aggregation-stage-candidates
           clutch-mongodb--query-operator-candidates
           clutch-mongodb--expression-operator-candidates
           clutch-mongodb--update-operator-candidates))
  "MongoDB MQL operator completion candidates.")

(defconst clutch-mongodb--db-method-candidates
  (clutch-mongodb--call-candidates
   '("getCollection" "runCommand"))
  "MongoDB shell methods useful after `db.'.")

(defconst clutch-mongodb--collection-method-candidates
  (clutch-mongodb--call-candidates
   '("aggregate" "countDocuments" "deleteOne" "distinct" "find" "findOne"
     "insertMany" "insertOne" "replaceOne" "updateOne"))
  "MongoDB shell methods useful after a collection expression.")

(defconst clutch-mongodb--collection-template-candidates
  '("find({}).limit(20)"
    "find({}, { FIELD: 1 }).limit(20)"
    "find({ FIELD: VALUE }).sort({ FIELD: -1 }).limit(20)"
    "aggregate([{ $match: {} }, { $limit: 20 }])"
    "aggregate([{ $match: {} }, { $group: { _id: \"$FIELD\", count: { $sum: 1 } } }])"
    "aggregate([{ $lookup: { from: \"COLLECTION\", localField: \"FIELD\", foreignField: \"_id\", as: \"joined\" } }])")
  "MongoDB collection helper templates exposed through completion.")

(defconst clutch-mongodb--find-chain-candidates
  (clutch-mongodb--call-candidates
   '("sort" "skip" "limit" "maxTimeMS" "allowDiskUse" "explain"))
  "MongoDB cursor helper methods useful after `find(...)'.")

(defconst clutch-mongodb--aggregate-chain-candidates
  (clutch-mongodb--call-candidates
   '("allowDiskUse" "maxTimeMS" "explain"))
  "MongoDB cursor helper methods useful after `aggregate(...)'.")

(defconst clutch-mongodb--cursor-chain-methods
  '("sort" "skip" "limit" "maxTimeMS" "allowDiskUse")
  "Non-terminal MongoDB cursor helper method names.")

(defconst clutch-mongodb--top-level-candidates
  (append '("db")
          (clutch-mongodb--call-candidates '("ObjectId" "ISODate")))
  "MongoDB shell top-level completion candidates.")

(defun clutch-mongodb--collection-candidates ()
  "Return cached MongoDB collection names for `clutch-connection'."
  (let ((schema (clutch--schema-for-connection clutch-connection)))
    (when (hash-table-p schema)
      (hash-table-keys schema))))

(defun clutch-mongodb--string-content-bounds ()
  "Return bounds for the current MongoDB string content, or nil."
  (let ((state (syntax-ppss)))
    (when (nth 3 state)
      (cons (1+ (nth 8 state)) (point)))))

(defun clutch-mongodb--collection-string-context-p (beg)
  "Return non-nil when string content at BEG names a MongoDB collection."
  (save-excursion
    (goto-char (1- beg))
    (or (looking-back
         "\\_<\\(?:coll\\|collection\\|from\\|into\\)\\_>\\s-*:\\s-*"
         (line-beginning-position))
        (looking-back "\\_<db\\.getCollection\\s-*(" (line-beginning-position)))))

(defun clutch-mongodb--field-string-context-p (beg)
  "Return non-nil when string content at BEG names a MongoDB field."
  (let ((content (buffer-substring-no-properties beg (point))))
    (or (string-prefix-p "$" content)
        (save-excursion
          (goto-char (1- beg))
          (looking-back
           "\\_<\\(?:localField\\|foreignField\\|connectFromField\\|connectToField\\)\\_>\\s-*:\\s-*"
           (line-beginning-position))))))

(defun clutch-mongodb--completion-bounds ()
  "Return completion bounds for MongoDB shell identifiers."
  (or (when-let* ((bounds (clutch-mongodb--string-content-bounds))
                  ((or (clutch-mongodb--collection-string-context-p (car bounds))
                       (clutch-mongodb--field-string-context-p (car bounds)))))
        bounds)
      (bounds-of-thing-at-point 'symbol)
      (and (looking-back "\\." (line-beginning-position))
           (cons (point) (point)))))

(defun clutch-mongodb--db-member-context-p (beg)
  "Return non-nil when BEG completes immediately after `db.'."
  (save-excursion
    (goto-char beg)
    (looking-back "\\_<db\\." (line-beginning-position))))

(defun clutch-mongodb--collection-method-context-p (beg)
  "Return non-nil when BEG completes a MongoDB collection method."
  (save-excursion
    (goto-char beg)
    (let ((prefix (buffer-substring-no-properties
                   (line-beginning-position) beg)))
      (or (string-match-p
           "\\_<db\\.[[:alpha:]_][[:alnum:]_]*\\_>\\s-*\\.\\s-*\\'"
           prefix)
          (string-match-p
           "\\_<db\\.getCollection\\s-*(\\s-*\"[^\"]+\"\\s-*)\\s-*\\.\\s-*\\'"
           prefix)))))

(defun clutch-mongodb--method-call-before-point ()
  "Return the MongoDB method call ending before point.
The return value is (METHOD . METHOD-START), or nil when point is not directly
after a helper call."
  (skip-chars-backward " \t\n\r")
  (when (eq (char-before) ?\))
    (condition-case _scan-error
        (progn
          (backward-list)
          (skip-chars-backward " \t\n\r")
          (let ((end (point)))
            (skip-chars-backward "[:alnum:]_")
            (when (< (point) end)
              (cons (buffer-substring-no-properties (point) end)
                    (point)))))
      (scan-error nil))))

(defun clutch-mongodb--cursor-chain-base-method (beg)
  "Return the base cursor method before completion at BEG, or nil."
  (save-excursion
    (goto-char beg)
    (skip-chars-backward " \t\n\r")
    (when (eq (char-before) ?.)
      (backward-char)
      (let (base terminal)
        (while (and (not base)
                    (not terminal)
                    (pcase-let ((`(,method . ,method-start)
                                 (clutch-mongodb--method-call-before-point)))
                      (cond
                       ((member method '("find" "aggregate"))
                        (setq base method)
                        nil)
                       ((string= method "explain")
                        (setq terminal t)
                        nil)
                       ((member method clutch-mongodb--cursor-chain-methods)
                        (goto-char method-start)
                        (skip-chars-backward " \t\n\r")
                        (when (eq (char-before) ?.)
                          (backward-char)
                          t))
                       (t nil)))))
        (and (not terminal) base)))))

(defun clutch-mongodb--field-key-context-p (beg)
  "Return non-nil when BEG completes an unquoted MongoDB document key."
  (save-excursion
    (goto-char beg)
    (skip-chars-backward " \t\n\r")
    (memq (char-before) '(?{ ?,))))

(defun clutch-mongodb--current-collection ()
  "Return the nearest collection expression before point, or nil."
  (let ((text (buffer-substring-no-properties (point-min) (point)))
        (start 0)
        collection)
    (while (string-match
            (concat "\\_<db\\.\\([[:alpha:]_][[:alnum:]_]*\\)\\_>\\s-*\\."
                    "\\|"
                    "\\_<db\\.getCollection\\s-*(\\s-*\"\\([^\"]+\\)\"\\s-*)")
            text start)
      (setq collection (or (match-string 1 text)
                           (match-string 2 text))
            start (match-end 0)))
    collection))

(defun clutch-mongodb--collection-columns (schema collection)
  "Return field names for COLLECTION using SCHEMA and lazy metadata loading."
  (or (clutch--cached-columns schema collection)
      (and clutch-connection
           schema
           (not (clutch-db-busy-p clutch-connection))
           (clutch--safe-completion-call
            (lambda ()
              (clutch--ensure-columns clutch-connection schema collection))))))

(defun clutch-mongodb--field-candidates (&optional field-path-p)
  "Return MongoDB field candidates.
When FIELD-PATH-P is non-nil, prefix candidates with `$' for aggregation field
path expressions."
  (let* ((schema (clutch--schema-for-connection clutch-connection))
         (collections (if-let* ((collection (clutch-mongodb--current-collection)))
                          (list collection)
                        (and (hash-table-p schema) (hash-table-keys schema))))
         fields)
    (when (hash-table-p schema)
      (dolist (collection collections)
        (dolist (field (clutch-mongodb--collection-columns schema collection))
          (when (stringp field)
            (push (if field-path-p (concat "$" field) field)
                  fields)))))
    (clutch-mongodb--unique-candidates (nreverse fields))))

(defun clutch-mongodb--completion-context (beg end)
  "Return the MongoDB completion context plist for BEG and END."
  (let ((prefix (buffer-substring-no-properties beg end)))
    (cond
     ((nth 4 (syntax-ppss)) nil)
     ((and (nth 3 (syntax-ppss))
           (clutch-mongodb--collection-string-context-p beg))
      (list :kind 'collection))
     ((and (nth 3 (syntax-ppss))
           (clutch-mongodb--field-string-context-p beg))
      (list :kind 'field-path
            :field-path (string-prefix-p "$" prefix)))
     ((clutch-mongodb--db-member-context-p beg)
      (list :kind 'db-member))
     ((clutch-mongodb--collection-method-context-p beg)
      (list :kind 'collection-method))
     ((if-let* ((base-method (clutch-mongodb--cursor-chain-base-method beg)))
          (pcase base-method
            ("find" (list :kind 'find-chain))
            ("aggregate" (list :kind 'aggregate-chain)))))
     ((string-prefix-p "$" prefix)
      (list :kind 'operator-key))
     ((clutch-mongodb--field-key-context-p beg)
      (list :kind 'field-key))
     (t
      (list :kind 'top-level)))))

(defun clutch-mongodb--completion-candidates (beg end)
  "Return MongoDB candidates for completion between BEG and END."
  (let* ((context (clutch-mongodb--completion-context beg end))
         (kind (plist-get context :kind)))
    (pcase kind
      ('collection
       (clutch-mongodb--collection-candidates))
      ('field-path
       (clutch-mongodb--field-candidates (plist-get context :field-path)))
      ('db-member
       (clutch-mongodb--unique-candidates
        (append (clutch-mongodb--collection-candidates)
                clutch-mongodb--db-method-candidates)))
      ('collection-method
       (append clutch-mongodb--collection-method-candidates
               clutch-mongodb--collection-template-candidates))
      ('find-chain
       clutch-mongodb--find-chain-candidates)
      ('aggregate-chain
       clutch-mongodb--aggregate-chain-candidates)
      ('operator-key
       clutch-mongodb--operator-candidates)
      ('field-key
       (clutch-mongodb--unique-candidates
        (append (clutch-mongodb--field-candidates)
                clutch-mongodb--operator-candidates)))
      ('top-level
       (clutch-mongodb--unique-candidates
        (append clutch-mongodb--top-level-candidates
                (clutch-mongodb--collection-candidates)
                clutch-mongodb--operator-candidates))))))

(defun clutch-mongodb--key-candidate-p (candidate kind)
  "Return non-nil when CANDIDATE in KIND should insert a trailing colon."
  (or (memq kind '(field-key operator-key))
      (and (eq kind 'top-level)
           (string-prefix-p "$" candidate))))

(defun clutch-mongodb-completion-at-point ()
  "Completion-at-point function for MongoDB Shell and MQL buffers."
  (when-let* ((bounds (clutch-mongodb--completion-bounds)))
    (let* ((beg (car bounds))
           (end (cdr bounds))
           (context (clutch-mongodb--completion-context beg end))
           (kind (plist-get context :kind))
           (candidates (clutch-mongodb--completion-candidates beg end)))
      (when candidates
        (list beg end candidates
              :exclusive 'no
              :exit-function
              (lambda (candidate status)
                (when (and (memq status '(finished exact sole))
                           (clutch-mongodb--key-candidate-p candidate kind)
                           (not (looking-at-p "\\s-*:")))
                  (insert ": "))))))))

(defun clutch--install-mongodb-completion-capfs ()
  "Install MongoDB completion CAPFs for the current buffer."
  (remove-hook 'completion-at-point-functions
               #'clutch-mongodb-completion-at-point t)
  (add-hook 'completion-at-point-functions
            #'clutch-mongodb-completion-at-point nil t)
  (add-hook 'corfu-mode-hook
            #'clutch--install-mongodb-completion-capfs nil t))

(defun clutch-mongodb-complete-at-point ()
  "Complete MongoDB Shell and MQL identifiers at point."
  (interactive)
  (let ((capf (clutch-mongodb-completion-at-point)))
    (pcase capf
      (`(,beg ,end ,collection . ,plist)
       (let ((completion-extra-properties plist)
             (completion-in-region-mode-predicate #'always))
         (unless (completion-in-region beg end collection
                                       (plist-get plist :predicate))
           (user-error "No completion at point"))))
      (_
       (user-error "No completion at point")))))

(defun clutch-mongodb-complete-or-indent ()
  "Complete MongoDB property names, falling back to indentation elsewhere."
  (interactive)
  (if (and (not (bounds-of-thing-at-point 'symbol))
           (looking-back "\\." (line-beginning-position)))
      (completion-at-point)
    (indent-for-tab-command)))

;;;###autoload
(defun clutch-mongodb-explain-query-at-point ()
  "Explain the current MongoDB find, findOne, or aggregate helper."
  (interactive)
  (clutch--ensure-connection)
  (pcase-let* ((`(,beg . ,end) (clutch--dwim-bounds-at-point))
               (query (string-trim
                       (buffer-substring-no-properties beg end))))
    (when (string-empty-p query)
      (user-error "No MongoDB query at point"))
    (let ((text (clutch-db-explain-query clutch-connection query)))
      (unless text
        (user-error "MongoDB explain unavailable for query"))
      (clutch--show-json-text-buffer "*clutch mongodb: explain*" text))))

;;;###autoload (autoload 'clutch-mongodb-dispatch "clutch-document" nil t)
(transient-define-prefix clutch-mongodb-dispatch ()
  "MongoDB query-console dispatch menu."
  [ :pad-keys t
   ["Connection"
    ("c" "Connect"       clutch-connect)
    ("q" "Query console" clutch-query-console)
    ("d" "Disconnect"    clutch-disconnect)]
   ["Execute"
    ("x" "DWIM"               clutch-execute-dwim)
    ("p" "Explain query"      clutch-mongodb-explain-query-at-point)
    ("r" "Region"             clutch-execute-region)
    ("b" "Buffer"             clutch-execute-buffer)
    ("k" "Copy agent context" clutch-copy-context-for-agent)]
   ["Objects"
    ("j" "Jump to object"   clutch-jump)
    ("D" "Describe object"  clutch-describe-dwim)
    ("o" "Object actions"   clutch-act-dwim)
    ("l" "Switch database"  clutch-switch-schema)
    ("s" "Refresh schema"   clutch-refresh-schema)]
   ["Completion"
    ("i" "Complete identifier" clutch-mongodb-complete-at-point)]])

(defun clutch-mongodb--install-query-keybindings (map)
  "Install MongoDB query-console key bindings into MAP."
  (clutch--install-query-keybindings map)
  (dolist (key '("C-c C-m" "C-c C-u" "C-c C-a"))
    (define-key map (kbd key) nil))
  (define-key map (kbd "C-c C-p") #'clutch-mongodb-explain-query-at-point)
  (define-key map (kbd "C-c ?") #'clutch-mongodb-dispatch)
  map)

(defvar clutch-mongodb-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map prog-mode-map)
    (clutch-mongodb--install-query-keybindings map)
    (define-key map (kbd "C-c TAB") #'clutch-mongodb-complete-at-point)
    (define-key map (kbd "C-c <tab>") #'clutch-mongodb-complete-at-point)
    (define-key map (kbd "TAB") #'clutch-mongodb-complete-or-indent)
    (define-key map (kbd "<tab>") #'clutch-mongodb-complete-or-indent)
    map)
  "Keymap for `clutch-mongodb-mode'.")

;;;###autoload
(define-derived-mode clutch-mongodb-mode prog-mode "clutch-mongodb"
  "Major mode for editing and executing MongoDB Shell and MQL queries.

\\<clutch-mongodb-mode-map>
Key bindings:
  \\[clutch-execute-dwim]	Execute region or statement/query at point
  \\[clutch-execute-region]	Execute region
  \\[clutch-execute-buffer]	Execute buffer
  \\[clutch-connect]	Connect to server
  \\[clutch-jump]	Object jump
  \\[clutch-describe-dwim]	Describe object
  \\[clutch-act-dwim]	Object actions
  \\[clutch-switch-schema]	Switch database
  \\[clutch-mongodb-complete-at-point]	Complete MongoDB shell identifier
  \\[clutch-mongodb-complete-or-indent]	Complete property or indent"
  :syntax-table clutch-mongodb-mode-syntax-table
  (setq-local comment-start "// ")
  (setq-local comment-end "")
  (setq-local comment-start-skip "\\(?://+\\|/\\*+\\)\\s-*")
  (setq-local font-lock-defaults '(clutch-mongodb-font-lock-keywords))
  (setq-local indent-line-function #'clutch-mongodb-indent-line)
  (clutch--query-mode-common-setup "clutch-mongodb")
  (clutch--install-mongodb-completion-capfs))

(provide 'clutch-document)

;;; clutch-document.el ends here
