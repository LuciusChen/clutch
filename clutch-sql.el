;;; clutch-sql.el --- SQL context, completion, eldoc, and xref -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; SQL statement context, table/alias extraction, completion, eldoc,
;; and xref support for Clutch SQL buffers.

;;; Code:

(require 'cl-lib)
(require 'seq)
(require 'subr-x)
(require 'xref)
(require 'clutch-backend)
(require 'clutch-schema)

(defvar clutch-connection)
(defvar clutch-sql-completion-case-style)
(defvar clutch--sql-keywords)

(defvar-local clutch--tables-in-buffer-cache nil
  "Cached result for `clutch--tables-in-buffer' in the current buffer.")

(defvar-local clutch--tables-in-query-cache nil
  "Cached result for `clutch--tables-in-query' in the current buffer.")

(defconst clutch--schema-inline-table-limit 3
  "Maximum number of statement tables for synchronous schema hints.")

(defconst clutch--schema-inline-min-prefix-length 2
  "Minimum symbol prefix length before loading column hints synchronously.")

(declare-function clutch--safe-completion-call "clutch-schema" (thunk))

;;;; SQL context, completion, eldoc, and xref

(defun clutch--eldoc-column-extras (col)
  "Return a space-joined string of constraint annotations for COL plist."
  (string-join
   (delq nil
         (list (when (not (plist-get col :nullable))
                 (propertize "NOT NULL" 'face 'font-lock-keyword-face))
               (when (plist-get col :primary-key)
                 (propertize "PK" 'face 'font-lock-builtin-face))
               (when-let* ((fk (plist-get col :foreign-key)))
                 (propertize (format "FK→%s.%s"
                                     (plist-get fk :ref-table)
                                     (plist-get fk :ref-column))
                             'face 'font-lock-constant-face))))
   "  "))

(defun clutch--eldoc-column-string (conn table col-name)
  "Format an eldoc string for COL-NAME in TABLE using CONN."
  (let* ((details (clutch--cached-column-details conn table))
         (col (and details
                   (cl-find col-name details
                            :key (lambda (d) (plist-get d :name))
                            :test (lambda (needle candidate)
                                    (string-equal (downcase needle)
                                                  (downcase candidate))))))
         (canonical-name (or (and col (plist-get col :name))
                             col-name))
         (header (concat (propertize table 'face 'font-lock-type-face)
                         "."
                         (propertize canonical-name
                                     'face 'font-lock-variable-name-face))))
    (unless details
      (clutch--ensure-column-details-async conn table))
    (if col
        (let ((type (plist-get col :type))
              (comment (plist-get col :comment))
              (extras (clutch--eldoc-column-extras col)))
          (string-join
           (delq nil (list header
                           (propertize type 'face 'font-lock-type-face)
                           (unless (string-empty-p extras) extras)
                           (when comment
                             (propertize (format "— %s" comment) 'face 'shadow))))
           "  "))
      header)))

(defun clutch--tables-in-buffer (schema)
  "Return table names from SCHEMA that appear in the current buffer."
  (let ((tick (buffer-chars-modified-tick)))
    (if (and clutch--tables-in-buffer-cache
             (eq (plist-get clutch--tables-in-buffer-cache :schema) schema)
             (= (plist-get clutch--tables-in-buffer-cache :tick) tick))
        (plist-get clutch--tables-in-buffer-cache :tables)
      (let ((seen (make-hash-table :test 'equal))
            text
            tables)
        (cl-labels
            ((identifier-start-p (ch)
               (and ch
                    (or (memq (char-syntax ch) '(?w ?_))
                        (memq ch '(?_ ?$ ?#)))))
             (identifier-char-p (ch)
               (and ch
                    (or (identifier-start-p ch)
                        (= ch ?.))))
             (record-token (token)
               (unless (string-empty-p token)
                 (puthash token t seen)
                 (puthash (downcase token) t seen)
                 (when (string-match "\\.\\([^.]+\\)\\'" token)
                   (puthash (match-string 1 token) t seen)
                   (puthash (downcase (match-string 1 token)) t seen)))))
          (save-excursion
            (goto-char (point-min))
            (while (not (eobp))
              (cond
               ((memq (char-after) '(?\" ?`))
                (let ((quote (char-after))
                      (beg (1+ (point))))
                  (forward-char)
                  (while (and (not (eobp))
                              (/= (char-after) quote))
                    (forward-char))
                  (record-token (buffer-substring-no-properties beg (point)))
                  (unless (eobp)
                    (forward-char))))
               ((identifier-start-p (char-after))
                (let ((beg (point)))
                  (while (identifier-char-p (char-after))
                    (forward-char))
                  (record-token (buffer-substring-no-properties
                                 beg (point)))))
               (t
                (forward-char))))))
        (maphash (lambda (tbl _cols)
                   (when (and
                          (stringp tbl)
                          (if (string-match-p
                               "\\`[[:alnum:]_$#]+\\(?:\\.[[:alnum:]_$#]+\\)*\\'"
                               tbl)
                              (gethash (downcase tbl) seen)
                           (let ((case-fold-search t))
                             (unless text
                               (setq text
                                     (buffer-substring-no-properties
                                      (point-min) (point-max))))
                             (string-match-p (regexp-quote tbl) text))))
                     (push tbl tables)))
                 schema)
        (setq tables (nreverse tables))
        (setq clutch--tables-in-buffer-cache
              (list :schema schema :tick tick :tables tables))
        tables))))

(defun clutch--statement-bounds ()
  "Return (BEG . END) for the SQL statement surrounding point."
  (let* ((text (buffer-substring-no-properties (point-min) (point-max)))
         (offset (- (point) (point-min)))
         (bounds (clutch-db-sql-context-statement-bounds text offset)))
    (cons (+ (point-min) (car bounds))
          (+ (point-min) (cdr bounds)))))

(defun clutch--compute-tables-in-query-cache (schema)
  "Return a fresh cache plist for table-name analysis on SCHEMA."
  (pcase-let* ((`(,beg . ,end) (clutch--statement-bounds))
               (tick (buffer-chars-modified-tick))
               (text (buffer-substring-no-properties beg end))
               (`(,found . ,aliases)
                (clutch--extract-tables-and-aliases text 0 (length text)))
               (statement-tables (delete-dups found)))
    (list :schema schema
          :tick tick
          :beg beg
          :end end
          :statement-tables statement-tables
          :statement-aliases aliases
          :tables (or statement-tables
                      (clutch--tables-in-buffer schema)))))

(defun clutch--tables-in-query-cache-entry (schema)
  "Return the cache entry for table analysis on SCHEMA, refreshing if needed."
  (pcase-let* ((`(,beg . ,end) (clutch--statement-bounds))
               (tick (buffer-chars-modified-tick))
               (cached clutch--tables-in-query-cache))
    (if (and cached
             (eq (plist-get cached :schema) schema)
             (= (plist-get cached :tick) tick)
             (= (plist-get cached :beg) beg)
             (= (plist-get cached :end) end))
        cached
      (setq clutch--tables-in-query-cache
            (clutch--compute-tables-in-query-cache schema)))))

(defun clutch--tables-in-current-statement (schema)
  "Return known table names mentioned in the current statement for SCHEMA."
  (plist-get (clutch--tables-in-query-cache-entry schema) :statement-tables))

(defun clutch--innermost-paren-range (text point-offset)
  "Return the innermost parenthesized range in TEXT.
The result is (BEG . END) containing POINT-OFFSET, or (0 . LEN) when
POINT-OFFSET is at the top level."
  (let* ((len (length text))
         (stack nil)
         (result (cons 0 len)))
    (clutch-db-sql-scan-code
     text 0 nil
     (lambda (pos ch _depth)
       (pcase ch
         (?\( (push pos stack))
         (?\) (when stack
                (let ((open (pop stack)))
                  (when (and (<= open point-offset)
                             (>= pos point-offset)
                             (< (- pos open) (- (cdr result) (car result))))
                    (setq result (cons (1+ open) pos)))))))
       nil))
    result))

(defun clutch--union-branch-range-in-scope (text point-offset scope)
  "Return the UNION branch range in TEXT for POINT-OFFSET within SCOPE.
SCOPE is a (BEG . END) pair in TEXT coordinates.  UNION / UNION ALL keywords
inside string literals, comments, or nested parentheses are ignored."
  (pcase-let* ((`(,scope-beg . ,scope-end) scope)
               (sub (substring text scope-beg scope-end))
               (sub-len (length sub))
               (sub-offset (max 0 (min sub-len (- point-offset scope-beg))))
               (effective-offset (if (and (> sub-len 0)
                                          (= sub-offset sub-len))
                                     (1- sub-offset)
                                   sub-offset))
               (boundaries (list 0))
               (case-fold-search t))
    (clutch-db-sql-scan-code
     sub 0 nil
     (lambda (pos _ch depth)
       (when (and (zerop depth)
                  (string-match "\\bunion\\b\\(?:[ \t\n\r]+all\\b\\)?" sub pos)
                  (= (match-beginning 0) pos))
         (push (match-beginning 0) boundaries)
         (push (match-end 0) boundaries))
       nil))
    (push sub-len boundaries)
    (setq boundaries (nreverse boundaries))
    (let ((beg 0)
          (end sub-len))
      (while boundaries
        (let ((boundary (pop boundaries)))
          (cond
           ((<= boundary effective-offset) (setq beg boundary))
           (t (setq end boundary boundaries nil)))))
      (cons (+ scope-beg beg) (+ scope-beg end)))))

(defun clutch--union-branch-range (text point-offset)
  "Return (BEG . END) of the UNION branch in TEXT containing POINT-OFFSET.
First narrows to the innermost parenthesized scope, then splits by
UNION / UNION ALL at depth 0 within that scope."
  (clutch--union-branch-range-in-scope
   text point-offset
   (clutch--innermost-paren-range text point-offset)))

(defun clutch--extract-tables-and-aliases (text beg end)
  "Extract tables and alias mappings from TEXT between BEG and END.
Return (TABLES . ALIASES) where TABLES is a list of table names and
ALIASES is an alist of (alias . table) pairs.
String literals and comments are ignored via masking."
  (let ((case-fold-search t)
        (masked (clutch-db-sql-mask-literal-or-comment text))
        (pos beg)
        tables aliases)
    (while (and (< pos end)
                (string-match
                 (rx word-start
                     (or (seq (group (or "from" "join" "update" "into"))
                              (+ (any " \t\n\r"))
                              (group (+ (any alnum "_$#.`\""))))
                         (seq (or (seq "truncate" (+ (any " \t\n\r")) "table")
                                  (seq "alter" (+ (any " \t\n\r")) "table")
                                  (seq "drop" (+ (any " \t\n\r")) "table"))
                              (+ (any " \t\n\r"))
                              (group (+ (any alnum "_$#.`\""))))
                         (seq "create" (+ (any " \t\n\r"))
                              (? (seq (or "unique" "fulltext" "spatial")
                                      (+ (any " \t\n\r"))))
                              "index"
                              (*? anything)
                              word-start "on"
                              (+ (any " \t\n\r"))
                              (group (+ (any alnum "_$#.`\""))))))
                 masked pos)
                (< (match-beginning 0) end))
      (let* ((dml-match (match-string 1 text))
             (table-end (or (match-end 2)
                            (match-end 3)
                            (match-end 4)))
             (table-token (and table-end
                               (or (match-string 2 text)
                                   (match-string 3 text)
                                   (match-string 4 text))))
             (table (clutch--normalize-statement-table-token table-token))
             (alias-consumed-end table-end))
        (setq pos table-end)
        (when (and dml-match
                   (string-match
                    "[ \t\n\r]+\\(?:as[ \t\n\r]+\\)?\\([[:alnum:]_$#`\"]+\\)"
                    masked table-end)
                   (= (match-beginning 0) table-end)
                   (< (match-beginning 0) end))
          (let ((alias-consumed-match-end (match-end 0)))
            (when-let* ((alias-token (match-string 1 text))
                        (alias (clutch--normalize-statement-table-token alias-token))
                        ((not (member (upcase alias) clutch--sql-keywords))))
              (setq alias-consumed-end alias-consumed-match-end)
              (push (cons alias table) aliases))))
        (setq pos alias-consumed-end)
        (when table (push table tables))))
    (cons (nreverse tables) (nreverse aliases))))

(defun clutch--table-aliases-in-current-statement (schema)
  "Return alias-to-table mappings for the UNION branch in SCHEMA containing point.
When the statement has no UNION, returns all aliases."
  (let* ((entry (clutch--tables-in-query-cache-entry schema))
         (all-aliases (plist-get entry :statement-aliases))
         (stmt-beg (plist-get entry :beg))
         (stmt-end (plist-get entry :end))
         (text (buffer-substring-no-properties stmt-beg stmt-end))
         (point-offset (- (point) stmt-beg))
         (range (clutch--union-branch-range text point-offset)))
    (if (and (= (car range) 0) (= (cdr range) (length text)))
        all-aliases
      (cdr (clutch--extract-tables-and-aliases text (car range) (cdr range))))))

(defun clutch--toplevel-union-branch-range (text point-offset)
  "Return (BEG . END) of the depth-0 UNION branch in TEXT containing POINT-OFFSET.
Unlike `clutch--union-branch-range', does not narrow into parenthesized
scopes first, so FROM/JOIN clauses remain visible from inside expressions."
  (clutch--union-branch-range-in-scope
   text point-offset
   (cons 0 (length text))))

(defun clutch--find-alias-in-range (text masked alias stmt-beg search-beg search-end)
  "Search for ALIAS definition in TEXT between SEARCH-BEG and SEARCH-END.
MASKED is the literal/comment-masked version of TEXT.
Returns the buffer position (offset by STMT-BEG), or nil."
  (let ((case-fold-search t)
        (pos search-beg))
    (catch 'found
      (while (and (< pos search-end)
                  (string-match
                   "\\b\\(from\\|join\\|update\\|into\\)[ \t\n\r]+\\([[:alnum:]_$#.`\"]+\\)"
                   masked pos))
        (when (>= (match-beginning 0) search-end)
          (throw 'found nil))
        (let ((table-end (match-end 2))
              (alias-pos nil))
          (setq pos table-end)
          (when (and (string-match
                      "[ \t\n\r]+\\(?:as[ \t\n\r]+\\)?\\(\"[^\"]+\"\\|`[^`]+`\\|[[:alnum:]_$#`\"]+\\)"
                      masked table-end)
                     (= (match-beginning 0) table-end)
                     (< (match-beginning 0) search-end))
            (let* ((token (match-string 1 text))
                   (normalized (clutch--normalize-statement-table-token token)))
              (when (and normalized
                         (not (member (upcase normalized) clutch--sql-keywords))
                         (string= (downcase normalized) (downcase alias)))
                (setq alias-pos (+ stmt-beg (match-beginning 1))))))
          (when alias-pos
            (throw 'found alias-pos))
          (setq pos (max pos (if (match-end 0) (match-end 0) (1+ pos)))))))))

(defun clutch--find-alias-definition-position (alias)
  "Return buffer position of ALIAS definition in the current statement.
First searches the innermost paren scope (subquery), then falls back to
the top-level UNION branch so expression parens like SUM(...) don't hide
outer FROM/JOIN clauses."
  (let* ((bounds (clutch--statement-bounds))
         (stmt-beg (car bounds))
         (text (buffer-substring-no-properties stmt-beg (cdr bounds)))
         (point-offset (- (point) stmt-beg))
         (masked (clutch-db-sql-mask-literal-or-comment text))
         (inner (clutch--union-branch-range text point-offset))
         (outer (clutch--toplevel-union-branch-range text point-offset)))
    (or (clutch--find-alias-in-range text masked alias stmt-beg
                                     (car inner) (cdr inner))
        (clutch--find-alias-in-range text masked alias stmt-beg
                                     (car outer) (cdr outer)))))

;;; xref backend — alias jump-to-definition

(defun clutch--xref-backend ()
  "Return `clutch' as xref backend in clutch SQL buffers.
Always claims the backend to prevent fallthrough to etags, which
triggers syntax_table errors in `sql-mode' derived buffers."
  (when clutch-connection 'clutch))

(defun clutch--xref-bare-identifier-char-p (ch)
  "Return non-nil when CH is part of an unquoted SQL identifier."
  (or (and (>= ch ?0) (<= ch ?9))
      (and (>= ch ?A) (<= ch ?Z))
      (and (>= ch ?a) (<= ch ?z))
      (memq ch '(?_ ?$ ?#))))

(defun clutch--xref-symbol-at-point ()
  "Return the SQL identifier at point without relying on syntax tables.
Returns (BEG . SYMBOL) or nil.  Uses SQL-aware token scanning so comments,
single-quoted strings, and multi-word quoted identifiers are handled
without consulting syntax tables."
  (pcase-let* ((`(,stmt-beg . ,stmt-end) (clutch--statement-bounds))
               (text (buffer-substring-no-properties stmt-beg stmt-end))
               (len (length text))
               (target (- (point) stmt-beg))
               (i 0))
    (when (and (>= target 0) (< target len))
      (catch 'hit
        (while (< i len)
          (if-let* ((skip (clutch-db-sql-skip-literal-or-comment text i)))
              (progn
                (when (and (<= i target) (< target skip))
                  (throw 'hit nil))
                (setq i skip))
            (let ((ch (aref text i)))
              (cond
               ((memq ch '(?\" ?`))
                (let* ((quote ch)
                       (end (or (cl-loop for j from (1+ i) below len
                                         when (= (aref text j) quote)
                                         return (1+ j))
                                len)))
                  (when (and (<= i target) (< target end))
                    (throw 'hit (cons (+ stmt-beg i)
                                      (substring text i end))))
                  (setq i end)))
               ((clutch--xref-bare-identifier-char-p ch)
                (let ((end (1+ i)))
                  (while (and (< end len)
                              (clutch--xref-bare-identifier-char-p
                               (aref text end)))
                    (cl-incf end))
                  (when (and (<= i target) (< target end))
                    (throw 'hit (cons (+ stmt-beg i)
                                      (substring text i end))))
                  (setq i end)))
               (t
                (cl-incf i))))))))))

(defun clutch--xref-qualified-identifier-qualifier (beg)
  "Return the qualifier immediately preceding identifier start BEG."
  (save-excursion
    (goto-char beg)
    (when (eq (char-before) ?.)
      (goto-char (max (point-min) (- beg 2)))
      (cdr (clutch--xref-symbol-at-point)))))

(defun clutch--xref-alias-at-point ()
  "Return the normalized statement alias at point, or nil.
Handles both bare aliases (`u') and qualified references (`u.name'), but does
not treat schema qualifiers in `schema.table' as aliases."
  (when-let* ((hit (clutch--xref-symbol-at-point))
              (beg (car hit))
              (sym (cdr hit)))
    (let* ((sym-alias (clutch--normalize-statement-table-token sym))
           (qualifier-alias
            (when-let* ((qualifier (clutch--xref-qualified-identifier-qualifier
                                    beg)))
              (clutch--normalize-statement-table-token qualifier))))
      (cond
       ((and qualifier-alias
             (clutch--find-alias-definition-position qualifier-alias))
        qualifier-alias)
       ((and sym-alias
             (clutch--find-alias-definition-position sym-alias))
        sym-alias)))))

(defun clutch--xref-source-table-at-point ()
  "Return the normalized source table at point, or nil."
  (when-let* ((hit (clutch--xref-symbol-at-point)))
    (pcase-let* ((`(,stmt-beg . ,stmt-end) (clutch--statement-bounds))
                 (text (buffer-substring-no-properties stmt-beg stmt-end))
                 (masked (clutch-db-sql-mask-literal-or-comment text))
                 (target (- (car hit) stmt-beg))
                 (case-fold-search t)
                 (pos 0))
      (catch 'found
        (while (string-match
                (rx word-start
                    (or "from" "join" "update" "into")
                    (+ (any " \t\n\r"))
                    (group (+ (any alnum "_$#.`\""))))
                masked pos)
          (let ((table-beg (match-beginning 1))
                (table-end (match-end 1)))
            (when (and (<= table-beg target) (< target table-end))
              (throw 'found
                     (clutch--normalize-statement-table-token
                      (match-string 1 text))))
            (setq pos (max table-end (1+ pos)))))))))

(cl-defmethod xref-backend-identifier-at-point ((_backend (eql 'clutch)))
  "Return the identifier at point, preferring normalized alias names."
  (or (clutch--xref-alias-at-point)
      (cdr (clutch--xref-symbol-at-point))
      ""))

(cl-defmethod xref-backend-definitions ((_backend (eql 'clutch)) identifier)
  "Return xref location of alias IDENTIFIER definition in the current statement."
  (if-let* ((pos (clutch--find-alias-definition-position identifier)))
      (list (xref-make (format "%s (alias)" identifier)
                       (xref-make-buffer-location (current-buffer) pos)))
    (when-let* ((table (clutch--xref-source-table-at-point))
                ((string-equal (downcase identifier) (downcase table))))
	      (user-error
	       "%s is already a source table in this statement; xref jumps SQL aliases only; use C-c C-d or C-c C-j for table lookup"
	       table))))

(cl-defmethod xref-backend-references ((_backend (eql 'clutch)) _identifier)
  "Not yet implemented."
  nil)

(defun clutch--tables-in-query (schema)
  "Return known table names for SCHEMA in the current statement.
This scans FROM/JOIN/UPDATE clauses in the SQL statement around point,
bounded by semicolons or blank lines.  Falls back to
`clutch--tables-in-buffer' when none are found."
  (plist-get (clutch--tables-in-query-cache-entry schema) :tables))

(defun clutch--identifier-match (identifier candidates)
  "Return canonical match for IDENTIFIER from string CANDIDATES, or nil.
Matching is case-insensitive so unquoted SQL identifiers still resolve when
buffer text and cached metadata differ only by case."
  (cl-find identifier candidates
           :test (lambda (needle candidate)
                   (string-equal (downcase needle)
                                 (downcase candidate)))))

(defun clutch--normalize-statement-table-token (token)
  "Normalize a raw table TOKEN parsed from SQL into a bare table name.
Handles schema-qualified names like \"HR\".\"EMPLOYEES\" or `db`.`table`."
  (when token
    (let* ((stripped (replace-regexp-in-string "[\"`]" "" token))
           (parts (split-string stripped "\\." t)))
      (car (last parts)))))

(defun clutch--statement-table-identifiers-in-sql (sql)
  "Return table identifiers referenced in SQL.
String literals and comments are ignored.  Returned names are normalized to
bare table identifiers because clutch metadata methods are scoped to the
current database/schema."
  (when (stringp sql)
    (delete-dups
     (car (clutch--extract-tables-and-aliases sql 0 (length sql))))))

(defun clutch--statement-table-identifiers ()
  "Return raw table identifiers referenced in the current statement."
  (pcase-let ((`(,beg . ,end) (clutch--statement-bounds)))
    (clutch--statement-table-identifiers-in-sql
     (buffer-substring-no-properties beg end))))

(defun clutch--qualified-identifier-qualifier (beg)
  "Return the qualifier token immediately preceding BEG, or nil.
For input like `u.name', returns `u' when BEG starts at `name'."
  (save-excursion
    (goto-char beg)
    (when (eq (char-before) ?.)
      (backward-char)
      (buffer-substring-no-properties
       (save-excursion
         (skip-chars-backward "[:alnum:]_$#`\"")
         (point))
       (point)))))

(defun clutch--qualified-identifier-table (schema beg)
  "Return the table referenced by the qualifier before BEG in SCHEMA, or nil.
Resolve both statement aliases like `u.name' and direct qualified table names
like `orders.id' within the current statement."
  (when-let* ((qualifier (and schema
                              (clutch--qualified-identifier-qualifier beg))))
    (or (cdr (assoc-string qualifier
                           (clutch--table-aliases-in-current-statement schema)
                           t))
        (let ((normalized (clutch--normalize-statement-table-token qualifier)))
          (when (member normalized
                        (or (clutch--tables-in-current-statement schema)
                            (clutch--statement-table-identifiers)))
            normalized)))))

(defconst clutch--sql-keywords
  '("SELECT" "FROM" "WHERE" "AND" "OR" "NOT" "IN" "IS" "NULL" "LIKE"
    "BETWEEN" "EXISTS" "CASE" "WHEN" "THEN" "ELSE" "END" "AS" "ON"
    "USING" "JOIN" "INNER" "LEFT" "RIGHT" "OUTER" "CROSS" "FULL"
    "INSERT" "INTO" "VALUES" "UPDATE" "SET" "DELETE"
    "CREATE" "ALTER" "DROP" "TABLE" "INDEX" "VIEW" "DATABASE"
    "GROUP" "BY" "ORDER" "ASC" "DESC" "HAVING" "LIMIT" "OFFSET"
    "PARTITION"
    "UNION" "ALL" "DISTINCT" "COUNT" "SUM" "AVG" "MIN" "MAX"
    "IF" "IFNULL" "COALESCE" "CAST" "CONCAT" "SUBSTRING"
    "PRIMARY" "KEY" "FOREIGN" "REFERENCES" "CONSTRAINT" "DEFAULT"
    "UNIQUE" "CHECK" "AUTO_INCREMENT"
    "TRUNCATE" "EXPLAIN" "SHOW" "DESCRIBE"
    "BEGIN" "COMMIT" "ROLLBACK" "TRANSACTION"
    "GRANT" "REVOKE" "WITH" "RECURSIVE" "TEMPORARY" "TEMP")
  "SQL reserved words used by completion and lightweight SQL parsing.")

(defconst clutch--sql-keyword-replacement-phrases
  '(("GROUP" . "GROUP BY")
    ("ORDER" . "ORDER BY")
    ("PARTITION" . "PARTITION BY")
    ("PRIMARY" . "PRIMARY KEY")
    ("FOREIGN" . "FOREIGN KEY"))
  "SQL keyword completions which replace a less useful first token.")

(defconst clutch--sql-keyword-additive-phrases
  '("INNER JOIN" "LEFT JOIN" "RIGHT JOIN" "CROSS JOIN"
    "UNION ALL"
    "IS NULL" "IS NOT NULL" "NOT NULL")
  "SQL keyword phrase completions offered alongside their first token.")

(defconst clutch--sql-function-docs
  (let ((ht (make-hash-table :test 'equal :size 160)))
    (dolist (entry
             '(;; Aggregate
               ("COUNT"        "COUNT(expr)"
                "Non-NULL row count; COUNT(*) for all rows")
               ("SUM"          "SUM(expr)"
                "Sum of non-NULL values")
               ("AVG"          "AVG(expr)"
                "Average of non-NULL values")
               ("MIN"          "MIN(expr)"
                "Minimum non-NULL value")
               ("MAX"          "MAX(expr)"
                "Maximum non-NULL value")
               ("GROUP_CONCAT" "GROUP_CONCAT([DISTINCT] expr [ORDER BY …] [SEPARATOR sep])"
                "Aggregate strings into one  [MySQL]")
               ("STRING_AGG"   "STRING_AGG(expr, sep [ORDER BY …])"
                "Aggregate strings into one  [PG]")
               ("ARRAY_AGG"    "ARRAY_AGG(expr [ORDER BY …])"
                "Aggregate values into array  [PG]")
               ("JSON_ARRAYAGG"  "JSON_ARRAYAGG(expr)"
                "Aggregate values into JSON array  [MySQL 8+/PG]")
               ("JSON_OBJECTAGG" "JSON_OBJECTAGG(key, val)"
                "Aggregate key-value pairs into JSON object  [MySQL 8+/PG]")
               ;; String
               ("CONCAT"       "CONCAT(str1, str2, …)"
                "Concatenate strings (NULL-safe variant: CONCAT_WS)  [MySQL]")
               ("CONCAT_WS"    "CONCAT_WS(sep, str1, str2, …)"
                "Concatenate with separator, skipping NULLs  [MySQL]")
               ("SUBSTRING"    "SUBSTRING(str, pos [, len])"
                "Extract substring; also SUBSTRING(str FROM pos FOR len)")
               ("SUBSTR"       "SUBSTR(str, pos [, len])"
                "Alias for SUBSTRING")
               ("LEFT"         "LEFT(str, len)"
                "Leftmost len characters")
               ("RIGHT"        "RIGHT(str, len)"
                "Rightmost len characters")
               ("LENGTH"       "LENGTH(str)"
                "Byte length  [MySQL]; character length in PG — use CHAR_LENGTH for characters")
               ("CHAR_LENGTH"  "CHAR_LENGTH(str)"
                "Number of characters in string")
               ("UPPER"        "UPPER(str)"
                "Convert string to uppercase")
               ("LOWER"        "LOWER(str)"
                "Convert string to lowercase")
               ("TRIM"         "TRIM([[BOTH|LEADING|TRAILING] [remstr] FROM] str)"
                "Remove leading/trailing characters (default: spaces)")
               ("LTRIM"        "LTRIM(str)"
                "Remove leading spaces")
               ("RTRIM"        "RTRIM(str)"
                "Remove trailing spaces")
               ("REPLACE"      "REPLACE(str, from_str, to_str)"
                "Replace all occurrences of from_str with to_str")
               ("INSTR"        "INSTR(str, substr)"
                "1-based position of first substr occurrence  [MySQL]")
               ("POSITION"     "POSITION(substr IN str)"
                "1-based position of first substr occurrence")
               ("STRPOS"       "STRPOS(str, substr)"
                "1-based position of first substr occurrence  [PG]")
               ("LOCATE"       "LOCATE(substr, str [, pos])"
                "Position of substr starting from pos  [MySQL]")
               ("LPAD"         "LPAD(str, len [, padstr])"
                "Left-pad string to length len")
               ("RPAD"         "RPAD(str, len [, padstr])"
                "Right-pad string to length len")
               ("REPEAT"       "REPEAT(str, n)"
                "Repeat string n times")
               ("REVERSE"      "REVERSE(str)"
                "Reverse a string")
               ("SPLIT_PART"   "SPLIT_PART(str, delim, n)"
                "n-th field after splitting on delim  [PG]")
               ("REGEXP_REPLACE" "REGEXP_REPLACE(str, pattern, repl [, flags])"
                "Replace regex matches in string")
               ("REGEXP_LIKE"  "REGEXP_LIKE(str, pattern [, match_type])"
                "TRUE if str matches regex pattern  [MySQL 8+]")
               ("CHR"          "CHR(n)"
                "Character from integer code point  [PG]")
               ("ASCII"        "ASCII(str)"
                "ASCII code of first character")
               ("HEX"          "HEX(str_or_num)"
                "Hexadecimal representation  [MySQL]")
               ("UNHEX"        "UNHEX(hex_str)"
                "Decode hex string to binary  [MySQL]")
               ;; Date / time
               ("NOW"          "NOW()"
                "Current date and time")
               ("CURRENT_TIMESTAMP" "CURRENT_TIMESTAMP"
                "Current date and time")
               ("CURDATE"      "CURDATE()"
                "Current date  [MySQL]")
               ("CURRENT_DATE" "CURRENT_DATE"
                "Current date")
               ("CURTIME"      "CURTIME()"
                "Current time  [MySQL]")
               ("DATE"         "DATE(expr)"
                "Extract date part from datetime  [MySQL]")
               ("TIME"         "TIME(expr)"
                "Extract time part from datetime  [MySQL]")
               ("DATE_FORMAT"  "DATE_FORMAT(date, format)"
                "Format date using strftime-like format  [MySQL]")
               ("TO_CHAR"      "TO_CHAR(val, fmt)"
                "Format date or number as string  [PG]")
               ("TO_DATE"      "TO_DATE(str, fmt)"
                "Parse string to date  [PG]")
               ("TO_TIMESTAMP" "TO_TIMESTAMP(str, fmt)"
                "Parse string to timestamp  [PG]")
               ("STR_TO_DATE"  "STR_TO_DATE(str, format)"
                "Parse string to date/time  [MySQL]")
               ("DATE_ADD"     "DATE_ADD(date, INTERVAL n unit)"
                "Add interval to date  [MySQL]")
               ("DATE_SUB"     "DATE_SUB(date, INTERVAL n unit)"
                "Subtract interval from date  [MySQL]")
               ("DATEDIFF"     "DATEDIFF(date1, date2)"
                "Days between date1 and date2 (date1 − date2)  [MySQL]")
               ("TIMESTAMPDIFF" "TIMESTAMPDIFF(unit, dt1, dt2)"
                "Difference in unit between dt1 and dt2  [MySQL]")
               ("EXTRACT"      "EXTRACT(unit FROM date)"
                "Extract field: YEAR MONTH DAY HOUR MINUTE SECOND …")
               ("YEAR"         "YEAR(date)"
                "Year part of date (1000–9999)")
               ("MONTH"        "MONTH(date)"
                "Month part of date (1–12)")
               ("DAY"          "DAY(date)"
                "Day part of date (1–31)")
               ("HOUR"         "HOUR(time)"
                "Hour part (0–23)")
               ("MINUTE"       "MINUTE(time)"
                "Minute part (0–59)")
               ("SECOND"       "SECOND(time)"
                "Second part (0–59)")
               ("UNIX_TIMESTAMP" "UNIX_TIMESTAMP([date])"
                "Seconds since 1970-01-01 UTC  [MySQL]")
               ("FROM_UNIXTIME" "FROM_UNIXTIME(ts [, format])"
                "Convert Unix timestamp to datetime  [MySQL]")
               ("CONVERT_TZ"   "CONVERT_TZ(dt, from_tz, to_tz)"
                "Convert datetime between timezones  [MySQL]")
               ("AGE"          "AGE(ts1 [, ts2])"
                "Interval between timestamps  [PG]")
               ;; Numeric
               ("ABS"          "ABS(x)"
                "Absolute value")
               ("CEIL"         "CEIL(x)"
                "Smallest integer ≥ x")
               ("CEILING"      "CEILING(x)"
                "Smallest integer ≥ x  [MySQL]")
               ("FLOOR"        "FLOOR(x)"
                "Largest integer ≤ x")
               ("ROUND"        "ROUND(x [, d])"
                "Round x to d decimal places (default 0)")
               ("TRUNCATE"     "TRUNCATE(x, d)"
                "Truncate x to d decimal places  [MySQL]")
               ("TRUNC"        "TRUNC(x [, d])"
                "Truncate x to d decimal places  [PG]")
               ("MOD"          "MOD(x, y)"
                "Remainder of x / y  (also: x % y)")
               ("POWER"        "POWER(x, y)"
                "x raised to the power y")
               ("POW"          "POW(x, y)"
                "x raised to the power y  [MySQL]")
               ("SQRT"         "SQRT(x)"
                "Square root of x")
               ("EXP"          "EXP(x)"
                "e raised to the power x")
               ("LN"           "LN(x)"
                "Natural logarithm of x")
               ("LOG"          "LOG([base, ] x)"
                "Logarithm of x (base e or specified base)")
               ("LOG2"         "LOG2(x)"
                "Base-2 logarithm  [MySQL]")
               ("LOG10"        "LOG10(x)"
                "Base-10 logarithm")
               ("SIGN"         "SIGN(x)"
                "-1, 0, or 1 depending on sign of x")
               ("GREATEST"     "GREATEST(val1, val2, …)"
                "Largest value among arguments")
               ("LEAST"        "LEAST(val1, val2, …)"
                "Smallest value among arguments")
               ("RAND"         "RAND([seed])"
                "Random float in [0, 1)  [MySQL]")
               ("RANDOM"       "RANDOM()"
                "Random float in [0, 1)  [PG]")
               ("PI"           "PI()"
                "Value of π (3.141593)")
               ;; Conditional / null-handling
               ("IF"           "IF(cond, true_val, false_val)"
                "Return true_val if cond is true, else false_val  [MySQL]")
               ("IFNULL"       "IFNULL(expr, alt)"
                "Return alt if expr is NULL  [MySQL]")
               ("NULLIF"       "NULLIF(expr1, expr2)"
                "Return NULL if expr1 = expr2, else expr1")
               ("COALESCE"     "COALESCE(val1, val2, …)"
                "First non-NULL value in list")
               ("NVL"          "NVL(expr, alt)"
                "Return alt if expr is NULL  (Oracle-compatible)")
               ;; Type conversion
               ("CAST"         "CAST(expr AS type)"
                "Explicit type conversion")
               ("CONVERT"      "CONVERT(expr, type) or CONVERT(expr USING charset)"
                "Convert type or character set  [MySQL]")
               ;; Window functions
               ("ROW_NUMBER"   "ROW_NUMBER() OVER (…)"
                "Sequential row number within partition (no ties)")
               ("RANK"         "RANK() OVER (…)"
                "Rank with gaps on ties")
               ("DENSE_RANK"   "DENSE_RANK() OVER (…)"
                "Rank without gaps on ties")
               ("NTILE"        "NTILE(n) OVER (…)"
                "Divide rows into n ranked buckets")
               ("PERCENT_RANK" "PERCENT_RANK() OVER (…)"
                "Relative rank: (rank − 1) / (rows − 1)")
               ("CUME_DIST"    "CUME_DIST() OVER (…)"
                "Cumulative distribution of row within partition")
               ("LAG"          "LAG(expr [, n [, default]]) OVER (…)"
                "Value from n rows before current row")
               ("LEAD"         "LEAD(expr [, n [, default]]) OVER (…)"
                "Value from n rows after current row")
               ("FIRST_VALUE"  "FIRST_VALUE(expr) OVER (…)"
                "First value in window frame")
               ("LAST_VALUE"   "LAST_VALUE(expr) OVER (…)"
                "Last value in window frame")
               ("NTH_VALUE"    "NTH_VALUE(expr, n) OVER (…)"
                "n-th value in window frame  [PG/MySQL 8+]")
               ;; JSON
               ("JSON_EXTRACT" "JSON_EXTRACT(json, path)"
                "Extract value at JSON path  [MySQL]  (also: json->>'$.key')")
               ("JSON_UNQUOTE" "JSON_UNQUOTE(json_val)"
                "Remove quoting from JSON string value  [MySQL]")
               ("JSON_OBJECT"  "JSON_OBJECT(key, val, …)"
                "Create JSON object  [MySQL]")
               ("JSON_ARRAY"   "JSON_ARRAY(val, …)"
                "Create JSON array  [MySQL]")
               ("JSON_CONTAINS" "JSON_CONTAINS(target, candidate [, path])"
                "TRUE if target contains candidate  [MySQL]")
               ;; Misc / info
               ("DATABASE"     "DATABASE()"
                "Current database name  [MySQL]")
               ("CURRENT_DATABASE" "CURRENT_DATABASE()"
                "Current database name  [PG]")
               ("USER"         "USER()"
                "Current user as user@host  [MySQL]")
               ("CURRENT_USER" "CURRENT_USER"
                "Current authenticated user")
               ("VERSION"      "VERSION()"
                "Server version string")
               ("LAST_INSERT_ID" "LAST_INSERT_ID([expr])"
                "Auto-increment ID from last INSERT  [MySQL]")
               ("ROW_COUNT"    "ROW_COUNT()"
                "Rows affected by last DML statement  [MySQL]")
               ("UUID"         "UUID()"
                "Generate a version-1 UUID  [MySQL]")
               ("SLEEP"        "SLEEP(n)"
                "Sleep n seconds  [MySQL]")
               ;; Clauses / keywords with syntax notes
               ("EXPLAIN"      "EXPLAIN [ANALYZE] query"
                "Show query execution plan")
               ("BETWEEN"      "expr BETWEEN low AND high"
                "Inclusive range test — equivalent to low ≤ expr ≤ high")
               ("EXISTS"       "EXISTS (subquery)"
                "TRUE if subquery returns at least one row")
               ("LIKE"         "str LIKE pattern"
                "Pattern match: % = any sequence, _ = exactly one character")
               ("ILIKE"        "str ILIKE pattern"
                "Case-insensitive pattern match  [PG]")
               ("REGEXP"       "str REGEXP pattern"
                "Regular expression match  [MySQL]")
               ("RLIKE"        "str RLIKE pattern"
                "Alias for REGEXP  [MySQL]")
               ("OVER"         "OVER ([PARTITION BY …] [ORDER BY …] [ROWS|RANGE frame])"
                "Window function clause")
               ("PARTITION"    "PARTITION BY col1, col2, …"
                "Divide rows into groups for window functions")
               ("WITH"         "WITH name [(cols)] AS (subquery) SELECT …"
                "Common Table Expression (CTE); prefix WITH RECURSIVE for recursive CTEs")
               ("RETURNING"    "INSERT/UPDATE/DELETE … RETURNING col, …"
                "Return values of modified rows  [PG]")
               ;; CASE expression
               ("CASE"         "CASE WHEN cond THEN val … [ELSE default] END"
                "Conditional expression; simple form: CASE expr WHEN val THEN res … [ELSE def] END")
               ("WHEN"         "WHEN condition THEN result"
                "Branch condition inside CASE expression")
               ("THEN"         "THEN result"
                "Result value for a matched CASE/WHEN branch")
               ("ELSE"         "ELSE default"
                "Fallback value when no CASE/WHEN branch matches")
               ("END"          "END"
                "Terminates a CASE expression")
               ;; Membership / set
               ("IN"           "expr IN (val1, val2, …) or expr IN (subquery)"
                "TRUE if expr equals any value in the list or subquery")
               ("NOT"          "NOT expr"
                "Logical negation")
               ("ANY"          "expr op ANY (subquery)"
                "TRUE if comparison holds for at least one subquery row")
               ("ALL"          "expr op ALL (subquery)"
                "TRUE if comparison holds for every subquery row")
               ;; JOIN keywords
               ("JOIN"         "table JOIN other ON condition"
                "INNER JOIN — return rows with matches in both tables")
               ("INNER"        "INNER JOIN table ON condition"
                "Return only rows with matches in both tables (default JOIN)")
               ("LEFT"         "LEFT [OUTER] JOIN table ON condition"
                "Return all left rows; NULL-fill unmatched right rows")
               ("RIGHT"        "RIGHT [OUTER] JOIN table ON condition"
                "Return all right rows; NULL-fill unmatched left rows")
               ("FULL"         "FULL [OUTER] JOIN table ON condition"
                "Return all rows from both sides, NULL-fill unmatched  [PG]")
               ("CROSS"        "CROSS JOIN table"
                "Cartesian product of both tables — no ON clause")
               ("ON"           "ON condition"
                "Join condition: ON t1.col = t2.col")
               ("USING"        "USING (col1, col2, …)"
                "Join on identically-named columns; equivalent to ON t1.col = t2.col")
               ;; Set operations
               ("UNION"        "query UNION [ALL] query"
                "Combine rows; ALL keeps duplicates; without ALL deduplicates")
               ("INTERSECT"    "query INTERSECT [ALL] query"
                "Rows present in both result sets  [PG/MySQL 8.0.31+]")
               ("EXCEPT"       "query EXCEPT [ALL] query"
                "Rows in first set not in second  [PG]; MySQL: EXCEPT")
               ("MINUS"        "query MINUS query"
                "Rows in first set not in second (Oracle/older MySQL synonym for EXCEPT)")
               ;; DML clause keywords
               ("INTO"         "INSERT INTO table (cols) VALUES (…)"
                "Target table for INSERT")
               ("VALUES"       "VALUES (val1, val2, …) [, (…)]"
                "Row value list for INSERT")
               ("SET"          "UPDATE table SET col = val, …"
                "Assignment list for UPDATE")
               ("FROM"         "FROM table [alias] [JOIN …]"
                "Source table(s) for SELECT / DELETE")
               ("WHERE"        "WHERE condition"
                "Filter rows; applied before GROUP BY")
               ("GROUP"        "GROUP BY col1, col2, …"
                "Aggregate rows into groups")
               ("HAVING"       "HAVING condition"
                "Filter groups after GROUP BY; may reference aggregates")
               ("ORDER"        "ORDER BY col [ASC|DESC] [NULLS FIRST|LAST]"
                "Sort result rows")
               ("LIMIT"        "LIMIT n [OFFSET m]"
                "Return at most n rows, skip m rows")
               ("OFFSET"       "OFFSET n"
                "Skip n rows before returning results")
               ("DISTINCT"     "SELECT DISTINCT col, …"
                "Eliminate duplicate rows from result set")
               ("ASC"          "ORDER BY col ASC"
                "Sort ascending (default)")
               ("DESC"         "ORDER BY col DESC"
                "Sort descending")
               ("NULLS"        "ORDER BY col NULLS FIRST|LAST"
                "Control NULL sort position  [PG/MySQL 8+]")))
      (puthash (car entry)
               (list :sig (cadr entry) :desc (caddr entry))
               ht))
    ht)
  "Hash table mapping uppercase SQL function/keyword names to doc plists.
Each value is a plist (:sig SIGNATURE :desc DESCRIPTION).")

(defun clutch--eldoc-keyword-string (sym)
  "Return an eldoc string for SQL keyword/function SYM, or nil."
  (when-let* ((doc (gethash (upcase sym) clutch--sql-function-docs))
              (sig  (plist-get doc :sig))
              (desc (plist-get doc :desc)))
    (concat (propertize sig  'face 'font-lock-function-name-face)
            (propertize (concat "  — " desc) 'face 'shadow))))

(defun clutch--completion-finished-status-p (status)
  "Return non-nil when completion STATUS means candidate was accepted."
  (memq status '(finished exact sole)))

(defun clutch--apply-sql-completion-case-style (text)
  "Return TEXT transformed by `clutch-sql-completion-case-style'."
  (pcase clutch-sql-completion-case-style
    ('lower (downcase text))
    ('upper (upcase text))
    (_ text)))

(defun clutch--sql-keyword-completion-raw-candidates ()
  "Return raw SQL keyword completion candidates before case conversion."
  (let ((replaced (mapcar #'car clutch--sql-keyword-replacement-phrases))
        (zero-arg-functions nil))
    (maphash
     (lambda (_name doc)
       (let ((sig (plist-get doc :sig)))
         (when (and (stringp sig)
                    (string-match-p "\\`[[:upper:]_]+()[[:space:]]*\\'" sig))
           (push (string-trim sig) zero-arg-functions))))
     clutch--sql-function-docs)
    (delete-dups
     (append
      (mapcar #'cdr clutch--sql-keyword-replacement-phrases)
      clutch--sql-keyword-additive-phrases
      zero-arg-functions
      (seq-remove (lambda (keyword) (member keyword replaced))
                  clutch--sql-keywords)))))

(defun clutch--sql-completion-insert-space-p (candidate)
  "Return non-nil when accepting CANDIDATE should add a trailing space."
  (not (string-suffix-p ")" candidate)))

(defun clutch--sql-keyword-completion-candidates ()
  "Return SQL keyword completion candidates honoring case style."
  (mapcar #'clutch--apply-sql-completion-case-style
          (clutch--sql-keyword-completion-raw-candidates)))

(defun clutch--sql-keyword-completion-candidate-p (text)
  "Return non-nil when TEXT is a SQL keyword completion candidate."
  (member (upcase text) (clutch--sql-keyword-completion-raw-candidates)))

(defun clutch--sql-identifier-completion-candidates (candidates)
  "Return completion CANDIDATES honoring identifier case style."
  (delete-dups
   (mapcar #'clutch--apply-sql-completion-case-style candidates)))

(defun clutch--sql-keyword-prefix-p (prefix)
  "Return non-nil when PREFIX matches the start of any SQL keyword."
  (let ((upcase-prefix (upcase prefix)))
    (seq-some (lambda (keyword)
                (string-prefix-p upcase-prefix keyword))
              (clutch--sql-keyword-completion-raw-candidates))))


(defun clutch-sql-keyword-completion-at-point ()
  "Completion-at-point function for SQL keywords.
Works without a database connection."
  (when-let* ((bounds (bounds-of-thing-at-point 'symbol)))
    (list (car bounds) (cdr bounds)
          (completion-table-case-fold
           (clutch--sql-keyword-completion-candidates))
          :exclusive 'no
          :exit-function (lambda (str status)
                           (when (and (clutch--completion-finished-status-p status)
                                      (clutch--sql-completion-insert-space-p str)
                                      (not (looking-at-p "\\s-")))
                             (insert " "))))))

(defun clutch--install-completion-capfs ()
  "Install completion CAPFs for the current buffer in priority order.
Identifier completion must run before SQL keyword completion so table names in
contexts like FROM/JOIN are not shadowed by keywords such as ORDER."
  (remove-hook 'completion-at-point-functions
               #'clutch-completion-at-point t)
  (remove-hook 'completion-at-point-functions
               #'clutch-sql-keyword-completion-at-point t)
  (add-hook 'completion-at-point-functions
            #'clutch-completion-at-point nil t)
  (add-hook 'completion-at-point-functions
            #'clutch-sql-keyword-completion-at-point t t)
  (add-hook 'corfu-mode-hook
            #'clutch--install-completion-capfs nil t))

(defun clutch--completion-table-context-p (beg)
  "Return non-nil when BEG is in a SQL table-name completion context."
  (string-match-p
   "\\b\\(FROM\\|JOIN\\|INTO\\|UPDATE\\|TABLE\\|DESCRIBE\\|DESC\\)\\s-+\\S-*\\'"
   (upcase (buffer-substring-no-properties (line-beginning-position) beg))))

(defun clutch--completion-context-tables (schema qualified-table prefix-len
                                                 table-context-p busy
                                                 &optional force-columns-p)
  "Return tables from SCHEMA whose columns are useful completion candidates.
QUALIFIED-TABLE narrows completion after an explicit qualifier.  PREFIX-LEN,
TABLE-CONTEXT-P, BUSY, and FORCE-COLUMNS-P decide whether column loading is
appropriate."
  (unless (or table-context-p busy
              (and (not qualified-table)
                   (not force-columns-p)
                   (< prefix-len clutch--schema-inline-min-prefix-length)))
    (let ((tables (or (and qualified-table (list qualified-table))
                      (and schema (clutch--tables-in-current-statement schema))
                      (clutch--statement-table-identifiers))))
      (when (and tables
                 (or qualified-table
                     (<= (length tables) clutch--schema-inline-table-limit)))
        tables))))

(defun clutch--completion-column-values (conn schema table prefix sync-columns-p)
  "Return raw column names for TABLE on CONN in completion context.
SCHEMA supplies cached columns.  PREFIX is passed to direct backend completion
when SYNC-COLUMNS-P is nil."
  (if sync-columns-p
      (or (clutch--cached-columns schema table)
          (and schema (clutch--ensure-columns conn schema table)))
    (or (clutch--cached-columns schema table)
        (clutch--safe-completion-call
         (lambda ()
           (clutch-db-complete-columns conn table prefix))))))

(defun clutch--completion-column-candidates (conn schema tables prefix
                                                  sync-columns-p)
  "Return column completion candidates for TABLES on CONN.
SCHEMA supplies cached columns.  PREFIX and SYNC-COLUMNS-P control backend
loading."
  (let (all)
    (dolist (tbl tables)
      (when-let* ((cols (clutch--completion-column-values
                         conn schema tbl prefix sync-columns-p)))
        (setq all (nconc all (copy-sequence cols)))))
    (clutch--sql-identifier-completion-candidates all)))

(defun clutch--completion-qualified-empty-prefix-bounds ()
  "Return point bounds for completion after a qualifier dot, or nil."
  (when-let* ((qualifier (clutch--qualified-identifier-qualifier (point)))
              ((not (string-empty-p qualifier))))
    (cons (point) (point))))

(defun clutch--completion-table-sources (schema tables)
  "Return visible table sources from SCHEMA for TABLES in the current statement."
  (let* ((aliases (and schema (clutch--table-aliases-in-current-statement schema)))
         (aliased-tables (mapcar #'cdr aliases))
         sources)
    (dolist (alias aliases)
      (when (and (car alias) (cdr alias))
        (push (list :qualifier (car alias)
                    :table (cdr alias))
              sources)))
    (dolist (table tables)
      (unless (member table aliased-tables)
        (push (list :qualifier table :table table)
              sources)))
    (nreverse sources)))

(defun clutch--completion-column-source-annotation (qualifier table)
  "Return annotation text for a column from QUALIFIER and TABLE."
  (concat
   (propertize "  " 'face 'shadow)
   (propertize (if (string= qualifier table)
                   table
                 (format "%s (%s)" qualifier table))
               'face 'clutch-object-source-face)))

(defun clutch--completion-visible-column-candidates
    (conn schema tables prefix sync-columns-p)
  "Return (CANDIDATES . ANNOTATION-FN) for visible columns on CONN.
SCHEMA and TABLES supply current statement tables.  PREFIX and SYNC-COLUMNS-P
control backend column loading."
  (let* ((sources (and tables (clutch--completion-table-sources schema tables)))
         (qualify-p (> (length sources) 1))
         (annotations (make-hash-table :test 'equal))
         candidates)
    (dolist (source sources)
      (let* ((qualifier (plist-get source :qualifier))
             (table (plist-get source :table)))
        (dolist (column (clutch--completion-column-values
                         conn schema table prefix sync-columns-p))
          (let* ((styled-column
                  (clutch--apply-sql-completion-case-style column))
                 (styled-qualifier
                  (clutch--apply-sql-completion-case-style qualifier))
                 (candidate (if qualify-p
                                (concat styled-qualifier "." styled-column)
                              styled-column)))
            (puthash candidate
                     (clutch--completion-column-source-annotation qualifier table)
                     annotations)
            (push candidate candidates)))))
    (setq candidates (delete-dups (nreverse candidates)))
    (when candidates
      (cons candidates
            (lambda (candidate)
              (gethash candidate annotations))))))

(defun clutch--completion-top-level-token-before (sql offset)
  "Return the last top-level SQL token in SQL before OFFSET."
  (let ((case-fold-search t)
        token)
    (clutch-db-sql-scan-code
     sql 0 (min offset (length sql))
     (lambda (pos _ch depth)
       (when (and (zerop depth)
                  (string-match
                   (rx word-start
                       (group
                        (or "select" "from" "where" "having" "on" "join"
                            "into" "update" "set" "values" "limit" "offset"
                            "fetch" "for"
                            (seq "group" (+ (any " \t\n\r")) "by")
                            (seq "order" (+ (any " \t\n\r")) "by")))
                       word-end)
                   sql pos)
                  (= (match-beginning 0) pos)
                  (<= (match-end 0) offset))
         (setq token (replace-regexp-in-string
                      "[ \t\n\r]+" " "
                      (upcase (match-string 1 sql)))))
       nil))
    token))

(defun clutch--completion-select-list-ready-p (sql start offset)
  "Return non-nil when OFFSET is an empty SELECT-list slot in SQL after START."
  (let ((pos start)
        (depth 0)
        significant)
    (while (< pos offset)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (if (> skip offset)
              (setq significant 'other
                    pos offset)
            (setq pos skip))
        (let ((ch (aref sql pos)))
          (cond
           ((memq ch '(?\s ?\t ?\n ?\r))
            (cl-incf pos))
           ((= ch ?\()
            (when (zerop depth)
              (setq significant 'other))
            (cl-incf depth)
            (cl-incf pos))
           ((= ch ?\))
            (when (zerop depth)
              (setq significant 'other))
            (setq depth (max 0 (1- depth)))
            (cl-incf pos))
           ((zerop depth)
            (setq significant (if (= ch ?,) 'comma 'other))
            (cl-incf pos))
           (t
            (cl-incf pos))))))
    (and (zerop depth)
         (memq significant '(nil comma)))))

(defun clutch--completion-select-list-empty-bounds ()
  "Return point bounds for empty SELECT-list completion, or nil."
  (pcase-let* ((`(,beg . ,end) (clutch--statement-bounds))
               (sql (buffer-substring-no-properties beg end))
               (offset (- (point) beg))
               (select-pos (clutch-db-sql-find-top-level-clause sql "SELECT"))
               (select-end (and select-pos (+ select-pos 6)))
               (from-pos (and select-end
                              (clutch-db-sql-find-top-level-clause
                               sql "FROM" select-end))))
    (when (and select-end from-pos
               (<= select-end offset)
               (< offset from-pos)
               (clutch--completion-select-list-ready-p sql select-end offset))
      (cons (point) (point)))))

(defun clutch--completion-empty-column-bounds ()
  "Return point bounds for empty-slot column completion, or nil."
  (when (not (clutch--completion-table-context-p (point)))
    (pcase-let* ((`(,beg . ,end) (clutch--statement-bounds))
                 (sql (buffer-substring-no-properties beg end))
                 (offset (- (point) beg))
                 (token (clutch--completion-top-level-token-before sql offset)))
      (when (or (and (equal token "SELECT")
                     (clutch--completion-select-list-empty-bounds))
                (member token '("WHERE" "HAVING" "GROUP BY" "ORDER BY" "ON")))
        (cons (point) (point))))))

(defun clutch-complete-at-point ()
  "Complete SQL identifiers at point."
  (interactive)
  (let ((capf (or (clutch-completion-at-point)
                  (clutch-sql-keyword-completion-at-point))))
    (pcase capf
      (`(,beg ,end ,collection . ,plist)
       (let ((completion-extra-properties plist)
             (completion-in-region-mode-predicate #'always))
         (unless (completion-in-region beg end collection
                                       (plist-get plist :predicate))
           (user-error "No completion at point"))))
      (_
       (user-error "No completion at point")))))

(defun clutch-completion-at-point ()
  "Completion-at-point function for SQL identifiers.
Skips column loading if the connection is busy (prevents re-entrancy
when completion triggers during an in-flight query)."
  (let* ((symbol-bounds (bounds-of-thing-at-point 'symbol))
         (qualified-bounds (clutch--completion-qualified-empty-prefix-bounds))
         (empty-column-bounds (and (not symbol-bounds)
                                   (not qualified-bounds)
                                   (clutch--completion-empty-column-bounds)))
         (bounds (or symbol-bounds
                     qualified-bounds
                     empty-column-bounds)))
    (when-let* ((conn clutch-connection)
                (bounds bounds))
      (let* ((beg (car bounds))
           (end (cdr bounds))
           (prefix (buffer-substring-no-properties beg end))
           (prefix-len (- end beg))
           (qualified-empty-prefix-p (and qualified-bounds (not symbol-bounds)))
           (empty-column-prefix-p (and empty-column-bounds t))
           (schema (clutch--schema-for-connection))
           (qualifier (and schema
                           (clutch--qualified-identifier-qualifier beg)))
           (table-context-p (clutch--completion-table-context-p beg))
           (busy (clutch-db-busy-p conn))
           (sync-columns-p (clutch-db-completion-sync-columns-p conn))
           (direct-table-candidates
            (when (and table-context-p
                       (>= prefix-len clutch--schema-inline-min-prefix-length))
              (or (clutch--schema-table-candidates conn schema prefix)
                  (clutch--safe-completion-call
                   (lambda () (clutch-db-complete-tables conn prefix))))))
           (qualified-table (and qualifier
                                 (clutch--qualified-identifier-table schema beg)))
           (context-tables
            (clutch--completion-context-tables
             schema qualified-table prefix-len table-context-p busy
             empty-column-prefix-p))
           (empty-column-result
            (and empty-column-prefix-p
                 context-tables
                 (clutch--completion-visible-column-candidates
                  conn schema context-tables prefix sync-columns-p)))
           (annotation-function (cdr-safe empty-column-result))
           (keyword-candidates
            (and (not empty-column-prefix-p)
                 (not qualified-empty-prefix-p)
                 context-tables
                 (not table-context-p)
                 (clutch--sql-keyword-prefix-p prefix)
                 (clutch--sql-keyword-completion-candidates)))
           (prefer-keyword-p
            (and (not qualified-empty-prefix-p)
                 (not table-context-p)
                 (null context-tables)
                 (clutch--sql-keyword-prefix-p prefix)))
           (candidates nil))
      (setq candidates
            (append
             keyword-candidates
             (cond
              (empty-column-prefix-p
               (car-safe empty-column-result))
              (prefer-keyword-p nil)
              (context-tables
               (clutch--completion-column-candidates
                conn schema context-tables prefix sync-columns-p))
              (qualified-empty-prefix-p nil)
              (t
               (clutch--sql-identifier-completion-candidates
                (append direct-table-candidates
                        (and schema (hash-table-keys schema))))))))
      (when candidates
        (append
         (list beg end
               (completion-table-case-fold candidates)
               :annotation-function annotation-function
               :exclusive 'no
               :exit-function
               (lambda (str status)
                 (when (and (clutch--completion-finished-status-p status)
                            (clutch--sql-keyword-completion-candidate-p str)
                            (clutch--sql-completion-insert-space-p str)
                            (not (looking-at-p "\\s-")))
                   (insert " "))))
         (when qualified-empty-prefix-p
           (list :company-prefix-length t))))))))

(defun clutch-complete-qualified-or-indent ()
  "Complete after a qualifier dot, falling back to indentation elsewhere."
  (interactive)
  (if (clutch--completion-qualified-empty-prefix-bounds)
      (completion-at-point)
    (indent-for-tab-command)))

(defun clutch--eldoc-schema-string (conn schema sym &optional qualified-table)
  "Return an eldoc string for SYM via SCHEMA on CONN, or nil.
Matches SYM as a table name first, then as a column in any visible table.
When QUALIFIED-TABLE is non-nil, resolve field metadata against that table
even if the current statement exceeds `clutch--schema-inline-table-limit'."
  (let ((sync-columns-p (clutch-db-completion-sync-columns-p conn)))
    (cond
     ((not (eq (gethash sym schema 'missing) 'missing))
      (let* ((cols    (clutch--cached-columns schema sym))
             (_       (when (and sync-columns-p (not cols))
                        (clutch--ensure-columns-async conn schema sym)))
             (comment (clutch--cached-table-comment conn sym))
             (_       (when (not (clutch--table-comment-cached-p conn sym))
                        (clutch--ensure-table-comment-async conn sym)))
             (n       (length cols)))
        (concat (propertize (format "[%s] " (clutch-db-database conn)) 'face 'shadow)
                (propertize sym 'face 'font-lock-type-face)
                (when cols
                  (propertize (format "  (%d col%s)" n (if (= n 1) "" "s"))
                              'face 'shadow))
                (when comment
                  (propertize (format "  — %s" comment) 'face 'shadow)))))
     ((>= (length sym) clutch--schema-inline-min-prefix-length)
      (let ((tables (or (and qualified-table (list qualified-table))
                        (clutch--tables-in-current-statement schema))))
        (when (and tables
                   (or qualified-table
                       (<= (length tables) clutch--schema-inline-table-limit)))
          (cl-loop for tbl in tables
                   for cached-cols = (clutch--cached-columns schema tbl)
                   for cols = (cond
                               (cached-cols cached-cols)
                               ((not sync-columns-p) nil)
                               ((clutch-db-busy-p conn)
                                (clutch--ensure-columns-async conn schema tbl)
                                nil)
                               (t
                                (clutch--ensure-columns conn schema tbl)))
                   for matched-col = (and cols
                                          (clutch--identifier-match sym cols))
                   when matched-col
                   return (clutch--eldoc-column-string conn tbl matched-col)))))
     (t nil))))

(defun clutch--eldoc-effective-symbol-at-point (sym schema)
  "Return the effective eldoc symbol at point for raw SYM and SCHEMA.
When point is on the schema qualifier of a schema-qualified table reference
like `schema.table', return the table part if it exists in SCHEMA.  Otherwise
return SYM unchanged."
  (or
   (save-excursion
     (when-let* ((bounds (bounds-of-thing-at-point 'symbol)))
       (goto-char (cdr bounds))
       (when (eq (char-after) ?`) (forward-char 1))
       (when (eq (char-after) ?.)
         (forward-char 1)
         (when (eq (char-after) ?`) (forward-char 1))
         (when-let* ((next-sym (thing-at-point 'symbol t))
                     (schema schema)
                     ((not (eq (gethash next-sym schema 'missing) 'missing))))
           next-sym))))
   sym))

(defun clutch--eldoc-function (&rest _)
  "Eldoc backend for `clutch-mode'.
Returns a documentation string for the SQL identifier at point.
Schema-based info (tables, columns) requires an active connection.
SQL keyword/function docs are shown even without a connection."
  (when-let* ((bounds (bounds-of-thing-at-point 'symbol))
              (sym (buffer-substring-no-properties (car bounds) (cdr bounds))))
    (let* ((schema (clutch--schema-for-connection))
           (qualified-table (clutch--qualified-identifier-table
                             schema (car bounds)))
           (effective-sym (clutch--eldoc-effective-symbol-at-point sym schema)))
    (or
     (when-let* ((conn clutch-connection)
                 (schema schema)
                 ((not (clutch-db-busy-p conn))))
       (clutch--eldoc-schema-string conn schema effective-sym qualified-table))
     (when-let* ((conn clutch-connection)
                 ((clutch-db-live-p conn))
                 ((not (clutch-db-busy-p conn))))
       (clutch--ensure-help-doc conn effective-sym))
     (clutch--eldoc-keyword-string effective-sym)))))

(provide 'clutch-sql)

;;; clutch-sql.el ends here
