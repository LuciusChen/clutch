;;; clutch-test-result.el --- Result buffer ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Filtering, shell commands, export, copy, aggregation, refine, paging,
;; sorting and foreign-key navigation tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; Filter

(ert-deftest clutch-test-client-filter-preserves-page-summary ()
  "Filtering must not change page extent or infer a smaller query total."
  (dolist (spec '((5 nil nil) (5 8 nil) (3 8 5)))
    (pcase-let ((`(,size ,total ,offset) spec))
      (clutch-test--with-result-state
          (:rows '((6 "six") (7 "seven") (8 "eight"))
           :page-current 1 :page-total-rows total :result-max-rows size)
        (setq-local clutch--page-offset offset
                    clutch--connection-render-state '(:connected-p t))
        (clutch--render-result)
        (let ((summary clutch--footer-base-string))
          (dolist (pattern '("eight" "missing"))
            (clutch-test--with-minibuffer-answers (list pattern)
              (call-interactively (key-binding (kbd "/"))))
            (should (equal summary clutch--footer-base-string))
            (should (string-match-p
                     (if (equal pattern "eight") "1/3 page matches"
                       "0/3 page matches")
                     (clutch--footer-mode-line-display)))
            (when (equal pattern "missing")
              (should (string-match-p "No matches on this page" (buffer-string)))
              (should (string-match-p "/.*clear" (buffer-string)))
              (should-not (text-property-not-all
                           (point-min) (point-max) 'clutch-row-idx nil))))
          (clutch-test--with-minibuffer-answers '("")
            (call-interactively (key-binding (kbd "/"))))
          (should (equal summary clutch--footer-base-string))
          (should (= 3 (length (clutch--result-display-rows))))
          (should (= 3 (length clutch--row-start-positions)))
          (should-not (string-match-p "No matches" (buffer-string))))))))

(ert-deftest clutch-test-client-filter-narrows-and-clears ()
  "Client-side filtering should update display rows and pattern.
Clearing the filter should restore the full result set."
  (dolist (case '((substring ("id" "name") ((1 "alice") (2 "bob") (3 "carol"))
                             "ALI" ((1 "alice")))
                  (formatted-value ("id" "value") ((1 nil) (2 42) (3 "hello"))
                                   "42" ((2 42)))
                  (no-matches ("id" "name") ((1 "alice") (2 "bob"))
                              "missing" nil)))
    (pcase-let ((`(,label ,columns ,rows ,pattern ,expected-rows) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-result-state
            (:columns columns :rows rows :render t)
          (clutch-test--with-minibuffer-answers (list pattern)
            (clutch-result-filter))
          (should (equal (clutch--result-display-rows) expected-rows))
          (should (equal clutch--filter-pattern pattern))
          (clutch-test--with-minibuffer-answers '("")
            (clutch-result-filter))
          (should-not clutch--filter-pattern)
          (should-not clutch--filtered-rows)
          (should (equal (clutch--result-display-rows) rows)))))))

(ert-deftest clutch-test-apply-filter-contract ()
  "WHERE filtering should validate, rewrite, clear, and execute consistently."
  (with-temp-buffer
    (should-error (clutch-result-apply-filter) :type 'user-error))
  (clutch-test--with-sqlite-result (conn result)
      '("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
        "INSERT INTO t VALUES (1, 'ann', 10), (2, 'bob', 20)")
      "SELECT name, score FROM t ORDER BY id"
    ;; The hidden row identity column is not offered, and the column at
    ;; point is the default.
    (should (equal (plist-get clutch--row-identity :indices) '(2)))
    (clutch--goto-cell 0 1)
    (clutch--sync-result-cursor-ui)
    (let (offered default)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (_prompt collection &optional _predicate _require-match
                                  _initial _history def &rest _)
                   (setq offered collection
                         default def)
                   ""))
                ((symbol-function 'read-string) (lambda (&rest _) "")))
        (clutch-result-apply-filter)
        (clutch-test--await-queries))
      (should (equal offered '("name" "score")))
      (should (equal default "score"))))
  ;; A result Clutch cannot rewrite refuses before running anything.
  (clutch-test--with-sqlite-result (conn result)
      '("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"
        "INSERT INTO t VALUES (1, 'ann'), (2, 'bob')")
      "SELECT a.id, b.id FROM t a JOIN t b ON a.id = b.id"
    (should-not clutch--result-server-rewritable)
    (let ((rows clutch--result-rows)
          (err (should-error (clutch-result-apply-filter) :type 'user-error)))
      (should (string-match-p "Server-side filter" (error-message-string err)))
      (should (zerop (hash-table-count clutch--running-queries)))
      (should (eq rows clutch--result-rows)))))

(ert-deftest clutch-test-where-filter-real-sqlite-workflow ()
  "\\`W' should change, mistype, write out and clear a filter like a user.
Answers follow the real readers, where an empty answer to a read with a
default returns the default."
  (clutch-test--with-sqlite-result (conn result)
      '("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
        "INSERT INTO people VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'abe', 30)")
      "SELECT id, name, score FROM people ORDER BY id"
    (let (shown)
      (cl-flet ((filter (&rest answers)
                  (setq shown nil)
                  (cl-letf (((symbol-function 'message)
                             (lambda (fmt &rest args)
                               (when fmt
                                 (push (apply #'format-message fmt args) shown)))))
                    (clutch-test--with-minibuffer-answers answers
                      (clutch-result-apply-filter)
                      (clutch-test--await-queries))))
                (ids ()
                  (mapcar #'car clutch--result-rows))
                (point-on (name)
                  (setq-local clutch--header-active-col
                              (cl-position name clutch--result-columns
                                           :test #'string=))))
        (filter "score" "> 15")
        (should (equal (ids) '(2 3)))
        (should (member "Filter applied: WHERE \"score\" > 15" shown))
        ;; Pressing W again picks a column, matched in any case.
        (filter "SCORE" "> 25")
        (should (equal (ids) '(3)))
        ;; A filter typed wrong keeps the result and the filter it had.
        (filter "score >")
        (should (equal (ids) '(3)))
        (should (equal (buffer-local-value 'clutch--where-filter result)
                       "\"score\" > 25"))
        (should (string-suffix-p "(result unchanged)" (car shown)))
        ;; Text other than a column name is the whole condition, shown
        ;; as typed once the result arrives.
        (filter "`name` LIKE 'a%'")
        (should (equal (ids) '(1 3)))
        (should (member "Filter applied: WHERE `name` LIKE 'a%'" shown))
        ;; With point on a column, RET picks it and an empty condition
        ;; clears the filter, with the query it filtered.
        (point-on "score")
        (filter "" "")
        (should (equal (ids) '(1 2 3)))
        (should-not (buffer-local-value 'clutch--where-filter result))
        (should (equal (buffer-local-value 'clutch--base-query result)
                       "SELECT id, name, score FROM people ORDER BY id"))
        (should (member "Filter cleared" shown))
        ;; A mistake without a filter keeps the query, so g and the
        ;; next filter still start from it.
        (filter "score >")
        (should (equal (ids) '(1 2 3)))
        (clutch-result-rerun)
        (clutch-test--await-queries)
        (should (equal (ids) '(1 2 3)))
        (filter "score" "> 25")
        (should (equal (ids) '(3)))
        ;; Without a column at point, RET asks for the whole condition.
        (point-on "nothing")
        (filter "" "id = 2")
        (should (equal (ids) '(2)))
        ;; A bare value after a column compares for equality.
        (filter "score" "10")
        (should (equal (ids) '(1)))
        (should (member "Filter applied: WHERE \"score\" = 10" shown))))))

(ert-deftest clutch-test-where-filter-column-prompt-prefers-exact-names ()
  "A name at the column prompt should pick the column it spells exactly.
Matching in any case only picks a column when no other one matches."
  (let ((conn (make-clutch-test-conn)))
    (dolist (case '((("SCORE" "score") "score" "\"score\" > 15")
                    (("SCORE" "score") "SCORE" "\"SCORE\" > 15")
                    (("SCORE" "name") "score" "\"SCORE\" > 15")))
      (pcase-let ((`(,columns ,answer ,expected) case))
        (clutch-test--with-minibuffer-answers (list answer "> 15")
          (should (equal (clutch--read-where-filter nil columns nil conn)
                         expected)))))
    ;; Another spelling of two columns that differ only in case names
    ;; neither, so it is the whole condition.
    (clutch-test--with-minibuffer-answers '("Score")
      (should (equal (clutch--read-where-filter nil '("SCORE" "score") nil conn)
                     "Score")))))

(ert-deftest clutch-test-kept-result-error-still-warns-of-unknown-transaction ()
  "A filter that loses the connection should still show the error page.
Only a server error on a live connection keeps the result, so a lost
connection keeps its warning that the transaction outcome is unknown."
  (let ((conn (make-clutch-test-conn :live nil))
        (table (clutch--table-key "t")))
    (clutch-test--with-isolated-metadata-caches
      (unwind-protect
          (with-temp-buffer
            (clutch--set-tx-uncertain conn)
            (clutch--set-table-metadata conn table
                                        :column-details '((:name "id")))
            (clutch-test--with-pop-to-buffer-capture shown
              (clutch--present-statement-outcome
               "SELECT 1" conn
               (list :error '(clutch-db-error "Connection closed")
                     :connection conn
                     :elapsed 0.1
                     :result-context '(:keep-result-on-error t)
                     :source-buffer (current-buffer)))
              (with-current-buffer shown
                (should (derived-mode-p 'clutch-result-mode))
                (should (string-match-p
                         (concat "Hint: .*"
                                 (regexp-quote
                                  clutch--transaction-outcome-unknown-message))
                         (buffer-string)))))
            ;; The lost connection is retired: its metadata goes, while the
            ;; transaction state it owes an answer for stays.
            (should-not (clutch--cached-column-details conn table))
            (should (clutch--tx-unresolved-p conn)))
        (clutch--clear-tx-state conn)))))

(ert-deftest clutch-test-rerun-keeps-filter-real-sqlite-workflow ()
  "\\`g' and the refresh after a submit should keep a server-side filter.
They ran the filtered SQL without its context, which left the result
without its filter, \\`W' and row editing."
  (clutch-test--with-sqlite-result (conn result)
      '("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
        "INSERT INTO people VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'abe', 30)")
      "SELECT id, name, score FROM people ORDER BY id"
    (cl-flet ((column (index)
                (mapcar (lambda (row) (nth index row)) clutch--result-rows))
              (filtered-and-editable-p ()
                (and (equal clutch--where-filter "\"score\" > 15")
                     (clutch-result--server-rewritable-p)
                     clutch--row-identity
                     t)))
      (clutch-test--with-minibuffer-answers '("score" "> 15")
        (clutch-result-apply-filter)
        (clutch-test--await-queries))
      (should (equal (column 0) '(2 3)))
      (clutch-result-rerun)
      (clutch-test--await-queries)
      (should (equal (column 0) '(2 3)))
      (should (filtered-and-editable-p))
      ;; Submitting an edit refreshes the result the same way.
      (let ((row (car clutch--result-rows)))
        (clutch-result--apply-edit
         0 1 "bea"
         (list :identity (clutch-db-row-identity-values
                          row clutch--row-identity)
               :original (nth 1 row)
               :original-state (cons nil (nth 1 row)))))
      (cl-letf (((symbol-function 'yes-or-no-p)
                 (lambda (&rest _) t)))
        (clutch-result-submit)
        (clutch-test--await-queries))
      (should (equal (column 1) '("bea" "abe")))
      (should (filtered-and-editable-p))
      (clutch-test--with-minibuffer-answers '("score" "> 25")
        (clutch-result-apply-filter)
        (clutch-test--await-queries))
      (should (equal (column 0) '(3))))))

;;;; Shell command on cell

(ert-deftest clutch-test-shell-command-on-cell-pipes-value-and-requires-cell ()
  "Shell commands should pipe the current cell value and reject non-cell points."
  (dolist (case `(("hello world" "hello world")
                  (42 "42")
                  (,clutch--cell-default-placeholder "<default>")))
    (pcase-let ((`(,value ,expected) case))
      (ert-info ((format "value: %S" value))
        (clutch-test--with-result-state
            (:columns '("id" "v") :rows (list (list 1 value)) :render t)
          (clutch-test--select-cells '(0 1))
          (clutch-test--with-pop-to-buffer-capture shown
            (clutch-result-shell-command-on-cell "cat")
            (should (equal (with-current-buffer shown (buffer-string))
                           expected)))))))
  (with-temp-buffer
    (should-error (clutch-result-shell-command-on-cell "cat") :type 'user-error)))

;;;; Export — dispatch and content

(ert-deftest clutch-test-export-command-writes-selected-format-content ()
  "Export command should write the chosen format through the real export path."
  (dolist (case '(("e c" clipboard "id,name\n1,\"a,b\"\n" nil)
                  ("e -h c" clipboard "1,\"a,b\"\n" nil)
                  ("e -f c" file "id,name\n1,\"a,b\"\n" "CSV")
                  ("e t" clipboard "id\tname\n1\ta,b\n" nil)
                  ("e -h t" clipboard "1\ta,b\n" nil)
                  ("e -f t" file "id\tname\n1\ta,b\n" "TSV")
                  ("e i" clipboard
                   "INSERT INTO users (\"id\", \"name\") VALUES (1, 'a,b');\n"
                   nil)
                  ("e -f i" file
                   "INSERT INTO users (\"id\", \"name\") VALUES (1, 'a,b');\n"
                   nil)
                  ;; UPDATE names the table as the query did.
                  ("e u" clipboard
                   "UPDATE users SET \"name\" = 'a,b' WHERE \"id\" = 1\n"
                   nil)
                  ("e -f u" file
                   "UPDATE users SET \"name\" = 'a,b' WHERE \"id\" = 1\n"
                   nil)))
    (pcase-let ((`(,keys ,target ,expected ,expected-coding-label) case))
      (ert-info ((format "keys: %s" keys))
        (let ((path (make-temp-file "clutch-export-"))
              (kill-ring nil)
              (kill-ring-yank-pointer nil)
              coding-label)
          (unwind-protect
              (clutch-test--with-sqlite-result (conn result)
                  '("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
                    "INSERT INTO users VALUES (1, 'a,b')")
                  "SELECT id, name FROM users"
                (cl-letf (((symbol-function 'completing-read)
                           (lambda (prompt choices &rest _args)
                             (if (string-match
                                  "\\`\\(CSV\\|TSV\\) encoding" prompt)
                                 (progn
                                   (should (member "utf-8" choices))
                                   (setq coding-label
                                         (match-string 1 prompt))
                                   "utf-8-bom")
                               (ert-fail
                                (format "Unexpected prompt: %s" prompt)))))
                          ((symbol-function 'read-file-name)
                           (lambda (&rest _args) path)))
                  (clutch-test--type-keys keys)
                  (clutch-test--await-queries)
                  (should (equal coding-label expected-coding-label))
                  ;; A delimited file is written in the chosen coding, whose
                  ;; byte order mark shows; SQL files have none.
                  (should
                   (equal (if (eq target 'clipboard)
                              (current-kill 0)
                            (with-temp-buffer
                              (set-buffer-multibyte nil)
                              (insert-file-contents-literally path)
                              (buffer-string)))
                          (if expected-coding-label
                              (concat "\357\273\277" expected)
                            expected)))))
            (delete-file path)))))))

(ert-deftest clutch-test-copy-and-export-switches-show-defaults ()
  "Copy and export menus should present their switches with their defaults.
Export exposes Header and Destination before a format is chosen, and copy
presents Header as enabled by default."
  (dolist (case '((clutch-result-export "-h" "(No|Yes)" "Yes")
                  (clutch-result-export "-f" "(Clipboard|File)" "Clipboard")
                  (clutch-result-copy-dispatch "-h" "(No|Yes)" "Yes")))
    (pcase-let ((`(,prefix ,key ,display ,default) case))
      (ert-info ((format "%s %s" prefix key))
        (let ((suffix (clutch-test--transient-suffix-for-key prefix key)))
          (should suffix)
          (let ((shown (transient-format-value suffix)))
            (should (equal (substring-no-properties shown) display))
            (should (eq (get-text-property (string-match default shown)
                                           'face shown)
                        'transient-value))))))))

(ert-deftest clutch-test-csv-content-escaping ()
  "CSV content should include header and escaped values."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "display,name"))
    (let ((csv (clutch--export-csv-content
                '((1 "a,b") (2 "x\"y") (3 "x\ry")
                  (4 nil) (5 "") (6 "NULL")))))
      (should (string-match-p "^id,\"display,name\"\n" csv))
      (should (string-match-p "1,\"a,b\"" csv))
      (should (string-match-p "2,\"x\"\"y\"" csv))
      (should (string-match-p "3,\"x\ry\"" csv))
      (should (string-suffix-p "4,\n5,\"\"\n6,NULL\n" csv)))
    (should (equal (clutch--export-csv-content '((1 "a,b")) t)
                   "1,\"a,b\"\n"))
    (let ((clutch-csv-delimiter ?\;))
      (should (equal (clutch--export-csv-content '((1 "a;b") (2 "c,d")))
                     "id;display,name\n1;\"a;b\"\n2;c,d\n")))
    (setq-local clutch--result-columns '("null" "empty" "marker" "comma" "quote"))
    (let ((clutch-export-null-value-text "NULL"))
      (should (equal (clutch--export-csv-content
                      '((nil "" "NULL" "a,b" "a\"b")) t)
                     "NULL,,\"NULL\",\"a,b\",\"a\"\"b\"\n")))))

(ert-deftest clutch-test-tsv-content-includes-header-and-escapes-fields ()
  "TSV content should preserve its tabular shape around special characters."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "display\tname"))
    (should
     (equal (clutch--export-tsv-content
             '((1 "a,b") (2 "x\ty") (3 "x\"y") (4 "x\ny")
               (5 nil) (6 "") (7 "NULL")))
            (concat
             "id\t\"display\tname\"\n"
             "1\ta,b\n"
             "2\t\"x\ty\"\n"
             "3\t\"x\"\"y\"\n"
             "4\t\"x\ny\"\n"
             "5\t\n6\t\"\"\n7\tNULL\n")))
    (should (equal (clutch--export-tsv-content '((1 "a,b")) t)
                   "1\ta,b\n"))))

(ert-deftest clutch-test-insert-content-builds-full-row-sql ()
  "INSERT export content should build SQL from the ROWS argument."
  :tags '(:smoke)
  (with-temp-buffer
    (setq-local clutch-connection (make-clutch-test-conn)
                clutch--result-columns '("id" "name")
                clutch--result-rows '((999 "current-page-only"))
                clutch--result-source-table "users"
                clutch--last-query "SELECT id, name FROM users")
    (should (equal (clutch--export-insert-content '((1 "a") (2 "b")))
                   (concat
                    "INSERT INTO users (\"id\", \"name\") VALUES (1, 'a');\n"
                    "INSERT INTO users (\"id\", \"name\") VALUES (2, 'b');\n")))
    (should-not (string-match-p "current-page-only"
                                (clutch--export-insert-content '((1 "a")))))))

(ert-deftest clutch-test-update-content-builds-full-row-sql ()
  "UPDATE export content should build SQL from the ROWS argument."
  (with-temp-buffer
    (setq-local clutch-connection (make-clutch-test-conn :table "users"
                                                         :columns '((:name "id")
                                                                    (:name "name")))
                clutch--result-source-table "users"
                clutch--result-columns '("id" "name")
                clutch--result-column-defs
                '((:name "id" :type-category numeric :source-column "id")
                  (:name "name" :type-category text :source-column "name"))
                clutch--result-rows '((999 "current-page-only"))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (should (equal (clutch--export-update-content '((1 "a") (2 "b")))
                   (concat
                    "UPDATE \"users\" SET \"name\" = 'a' WHERE \"id\" = 1\n"
                    "UPDATE \"users\" SET \"name\" = 'b' WHERE \"id\" = 2\n")))
    (should-not (string-match-p "current-page-only"
                                (clutch--export-update-content '((1 "a")))))))

(ert-deftest clutch-test-result-export-formats-follow-result-surface ()
  "Export choices should match SQL, document, and key/value result surfaces."
  (require 'mongodb)
  (require 'clutch-mongodb)
  (cl-labels
      ((render-menu ()
         (let ((menu (clutch-test--transient-menu-text 'clutch-result-export)))
           (should (string-match-p "Export all result rows" menu))
           menu))
       (check-menu (menu present absent)
         (dolist (label present)
           (should (string-match-p (regexp-quote label) menu)))
         (dolist (label absent)
           (should-not (string-match-p (regexp-quote label) menu)))))
    (clutch-test--with-result-state
        (:connection (make-clutch-mongodb-conn
                      :client (make-mongodb-conn :closed t) :database "test")
         :connection-params '(:backend mongodb))
      (check-menu (render-menu)
                  '("CSV" "TSV" "Insert many")
                  '("INSERT SQL" "UPDATE SQL")))
    (clutch-test--with-result-state
        (:connection nil :connection-params '(:backend sqlite))
      (check-menu (render-menu)
                  '("CSV" "TSV" "INSERT SQL" "UPDATE SQL")
                  '("Insert many")))
    (clutch-test--with-result-state
        (:connection nil :connection-params '(:backend redis))
      (check-menu (render-menu)
                  '("CSV" "TSV")
                  '("INSERT SQL" "UPDATE SQL"
                    "Insert many")))))

(ert-deftest clutch-test-copy-export-menus-show-data-scope ()
  "Menus distinguish current cell, selection and locally filtered export."
  (clutch-test--with-result-state
   (:connection nil :connection-params '(:backend sqlite) :render t)
   (let ((transient-mark-mode t))
     (goto-char (point-min))
     (should (string-match-p "Copy current cell"
                             (clutch-test--transient-menu-text
                              'clutch-result-copy-dispatch)))
     (push-mark (point-max) nil t)
     (should (string-match-p "Copy selected cells"
                             (clutch-test--transient-menu-text
                              'clutch-result-copy-dispatch)))
     (deactivate-mark)
     (setq-local clutch--filter-pattern "alice")
     (should (string-match-p "ignores local filter"
                             (clutch-test--transient-menu-text
                              'clutch-result-export))))))

(ert-deftest clutch-test-menu-text-stays-ascii ()
  "Transient menu text stays ASCII so each column lines up.
Transient pads a column by `string-width', which counts an arrow or an
ellipsis as one column even where the font draws it wider, as
PragmataPro does, shifting the rest of that row."
  (let ((dir (file-name-directory (locate-library "clutch.el")))
        texts wide)
    (dolist (file (directory-files dir t "\\`clutch.*\\.el\\'"))
      (with-temp-buffer
        (insert-file-contents file)
        (while (re-search-forward "^(transient-define-prefix " nil t)
          (goto-char (match-beginning 0))
          (cl-labels ((walk (x)
                        (cond ((stringp x)
                               (push x texts)
                               (when (string-match-p "[^[:ascii:]]" x)
                                 (push x wide)))
                              ((consp x) (walk (car x)) (walk (cdr x)))
                              ((vectorp x) (mapc #'walk x)))))
            ;; The menu's groups are vectors, unlike its docstring.
            (dolist (part (read (current-buffer)))
              (when (vectorp part)
                (walk part)))))))
    (should texts)
    (should-not wide)))

(ert-deftest clutch-test-document-copy-uses-backend-snippets ()
  "Document helper copy should use backend-owned mutation snippet generation.
It should explain an ended session and work on a lost one."
  (require 'mongodb)
  (require 'clutch-mongodb)
  (let ((lost (make-clutch-mongodb-conn
               :client (make-mongodb-conn :closed t) :database "test"))
        kill-ring
        kill-ring-yank-pointer)
    (clutch-test--with-result-state
        (:connection nil
         :connection-params '(:backend mongodb)
         :source-table "users"
         :columns '("_id" "name" "age" "clutch__document")
         :column-defs '((:name "_id" :type-category numeric)
                        (:name "name" :type-category text)
                        (:name "age" :type-category numeric)
                        (:name "clutch__document"
                         :type-category json
                         :hidden t
                         :document-source t))
         :rows (list (list 7 "Ann" 30
                           (mongodb-document
                            '(("_id" . 7) ("name" . "Ann") ("age" . 30)))))
         :render t)
      (ert-info ("ended session")
        (let ((err (should-error
                    (clutch-result-copy 'document-insert-one '((0) 0))
                    :type 'user-error)))
          (should (string-match-p "Connection closed"
                                  (error-message-string err)))))
      (setq-local clutch-connection lost)
      (ert-info ("insert one on a lost connection")
        (clutch-result-copy 'document-insert-one '((0) 0))
        (should (equal (current-kill 0)
                       (concat "db.getCollection(\"users\").insertOne("
                               "{\"_id\":7,\"name\":\"Ann\",\"age\":30});"))))
      ;; The snippet names the collection, the document and only the
      ;; selected field.
      (ert-info ("update the selected field")
        (clutch-test--select-cells '(0 1))
        (clutch-result-copy 'document-update-one-set)
        (should (equal (current-kill 0)
                       (concat "db.getCollection(\"users\").updateOne("
                               "{\"_id\":7}, {\"$set\":{\"name\":\"Ann\"}});")))))))

(ert-deftest clutch-test-non-sql-results-reject-sql-mutation-commands ()
  "Document and key/value results should reject SQL-only mutation commands."
  (dolist (backend '(mongodb redis))
    (dolist (case '((copy "Copy INSERT SQL is SQL-only")
                    (edit "Edit / re-edit is SQL-only")))
      (pcase-let ((`(,op ,expected-message) case))
        (ert-info ((format "%s: %s" backend op))
          (clutch-test--with-result-state
              (:connection nil :connection-params (list :backend backend))
            (let ((err (pcase op
                         ('copy (should-error (clutch-result-copy 'insert)
                                              :type 'user-error))
                         ('edit (should-error (clutch-result-edit-cell)
                                              :type 'user-error)))))
              (should (string-match-p expected-message
                                      (error-message-string err))))))))))

(ert-deftest clutch-test-insert-sql-uses-placeholder-for-ambiguous-source ()
  "INSERT copy/export should use a placeholder table for ambiguous queries."
  (clutch-test--with-result-state
      (:connection (make-clutch-test-conn)
       :connection-params '(:backend sqlite)
       :columns '("id" "name")
       :rows '((1 "a") (2 "b"))
       :last-query
       "SELECT u.id, p.name FROM users u JOIN posts p ON p.user_id = u.id"
       :render t)
    (let (kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(0 0))
      (clutch-result-copy 'insert)
      (should (equal (current-kill 0)
                     "INSERT INTO \"MY_TABLE\" (\"id\") VALUES (1);"))
      (should (equal (clutch--export-insert-content '((1 "a") (2 "b")))
                     (concat
                      "INSERT INTO \"MY_TABLE\" (\"id\", \"name\") VALUES (1, 'a');\n"
                      "INSERT INTO \"MY_TABLE\" (\"id\", \"name\") VALUES (2, 'b');\n"))))))

(ert-deftest clutch-test-pg-array-mutation-builders-use-array-literals ()
  "PostgreSQL array mutations should render JSON-style edits as array literals."
  (require 'clutch-db-pg)
  (require 'pgsql)
  (let ((conn (clutch-db-pg--make-connection :client 'fake-pgsql-client)))
    (clutch-test--with-result-state
        (:connection conn
         :source-table "models"
         :columns '("id" "precision")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "precision" :type-category text
                         :backend-type "_int4"))
         :rows '((1 [0 1]))
         :row-identity (clutch-test--primary-row-identity
                        "models" '("id") '(0))
         :pending-edits
         (list (cons (cons (vector 1) 1) "[0,1,2]")))
      (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table &optional _strict)
                   '((:name "id" :type "integer" :backend-type "int4")
                     (:name "precision" :type "ARRAY"
                      :backend-type "_int4")))))
        (should (equal (clutch-result--pending-sql-statements)
                       '("UPDATE \"models\" SET \"precision\" = '{0,1,2}' WHERE \"id\" = 1")))
        (should (equal
                 (clutch-result--render-statements
                  (list (clutch-result-insert--build-sql
                         conn "models" '(("precision" . "[0,1,2]")))))
                 '("INSERT INTO \"models\" (\"precision\") VALUES ('{0,1,2}')")))
        (should (equal
                 (clutch-result--build-insert-statements-for-rows
                  '((1 [0 1 2])) '(1))
                 '("INSERT INTO \"models\" (\"precision\") VALUES ('{0,1,2}');")))))))

(ert-deftest clutch-test-copy-update-uses-selection ()
  "UPDATE copy should generate SQL from the active row/column selection."
  (dolist (case
           `(("region rectangle" ((0 1) (1 2))
              ,(concat
                "UPDATE \"users\" SET \"name\" = 'a', \"status\" = 'new' WHERE \"id\" = 1\n"
                "UPDATE \"users\" SET \"name\" = 'b', \"status\" = 'done' WHERE \"id\" = 2"))
             ("current cell" ((0 1))
              "UPDATE \"users\" SET \"name\" = 'a' WHERE \"id\" = 1")))
    (pcase-let ((`(,label ,cells ,expected) case))
      (ert-info ((format "copy UPDATE selection: %s" label))
        (clutch-test--with-result-state
            (:connection (make-clutch-test-conn :table "users"
                                                :columns '((:name "id")
                                                           (:name "name")
                                                           (:name "status")))
             :source-table "users"
             :columns '("id" "name" "status")
             :column-defs '((:name "id" :type-category numeric)
                            (:name "name" :type-category text)
                            (:name "status" :type-category text))
             :row-identity (clutch-test--primary-row-identity "users" '("id") '(0))
             :rows '((1 "a" "new") (2 "b" "done"))
             :render t)
          (let (kill-ring kill-ring-yank-pointer)
            (apply #'clutch-test--select-cells cells)
            (clutch-result--copy-rows 'update)
            (should (equal (current-kill 0) expected))))))))

(ert-deftest clutch-test-copy-builders-use-filtered-visible-row ()
  "Copy builders should resolve visible indices through filtered display rows."
  (clutch-test--with-result-state
      (:connection (make-clutch-test-conn :table "users"
                                          :columns '((:name "id") (:name "name")))
       :source-table "users"
       :columns '("id" "name")
       :row-identity (clutch-test--primary-row-identity "users" '("id") '(0))
       :rows '((1 "alpha") (2 "beta"))
       :filter-pattern "beta"
       :filtered-rows '((2 "beta"))
       :render t)
    (let (kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(0 1))
      (clutch-result--copy-rows 'update)
      (should (equal (current-kill 0)
                     "UPDATE \"users\" SET \"name\" = 'beta' WHERE \"id\" = 2"))
      (should (equal (clutch--delimited-lines-for-rows
                      (clutch-result--rows-for-display-indices '(0)) '(1) ?,)
                     '("name" "beta")))
      (should (equal (clutch-result--build-insert-statements-for-rows
                      (clutch-result--rows-for-display-indices '(0))
                      '(1))
                     '("INSERT INTO \"users\" (\"name\") VALUES ('beta');"))))))

(ert-deftest clutch-test-copy-update-rejects-non-writable-selections ()
  "UPDATE copy should reject selections that cannot produce writable SET columns."
  (dolist (case
           (list
            (list :label "pk only"
                  :columns '("id" "name")
                  :defs '((:name "id" :type-category numeric)
                          (:name "name" :type-category text))
                  :row '(1 "a")
                  :indices '(0)
                  :details '((:name "id") (:name "name"))
                  :message "Cannot copy UPDATE SQL: no writable source columns selected")
            (list :label "computed result column"
                  :columns '("id" "name" "computed_total")
                  :defs '((:name "id" :type-category numeric)
                          (:name "name" :type-category text)
                          (:name "computed_total" :type-category numeric))
                  :row '(1 "alice" 42)
                  :indices '(0 1 2)
                  :details '((:name "id") (:name "name"))
                  :message "Cannot copy UPDATE SQL: selected columns are not writable source columns: computed_total")
            (list :label "generated source column"
                  :columns '("id" "generated_name")
                  :defs '((:name "id" :type-category numeric)
                          (:name "generated_name" :type-category text))
                  :row '(1 "alice")
                  :indices '(0 1)
                  :details '((:name "id") (:name "generated_name" :generated t))
                  :message "Cannot copy UPDATE SQL: selected columns are not writable source columns: generated_name")))
    (ert-info ((format "copy UPDATE rejection: %s" (plist-get case :label)))
      (clutch-test--with-result-state
          (:connection (make-clutch-test-conn :table "users"
                                              :columns (plist-get case :details))
           :columns (plist-get case :columns)
           :column-defs (plist-get case :defs)
           :source-table "users"
           :row-identity (clutch-test--primary-row-identity "users" '("id") '(0)))
        (let ((err (should-error
                    (clutch-result--build-update-statements-for-rows
                     (list (plist-get case :row))
                     (plist-get case :indices)
                     "copy UPDATE SQL")
                    :type 'user-error)))
          (should (string-match-p
                   (regexp-quote (plist-get case :message))
                   (error-message-string err))))))))

(ert-deftest clutch-test-staged-update-rejects-non-writable-source-columns ()
  "A staged edit should not build an UPDATE of a column no longer writable.
Editing refuses such a column, so this happens when the table changes
after the edit is staged."
  (dolist (case '(("generated" ((:name "id") (:name "name" :generated t)))
                  ("missing" ((:name "id")))))
    (ert-info ((car case))
      (with-temp-buffer
        (setq-local clutch-connection (make-clutch-test-conn :table "users"
                                                             :columns (cadr case))
                    clutch--result-columns '("id" "name")
                    clutch--result-column-defs
                    '((:name "id" :backend-type "int4" :source-column "id")
                      (:name "name" :source-column "name"))
                    clutch--result-source-table "users"
                    clutch--row-identity (clutch-test--primary-row-identity
                                          "users" '("id") '(0))
                    clutch--pending-edits '((([1] . 1) . "alice")))
        (should (equal (error-message-string
                        (should-error (clutch-result--build-update-statements)
                                      :type 'user-error))
                       "Cannot build UPDATE: selected columns are not writable source columns: name"))))))

;;;; Agent context copy

(defun clutch-test--copy-agent-context ()
  "Run `clutch-copy-context-for-agent' with deterministic metadata.
Return the copied text.  The metadata has what SQLite does not report:
table and column comments and an index."
  (let ((clutch--table-metadata-cache (make-hash-table :test 'eq))
        (clutch--object-cache (make-hash-table :test 'eq))
        kill-ring
        kill-ring-yank-pointer)
    (cl-letf (((symbol-function 'clutch--ensure-connection)
               #'ignore)
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "app@db.local:5432/app"))
              ((symbol-function 'clutch-db-display-name)
               (lambda (_conn) "PostgreSQL"))
              ((symbol-function 'clutch-db-database)
               (lambda (_conn) "app"))
              ((symbol-function 'clutch-db-current-schema)
               (lambda (_conn) "public"))
              ((symbol-function 'clutch-db-table-comment)
               (lambda (_conn table &optional _schema)
                 (when (string= table "users")
                   "application users")))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn table)
                 (when (string= table "users")
                   (list (list :name "id" :type "bigint"
                               :nullable nil :primary-key t)
                         (list :name "email" :type "text"
                               :nullable nil :comment "login email"
                               :foreign-key '(:ref-table "orgs"
                                              :ref-column "id"))))))
              ((symbol-function 'clutch--object-related-entries)
               (lambda (_conn entry type)
                 (when (and (string= (plist-get entry :name) "users")
                            (string= type "INDEX"))
                   (list (list :name "users_email_idx" :type "INDEX"
                               :target-table "users" :unique t))))))
      (clutch-copy-context-for-agent)
      (current-kill 0))))

(defconst clutch-test--agent-context-schema
  '("CREATE TABLE orgs (id INTEGER PRIMARY KEY)"
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT NOT NULL REFERENCES orgs(id))"
    "INSERT INTO users VALUES (1, 'ada', 'ada@example.com'), (2, 'bob', 'bob@example.com')")
  "SQLite schema behind the agent context tests.")

(ert-deftest clutch-test-copy-context-for-agent-from-query-console ()
  "Agent context copy should use the public query-console command path."
  (clutch-test--with-sqlite-connection conn
    (dolist (sql clutch-test--agent-context-schema)
      (clutch-db-query conn sql))
    (dolist (case '(("normal" "SELECT id, email FROM users WHERE active = 1" t)
                    ("trailing semicolon"
                     "SELECT id, email FROM users WHERE active = 1;" nil)))
      (pcase-let ((`(,label ,sql ,check-metadata) case))
        (ert-info ((format "case: %s" label))
          (with-temp-buffer
            (clutch-mode)
            (insert sql)
            (setq-local clutch-connection conn)
            (let (kill-ring kill-ring-yank-pointer)
              (clutch-copy-context-for-agent)
              (let ((copied (current-kill 0)))
                (should (string-match-p
                         "SELECT id, email FROM users WHERE active = 1" copied))
                (should (string-match-p "## Table: users" copied))
                (when check-metadata
                  (should (string-match-p "# Clutch database context" copied))
                  (should (string-match-p "- Backend: SQLite" copied))
                  (should (string-match-p "- Connection: sqlite::memory:"
                                          copied))
                  (should (string-match-p "users (TABLE)" copied))
                  (should (string-match-p "Columns (3)" copied))
                  (should (string-match-p "id[[:space:]]+integer[[:space:]]+PK"
                                          copied))
                  (should (string-match-p
                           "email[[:space:]]+text[[:space:]]+NOT NULL, FK -> orgs.id"
                           copied))))))))))
  ;; Comments and indexes, which SQLite does not report, show as well.
  (ert-info ("case: comments and indexes")
    (with-temp-buffer
      (clutch-mode)
      (insert "SELECT id, email FROM users WHERE active = 1")
      (setq-local clutch-connection 'fake-conn)
      (let ((copied (clutch-test--copy-agent-context)))
        (should (string-match-p "Comment\n  application users" copied))
        (should (string-match-p
                 "email[[:space:]]+text[[:space:]]+NOT NULL, FK -> orgs.id, login email"
                 copied))
        (should (string-match-p "Indexes (1)" copied))
        (should (string-match-p "users_email_idx[[:space:]]+UNIQUE"
                                copied))
        (should-not (string-match-p "Row identity candidates" copied))))))

(ert-deftest clutch-test-result-mode-k-copies-agent-context ()
  "The documented result-mode k binding should copy agent context."
  (should (eq (lookup-key clutch-result-mode-map "k")
              #'clutch-copy-context-for-agent)))

(ert-deftest clutch-test-result-mode-header-follows-row-text ()
  "Result headers should follow buffer-local text scaling exactly once.
They should keep the rows' font under a proportional theme: a theme such
as modus with `modus-themes-variable-pitch-ui' gives the header line a
proportional font, while header padding follows the rows.  Only the
family is remapped, so text scaling still applies once."
  (require 'face-remap)
  (let ((text-scale-remap-header-line nil))
    (with-temp-buffer
      (clutch-result-mode)
      (ert-info ("text scaling")
        (should (local-variable-p 'text-scale-remap-header-line))
        (should text-scale-remap-header-line)
        (dolist (face '(mode-line mode-line-inactive))
          (let ((spec (cadr (assq face face-remapping-alist))))
            (should-not (eq (plist-get spec :inherit) 'default)))))
      (ert-info ("row font")
        (let ((spec (cadr (assq 'header-line face-remapping-alist))))
          (should (equal (plist-get spec :family)
                         (face-attribute 'default :family nil t)))
          (should-not (plist-member spec :inherit)))))))

(ert-deftest clutch-test-result-mouse-click-below-table-preserves-point ()
  "Clicking below the rendered table should not move the current cell."
  (should (eq (lookup-key clutch-result-mode-map [mouse-1])
              #'clutch-result-mouse-set-point))
  (should (eq (lookup-key clutch-result-mode-map [down-mouse-1])
              #'clutch-result-mouse-set-point))
  (save-window-excursion
    (with-temp-buffer
      (switch-to-buffer (current-buffer))
      (clutch-result-mode)
      (let ((inhibit-read-only t))
        (insert "row\n"))
      (goto-char 2)
      (let ((event-position (point-max))
            delegated)
        (cl-letf (((symbol-function 'mouse-drag-region)
                   (lambda (_event) (setq delegated 'drag)))
                  ((symbol-function 'mouse-set-point)
                   (lambda (_event) (setq delegated 'set-point))))
          (clutch-result-mouse-set-point
           (list 'mouse-1
                 (list (selected-window) event-position '(0 . 0) 0)))
          (should (= (point) 2))
          (should-not delegated)
          (setq event-position 1)
          (clutch-result-mouse-set-point
           (list 'down-mouse-1
                 (list (selected-window) event-position '(0 . 0) 0)))
          (should (eq delegated 'drag))
          (clutch-result-mouse-set-point
           (list 'mouse-1
                 (list (selected-window) event-position '(0 . 0) 0)))
          (should (eq delegated 'set-point)))))))

(ert-deftest clutch-test-copy-context-for-agent-includes-result-samples ()
  "Agent context copy should include a sample of the latest matching result.
A result buffer copies its effective SQL and current sample rows, and a
query console includes its latest result's sample while that result's SQL
is the console's."
  (let ((clutch-agent-context-max-result-rows 1)
        kill-ring
        kill-ring-yank-pointer)
    (clutch-test--with-sqlite-result (conn result)
        clutch-test--agent-context-schema
        "SELECT name, email FROM users ORDER BY id"
      ;; The primary key is not selected, so a hidden row identity is.
      (should (equal (plist-get clutch--row-identity :indices) '(2)))
      (let ((console (cl-find-if
                      (lambda (buffer)
                        (and (not (eq buffer result))
                             (eq (buffer-local-value 'clutch--last-result-buffer
                                                     buffer)
                                 result)))
                      (buffer-list))))
        (ert-info ("console of the result")
          (with-current-buffer console
            (clutch-copy-context-for-agent)
            (let ((copied (current-kill 0)))
              (should (string-match-p "## Result sample" copied))
              (should (string-match-p
                       "```text\nname\temail\nada\tada@example.com\n```"
                       copied)))))
        (ert-info ("filtered result")
          (clutch-test--with-minibuffer-answers '("email" "LIKE '%example.com'")
            (clutch-result-apply-filter)
            (clutch-test--await-queries))
          (clutch-copy-context-for-agent)
          (let ((copied (current-kill 0)))
            (should (string-match-p
                     (concat "SELECT \\* FROM (SELECT name, email FROM users[^)]*) "
                             "[^\n]*WHERE \"email\" LIKE '%example.com'")
                     copied))
            (should (string-match-p "## Result sample" copied))
            (should (string-match-p "Showing 1 of 2 visible rows" copied))
            (should (string-match-p
                     "```text\nname\temail\nada\tada@example.com\n```"
                     copied))
            (should-not (string-match-p "clutch__rid" copied))
            (should-not (string-match-p "bob" copied))))
        (ert-info ("console once the result's SQL is not its own")
          (with-current-buffer console
            (clutch-copy-context-for-agent)
            (should-not (string-match-p "## Result sample"
                                        (current-kill 0))))))))
  ;; The copied SQL is the result's base query under its filter, not the
  ;; text it last ran.
  (ert-info ("filtered result state")
    (clutch-test--with-result-state
        (:base-query "SELECT id, email FROM users"
         :where-filter "active = 1"
         :source-table "users"
         :columns '("id" "email")
         :rows '((1 "ada@example.com")))
      (should (string-match-p
               (concat "SELECT \\* FROM (SELECT id, email FROM users) "
                       "AS _clutch_filter WHERE active = 1")
               (clutch-test--copy-agent-context))))))

;;;; Aggregate and copy

(ert-deftest clutch-test-selected-row-indices-priority ()
  "Selection priority should be region > current row."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows '((1 "a") (2 "b") (3 "c") (4 "d"))
       :render t)
    (clutch-test--select-cells '(2 0) '(3 1))
    (should (equal (clutch--selected-row-indices) '(2 3)))
    (deactivate-mark)
    (clutch--goto-cell 1 1)
    (should (equal (clutch--selected-row-indices) '(1)))))

(ert-deftest clutch-test-aggregate-selection-scenarios ()
  "Aggregate should summarize current, filtered, and rectangular selections."
  (dolist (case '((current
                   ("id" "score")
                   ((1 "1.5") (2 "2.5") (3 "x") (4 4))
                   nil nil (1 1) nil
                   ("Aggregate \\[score\\]" "sum=2.5" "avg=2.5"
                    "\\[rows=1 cells=1 skipped=0\\]")
                   nil)
                  (filtered
                   ("id" "score")
                   ((1 10) (2 20))
                   "20" ((2 20)) (0 1) nil
                   ("sum=20")
                   ("sum=10"))
                  (region-multi
                   ("id" "a" "b")
                   ((1 10 20) (2 11 21))
                   nil nil (0 1) (1 2)
                   ("Aggregate \\[selection\\]" "sum=62" "avg=15.5"
                    "\\[rows=2 cells=4 skipped=0\\]")
                   nil)
                  (region-single
                   ("id" "score")
                   ((1 "1") (2 "2") (3 "3"))
                   nil nil (0 1) (1 1)
                   ("Aggregate \\[score\\]" "sum=3" "avg=1.5"
                    "\\[rows=2 cells=2 skipped=0\\]")
                   nil)
                  (scientific
                   ("id" "score")
                   ((1 "1E+3") (2 "2.5"))
                   nil nil (0 1) (1 1)
                   ("sum=1002.5" "\\[rows=2 cells=2 skipped=0\\]")
                   nil)))
    (pcase-let ((`(,name ,columns ,rows ,filter ,filtered ,from ,to
                         ,expected ,absent)
                 case))
      (ert-info ((symbol-name name))
        (clutch-test--with-result-state
            (:columns columns
             :rows rows
             :filter-pattern filter
             :filtered-rows filtered
             :render t)
          (let (kill-ring kill-ring-yank-pointer)
            (clutch-test--select-cells from to)
            (clutch-result-aggregate)
            (let ((summary (current-kill 0)))
              (dolist (pattern expected)
                (should (string-match-p pattern summary)))
              (dolist (pattern absent)
                (should-not (string-match-p pattern summary))))))))))

(ert-deftest clutch-test-aggregate-refreshes-footer-without-redrawing-body ()
  "Aggregate should update footer state without rebuilding the result body."
  (clutch-test--with-result-state
      (:columns '("id" "score")
       :rows '((1 10) (2 20))
       :render t)
    (let (kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(1 1))
      (let ((body (buffer-string))
            (tick (buffer-chars-modified-tick)))
        (clutch-result-aggregate)
        (should (equal (buffer-string) body))
        (should (= (buffer-chars-modified-tick) tick))
        (should (= (plist-get clutch--aggregate-summary :sum) 20))
        (should (string-match-p "sum=20"
                                (clutch--footer-mode-line-display)))))))

(ert-deftest clutch-test-aggregate-with-prefix-refines-region ()
  "Prefix-arg aggregate should use refined rectangle selection."
  (clutch-test--with-result-state
      (:columns '("id" "score")
       :rows '((1 "1") (2 "2") (3 "3"))
       :render t)
    (let (kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(0 1) '(2 1))
      (clutch-result-aggregate t)
      (should clutch-refine-mode)
      (clutch--goto-cell 1 1)
      (clutch-refine-toggle-row)
      (clutch-refine-confirm)
      (let ((summary (current-kill 0)))
        (should (string-match-p "sum=4" summary))
        (should (string-match-p "\\[rows=2 cells=2 skipped=0\\]" summary))))))

(ert-deftest clutch-test-down-cell-keeps-region-active ()
  "Row navigation should keep region active for selection workflows."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows '((1 "a") (2 "b") (3 "c"))
       :render t)
    (clutch-test--select-cells '(0 1) '(1 1))
    (let ((deactivate-mark t))
      (clutch-result-down-cell)
      (should-not deactivate-mark))
    (should (equal (cl-subseq (clutch--cell-at-point) 0 2) '(2 1)))
    (should (use-region-p))))

(ert-deftest clutch-test-tsv-copy-selection-contract ()
  "TSV copy should use the selected columns with optional headers."
  (dolist (case '((((0 0) (1 2)) nil "id\tname\tstate\n1\talice\t<default>\n2\tbob\tactive")
                  (((0 1)) nil "name\nalice")
                  (((0 2)) t "<default>")))
    (pcase-let ((`(,cells ,omit-header ,expected) case))
      (ert-info ((format "cells: %S" cells))
        (clutch-test--with-result-state
            (:columns '("id" "name" "state")
             :rows `((1 "alice" ,clutch--cell-default-placeholder)
                     (2 "bob" "active"))
             :render t)
          (let (kill-ring kill-ring-yank-pointer)
            (apply #'clutch-test--select-cells cells)
            (clutch-result-copy 'tsv nil omit-header)
            (should (equal (current-kill 0) expected))))))))

(ert-deftest clutch-test-copy-format-commands-honor-header-switch ()
  "Public copy format commands should copy through the real entry point.
CSV, TSV and Org table copies honor the installed no-header switch."
  (dolist (case '(("c c" "name\nalice")
                  ("c t" "name\nalice")
                  ("c o" "| name  |\n|-------|\n| alice |")
                  ("c -h c" "alice")
                  ("c -h t" "alice")
                  ("c -h o" "| alice |")))
    (pcase-let ((`(,keys ,expected) case))
      (ert-info (keys)
        (clutch-test--with-result-state
            (:columns '("id" "name")
             :rows '((1 "alice"))
             :render t)
          (let (kill-ring kill-ring-yank-pointer)
            (clutch-test--select-cells '(0 1))
            (clutch-test--type-keys keys)
            (should (equal (current-kill 0) expected))))))))

(ert-deftest clutch-test-copy-fmt-with-refine-uses-refined-rectangle ()
  "Refined copy should copy the final rectangle, not the initial region.
Without a region, choosing a format with refinement is refused."
  (clutch-test--with-result-state
      (:columns '("id" "name" "score")
       :rows '((1 "alice" 10)
               (2 "bob" 20)
               (3 "cam" 30))
       :render t)
    (let (kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(0 1) '(2 2))
      (clutch-test--type-keys "c -r c")
      (should clutch-refine-mode)
      (should (equal clutch--refine-rect '((0 1 2) . (1 2))))
      (clutch--goto-cell 1 1)
      (clutch-refine-toggle-row)
      (clutch-refine-toggle-col)
      (clutch-refine-confirm)
      (should (equal (current-kill 0) "score\n10\n30"))
      (ert-info ("without a region")
        (let ((err (should-error (clutch-test--type-keys "c -r c")
                                 :type 'user-error)))
          (should (string-match-p "Set a region"
                                  (error-message-string err))))
        (should-not clutch-refine-mode)))))

(ert-deftest clutch-test-copy-org-table-escapes-table-sensitive-content ()
  "Org table copy should keep one logical table row per result row."
  (clutch-test--with-result-state
      (:columns '("city" "amount" "note")
       :column-defs '((:name "city" :type-category text)
                      (:name "amount" :type-category numeric)
                      (:name "note" :type-category text))
       :rows '(("sh" 1 "a|b")
               ("Tokyo" 200 "x\ny"))
       :render t)
    (let (kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(0 0) '(1 2))
      (clutch-result-copy 'org-table)
      (should (equal (current-kill 0)
                     (concat "| city  | amount | note    |\n"
                             "|-------+--------+---------|\n"
                             "| sh    |      1 | a\\vertb |\n"
                             "| Tokyo |    200 | x\\ny    |"))))))

(ert-deftest clutch-test-copy-delimited-unified-entry-uses-selection ()
  "CSV/TSV copy uses the selection without validating each value twice."
  (dolist (case '((((0 1) (1 2)) "c1,c2\na1,a2\nb1,b2" 6)
                  (((0 1)) "c1\na1" 2)))
    (pcase-let ((`(,cells ,expected ,max-checks) case))
      (clutch-test--with-result-state
          (:columns '("c0" "c1" "c2")
           :rows '((a0 a1 a2)
                   (b0 b1 b2)
                   (c0 c1 c2))
           :render t)
        (dolist (kind '(csv tsv))
          (let ((checks 0)
                (check (symbol-function 'clutch-db-require-complete-value))
                kill-ring kill-ring-yank-pointer)
            ;; Copying deactivates the region, as it does for a user.
            (apply #'clutch-test--select-cells cells)
            (cl-letf (((symbol-function 'clutch-db-require-complete-value)
                       (lambda (value) (cl-incf checks) (funcall check value))))
              (clutch-result-copy kind)
              (should (equal (current-kill 0)
                             (if (eq kind 'tsv)
                                 (subst-char-in-string ?, ?\t expected)
                               expected)))
              (should (<= checks max-checks)))))))))

(ert-deftest clutch-test-copy-insert-unified-entry-uses-selection ()
  "Unified INSERT copy should use either the active region or current cell."
  (dolist (case
           `((((0 0) (1 1))
              ,(concat "INSERT INTO t (\"id\", \"name\") VALUES (1, 'a');\n"
                       "INSERT INTO t (\"id\", \"name\") VALUES (2, 'b');"))
             (((0 1))
              "INSERT INTO t (\"name\") VALUES ('a');")))
    (pcase-let ((`(,cells ,expected) case))
      (ert-info ((format "cells: %S" cells))
        (clutch-test--with-sqlite-result (conn result)
            '("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
              "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)")
            "SELECT id, name, age FROM t"
          (let (kill-ring kill-ring-yank-pointer)
            (apply #'clutch-test--select-cells cells)
            (clutch-result-copy 'insert)
            (should (equal (current-kill 0) expected))))))))

;;;; Refine

(defun clutch-test--setup-refine-result-buffer ()
  "Populate the current buffer with a small rendered result table."
  (clutch-test--init-result-state
   '(:columns ("id" "name")
     :column-defs ((:name "id" :type-category numeric)
                   (:name "name" :type-category text))
     :rows ((1 "alice-long-value") (2 "bob") (3 "carol"))
     :column-widths [2 5]))
  (clutch--refresh-display))

(ert-deftest clutch-test-refine-start-and-cancel ()
  "Starting refine mode should enable the mode and create selection overlays.
Cancelling should exit without invoking the callback and drop them."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (let* ((called nil)
           (callback (lambda (_rect) (setq called t))))
      (ert-info ("start")
        (clutch-result--start-refine '((0 1 2) . (0 1)) callback)
        (should clutch-refine-mode)
        (should (equal clutch--refine-rect '((0 1 2) . (0 1))))
        (should clutch--refine-overlays)
        (should (eq clutch--refine-callback callback)))
      (ert-info ("cancel")
        (clutch-refine-cancel)
        (should-not called)
        (should-not clutch-refine-mode)
        (should-not clutch--refine-overlays)))))

(ert-deftest clutch-test-refine-suspends-automatic-cell-preview ()
  "Refine mode should close and suppress automatic cell previews."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (goto-char (point-min))
    (let ((match (text-property-search-forward
                  'clutch-cell-truncated t #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (let ((clutch-cell-preview-style 'child-frame)
          (clutch--cell-preview-state
           (list :source-buffer (current-buffer)))
          (clutch--cell-preview-timer 'pending)
          cancelled
          scheduled)
      (cl-letf (((symbol-function 'cancel-timer)
                 (lambda (timer) (setq cancelled timer)))
                ((symbol-function 'clutch--cell-preview-supported-p)
                 (lambda (_window) t))
                ((symbol-function 'run-with-idle-timer)
                 (lambda (&rest _args)
                   (setq scheduled t)
                   nil)))
        (clutch-result--start-refine '((0) . (0 1)) #'ignore)
        (should (eq cancelled 'pending))
        (should-not clutch--cell-preview-timer)
        (should-not clutch--cell-preview-state)
        (clutch--schedule-cell-preview)
        (should-not scheduled)
        (clutch-refine-cancel)
        (clutch--schedule-cell-preview)
        (should scheduled)))))

(ert-deftest clutch-test-refine-toggle-excludes-and-includes ()
  "Refine mode should toggle row and column exclusions at point.
It refuses a row or column outside the selection, and a point off the
table."
  (dolist (case '((row clutch-row-idx clutch-refine-toggle-row
                       clutch--refine-excluded-rows)
                  (column clutch-col-idx clutch-refine-toggle-col
                          clutch--refine-excluded-cols)))
    (pcase-let ((`(,label ,property ,toggle ,state-var) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (clutch-test--setup-refine-result-buffer)
          (clutch-result--start-refine '((0 1 2) . (0 1)) #'ignore)
          (goto-char (point-min))
          (let ((match (text-property-search-forward property 1 #'eq)))
            (should match)
            (goto-char (prop-match-beginning match)))
          (funcall toggle)
          (should (equal (symbol-value state-var) '(1)))
          (funcall toggle)
          (should-not (symbol-value state-var))))))
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (clutch-result--start-refine '((0 1) . (0)) #'ignore)
    (dolist (case '(((2 0) clutch-refine-toggle-row "Row not in selection")
                    ((0 1) clutch-refine-toggle-col "Column not in selection")
                    (nil clutch-refine-toggle-row "No row at point")
                    (nil clutch-refine-toggle-col "No column at point")))
      (pcase-let ((`(,cell ,toggle ,text) case))
        (ert-info ((format "refused: %s" text))
          (if cell
              (apply #'clutch--goto-cell cell)
            (goto-char (point-max)))
          (should (equal (error-message-string
                          (should-error (funcall toggle) :type 'user-error))
                         text)))))))

(ert-deftest clutch-test-refine-confirm-calls-callback-with-filtered-rect ()
  "Refine confirm should pass the remaining rectangle to the callback."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (let (seen)
      (clutch-result--start-refine '((0 1 2) . (0 1))
                                   (lambda (rect) (setq seen rect)))
      (goto-char (point-min))
      (let ((row-match (text-property-search-forward 'clutch-row-idx 1 #'eq)))
        (should row-match)
        (goto-char (prop-match-beginning row-match)))
      (clutch-refine-toggle-row)
      (goto-char (point-min))
      (let ((col-match (text-property-search-forward 'clutch-col-idx 0 #'eq)))
        (should col-match)
        (goto-char (prop-match-beginning col-match)))
      (clutch-refine-toggle-col)
      (clutch-refine-confirm)
      (should (equal seen '((0 2) . (1))))
      (should-not clutch-refine-mode))))

(ert-deftest clutch-test-refine-confirm-errors-when-selection-empty ()
  "Refine confirm should error when row or column exclusions empty the selection."
  (dolist (case '((all-rows ((0) . (0 1)) clutch-row-idx 0
                            clutch-refine-toggle-row)
                  (all-cols ((0 1) . (0)) clutch-col-idx 0
                            clutch-refine-toggle-col)))
    (pcase-let ((`(,label ,rect ,property ,value ,toggle) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (clutch-test--setup-refine-result-buffer)
          (clutch-result--start-refine rect #'ignore)
          (goto-char (point-min))
          (let ((match (text-property-search-forward property value #'eq)))
            (should match)
            (goto-char (prop-match-beginning match)))
          (funcall toggle)
          (should-error (clutch-refine-confirm) :type 'user-error))))))

;;;; Page navigation and sorting

(defun clutch-test--insert-ids-sql (count)
  "Return SQL inserting the ids 1 to COUNT into table t."
  (concat "INSERT INTO t (id) VALUES "
          (mapconcat (lambda (id) (format "(%d)" id))
                     (number-sequence 1 count) ", ")))

(ert-deftest clutch-test-page-navigation-contract ()
  "Page commands should error at boundaries and dispatch target pages."
  (let ((clutch-result-max-rows 2))
    (clutch-test--with-sqlite-result (conn result)
        (list "CREATE TABLE t (id INTEGER PRIMARY KEY)"
              (clutch-test--insert-ids-sql 12))
        "SELECT id FROM t ORDER BY id"
      (cl-flet ((ids ()
                  (mapcar #'car clutch--result-rows))
                (run (command)
                  (funcall command)
                  (clutch-test--await-queries))
                (refused (command text)
                  (let ((err (should-error (funcall command)
                                           :type 'user-error)))
                    (should (equal (error-message-string err) text)))))
        (refused #'clutch-result-prev-page "Already on first page")
        (refused #'clutch-result-first-page "Already on first page")
        (run #'clutch-result-next-page)
        (should (= clutch--page-current 1))
        (should (equal (ids) '(3 4)))
        (dotimes (_ 2) (run #'clutch-result-next-page))
        (should (= clutch--page-current 3))
        (run #'clutch-result-prev-page)
        (should (= clutch--page-current 2))
        (should (equal (ids) '(5 6)))
        (run #'clutch-result-first-page)
        (should (= clutch--page-current 0))
        (should (equal (ids) '(1 2)))
        (dotimes (_ 5) (run #'clutch-result-next-page))
        (should (equal (ids) '(11 12)))
        (refused #'clutch-result-next-page "Already on last page")))))

(ert-deftest clutch-test-last-page-navigation ()
  "Last page should calculate final windows and reject boundary states."
  (dolist (case '((normal 237 50 0 4 187 "188-237 of 237 rows")
                  (shift-to-last-window 578 500 1 1 78 "79-578 of 578 rows")))
    (pcase-let ((`(,label ,total ,page-size ,start-page
                          ,expected-page ,expected-offset ,summary)
                 case))
      (ert-info ((symbol-name label))
        (let ((clutch-result-max-rows page-size))
          (clutch-test--with-sqlite-result (conn result)
              (list "CREATE TABLE t (id INTEGER PRIMARY KEY)"
                    (clutch-test--insert-ids-sql total))
              "SELECT id FROM t ORDER BY id"
            (dotimes (_ start-page)
              (clutch-result-next-page)
              (clutch-test--await-queries))
            ;; The rows are counted first, then the last full window loads.
            (clutch-result-last-page)
            (clutch-test--await-queries)
            (should (= clutch--page-total-rows total))
            (should (= clutch--page-current expected-page))
            (should (= clutch--page-offset expected-offset))
            (should (= (length clutch--result-rows) page-size))
            (should (= (caar (last clutch--result-rows)) total))
            (should (string-match-p (regexp-quote summary)
                                    clutch--footer-base-string))
            (let ((err (should-error (clutch-result-last-page)
                                     :type 'user-error)))
              (should (equal (error-message-string err)
                             "Already on last page"))))))))
  (ert-info ("single-page")
    (let ((clutch-result-max-rows 50))
      (clutch-test--with-sqlite-result (conn result)
          (list "CREATE TABLE t (id INTEGER PRIMARY KEY)"
                (clutch-test--insert-ids-sql 30))
          "SELECT id FROM t ORDER BY id"
        ;; Before the rows are counted, M-> counts them and then only
        ;; says this page is the last; once counted, it refuses.
        (let (shown)
          (cl-letf (((symbol-function 'message)
                     (lambda (fmt &rest args)
                       (when fmt
                         (push (apply #'format-message fmt args) shown)))))
            (clutch-result-last-page)
            (clutch-test--await-queries))
          (should (= clutch--page-total-rows 30))
          (should (equal (car shown) "Already on last page")))
        (should-error (clutch-result-last-page) :type 'user-error)))))

(ert-deftest clutch-test-sort-cycle-real-sqlite-workflow ()
  "Keyboard and header sorting should cycle a column's sort state.
\\`s' sorts an unsorted column ascending, then descending, then not at
all, each time from page 0, and another column starts its own cycle; off
a column there is nothing to sort.  A header click sorts by the column
name it captured, not a stale index."
  (let ((clutch-result-max-rows 2))
    (clutch-test--with-sqlite-result (conn result)
        '("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
          "INSERT INTO t VALUES (1, 'cam', 30), (2, 'alice', 20), (3, 'bob', 10)")
        "SELECT id, name, age FROM t"
      (cl-flet ((names ()
                  (mapcar #'cadr clutch--result-rows))
                (next-page ()
                  (clutch-result-next-page)
                  (clutch-test--await-queries))
                (press-s-on (column)
                  (clutch--goto-cell 0 column)
                  (call-interactively #'clutch-result-sort-by-column)
                  (clutch-test--await-queries))
                (click (&rest args)
                  (apply #'clutch-result--sort-by-column-index args)
                  (clutch-test--await-queries)))
        (ert-info ("s at point")
          (next-page)
          (press-s-on 1)
          (should (equal clutch--order-by '("name" . "ASC")))
          (should (= clutch--page-current 0))
          (should (equal (names) '("alice" "bob")))
          (press-s-on 1)
          (should (equal clutch--order-by '("name" . "DESC")))
          (should (equal (names) '("cam" "bob")))
          (next-page)
          (press-s-on 1)
          (should-not clutch--order-by)
          (should-not clutch--sort-column)
          (should-not clutch--sort-descending)
          (should (= clutch--page-current 0))
          (should (equal (names) '("cam" "alice")))
          (press-s-on 1)
          (press-s-on 2)
          (should (equal clutch--order-by '("age" . "ASC")))
          (should (equal (names) '("bob" "alice")))
          (goto-char (point-max))
          (let ((err (should-error (call-interactively
                                    #'clutch-result-sort-by-column)
                                   :type 'user-error)))
            (should (string-match-p "No column at point"
                                    (error-message-string err)))))
        (ert-info ("header click")
          (click 99 "name")
          (should (equal clutch--order-by '("name" . "ASC")))
          (should (equal (names) '("alice" "bob")))
          (let ((err (should-error (click 1 "gone") :type 'user-error)))
            (should (string-match-p "Column not found"
                                    (error-message-string err))))
          (should (equal clutch--order-by '("name" . "ASC"))))))))

(ert-deftest clutch-test-auto-commit-transient-description-shows-state ()
  "Auto-commit transient label should show manual and automatic states."
  (dolist (mode '(manual auto))
    (with-temp-buffer
      (setq-local clutch-connection (make-clutch-test-conn :commit-mode mode))
      (let ((description (clutch--dispatch-auto-commit-description)))
        (should (equal (substring-no-properties description)
                       "Auto-commit (manual|auto)"))
        (let ((case-fold-search nil))
          (should (eq (get-text-property
                       (string-match (symbol-name mode) description)
                       'face description)
                      'transient-value)))))))

(ert-deftest clutch-test-dispatch-menu-names-its-connection-and-transactions ()
  "The main menu heads itself with the connection and hides unused controls."
  (with-temp-buffer
    (should (string-match-p "Not connected"
                            (clutch-test--transient-menu-text 'clutch-dispatch))))
  (dolist (mode '(auto nil))
    (with-temp-buffer
      (setq-local clutch-connection
                  (make-clutch-test-conn :backend 'mysql :commit-mode mode
                                         :params '(:host "db" :port 3306
                                                   :user "me")))
      (clutch--refresh-connection-render-state)
      (let ((menu (clutch-test--transient-menu-text 'clutch-dispatch)))
        (should (string-match-p "me@db .* mysql" menu))
        (should (eq (and (string-match-p "Transaction" menu) t)
                    (and mode t)))
        (should (eq (and (string-match-p "Rollback" menu) t)
                    (and mode t)))))))

(ert-deftest clutch-test-result-menu-heads-itself-with-connection-and-page ()
  "The result menu names its connection, row count and page."
  (clutch-test--with-result-state
      (:columns '("id") :column-widths [4] :rows '((1) (2)) :render t)
    (setq-local clutch-connection
                (make-clutch-test-conn :backend 'mysql
                                       :params '(:host "db" :port 3306
                                                 :user "me")))
    (clutch--refresh-connection-render-state)
    (should (string-match-p "me@db · 2 rows\n"
                            (clutch-test--transient-menu-text
                             'clutch-result-dispatch)))
    (setq-local clutch--page-current 2)
    (should (string-match-p "me@db · 2 rows · page 3\n"
                            (clutch-test--transient-menu-text
                             'clutch-result-dispatch)))))

(ert-deftest clutch-test-menu-headings-ask-the-server-nothing ()
  "Opening a dispatch menu reads cached state, as a schema query can block."
  (let ((conn (make-clutch-test-conn :backend 'mysql :commit-mode 'auto
                                     :params '(:host "db" :port 3306
                                               :user "me"))))
    (clutch-test--with-result-state
        (:columns '("id") :column-widths [4] :rows '((1)) :render t)
      (setq-local clutch-connection conn)
      (clutch--refresh-connection-render-state)
      (let* ((asked nil)
             (probe (lambda (&rest _) (setq asked t))))
        (advice-add 'clutch-db-current-schema :before probe)
        (unwind-protect
            (progn
              (clutch-test--transient-menu-text 'clutch-dispatch)
              (clutch-test--transient-menu-text 'clutch-result-dispatch))
          (advice-remove 'clutch-db-current-schema probe))
        (should-not asked)))))

(ert-deftest clutch-test-layout-keys-keep-the-result-menu-open ()
  "Adjusting the layout repeatedly does not reopen the menu each time."
  (let ((suffixes (transient-suffixes 'clutch-result-dispatch)))
    (dolist (key '("=" "-" "[" "]"))
      (ert-info ((format "key: %s" key))
        (let ((suffix (cl-find-if (lambda (suffix)
                                    (and (slot-boundp suffix 'key)
                                         (equal (oref suffix key) key)))
                                  suffixes)))
          (should suffix)
          (should (and (slot-boundp suffix 'transient)
                       (oref suffix transient))))))))

(ert-deftest clutch-test-query-console-sqli-keys ()
  "Console keys use Clutch results instead of an unrelated SQLi process."
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (other (clutch-db-connect 'sqlite '(:database ":memory:")))
        (source (generate-new-buffer " *clutch-console-keys*"))
        result)
    (unwind-protect
        (save-window-excursion
          (switch-to-buffer source)
          (clutch-mode)
          (setq-local clutch-connection conn)
          (should (eq (key-binding (kbd "C-c C-n")) #'undefined))
          (should-error (call-interactively (key-binding (kbd "C-c C-z")))
                        :type 'user-error)
          (dolist (sql '("CREATE TABLE probe (id INTEGER)" "SELECT 1"))
            (switch-to-buffer source)
            (erase-buffer)
            (insert sql)
            (clutch-execute-buffer)
            (setq result (get-buffer (clutch-result--buffer-name)))
            (with-current-buffer result
              (should (derived-mode-p 'clutch-result-mode)))
            (switch-to-buffer source)
            (call-interactively (key-binding (kbd "C-c C-z")))
            (should (eq (window-buffer (selected-window)) result)))
          ;; A closed result window comes back below the console, where
          ;; queries show results, even where the frame would split sideways.
          (switch-to-buffer source)
          (delete-other-windows)
          (let ((split-width-threshold 20))
            (call-interactively (key-binding (kbd "C-c C-z"))))
          (let ((below (window-in-direction 'below (get-buffer-window source))))
            (should below)
            (should (eq (window-buffer below) result)))
          ;; The result belongs to this connection, not every SQL console.
          (switch-to-buffer source)
          (let ((clutch-connection other))
            (should-error (call-interactively (key-binding (kbd "C-c C-z")))
                          :type 'user-error))
          (kill-buffer result)
          (should-error (call-interactively (key-binding (kbd "C-c C-z")))
                        :type 'user-error))
      (when (buffer-live-p result) (kill-buffer result))
      (kill-buffer source)
      (clutch-db-disconnect other)
      (when (clutch-db-live-p conn) (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-query-dispatches-route-x-to-dwim ()
  "SQL and MongoDB dispatch menus should share the DWIM execute route."
  (require 'clutch-document)
  (should (eq (lookup-key clutch-mode-map (kbd "C-c ?"))
              #'clutch-dispatch))
  (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c ?"))
              #'clutch-mongodb-dispatch))
  (dolist (prefix '(clutch-dispatch clutch-mongodb-dispatch))
    (ert-info ((format "prefix: %s" prefix))
      (let ((execute
             (cl-find-if
              (lambda (suffix)
                (and (slot-boundp suffix 'key)
                     (equal (oref suffix key) "x")))
              (transient-suffixes prefix))))
        (should execute)
        (should (eq (oref execute command) #'clutch-execute-dwim))))))

(ert-deftest clutch-test-edit-transient-heading-shows-staged-count ()
  "Edit transient heading should summarize staged mutation count."
  (with-temp-buffer
    (setq-local clutch--pending-edits '(edit-a edit-b)
                clutch--pending-deletes '(delete-a)
                clutch--pending-inserts '(insert-a insert-b insert-c))
    (should (equal (substring-no-properties
                    (clutch-result--edit-transient-heading))
                   "Edit (6 staged)"))
    (setq-local clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil)
    (should (equal (clutch-result--edit-transient-heading) "Edit"))))

(ert-deftest clutch-test-result-dispatch-pending-actions-follow-state ()
  "Result dispatch should show pending actions only while changes are staged."
  (clutch-test--with-result-state
      (:connection nil :connection-params '(:backend sqlite))
    (let ((labels '("Submit staged"
                    "Discard staged at point"
                    "Copy staged SQL"
                    "Save staged SQL"))
          (menu (clutch-test--transient-menu-text 'clutch-result-dispatch)))
      (dolist (label labels)
        (should-not (string-match-p (regexp-quote label) menu)))
      (setq-local clutch--pending-edits '(pending))
      (setq menu (clutch-test--transient-menu-text 'clutch-result-dispatch))
      (dolist (label labels)
        (should (string-match-p (regexp-quote label) menu))))))

(ert-deftest clutch-test-filter-transient-descriptions-show-current-values ()
  "Result filter labels should expose inactive and active values."
  (with-temp-buffer
    (should (equal (substring-no-properties
                    (clutch-result--client-filter-transient-description))
                   "Client filter (none|active)"))
    (setq-local clutch--filter-pattern "alice"
                clutch--where-filter "age > 18")
    (should (equal (substring-no-properties
                    (clutch-result--client-filter-transient-description))
                   "Client filter (none|active) [alice]"))
    (should (equal (substring-no-properties
                    (clutch-result--where-filter-transient-description))
                   "WHERE filter (none|active) [age > 18]"))))

(ert-deftest clutch-test-fullscreen-transient-description-shows-layout-state ()
  "Result layout label should expose window and fullscreen states."
  (with-temp-buffer
    (setq-local clutch--pre-fullscreen-config nil)
    (should (equal (substring-no-properties
                    (clutch-result--fullscreen-transient-description))
                   "Fullscreen (off|on)"))
    (setq-local clutch--pre-fullscreen-config 'saved-configuration)
    (let ((description (clutch-result--fullscreen-transient-description)))
      (should (equal (substring-no-properties description)
                     "Fullscreen (off|on)"))
      (should (eq (get-text-property (string-match "on" description)
                                    'face description)
                  'transient-value)))))

(defun clutch-test--result-view-after (first second)
  "Present FIRST, scroll its result far down, present SECOND; return the view.
The console stays selected and the result sits in another window, as when a
statement runs from the console.  Return (START-LINE POINT-LINE) of the
result window."
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (console (generate-new-buffer " *console*")))
    (unwind-protect
        (save-window-excursion
          (clutch-db-query conn "CREATE TABLE t (id INTEGER)")
          (dotimes (i 200)
            (clutch-db-query conn (format "INSERT INTO t VALUES (%d)" (1+ i))))
          (delete-other-windows)
          (switch-to-buffer console)
          (clutch-test--execute-and-present first conn)
          (let* ((result (window-buffer (selected-window)))
                 (window (selected-window)))
            (with-current-buffer result
              (set-window-point window (aref clutch--row-start-positions 150))
              (set-window-start window (aref clutch--row-start-positions 145)))
            (redisplay t)
            (select-window (split-window))
            (switch-to-buffer console)
            (clutch-test--execute-and-present second conn)
            (redisplay t)
            (let ((window (get-buffer-window result)))
              (with-current-buffer result
                (list (line-number-at-pos (window-start window))
                      (line-number-at-pos (window-point window)))))))
      (kill-buffer console)
      (dolist (buffer (buffer-list))
        (when (with-current-buffer buffer
                (derived-mode-p 'clutch-result-mode))
          (kill-buffer buffer)))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-another-query-starts-at-the-top ()
  "A result for a different query is shown from its first row."
  (should (equal (clutch-test--result-view-after
                  "SELECT id FROM t" "SELECT id FROM t WHERE id > 0")
                 '(1 1))))

(ert-deftest clutch-test-rerun-keeps-the-scrolled-view ()
  "Running the same query again keeps where the result was scrolled to."
  (let ((view (clutch-test--result-view-after
               "SELECT id FROM t" "SELECT id FROM t")))
    (should (> (car view) 100))
    (should (> (cadr view) 100))))

(ert-deftest clutch-test-sort-transient-description-contract ()
  "Result sort transient description should reflect current sort context."
  (clutch-test--with-result-state
      (:columns '("created_at")
       :column-defs '((:name "created_at"))
       :server-rewritable t)
    (insert "created_at")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 0))
    (goto-char (point-min))
    (let ((desc (clutch-result--sort-transient-description)))
      (should (string-match-p "Sort current" desc))
      (should (string-match-p "(none|asc|desc)" desc))
      (should (string-match-p "\\[created_at\\]" desc))
      (should (eq (get-text-property (string-match "none" desc) 'face desc)
                  'transient-value)))
    (setq-local clutch--sort-column "created_at"
                clutch--sort-descending t)
    (let ((desc (clutch-result--sort-transient-description)))
      (should (eq (get-text-property (string-match "desc" desc) 'face desc)
                  'transient-value)))
    (remove-text-properties (point-min) (point-max) '(clutch-col-idx nil))
    (should (equal (substring-no-properties
                    (clutch-result--sort-transient-description))
                   "Sort current (no column)")))
  (clutch-test--with-result-state
      (:columns '("id" "name" "age")
       :column-defs '((:name "id")
                      (:name "name")
                      (:name "age"))
       :server-rewritable t
       :sort-column "name"
       :sort-descending t)
    (insert "age")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 2))
    (goto-char (point-min))
    (let ((desc (substring-no-properties
                 (clutch-result--sort-transient-description))))
      (should (string-match-p "Sort current" desc))
      (should (string-match-p "\\[age\\]" desc))
      (should (string-match-p "(none|asc|desc)" desc))))
  (clutch-test--with-result-state
      (:columns '("score" "score")
       :column-defs '((:name "score") (:name "score"))
       :server-rewritable nil
       :sort-column "score"
       :sort-descending t)
    (insert "score")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 0))
    (goto-char (point-min))
    (setq-local clutch--local-sort-column-index 1)
    (let ((desc (clutch-result--sort-transient-description)))
      (should (string-match-p "Sort page" desc))
      (should (eq (get-text-property (string-match "none" desc) 'face desc)
                  'transient-value)))
    (put-text-property (point-min) (point-max) 'clutch-col-idx 1)
    (let ((desc (clutch-result--sort-transient-description)))
      (should (eq (get-text-property (string-match "desc" desc) 'face desc)
                  'transient-value)))))

(ert-deftest clutch-test-local-sort-cycles-on-an-empty-page ()
  "Sorting an empty page locally should cycle back to unsorted."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows nil
       :render t)
    (clutch-result--sort-by-column-index 1)
    (should (equal clutch--sort-column "name"))
    (clutch-result--sort-by-column-index 1)
    (should clutch--sort-descending)
    (clutch-result--sort-by-column-index 1)
    (should-not clutch--sort-column)
    (should-not clutch--sort-descending)))

(ert-deftest clutch-test-sort-rejects-hidden-row-identity-column ()
  "Server-side sort should only accept visible user columns."
  (clutch-test--with-result-state
      (:columns '("clutch__rid_0" "id" "name")
       :column-defs '((:name "clutch__rid_0" :hidden t)
                      (:name "id")
                      (:name "name"))
       :server-rewritable t)
    (let ((err (should-error (clutch-result--sort "clutch__rid_0" nil)
                             :type 'user-error)))
      (should (string-match-p "Column clutch__rid_0 not found"
                              (error-message-string err))))))

(ert-deftest clutch-test-sort-falls-back-to-current-page-for-nonrewritable-result ()
  "Arbitrary query results should cycle a local current-page sort."
  (clutch-test--with-result-state
      (:columns '("score" "score")
       :column-defs '((:name "score") (:name "score"))
       :rows '((1 20) (2 nil) (3 10) (4 10))
       :server-rewritable nil
       :render t)
    (cl-flet ((first-shown ()
                (clutch--goto-cell 0 0)
                (nth 2 (clutch--cell-at-point))))
      (clutch-result--sort-by-column-index 1)
      (should (equal (mapcar #'car clutch--result-rows) '(2 3 4 1)))
      (should (= (first-shown) 2))
      (should (equal clutch--local-sort-original-rows
                     '((1 20) (2 nil) (3 10) (4 10))))
      (should (equal clutch--sort-column "score"))
      (should-not clutch--sort-descending)
      (should-not clutch--order-by)
      (should (= clutch--local-sort-column-index 1))
      (clutch--goto-cell 0 1)
      (should (string-match-p
               "Sort page"
               (clutch-result--sort-transient-description)))
      (should (string-match-p
               "ASC\\[score\\] page"
               (substring-no-properties (clutch--footer-sort-part))))
      (clutch-result--sort-by-column-index 1)
      (should (equal (mapcar #'car clutch--result-rows) '(1 3 4 2)))
      (should (= (first-shown) 1))
      (should clutch--sort-descending)
      (should (= clutch--local-sort-column-index 1))
      (clutch-result--sort-by-column-index 1)
      (should (equal (mapcar #'car clutch--result-rows) '(1 2 3 4)))
      (should (= (first-shown) 1))
      (should-not clutch--local-sort-original-rows)
      (should-not clutch--local-sort-column-index)
      (should-not clutch--sort-column))))

(ert-deftest clutch-test-local-numeric-sort-orders-by-value ()
  "Local numeric sorting should not use text order or floating point.
It orders exponent floats, infinity, and NaN as well."
  (dolist (case
           '((exact-decimals
              ((:name "amount" :type-category numeric))
              (("12345678901234567890.12345678901234567891") ("10") (nil) ("2")
               ("12345678901234567890.12345678901234567890"))
              nil
              (nil "2" "10" "12345678901234567890.12345678901234567890"
                   "12345678901234567890.12345678901234567891"))
             (floats-and-sentinels
              ((:name "label" :type-category text)
               (:name "amount" :type-category numeric))
              ((two 2) (nan 0.0e+NaN) (negative -1e+20) (infinity 1.0e+INF))
              1
              (negative two infinity nan))))
    (pcase-let ((`(,label ,column-defs ,rows ,index ,expected) case))
      (ert-info ((symbol-name label))
        (with-temp-buffer
          (setq-local clutch--result-columns
                      (mapcar (lambda (def) (plist-get def :name)) column-defs)
                      clutch--result-column-defs column-defs
                      clutch--local-sort-original-rows rows
                      clutch--filter-pattern nil)
          (clutch-result--sort-local-page "amount" nil index)
          (should (equal (mapcar #'car clutch--result-rows) expected)))))))

;;;; Foreign-key navigation

(ert-deftest clutch-test-qualified-sqlite-result-follows-keys-in-its-schema ()
  "Following a foreign key of aux.children should open aux.parents.
SQLite finds main.parents first for the bare name, with a row of the same key."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        result-buf)
    (unwind-protect
        (progn
          (dolist (sql '("CREATE TABLE main.parents (id INTEGER PRIMARY KEY, label TEXT)"
                         "INSERT INTO main.parents VALUES (1, 'main')"
                         "ATTACH DATABASE ':memory:' AS aux"
                         "CREATE TABLE aux.parents (id INTEGER PRIMARY KEY, label TEXT)"
                         "INSERT INTO aux.parents VALUES (1, 'aux')"
                         "CREATE TABLE aux.children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))"
                         "INSERT INTO aux.children VALUES (1, 1)"))
            (clutch-db-query conn sql))
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection conn)
            (let ((source (current-buffer)))
              (clutch-test--execute-and-present "SELECT * FROM aux.children" conn)
              (setq result-buf
                    (buffer-local-value 'clutch--last-result-buffer source))))
          (ert-run-idle-timers)
          (with-current-buffer result-buf
            (clutch-record--follow-fk
             (cdr (assq 1 clutch--fk-info)) 1 result-buf)
            (clutch-test--await-queries)
            (should (equal clutch--last-query
                           "SELECT * FROM \"aux\".\"parents\" WHERE \"id\" = 1"))
            ;; The followed row is the aux parent, its hidden row identity
            ;; after its columns.
            (should (equal (mapcar (lambda (row) (cl-subseq row 0 2))
                                   clutch--result-rows)
                           '((1 "aux"))))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-copy-csv-separates-fields-with-the-delimiter ()
  "CSV copy separates fields with `clutch-csv-delimiter' and quotes it."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows '((1 "a;b") (2 "c,d"))
       :render t)
    (let ((clutch-csv-delimiter ?\;)
          kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(0 0) '(1 1))
      (cl-letf (((symbol-function 'transient-args) (lambda (_prefix) nil))
                ((symbol-function 'transient-arg-value)
                 (lambda (_flag _args) nil)))
        (clutch-result-copy-csv)
        (should (equal (current-kill 0) "id;name\n1;\"a;b\"\n2;c,d"))))))

(provide 'clutch-test-result)

;;; clutch-test-result.el ends here
