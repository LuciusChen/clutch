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
           (cl-letf (((symbol-function 'read-string)
                      (lambda (&rest _) pattern)))
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
         (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "")))
           (call-interactively (key-binding (kbd "/"))))
         (should (equal summary clutch--footer-base-string))
         (should (= 3 (length (clutch--result-display-rows))))
         (should (= 3 (length clutch--row-start-positions)))
         (should-not (string-match-p "No matches" (buffer-string))))))))

(ert-deftest clutch-test-filter-apply-state ()
  "Client-side filtering should update display rows and pattern."
  (dolist (case
           '((substring
              ("id" "name")
              ((:name "id" :type-category numeric)
               (:name "name" :type-category text))
              ((1 "alice") (2 "bob") (3 "carol"))
              "ALI" ((1 "alice")) "ALI")
             (formatted-value
              ("id" "value")
              ((:name "id" :type-category numeric)
               (:name "value" :type-category numeric))
              ((1 nil) (2 42) (3 "hello"))
              "42" ((2 42)) "42")
             (no-matches
              ("id" "name")
              ((:name "id") (:name "name"))
              ((1 "alice") (2 "bob"))
              "missing" nil "missing")))
    (pcase-let ((`(,label ,columns ,column-defs ,rows
                          ,pattern ,expected-rows ,expected-pattern)
                 case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-result-state
            (:columns columns
             :column-defs column-defs
             :rows rows)
          (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
            (clutch-result--apply-filter pattern)
            (should (equal (clutch--result-display-rows) expected-rows))
            (should (equal clutch--filter-pattern expected-pattern))))))))

(ert-deftest clutch-test-filter-clear-restores-all-rows ()
  "Clearing the client-side filter should restore the full result set."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "name" :type-category text))
       :rows '((1 "alice") (2 "bob") (3 "carol"))
       :column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ali")
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _args) "")))
        (clutch-result-filter))
      (should-not clutch--filtered-rows)
      (should-not clutch--filter-pattern))))

(ert-deftest clutch-test-apply-filter-contract ()
  "WHERE filtering should validate, rewrite, clear, and execute consistently."
  (with-temp-buffer
    (should-error (clutch-result-apply-filter) :type 'user-error))
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--result-server-pageable t
                clutch--result-server-rewritable t
                clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name"))
                clutch--header-active-col 1
                clutch--where-filter nil)
    (let (seen)
      (cl-letf (((symbol-function 'clutch--read-where-filter)
                 (lambda (_current columns default-col _conn)
                   (setq seen (list columns default-col))
                   "id > 1"))
                ((symbol-function 'clutch--execute) #'ignore))
        (clutch-result-apply-filter)
        (should (equal seen '(("id" "name") "id"))))))
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--base-query "SELECT * FROM t"
                clutch--result-source-table "t"
                clutch--result-server-pageable t
                clutch--result-server-rewritable t
                clutch--where-filter "id > 5")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--execute)
                 (lambda (sql &optional result-context)
                   (setq captured (list sql result-context)))))
        (clutch-test--with-minibuffer-answers '("")
          (clutch-result-apply-filter))
        ;; The cleared filter state is installed with the new result.
        (should (equal captured
                       '("SELECT * FROM t"
                         (:base-query nil :where-filter nil
                          :keep-result-on-error t
                          :success-message "Filter cleared")))))))
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable nil
                clutch-connection 'fake-conn
                clutch--last-query "SELECT a.*, b.* FROM a JOIN b ON a.id = b.id LIMIT 10"
                clutch--base-query clutch--last-query
                clutch--result-columns '("id" "name" "id"))
    (let (executed)
      (cl-letf (((symbol-function 'completing-read) (lambda (&rest _args) "id"))
                ((symbol-function 'read-string) (lambda (&rest _args) "= 1"))
                ((symbol-function 'clutch--execute)
                 (lambda (&rest _args) (setq executed t))))
        (let ((err (should-error (clutch-result-apply-filter)
                                 :type 'user-error)))
          (should (string-match-p "Server-side filter"
                                  (error-message-string err))))
        (should-not executed)))))

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
        ;; clears the filter.
        (point-on "score")
        (filter "" "")
        (should (equal (ids) '(1 2 3)))
        (should-not (buffer-local-value 'clutch--where-filter result))
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
        (should (equal (ids) '(2)))))))

(ert-deftest clutch-test-where-filter-column-prompt-prefers-exact-names ()
  "A name at the column prompt should pick the column it spells exactly.
Matching in any case only picks a column when no other one matches."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn name) (format "\"%s\"" name))))
    (dolist (case '((("SCORE" "score") "score" "\"score\" > 15")
                    (("SCORE" "score") "SCORE" "\"SCORE\" > 15")
                    (("SCORE" "name") "score" "\"SCORE\" > 15")))
      (pcase-let ((`(,columns ,answer ,expected) case))
        (clutch-test--with-minibuffer-answers (list answer "> 15")
          (should (equal (clutch--read-where-filter nil columns nil 'conn)
                         expected)))))
    ;; Another spelling of two columns that differ only in case names
    ;; neither, so it is the whole condition.
    (clutch-test--with-minibuffer-answers '("Score")
      (should (equal (clutch--read-where-filter nil '("SCORE" "score") nil 'conn)
                     "Score")))))

(ert-deftest clutch-test-kept-result-error-still-warns-of-unknown-transaction ()
  "A filter that loses the connection should still show the error page.
Only a server error on a live connection keeps the result, so a lost
connection keeps its warning that the transaction outcome is unknown."
  (let (hint retired)
    (with-temp-buffer
      (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) nil))
                ((symbol-function 'clutch--tx-unresolved-p) (lambda (_conn) t))
                ((symbol-function 'clutch--remember-execute-error)
                 (lambda (&rest _) (cons "Connection closed" "Connection closed")))
                ((symbol-function 'clutch-result--display-error)
                 (lambda (_conn _sql _summary _message &optional _elapsed shown)
                   (setq hint shown)
                   nil))
                ((symbol-function 'clutch--retire-query-connection)
                 (lambda (_conn) (setq retired t)))
                ((symbol-function 'message) #'ignore))
        (clutch--present-statement-outcome
         "SELECT 1" 'fake-conn
         (list :error '(clutch-db-error "Connection closed")
               :connection 'fake-conn
               :elapsed 0.1
               :result-context '(:keep-result-on-error t)
               :source-buffer (current-buffer)))))
    (should retired)
    (should (string-match-p
             (regexp-quote clutch--transaction-outcome-unknown-message)
             (or hint "")))))

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
        (with-temp-buffer
          (let (captured)
            (cl-letf (((symbol-function 'clutch--cell-at-point)
                      (lambda () (list 0 1 value)))
                      ((symbol-function 'clutch--view-in-buffer)
                       (lambda (val &rest _args)
                         (setq captured val))))
              (clutch-result-shell-command-on-cell "cat")
              (should (string-match-p expected captured))))))))
  (with-temp-buffer
    (should-error (clutch-result-shell-command-on-cell "cat") :type 'user-error)))

;;;; Export — dispatch and content

(ert-deftest clutch-test-export-command-writes-selected-format-content ()
  "Export command should write the chosen format through the real export path."
  (dolist (case '(("c" nil clipboard "id,name\n1,\"a,b\"\n" nil)
                  ("c" ("--no-header") clipboard "1,\"a,b\"\n" nil)
                  ("c" ("--file") file "id,name\n1,\"a,b\"\n" "CSV")
                  ("t" nil clipboard "id\tname\n1\ta,b\n" nil)
                  ("t" ("--no-header") clipboard "1\ta,b\n" nil)
                  ("t" ("--file") file "id\tname\n1\ta,b\n" "TSV")
                  ("i" nil clipboard
                   "INSERT INTO users (\"id\", \"name\") VALUES (1, 'a,b');\n"
                   nil)
                  ("i" ("--file") file
                   "INSERT INTO users (\"id\", \"name\") VALUES (1, 'a,b');\n"
                   nil)
                  ;; UPDATE names the table as the query did.
                  ("u" nil clipboard
                   "UPDATE users SET \"name\" = 'a,b' WHERE \"id\" = 1\n"
                   nil)
                  ("u" ("--file") file
                   "UPDATE users SET \"name\" = 'a,b' WHERE \"id\" = 1\n"
                   nil)))
    (pcase-let ((`(,key ,args ,target ,expected ,expected-coding-label) case))
      (ert-info ((format "export key: %s, args: %S" key args))
        (let ((path (make-temp-file "clutch-export-"))
              (kill-ring nil)
              (kill-ring-yank-pointer nil)
              (write-region-function (symbol-function 'write-region))
              coding-label
              written-coding)
          (unwind-protect
              (clutch-test--with-sqlite-result (_conn _result)
                  '("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
                    "INSERT INTO users VALUES (1, 'a,b')")
                  "SELECT id, name FROM users"
                (let ((suffix
                       (clutch-test--transient-suffix-for-key
                        'clutch-result-export key)))
                  (should suffix)
                  (cl-letf (((symbol-function 'transient-args)
                             (lambda (_prefix) args))
                            ((symbol-function 'completing-read)
                             (lambda (prompt choices &rest _args)
                               (if (string-match
                                    "\\`\\(CSV\\|TSV\\) encoding" prompt)
                                   (progn
                                     (should (member "utf-8" choices))
                                     (setq coding-label
                                           (match-string 1 prompt))
                                     "utf-8")
                                 (ert-fail
                                  (format "Unexpected prompt: %s" prompt)))))
                            ((symbol-function 'read-file-name)
                             (lambda (&rest _args) path))
                            ((symbol-function 'write-region)
                             (lambda (&rest write-args)
                               (setq written-coding coding-system-for-write)
                               (apply write-region-function write-args))))
                    (funcall (oref suffix command))
                    (should (equal coding-label expected-coding-label))
                    (when expected-coding-label
                      (should (eq written-coding 'utf-8)))
                    (should
                     (equal (if (eq target 'clipboard)
                                (current-kill 0)
                              (with-temp-buffer
                                (insert-file-contents path)
                                (buffer-string)))
                            expected)))))
            (ignore-errors (delete-file path))))))))

(ert-deftest clutch-test-export-transient-shares-copy-option-model ()
  "Export should expose Header and Destination before choosing a format."
  (let* ((header
          (clutch-test--transient-suffix-for-key 'clutch-result-export "-h"))
         (destination
          (clutch-test--transient-suffix-for-key 'clutch-result-export "-f"))
         (header-display (and header (transient-format-value header)))
         (destination-display
          (and destination (transient-format-value destination))))
    (should header)
    (should destination)
    (should (equal (substring-no-properties header-display) "(No|Yes)"))
    (should (eq (get-text-property (string-match "Yes" header-display)
                                  'face header-display)
                'transient-value))
    (should (equal (substring-no-properties destination-display)
                   "(Clipboard|File)"))
    (should (eq (get-text-property
                 (string-match "Clipboard" destination-display)
                 'face destination-display)
                'transient-value))))

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
    (clutch-test--with-native-document-result-buffer
      (cl-letf (((symbol-function
                  'clutch-db-document-mutation-supported-p)
                 (lambda (_conn action) (eq action 'insert-many))))
        (check-menu (render-menu)
                    '("CSV" "TSV" "Insert many")
                    '("INSERT SQL" "UPDATE SQL"))))
    (with-temp-buffer
      (setq-local clutch-connection 'sql-conn
                  clutch--connection-params nil)
      (clutch-test--with-connection-data-model
          ('sql-conn 'mysql 'relational)
        (check-menu (render-menu)
                    '("CSV" "TSV" "INSERT SQL" "UPDATE SQL")
                    '("Insert many"))))
    (with-temp-buffer
      (setq-local clutch-connection 'redis-conn
                  clutch--connection-params nil)
      (clutch-test--with-connection-data-model
          ('redis-conn 'redis 'key-value)
        (check-menu (render-menu)
                    '("CSV" "TSV")
                    '("INSERT SQL" "UPDATE SQL"
                      "Insert many"))))))

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

(ert-deftest clutch-test-document-copy-uses-backend-mutation-snippet-generic ()
  "Document helper copy should use backend-owned mutation snippet generation."
  (clutch-test--with-native-document-result-buffer
    (let ((doc '(("_id" . 7) ("name" . "Ann")))
          captured
          kill-ring
          kill-ring-yank-pointer)
      (clutch-test--init-result-state
       (list :connection 'document-conn
             :source-table "users"
             :columns '("_id" "name" "clutch__document")
             :column-defs '((:name "_id" :type-category numeric)
                            (:name "name" :type-category text)
                            (:name "clutch__document"
                             :type-category json
                             :hidden t
                             :document-source t))
             :rows (list (list 7 "Ann" doc))
             :render t))
      (clutch-test--select-cells '(0 1))
      (cl-letf (((symbol-function 'clutch-db-document-mutation-supported-p)
                 (lambda (_conn action) (eq action 'update-one-set)))
                ((symbol-function 'clutch-db-document-mutation-snippets)
                 (lambda (conn action collection documents &optional fields)
                   (setq captured
                         (list conn action collection documents fields))
                   '("doc.update.snippet();"))))
        (clutch-result-copy 'document-update-one-set)
        (should (equal captured
                       (list 'document-conn
                             'update-one-set
                             "users"
                             (list doc)
                             '("name"))))
        (should (equal (current-kill 0) "doc.update.snippet();"))))))

(ert-deftest clutch-test-document-copy-reports-an-ended-session ()
  "Document copy should explain an ended session and work on a lost one."
  (require 'mongodb)
  (require 'clutch-mongodb)
  (let ((lost (make-clutch-mongodb-conn
               :client (make-mongodb-conn :closed t) :database "test"))
        kill-ring kill-ring-yank-pointer)
    (clutch-test--with-result-state
        (:connection nil :connection-params '(:backend mongodb)
         :source-table "users" :columns '("_id" "clutch__document")
         :column-defs '((:name "_id")
                        (:name "clutch__document" :document-source t :hidden t))
         :rows (list (list 1 (mongodb-document '(("_id" . 1))))))
      (let ((err (should-error
                  (clutch-result-copy 'document-insert-one '((0) 0))
                  :type 'user-error)))
        (should (string-match-p "Connection closed" (error-message-string err))))
      (setq-local clutch-connection lost)
      (clutch-result-copy 'document-insert-one '((0) 0))
      (should (equal (current-kill 0)
                     "db.getCollection(\"users\").insertOne({\"_id\":1});")))))

(ert-deftest clutch-test-non-sql-results-reject-sql-mutation-commands ()
  "Document and key/value results should reject SQL-only mutation commands."
  (dolist (surface '(document key-value))
    (dolist (case '((copy "Copy INSERT SQL is SQL-only")
                    (edit "Edit / re-edit is SQL-only")))
      (pcase-let ((`(,op ,expected-message) case))
        (pcase surface
          ('document
           (clutch-test--with-native-document-result-buffer
             (let ((err (pcase op
                          ('copy (should-error (clutch-result-copy 'insert)
                                               :type 'user-error))
                          ('edit (should-error (clutch-result-edit-cell)
                                               :type 'user-error)))))
               (should (string-match-p expected-message
                                       (error-message-string err))))))
          ('key-value
           (with-temp-buffer
             (setq-local clutch-connection 'redis-conn
                         clutch--connection-params nil)
             (clutch-test--with-connection-data-model
                 ('redis-conn 'redis 'key-value)
               (let ((err (pcase op
                            ('copy (should-error (clutch-result-copy 'insert)
                                                 :type 'user-error))
                            ('edit (should-error (clutch-result-edit-cell)
                                                 :type 'user-error)))))
                 (should (string-match-p expected-message
                                         (error-message-string err))))))))))))

(ert-deftest clutch-test-insert-sql-uses-placeholder-for-ambiguous-source ()
  "INSERT copy/export should use a placeholder table for ambiguous queries."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id")
                clutch--result-rows '((1))
                clutch--last-query
                "SELECT u.id FROM users u JOIN posts p ON p.user_id = u.id")
    (cl-letf (((symbol-function 'clutch--cell-at-point)
               (lambda () (list 0 0 1)))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn s) s))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn s) (format "'%s'" s))))
      (clutch-result--copy-rows 'insert)
      (should (equal (current-kill 0)
                     "INSERT INTO MY_TABLE (id) VALUES (1);"))
      (setq-local clutch--result-columns '("id" "name"))
      (should (equal (clutch--export-insert-content '((1 "a") (2 "b")))
                     (concat
                      "INSERT INTO MY_TABLE (id, name) VALUES (1, 'a');\n"
                      "INSERT INTO MY_TABLE (id, name) VALUES (2, 'b');\n"))))))

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
  "Run `clutch-copy-context-for-agent' with deterministic metadata."
  (let ((clutch--table-metadata-cache (make-hash-table :test 'eq))
        (clutch--object-cache (make-hash-table :test 'eq))
        copied)
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
                               :target-table "users" :unique t)))))
              ((symbol-function 'kill-new)
               (lambda (text) (setq copied text))))
      (clutch-copy-context-for-agent))
    copied))

(ert-deftest clutch-test-copy-context-for-agent-from-query-console ()
  "Agent context copy should use the public query-console command path."
  (dolist (case '(("normal" "SELECT id, email FROM users WHERE active = 1" t)
                  ("trailing semicolon"
                   "SELECT id, email FROM users WHERE active = 1;" nil)))
    (pcase-let ((`(,label ,sql ,check-metadata) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (clutch-mode)
          (insert sql)
          (setq-local clutch-connection 'fake-conn)
          (let ((copied (clutch-test--copy-agent-context)))
            (should (string-match-p
                     "SELECT id, email FROM users WHERE active = 1" copied))
            (should (string-match-p "## Table: users" copied))
            (when check-metadata
              (should (string-match-p "# Clutch database context" copied))
              (should (string-match-p "- Backend: PostgreSQL" copied))
              (should (string-match-p "users (TABLE)" copied))
              (should (string-match-p "Comment\n  application users" copied))
              (should (string-match-p "Columns (2)" copied))
              (should (string-match-p
                       "id[[:space:]]+bigint[[:space:]]+NOT NULL, PK"
                       copied))
              (should (string-match-p
                       "email[[:space:]]+text[[:space:]]+NOT NULL, FK -> orgs.id, login email"
                       copied))
              (should (string-match-p "Indexes (1)" copied))
              (should (string-match-p "users_email_idx[[:space:]]+UNIQUE"
                                      copied))
              (should-not (string-match-p "Row identity candidates"
                                          copied)))))))))

(ert-deftest clutch-test-result-mode-k-copies-agent-context ()
  "The documented result-mode k binding should copy agent context."
  (should (eq (lookup-key clutch-result-mode-map "k")
              #'clutch-copy-context-for-agent)))

(ert-deftest clutch-test-result-mode-scales-header-with-buffer-text ()
  "Result headers should follow buffer-local text scaling exactly once."
  (require 'face-remap)
  (let ((text-scale-remap-header-line nil))
    (with-temp-buffer
      (clutch-result-mode)
      (should (local-variable-p 'text-scale-remap-header-line))
      (should text-scale-remap-header-line)
      (dolist (face '(mode-line mode-line-inactive))
        (let ((spec (cadr (assq face face-remapping-alist))))
          (should-not (eq (plist-get spec :inherit) 'default)))))))

(ert-deftest clutch-test-result-mode-draws-header-in-the-row-font ()
  "Result headers should keep the rows' font under a proportional theme.
A theme such as modus with `modus-themes-variable-pitch-ui' gives the
header line a proportional font, while header padding follows the rows.
Only the family is remapped, so text scaling still applies once."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((spec (cadr (assq 'header-line face-remapping-alist))))
      (should (equal (plist-get spec :family)
                     (face-attribute 'default :family nil t)))
      (should-not (plist-member spec :inherit)))))

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

(ert-deftest clutch-test-copy-context-for-agent-from-result-buffer-includes-sample ()
  "Agent context copy should include effective result SQL and current sample rows."
  (let ((clutch-agent-context-max-result-rows 1)
        copied)
    (clutch-test--with-result-state
        (:base-query "SELECT id, email FROM users"
         :where-filter "active = 1"
         :source-table "users"
         :columns '("clutch__rid_0" "id" "email")
         :column-defs '((:name "clutch__rid_0" :hidden t)
                        (:name "id")
                        (:name "email"))
         :rows (list (vector "rid-1" 1 "ada@example.com")
                     (vector "rid-2" 2 "bob@example.com")))
      (cl-letf (((symbol-function 'clutch-db-apply-where)
                 (lambda (_conn sql filter)
                   (format "SELECT * FROM (%s) AS clutch_q WHERE %s" sql filter))))
        (setq copied (clutch-test--copy-agent-context))))
    (should (string-match-p
             "SELECT \\* FROM (SELECT id, email FROM users) AS clutch_q WHERE active = 1"
             copied))
    (should (string-match-p "## Result sample" copied))
    (should (string-match-p "Showing 1 of 2 visible rows" copied))
    (should (string-match-p "id\temail" copied))
    (should (string-match-p "1\tada@example.com" copied))
    (should-not (string-match-p "clutch__rid_0" copied))
    (should-not (string-match-p "rid-1" copied))
    (should-not (string-match-p "bob@example.com" copied))))

(ert-deftest clutch-test-copy-context-for-agent-from-query-console-includes-last-result-sample ()
  "Agent context copy from a query console should include its latest result sample."
  (let ((clutch-agent-context-max-result-rows 1)
        (result-buf (generate-new-buffer " *clutch-agent-result*"))
        copied)
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (insert "SELECT id, email FROM users")
          (setq-local clutch-connection 'fake-conn
                      clutch--last-result-buffer result-buf)
          (with-current-buffer result-buf
            (clutch-test--init-result-state
             (list :base-query "SELECT id, email FROM users"
                   :columns '("id" "email")
                   :rows (list (vector 1 "ada@example.com")
                               (vector 2 "bob@example.com")))))
          (setq copied (clutch-test--copy-agent-context))
          ;; The sample-section details are proven by the result-buffer
          ;; test above; here the rule is that a console resolves its
          ;; last result buffer at all.
          (should (string-match-p "## Result sample" copied))
          (should (string-match-p "1\tada@example.com" copied)))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf)))))

;;;; Aggregate and copy

(ert-deftest clutch-test-selected-row-indices-priority ()
  "Selection priority should be region > current row."
  (with-temp-buffer
    (cl-letf (((symbol-function 'use-region-p) (lambda () t))
              ((symbol-function 'clutch--rows-in-region)
               (lambda (_beg _end) '(2 3)))
              ((symbol-function 'clutch--row-idx-at-line)
               (lambda () 1))
              ((symbol-function 'region-beginning) (lambda () 10))
              ((symbol-function 'region-end) (lambda () 20)))
      (should (equal (clutch--selected-row-indices) '(2 3)))))
  (with-temp-buffer
    (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
              ((symbol-function 'clutch--row-idx-at-line)
               (lambda () 4)))
      (should (equal (clutch--selected-row-indices) '(4))))))

(ert-deftest clutch-test-aggregate-selection-scenarios ()
  "Aggregate should summarize current, filtered, and rectangular selections."
  (dolist (case '((current nil
                   ("id" "score")
                   ((1 "1.5") (2 "2.5") (3 "x") (4 4))
                   nil nil nil (1 1 "2.5")
                   ("Aggregate \\[score\\]" "sum=2.5" "avg=2.5"
                    "\\[rows=1 cells=1 skipped=0\\]")
                   nil)
                  (filtered nil
                   ("id" "score")
                   ((1 10) (2 20))
                   "20" ((2 20)) nil (0 1 20)
                   ("sum=20")
                   ("sum=10"))
                  (region-multi t
                   ("id" "a" "b")
                   ((1 10 20) (2 11 21))
                   nil nil ((0 1) 1 2) nil
                   ("Aggregate \\[selection\\]" "sum=62" "avg=15.5"
                    "\\[rows=2 cells=4 skipped=0\\]")
                   nil)
                  (region-single t
                   ("id" "score")
                   ((1 "1") (2 "2") (3 "3"))
                   nil nil ((0 2) 1) (0 1 "1")
                   ("Aggregate \\[score\\]" "sum=4" "avg=2"
                    "\\[rows=2 cells=2 skipped=0\\]")
                   nil)
                  (scientific t
                   ("id" "score")
                   ((1 "1E+3") (2 "2.5"))
                   nil nil ((0 1) 1) (0 1 "1E+3")
                   ("sum=1002.5" "\\[rows=2 cells=2 skipped=0\\]")
                   nil)))
    (pcase-let ((`(,name ,region-active ,columns ,rows ,filter ,filtered
                         ,rect ,cell ,expected ,absent)
                 case))
      (ert-info ((symbol-name name))
        (clutch-test--with-result-state
            (:columns columns
             :rows rows
             :filter-pattern filter
             :filtered-rows filtered)
          (let (kill-ring kill-ring-yank-pointer)
            (cl-letf (((symbol-function 'use-region-p)
                       (lambda () region-active))
                      ((symbol-function 'clutch-result--region-rectangle-indices)
                       (lambda () rect))
                      ((symbol-function 'clutch--cell-at-point)
                       (lambda () cell)))
              (clutch-result-aggregate)
              (let ((summary (current-kill 0)))
                (dolist (pattern expected)
                  (should (string-match-p pattern summary)))
                (dolist (pattern absent)
                  (should-not (string-match-p pattern summary)))))))))))

(ert-deftest clutch-test-aggregate-refreshes-footer-without-redrawing-body ()
  "Aggregate should update footer state without rebuilding the result body."
  (clutch-test--with-result-state
      (:columns '("id" "score")
       :rows '((1 10) (2 20)))
    (let ((footer-refreshes 0)
          (body-refreshes 0)
          kill-ring
          kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(1 1 20)))
                ((symbol-function 'clutch--refresh-footer-line)
                 (lambda () (cl-incf footer-refreshes)))
                ((symbol-function 'clutch--refresh-display)
                 (lambda () (cl-incf body-refreshes))))
        (clutch-result-aggregate)
        (should (= footer-refreshes 1))
        (should (= body-refreshes 0))
        (should (= (plist-get clutch--aggregate-summary :sum) 20))))))

(ert-deftest clutch-test-aggregate-with-prefix-refines-region ()
  "Prefix-arg aggregate should use refined rectangle selection."
  (clutch-test--with-result-state
      (:columns '("id" "score")
       :rows '((1 "1") (2 "2") (3 "3")))
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1 2) . (1))))
                ((symbol-function 'clutch-result--start-refine)
                 (lambda (_rect callback)
                   (funcall callback '((0 2) . (1)))))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(0 1 "1"))))
        (clutch-result-aggregate t)
        (let ((summary (current-kill 0)))
          (should (string-match-p "sum=4" summary))
          (should (string-match-p "\\[rows=2 cells=2 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-down-cell-keeps-region-active ()
  "Row navigation should keep region active for selection workflows."
  (with-temp-buffer
    (let ((deactivate-mark t))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch--col-idx-at-point) (lambda () 1))
                ((symbol-function 'get-text-property)
                 (lambda (_pos prop &optional _object)
                   (when (eq prop 'clutch-row-idx) 2)))
                ((symbol-function 'clutch--goto-cell) (lambda (&rest _args) nil)))
        (clutch-result-down-cell)
        (should-not deactivate-mark)))))

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

(ert-deftest clutch-test-copy-format-commands-copy-visible-content ()
  "Public CSV and TSV copy commands should copy through the real entry point."
  (dolist (case '((clutch-result-copy-csv "name\nalice")
                  (clutch-result-copy-org-table "| name  |\n|-------|\n| alice |")
                  (clutch-result-copy-tsv "name\nalice")))
    (pcase-let ((`(,command ,expected-text) case))
      (ert-info ((symbol-name command))
        (clutch-test--with-result-state
            (:columns '("id" "name")
             :rows '((1 "alice"))
             :render t)
          (let (kill-ring kill-ring-yank-pointer)
            (clutch-test--select-cells '(0 1))
            (cl-letf (((symbol-function 'transient-args)
                       (lambda (_prefix) nil))
                      ((symbol-function 'transient-arg-value)
                       (lambda (_flag _args) nil)))
              (funcall command)
              (should (equal (current-kill 0) expected-text)))))))))

(ert-deftest clutch-test-copy-header-switch-omits-tabular-headers ()
  "Copy commands should honor the installed no-header switch."
  (dolist (case '((clutch-result-copy-tsv "alice")
                  (clutch-result-copy-csv "alice")
                  (clutch-result-copy-org-table "| alice |")))
    (pcase-let ((`(,command ,expected) case))
      (clutch-test--with-result-state
          (:columns '("name")
           :rows '(("alice"))
           :render t)
        (let (kill-ring kill-ring-yank-pointer)
          (clutch-test--select-cells '(0 0))
          (cl-letf (((symbol-function 'transient-args)
                     (lambda (_prefix) '("--no-header")))
                    ((symbol-function 'transient-arg-value)
                     (lambda (flag args) (member flag args))))
            (funcall command)
            (should (equal (current-kill 0) expected))))))))

(ert-deftest clutch-test-copy-header-switch-defaults-to-yes ()
  "Copy transient should present Header as enabled by default."
  (let* ((suffix
          (clutch-test--transient-suffix-for-key
           'clutch-result-copy-dispatch "-h"))
         (display (and suffix (transient-format-value suffix))))
    (should suffix)
    (should (equal (substring-no-properties display) "(No|Yes)"))
    (should (eq (get-text-property (string-match "Yes" display)
                                  'face display)
                'transient-value))))

(ert-deftest clutch-test-copy-fmt-with-refine-uses-refined-rectangle ()
  "Refined copy should copy the final rectangle, not the initial region."
  (clutch-test--with-result-state
      (:columns '("id" "name" "score")
       :rows '((1 "alice" 10)
               (2 "bob" 20)
               (3 "cam" 30))
       :render t)
    (let (kill-ring kill-ring-yank-pointer)
      (clutch-test--select-cells '(0 1) '(2 2))
      (cl-letf (((symbol-function 'transient-args)
                 (lambda (_prefix) '("--refine")))
                ((symbol-function 'transient-arg-value)
                 (lambda (flag args)
                   (and (equal flag "--refine")
                        (member "--refine" args))))
                ((symbol-function 'clutch-result--start-refine)
                 (lambda (rect callback)
                   (should (equal rect '((0 1 2) . (1 2))))
                   (funcall callback '((0 2) . (2))))))
        (clutch-result--copy-fmt 'csv)
        (should (equal (current-kill 0) "score\n10\n30"))))))

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
           '((((0 0) (1 1))
              ("INSERT INTO t (\"id\", \"name\") VALUES ('1', 'a');"
               "INSERT INTO t (\"id\", \"name\") VALUES ('2', 'b');"))
	     (((0 1))
	      ("INSERT INTO t (\"name\") VALUES ('a');"))))
    (pcase-let ((`(,cells ,expected-lines) case))
      (clutch-test--with-result-state
          (:connection-params '(:backend mysql)
           :columns '("id" "name" "age")
           :rows '((1 "a" 10) (2 "b" 20))
           :source-table "t"
           :last-query "SELECT id, name, age FROM t"
           :render t)
        (let (kill-ring kill-ring-yank-pointer)
          (apply #'clutch-test--select-cells cells)
          (cl-letf (((symbol-function 'clutch-db-escape-identifier)
                     (lambda (_conn s) (format "\"%s\"" s)))
                    ((symbol-function 'clutch-db-value-to-literal)
                     (lambda (_conn v &optional _formatter)
                       (format "'%s'" v))))
            (clutch-result-copy 'insert)
            (if (cdr cells)
                (dolist (expected expected-lines)
                  (should (string-match-p (regexp-quote expected)
                                          (current-kill 0))))
              (should (equal (current-kill 0) (car expected-lines))))))))))

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

(ert-deftest clutch-test-refine-start-activates-mode-and-overlays ()
  "Starting refine mode should enable the mode and create selection overlays."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (let ((callback (lambda (_rect) nil)))
      (clutch-result--start-refine '((0 1 2) . (0 1)) callback)
      (should clutch-refine-mode)
      (should (equal clutch--refine-rect '((0 1 2) . (0 1))))
      (should clutch--refine-overlays)
      (should (eq clutch--refine-callback callback)))))

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
  "Refine mode should toggle row and column exclusions at point."
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
          (should-not (symbol-value state-var)))))))

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

(ert-deftest clutch-test-refine-cancel-does-not-call-callback ()
  "Refine cancel should exit without invoking the callback."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (let ((called nil))
      (clutch-result--start-refine '((0 1 2) . (0 1))
                                   (lambda (_rect) (setq called t)))
      (clutch-refine-cancel)
      (should-not called)
      (should-not clutch-refine-mode))))

;;;; Page navigation and sorting

(ert-deftest clutch-test-page-navigation-contract ()
  "Page commands should error at boundaries and dispatch target pages."
  (dolist (case '((next clutch-result-next-page)
                  (previous clutch-result-prev-page)
                  (first clutch-result-first-page)))
    (pcase-let ((`(,label ,command) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (pcase label
            ('next
             (setq-local clutch--result-rows (make-list 10 '(1))
                         clutch--page-has-more nil
                         clutch-result-max-rows 10))
            ((or 'previous 'first)
             (setq-local clutch--page-current 0)))
          (should-error (funcall command) :type 'user-error)))))
  (dolist (case '((next clutch-result-next-page 0 1)
                  (previous clutch-result-prev-page 3 2)
                  (first clutch-result-first-page 5 0)))
    (pcase-let ((`(,label ,command ,current-page ,expected-page) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (setq-local clutch--result-rows (make-list 50 '(1))
                      clutch-result-max-rows 50
                      clutch--page-current current-page
                      clutch--page-has-more t)
          (let (executed-page)
            (cl-letf (((symbol-function 'clutch-result--execute-page)
                       (lambda (page) (setq executed-page page))))
              (funcall command)
              (should (= executed-page expected-page)))))))))

(ert-deftest clutch-test-last-page-navigation ()
  "Last page should calculate final windows and reject boundary states."
  (dolist (case '((normal 0 nil 237 50 4 187 nil)
                  (already-last 4 187 237 50 nil nil user-error)
                  (shift-to-last-window 1 500 578 500 1 78 nil)
                  (single-page 0 nil 30 50 nil nil user-error)))
    (pcase-let ((`(,label ,current ,offset ,total ,page-size
                          ,expected-page ,expected-offset ,error-type)
                 case))
      (ert-info ((symbol-name label))
        (with-temp-buffer
          (setq-local clutch--page-current current
                      clutch--page-offset offset
                      clutch--page-total-rows total
                      clutch-result-max-rows page-size)
          (let (executed-page executed-offset)
            (cl-letf (((symbol-function 'clutch-result--execute-page)
                       (lambda (page &optional offset)
                         (setq executed-page page
                               executed-offset offset))))
              (if error-type
                  (should-error (clutch-result-last-page) :type error-type)
                (clutch-result-last-page)
                (should (= executed-page expected-page))
                (should (= executed-offset expected-offset))))))))))

(defmacro clutch-test--with-instant-pages (pages-var &rest body)
  "Run BODY with every result page load succeeding at once.
Each loaded page number is pushed onto PAGES-VAR, and every page holds the
result's current rows."
  (declare (indent 1) (debug (symbolp body)))
  `(let ((,pages-var nil))
     (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
               ((symbol-function 'clutch-db-build-paged-sql)
                (lambda (_conn _sql page-num &rest _)
                  (push page-num ,pages-var)
                  "SELECT 1"))
               ((symbol-function 'clutch-result--run-query)
                (lambda (_sql _query on-result)
                  (funcall on-result
                           (make-clutch-db-result
                            :columns clutch--result-column-defs
                            :rows clutch--result-rows)
                           0)))
               ((symbol-function 'clutch--refresh-display) #'ignore)
               ((symbol-function 'message) #'ignore))
       ,@body)))

(ert-deftest clutch-test-sort-by-column-state-machine ()
  "Keyboard sorting should cycle column state and reject non-column points."
  (dolist (case '((toggle "name" 1 "name" nil nil nil ("name" t nil))
                  (clear "name" 1 "name" t ("name" . "DESC") clear nil)
                  (new-column "age" 2 "name" t nil nil ("age" nil nil))
                  (no-column nil nil nil nil nil error nil)))
    (pcase-let ((`(,label ,text ,col-idx ,sort-column ,sort-descending
                         ,order-by ,expected-state ,expected-sort)
                 case))
      (ert-info ((symbol-name label))
        (clutch-test--with-result-state
            (:columns '("id" "name" "age")
             :column-defs '((:name "id")
                            (:name "name")
                            (:name "age"))
             :server-rewritable t
             :server-pageable t
             :base-query "SELECT id, name, age FROM t"
             :sort-column sort-column
             :sort-descending sort-descending
             :page-current 4)
          (when text
            (insert text)
            (add-text-properties (point-min) (point-max)
                                 (list 'clutch-col-idx col-idx))
            (goto-char (point-min)))
          (setq-local clutch--order-by order-by)
          (let (sort-args)
            (clutch-test--with-instant-pages pages
              (cl-letf (((symbol-function 'completing-read)
                         (lambda (&rest _)
                           (error "unexpected sort column prompt")))
                        ((symbol-function 'clutch-result--sort)
                         (lambda (col desc &optional idx)
                           (setq sort-args (list col desc idx)))))
                (pcase expected-state
                  ('error
                   (let ((err (should-error (clutch-result-sort-by-column)
                                            :type 'user-error)))
                     (should (string-match-p "No column at point"
                                             (error-message-string err)))))
                  ('clear
                   (clutch-result-sort-by-column)
                   (should-not clutch--sort-column)
                   (should-not clutch--sort-descending)
                   (should-not clutch--order-by)
                   (should (= clutch--page-current 0))
                   (should (equal pages '(0)))
                   (should-not sort-args))
                  (_
                   (clutch-result-sort-by-column)
                   (should (equal sort-args expected-sort))))))))))))

(ert-deftest clutch-test-auto-commit-transient-description-shows-state ()
  "Auto-commit transient label should show manual and automatic states."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-connection)
    (cl-letf (((symbol-function 'clutch--dispatch-transaction-controls-inapt-p)
               (lambda () nil)))
      (dolist (case '((t manual) (nil auto)))
        (pcase-let ((`(,manual-p ,active) case))
          (cl-letf (((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_connection) manual-p)))
            (let ((description (clutch--dispatch-auto-commit-description)))
              (should (equal (substring-no-properties description)
                             "Auto-commit (manual|auto)"))
              (let ((case-fold-search nil))
                (should (eq (get-text-property
                             (string-match (symbol-name active) description)
                             'face description)
                            'transient-value))))))))))

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
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch-result--action-supported-p)
               (lambda (action) (eq action 'sql-mutation))))
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
          (should (string-match-p (regexp-quote label) menu)))))))

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
                   "Layout (window|fullscreen)"))
    (setq-local clutch--pre-fullscreen-config 'saved-configuration)
    (let ((description (clutch-result--fullscreen-transient-description)))
      (should (equal (substring-no-properties description)
                     "Layout (window|fullscreen)"))
      (should (eq (get-text-property (string-match "fullscreen" description)
                                    'face description)
                  'transient-value)))))

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

(ert-deftest clutch-test-sort-by-header-column-contract ()
  "Header sorting should use captured names and cycle sort state."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :server-rewritable t
       :server-pageable t
       :base-query "SELECT id, name FROM t")
    (clutch-test--with-instant-pages pages
      (clutch-result--sort-by-column-index 99 "name")
      (should (equal clutch--sort-column "name"))
      (should (equal clutch--order-by '("name" . "ASC")))
      (should (equal pages '(0)))))
  (clutch-test--with-result-state
      (:columns '("id" "age")
       :column-defs '((:name "id") (:name "age")))
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--sort)
                 (lambda (name descending)
                   (setq sort-args (list name descending)))))
        (let ((err (should-error
                    (clutch-result--sort-by-column-index 1 "name")
                    :type 'user-error)))
          (should (string-match-p "Column not found"
                                  (error-message-string err)))))
      (should-not sort-args)))
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :server-rewritable t
       :server-pageable t
       :base-query "SELECT id, name FROM t"
       :page-current 3)
    (clutch-test--with-instant-pages pages
      (clutch-result--sort-by-column-index 1)
      (should (equal clutch--sort-column "name"))
      (should-not clutch--sort-descending)
      (should (equal clutch--order-by '("name" . "ASC")))
      (should (= clutch--page-current 0))
      (clutch-result--sort-by-column-index 1)
      (should (equal clutch--sort-column "name"))
      (should clutch--sort-descending)
      (should (equal clutch--order-by '("name" . "DESC")))
      (clutch-result--sort-by-column-index 1)
      (should-not clutch--sort-column)
      (should-not clutch--sort-descending)
      (should-not clutch--order-by)
      (clutch-result--sort-by-column-index 0)
      (should (equal clutch--sort-column "id"))
      (should-not clutch--sort-descending)
      (should (equal clutch--order-by '("id" . "ASC")))
      (should (equal (nreverse pages) '(0 0 0 0))))))

(ert-deftest clutch-test-local-sort-cycles-on-an-empty-page ()
  "Sorting an empty page locally should cycle back to unsorted."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows nil)
    (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-result--sort-by-column-index 1)
      (should (equal clutch--sort-column "name"))
      (clutch-result--sort-by-column-index 1)
      (should clutch--sort-descending)
      (clutch-result--sort-by-column-index 1)
      (should-not clutch--sort-column)
      (should-not clutch--sort-descending))))

(ert-deftest clutch-test-sort-rejects-hidden-row-identity-column ()
  "Server-side sort should only accept visible user columns."
  (clutch-test--with-result-state
      (:columns '("clutch__rid_0" "id" "name")
       :column-defs '((:name "clutch__rid_0" :hidden t)
                      (:name "id")
                      (:name "name"))
       :server-rewritable t)
    (let (paged)
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (&rest _args) (setq paged t))))
        (let ((err (should-error (clutch-result--sort "clutch__rid_0" nil)
                                 :type 'user-error)))
          (should (string-match-p "Column clutch__rid_0 not found"
                                  (error-message-string err))))
        (should-not paged)))))

(ert-deftest clutch-test-sort-falls-back-to-current-page-for-nonrewritable-result ()
  "Arbitrary query results should cycle a local current-page sort."
  (clutch-test--with-result-state
      (:columns '("score" "score")
       :column-defs '((:name "score") (:name "score"))
       :rows '((1 20) (2 nil) (3 10) (4 10))
       :server-rewritable nil)
    (insert "score")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 1))
    (goto-char (point-min))
    (let ((refreshed 0))
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (&rest _args)
                   (ert-fail "local sort must not execute a query")))
                ((symbol-function 'clutch--refresh-display)
                 (lambda () (cl-incf refreshed)))
                ((symbol-function 'message) #'ignore))
        (clutch-result--sort-by-column-index 1)
        (should (equal (mapcar #'car clutch--result-rows) '(2 3 4 1)))
        (should (equal clutch--local-sort-original-rows
                       '((1 20) (2 nil) (3 10) (4 10))))
        (should (equal clutch--sort-column "score"))
        (should-not clutch--sort-descending)
        (should-not clutch--order-by)
        (should (= clutch--local-sort-column-index 1))
        (should (string-match-p
                 "Sort page"
                 (clutch-result--sort-transient-description)))
        (should (string-match-p
                 "ASC\\[score\\] page"
                 (substring-no-properties (clutch--footer-sort-part))))
        (clutch-result--sort-by-column-index 1)
        (should (equal (mapcar #'car clutch--result-rows) '(1 3 4 2)))
        (should clutch--sort-descending)
        (should (= clutch--local-sort-column-index 1))
        (clutch-result--sort-by-column-index 1)
        (should (equal (mapcar #'car clutch--result-rows) '(1 2 3 4)))
        (should-not clutch--local-sort-original-rows)
        (should-not clutch--local-sort-column-index)
        (should-not clutch--sort-column)
        (should (= refreshed 3))))))

(ert-deftest clutch-test-local-numeric-sort-keeps-exact-decimal-order ()
  "Local numeric sorting should not use text order or floating point."
  (with-temp-buffer
    (setq-local clutch--result-columns '("amount")
                clutch--result-column-defs
                '((:name "amount" :type-category numeric))
                clutch--local-sort-original-rows
                '(("12345678901234567890.12345678901234567891")
                  ("10")
                  (nil)
                  ("2")
                  ("12345678901234567890.12345678901234567890"))
                clutch--filter-pattern nil)
    (clutch-result--sort-local-page "amount" nil)
    (should
     (equal clutch--result-rows
            '((nil)
              ("2")
              ("10")
              ("12345678901234567890.12345678901234567890")
              ("12345678901234567890.12345678901234567891"))))))

(ert-deftest clutch-test-local-numeric-sort-orders-floats-and-sentinels ()
  "Local numeric sorting should order exponent floats, infinity, and NaN."
  (with-temp-buffer
    (setq-local clutch--result-columns '("label" "amount")
                clutch--result-column-defs
                '((:name "label" :type-category text)
                  (:name "amount" :type-category numeric))
                clutch--local-sort-original-rows
                (list (list 'two 2)
                      (list 'nan 0.0e+NaN)
                      (list 'negative -1e+20)
                      (list 'infinity 1.0e+INF))
                clutch--filter-pattern nil)
    (clutch-result--sort-local-page "amount" nil 1)
    (should (equal (mapcar #'car clutch--result-rows)
                   '(negative two infinity nan)))))

;;;; Foreign-key navigation

(ert-deftest clutch-test-qualified-sqlite-result-follows-keys-in-its-schema ()
  "Following a foreign key of aux.children should open aux.parents.
SQLite finds main.parents first for the bare name, with a row of the same key."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        result-buf followed)
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
            (cl-letf (((symbol-function 'clutch--execute)
                       (lambda (sql &rest _) (setq followed sql))))
              (clutch-record--follow-fk
               (cdr (assq 1 clutch--fk-info)) 1 result-buf)))
          (should (equal followed
                         "SELECT * FROM \"aux\".\"parents\" WHERE \"id\" = 1"))
          (should (equal (clutch-db-result-rows (clutch-db-query conn followed))
                         '((1 "aux")))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (clutch-db-disconnect conn))))

(provide 'clutch-test-result)

;;; clutch-test-result.el ends here
